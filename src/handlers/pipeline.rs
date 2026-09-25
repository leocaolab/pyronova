//! The request pipeline every serving path shares: preprocessing (fast path, route
//! resolution, static files, fallback, 405, 404), body collection under one budget, and
//! one `finish` that applies CORS and writes the access-log line for every response.
//!
//! The paths (GIL, sub-interpreter pool, TPC inline + bridge) differ only in where the
//! handler runs; everything before and after that is here.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::body::{Body as _, Incoming};
use hyper::header::{HeaderValue, CONTENT_LENGTH};
use hyper::http::request::Parts;
use hyper::{Method, Request, Response};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::body::{collect, full_body, BodyGate, BoxBody, REQUEST_BUDGET};
use crate::error::{refuse, HandlerError, Refusal, RequestLabel, RequestTag};
use crate::request_head::RequestHead;
use crate::request_id::RequestId;
use crate::router::{Call, Params};
use crate::site::Site;

/// Where a response came from, as the access log's `mode` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Served {
    /// No Python ran: fast path, static file, 404, a rejected body.
    Engine,
    /// A handler on the main interpreter, dispatched from a Tokio thread.
    Main,
    /// A sub-interpreter pool worker.
    Pool,
    /// A TPC thread's own sub-interpreter.
    Inline,
    /// The main interpreter, through the TPC bridge.
    Bridge,
    /// A WebSocket upgrade (the 101 or its rejection).
    WebSocket,
}

impl Served {
    fn label(self) -> &'static str {
        match self {
            Served::Engine => "rust",
            Served::Main => "gil",
            Served::Pool => "subinterp",
            Served::Inline => "tpc-inline",
            Served::Bridge => "tpc-gil-bridge",
            Served::WebSocket => "websocket",
        }
    }
}

/// The request as the access log names it.
pub(crate) struct RequestLine<'a> {
    pub(crate) method: &'a Method,
    pub(crate) path: &'a str,
    pub(crate) start: Instant,
}

impl<'a> RequestLine<'a> {
    pub(crate) fn of(label: &'a RequestLabel, start: Instant) -> Self {
        RequestLine {
            method: &label.method,
            path: label.uri.path(),
            start,
        }
    }
}

/// Applies CORS, drops the body of a `HEAD` response, and writes the access-log line.
/// Every response of every path goes through here exactly once.
#[inline]
pub(crate) fn finish(
    mut resp: Response<BoxBody>,
    site: &Site,
    line: &RequestLine<'_>,
    served: Served,
) -> Response<BoxBody> {
    if let Some(cors) = &site.config.cors {
        cors.apply(resp.headers_mut());
    }
    if *line.method == Method::HEAD {
        resp = without_body(resp);
    }
    let status = resp.status();
    if site.config.access_log.samples(status) {
        tracing::info!(
            target: "pyronova::access",
            method = line.method.as_str(),
            path = line.path,
            status = status.as_u16(),
            latency_us = line.start.elapsed().as_micros() as u64,
            mode = served.label(),
            "Request handled"
        );
    }
    resp
}

/// A `HEAD` response: the headers a `GET` would get, with its length, and no body.
/// (HTTP/1.1 would drop the body itself; HTTP/2 would send it.)
fn without_body(resp: Response<BoxBody>) -> Response<BoxBody> {
    let (mut parts, body) = resp.into_parts();
    if let Some(len) = body.size_hint().exact() {
        parts
            .headers
            .entry(CONTENT_LENGTH)
            .or_insert_with(|| HeaderValue::from(len));
    }
    let empty = Empty::<Bytes>::new().map_err(|e| match e {}).boxed();
    Response::from_parts(parts, empty)
}

// ─────────────────────────── preprocessing ───────────────────────────

/// What preprocessing decided. A return value moved once per request and never stored,
/// so the size gap between the variants costs nothing; boxing `Prepared` would cost an
/// allocation on every dispatched request.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Preprocessed {
    /// Answered without a handler (gRPC, fast path, static file, 405, 404), already
    /// finished.
    Respond(Response<BoxBody>),
    /// A handler runs it.
    Dispatch(Prepared),
}

/// A request resolved to a handler. Its head is moved out of hyper's request, not copied:
/// the handler's `Request` takes the method, URI and headers as they are.
pub(crate) struct Prepared {
    pub(crate) head: RequestHead,
    pub(crate) body: Incoming,
    pub(crate) call: Call,
    pub(crate) start: Instant,
}

/// The request's `Accept-Encoding`, kept for the response after the headers move into the
/// handler's `Request`. A refcount copy of the field, no allocation.
pub(crate) struct AcceptEncoding(Option<HeaderValue>);

impl AcceptEncoding {
    pub(crate) fn of(head: &RequestHead) -> Self {
        Self(
            head.parts
                .headers
                .get(hyper::header::ACCEPT_ENCODING)
                .cloned(),
        )
    }

    /// The codings the client accepts; `None` when it sent no `Accept-Encoding`, or one
    /// that isn't visible ASCII (then nothing is known to be accepted).
    pub(crate) fn as_str(&self) -> Option<&str> {
        self.0.as_ref().and_then(|v| v.to_str().ok())
    }
}

/// gRPC benchmark method (when enabled), fast path, route resolution, then static file,
/// fallback, 405 or 404.
pub(crate) async fn preprocess(
    req: Request<Incoming>,
    site: &Site,
    client_ip: std::net::IpAddr,
) -> Result<Preprocessed, hyper::Error> {
    crate::monitor::count_request();
    if site.config.grpc_benchmark && crate::grpc::is_get_sum_call(&req) {
        return grpc_get_sum(req, site).await.map(Preprocessed::Respond);
    }
    let start = Instant::now();

    if let Some(fast) = site
        .routes
        .fast_response(req.method().as_str(), req.uri().path())
    {
        let line = RequestLine {
            method: req.method(),
            path: req.uri().path(),
            start,
        };
        let resp = full_body(fast.to_response());
        return Ok(Preprocessed::Respond(finish(
            resp,
            site,
            &line,
            Served::Engine,
        )));
    }

    let (parts, body) = req.into_parts();
    let resolved = site.routes.resolve(parts.method.as_str(), parts.uri.path());
    let (call, params) = match resolved {
        Some(found) => found,
        None => match unrouted(&parts, site).await {
            Unrouted::Fallback(call) => (call, Params::new()),
            Unrouted::Respond(resp) => {
                let line = RequestLine {
                    method: &parts.method,
                    path: parts.uri.path(),
                    start,
                };
                return Ok(Preprocessed::Respond(finish(
                    resp,
                    site,
                    &line,
                    Served::Engine,
                )));
            }
        },
    };
    let request_id = RequestId::of(&parts.headers, site.config.request_id_header.as_ref());
    Ok(Preprocessed::Dispatch(Prepared {
        head: RequestHead {
            parts,
            params,
            client_ip,
            request_id,
        },
        body,
        call,
        start,
    }))
}

enum Unrouted {
    Fallback(Call),
    Respond(Response<BoxBody>),
}

/// No route matched: a static file (GET/HEAD), else the fallback handler, else 405 when
/// the path has routes for other methods, else 404.
async fn unrouted(parts: &Parts, site: &Site) -> Unrouted {
    let method = &parts.method;
    let path = parts.uri.path();
    if method == Method::GET || method == Method::HEAD {
        if let Some(resp) = crate::static_fs::try_static_file(path, &site.routes.static_dirs).await
        {
            return Unrouted::Respond(full_body(resp));
        }
    }
    if let Some(call) = site.routes.fallback_call() {
        return Unrouted::Fallback(call);
    }
    let resp = match site.routes.allowed_methods(path) {
        Some(allow) => crate::response::method_not_allowed_response(allow),
        None => crate::response::not_found_response(),
    };
    Unrouted::Respond(full_body(resp))
}

/// gRPC needs HTTP/2 trailers (`grpc-status`) the normal response path can't model, so the
/// benchmark method is answered outside the route table.
async fn grpc_get_sum(
    req: Request<Incoming>,
    site: &Site,
) -> Result<Response<BoxBody>, hyper::Error> {
    let start = Instant::now();
    let resp = crate::grpc::handle_get_sum(req, site.config.limits.max_body_bytes).await?;
    let line = RequestLine {
        method: &Method::POST,
        path: crate::grpc::GET_SUM_PATH,
        start,
    };
    Ok(finish(resp, site, &line, Served::Engine))
}

// ─────────────────────────── body collection ───────────────────────────

/// The sub-interpreter pool's admission gate: a body past `skip_bytes` needs a permit.
/// The upfront decision keys off `Content-Length`, which the client controls (chunked and
/// HTTP/2 bodies carry none, and it can be under-declared), so the collector re-checks
/// against the bytes actually buffered and takes the permit then.
pub(crate) struct Admission<'a> {
    pub(crate) semaphore: &'a Arc<Semaphore>,
    pub(crate) skip_bytes: u64,
    /// Taken upfront from an honestly declared large `Content-Length`.
    pub(crate) permit: Option<OwnedSemaphorePermit>,
}

/// A collected body and the admission permit it took, if it needed one. The permit is
/// held until the response is ready, so admitted bodies bound the memory in flight.
pub(crate) struct Admitted {
    pub(crate) body: Bytes,
    pub(crate) permit: Option<OwnedSemaphorePermit>,
}

/// The whole body for a handler's request: at most `max` bytes, within
/// [`REQUEST_BUDGET`].
pub(crate) async fn collect_body(body: Incoming, max: usize) -> Result<Bytes, HandlerError> {
    Ok(crate::body::read_body(body, max).await?)
}

/// [`collect_body`], passing the pool's admission gate: a body that needs a permit when
/// none is free is refused as overloaded.
pub(crate) async fn collect_body_with_admission(
    body: Incoming,
    max: usize,
    mut admission: Admission<'_>,
) -> Result<Admitted, HandlerError> {
    let body = collect(body, max, &mut admission).await?;
    Ok(Admitted {
        body,
        permit: admission.permit,
    })
}

/// No admission permit was free.
pub(crate) fn overloaded() -> HandlerError {
    refuse(Refusal::Overloaded("admission permits"))
}

impl BodyGate for Admission<'_> {
    type Error = HandlerError;
    fn admit(&mut self, buffered: usize) -> Result<(), HandlerError> {
        if self.permit.is_none() && buffered as u64 > self.skip_bytes {
            let permit = self
                .semaphore
                .clone()
                .try_acquire_owned()
                .map_err(|_| overloaded())?;
            self.permit = Some(permit);
        }
        Ok(())
    }
}

// ─────────────────────────── giving up ───────────────────────────

/// The response for an error that happened here, at the edge (no reply, no body, no
/// capacity): logged now, then rendered.
pub(crate) fn fail(error: HandlerError, request: &RequestTag<'_>) -> Response<BoxBody> {
    full_body(error.log(request).into_response())
}

/// Waits for a handler's reply for at most [`REQUEST_BUDGET`]. A reply that ends without
/// an answer is `lost`'s error (a closed channel, a panicked task).
pub(crate) async fn await_reply<T, E>(
    reply: impl std::future::Future<Output = Result<T, E>>,
    lost: impl FnOnce(E) -> HandlerError,
) -> Result<T, HandlerError> {
    match tokio::time::timeout(REQUEST_BUDGET, reply).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(lost(e)),
        Err(_) => Err(refuse(Refusal::Timeout)),
    }
}

/// [`await_reply`] for a request whose body is streamed to its handler: the budget starts
/// once the body has been read (`body_read`, the feeder, ends), as a buffered request's
/// starts once its body is in. Until then the feeder's own deadline bounds the wait (the
/// whole body within [`REQUEST_BUDGET`]), so a client that stalls is answered 408 by the
/// body's budget, not 504 by the handler's.
pub(crate) async fn await_streamed_reply<T, E>(
    body_read: &mut (impl std::future::Future + Unpin),
    reply: impl std::future::Future<Output = Result<T, E>>,
    lost: impl FnOnce(E) -> HandlerError,
) -> Result<T, HandlerError> {
    tokio::pin!(reply);
    tokio::select! {
        replied = &mut reply => return replied.map_err(lost),
        _ = body_read => {}
    }
    await_reply(reply, lost).await
}

/// A blocking task that ended without a reply: its panic, with the payload.
pub(crate) fn task_lost(e: tokio::task::JoinError) -> HandlerError {
    match e.try_into_panic() {
        Ok(payload) => HandlerError::panic(payload),
        Err(_) => refuse(Refusal::WorkerLost("the handler's task was cancelled")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head_with(accept_encoding: Option<&[u8]>) -> RequestHead {
        let (mut parts, ()) = Request::new(()).into_parts();
        if let Some(value) = accept_encoding {
            parts.headers.insert(
                hyper::header::ACCEPT_ENCODING,
                HeaderValue::from_bytes(value).unwrap(),
            );
        }
        RequestHead {
            parts,
            params: Params::new(),
            client_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            request_id: RequestId::mint(),
        }
    }

    #[test]
    fn accept_encoding_is_absent_not_empty() {
        assert_eq!(AcceptEncoding::of(&head_with(None)).as_str(), None);
        assert_eq!(
            AcceptEncoding::of(&head_with(Some(b"gzip, br"))).as_str(),
            Some("gzip, br")
        );
        // Not visible ASCII: nothing is known to be accepted.
        assert_eq!(
            AcceptEncoding::of(&head_with(Some(b"gzip\xff"))).as_str(),
            None
        );
    }

    #[test]
    fn a_head_response_keeps_the_length_and_drops_the_body() {
        let resp = full_body(Response::new(http_body_util::Full::new(
            Bytes::from_static(b"hello"),
        )));
        let resp = without_body(resp);
        assert_eq!(resp.headers()[CONTENT_LENGTH], "5");
        assert_eq!(resp.body().size_hint().exact(), Some(0));
    }
}
