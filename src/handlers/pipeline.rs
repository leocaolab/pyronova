//! The request pipeline every serving path shares: preprocessing (fast path, route
//! resolution, static files, fallback, 404), body collection under one budget, and one
//! `finish` that applies CORS and writes the access-log line for every response.
//!
//! The paths (GIL, sub-interpreter pool, TPC inline + bridge) differ only in where the
//! handler runs; everything before and after that is here.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::http::request::Parts;
use hyper::{Request, Response};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::error::{HandlerError, RequestTag};
use super::{full_body, BoxBody};
use crate::request_id::RequestId;
use crate::router::{Call, Params};
use crate::site::Site;

/// How long the server waits for one step of a request — reading its body, or a handler
/// producing its response — before it gives up on it.
pub(crate) const REQUEST_BUDGET: Duration = Duration::from_secs(30);

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
    pub(crate) method: &'a str,
    pub(crate) path: &'a str,
    pub(crate) start: Instant,
}

/// Applies CORS and writes the access-log line. Every response of every path goes through
/// here exactly once.
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
    let status = resp.status();
    if site.config.access_log.samples(status) {
        tracing::info!(
            target: "pyronova::access",
            method = line.method,
            path = line.path,
            status = status.as_u16(),
            latency_us = line.start.elapsed().as_micros() as u64,
            mode = served.label(),
            "Request handled"
        );
    }
    resp
}

// ─────────────────────────── preprocessing ───────────────────────────

/// What preprocessing decided. A return value moved once per request and never stored,
/// so the size gap between the variants costs nothing; boxing `Prepared` would cost an
/// allocation on every dispatched request.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Preprocessed {
    /// Answered without a handler (gRPC, fast path, static file, 404), already finished.
    Respond(Response<BoxBody>),
    /// A handler runs it.
    Dispatch(Prepared),
}

/// A request resolved to a handler. `parts` is moved out of hyper's request, not copied:
/// the TPC inline path borrows method, path and headers from it without allocating.
pub(crate) struct Prepared {
    pub(crate) parts: Parts,
    pub(crate) body: Incoming,
    pub(crate) call: Call,
    pub(crate) params: Params,
    pub(crate) start: Instant,
    /// Written here, once; the handler's `Request`, the error log and a 5xx body read it.
    pub(crate) request_id: RequestId,
}

impl Prepared {
    pub(crate) fn query(&self) -> &str {
        self.parts.uri.query().unwrap_or("")
    }

    pub(crate) fn accept_encoding(&self) -> AcceptEncoding {
        AcceptEncoding::of(&self.parts.headers)
    }
}

/// The request's `Accept-Encoding`, kept for the response after the headers move into the
/// handler's `Request`. A refcount copy of the field, no allocation.
pub(crate) struct AcceptEncoding(Option<hyper::header::HeaderValue>);

impl AcceptEncoding {
    pub(crate) fn of(headers: &hyper::HeaderMap) -> Self {
        Self(headers.get(hyper::header::ACCEPT_ENCODING).cloned())
    }

    /// The codings the client accepts; `""` (none) when it sent no `Accept-Encoding` or
    /// one that isn't plain text.
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_ref().and_then(|v| v.to_str().ok()).unwrap_or("")
    }
}

/// gRPC benchmark method (when enabled), fast path, route resolution, then static file,
/// fallback or 404.
pub(crate) async fn preprocess(
    req: Request<Incoming>,
    site: &Site,
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
            method: req.method().as_str(),
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
                    method: parts.method.as_str(),
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
        parts,
        body,
        call,
        params,
        start,
        request_id,
    }))
}

enum Unrouted {
    Fallback(Call),
    Respond(Response<BoxBody>),
}

/// No route matched: a static file (GET/HEAD), else the fallback handler, else 404.
async fn unrouted(parts: &Parts, site: &Site) -> Unrouted {
    let method = &parts.method;
    if method == hyper::Method::GET || method == hyper::Method::HEAD {
        if let Some(resp) =
            crate::static_fs::try_static_file(parts.uri.path(), &site.routes.static_dirs).await
        {
            return Unrouted::Respond(full_body(resp));
        }
    }
    match site.routes.fallback_call() {
        Some(call) => Unrouted::Fallback(call),
        None => Unrouted::Respond(full_body(crate::response::not_found_response())),
    }
}

/// gRPC needs HTTP/2 trailers (`grpc-status`) the normal response path can't model, so the
/// benchmark method is answered outside the route table.
async fn grpc_get_sum(
    req: Request<Incoming>,
    site: &Site,
) -> Result<Response<BoxBody>, hyper::Error> {
    let start = Instant::now();
    let resp = crate::grpc::handle_get_sum(req).await?;
    let line = RequestLine {
        method: "POST",
        path: crate::grpc::GET_SUM_PATH,
        start,
    };
    Ok(finish(resp, site, &line, Served::Engine))
}

// ─────────────────────────── body collection ───────────────────────────

/// Why a request body was not read, buffered or streamed: the client's doing (a 4xx).
/// `Clone` (the read error is shared) so a streamed body's rejection can travel through the
/// handler as a Python exception and back (`python::body_stream::BodyRejected`).
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum BodyReject {
    #[error("request body is larger than max_body_size")]
    TooLarge,
    #[error("request body did not arrive within {REQUEST_BUDGET:?}")]
    TimedOut,
    #[error("request body read failed: {0}")]
    Read(#[source] Arc<hyper::Error>),
}

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

/// The whole body, at most `max` bytes, within [`REQUEST_BUDGET`].
pub(crate) async fn read_body(body: Incoming, max: usize) -> Result<Bytes, BodyReject> {
    collect(body, max, &mut Ungated).await
}

/// [`read_body`], for a handler's request.
pub(crate) async fn collect_body(body: Incoming, max: usize) -> Result<Bytes, HandlerError> {
    Ok(read_body(body, max).await?)
}

/// [`collect_body`], passing the pool's admission gate: a body that needs a permit when
/// none is free is [`HandlerError::Overloaded`].
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
pub(crate) const OVERLOADED: HandlerError = HandlerError::Overloaded("admission permits");

/// What a body must pass, besides the size cap, as it is buffered; its error type says
/// what refusing it means.
trait BodyGate {
    type Error: From<BodyReject>;
    /// Called with the bytes buffered so far, after each frame.
    fn admit(&mut self, buffered: usize) -> Result<(), Self::Error>;
}

/// No gate: only the size cap and the budget.
struct Ungated;

impl BodyGate for Ungated {
    type Error = BodyReject;
    fn admit(&mut self, _buffered: usize) -> Result<(), BodyReject> {
        Ok(())
    }
}

impl BodyGate for Admission<'_> {
    type Error = HandlerError;
    fn admit(&mut self, buffered: usize) -> Result<(), HandlerError> {
        if self.permit.is_none() && buffered as u64 > self.skip_bytes {
            let permit = self
                .semaphore
                .clone()
                .try_acquire_owned()
                .map_err(|_| OVERLOADED)?;
            self.permit = Some(permit);
        }
        Ok(())
    }
}

async fn collect<G: BodyGate>(
    mut body: Incoming,
    max: usize,
    gate: &mut G,
) -> Result<Bytes, G::Error> {
    let read = async {
        let mut buf = BodyBuf::Empty;
        while let Some(frame) = body.frame().await {
            // Trailer frames carry no body bytes.
            let Ok(data) = frame
                .map_err(|e| BodyReject::Read(Arc::new(e)))?
                .into_data()
            else {
                continue;
            };
            if buf.len().saturating_add(data.len()) > max {
                return Err(BodyReject::TooLarge.into());
            }
            buf = buf.push(data);
            gate.admit(buf.len())?;
        }
        Ok::<_, G::Error>(buf.freeze())
    };
    tokio::time::timeout(REQUEST_BUDGET, read)
        .await
        .map_err(|_| BodyReject::TimedOut)?
}

/// Body bytes as they arrive. A one-frame body (the common case) is kept as hyper handed
/// it over, with no copy.
enum BodyBuf {
    Empty,
    One(Bytes),
    Many(BytesMut),
}

impl BodyBuf {
    fn len(&self) -> usize {
        match self {
            BodyBuf::Empty => 0,
            BodyBuf::One(b) => b.len(),
            BodyBuf::Many(b) => b.len(),
        }
    }

    fn push(self, data: Bytes) -> Self {
        match self {
            BodyBuf::Empty => BodyBuf::One(data),
            BodyBuf::One(first) => {
                let mut joined = BytesMut::with_capacity(first.len() + data.len());
                joined.extend_from_slice(&first);
                joined.extend_from_slice(&data);
                BodyBuf::Many(joined)
            }
            BodyBuf::Many(mut joined) => {
                joined.extend_from_slice(&data);
                BodyBuf::Many(joined)
            }
        }
    }

    fn freeze(self) -> Bytes {
        match self {
            BodyBuf::Empty => Bytes::new(),
            BodyBuf::One(b) => b,
            BodyBuf::Many(b) => b.freeze(),
        }
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
        Err(_) => Err(HandlerError::Timeout),
    }
}

/// [`await_reply`] for a request whose body is streamed to its handler: the budget starts
/// once the body has been read (`body_read`, the feeder, ends), as a buffered request's
/// starts once its body is in. Until then the feeder's own per-step budget bounds the
/// wait, so an upload that keeps moving is not cut off, and a client that stalls is
/// answered 408 by the body's budget, not 504 by the handler's.
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
        Err(_) => HandlerError::WorkerLost("the handler's task was cancelled"),
    }
}
