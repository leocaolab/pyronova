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

use super::{full_body, BoxBody};
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

/// gRPC short-circuit, fast path, route resolution, then static file, fallback or 404.
pub(crate) async fn preprocess(
    req: Request<Incoming>,
    site: &Site,
) -> Result<Preprocessed, hyper::Error> {
    if crate::grpc::is_grpc_request(&req) {
        return grpc(req, site).await.map(Preprocessed::Respond);
    }
    crate::monitor::count_request();
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
    Ok(Preprocessed::Dispatch(Prepared {
        parts,
        body,
        call,
        params,
        start,
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

/// gRPC needs HTTP/2 trailers (`grpc-status`) the normal response path can't model, so it
/// goes to the hand-rolled unary dispatcher.
async fn grpc(req: Request<Incoming>, site: &Site) -> Result<Response<BoxBody>, hyper::Error> {
    let start = Instant::now();
    let path = req.uri().path().to_owned();
    let resp = crate::grpc::handle_grpc(req).await?;
    let line = RequestLine {
        method: "POST",
        path: &path,
        start,
    };
    Ok(finish(resp, site, &line, Served::Engine))
}

// ─────────────────────────── body collection ───────────────────────────

/// Why a request body was not collected.
#[derive(Debug)]
pub(crate) enum BodyReject {
    /// Over `max_body_size`.
    TooLarge,
    /// It needed an admission permit and none was free.
    Overloaded,
    /// Not complete within [`REQUEST_BUDGET`].
    TimedOut,
    /// The connection failed while reading it.
    Read(hyper::Error),
}

impl BodyReject {
    pub(crate) fn into_response(self) -> Response<BoxBody> {
        match self {
            BodyReject::TooLarge => full_body(crate::response::payload_too_large_response()),
            BodyReject::Overloaded => refuse(Refusal::Overloaded("server overloaded")),
            BodyReject::TimedOut => full_body(crate::response::request_timeout_response()),
            BodyReject::Read(e) => {
                tracing::warn!(target: "pyronova::server", error = %e, "request body read failed");
                full_body(crate::response::bad_request_response(&format!(
                    "request body read failed: {e}"
                )))
            }
        }
    }
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
pub(crate) async fn collect_body(body: Incoming, max: usize) -> Result<Bytes, BodyReject> {
    collect(body, max, None).await.map(|admitted| admitted.body)
}

/// [`collect_body`], passing the pool's admission gate.
pub(crate) async fn collect_body_with_admission(
    body: Incoming,
    max: usize,
    admission: Admission<'_>,
) -> Result<Admitted, BodyReject> {
    collect(body, max, Some(admission)).await
}

async fn collect(
    mut body: Incoming,
    max: usize,
    mut admission: Option<Admission<'_>>,
) -> Result<Admitted, BodyReject> {
    let read = async {
        let mut buf = BodyBuf::Empty;
        while let Some(frame) = body.frame().await {
            // Trailer frames carry no body bytes.
            let Ok(data) = frame.map_err(BodyReject::Read)?.into_data() else {
                continue;
            };
            if buf.len().saturating_add(data.len()) > max {
                return Err(BodyReject::TooLarge);
            }
            buf = buf.push(data);
            if let Some(gate) = admission.as_mut() {
                if gate.permit.is_none() && buf.len() as u64 > gate.skip_bytes {
                    let permit = gate
                        .semaphore
                        .clone()
                        .try_acquire_owned()
                        .map_err(|_| BodyReject::Overloaded)?;
                    gate.permit = Some(permit);
                }
            }
        }
        Ok(buf.freeze())
    };
    let body = tokio::time::timeout(REQUEST_BUDGET, read)
        .await
        .map_err(|_| BodyReject::TimedOut)??;
    Ok(Admitted {
        body,
        permit: admission.and_then(|gate| gate.permit),
    })
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

/// A request the server accepted but gave up on. Each one is counted in
/// `DROPPED_REQUESTS`, on every path.
pub(crate) enum Refusal {
    /// A queue or permit budget is full (503, retry).
    Overloaded(&'static str),
    /// The workers that would run it are gone: the server is shutting down (503).
    ShuttingDown(&'static str),
    /// The worker running it dropped its reply: it panicked or exited (500).
    WorkerLost(&'static str),
    /// The handler did not answer within [`REQUEST_BUDGET`] (504).
    TimedOut,
}

pub(crate) fn refuse(refusal: Refusal) -> Response<BoxBody> {
    crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let resp = match refusal {
        Refusal::Overloaded(why) => crate::response::overloaded_response(why),
        Refusal::ShuttingDown(why) => crate::response::unavailable_response(why),
        Refusal::WorkerLost(why) => crate::response::error_response(why),
        Refusal::TimedOut => crate::response::gateway_timeout_response(),
    };
    full_body(resp)
}

/// Waits for a handler's reply for at most [`REQUEST_BUDGET`]. A reply channel closed
/// without an answer means the worker was lost (`lost` says which).
pub(crate) async fn await_reply<T, E>(
    reply: impl std::future::Future<Output = Result<T, E>>,
    lost: &'static str,
) -> Result<T, Refusal> {
    match tokio::time::timeout(REQUEST_BUDGET, reply).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) => Err(Refusal::WorkerLost(lost)),
        Err(_) => Err(Refusal::TimedOut),
    }
}
