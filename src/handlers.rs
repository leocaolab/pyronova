use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Response, StatusCode};
use pyo3::prelude::*;
use pyo3::types::PyString;

use crate::python::interp;
use crate::python::stream::PyronovaStream;
use crate::response::{error_response, extract_response_data, payload_too_large_response};
use crate::router::FrozenRoutes;
use crate::types::{PyronovaRequest, PyronovaResponse, ResponseData};

pub(crate) type SharedPool = Arc<interp::InterpreterPool>;

// Per-dispatch-path handlers live in submodules so each hot path has
// its own file; shared helpers (CORS, body collect, fast-response
// builder, stream response, call_handler_with_hooks) stay below.
pub(crate) mod gil;
pub(crate) mod subinterp;
pub(crate) mod tpc;
pub(crate) use gil::handle_request;
pub(crate) use subinterp::handle_request_subinterp;
pub(crate) use tpc::handle_request_tpc_inline;

/// Bounded body collection. Returns Err with a ready-made error response
/// on oversize/stream errors.
///
/// `#[inline]` on this and the other hot-path helpers (`apply_cors`,
/// `full_body`, `build_fast_response`, `build_stream_response`,
/// `max_body_size`): when these lived as private `fn` in the old
/// monolithic handlers.rs, same-module call sites auto-inlined them.
/// After the split into `handlers/{tpc,gil,subinterp}.rs` they became
/// `pub(crate) fn` — visible across submodules — and the compiler
/// became conservative about inlining them even under fat LTO,
/// emitting separate callable symbols. Explicit `#[inline]` restores
/// the pre-split behavior. Measured 2% regression on bench_inmem
/// Python w=6 without these hints.
#[inline]
pub(crate) async fn collect_body_bounded(body: Incoming) -> Result<Vec<u8>, Response<BoxBody>> {
    use http_body_util::Limited;
    let max = max_body_size();
    let limited = Limited::new(body, max);
    match limited.collect().await {
        Ok(c) => Ok(c.to_bytes().to_vec()),
        Err(e) => {
            if e.downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
            {
                Err(full_body(payload_too_large_response()))
            } else {
                Err(full_body(error_response("body read failed")))
            }
        }
    }
}

/// Default max request body size (10 MB). Configurable via `app.max_body_size`.
const DEFAULT_MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Global max body size — set once at startup, read on every request (lock-free).
static MAX_BODY_SIZE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(DEFAULT_MAX_BODY_SIZE);

pub(crate) fn set_max_body_size(size: usize) {
    MAX_BODY_SIZE.store(size, std::sync::atomic::Ordering::Relaxed);
}

/// Decide whether to emit an access-log line for this request.
///
/// Three layers (cheapest first):
///   1. logging not enabled → never.
///   2. `always_status > 0` and response status meets it → always.
///      Lets operators keep full visibility of 4xx/5xx without paying
///      the per-request log cost on every 2xx.
///   3. otherwise sample 1-in-N via the shared atomic counter on the
///      route table. `sample_n=1` short-circuits to "log all" without
///      touching the atomic.
#[inline]
pub(crate) fn should_log_request(routes: &crate::router::RouteTable, status: u16) -> bool {
    if !routes.request_logging {
        return false;
    }
    if routes.request_log_always_status > 0 && status >= routes.request_log_always_status {
        return true;
    }
    let n = routes.request_log_sample_n;
    if n <= 1 {
        return true;
    }
    routes
        .request_log_counter
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .is_multiple_of(n)
}

#[inline]
pub(crate) fn max_body_size() -> usize {
    MAX_BODY_SIZE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Result from handler: either a normal response or a stream.
pub(crate) enum HandlerResult {
    PyronovaResponse(Result<ResponseData, String>),
    PyronovaStream(StreamInfo),
}

pub(crate) struct StreamInfo {
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, std::convert::Infallible>>,
    content_type: String,
    status: u16,
    headers: HashMap<String, String>,
}

/// If `obj` is a coroutine (from `async def`), execute it via a thread-local
/// persistent asyncio event loop. Otherwise return it unchanged.
///
/// Uses thread_local to cache event loop per spawn_blocking thread —
/// avoids asyncio.run() overhead of creating/destroying loop per request.
fn resolve_coroutine(py: Python<'_>, obj: Py<PyAny>) -> Result<Py<PyAny>, String> {
    use std::cell::RefCell;

    /// RAII wrapper that calls `loop.close()` via GIL when the thread dies.
    /// Prevents FD / task leaks from orphaned asyncio event loops.
    ///
    /// Safety: guards against Py_Finalize race — if the Python interpreter
    /// is already shutting down when this thread exits, we skip the FFI call
    /// (leak the loop object) rather than segfault on invalid thread state.
    struct LoopGuard(Option<Py<PyAny>>);
    impl Drop for LoopGuard {
        fn drop(&mut self) {
            if let Some(loop_obj) = self.0.take() {
                // Check that the Python interpreter is still alive.
                // During process shutdown, Tokio's blocking thread pool may
                // tear down threads AFTER Py_Finalize has run, making
                // Python::attach() UB (use-after-free on global interpreter).
                if unsafe { pyo3::ffi::Py_IsInitialized() } != 0 {
                    Python::attach(|py| {
                        let _ = loop_obj.call_method0(py, "close");
                    });
                    // loop_obj's Py<PyAny>::Drop fires here under a live
                    // interp → benign refcount decrement.
                } else {
                    // Interp is gone. Py<PyAny>::Drop would try to
                    // Py_DECREF on a finalized interpreter → segfault.
                    // Physically leak the pointer — at this point the
                    // whole process is exiting, so the OS reclaims it.
                    std::mem::forget(loop_obj);
                }
            }
        }
    }

    thread_local! {
        static LOOP: RefCell<LoopGuard> = const { RefCell::new(LoopGuard(None)) };
    }

    let bound = obj.bind(py);
    // Awaitable detection via C-level type slot probe.
    //
    // The canonical Python way — `inspect.isawaitable(obj)` — dispatches
    // through the inspect module, costing ~μs per call (import + attr
    // lookup + method call + refcount dance). On a hot per-request
    // middleware path at 400k rps that's measurable — Pyronova v1.4.5
    // bench saw ~5% throughput loss from this single check.
    //
    // Instead, read the type's `tp_as_async->am_await` slot directly.
    // Any awaitable (native coroutine, asyncio.Task, asyncio.Future,
    // user classes implementing __await__ via PyType_FromSpec with
    // Py_am_await) has am_await populated. Cost: one pointer chase +
    // one null check. Nanoseconds, L1-resident.
    let is_awaitable = unsafe {
        let ptr = bound.as_ptr();
        if pyo3::ffi::PyCoro_CheckExact(ptr) == 1 {
            true
        } else {
            let tp = pyo3::ffi::Py_TYPE(ptr);
            if tp.is_null() {
                false
            } else {
                let async_slots = (*tp).tp_as_async;
                !async_slots.is_null() && (*async_slots).am_await.is_some()
            }
        }
    };
    if !is_awaitable {
        return Ok(obj);
    }

    LOOP.with(|tl| {
        let mut guard = tl.borrow_mut();

        if guard.0.is_none() {
            let asyncio = py
                .import("asyncio")
                .map_err(|e| format!("import asyncio: {e}"))?;
            let new_loop = asyncio
                .call_method0("new_event_loop")
                .map_err(|e| format!("new_event_loop: {e}"))?;
            let _ = asyncio.call_method1("set_event_loop", (&new_loop,));
            guard.0 = Some(new_loop.unbind());
        }

        let event_loop = guard.0.as_ref().unwrap().bind(py);
        let result = event_loop
            .call_method1("run_until_complete", (bound,))
            .map_err(|e| format!("run_until_complete error: {e}"))?;
        Ok(result.unbind())
    })
}

// ---------------------------------------------------------------------------
// Shared helpers — imported by tpc.rs / gil.rs / subinterp.rs submodules
// ---------------------------------------------------------------------------

pub(crate) type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

/// Inject CORS headers into any response (normal or error).
///
/// Per W3C CORS spec, Access-Control-Allow-Credentials and
/// Access-Control-Expose-Headers must be present on actual responses
/// (GET/POST/etc.), not only OPTIONS preflight.
#[inline]
pub(crate) fn apply_cors(resp: &mut Response<BoxBody>, cors: Option<&crate::router::CorsConfig>) {
    let Some(cfg) = cors else { return };
    let headers = resp.headers_mut();
    // insert (not append) to avoid duplicates
    if let Ok(v) = cfg.origin.parse() {
        headers.insert("access-control-allow-origin", v);
    }
    if let Ok(v) = cfg.methods.parse() {
        headers.insert("access-control-allow-methods", v);
    }
    if let Ok(v) = cfg.headers.parse() {
        headers.insert("access-control-allow-headers", v);
    }
    if cfg.allow_credentials {
        headers.insert(
            "access-control-allow-credentials",
            hyper::header::HeaderValue::from_static("true"),
        );
    }
    if let Some(expose) = cfg.expose_headers.as_ref() {
        if let Ok(v) = expose.parse() {
            headers.insert("access-control-expose-headers", v);
        }
    }
}

#[inline]
pub(crate) fn full_body(resp: Response<Full<Bytes>>) -> Response<BoxBody> {
    // `Full<Bytes>::Error` is `std::convert::Infallible` (uninhabited) — this
    // body can never yield an error. `match e {}` is the compiler-proven total
    // conversion to the boxed body's `hyper::Error`, with no runtime panic path.
    resp.map(|b| b.map_err(|e| match e {}).boxed())
}

/// Turn a sub-interpreter/TPC handler result into a hyper response.
///
/// Shared by the sub-interp-pool (`subinterp.rs`) and TPC-inline (`tpc.rs`)
/// handlers — both take a `Result<SubInterpResponse, String>` and apply the
/// *same* content-type detection, compression, status mapping, and header
/// assembly. Keeping it in one place removes the change-one-forget-the-other
/// hazard between the two hot paths. Callers still own CORS + access logging,
/// which differ per mode. `handler_name` only enriches the error log on the
/// (near-impossible) invalid-header path.
pub(crate) fn build_subinterp_http_response(
    result: Result<crate::python::interp::SubInterpResponse, String>,
    accept_encoding: &str,
    handler_name: Option<&str>,
) -> Response<BoxBody> {
    match result {
        Ok(mut resp) => {
            let ct_owned: String = resp.content_type.clone().unwrap_or_else(|| {
                if resp.is_json || resp.body.starts_with(b"{") || resp.body.starts_with(b"[") {
                    "application/json".to_string()
                } else {
                    "text/plain; charset=utf-8".to_string()
                }
            });
            let body_bytes = crate::compression::maybe_compress_subinterp(
                std::mem::take(&mut resp.body),
                &ct_owned,
                &mut resp.headers,
                accept_encoding,
            );
            let status =
                StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let mut builder = Response::builder()
                .status(status)
                .header("content-type", &ct_owned)
                .header("server", crate::response::SERVER_HEADER);
            for (k, v) in &resp.headers {
                builder = builder.header(k.as_str(), v.as_str());
            }
            match builder.body(Full::new(body_bytes)) {
                Ok(r) => full_body(r),
                Err(e) => {
                    tracing::error!(
                        target: "pyronova::handler",
                        error = %e,
                        handler = handler_name.unwrap_or("?"),
                        "handler returned invalid response headers"
                    );
                    full_body(error_response("invalid response headers"))
                }
            }
        }
        Err(e) => full_body(error_response(&e)),
    }
}

/// Build a hyper response from a `FastResponse` — used for the fast-path
/// route table (`app.add_fast_response`). No Python touched; just copies
/// the pre-built Bytes + headers into a Response<Full<Bytes>>.
#[inline]
pub(crate) fn build_fast_response(
    fr: &crate::router::FastResponse,
    cors: Option<&crate::router::CorsConfig>,
) -> Response<Full<Bytes>> {
    let status = StatusCode::from_u16(fr.status).unwrap_or(StatusCode::OK);
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", &fr.content_type)
        .header("server", crate::response::SERVER_HEADER);
    for (k, v) in &fr.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    // Inline apply_cors to avoid BoxBody alloc + rebox on the fast path.
    if let Some(cfg) = cors {
        if let Ok(v) = cfg.origin.parse::<hyper::header::HeaderValue>() {
            builder = builder.header("access-control-allow-origin", v);
        }
        if let Ok(v) = cfg.methods.parse::<hyper::header::HeaderValue>() {
            builder = builder.header("access-control-allow-methods", v);
        }
        if let Ok(v) = cfg.headers.parse::<hyper::header::HeaderValue>() {
            builder = builder.header("access-control-allow-headers", v);
        }
        if cfg.allow_credentials {
            builder = builder.header("access-control-allow-credentials", "true");
        }
        if let Some(expose) = cfg.expose_headers.as_ref() {
            if let Ok(v) = expose.parse::<hyper::header::HeaderValue>() {
                builder = builder.header("access-control-expose-headers", v);
            }
        }
    }
    builder
        .body(Full::new(fr.body.clone()))
        .unwrap_or_else(|e| {
            tracing::error!(target: "pyronova::server", error = %e, "failed to build fast response body");
            error_response("invalid fast response")
        })
}

/// Feeder task for `stream=True` routes. Reads one hyper body frame at a
/// time and pushes each data chunk into the `PyronovaBodyStream`'s mpsc channel.
/// Enforces `max_size` as a running total (defense against malicious
/// unbounded uploads) and per-frame read timeout (Slowloris defense, same
/// 30 s budget as the buffered path).
pub(crate) async fn stream_body_feeder(
    body: Incoming,
    tx: tokio::sync::mpsc::Sender<crate::python::body_stream::ChunkMsg>,
    max_size: usize,
) {
    use crate::python::body_stream::ChunkMsg;
    use hyper::body::Body;
    let mut body = body;
    let mut total: usize = 0;
    loop {
        let frame_res = match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)),
        )
        .await
        {
            Ok(Some(Ok(f))) => f,
            Ok(Some(Err(e))) => {
                let _ = tx.send(ChunkMsg::Err(format!("body read: {e}"))).await;
                return;
            }
            Ok(None) => {
                let _ = tx.send(ChunkMsg::Eof).await;
                return;
            }
            Err(_) => {
                let _ = tx
                    .send(ChunkMsg::Err("body read timeout (30s)".into()))
                    .await;
                return;
            }
        };
        if let Ok(chunk) = frame_res.into_data() {
            total = total.saturating_add(chunk.len());
            if total > max_size {
                let _ = tx
                    .send(ChunkMsg::Err(format!(
                        "body exceeds max_body_size ({max_size} bytes)"
                    )))
                    .await;
                return;
            }
            // `.send().await` propagates backpressure all the way back to
            // hyper's poll_frame: a slow Python consumer blocks the feeder,
            // which blocks the next poll, which closes the TCP receive
            // window so the client slows down on the wire. The previous
            // `std::sync::mpsc::Sender::send()` was unbounded — a fast
            // network peer could dump gigabytes into RAM before the Python
            // handler consumed the first chunk. See body_stream.rs module
            // doc for the bound (CHANNEL_CAPACITY = 8 frames in flight).
            if tx.send(ChunkMsg::Data(chunk)).await.is_err() {
                // Handler dropped the stream — no one to receive further chunks.
                return;
            }
        }
        // Trailer / metadata frames are ignored for body streaming.
    }
}

// ---------------------------------------------------------------------------
// Shared: call handler with full middleware chain (runs in blocking thread)
// ---------------------------------------------------------------------------

pub(crate) fn call_handler_with_hooks(
    routes: FrozenRoutes,
    handler_idx: usize,
    sky_req: PyronovaRequest,
) -> HandlerResult {
    use std::sync::atomic::Ordering::Relaxed;

    // Track GIL queue: +1 before acquiring, -1 after acquiring
    crate::monitor::GIL_QUEUE_LENGTH.fetch_add(1, Relaxed);

    // Passive GIL contention measurement: record the wall-clock time spent
    // waiting to acquire the GIL. This replaces the active watchdog probe —
    // measures real request latency instead of artificial contention.
    let gil_wait_start = std::time::Instant::now();

    Python::attach(|py| {
        crate::monitor::GIL_QUEUE_LENGTH.fetch_sub(1, Relaxed);
        crate::monitor::record_gil_wait(gil_wait_start.elapsed().as_micros() as u64);
        let hold_start = std::time::Instant::now();

        // FrozenRoutes: zero-cost iteration — direct Arc<RouteTable> reference.
        // No Vec collect, no clone_ref. Just borrow from the Arc.
        let before_hooks = &routes.before_hooks;
        let after_hooks = &routes.after_hooks;

        let handler = if handler_idx == usize::MAX {
            match routes.fallback_handler.as_ref() {
                Some(h) => h,
                None => {
                    return HandlerResult::PyronovaResponse(Ok(crate::types::ResponseData {
                        body: bytes::Bytes::from_static(b"{\"error\":\"Not Found\"}"),
                        content_type: "application/json".to_string(),
                        status: 404,
                        headers: Default::default(),
                    }))
                }
            }
        } else {
            // Bound-check rather than panic on out-of-range index.
            // Caller is expected to pass a valid index, but router bugs
            // or future refactors could feed garbage; pre-fix that would
            // panic the worker thread mid-request (arc handlers-3).
            // Treat as a 500 instead.
            match routes.handlers.get(handler_idx) {
                Some(h) => h,
                None => {
                    tracing::error!(
                        target: "pyronova::app",
                        handler_idx,
                        n_handlers = routes.handlers.len(),
                        "handler_idx out of range — returning 500 \
                         instead of panicking; check caller validation"
                    );
                    return HandlerResult::PyronovaResponse(Err(
                        "internal: handler index out of range".to_string(),
                    ));
                }
            }
        };

        // before_request hooks
        //
        // Drive the hook return through resolve_coroutine — an `async def`
        // middleware returns a coroutine that must be awaited, not treated
        // as a live response. Without this, `!bound.is_none()` was true
        // for the coroutine object and the framework returned
        // "<coroutine object ...>" as a 200 body.
        for hook in before_hooks {
            match hook.call1(py, (sky_req.clone(),)) {
                Ok(result) => {
                    let result = match resolve_coroutine(py, result) {
                        Ok(r) => r,
                        Err(e) => {
                            return HandlerResult::PyronovaResponse(Err(format!(
                                "before_request hook error: {e}"
                            )))
                        }
                    };
                    let bound = result.bind(py);
                    if !bound.is_none() {
                        return HandlerResult::PyronovaResponse(extract_response_data(
                            py,
                            bound.clone(),
                        ));
                    }
                }
                Err(e) => {
                    return HandlerResult::PyronovaResponse(Err(format!(
                        "before_request hook error: {e}"
                    )))
                }
            }
        }

        // Main handler
        let handler_result = match handler.call1(py, (sky_req.clone(),)) {
            Ok(obj) => {
                // If handler returned a coroutine (async def), run it via asyncio
                let obj = match resolve_coroutine(py, obj) {
                    Ok(o) => o,
                    Err(e) => return HandlerResult::PyronovaResponse(Err(e)),
                };

                // Check if handler returned a PyronovaStream (SSE)
                // Use is_instance_of — single C pointer compare, no string alloc.
                let bound = obj.bind(py);
                if bound.is_instance_of::<PyronovaStream>() {
                    let stream_ref = match bound.cast::<PyronovaStream>() {
                        Ok(s) => s.get(),
                        Err(e) => return HandlerResult::PyronovaResponse(Err(e.to_string())),
                    };
                    let rx = match stream_ref.take_rx() {
                        Some(r) => r,
                        None => {
                            return HandlerResult::PyronovaResponse(Err(
                                "PyronovaStream already consumed".to_string(),
                            ))
                        }
                    };
                    let content_type = stream_ref.content_type.clone();
                    let status = stream_ref.status_code;
                    let hdrs = stream_ref.headers.clone();

                    return HandlerResult::PyronovaStream(StreamInfo {
                        rx,
                        content_type,
                        status,
                        headers: hdrs,
                    });
                }

                let resp = (|| -> Result<ResponseData, String> {
                    let mut resp_data = extract_response_data(py, obj.bind(py).clone())?;

                    // after_request hooks
                    for hook in after_hooks {
                        let body_py: Py<PyAny> = match std::str::from_utf8(&resp_data.body) {
                            Ok(s) => PyString::new(py, s).into_any().unbind(),
                            Err(_) => pyo3::types::PyBytes::new(py, &resp_data.body)
                                .into_any()
                                .unbind(),
                        };
                        let current_resp = Py::new(
                            py,
                            PyronovaResponse {
                                body: body_py,
                                status_code: resp_data.status,
                                content_type: Some(resp_data.content_type.clone()),
                                headers: resp_data.headers.clone(),
                            },
                        )
                        .map_err(|e| format!("failed to create PyronovaResponse: {e}"))?;
                        match hook.call1(py, (sky_req.clone(), current_resp)) {
                            Ok(result) => {
                                // Resolve awaitables from async after_request
                                // hooks — same reasoning as before_hooks.
                                let result = resolve_coroutine(py, result)
                                    .map_err(|e| format!("after_request hook error: {e}"))?;
                                let bound = result.bind(py);
                                if !bound.is_none() {
                                    resp_data = extract_response_data(py, bound.clone())?;
                                }
                            }
                            Err(e) => return Err(format!("after_request hook error: {e}")),
                        }
                    }

                    Ok(resp_data)
                })();
                HandlerResult::PyronovaResponse(resp)
            }
            Err(e) => {
                // Log the full PyErr (includes traceback via PyErr_Print
                // routed through the Python logging bridge) server-side.
                // Previously `{e}` forwarded a one-line repr back to the
                // client with zero operator-visible traceback AND leaked
                // internal paths to the caller. Keep the client response
                // generic; the real diagnostic lives in pyronova::server logs.
                e.display(py);
                tracing::error!(
                    target: "pyronova::server",
                    error = %e,
                    "handler raised an exception",
                );
                HandlerResult::PyronovaResponse(Err("handler error".to_string()))
            }
        };

        // Record GIL hold time before releasing GIL
        crate::monitor::GIL_HOLD_MAX_US.fetch_max(hold_start.elapsed().as_micros() as u64, Relaxed);

        handler_result
    })
}

/// Build a streaming SSE response from a channel receiver.
#[inline]
pub(crate) fn build_stream_response(info: StreamInfo) -> Response<BoxBody> {
    use tokio_stream::StreamExt;

    let stream =
        tokio_stream::wrappers::ReceiverStream::new(info.rx).map(|result| result.map(Frame::data));

    let body = StreamBody::new(stream);
    // The SSE channel item type is `Result<Bytes, std::convert::Infallible>`
    // (see `StreamInfo.rx`), so this body's error is uninhabited and can never
    // be produced. `match e {}` is the total, panic-free conversion to the
    // boxed body's `hyper::Error`.
    let boxed: BoxBody = BoxBody::new(body.map_err(|e| match e {}));

    let status = StatusCode::from_u16(info.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", &info.content_type)
        .header("cache-control", "no-cache")
        .header("connection", "keep-alive")
        .header("server", crate::response::SERVER_HEADER);
    for (k, v) in &info.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    builder.body(boxed).unwrap_or_else(|e| {
        tracing::error!(target: "pyronova::handler", error = %e, "stream handler returned invalid response headers");
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header("server", crate::response::SERVER_HEADER)
            .body(BoxBody::default())
            .expect("static 500 response is always valid")
    })
}

// ─────────────────────── shared request preprocessing ───────────────────

/// Outcome of [`preprocess_request`].
pub(crate) enum Preprocessed {
    /// A short-circuit response was produced (gRPC reply, fast-path hit,
    /// static-file hit, or 404). Return it to the client directly.
    Respond(Response<BoxBody>),
    /// The request matched a route (or the fallback handler). Carries the
    /// prepared request state for the caller's mode-specific dispatch.
    Dispatch(Prepared),
}

/// Request state common to every dispatch mode, produced once by
/// [`preprocess_request`] and consumed by each handler's dispatch logic.
pub(crate) struct Prepared {
    pub method: Arc<str>,
    pub path: Arc<str>,
    pub query: String,
    pub raw_headers: hyper::HeaderMap,
    pub accept_encoding: String,
    pub body: Incoming,
    pub handler_idx: usize,
    pub params: Vec<(String, String)>,
    pub start: std::time::Instant,
}

/// Run the request-preprocessing pipeline shared by every dispatch mode:
/// gRPC short-circuit, request counting, fast-path lookup, method/path/
/// query + header extraction, body hand-off, route lookup, and the
/// static-file / 404 fallback.
///
/// The pieces that differ between modes are passed in: `cors` and
/// `static_dirs` come from the active source (`FrozenRoutes` for GIL/TPC,
/// the `InterpreterPool` for sub-interp), and `lookup` performs the
/// source-specific route match. `routes` always supplies the fast-path
/// table and the fallback flag (identical across modes).
///
/// Returns [`Preprocessed::Respond`] for any short-circuit, otherwise
/// [`Preprocessed::Dispatch`] with the prepared request state. This is
/// the single source of truth for preprocessing — previously the gRPC /
/// fast-path / static-file / 404 / CORS logic was copy-pasted across the
/// GIL and sub-interp handlers (the "Same CORS fix as handle_request"
/// comments being a live example of change-one-forget-the-other drift).
pub(crate) async fn preprocess_request(
    req: hyper::Request<Incoming>,
    routes: &FrozenRoutes,
    cors: Option<&crate::router::CorsConfig>,
    static_dirs: &[(String, String)],
    lookup: impl FnOnce(&str, &str) -> Option<(usize, Vec<(String, String)>)>,
) -> Result<Preprocessed, hyper::Error> {
    // gRPC short-circuit: gRPC needs HTTP/2 trailers (`grpc-status`) the
    // normal Response<Full<Bytes>> path can't model, so it goes to the
    // hand-rolled unary dispatcher.
    if crate::grpc::is_grpc_request(&req) {
        return Ok(Preprocessed::Respond(crate::grpc::handle_grpc(req).await?));
    }
    crate::monitor::count_request();
    let start = std::time::Instant::now();

    // Fast-path: zero-alloc lookup, borrowing method/path from hyper.
    // The fast-path table and its CORS source are always `routes`.
    if !routes.fast_responses.is_empty() {
        if let Some(fr) = routes
            .fast_responses
            .get(req.method().as_str())
            .and_then(|m| m.get(req.uri().path()))
        {
            return Ok(Preprocessed::Respond(full_body(build_fast_response(
                fr,
                routes.cors_config.as_ref(),
            ))));
        }
    }

    let method: Arc<str> = Arc::from(req.method().as_str());
    let uri = req.uri().clone();
    let path: Arc<str> = Arc::from(uri.path());
    let query = uri.query().unwrap_or("").to_string();

    // Lazy headers: keep the raw HeaderMap, convert only on access.
    let raw_headers = req.headers().clone();
    // Capture Accept-Encoding before raw_headers is moved downstream.
    let accept_encoding = raw_headers
        .get(hyper::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // Take ownership of the body before routing; the caller decides
    // whether to collect it (buffered) or hand it to a feeder (streaming).
    let body = req.into_body();

    let lookup_result = lookup(&method, &path);

    if lookup_result.is_none() && (method.as_ref() == "GET" || method.as_ref() == "HEAD") {
        if let Some(resp) = crate::static_fs::try_static_file(&path, static_dirs).await {
            // Static-file hit must still carry CORS — otherwise CORS-
            // configured apps serve assets without allow-origin headers.
            let mut r = full_body(resp);
            apply_cors(&mut r, cors);
            return Ok(Preprocessed::Respond(r));
        }
    }

    let (handler_idx, params) = match lookup_result {
        Some(v) => v,
        None if routes.fallback_handler.is_some() => (usize::MAX, Vec::new()),
        // 404 must still carry CORS so a browser OPTIONS preflight against
        // an unknown path doesn't surface as an opaque CORS failure.
        None => {
            let mut r = full_body(crate::response::not_found_response());
            apply_cors(&mut r, cors);
            return Ok(Preprocessed::Respond(r));
        }
    };

    Ok(Preprocessed::Dispatch(Prepared {
        method,
        path,
        query,
        raw_headers,
        accept_encoding,
        body,
        handler_idx,
        params,
        start,
    }))
}
