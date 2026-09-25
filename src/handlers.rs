use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{HeaderValue, CACHE_CONTROL, CONNECTION, CONTENT_TYPE, SERVER};
use hyper::Response;
use pyo3::prelude::*;

use crate::python::interp;
use crate::python::stream::PyronovaStream;
use crate::response::{extract_response_data, status_or_500};
use crate::router::Target;
use crate::site::Site;
use crate::types::{PyronovaRequest, ResponseData, ResponseHeaders};

pub(crate) type SharedPool = Arc<interp::InterpreterPool>;

// Per-dispatch-path handlers live in submodules; the pipeline every path shares
// (preprocess, collect_body, finish) is `pipeline`; helpers used by several paths stay
// below.
pub(crate) mod gil;
pub(crate) mod pipeline;
pub(crate) mod subinterp;
pub(crate) mod tpc;
pub(crate) use gil::handle_request;
pub(crate) use subinterp::handle_request_subinterp;
pub(crate) use tpc::handle_request_tpc_inline;

/// Default max request body size (10 MB). Configurable via `app.max_body_size`.
const DEFAULT_MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Global max body size — set once at startup, read on every request (lock-free).
static MAX_BODY_SIZE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(DEFAULT_MAX_BODY_SIZE);

pub(crate) fn set_max_body_size(size: usize) {
    MAX_BODY_SIZE.store(size, std::sync::atomic::Ordering::Relaxed);
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
    content_type: HeaderValue,
    status: u16,
    headers: ResponseHeaders,
}

/// A thread's persistent asyncio event loop, and the interpreter it was created in.
///
/// Calls `loop.close()` when the thread dies, so orphaned loops don't leak FDs or tasks.
/// The close re-attaches to the loop's own interpreter explicitly: the thread may have no
/// thread state by then, and once several interpreters have executed the engine a bare
/// `Python::attach` there is refused (Layer 2, C4).
///
/// Safety: guards against the Py_Finalize race — if the interpreter is already finalized
/// when the thread exits, the loop is leaked instead of touched.
struct LoopGuard(Option<(Py<PyAny>, crate::run_context::Interp)>);

impl Drop for LoopGuard {
    /// [arc:intentional-handle] reason: we are in Drop with no error
    /// channel to propagate to and no caller to defer to. A failed
    /// close() can still leak FDs/tasks, so rather than silently dropping
    /// the error we log it (with the error object) so the leak is
    /// diagnosable.
    fn drop(&mut self) {
        if let Some((loop_obj, interp)) = self.0.take() {
            // During process shutdown, Tokio's blocking thread pool may tear down
            // threads after Py_Finalize has run; attaching then is a use-after-free.
            if unsafe { pyo3::ffi::Py_IsInitialized() } != 0 {
                crate::run_context::attach_to(interp, move |py| close_loop(py, loop_obj));
            } else {
                // Interp is gone. Py<PyAny>::Drop would Py_DECREF on a finalized
                // interpreter → segfault. The process is exiting; leak it.
                std::mem::forget(loop_obj);
            }
        }
    }
}

fn close_loop(py: Python<'_>, loop_obj: Py<PyAny>) {
    if let Err(e) = loop_obj.call_method0(py, "close") {
        tracing::warn!(
            target: "pyronova::server",
            error = %e,
            "event loop close() failed; loop FDs/tasks may have leaked"
        );
    }
    // loop_obj drops here, attached.
}

thread_local! {
    static LOOP: std::cell::RefCell<LoopGuard> =
        const { std::cell::RefCell::new(LoopGuard(None)) };
}

/// Close this thread's event loop now, while attached, instead of at thread exit. For
/// long-lived main-side threads that must drop every `Py<T>` they own before releasing
/// their thread state.
pub(crate) fn close_thread_event_loop(py: Python<'_>) {
    let taken = LOOP.with(|tl| tl.borrow_mut().0.take());
    if let Some((loop_obj, _interp)) = taken {
        close_loop(py, loop_obj);
    }
}

/// If `obj` is a coroutine (from `async def`), execute it via a thread-local
/// persistent asyncio event loop. Otherwise return it unchanged.
///
/// Uses thread_local to cache event loop per spawn_blocking thread —
/// avoids asyncio.run() overhead of creating/destroying loop per request.
fn resolve_coroutine(py: Python<'_>, obj: Py<PyAny>) -> Result<Py<PyAny>, String> {
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
            asyncio
                .call_method1("set_event_loop", (&new_loop,))
                .map_err(|e| format!("set_event_loop: {e}"))?;
            guard.0 = Some((new_loop.unbind(), crate::run_context::Interp::current(py)));
        }

        let (loop_obj, loop_interp) = guard.0.as_ref().unwrap();
        // R-4: the loop belongs to the interpreter that created it. Only main-side threads
        // reach this path; one arriving from another interpreter is a bug.
        debug_assert_eq!(
            loop_interp.id(),
            crate::run_context::Interp::current(py).id(),
            "thread-local event loop reused from a different interpreter"
        );
        let event_loop = loop_obj.bind(py);
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

#[inline]
pub(crate) fn full_body(resp: Response<Full<Bytes>>) -> Response<BoxBody> {
    // `Full<Bytes>::Error` is `std::convert::Infallible` (uninhabited) — this
    // body can never yield an error. `match e {}` is the compiler-proven total
    // conversion to the boxed body's `hyper::Error`, with no runtime panic path.
    resp.map(|b| b.map_err(|e| match e {}).boxed())
}

/// A handler's result as a hyper response, compressed if the client accepts it. Every
/// interpreter's handlers (main, pool workers, TPC inline) end here.
pub(crate) fn http_response(
    mut result: Result<ResponseData, String>,
    accept_encoding: &str,
) -> Response<BoxBody> {
    if let Ok(data) = result.as_mut() {
        crate::compression::maybe_compress(data, accept_encoding);
    }
    full_body(crate::response::build_response(result))
}

/// A main-interpreter handler result (GIL mode, the pool's `gil=True` routes, the TPC
/// bridge) as a hyper response: a buffered response, or an SSE stream.
pub(crate) fn build_main_http_response(
    result: HandlerResult,
    accept_encoding: &str,
) -> Response<BoxBody> {
    match result {
        HandlerResult::PyronovaResponse(result) => http_response(result, accept_encoding),
        HandlerResult::PyronovaStream(info) => build_stream_response(info),
    }
}

/// Feeder task for `stream=True` routes. Reads one hyper body frame at a
/// time and pushes each data chunk into the `PyronovaBodyStream`'s mpsc channel.
/// Enforces `max_size` as a running total (defense against malicious
/// unbounded uploads) and a per-frame read deadline of [`pipeline::REQUEST_BUDGET`]
/// (Slowloris defense, same budget as the buffered path).
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
            pipeline::REQUEST_BUDGET,
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
                    .send(ChunkMsg::Err(format!(
                        "body read timeout ({:?})",
                        pipeline::REQUEST_BUDGET
                    )))
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
            // window so the client slows down on the wire. See body_stream.rs
            // module doc for the bound (CHANNEL_CAPACITY = 8 frames in flight).
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

/// Runs `target`'s handler on the main interpreter with the before/after hooks, all in one
/// fresh `contextvars.Context` (see `python::request_context`).
pub(crate) fn call_handler_with_hooks(
    site: &Site,
    target: Target,
    sky_req: PyronovaRequest,
) -> HandlerResult {
    use std::sync::atomic::Ordering::Relaxed;

    // Track GIL queue: +1 before acquiring, -1 after acquiring
    crate::monitor::GIL_QUEUE_LENGTH.fetch_add(1, Relaxed);

    // Passive GIL contention measurement: record the wall-clock time spent
    // waiting to acquire the GIL. This replaces the active watchdog probe —
    // measures real request latency instead of artificial contention.
    let gil_wait_start = std::time::Instant::now();

    crate::run_context::main_attach(|py| {
        crate::monitor::GIL_QUEUE_LENGTH.fetch_sub(1, Relaxed);
        crate::monitor::record_gil_wait(gil_wait_start.elapsed().as_micros() as u64);
        let hold_start = std::time::Instant::now();

        let result = crate::python::request_context::in_request_context(py, || {
            run_with_hooks(py, site, target, sky_req)
        })
        .unwrap_or_else(|e| {
            HandlerResult::PyronovaResponse(Err(format!(
                "could not enter the request's contextvars.Context: {e}"
            )))
        });

        // Record GIL hold time before releasing GIL
        crate::monitor::GIL_HOLD_MAX_US.fetch_max(hold_start.elapsed().as_micros() as u64, Relaxed);
        result
    })
}

fn run_with_hooks(
    py: Python<'_>,
    site: &Site,
    target: Target,
    sky_req: PyronovaRequest,
) -> HandlerResult {
    let routes = &site.routes;
    // One `Request` object for the hooks and the handler.
    let req = match Py::new(py, sky_req) {
        Ok(r) => r,
        Err(e) => {
            return HandlerResult::PyronovaResponse(Err(format!("failed to create Request: {e}")))
        }
    };

    match run_before_hooks(py, &routes.before_hooks, &req) {
        Ok(None) => {}
        Ok(Some(short_circuit)) => return HandlerResult::PyronovaResponse(Ok(short_circuit)),
        Err(e) => return HandlerResult::PyronovaResponse(Err(e)),
    }

    let obj = match routes.handler(target).call1(py, (req.clone_ref(py),)) {
        Ok(obj) => obj,
        Err(e) => {
            // Log the full PyErr (traceback via the Python logging bridge) server-side;
            // the client gets a generic 500.
            e.display(py);
            tracing::error!(
                target: "pyronova::server",
                error = %e,
                "handler raised an exception",
            );
            return HandlerResult::PyronovaResponse(Err("handler error".to_string()));
        }
    };
    // If handler returned a coroutine (async def), run it via asyncio
    let obj = match resolve_coroutine(py, obj) {
        Ok(o) => o,
        Err(e) => return HandlerResult::PyronovaResponse(Err(e)),
    };

    // A `Stream` (SSE) goes out as a streaming body. `is_instance_of` is a single C
    // pointer compare, no string alloc.
    let bound = obj.bind(py);
    if bound.is_instance_of::<PyronovaStream>() {
        return stream_result(bound);
    }

    let resp = extract_response_data(py, bound.clone())
        .and_then(|data| run_after_hooks(py, &routes.after_hooks, &req, data));
    HandlerResult::PyronovaResponse(resp)
}

fn stream_result(bound: &Bound<'_, PyAny>) -> HandlerResult {
    let stream_ref = match bound.cast::<PyronovaStream>() {
        Ok(s) => s.get(),
        Err(e) => return HandlerResult::PyronovaResponse(Err(e.to_string())),
    };
    let Some(rx) = stream_ref.take_rx() else {
        return HandlerResult::PyronovaResponse(Err("PyronovaStream already consumed".to_string()));
    };
    HandlerResult::PyronovaStream(StreamInfo {
        rx,
        content_type: stream_ref.content_type.clone(),
        status: stream_ref.status_code,
        headers: stream_ref.headers.clone(),
    })
}

/// The before-request hooks, in order, until one returns a response: `Ok(Some(resp))`
/// short-circuits the request with it. An `async def` hook's coroutine is awaited, not
/// taken for a response.
pub(crate) fn run_before_hooks(
    py: Python<'_>,
    hooks: &[Py<PyAny>],
    req: &Py<PyronovaRequest>,
) -> Result<Option<ResponseData>, String> {
    for hook in hooks {
        let result = hook
            .call1(py, (req.clone_ref(py),))
            .map_err(|e| format!("before_request hook error: {e}"))?;
        let result =
            resolve_coroutine(py, result).map_err(|e| format!("before_request hook error: {e}"))?;
        let bound = result.bind(py);
        if !bound.is_none() {
            return extract_response_data(py, bound.clone()).map(Some);
        }
    }
    Ok(None)
}

/// The after-request hooks, in order: each gets the response so far and may replace it.
/// One that raises fails the request (500), on every interpreter.
fn run_after_hooks(
    py: Python<'_>,
    hooks: &[Py<PyAny>],
    req: &Py<PyronovaRequest>,
    mut resp_data: ResponseData,
) -> Result<ResponseData, String> {
    for hook in hooks {
        let current_resp = resp_data
            .to_py(py)
            .map_err(|e| format!("failed to create Response: {e}"))?;
        let result = hook
            .call1(py, (req.clone_ref(py), current_resp))
            .map_err(|e| format!("after_request hook error: {e}"))?;
        let result =
            resolve_coroutine(py, result).map_err(|e| format!("after_request hook error: {e}"))?;
        let bound = result.bind(py);
        if !bound.is_none() {
            resp_data = extract_response_data(py, bound.clone())?;
        }
    }
    Ok(resp_data)
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

    // The stream's headers were validated when it was made; its own values replace the
    // defaults.
    let mut headers = info.headers.into_map();
    headers.entry(CONTENT_TYPE).or_insert(info.content_type);
    headers
        .entry(CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("no-cache"));
    headers
        .entry(CONNECTION)
        .or_insert(HeaderValue::from_static("keep-alive"));
    headers
        .entry(SERVER)
        .or_insert(HeaderValue::from_static(crate::response::SERVER_HEADER));
    let mut resp = Response::new(boxed);
    *resp.status_mut() = status_or_500(info.status);
    *resp.headers_mut() = headers;
    resp
}
