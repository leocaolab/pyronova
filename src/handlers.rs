use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper::header::{HeaderValue, CACHE_CONTROL, CONNECTION, CONTENT_TYPE, SERVER};
use hyper::Response;
use pyo3::prelude::*;

use crate::body::{full_body, BoxBody};
use crate::error::{catch_panic, HandlerError, Logged, ResponseError, Stage};
use crate::python::interp;
use crate::python::request_context::{in_request_context, Awaitable, RequestContext};
use crate::python::stream::PyronovaStream;
use crate::response::extract_response_data;
use crate::router::Target;
use crate::site::Site;
use crate::types::{PyronovaRequest, ResponseData, ResponseHeaders};

use pipeline::AcceptEncoding;

pub(crate) type SharedPool = Arc<interp::InterpreterPool>;

// Per-dispatch-path handlers live in submodules; the pipeline every path shares
// (preprocess, collect_body, finish) is `pipeline`; helpers used by several paths stay
// below.
pub(crate) mod error;
pub(crate) mod gil;
pub(crate) mod pipeline;
pub(crate) mod subinterp;
pub(crate) mod tpc;
pub(crate) use gil::handle_request;
pub(crate) use subinterp::handle_request_subinterp;
pub(crate) use tpc::handle_request_tpc_inline;

/// What a main-interpreter handler answered: a buffered response, or a stream (SSE).
pub(crate) enum MainReply {
    Response(ResponseData),
    Stream(StreamInfo),
}

pub(crate) struct StreamInfo {
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, std::convert::Infallible>>,
    content_type: HeaderValue,
    status: hyper::StatusCode,
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

/// Runs what a hook or handler returned to a value: an awaitable is driven to completion
/// on this thread's persistent asyncio event loop (cached per thread, so no loop is made
/// and torn down per request), in the request's own context (`rc`) unless it is a
/// `Future`, which runs in its own. A plain value is returned unchanged.
///
/// An exception the awaitable raises is `stage`'s; one creating the loop is setup's.
fn resolve_coroutine(
    py: Python<'_>,
    rc: &RequestContext<'_>,
    obj: Py<PyAny>,
    stage: Stage,
) -> Result<Py<PyAny>, HandlerError> {
    let bound = obj.bind(py);
    let Some(awaitable) = Awaitable::of(bound) else {
        return Ok(obj);
    };

    LOOP.with(|tl| {
        let mut guard = tl.borrow_mut();

        let (loop_obj, loop_interp) = match &mut guard.0 {
            Some(existing) => &*existing,
            empty @ None => {
                let new_loop =
                    new_event_loop(py).map_err(|e| HandlerError::python(py, Stage::Setup, &e))?;
                &*empty.insert((new_loop.unbind(), crate::run_context::Interp::current(py)))
            }
        };
        // The loop belongs to the interpreter that created it. Only main-side threads reach
        // this path; running it from another interpreter would mix two interpreters'
        // objects, so the request fails instead.
        let current = crate::run_context::Interp::current(py);
        if loop_interp.id() != current.id() {
            return Err(HandlerError::ForeignEventLoop {
                loop_interp: loop_interp.id(),
                current: current.id(),
            });
        }
        let event_loop = loop_obj.bind(py);
        let result = match awaitable {
            Awaitable::InTask => rc.run_in_task(event_loop, bound),
            Awaitable::Future => event_loop.call_method1("run_until_complete", (bound,)),
        };
        Ok(result
            .map_err(|e| HandlerError::python(py, stage, &e))?
            .unbind())
    })
}

/// A new asyncio event loop, set as this thread's current one.
fn new_event_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let asyncio = py.import("asyncio")?;
    let new_loop = asyncio.call_method0("new_event_loop")?;
    asyncio.call_method1("set_event_loop", (&new_loop,))?;
    Ok(new_loop)
}

// ---------------------------------------------------------------------------
// Shared helpers — imported by tpc.rs / gil.rs / subinterp.rs submodules
// ---------------------------------------------------------------------------

/// A handler's result as a hyper response, compressed as the app's `compression` settings
/// allow if the client accepts it. Every interpreter's handlers (main, pool workers, TPC
/// inline) end here.
pub(crate) async fn http_response(
    result: Result<ResponseData, Logged>,
    accept_encoding: &AcceptEncoding,
    compression: Option<&crate::compression::Settings>,
) -> Response<BoxBody> {
    let resp = match result {
        Ok(data) => {
            let data = match accept_encoding.as_str() {
                Some(accepted) => crate::compression::compress(data, accepted, compression).await,
                None => data,
            };
            crate::response::build_response(data)
        }
        Err(logged) => logged.into_response(),
    };
    full_body(resp)
}

/// A main-interpreter handler result (GIL mode, the pool's `gil=True` routes, the TPC
/// bridge) as a hyper response: a buffered response, or an SSE stream.
pub(crate) async fn build_main_http_response(
    result: Result<MainReply, Logged>,
    accept_encoding: &AcceptEncoding,
    compression: Option<&crate::compression::Settings>,
) -> Response<BoxBody> {
    match result {
        Ok(MainReply::Response(data)) => {
            http_response(Ok(data), accept_encoding, compression).await
        }
        Ok(MainReply::Stream(info)) => build_stream_response(info),
        Err(logged) => http_response(Err(logged), accept_encoding, compression).await,
    }
}

// ---------------------------------------------------------------------------
// Shared: call handler with full middleware chain (runs in blocking thread)
// ---------------------------------------------------------------------------

/// Runs `target`'s handler on the main interpreter with the before/after hooks, all in one
/// fresh `contextvars.Context` (see `python::request_context`). A failure is logged here,
/// on the thread that ran it; a panic is such a failure, so the thread (a bridge worker,
/// a blocking-pool thread) survives it.
pub(crate) fn call_handler_with_hooks(
    site: &Site,
    target: Target,
    sky_req: PyronovaRequest,
) -> Result<MainReply, Logged> {
    use std::sync::atomic::Ordering::Relaxed;

    // Track GIL queue: +1 before acquiring, -1 after acquiring
    crate::monitor::GIL_QUEUE_LENGTH.fetch_add(1, Relaxed);

    // Passive GIL contention measurement: record the wall-clock time spent
    // waiting to acquire the GIL. This replaces the active watchdog probe —
    // measures real request latency instead of artificial contention.
    let gil_wait_start = std::time::Instant::now();

    // The request as its error log line names it; `sky_req` moves into its `Request`.
    let label = sky_req.label();

    crate::run_context::main_attach(|py| {
        crate::monitor::GIL_QUEUE_LENGTH.fetch_sub(1, Relaxed);
        crate::monitor::record_gil_wait(gil_wait_start.elapsed().as_micros() as u64);
        let hold_start = std::time::Instant::now();

        let result = catch_panic(|| {
            in_request_context(py, |rc| run_with_hooks(py, rc, site, target, sky_req))
                .unwrap_or_else(|e| Err(HandlerError::python(py, Stage::Setup, &e)))
        });

        // Record GIL hold time before releasing GIL
        crate::monitor::GIL_HOLD_MAX_US.fetch_max(hold_start.elapsed().as_micros() as u64, Relaxed);
        result.map_err(|e| e.log(&label.tag()))
    })
}

fn run_with_hooks(
    py: Python<'_>,
    rc: &RequestContext<'_>,
    site: &Site,
    target: Target,
    sky_req: PyronovaRequest,
) -> Result<MainReply, HandlerError> {
    let routes = &site.routes;
    // One `Request` object for the hooks and the handler.
    let req = Py::new(py, sky_req).map_err(|e| HandlerError::python(py, Stage::Setup, &e))?;

    if let Some(short_circuit) = run_before_hooks(py, rc, &routes.before_hooks, &req)? {
        return Ok(MainReply::Response(short_circuit));
    }

    let obj = routes
        .handler(target)
        .call1(py, (req.clone_ref(py),))
        .map_err(|e| HandlerError::python(py, Stage::Handler, &e))?;
    // If handler returned a coroutine (async def), run it via asyncio
    let obj = resolve_coroutine(py, rc, obj, Stage::Handler)?;

    // A `Stream` (SSE) goes out as a streaming body. `is_instance_of` is a single C
    // pointer compare, no string alloc.
    let bound = obj.bind(py);
    if bound.is_instance_of::<PyronovaStream>() {
        return Ok(MainReply::Stream(stream_info(bound)?));
    }

    let data = extract_response_data(py, bound.clone())?;
    let data = run_after_hooks(py, rc, &routes.after_hooks, &req, data)?;
    Ok(MainReply::Response(data))
}

/// A `Stream` a handler returned, taken for sending. `bound` is a `Stream` instance.
fn stream_info(bound: &Bound<'_, PyAny>) -> Result<StreamInfo, ResponseError> {
    let stream = bound
        .cast::<PyronovaStream>()
        .map_err(|_| ResponseError::StreamConsumed)?
        .get();
    let status = crate::response::http_status(stream.status_code)?;
    let rx = stream.take_rx().ok_or(ResponseError::StreamConsumed)?;
    Ok(StreamInfo {
        rx,
        content_type: stream.content_type.clone(),
        status,
        headers: stream.headers.clone(),
    })
}

/// The before-request hooks, in order, until one returns a response: `Ok(Some(resp))`
/// short-circuits the request with it. An `async def` hook's coroutine is awaited, not
/// taken for a response.
pub(crate) fn run_before_hooks(
    py: Python<'_>,
    rc: &RequestContext<'_>,
    hooks: &[Py<PyAny>],
    req: &Py<PyronovaRequest>,
) -> Result<Option<ResponseData>, HandlerError> {
    for hook in hooks {
        let result = hook
            .call1(py, (req.clone_ref(py),))
            .map_err(|e| HandlerError::python(py, Stage::BeforeHook, &e))?;
        let result = resolve_coroutine(py, rc, result, Stage::BeforeHook)?;
        let bound = result.bind(py);
        if !bound.is_none() {
            return Ok(Some(extract_response_data(py, bound.clone())?));
        }
    }
    Ok(None)
}

/// The after-request hooks, in order: each gets the response so far and may replace it.
/// One that raises fails the request (500), on every interpreter.
fn run_after_hooks(
    py: Python<'_>,
    rc: &RequestContext<'_>,
    hooks: &[Py<PyAny>],
    req: &Py<PyronovaRequest>,
    mut resp_data: ResponseData,
) -> Result<ResponseData, HandlerError> {
    for hook in hooks {
        let current_resp = resp_data.to_py(py)?;
        let result = hook
            .call1(py, (req.clone_ref(py), current_resp))
            .map_err(|e| HandlerError::python(py, Stage::AfterHook, &e))?;
        let result = resolve_coroutine(py, rc, result, Stage::AfterHook)?;
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
    *resp.status_mut() = info.status;
    *resp.headers_mut() = headers;
    resp
}
