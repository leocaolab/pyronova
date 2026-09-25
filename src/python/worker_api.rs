//! The engine functions a sub-interpreter worker's async engine (`_async_engine.py`) calls
//! (Layer 2, C5): pull the next request, answer it (a response, an exception, a timeout),
//! turn a handler's return value into a `Response`, and get the worker app's handlers and
//! hooks.
//!
//! They replace the `extern "C"` functions that used to be injected into worker globals.
//! PyO3 does the argument parsing; a Rust panic becomes a `RuntimeError` (not PyO3's
//! `PanicException`, a `BaseException` that the engine's fetcher thread, which catches
//! `Exception`, would die on).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use pyo3::exceptions::{PyBaseException, PyRuntimeError};
use pyo3::prelude::*;

use super::ffi::{get_worker_state, Pending, PyObjRef};
use super::worker::worker_response;
use crate::handlers::error::{panic_message, HandlerError, PyException, RequestTag, Stage};
use crate::types::ResponseData;

/// Runs `f`, turning a Rust panic into a `RuntimeError` naming `context`.
fn no_panic<T>(context: &'static str, f: impl FnOnce() -> PyResult<T>) -> PyResult<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let msg = panic_message(&*payload);
            tracing::error!(target: "pyronova::server", context, panic = %msg, "Rust panic in a worker API call");
            Err(PyRuntimeError::new_err(format!(
                "Rust panic in {context}: {msg}"
            )))
        }
    }
}

/// The next request for async worker `worker_id`: `(req_id, handler_idx, request)`, or
/// `None` once its pool is gone (channel closed, or a newer pool replaced its slot).
/// Waits with the GIL released.
#[pyfunction]
pub(crate) fn _worker_recv(
    py: Python<'_>,
    worker_id: usize,
    pool_id: u64,
) -> PyResult<Option<(u64, usize, Py<crate::types::PyronovaRequest>)>> {
    no_panic("_worker_recv", || {
        // `pool_id` is the worker's birth certificate: a zombie from an earlier pool whose
        // slot has been replaced sees `None` and exits instead of stealing requests.
        let state = match get_worker_state(worker_id) {
            Some(s) if s.pool_id == pool_id => s,
            _ => return Ok(None),
        };
        let wait = Arc::clone(&state);
        let Some(req) = py.detach(move || wait.rx.recv().ok()) else {
            return Ok(None);
        };
        let req_id = state.next_req_id.fetch_add(1, Ordering::Relaxed);
        let (route, request, reply) = req.into_request();
        let pending = Pending {
            reply,
            request_id: request.request_id.clone(),
            method: Arc::clone(&request.method),
            path: Arc::clone(&request.path),
        };
        let request = Py::new(py, request)?;
        // Only now that nothing can fail: a send dropped before this point reaches the
        // waiting caller as an error instead of an orphaned map entry.
        state
            .response_map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(req_id, pending);
        Ok(Some((req_id, route.index(), request)))
    })
}

/// Sends `response` (anything a handler may return; the async engine passes a `Response`)
/// back for request `req_id` of async worker `worker_id`.
#[pyfunction]
pub(crate) fn _worker_send(
    py: Python<'_>,
    worker_id: usize,
    pool_id: u64,
    req_id: u64,
    response: Bound<'_, PyAny>,
) -> PyResult<()> {
    no_panic("_worker_send", || {
        // SAFETY: attached to `response`'s interpreter (this is a pymethod call).
        let parsed = unsafe {
            worker_response(
                py,
                PyObjRef::from_borrowed(response.as_ptr())
                    .ok_or_else(|| PyRuntimeError::new_err("_worker_send: null response"))?,
            )
        };
        answer(
            worker_id,
            pool_id,
            req_id,
            parsed.map_err(HandlerError::from),
        );
        Ok(())
    })
}

/// Answers request `req_id` of async worker `worker_id` with the exception its task raised
/// (a hook's or the handler's): logged here with its traceback, a generic 500 for the
/// client.
#[pyfunction]
pub(crate) fn _worker_fail(
    py: Python<'_>,
    worker_id: usize,
    pool_id: u64,
    req_id: u64,
    exception: Bound<'_, PyBaseException>,
) -> PyResult<()> {
    no_panic("_worker_fail", || {
        let error = HandlerError::Python {
            stage: Stage::AsyncTask,
            exception: PyException::capture(py, &PyErr::from_value(exception.into_any())),
        };
        answer(worker_id, pool_id, req_id, Err(error));
        Ok(())
    })
}

/// Answers request `req_id` of async worker `worker_id` with a timeout (504): its task ran
/// past the request budget and was cancelled.
#[pyfunction]
pub(crate) fn _worker_timed_out(worker_id: usize, pool_id: u64, req_id: u64) -> PyResult<()> {
    no_panic("_worker_timed_out", || {
        answer(worker_id, pool_id, req_id, Err(HandlerError::Timeout));
        Ok(())
    })
}

/// Sends `result` to the caller waiting for request `req_id`, an error logged first, with
/// the request it belongs to. A result nobody waits for any more (the caller timed out, or
/// this is a zombie worker of an earlier pool) is dropped; an error is still logged.
fn answer(worker_id: usize, pool_id: u64, req_id: u64, result: Result<ResponseData, HandlerError>) {
    // Same pool-id guard as `_worker_recv`: a zombie's result is dropped rather than
    // answering a request of the live pool.
    let pending = get_worker_state(worker_id)
        .filter(|s| s.pool_id == pool_id)
        .and_then(|state| {
            let mut map = state.response_map.lock().unwrap_or_else(|e| e.into_inner());
            let pending = map.remove(&req_id);
            // Periodic orphan sweep: purge entries whose receivers were dropped (the Rust
            // side timed out), so handlers that die between recv and send can't grow it.
            if map.len() > 64 {
                map.retain(|_id, p| !p.reply.is_closed());
            }
            pending
        });
    let Some(pending) = pending else {
        if let Err(error) = result {
            tracing::error!(
                target: "pyronova::handler", worker_id, req_id, error = %error,
                "async request failed after its caller stopped waiting"
            );
        }
        return;
    };
    let tag = RequestTag {
        id: &pending.request_id,
        method: &pending.method,
        path: &pending.path,
    };
    let reply = result.map_err(|e| e.log(&tag));
    if pending.reply.send(reply).is_err() {
        tracing::debug!(target: "pyronova::server", req_id, worker_id, "the caller timed out (504); dropping the result");
    }
}

/// A handler's (or hook's) return value as a `Response`, with the one mapping every
/// interpreter uses, so after-request hooks see a `Response` on every path. A `Stream`
/// raises: streaming responses need `gil=True, stream=True` (FR-16).
#[pyfunction]
pub(crate) fn _worker_to_response(py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    no_panic("_worker_to_response", || {
        if value.is_instance_of::<crate::types::PyronovaResponse>() {
            return Ok(value.unbind());
        }
        // SAFETY: attached to `value`'s interpreter (this is a pymethod call).
        let data = unsafe {
            let obj = PyObjRef::from_borrowed(value.as_ptr())
                .ok_or_else(|| PyRuntimeError::new_err("_worker_to_response: null value"))?;
            worker_response(py, obj).map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        };
        let response = data
            .to_py(py)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(response.into_any().unbind())
    })
}

/// The handlers of the app this worker's script registered routes on, indexed like the
/// main interpreter's route table.
#[pyfunction]
pub(crate) fn _worker_app_handlers(py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
    no_panic("_worker_app_handlers", || {
        Ok(crate::app::worker_routes(py)
            .map(|r| r.handlers)
            .unwrap_or_default())
    })
}

/// `before_request` hooks and `after_request` hooks.
type Hooks = (Vec<Py<PyAny>>, Vec<Py<PyAny>>);

/// The worker app's `before_request` and `after_request` hooks, in registration order.
#[pyfunction]
pub(crate) fn _worker_app_hooks(py: Python<'_>) -> PyResult<Hooks> {
    no_panic("_worker_app_hooks", || {
        Ok(crate::app::worker_routes(py)
            .map(|r| (r.before_hooks, r.after_hooks))
            .unwrap_or_default())
    })
}
