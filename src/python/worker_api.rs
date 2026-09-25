//! The engine functions a sub-interpreter worker's async engine (`_async_engine.py`) calls
//! (Layer 2, C5): pull the next request, send a response back, turn a handler's return
//! value into a `Response`, and get the worker app's handlers and hooks.
//!
//! They replace the `extern "C"` functions that used to be injected into worker globals.
//! PyO3 does the argument parsing; a Rust panic becomes a `RuntimeError` (not PyO3's
//! `PanicException`, a `BaseException` that the engine's fetcher thread, which catches
//! `Exception`, would die on).

use std::sync::atomic::Ordering;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::ffi::{get_worker_state, PyObjRef};
use super::pool::SubInterpResponse;
use super::worker::{build_response, new_request, parse_result};

/// Runs `f`, turning a Rust panic into a `RuntimeError` naming `context`.
fn no_panic<T>(context: &'static str, f: impl FnOnce() -> PyResult<T>) -> PyResult<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
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
        let wait = std::sync::Arc::clone(&state);
        let Some(req) = py.detach(move || wait.rx.recv().ok()) else {
            return Ok(None);
        };
        let req_id = state.next_req_id.fetch_add(1, Ordering::Relaxed);
        let headers = crate::types::extract_headers(&req.headers);
        let request = Py::new(
            py,
            new_request(
                &req.method,
                &req.path,
                req.params,
                &req.query,
                req.body,
                headers,
                req.client_ip,
            ),
        )?;
        // Only now that nothing can fail: a send dropped before this point reaches the
        // waiting caller as an error instead of an orphaned map entry.
        state
            .response_map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(req_id, req.response_tx);
        Ok(Some((req_id, req.route.index(), request)))
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
        let parsed: Result<SubInterpResponse, String> = unsafe {
            parse_result(
                py,
                PyObjRef::from_borrowed(response.as_ptr())
                    .ok_or_else(|| PyRuntimeError::new_err("_worker_send: null response"))?,
            )
        };
        // Same pool-id guard as `_worker_recv`: a zombie's result is dropped, and the
        // caller times out, rather than answering a request of the live pool.
        let Some(state) = get_worker_state(worker_id).filter(|s| s.pool_id == pool_id) else {
            return Ok(());
        };
        let mut map = state.response_map.lock().unwrap_or_else(|e| e.into_inner());
        match map.remove(&req_id) {
            Some(tx) if !tx.is_closed() => {
                let _ = tx.send(parsed);
            }
            Some(_) => {
                tracing::debug!(target: "pyronova::server", req_id, worker_id, "response_map: receiver gone (client timed out), dropping result");
            }
            None => {
                tracing::debug!(target: "pyronova::server", req_id, worker_id, "response_map miss — client already timed out (504)");
            }
        }
        // Periodic orphan sweep: purge entries whose receivers were dropped (the Rust side
        // timed out), so handlers that die between recv and send can't grow the map.
        if map.len() > 64 {
            map.retain(|_id, tx| !tx.is_closed());
        }
        Ok(())
    })
}

/// A handler's (or hook's) return value as a `Response`, with the same mapping as the sync
/// worker path (M4 review N1a/N1b), so after-request hooks see a `Response` on both paths.
/// A `Stream` raises: streaming responses need `gil=True, stream=True` (FR-16).
#[pyfunction]
pub(crate) fn _worker_to_response(py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    no_panic("_worker_to_response", || {
        if value.is_instance_of::<crate::types::PyronovaResponse>() {
            return Ok(value.unbind());
        }
        // SAFETY: attached to `value`'s interpreter (this is a pymethod call).
        unsafe {
            let obj = PyObjRef::from_borrowed(value.as_ptr())
                .ok_or_else(|| PyRuntimeError::new_err("_worker_to_response: null value"))?;
            let parsed = parse_result(py, obj).map_err(PyRuntimeError::new_err)?;
            let resp = build_response(py, &parsed).map_err(PyRuntimeError::new_err)?;
            Ok(Bound::from_owned_ptr(py, resp.into_raw()).unbind())
        }
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
