//! One `contextvars.Context` per request.
//!
//! A sync handler runs on a long-lived thread (a TPC thread, a pool worker, a
//! `spawn_blocking` thread, a bridge thread), and that thread's current context outlives
//! the request: a `ContextVar` a hook or handler sets would be seen by the next request on
//! the thread. Every request's before-hooks, handler and after-hooks run instead inside a
//! fresh context entered for the request and exited after it, so they share their writes
//! with each other and with nothing else. Async handlers get the same from their `Task`,
//! which runs in a copy of the context it was created in.

use pyo3::ffi;
use pyo3::prelude::*;

/// Runs `f` in a new, empty `contextvars.Context`, entered on this thread for the call.
///
/// Costs one context object (the empty variable map is CPython's shared singleton) and
/// two pointer swaps on the thread state.
pub(crate) fn in_request_context<R>(py: Python<'_>, f: impl FnOnce() -> R) -> PyResult<R> {
    // SAFETY: attached (`py`); `PyContext_New` returns a new reference or NULL with an
    // exception set.
    let ctx = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyContext_New())? };
    // SAFETY: attached; `ctx` is a context object. Fails only if it is already entered,
    // and it was created just now.
    if unsafe { ffi::PyContext_Enter(ctx.as_ptr()) } != 0 {
        return Err(PyErr::fetch(py));
    }
    let _entered = Entered(ctx);
    Ok(f())
}

/// Exits the request's context when the call ends, panics included.
struct Entered<'py>(Bound<'py, PyAny>);

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        // SAFETY: attached (the `Bound`'s token); this context was entered on this thread
        // by `in_request_context`. Exit fails only if Python code left another context
        // current, i.e. entered one it never exited.
        if unsafe { ffi::PyContext_Exit(self.0.as_ptr()) } != 0 {
            let err = PyErr::fetch(self.0.py());
            tracing::error!(
                target: "pyronova::server",
                error = %err,
                "leaving the request's contextvars.Context failed: the handler entered a \
                 context it did not exit"
            );
        }
    }
}
