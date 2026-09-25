//! One `contextvars.Context` per request.
//!
//! A sync handler runs on a long-lived thread (a TPC thread, a pool worker, a
//! `spawn_blocking` thread, a bridge thread), and that thread's current context outlives
//! the request: a `ContextVar` a hook or handler sets would be seen by the next request on
//! the thread. Every request's before-hooks, handler and after-hooks run instead inside a
//! fresh context entered for the request and exited after it, so they share their writes
//! with each other and with nothing else.
//!
//! An `async def` hook or handler on these paths runs to completion on the thread's event
//! loop, as a task whose context is the request's own ([`RequestContext::run_coroutine`]),
//! so what it sets is seen by the rest of the request. (A task created the default way
//! runs in a copy of the context, and its writes are lost when it ends.) The async engine
//! gets the same by running the whole request as one task.

use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// The request's `contextvars.Context`, entered on this thread for the request.
pub(crate) struct RequestContext<'py>(Bound<'py, PyAny>);

/// Runs `f` in a new, empty `contextvars.Context`, entered on this thread for the call.
///
/// Costs one context object (the empty variable map is CPython's shared singleton) and
/// two pointer swaps on the thread state.
pub(crate) fn in_request_context<'py, R>(
    py: Python<'py>,
    f: impl FnOnce(&RequestContext<'py>) -> R,
) -> PyResult<R> {
    // SAFETY: attached (`py`); `PyContext_New` returns a new reference or NULL with an
    // exception set.
    let ctx = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyContext_New())? };
    // SAFETY: attached; a context object. Fails only if it is already entered, and it was
    // created just now.
    if unsafe { ffi::PyContext_Enter(ctx.as_ptr()) } != 0 {
        return Err(PyErr::fetch(py));
    }
    let entered = Entered(RequestContext(ctx));
    Ok(f(&entered.0))
}

/// What a hook or handler returned, as far as running it goes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Returned {
    /// A plain value: the response (or `None`).
    Value,
    /// A native coroutine (`async def`): run it with [`RequestContext::run_coroutine`].
    Coroutine,
    /// Another awaitable (a `Future`, a `Task`, an object with `__await__`): it carries
    /// its own context, so it is run as it is.
    OtherAwaitable,
}

impl Returned {
    /// Classifies `obj` with C-level type probes: `PyCoro_CheckExact` (one tag compare)
    /// catches `async def`; any other awaitable has `tp_as_async->am_await`. Nanoseconds,
    /// no attribute lookup: this runs for every hook and handler result.
    pub(crate) fn of(obj: &Bound<'_, PyAny>) -> Self {
        let ptr = obj.as_ptr();
        // SAFETY: `ptr` is a live object (borrowed from `obj`); its type pointer is never
        // NULL and `tp_as_async` is NULL or points to the type's async slots.
        unsafe {
            if ffi::PyCoro_CheckExact(ptr) == 1 {
                return Returned::Coroutine;
            }
            let slots = (*ffi::Py_TYPE(ptr)).tp_as_async;
            if !slots.is_null() && (*slots).am_await.is_some() {
                Returned::OtherAwaitable
            } else {
                Returned::Value
            }
        }
    }
}

impl<'py> RequestContext<'py> {
    /// Runs the coroutine `coro` to completion on `event_loop` as a task whose context is
    /// this one: the context is left for the run (a context can't be entered twice) and
    /// entered again after it, however the run ends.
    pub(crate) fn run_coroutine(
        &self,
        event_loop: &Bound<'py, PyAny>,
        coro: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = self.0.py();
        let _suspended = self.suspend()?;
        let kwargs = PyDict::new(py);
        kwargs.set_item(intern!(py, "context"), &self.0)?;
        let task = event_loop.call_method(intern!(py, "create_task"), (coro,), Some(&kwargs))?;
        event_loop.call_method1(intern!(py, "run_until_complete"), (task,))
    }

    /// Leaves this context until the returned guard drops.
    fn suspend(&self) -> PyResult<Suspended<'_, 'py>> {
        // SAFETY: attached (the `Bound`'s token); this context is the current one, entered
        // by `in_request_context`, unless Python code entered another and never left it.
        if unsafe { ffi::PyContext_Exit(self.0.as_ptr()) } != 0 {
            return Err(PyErr::fetch(self.0.py()));
        }
        Ok(Suspended(self))
    }
}

/// Enters the request's context again after [`RequestContext::run_coroutine`].
struct Suspended<'a, 'py>(&'a RequestContext<'py>);

impl Drop for Suspended<'_, '_> {
    fn drop(&mut self) {
        let ctx = &self.0 .0;
        // SAFETY: attached; the context was left by `suspend` and nothing entered it since
        // (its task ran to completion, or never started).
        if unsafe { ffi::PyContext_Enter(ctx.as_ptr()) } != 0 {
            let err = PyErr::fetch(ctx.py());
            tracing::error!(
                target: "pyronova::server",
                error = %err,
                "re-entering the request's contextvars.Context after an awaitable failed"
            );
        }
    }
}

/// Exits the request's context when the call ends, panics included.
struct Entered<'py>(RequestContext<'py>);

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        let ctx = &self.0 .0;
        // SAFETY: attached (the `Bound`'s token); this context was entered on this thread
        // by `in_request_context`. Exit fails only if Python code left another context
        // current, i.e. entered one it never exited.
        if unsafe { ffi::PyContext_Exit(ctx.as_ptr()) } != 0 {
            let err = PyErr::fetch(ctx.py());
            tracing::error!(
                target: "pyronova::server",
                error = %err,
                "leaving the request's contextvars.Context failed: the handler entered a \
                 context it did not exit"
            );
        }
    }
}
