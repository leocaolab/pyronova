//! Explicit-interpreter attach for Rust threads (Layer 2, C2 + C4).
//!
//! Once more than one interpreter has executed the engine module, the PyO3 fork
//! refuses a bare `Python::attach` (and a `Py<T>` drop) on a thread that has no
//! Python thread state: it cannot tell which interpreter is meant, and landing in
//! main by accident would run another interpreter's objects under the wrong GIL.
//! Every Rust thread that enters Python therefore names its interpreter here:
//!
//! - [`main_attach`]: run on the main interpreter from any thread that is not bound to
//!   a sub-interpreter. The only spelling allowed outside the allowlist in
//!   `tests/test_attach_allowlist.py`.
//! - [`attach_to`]: the same, for an interpreter captured earlier with
//!   [`Interp::current`].
//!
//! Invariant: no Python-level operation on a main-interpreter object (attach, clone_ref,
//! `Py<T>` drop) happens on a thread bound to a sub-interpreter (TPC threads, pool
//! workers). `Arc<RouteTable>` clones may pass through those threads as plain Rust values:
//! `PyronovaApp::run` keeps the last clone and drops it on main, attached.

use std::sync::OnceLock;

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::InterpreterHandle;

/// An interpreter captured while attached to it: the fork's handle, for attaching from a
/// thread with no thread state, plus the raw pointer, for comparing with the interpreter a
/// thread is already bound to (the handle doesn't expose it).
#[derive(Clone, Copy)]
pub(crate) struct Interp {
    handle: InterpreterHandle,
    raw: *mut ffi::PyInterpreterState,
}

// SAFETY: both fields are a pointer to an interpreter, used only for identity and for
// `InterpreterHandle::attach` (itself `Send + Sync`). Nothing is dereferenced here.
unsafe impl Send for Interp {}
unsafe impl Sync for Interp {}

impl Interp {
    /// The interpreter the calling thread is attached to.
    pub(crate) fn current(py: Python<'_>) -> Self {
        Self {
            handle: InterpreterHandle::current(py),
            // SAFETY: `py` witnesses a current thread state.
            raw: unsafe { ffi::PyInterpreterState_Get() },
        }
    }

    /// True for the main interpreter.
    pub(crate) fn is_main(&self) -> bool {
        // SAFETY: always safe to call.
        self.raw == unsafe { ffi::PyInterpreterState_Main() }
    }

    pub(crate) fn id(&self) -> i64 {
        // SAFETY: the interpreter is alive for as long as anything that captured it can run.
        unsafe { ffi::PyInterpreterState_GetID(self.raw) }
    }
}

/// The main interpreter, captured when the engine module executes there (always before
/// any server runs, since the server is started through the module).
static MAIN: OnceLock<Interp> = OnceLock::new();

/// Record the main interpreter. Called from the module's exec; a no-op in any other
/// interpreter and on every call after the first.
pub(crate) fn capture_main(py: Python<'_>) {
    let here = Interp::current(py);
    // SAFETY: always safe to call.
    if here.raw == unsafe { ffi::PyInterpreterState_Main() } {
        let _ = MAIN.set(here);
    }
}

/// Whether the calling thread is attached to the main interpreter.
pub(crate) fn on_main(_py: Python<'_>) -> bool {
    // SAFETY: `_py` witnesses a current thread state; both calls are then valid.
    unsafe { ffi::PyInterpreterState_Get() == ffi::PyInterpreterState_Main() }
}

/// The main interpreter.
///
/// # Panics
///
/// If the engine module never executed on main.
pub(crate) fn main_interp() -> Interp {
    *MAIN
        .get()
        .expect("the engine module has not executed on the main interpreter")
}

/// Run `f` attached to the main interpreter, from any thread not bound to a
/// sub-interpreter.
///
/// A thread with no thread state gets one main thread state for its whole life (created
/// on first use, released by a thread-local destructor), so a pool thread that serves
/// many requests doesn't create and destroy one per call.
///
/// # Panics
///
/// If the engine module never executed on main (impossible for code reached through
/// it), or if the calling thread is bound to a sub-interpreter.
pub(crate) fn main_attach<F, R>(f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    let main = main_interp();
    // SAFETY: always safe to call; null when this thread has no thread state.
    if unsafe { ffi::PyGILState_GetThisThreadState() }.is_null() {
        // `try_with`: during thread-local teardown the slot may already be gone; the
        // one-off path below still attaches correctly.
        let _ = THREAD_MAIN_TSTATE.try_with(|slot| slot.ensure(main));
    }
    attach_to(main, f)
}

/// Run `f` attached to `interp`, from any thread.
///
/// - The thread already has a thread state for `interp` (a thread-local main thread state,
///   a worker thread, the main thread): plain `Python::attach`, which re-attaches that
///   thread state through `PyGILState_Ensure` whether or not it is current.
/// - The thread has no thread state: the fork's `InterpreterHandle::attach`, which creates
///   one for `interp` and destroys it afterwards.
///
/// `InterpreterHandle::attach` is never called on a thread that already has a thread
/// state: its fast path compares only the thread's bound thread state, and on a thread
/// that is bound but detached (the main thread inside `py.detach`) it would run `f`
/// without the GIL.
///
/// # Panics
///
/// If the thread is bound to a different interpreter.
pub(crate) fn attach_to<F, R>(interp: Interp, f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    // SAFETY: always safe to call; null when this thread has no thread state.
    let bound = unsafe { ffi::PyGILState_GetThisThreadState() };
    if bound.is_null() {
        return interp.handle.attach(f);
    }
    // SAFETY: `bound` is a live thread state of this thread.
    let bound_interp = unsafe { ffi::PyThreadState_GetInterpreter(bound) };
    assert!(
        bound_interp == interp.raw,
        "attach_to(interpreter {}) on a thread bound to interpreter {}",
        interp.id(),
        // SAFETY: `bound_interp` belongs to a live thread state.
        unsafe { ffi::PyInterpreterState_GetID(bound_interp) },
    );
    Python::attach(f)
}

/// A main-interpreter thread state owned by the current OS thread for its whole life.
struct ThreadMainTstate(std::cell::Cell<*mut ffi::PyThreadState>);

thread_local! {
    static THREAD_MAIN_TSTATE: ThreadMainTstate =
        const { ThreadMainTstate(std::cell::Cell::new(std::ptr::null_mut())) };
}

impl ThreadMainTstate {
    fn ensure(&self, main: Interp) {
        if !self.0.get().is_null() {
            return;
        }
        // SAFETY: the main interpreter is alive; `PyThreadState_New` needs no GIL. It binds
        // the new thread state to this OS thread and, since the thread has none, makes it the
        // thread's gilstate thread state, so `PyGILState_Ensure` re-attaches it.
        let tstate = unsafe { ffi::PyThreadState_New(main.raw) };
        assert!(!tstate.is_null(), "PyThreadState_New returned null");
        self.0.set(tstate);
    }
}

impl Drop for ThreadMainTstate {
    fn drop(&mut self) {
        let tstate = self.0.get();
        // SAFETY: always safe to call.
        if tstate.is_null() || unsafe { ffi::Py_IsInitialized() } == 0 {
            // Never created, or finalized under us: nothing to release into.
            return;
        }
        // SAFETY: `tstate` was created on this thread and is not current: every attach on
        // it went through `PyGILState_Ensure`/`Release`, which leaves it detached. Clear
        // needs the GIL and the tstate current; DeleteCurrent releases both.
        unsafe {
            ffi::PyEval_RestoreThread(tstate);
            ffi::PyThreadState_Clear(tstate);
            ffi::PyThreadState_DeleteCurrent();
        }
    }
}
