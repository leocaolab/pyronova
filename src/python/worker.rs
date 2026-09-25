//! `SubInterpreterWorker` — owns one CPython sub-interpreter and runs
//! request handlers inside it. This is the densest concentration of
//! `unsafe` + raw `pyo3::ffi` in the codebase.
//!
//! A worker runs the same program as the main interpreter (Layer 2): its bootstrap sets up
//! logging, the GC policy and C-extension isolation, then the user's script executes as a
//! real module and imports the real `pyronova` package and engine. The worker takes its
//! handlers from the app that script registered routes on, checked index by index against
//! the main interpreter's table.

use std::ffi::{CStr, CString};

use pyo3::ffi;
use pyo3::prelude::*;

use super::convert::*;
use super::ffi::*;
use crate::router::RouteSignature;
use crate::types::ResponseData;

/// Name of the module the user's script executes as in a worker. Not `__main__`, so a script's
/// `if __name__ == "__main__": app.run()` does not run in workers.
const SCRIPT_MODULE: &CStr = c"__pyronova_worker__";
/// Name of the module the bootstrap executes as.
const BOOTSTRAP_MODULE: &CStr = c"__pyronova_bootstrap__";
/// Name of the module the async engine executes as.
const ASYNC_ENGINE_MODULE: &CStr = c"__pyronova_async_engine__";

// ---------------------------------------------------------------------------
// Safe sub-interpreter
// ---------------------------------------------------------------------------

pub(crate) struct SubInterpreterWorker {
    /// Thread state (saved after releasing GIL)
    pub(crate) tstate: *mut ffi::PyThreadState,
    /// This worker's index: `WORKER_ID` in its bootstrap (log records) and async engine
    /// (its slot in `WORKER_STATES`).
    pub(crate) worker_id: usize,
    /// Handlers, indexed like the main interpreter's route table (owned references).
    handlers: Vec<*mut ffi::PyObject>,
    /// `before_request` / `after_request` hooks, in registration order (owned references).
    before_hooks: Vec<*mut ffi::PyObject>,
    after_hooks: Vec<*mut ffi::PyObject>,
    /// Cached: persistent asyncio event loop for this sub-interpreter
    asyncio_loop: *mut ffi::PyObject,
    /// Cached: loop.run_until_complete method
    loop_run_func: *mut ffi::PyObject,
    /// Pool instance id (see `POOL_ID_COUNTER`). Exposed to the async
    /// engine as `POOL_ID` so it can be passed into every
    /// `_worker_recv` / `_worker_send` call for the zombie-worker guard.
    pub(crate) pool_id: u64,
    /// Cached `gc.collect` function pointer. `_bootstrap.py` runs
    /// `gc.disable()` at sub-interp init so CPython's threshold-based
    /// automatic triggers never fire. Instead we call this manually at
    /// a request-count cadence (see `gc_threshold`) and, in TPC idle mode,
    /// when the worker's thread goes quiet, pushing all cycle-collection
    /// work off the hot path and into slots between requests.
    gc_collect_func: *mut ffi::PyObject,
    /// Collect once this many requests ran since the last collect; 0 disables the
    /// count trigger (use when you've verified your handler graph creates no cycles —
    /// ref-counting handles everything else instantly). `PYRONOVA_GC_THRESHOLD=N`;
    /// TPC idle mode sets it to the OOM failsafe.
    pub(crate) gc_threshold: u64,
    /// Requests this worker has run, counted as each is dispatched (`call_handler`).
    /// Per-worker = per-thread, so no atomics needed.
    requests_served: u64,
    /// `requests_served` at the last `gc.collect()`.
    collected_at: u64,
    /// Set once the interpreter is ended (or deliberately abandoned). `Drop` checks it.
    ended: bool,
}

unsafe impl Send for SubInterpreterWorker {}

impl Drop for SubInterpreterWorker {
    fn drop(&mut self) {
        // The interpreter and the references above belong to it; they can only be released
        // with its thread state current, which `end` does. A worker dropped without `end`
        // leaks them rather than decref'ing into whatever interpreter is current here
        // (Layer 2, FR-19).
        if !self.ended && !self.tstate.is_null() {
            tracing::error!(
                target: "pyronova::server",
                worker = self.worker_id,
                "sub-interpreter worker dropped without being ended; leaking its interpreter"
            );
        }
    }
}

impl SubInterpreterWorker {
    /// Create a new sub-interpreter, run the bootstrap and the user's script in it, and
    /// bind the handlers of the app the script registered routes on.
    ///
    /// # Safety
    /// Must be called while the main interpreter's thread state is current.
    /// Switches to the new sub-interpreter and back to main on completion.
    pub(crate) unsafe fn new(
        worker_id: usize,
        script: &str,
        script_path: &str,
        expected: &RouteSignature,
        pool_id: u64,
        shared_state: &crate::state::SharedMap,
    ) -> Result<Self, String> {
        let main_tstate = ffi::PyThreadState_Get();

        let mut new_tstate: *mut ffi::PyThreadState = std::ptr::null_mut();
        let config = ffi::PyInterpreterConfig {
            use_main_obmalloc: 0,
            allow_fork: 0,
            allow_exec: 0,
            allow_threads: 1,
            allow_daemon_threads: 0,
            check_multi_interp_extensions: 1, // Strict: only extensions declaring multi-interp support
            gil: ffi::PyInterpreterConfig_OWN_GIL,
        };

        let status = ffi::Py_NewInterpreterFromConfig(&mut new_tstate, &config);
        if ffi::PyStatus_IsError(status) != 0 || new_tstate.is_null() {
            ffi::PyThreadState_Swap(main_tstate);
            return Err("Py_NewInterpreterFromConfig failed".to_string());
        }

        // Past this point we own a live sub-interpreter. Any early error
        // must Py_EndInterpreter it before returning, or the sub-interp
        // (and the thread resources it pins) leak permanently. The half-built
        // worker's references are released inside `init_in_sub_interp`, while
        // this interpreter is still current.
        match Self::init_in_sub_interp(
            worker_id,
            script,
            script_path,
            expected,
            pool_id,
            shared_state,
        ) {
            Ok(worker) => {
                ffi::PyThreadState_Swap(main_tstate);
                Ok(worker)
            }
            Err(e) => {
                ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
                ffi::PyThreadState_Swap(main_tstate);
                Err(e)
            }
        }
    }

    /// Run every init step that executes INSIDE the freshly-created
    /// sub-interpreter. Returns a worker whose `tstate` is already saved
    /// via PyEval_SaveThread (GIL released). Caller is responsible for
    /// swapping back to the main tstate, and for Py_EndInterpreter on error.
    ///
    /// # Safety
    /// Must be called with a sub-interpreter's thread state current.
    unsafe fn init_in_sub_interp(
        worker_id: usize,
        script: &str,
        script_path: &str,
        expected: &RouteSignature,
        pool_id: u64,
        shared_state: &crate::state::SharedMap,
    ) -> Result<Self, String> {
        // NOT `Python::attach`: this runs on the main OS thread, whose gilstate thread
        // state is main's, while this sub-interpreter's thread state is current and holds
        // its GIL (`Py_NewInterpreterFromConfig` in `new`). Take the token for it directly.
        let py = Python::assume_attached();

        // Before the script runs: a `PyronovaApp` or `SharedState` it creates in this
        // interpreter must see the running app's map (Layer 2, C2 / FR-5).
        crate::state::hand_to_worker(py, shared_state)?;

        // 1. The bootstrap (logging bridge, GC policy, C-extension isolation) in its own
        //    namespace, with this worker's id (FR-20).
        let bootstrap = new_module(BOOTSTRAP_MODULE, None, Some((worker_id, pool_id)))?;
        exec_in(
            include_str!("../../python/pyronova/_bootstrap.py"),
            "pyronova/_bootstrap.py",
            &bootstrap,
            worker_id,
        )?;

        // 2. The user's script as a real module, compiled with its own path so tracebacks
        //    point at it, `from __future__` imports work, and `typing.get_type_hints` finds
        //    the module in `sys.modules` (FR-20).
        let script_module = new_module(SCRIPT_MODULE, Some(script_path), None)?;
        exec_in(script, script_path, &script_module, worker_id)?;

        // 3. The handlers of the app the script registered routes on, index by index
        //    against main's table (FR-3, FR-4).
        let routes = crate::app::worker_routes(py);
        let (handlers, before_hooks, after_hooks) = match routes {
            Some(r) if r.signature == *expected => (r.handlers, r.before_hooks, r.after_hooks),
            Some(r) => {
                return Err(format!(
                    "worker {worker_id}: the script registered a different route table than \
                     the main interpreter (a script must register the same routes in every \
                     interpreter; routes that exist only on main are registered after \
                     app.run() starts and must be gil=True).\n{}",
                    RouteSignature::describe_mismatch(expected, &r.signature)
                ))
            }
            None if expected.is_empty() => (Vec::new(), Vec::new(), Vec::new()),
            None => {
                return Err(format!(
                    "worker {worker_id}: the script registered no routes in the worker, but \
                     the main interpreter has {} route(s). A worker executes the whole script \
                     and serves the app it registers routes on; don't register routes only \
                     in the main interpreter.",
                    expected.routes.len()
                ))
            }
        };
        let owned = |v: Vec<Py<PyAny>>| -> Vec<*mut ffi::PyObject> {
            v.into_iter().map(|h| h.into_ptr()).collect()
        };
        let handlers = owned(handlers);
        let before_hooks = owned(before_hooks);
        let after_hooks = owned(after_hooks);

        // Create persistent asyncio event loop for this sub-interpreter
        let (asyncio_loop, loop_run_func) = {
            let asyncio_mod = ffi::PyImport_ImportModule(c"asyncio".as_ptr());
            if !asyncio_mod.is_null() {
                let loop_obj = ffi::PyObject_CallMethod(
                    asyncio_mod,
                    c"new_event_loop".as_ptr(),
                    std::ptr::null(),
                );
                let run_func = if !loop_obj.is_null() {
                    // Set as current loop; Py_DECREF the None return value.
                    let set_result = ffi::PyObject_CallMethod(
                        asyncio_mod,
                        c"set_event_loop".as_ptr(),
                        c"O".as_ptr(),
                        loop_obj,
                    );
                    if !set_result.is_null() {
                        ffi::Py_DECREF(set_result);
                    } else {
                        ffi::PyErr_Clear();
                    }
                    ffi::PyObject_GetAttrString(loop_obj, c"run_until_complete".as_ptr())
                } else {
                    ffi::PyErr_Clear();
                    std::ptr::null_mut()
                };
                ffi::Py_DECREF(asyncio_mod);
                (loop_obj, run_func)
            } else {
                ffi::PyErr_Clear();
                (std::ptr::null_mut(), std::ptr::null_mut())
            }
        };

        // Cache gc.collect so the scheduled-GC path doesn't re-import
        // per tick. `_bootstrap.py` has already called gc.disable() at
        // this point; the function pointer is just for manual triggers.
        let gc_collect_func = {
            let gc_mod = ffi::PyImport_ImportModule(c"gc".as_ptr());
            if !gc_mod.is_null() {
                let f = ffi::PyObject_GetAttrString(gc_mod, c"collect".as_ptr());
                ffi::Py_DECREF(gc_mod);
                if f.is_null() {
                    ffi::PyErr_Clear();
                }
                f
            } else {
                ffi::PyErr_Clear();
                std::ptr::null_mut()
            }
        };

        // Read the threshold env var once at sub-interp init (it's set
        // on the main process before any sub-interp spawns). 0 disables
        // scheduled collection.
        // Default 100_000 — conservative. At 100k rps/thread that's one
        // scheduled collect per second, which is invisible in P99. The
        // old CPython-default threshold-based auto-trigger fired at
        // ~hundreds of collects per second under the same load → P99
        // jumps to 2-10ms. Measurement: on the baseline test,
        // threshold=5000 gave p99=2.0ms; threshold=100_000 gave
        // p99≈300µs; threshold=0 (disabled) gave p99=240µs.
        //
        // Workloads that verified they create no cycles can set
        // PYRONOVA_GC_THRESHOLD=0 for best P99. Workloads with
        // known-high cycle churn may want threshold=10_000.
        let gc_threshold: u64 = std::env::var("PYRONOVA_GC_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000);

        // The module references are released while this interpreter's thread state is
        // still current: dropped after `PyEval_SaveThread` they would find none attached,
        // and `PyObjRef` leaks rather than DECREFs (the modules stay in `sys.modules`).
        drop(bootstrap);
        drop(script_module);

        // Release this sub-interpreter's GIL. Outer `new()` swaps back to
        // the main interpreter after we return.
        let saved = ffi::PyEval_SaveThread();

        Ok(SubInterpreterWorker {
            tstate: saved,
            worker_id,
            handlers,
            before_hooks,
            after_hooks,
            asyncio_loop,
            loop_run_func,
            pool_id,
            gc_collect_func,
            gc_threshold,
            requests_served: 0,
            collected_at: 0,
            ended: false,
        })
    }

    /// Ends this worker's sub-interpreter: with its thread state current, release every
    /// reference the worker holds, then `Py_EndInterpreter` (Layer 2, FR-19).
    ///
    /// Works from the worker's own thread (normal shutdown, no thread state current) and
    /// from the thread that created it (a failed start, possibly with main's thread state
    /// current, which is detached for the duration and restored afterwards).
    ///
    /// # Safety
    /// The runtime must not be finalized (see [`Self::abandon`]), and `tstate` must be
    /// usable on the calling thread: the worker's rebound thread state on its own thread,
    /// or the creator thread state on the creating thread.
    pub(crate) unsafe fn end(mut self) {
        self.ended = true;
        if self.tstate.is_null() {
            return;
        }
        let previous = ffi::PyThreadState_GetUnchecked();
        if !previous.is_null() {
            ffi::PyEval_SaveThread();
        }
        ffi::PyEval_RestoreThread(self.tstate);
        let owned = self
            .handlers
            .drain(..)
            .chain(self.before_hooks.drain(..))
            .chain(self.after_hooks.drain(..))
            .chain([self.loop_run_func, self.asyncio_loop, self.gc_collect_func]);
        for p in owned {
            if !p.is_null() {
                ffi::Py_DECREF(p);
            }
        }
        self.loop_run_func = std::ptr::null_mut();
        self.asyncio_loop = std::ptr::null_mut();
        self.gc_collect_func = std::ptr::null_mut();
        ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
        self.tstate = std::ptr::null_mut();
        if !previous.is_null() {
            ffi::PyEval_RestoreThread(previous);
        }
    }

    /// Gives up on this worker without touching Python: for a worker whose thread outlived
    /// `Py_Finalize` (forgotten after the shutdown grace period), where restoring its thread
    /// state would be a use-after-free. The OS reclaims its memory at exit.
    pub(crate) fn abandon(mut self) {
        self.ended = true;
    }

    /// Ends the worker a TPC thread served through an `Rc<RefCell<_>>`, on that thread, once
    /// its runtime and every task holding a clone are gone (FR-19).
    pub(crate) fn end_shared(worker: std::rc::Rc<std::cell::RefCell<Self>>) {
        match std::rc::Rc::try_unwrap(worker) {
            Ok(cell) => {
                let worker = cell.into_inner();
                // A thread forgotten past shutdown may run this after `Py_Finalize`.
                if unsafe { ffi::Py_IsInitialized() } != 0 {
                    // SAFETY: on the worker's own thread, no thread state current.
                    unsafe { worker.end() };
                } else {
                    worker.abandon();
                }
            }
            Err(still_shared) => tracing::error!(
                target: "pyronova::server",
                worker = still_shared.borrow().worker_id,
                "a worker is still referenced after its thread's runtime ended; leaking its \
                 interpreter"
            ),
        }
    }

    /// Ends every worker in `workers` on the calling (creating) thread: the clean-up of a
    /// start that failed after some workers were built (FR-19).
    ///
    /// # Safety
    /// As for [`Self::end`], on the thread that created the workers, before any of them was
    /// rebound to another thread.
    pub(crate) unsafe fn end_all(workers: impl IntoIterator<Item = Self>) {
        for w in workers {
            if ffi::Py_IsInitialized() != 0 {
                w.end();
            } else {
                w.abandon();
            }
        }
    }

    /// Runs the async engine in this worker's interpreter until its request channel closes.
    ///
    /// # Safety
    /// Must be called with this worker's thread state current.
    pub(crate) unsafe fn run_async_engine(&self) -> Result<(), String> {
        // Its own namespace, not the script's globals (M4 review N1d); `WORKER_ID` and
        // `POOL_ID` identify this worker's slot in `WORKER_STATES`.
        let engine = new_module(
            ASYNC_ENGINE_MODULE,
            None,
            Some((self.worker_id, self.pool_id)),
        )?;
        exec_in(
            include_str!("../../python/pyronova/_async_engine.py"),
            "pyronova/_async_engine.py",
            &engine,
            self.worker_id,
        )
    }

    /// Build a fresh `Request` instance for this request.
    ///
    /// Returns a NEW owned reference (caller must DECREF). Constructs a
    /// `PyronovaRequest` pyclass via `Py::new`; PyO3's generated
    /// `tp_dealloc` Rust-drops every field when the returned object's
    /// refcount reaches zero, so no `SlotClearer` / instance recycling is
    /// needed and there is nothing to leak under PEP 684 sub-interpreters.
    #[allow(clippy::too_many_arguments)]
    fn build_request(
        py: Python<'_>,
        method: &str,
        path: &str,
        params: &[(String, String)],
        query: &str,
        body: bytes::Bytes,
        headers: hyper::HeaderMap,
        client_ip: std::net::IpAddr,
    ) -> Result<*mut ffi::PyObject, String> {
        // params, headers and body are materialized lazily by the pyclass getters, so a
        // handler that never touches `.params` / `.headers` / `.body` pays nothing for
        // them.
        let req = new_request(
            method,
            path,
            params.to_vec(),
            query,
            body,
            headers,
            client_ip,
        );
        Py::new(py, req)
            .map(|obj| obj.into_ptr())
            .map_err(|e| format!("Py::new(Request) failed: {e}"))
    }

    /// If obj is awaitable (coroutine / Task / Future / custom __await__),
    /// drive it via the persistent event loop. Otherwise return unchanged.
    ///
    /// Detection is a C-level type-slot probe:
    ///   1. Fast path `PyCoro_CheckExact` — one tag compare, catches
    ///      the common `async def` case.
    ///   2. Fallback: read `Py_TYPE(obj)->tp_as_async->am_await` —
    ///      any real awaitable (Task, Future, user class with
    ///      `__await__`) has this slot populated. One pointer chase +
    ///      null check. Nanoseconds, L1-resident.
    ///
    /// We avoid `PyObject_HasAttrString(obj, "__await__")` here: that
    /// path would intern the string, walk the MRO, and potentially
    /// trigger descriptor protocol — μs-level, and at 400k rps on the
    /// hot hook path it showed up as a measurable 5% throughput loss.
    unsafe fn resolve_coroutine(&self, obj: PyObjRef) -> Result<PyObjRef, String> {
        let ptr = obj.as_ptr();
        let is_awaitable = if ffi::PyCoro_CheckExact(ptr) == 1 {
            true
        } else {
            let tp = ffi::Py_TYPE(ptr);
            if tp.is_null() {
                false
            } else {
                let async_slots = (*tp).tp_as_async;
                !async_slots.is_null() && (*async_slots).am_await.is_some()
            }
        };
        if !is_awaitable {
            return Ok(obj); // Plain value — pass through
        }
        if self.loop_run_func.is_null() {
            return Err("async handler used but asyncio event loop not available".to_string());
        }
        // Call loop.run_until_complete(awaitable)
        let args =
            PyObjRef::from_owned(ffi::PyTuple_New(1)).ok_or("failed to create args tuple")?;
        ffi::PyTuple_SetItem(args.as_ptr(), 0, obj.into_raw());
        let result = PyObjRef::from_owned(ffi::PyObject_Call(
            self.loop_run_func,
            args.as_ptr(),
            std::ptr::null_mut(),
        ));
        match result {
            Some(r) => Ok(r),
            None => {
                log_and_clear_py_exception("loop.run_until_complete");
                Err("loop.run_until_complete() failed".to_string())
            }
        }
    }

    /// Call a handler function and return the response.
    ///
    /// # Safety
    /// Must be called with this sub-interpreter's GIL held.
    #[allow(clippy::too_many_arguments)]
    pub(crate) unsafe fn call_handler(
        &mut self,
        route: crate::router::RouteId,
        method: &str,
        path: &str,
        params: &[(String, String)],
        query: &str,
        body: bytes::Bytes,
        headers: hyper::HeaderMap,
        client_ip: std::net::IpAddr,
    ) -> Result<ResponseData, String> {
        self.attached(|worker, py| {
            worker.requests_served += 1;
            // The hooks and the handler share one fresh `contextvars.Context`.
            let response = crate::python::request_context::in_request_context(py, || {
                worker.call_handler_attached(
                    py,
                    route.index(),
                    method,
                    path,
                    params,
                    query,
                    body,
                    headers,
                    client_ip,
                )
            })
            .unwrap_or_else(|e| {
                Err(format!(
                    "could not enter the request's contextvars.Context: {e}"
                ))
            });
            if worker.gc_threshold > 0 && worker.requests_since_collect() >= worker.gc_threshold {
                worker.collect_garbage(py);
            }
            response
        })
    }

    /// Runs `gc.collect()` between requests, from the thread this worker is bound to (TPC
    /// idle mode).
    ///
    /// # Safety
    /// On the thread `rebind_tstate_to_current_thread` bound this worker to, with no thread
    /// state current.
    pub(crate) unsafe fn collect_garbage_between_requests(&mut self) {
        let tstate = std::cell::Cell::new(self.tstate);
        {
            let _gil = SubInterpGilGuard::acquire(tstate.get(), &tstate);
            self.attached(|worker, py| worker.collect_garbage(py));
        }
        self.tstate = tstate.get();
    }

    /// Requests this worker has run.
    pub(crate) fn requests_served(&self) -> u64 {
        self.requests_served
    }

    /// Requests this worker has run since its last `gc.collect()`.
    pub(crate) fn requests_since_collect(&self) -> u64 {
        self.requests_served - self.collected_at
    }

    /// Runs `f` attached to this worker's interpreter.
    ///
    /// # Safety
    /// This worker's thread state is current on the calling thread (`SubInterpGilGuard`).
    unsafe fn attached<R>(&mut self, f: impl FnOnce(&mut Self, Python<'_>) -> R) -> R {
        // Re-entrant: this worker's thread state is current (SubInterpGilGuard), and it is
        // the thread's gilstate one (`rebind_tstate_to_current_thread`), so this registers
        // the attach with PyO3 without switching thread states.
        Python::attach(|py| f(self, py))
    }

    /// One full `gc.collect()`. `_bootstrap.py` ran `gc.disable()`, so this is the only
    /// cycle collection the worker gets. A failure is logged with its real error and
    /// taken off the interpreter, so the next request starts with no exception set.
    fn collect_garbage(&mut self, py: Python<'_>) {
        self.collected_at = self.requests_served;
        if self.gc_collect_func.is_null() {
            return;
        }
        // SAFETY: attached (`py`); `gc_collect_func` is an owned reference to this
        // interpreter's `gc.collect`. `PyObject_CallNoArgs` skips the empty-tuple alloc.
        let collected = unsafe {
            Bound::from_owned_ptr_or_err(py, ffi::PyObject_CallNoArgs(self.gc_collect_func))
        };
        if let Err(err) = collected {
            let traceback = err
                .traceback(py)
                .and_then(|tb| tb.format().ok())
                .unwrap_or_default();
            tracing::error!(
                target: "pyronova::app",
                worker_id = self.worker_id,
                error = %err,
                traceback,
                "gc.collect() raised; the worker keeps serving, but this signals OOM, heap \
                 corruption or interpreter damage"
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    unsafe fn call_handler_attached(
        &mut self,
        py: Python<'_>,
        handler_idx: usize,
        method: &str,
        path: &str,
        params: &[(String, String)],
        query: &str,
        body: bytes::Bytes,
        headers: hyper::HeaderMap,
        client_ip: std::net::IpAddr,
    ) -> Result<ResponseData, String> {
        let func = *self.handlers.get(handler_idx).ok_or_else(|| {
            format!(
                "handler index {handler_idx} out of range ({} handlers)",
                self.handlers.len()
            )
        })?;

        // Fresh `Request` (new owned ref). The Rust-backed type's
        // `tp_dealloc` synchronously DECREFs all slot fields when this
        // PyObjRef drops at scope end — no SlotClearer / instance
        // recycling needed, no PEP 684 finalizer bug to work around.
        // ── Leak-hunt bisection hook (leak_detect feature only) ──────
        // PYRONOVA_BISECT values:
        //   "skip_all"     — no build_request, no handler call; return a
        //                    fixed response. Exercises hyper +
        //                    channel only. If this still leaks, the
        //                    leak is NOT in the Python side at all.
        //   "skip_handler" — build_request + dealloc runs, but handler
        //                    is not invoked. Isolates request-object
        //                    construction from handler/response path.
        //   "skip_build"   — handler runs with Py_None as the request
        //                    arg (user code will crash, but we only
        //                    care about memory — use a handler that
        //                    ignores its arg, e.g. `def h(req): return "ok"`).
        // Unset / any other value: normal execution.
        #[cfg(feature = "leak_detect")]
        let bisect_mode = std::env::var("PYRONOVA_BISECT").ok();
        #[cfg(not(feature = "leak_detect"))]
        let bisect_mode: Option<String> = None;

        if bisect_mode.as_deref() == Some("skip_all") {
            return Ok(bisect_response());
        }

        let request_ref: Option<PyObjRef> = if bisect_mode.as_deref() == Some("skip_build") {
            // Hand the handler Py_None instead of a built request.
            Some(PyObjRef::from_borrowed(ffi::Py_None()).unwrap())
        } else {
            Some(
                PyObjRef::from_owned(Self::build_request(
                    py, method, path, params, query, body, headers, client_ip,
                )?)
                .ok_or("build_request returned null")?,
            )
        };
        let request = request_ref.unwrap();
        let request_ptr = request.as_ptr();

        if bisect_mode.as_deref() == Some("skip_handler") {
            // Drop request (triggers tp_dealloc) and return a fixed
            // response — skips hooks, Vectorcall, parse_result.
            drop(request);
            return Ok(bisect_response());
        }

        // Run before_request hooks
        for &hook_func in &self.before_hooks {
            let hook_args =
                PyObjRef::from_owned(ffi::PyTuple_New(1)).ok_or("failed to create hook args")?;
            ffi::Py_INCREF(request_ptr);
            ffi::PyTuple_SetItem(hook_args.as_ptr(), 0, request_ptr);

            let hook_result = PyObjRef::from_owned(ffi::PyObject_Call(
                hook_func,
                hook_args.as_ptr(),
                std::ptr::null_mut(),
            ));

            match hook_result {
                Some(r) => {
                    // Drive async hooks through the event loop so
                    // `async def` middleware doesn't leak a bare
                    // coroutine object as a "short-circuit response".
                    let resolved = self.resolve_coroutine(r)?;
                    if resolved.as_ptr() != ffi::Py_None() {
                        return worker_response(py, resolved);
                    }
                }
                None => {
                    // Hook raised an exception. We previously logged
                    // with PyErr_Print and fell through to the main
                    // handler — a critical bypass for auth / ACL hooks
                    // that signal denial by raising. Return an error
                    // so the caller serves 500 instead of the
                    // unprotected handler output.
                    log_and_clear_py_exception("before_request hook");
                    let hook_name = callable_name(hook_func);
                    return Err(format!(
                        "before_request hook {hook_name:?} raised an exception"
                    ));
                }
            }
        }

        // Call handler(request). We don't own a ref to request_ptr
        // (worker struct does) — pass it through directly.
        let args_arr = [request_ptr];
        let result_obj = PyObjRef::from_owned(ffi::PyObject_Vectorcall(
            func,
            args_arr.as_ptr(),
            1,
            std::ptr::null_mut(),
        ));

        let mut response = match result_obj {
            Some(r) => {
                let resolved = self.resolve_coroutine(r)?;
                worker_response(py, resolved)?
            }
            None => {
                // req_for_hooks dropped here automatically → DECREF
                log_and_clear_py_exception("sub-interp handler");
                return Err("handler raised an exception".to_string());
            }
        };

        // Run after_request hooks: hook(request, response) → response. One that raises
        // fails the request, as on the main interpreter.
        for &hook_func in &self.after_hooks {
            let resp_obj = PyObjRef::from_owned(
                response
                    .to_py(py)
                    .map_err(|e| format!("failed to create Response: {e}"))?
                    .into_ptr(),
            )
            .ok_or("Response object is null")?;

            let hook_args =
                PyObjRef::from_owned(ffi::PyTuple_New(2)).ok_or("failed to create hook args")?;
            ffi::Py_INCREF(request_ptr);
            ffi::PyTuple_SetItem(hook_args.as_ptr(), 0, request_ptr);
            ffi::PyTuple_SetItem(hook_args.as_ptr(), 1, resp_obj.into_raw());

            let hook_result = PyObjRef::from_owned(ffi::PyObject_Call(
                hook_func,
                hook_args.as_ptr(),
                std::ptr::null_mut(),
            ));

            match hook_result {
                Some(r) => {
                    // Drive async after_hooks through the event loop.
                    let resolved = self.resolve_coroutine(r)?;
                    if resolved.as_ptr() != ffi::Py_None() {
                        response = worker_response(py, resolved)?;
                    }
                }
                None => {
                    log_and_clear_py_exception("after_request hook");
                    let hook_name = callable_name(hook_func);
                    return Err(format!(
                        "after_request hook {hook_name:?} raised an exception"
                    ));
                }
            }
        }

        Ok(response)
    }
}

// ---------------------------------------------------------------------------
// Worker init helpers
// ---------------------------------------------------------------------------

/// A new module named `name`, registered in `sys.modules`, with builtins, `__file__` and,
/// for the bootstrap and async engine, `WORKER_ID` / `POOL_ID`.
///
/// # Safety
/// The interpreter the module belongs to must have its thread state current.
unsafe fn new_module(
    name: &CStr,
    file: Option<&str>,
    ids: Option<(usize, u64)>,
) -> Result<PyObjRef, String> {
    let module = PyObjRef::from_owned(ffi::PyModule_New(name.as_ptr())).ok_or_else(|| {
        log_and_clear_py_exception("PyModule_New");
        format!("failed to create module {name:?}")
    })?;
    let dict = ffi::PyModule_GetDict(module.as_ptr()); // borrowed
    let set = |key: &CStr, value: *mut ffi::PyObject| -> Result<(), String> {
        if value.is_null() || ffi::PyDict_SetItemString(dict, key.as_ptr(), value) != 0 {
            log_and_clear_py_exception("module setup");
            return Err(format!("failed to set {key:?} on module {name:?}"));
        }
        Ok(())
    };
    set(c"__builtins__", ffi::PyEval_GetBuiltins())?;
    if let Some(path) = file {
        let py_file = py_str(path).ok_or("failed to create __file__ str")?;
        set(c"__file__", py_file.as_ptr())?;
    }
    if let Some((worker_id, pool_id)) = ids {
        let wid = PyObjRef::from_owned(ffi::PyLong_FromSize_t(worker_id)).ok_or("WORKER_ID")?;
        set(c"WORKER_ID", wid.as_ptr())?;
        let pid =
            PyObjRef::from_owned(ffi::PyLong_FromUnsignedLongLong(pool_id)).ok_or("POOL_ID")?;
        set(c"POOL_ID", pid.as_ptr())?;
    }
    let modules = ffi::PyImport_GetModuleDict(); // borrowed
    if ffi::PyDict_SetItemString(modules, name.as_ptr(), module.as_ptr()) != 0 {
        log_and_clear_py_exception("sys.modules registration");
        return Err(format!("failed to register {name:?} in sys.modules"));
    }
    Ok(module)
}

/// Compiles `src` as `filename` and executes it in `module`'s namespace. On an exception,
/// prints its traceback (to the process's stderr) and returns its text.
///
/// # Safety
/// The interpreter `module` belongs to must have its thread state current.
unsafe fn exec_in(
    src: &str,
    filename: &str,
    module: &PyObjRef,
    worker_id: usize,
) -> Result<(), String> {
    let src_c = CString::new(src).map_err(|e| format!("{filename}: {e}"))?;
    let file_c = CString::new(filename).map_err(|e| format!("{filename}: {e}"))?;
    let dict = ffi::PyModule_GetDict(module.as_ptr()); // borrowed
    let code = PyObjRef::from_owned(ffi::Py_CompileString(
        src_c.as_ptr(),
        file_c.as_ptr(),
        ffi::Py_file_input,
    ));
    let result = match code {
        Some(code) => PyObjRef::from_owned(ffi::PyEval_EvalCode(code.as_ptr(), dict, dict)),
        None => None,
    };
    if result.is_some() {
        return Ok(());
    }
    // Keep the exception's own text for the startup error, and print the full traceback.
    let exc = ffi::PyErr_GetRaisedException();
    let text = if exc.is_null() {
        "no exception set".to_string()
    } else {
        let s = PyObjRef::from_owned(ffi::PyObject_Str(exc));
        let ty = ffi::Py_TYPE(exc);
        let ty_name = if ty.is_null() {
            String::new()
        } else {
            CStr::from_ptr((*ty).tp_name).to_string_lossy().into_owned()
        };
        let msg = s
            .and_then(|s| pyobj_to_string(s.as_ptr()).ok())
            .unwrap_or_default();
        ffi::PyErr_Clear();
        ffi::PyErr_SetRaisedException(exc);
        ffi::PyErr_Print();
        format!("{ty_name}: {msg}")
    };
    Err(format!(
        "worker {worker_id}: {filename} raised while the worker started: {text}"
    ))
}

/// A `Request` for one incoming request; shared by the sync worker path and the async
/// engine's `_worker_recv`.
pub(crate) fn new_request(
    method: &str,
    path: &str,
    params: Vec<(String, String)>,
    query: &str,
    body: bytes::Bytes,
    headers: hyper::HeaderMap,
    client_ip: std::net::IpAddr,
) -> crate::types::PyronovaRequest {
    crate::types::PyronovaRequest {
        method: std::sync::Arc::from(method),
        path: std::sync::Arc::from(path),
        params,
        query: query.to_string(),
        headers,
        client_ip_addr: client_ip,
        body_bytes: body,
        body_stream_rx: std::sync::Arc::new(std::sync::Mutex::new(None)),
        query_cache: std::sync::OnceLock::new(),
        query_all_cache: std::sync::OnceLock::new(),
    }
}

// ---------------------------------------------------------------------------
// Handler result → response (shared by the sync path and the async engine)
// ---------------------------------------------------------------------------

/// A worker handler's (or hook's) return value as a response: the one mapping every
/// interpreter uses (`response::extract_response_data`), except that a worker can't
/// stream, so a `Stream` is an error (streaming needs `gil=True, stream=True`, FR-16).
///
/// # Safety
/// Must be called with the GIL of the interpreter `result_obj` belongs to.
pub(crate) unsafe fn worker_response(
    py: Python<'_>,
    result_obj: PyObjRef,
) -> Result<ResponseData, String> {
    let value = Bound::from_owned_ptr(py, result_obj.into_raw());
    if value.is_instance_of::<crate::python::stream::PyronovaStream>() {
        let msg = "a sub-interpreter handler returned a Stream; streaming responses need \
                   gil=True, stream=True on the route";
        tracing::error!(target: "pyronova::handler", "{msg}");
        return Err(msg.to_string());
    }
    crate::response::extract_response_data(py, value)
}

/// The fixed response of the leak-hunt bisection modes.
fn bisect_response() -> ResponseData {
    ResponseData {
        body: bytes::Bytes::from_static(b"ok"),
        content_type: hyper::header::HeaderValue::from_static("text/plain; charset=utf-8"),
        status: 200,
        headers: crate::types::ResponseHeaders::new(),
    }
}

/// A callable's `__name__`, for error messages.
unsafe fn callable_name(obj: *mut ffi::PyObject) -> String {
    let name = PyObjRef::from_owned(ffi::PyObject_GetAttrString(obj, c"__name__".as_ptr()));
    match name.and_then(|n| pyobj_to_string(n.as_ptr()).ok()) {
        Some(n) => n,
        None => {
            ffi::PyErr_Clear();
            "<unnamed>".to_string()
        }
    }
}
