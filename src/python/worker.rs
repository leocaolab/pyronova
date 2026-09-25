//! `SubInterpreterWorker` — owns one CPython sub-interpreter and runs
//! request handlers inside it. This is the densest concentration of
//! `unsafe` + raw `pyo3::ffi` in the codebase.
//!
//! A worker runs the same program as the main interpreter (Layer 2): its bootstrap sets up
//! logging, the GC policy and C-extension isolation, then the user's script executes as a
//! real module and imports the real `pyronova` package and engine. The worker takes its
//! handlers from the app that script registered routes on, checked index by index against
//! the main interpreter's table.

use std::collections::HashMap;
use std::ffi::{CStr, CString};

use pyo3::ffi;
use pyo3::prelude::*;

use super::convert::*;
use super::ffi::*;
use super::pool::*;
use crate::router::RouteSignature;

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
    /// a request-count cadence (see `gc_threshold` + `gc_counter`),
    /// pushing all cycle-collection work off the hot path and into
    /// deterministic slots between requests.
    pub(crate) gc_collect_func: *mut ffi::PyObject,
    /// Trigger interval in requests. 0 disables scheduled collection
    /// entirely (use when you've verified your handler graph creates no
    /// cycles — ref-counting handles everything else instantly).
    /// Default 5000, overridable via `PYRONOVA_GC_THRESHOLD=N`.
    pub(crate) gc_threshold: u64,
    /// Request counter for the GC scheduler. Incremented at the end of
    /// each `call_handler`; every `gc_threshold` ticks we invoke
    /// `gc.collect()`. Per-worker = per-thread, so no atomics needed.
    gc_counter: u64,
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
            gc_counter: 0,
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
        body: &[u8],
        headers: &HashMap<String, String>,
        client_ip: std::net::IpAddr,
    ) -> Result<*mut ffi::PyObject, String> {
        // `headers` is already converted to a HashMap (the Tokio side
        // extracted the hyper HeaderMap off the worker thread), so use the
        // pre-converted variant. params → dict and body → bytes are still
        // materialized lazily by the pyclass getters, so a handler that
        // never touches `.params` / `.headers` / `.body` pays nothing for
        // them — same laziness the old raw type had, minus the hand-written
        // FFI.
        let req = new_request(
            method,
            path,
            params.to_vec(),
            query,
            bytes::Bytes::copy_from_slice(body),
            headers.clone(),
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
        handler_idx: usize,
        method: &str,
        path: &str,
        params: &[(String, String)],
        query: &str,
        body: &[u8],
        headers: &HashMap<String, String>,
        client_ip: std::net::IpAddr,
    ) -> Result<SubInterpResponse, String> {
        // Re-entrant: this worker's thread state is current (SubInterpGilGuard), and it is
        // the thread's gilstate one (`rebind_tstate_to_current_thread`), so this registers
        // the attach with PyO3 without switching thread states.
        Python::attach(|py| {
            self.call_handler_attached(
                py,
                handler_idx,
                method,
                path,
                params,
                query,
                body,
                headers,
                client_ip,
            )
        })
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
        body: &[u8],
        headers: &HashMap<String, String>,
        client_ip: std::net::IpAddr,
    ) -> Result<SubInterpResponse, String> {
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
        //                    fixed SubInterpResponse. Exercises hyper +
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
            return Ok(SubInterpResponse {
                body: b"ok".to_vec(),
                status: 200,
                content_type: None,
                headers: Vec::new(),
                is_json: false,
            });
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
            return Ok(SubInterpResponse {
                body: b"ok".to_vec(),
                status: 200,
                content_type: None,
                headers: Vec::new(),
                is_json: false,
            });
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
                        return parse_result(py, resolved);
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
                parse_result(py, resolved)?
            }
            None => {
                // req_for_hooks dropped here automatically → DECREF
                log_and_clear_py_exception("sub-interp handler");
                return Err("handler raised an exception".to_string());
            }
        };

        // Run after_request hooks: hook(request, response) → response.
        for &hook_func in &self.after_hooks {
            // Build a Response from the current response
            let resp_obj = build_response(py, &response)?;

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
                        response = parse_result(py, resolved)?;
                    }
                }
                None => {
                    log_and_clear_py_exception("after_request hook");
                }
            }
        }

        // Smart GC: count requests and trigger `gc.collect()` at the
        // configured interval. gc.disable() was called at sub-interp
        // init (see _bootstrap.py) so this is the only cycle collector
        // running — Python's threshold-based auto-trigger never fires.
        // Cost per request is a single u64 increment + compare; the
        // collect itself fires at most once per `gc_threshold` calls
        // and runs under the GIL we already hold.
        if self.gc_threshold > 0 && !self.gc_collect_func.is_null() {
            self.gc_counter = self.gc_counter.wrapping_add(1);
            if self.gc_counter.is_multiple_of(self.gc_threshold) {
                // `PyObject_CallNoArgs` skips the empty-tuple alloc that
                // `PyObject_Call` would require; saves a small per-tick
                // cost and is the idiomatic 3.9+ invocation. `gc.collect()`
                // with no args = full 3-generation collection — cheap
                // when there are few cycles, which is the common case
                // under our ref-count-first request lifecycle.
                let res = ffi::PyObject_CallNoArgs(self.gc_collect_func);
                if !res.is_null() {
                    ffi::Py_DECREF(res);
                } else {
                    // Clear any exception raised during the collect so
                    // we don't leak it into the handler's return path
                    // (handler already succeeded).
                    ffi::PyErr_Clear();
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
    headers: HashMap<String, String>,
    client_ip: std::net::IpAddr,
) -> crate::types::PyronovaRequest {
    use crate::types::{LazyHeaders, PyronovaRequest};
    PyronovaRequest {
        method: std::sync::Arc::from(method),
        path: std::sync::Arc::from(path),
        params,
        query: query.to_string(),
        headers_source: LazyHeaders::Converted(headers),
        headers_cache: std::sync::OnceLock::new(),
        client_ip_addr: client_ip,
        body_bytes: body,
        body_stream_rx: std::sync::Arc::new(std::sync::Mutex::new(None)),
        query_cache: std::sync::OnceLock::new(),
        query_all_cache: std::sync::OnceLock::new(),
    }
}

// ---------------------------------------------------------------------------
// Handler result ↔ response (shared by the sync path and the async engine)
// ---------------------------------------------------------------------------

/// `isojson.dumps` (orjson-compatible, and safe in own-GIL sub-interpreters), falling back
/// to `json.dumps`; one per interpreter. Not orjson: it keeps its types and strings in
/// process-global statics, so every sub-interpreter got the first one's objects, and ending
/// any sub-interpreter freed objects another one still used.
static JSON_DUMPS: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();

fn json_dumps_func(py: Python<'_>) -> Result<&Py<PyAny>, String> {
    JSON_DUMPS
        .get_or_try_init(py, || {
            py.import("isojson")
                .or_else(|_| py.import("json"))
                .and_then(|m| m.getattr("dumps"))
                .map(|f| f.unbind())
        })
        .map_err(|e| format!("no JSON serializer: {e}"))
}

/// Serialize a Python dict/list to a JSON string (isojson returns bytes, json a str).
unsafe fn json_dumps(py: Python<'_>, obj: PyObjRef) -> Result<String, String> {
    let dumps = json_dumps_func(py)?.as_ptr();
    let args = PyObjRef::from_owned(ffi::PyTuple_New(1)).ok_or("failed to create tuple")?;
    ffi::PyTuple_SetItem(args.as_ptr(), 0, obj.into_raw());

    let result = PyObjRef::from_owned(ffi::PyObject_Call(
        dumps,
        args.as_ptr(),
        std::ptr::null_mut(),
    ))
    .ok_or_else(|| {
        log_and_clear_py_exception("json.dumps");
        "json.dumps failed".to_string()
    })?;

    if ffi::PyBytes_Check(result.as_ptr()) != 0 {
        let ptr = ffi::PyBytes_AsString(result.as_ptr());
        let size = ffi::PyBytes_Size(result.as_ptr());
        // PyBytes_Size returns -1 on error (Py_ssize_t). Cast to
        // usize without checking would yield usize::MAX and feed
        // an enormous slice to from_raw_parts → UB (arc interp-2).
        if ptr.is_null() || size < 0 {
            if !ffi::PyErr_Occurred().is_null() {
                ffi::PyErr_Clear();
            }
            return Err("failed to extract bytes".to_string());
        }
        let bytes = std::slice::from_raw_parts(ptr as *const u8, size as usize);
        String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())
    } else {
        pyobj_to_string(result.as_ptr())
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

/// Copies a `bytes` object's contents.
unsafe fn bytes_contents(obj: *mut ffi::PyObject) -> Vec<u8> {
    let size = ffi::PyBytes_Size(obj);
    let ptr = ffi::PyBytes_AsString(obj);
    if !ptr.is_null() && size > 0 {
        std::slice::from_raw_parts(ptr as *const u8, size as usize).to_vec()
    } else {
        ffi::PyErr_Clear();
        Vec::new()
    }
}

/// Map a handler's return value to a response. The same mapping serves the sync worker
/// path and the async engine (`_worker_to_response` / `_worker_send`, M4 review N1b):
///
/// - a `Response` (or anything with `status_code` + `body`) → its fields;
/// - `dict` → JSON;
/// - `str` → its text (the HTTP layer labels a `{`/`[`-prefixed body JSON);
/// - `bytes` → the bytes, `application/octet-stream`;
/// - `None` → an empty 200;
/// - a `Stream` → an error: streaming responses need `gil=True, stream=True` (FR-16);
/// - anything else → `str(value)`.
///
/// # Safety
/// Must be called with the GIL of the interpreter `result_obj` belongs to.
pub(crate) unsafe fn parse_result(
    py: Python<'_>,
    result_obj: PyObjRef,
) -> Result<SubInterpResponse, String> {
    let ptr = result_obj.as_ptr();

    // Check if it's a Response or any response-like object
    // (duck typing: has status_code + body attributes).
    //
    // PyObject_IsInstance returns 1 (true), 0 (false), or -1 (error
    // with exception set). Treating -1 as false without clearing
    // the exception is a SystemError latent bomb — the next C-API
    // call short-circuits on the pending exception.
    let resp_cls = py.get_type::<crate::types::PyronovaResponse>();
    let is_response = match ffi::PyObject_IsInstance(ptr, resp_cls.as_ptr()) {
        1 => true,
        -1 => {
            ffi::PyErr_Clear();
            // Fall through to duck-type check.
            let has_status = ffi::PyObject_HasAttrString(ptr, c"status_code".as_ptr()) == 1;
            let has_body = ffi::PyObject_HasAttrString(ptr, c"body".as_ptr()) == 1;
            has_status && has_body
        }
        _ => {
            // 0 (not an instance) — try duck-typing.
            let has_status = ffi::PyObject_HasAttrString(ptr, c"status_code".as_ptr()) == 1;
            let has_body = ffi::PyObject_HasAttrString(ptr, c"body".as_ptr()) == 1;
            has_status && has_body
        }
    };
    if is_response {
        return parse_response(py, result_obj);
    }

    // A worker can't stream a response: the body would be `str(stream)`. Refuse loudly.
    let stream_cls = py.get_type::<crate::python::stream::PyronovaStream>();
    if ffi::PyObject_IsInstance(ptr, stream_cls.as_ptr()) == 1 {
        let msg = "a sub-interpreter handler returned a Stream; streaming responses need \
                   gil=True, stream=True on the route";
        tracing::error!(target: "pyronova::handler", "{msg}");
        return Err(msg.to_string());
    }
    ffi::PyErr_Clear();

    // dict → JSON
    if ffi::PyDict_Check(ptr) != 0 {
        let json_str = json_dumps(py, result_obj)?;
        return Ok(SubInterpResponse {
            body: json_str.into_bytes(),
            status: 200,
            content_type: None,
            headers: Vec::new(),
            is_json: true,
        });
    }

    // string
    if ffi::PyUnicode_Check(ptr) != 0 {
        let s = pyobj_to_string(ptr)?;
        return Ok(SubInterpResponse {
            body: s.into_bytes(),
            status: 200,
            content_type: None,
            headers: Vec::new(),
            is_json: false,
        });
    }

    // bytes → raw body
    if ffi::PyBytes_Check(ptr) != 0 {
        return Ok(SubInterpResponse {
            body: bytes_contents(ptr),
            status: 200,
            content_type: Some("application/octet-stream".to_string()),
            headers: Vec::new(),
            is_json: false,
        });
    }

    // None → empty 200
    if ptr == ffi::Py_None() {
        return Ok(SubInterpResponse {
            body: Vec::new(),
            status: 200,
            content_type: None,
            headers: Vec::new(),
            is_json: false,
        });
    }

    // fallback: str(result)
    let str_obj = PyObjRef::from_owned(ffi::PyObject_Str(ptr)).ok_or_else(|| {
        ffi::PyErr_Clear();
        "str() failed".to_string()
    })?;
    let s = pyobj_to_string(str_obj.as_ptr())?;
    Ok(SubInterpResponse {
        body: s.into_bytes(),
        status: 200,
        content_type: None,
        headers: Vec::new(),
        is_json: false,
    })
}

/// Build a `Response` Python object from a SubInterpResponse.
///
/// # Safety
/// Must be called with the GIL of the target interpreter.
pub(crate) unsafe fn build_response(
    py: Python<'_>,
    resp: &SubInterpResponse,
) -> Result<PyObjRef, String> {
    let resp_cls = py.get_type::<crate::types::PyronovaResponse>();

    // Convert body to Python object — use bytes for binary, str for text
    let py_body = if resp.is_json || std::str::from_utf8(&resp.body).is_ok() {
        let body_str = unsafe { std::str::from_utf8_unchecked(&resp.body) };
        py_str(body_str).ok_or("failed to create body str")?
    } else {
        // Binary data: use PyBytes to avoid UTF-8 corruption
        PyObjRef::from_owned(ffi::PyBytes_FromStringAndSize(
            resp.body.as_ptr() as *const _,
            resp.body.len() as isize,
        ))
        .ok_or("failed to create body bytes")?
    };
    let py_status = PyObjRef::from_owned(ffi::PyLong_FromLong(resp.status as i64))
        .ok_or("failed to create status")?;
    let py_ct = match &resp.content_type {
        Some(ct) => py_str(ct).ok_or("failed to create content_type")?,
        None => PyObjRef::from_borrowed(ffi::Py_None()).unwrap(),
    };
    let py_headers = py_str_dict_from_vec(&resp.headers).ok_or("failed to create headers dict")?;

    // Response(body, status_code, content_type, headers)
    let args = PyObjRef::from_owned(ffi::PyTuple_New(0)).ok_or("failed to create args")?;
    let kwargs = PyObjRef::from_owned(ffi::PyDict_New()).ok_or("failed to create kwargs")?;

    ffi::PyDict_SetItemString(kwargs.as_ptr(), c"body".as_ptr(), py_body.as_ptr());
    ffi::PyDict_SetItemString(kwargs.as_ptr(), c"status_code".as_ptr(), py_status.as_ptr());
    ffi::PyDict_SetItemString(kwargs.as_ptr(), c"content_type".as_ptr(), py_ct.as_ptr());
    ffi::PyDict_SetItemString(kwargs.as_ptr(), c"headers".as_ptr(), py_headers.as_ptr());

    PyObjRef::from_owned(ffi::PyObject_Call(
        resp_cls.as_ptr(),
        args.as_ptr(),
        kwargs.as_ptr(),
    ))
    .ok_or_else(|| {
        log_and_clear_py_exception("_Response construction");
        "failed to create _Response".to_string()
    })
}

/// Parse a `Response` (or duck-typed response) Python object.
unsafe fn parse_response(py: Python<'_>, obj: PyObjRef) -> Result<SubInterpResponse, String> {
    let ptr = obj.as_ptr();

    // status_code
    let status = {
        let attr = PyObjRef::from_owned(ffi::PyObject_GetAttrString(ptr, c"status_code".as_ptr()));
        match attr {
            Some(a) => {
                let code = ffi::PyLong_AsLong(a.as_ptr());
                if code == -1 && !ffi::PyErr_Occurred().is_null() {
                    ffi::PyErr_Clear();
                    200
                } else {
                    code as u16
                }
            }
            None => {
                ffi::PyErr_Clear();
                200
            }
        }
    };

    // content_type
    let content_type = {
        let attr = PyObjRef::from_owned(ffi::PyObject_GetAttrString(ptr, c"content_type".as_ptr()));
        match attr {
            Some(a) if a.as_ptr() != ffi::Py_None() => pyobj_to_string(a.as_ptr()).ok(),
            _ => {
                ffi::PyErr_Clear();
                None
            }
        }
    };

    // headers
    //
    // CRITICAL: PyDict_Next forbids dict mutation during iteration.
    // PyObject_Str may invoke user __str__ which could mutate the
    // dict → undefined behaviour / segfault. We collect borrowed
    // key/value refs first, INCREF them, then release the iteration
    // scope before calling any method that may re-enter Python.
    let mut resp_headers: Vec<(String, String)> = Vec::new();
    {
        let attr = PyObjRef::from_owned(ffi::PyObject_GetAttrString(ptr, c"headers".as_ptr()));
        if let Some(a) = &attr {
            if ffi::PyDict_Check(a.as_ptr()) != 0 {
                // Phase 1: snapshot (no user code runs).
                let mut snapshot: Vec<(PyObjRef, PyObjRef)> = Vec::new();
                let mut pos: isize = 0;
                let mut key: *mut ffi::PyObject = std::ptr::null_mut();
                let mut val: *mut ffi::PyObject = std::ptr::null_mut();
                while ffi::PyDict_Next(a.as_ptr(), &mut pos, &mut key, &mut val) != 0 {
                    // PyDict_Next returns borrowed refs — INCREF to own them.
                    if let (Some(k), Some(v)) =
                        (PyObjRef::from_borrowed(key), PyObjRef::from_borrowed(val))
                    {
                        snapshot.push((k, v));
                    }
                }
                // Phase 2: convert — safe to invoke __str__ now.
                for (k_obj, v_obj) in snapshot {
                    let str_key = PyObjRef::from_owned(ffi::PyObject_Str(k_obj.as_ptr()));
                    if let Some(sk) = str_key {
                        if let Ok(k) = pyobj_to_string(sk.as_ptr()) {
                            // Check if value is a Python list — e.g. multiple Set-Cookie values
                            if ffi::PyList_Check(v_obj.as_ptr()) != 0 {
                                // Phase 1: snapshot the list items (no user code
                                // runs). PyList_GetItem returns borrowed refs, and
                                // PyObject_Str below may invoke user __str__ which
                                // could mutate the list → invalidating the borrow.
                                // INCREF each item to own it before converting.
                                let n = ffi::PyList_Size(v_obj.as_ptr());
                                let mut items: Vec<PyObjRef> = Vec::new();
                                for i in 0..n {
                                    let item = ffi::PyList_GetItem(v_obj.as_ptr(), i);
                                    if item.is_null() {
                                        ffi::PyErr_Clear();
                                        continue;
                                    }
                                    if let Some(owned) = PyObjRef::from_borrowed(item) {
                                        items.push(owned);
                                    }
                                }
                                // Phase 2: convert — safe to invoke __str__ now.
                                for item in items {
                                    if let Some(item_str) =
                                        PyObjRef::from_owned(ffi::PyObject_Str(item.as_ptr()))
                                    {
                                        if let Ok(v) = pyobj_to_string(item_str.as_ptr()) {
                                            resp_headers.push((k.clone(), v));
                                        } else {
                                            ffi::PyErr_Clear();
                                        }
                                    } else {
                                        ffi::PyErr_Clear();
                                    }
                                }
                            } else {
                                let str_val =
                                    PyObjRef::from_owned(ffi::PyObject_Str(v_obj.as_ptr()));
                                if let Some(sv) = str_val {
                                    if let Ok(v) = pyobj_to_string(sv.as_ptr()) {
                                        resp_headers.push((k, v));
                                    } else {
                                        ffi::PyErr_Clear();
                                    }
                                } else {
                                    ffi::PyErr_Clear();
                                }
                            }
                        }
                    } else {
                        ffi::PyErr_Clear();
                    }
                }
            }
        }
        ffi::PyErr_Clear();
    }

    // body (returns Vec<u8>)
    let (body, is_json): (Vec<u8>, bool) = {
        let attr = PyObjRef::from_owned(ffi::PyObject_GetAttrString(ptr, c"body".as_ptr()));
        match attr {
            Some(a) => {
                if ffi::PyDict_Check(a.as_ptr()) != 0 {
                    match json_dumps(py, a) {
                        Ok(s) => (s.into_bytes(), true),
                        Err(e) => {
                            tracing::error!(
                                target: "pyronova::server",
                                error = %e,
                                "JSON serialization failed for response body dict"
                            );
                            let msg = format!(r#"{{"error":"json serialization failed: {}"}}"#, e);
                            return Ok(SubInterpResponse {
                                body: msg.into_bytes(),
                                status: 500,
                                content_type: Some("application/json".to_string()),
                                headers: resp_headers,
                                is_json: true,
                            });
                        }
                    }
                } else if ffi::PyBytes_Check(a.as_ptr()) != 0 {
                    // Raw bytes — pass through without UTF-8 conversion
                    (bytes_contents(a.as_ptr()), false)
                } else if ffi::PyUnicode_Check(a.as_ptr()) != 0 {
                    (
                        pyobj_to_string(a.as_ptr()).unwrap_or_default().into_bytes(),
                        false,
                    )
                } else {
                    let str_obj = PyObjRef::from_owned(ffi::PyObject_Str(a.as_ptr()));
                    match str_obj {
                        Some(s) => (
                            pyobj_to_string(s.as_ptr()).unwrap_or_default().into_bytes(),
                            false,
                        ),
                        None => {
                            ffi::PyErr_Clear();
                            (Vec::new(), false)
                        }
                    }
                }
            }
            None => {
                ffi::PyErr_Clear();
                (Vec::new(), false)
            }
        }
    };

    Ok(SubInterpResponse {
        body,
        status,
        content_type,
        headers: resp_headers,
        is_json,
    })
}
