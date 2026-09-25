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

use pyo3::exceptions::PyImportError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyString;

use super::ffi::*;
use super::request_context::{in_request_context, RequestContext, Returned};
use crate::handlers::error::{catch_panic, HandlerError, PyException, Stage};
use crate::response::ResponseError;
use crate::router::{RouteId, RouteSignature};
use crate::types::{PyronovaRequest, ResponseData};

/// Name of the module the user's script executes as in a worker. Not `__main__`, so a script's
/// `if __name__ == "__main__": app.run()` does not run in workers.
const SCRIPT_MODULE: &CStr = c"__pyronova_worker__";
/// Name of the module the bootstrap executes as.
const BOOTSTRAP_MODULE: &CStr = c"__pyronova_bootstrap__";
/// Name of the module the async engine executes as.
const ASYNC_ENGINE_MODULE: &CStr = c"__pyronova_async_engine__";

// ---------------------------------------------------------------------------
// Start errors
// ---------------------------------------------------------------------------

/// `Py_NewInterpreterFromConfig` refused, with what its `PyStatus` said.
#[derive(Debug, thiserror::Error)]
#[error(
    "Py_NewInterpreterFromConfig failed{}: {}",
    .func.as_deref().map(|f| format!(" in {f}")).unwrap_or_default(),
    .message.as_deref().unwrap_or("it returned no error message and no thread state")
)]
pub(crate) struct NewInterpreterError {
    func: Option<String>,
    message: Option<String>,
}

impl NewInterpreterError {
    /// What `status` reports.
    ///
    /// # Safety
    /// `status.func` and `status.err_msg` are NULL or point to NUL-terminated strings (as
    /// CPython's `PyStatus` guarantees).
    unsafe fn from_status(status: &ffi::PyStatus) -> Self {
        let text = |p: *const std::ffi::c_char| {
            (!p.is_null()).then(|| CStr::from_ptr(p).to_string_lossy().into_owned())
        };
        NewInterpreterError {
            func: text(status.func),
            message: text(status.err_msg),
        }
    }
}

/// Why a worker could not start.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WorkerStartError {
    #[error(transparent)]
    NewInterpreter(#[from] NewInterpreterError),
    #[error(transparent)]
    State(#[from] crate::state::StateError),
    #[error(
        "worker {worker}: {file} raised while the worker started: {exception}\n{}",
        .exception.traceback()
    )]
    Script {
        worker: usize,
        file: String,
        exception: PyException,
    },
    /// The user's script raised `ImportError` in the worker. A worker executes the script
    /// as a module of its own, outside the package it belongs to on main, so a relative
    /// import there fails with "attempted relative import with no known parent package".
    #[error(
        "worker {worker}: {file} raised while the worker started: {exception}\n{}\
         (A worker executes {file} as a module of its own, `__pyronova_worker__`, outside \
         any package, so a relative import in it (`from . import x`, `from .models import \
         X`) cannot resolve there. Import by absolute name (`from mypkg.models import X`), \
         with the package importable from sys.path.)",
        .exception.traceback()
    )]
    ScriptImport {
        worker: usize,
        file: String,
        exception: PyException,
    },
    #[error("worker {worker}: {step} failed: {exception}\n{}", .exception.traceback())]
    Setup {
        worker: usize,
        step: &'static str,
        exception: PyException,
    },
    #[error("worker {worker}: {file} can't be compiled: it contains a NUL byte ({source})")]
    Nul {
        worker: usize,
        file: String,
        source: std::ffi::NulError,
    },
    #[error(
        "worker {worker}: the script registered a different route table than the main \
         interpreter (a script must register the same routes in every interpreter; routes \
         that exist only on main are registered after app.run() starts and must be \
         gil=True).\n{mismatch}"
    )]
    RouteMismatch { worker: usize, mismatch: String },
    #[error(
        "worker {worker}: the script registered no routes in the worker, but the main \
         interpreter has {expected} route(s). A worker executes the whole script and serves \
         the app it registers routes on; don't register routes only in the main interpreter."
    )]
    NoRoutes { worker: usize, expected: usize },
}

impl WorkerStartError {
    /// The bootstrap, which ships with the engine, did not run.
    fn bootstrap(worker: usize, file: &str, error: ExecError) -> Self {
        match error {
            ExecError::Nul(source) => WorkerStartError::Nul {
                worker,
                file: file.to_string(),
                source,
            },
            ExecError::Import(exception) | ExecError::Raised(exception) => {
                WorkerStartError::Script {
                    worker,
                    file: file.to_string(),
                    exception,
                }
            }
        }
    }

    /// The user's script did not run.
    fn script(worker: usize, file: &str, error: ExecError) -> Self {
        match error {
            ExecError::Import(exception) => WorkerStartError::ScriptImport {
                worker,
                file: file.to_string(),
                exception,
            },
            other => Self::bootstrap(worker, file, other),
        }
    }
}

/// Why a worker's async engine stopped serving. The engine runs for the worker's whole
/// life, so an exception it ends with happened at run time, not while the worker started.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AsyncEngineError {
    #[error("worker {worker}: creating the async engine module failed: {exception}\n{}", .exception.traceback())]
    Setup {
        worker: usize,
        exception: PyException,
    },
    #[error("worker {worker}: the async engine source contains a NUL byte ({source})")]
    Nul {
        worker: usize,
        source: std::ffi::NulError,
    },
    #[error("worker {worker}: the async engine stopped: {exception}\n{}", .exception.traceback())]
    Stopped {
        worker: usize,
        exception: PyException,
    },
}

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
    /// This sub-interpreter's persistent asyncio event loop (owned).
    asyncio_loop: *mut ffi::PyObject,
    /// Its `run_until_complete` method (owned).
    loop_run_func: *mut ffi::PyObject,
    /// Pool instance id (see `POOL_ID_COUNTER`). Exposed to the async
    /// engine as `POOL_ID` so it can be passed into every
    /// `_worker_recv` / `_worker_send` call for the zombie-worker guard.
    pub(crate) pool_id: u64,
    /// This interpreter's `gc.collect` (owned). `_bootstrap.py` runs
    /// `gc.disable()` at sub-interp init so CPython's threshold-based
    /// automatic triggers never fire. Instead we call this manually at
    /// a request-count cadence (see `gc_threshold`) and, in TPC idle mode,
    /// when the worker's thread goes quiet, pushing all cycle-collection
    /// work off the hot path and into slots between requests.
    gc_collect_func: *mut ffi::PyObject,
    /// Collect once this many requests ran since the last collect; 0 disables the
    /// count trigger (use when you've verified your handler graph creates no cycles —
    /// ref-counting handles everything else instantly). Set at build from the GC config
    /// (`GcConfig::count_trigger`): the threshold in count mode, the OOM failsafe in idle
    /// mode, 0 in off mode.
    gc_threshold: u64,
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

/// The program every worker of one server runs, read on main when the workers are built.
pub(crate) struct WorkerProgram {
    pub(crate) script_path: String,
    /// The app's script (`script_path`'s text), which each worker executes.
    pub(crate) script: String,
    /// Main's `sys.path`. A new interpreter's own `sys.path` is only what the process
    /// started with; whatever main added since (the CLI's working directory, a test
    /// runner's root, the program's own inserts) would be missing, and the script's
    /// imports would not resolve as they do on main.
    pub(crate) import_path: Vec<String>,
}

impl WorkerProgram {
    /// Reads `script_path` and main's `sys.path` now. An unreadable script, or a
    /// `sys.path` entry that is not a `str`, is an error.
    pub(crate) fn read(py: Python<'_>, script_path: String) -> PyResult<Self> {
        let script = std::fs::read_to_string(&script_path).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("read script '{script_path}': {e}"))
        })?;
        let import_path = py.import("sys")?.getattr("path")?.extract()?;
        Ok(WorkerProgram {
            script_path,
            script,
            import_path,
        })
    }
}

/// What every worker of one server is built from.
#[derive(Clone, Copy)]
pub(crate) struct WorkerSpec<'a> {
    pub(crate) program: &'a WorkerProgram,
    /// The routes the script must register: main's table up to its seal.
    pub(crate) expected: &'a RouteSignature,
    /// See `SubInterpreterWorker::pool_id`.
    pub(crate) pool_id: u64,
    pub(crate) shared_state: &'a crate::state::SharedMap,
    /// See `SubInterpreterWorker::gc_threshold`.
    pub(crate) gc_threshold: u64,
    /// The served app's limits; a worker whose script set others says they are ignored.
    pub(crate) limits: crate::site::Limits,
}

/// What a worker's interpreter holds for serving, built by its init.
struct Serving<'py> {
    handlers: Vec<Py<PyAny>>,
    before_hooks: Vec<Py<PyAny>>,
    after_hooks: Vec<Py<PyAny>>,
    asyncio_loop: Bound<'py, PyAny>,
    loop_run_func: Bound<'py, PyAny>,
    gc_collect_func: Bound<'py, PyAny>,
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
        spec: &WorkerSpec<'_>,
    ) -> Result<Self, WorkerStartError> {
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
            return Err(NewInterpreterError::from_status(&status).into());
        }

        // Past this point we own a live sub-interpreter. Any early error
        // must Py_EndInterpreter it before returning, or the sub-interp
        // (and the thread resources it pins) leak permanently. The half-built
        // worker's references are released inside `init_in_sub_interp`, while
        // this interpreter is still current.
        match Self::init_in_sub_interp(worker_id, spec) {
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
        spec: &WorkerSpec<'_>,
    ) -> Result<Self, WorkerStartError> {
        // NOT `Python::attach`: this runs on the main OS thread, whose gilstate thread
        // state is main's, while this sub-interpreter's thread state is current and holds
        // its GIL (`Py_NewInterpreterFromConfig` in `new`). Take the token for it directly.
        let py = Python::assume_attached();
        let serving = Self::prepare(py, worker_id, spec)?;

        let owned = |v: Vec<Py<PyAny>>| -> Vec<*mut ffi::PyObject> {
            v.into_iter().map(|h| h.into_ptr()).collect()
        };
        let mut worker = SubInterpreterWorker {
            tstate: std::ptr::null_mut(),
            worker_id,
            handlers: owned(serving.handlers),
            before_hooks: owned(serving.before_hooks),
            after_hooks: owned(serving.after_hooks),
            asyncio_loop: serving.asyncio_loop.into_ptr(),
            loop_run_func: serving.loop_run_func.into_ptr(),
            pool_id: spec.pool_id,
            gc_collect_func: serving.gc_collect_func.into_ptr(),
            gc_threshold: spec.gc_threshold,
            requests_served: 0,
            collected_at: 0,
            ended: false,
        };

        // Release this sub-interpreter's GIL. Outer `new()` swaps back to
        // the main interpreter after we return.
        worker.tstate = ffi::PyEval_SaveThread();
        Ok(worker)
    }

    /// The fallible part of the init, in this interpreter: the bootstrap, the script, the
    /// handlers it registered, the event loop, `gc.collect` and the JSON serializer. On an
    /// error everything built so far is released here, while this interpreter is current.
    ///
    /// # Safety
    /// This worker's sub-interpreter thread state is current (`py` is its token).
    unsafe fn prepare<'py>(
        py: Python<'py>,
        worker_id: usize,
        spec: &WorkerSpec<'_>,
    ) -> Result<Serving<'py>, WorkerStartError> {
        let WorkerSpec {
            program,
            expected,
            pool_id,
            shared_state,
            ..
        } = *spec;
        let WorkerProgram {
            script_path,
            script,
            import_path,
        } = program;
        let setup = |step: &'static str| {
            move |e: PyErr| WorkerStartError::Setup {
                worker: worker_id,
                step,
                exception: PyException::capture(py, &e),
            }
        };

        // Before the script runs: a `PyronovaApp` or `SharedState` it creates in this
        // interpreter must see the running app's map (Layer 2, C2 / FR-5).
        crate::state::hand_to_worker(py, shared_state)?;

        // Before anything is imported: the bootstrap and the script resolve their imports
        // as they do on main.
        pyo3::types::PyList::new(py, import_path)
            .and_then(|path| py.import("sys")?.setattr("path", path))
            .map_err(setup("setting sys.path to main's"))?;

        // 1. The bootstrap (logging bridge, GC policy, C-extension isolation) in its own
        //    namespace, with this worker's id (FR-20).
        let bootstrap = new_module(py, BOOTSTRAP_MODULE, None, Some((worker_id, pool_id)))
            .map_err(setup("creating the bootstrap module"))?;
        const BOOTSTRAP_FILE: &str = "pyronova/_bootstrap.py";
        exec_in(
            py,
            include_str!("../../python/pyronova/_bootstrap.py"),
            BOOTSTRAP_FILE,
            &bootstrap,
        )
        .map_err(|e| WorkerStartError::bootstrap(worker_id, BOOTSTRAP_FILE, e))?;

        // 2. The user's script as a real module, compiled with its own path so tracebacks
        //    point at it, `from __future__` imports work, and `typing.get_type_hints` finds
        //    the module in `sys.modules` (FR-20).
        let script_module = new_module(py, SCRIPT_MODULE, Some(script_path.as_str()), None)
            .map_err(setup("creating the script module"))?;
        exec_in(py, script, script_path, &script_module)
            .map_err(|e| WorkerStartError::script(worker_id, script_path, e))?;

        // 3. The persistent event loop async handlers and hooks run on.
        let asyncio_loop = new_event_loop(py).map_err(setup("creating the asyncio event loop"))?;
        let loop_run_func = asyncio_loop
            .getattr("run_until_complete")
            .map_err(setup("looking up loop.run_until_complete"))?;

        // 4. `gc.collect`, so the scheduled-GC path doesn't re-import per tick.
        //    `_bootstrap.py` has already called gc.disable().
        let gc_collect_func = py
            .import("gc")
            .and_then(|gc| gc.getattr("collect"))
            .map_err(setup("looking up gc.collect"))?;

        // 5. The JSON serializer (isojson, a hard dependency).
        crate::response::require_json(py).map_err(setup("loading the JSON serializer"))?;

        // 6. Last, nothing fallible after it: the handlers of the app the script registered
        //    routes on, index by index against main's table (FR-3, FR-4).
        let routes = match crate::app::worker_routes(py) {
            Some(r) if r.signature == *expected => r,
            Some(r) => {
                return Err(WorkerStartError::RouteMismatch {
                    worker: worker_id,
                    mismatch: RouteSignature::describe_mismatch(expected, &r.signature),
                })
            }
            None if expected.is_empty() => crate::app::WorkerRoutes::empty(),
            None => {
                return Err(WorkerStartError::NoRoutes {
                    worker: worker_id,
                    expected: expected.routes.len(),
                })
            }
        };
        for ignored in spec.limits.ignored_in_worker(&routes.limits) {
            tracing::warn!(target: "pyronova::server", worker = worker_id, "{ignored}");
        }

        Ok(Serving {
            handlers: routes.handlers,
            before_hooks: routes.before_hooks,
            after_hooks: routes.after_hooks,
            asyncio_loop,
            loop_run_func,
            gc_collect_func,
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

    /// Ends the worker a TPC thread served, on that thread, once its runtime and every task
    /// that used it are gone (FR-19). A thread forgotten past shutdown may run this after
    /// `Py_Finalize`; the worker is abandoned then.
    pub(crate) fn end_served(self) {
        if unsafe { ffi::Py_IsInitialized() } != 0 {
            // SAFETY: on the worker's own thread, no thread state current.
            unsafe { self.end() };
        } else {
            self.abandon();
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
    /// An exception it ends with is returned with its traceback.
    ///
    /// # Safety
    /// Must be called with this worker's thread state current.
    pub(crate) unsafe fn run_async_engine(&mut self) -> Result<(), AsyncEngineError> {
        let (worker, pool_id) = (self.worker_id, self.pool_id);
        self.attached(|_, py| {
            // Its own namespace, not the script's globals (M4 review N1d); `WORKER_ID` and
            // `POOL_ID` identify this worker's slot in `WORKER_STATES`.
            let engine = new_module(py, ASYNC_ENGINE_MODULE, None, Some((worker, pool_id)))
                .map_err(|e| AsyncEngineError::Setup {
                    worker,
                    exception: PyException::capture(py, &e),
                })?;
            exec_in(
                py,
                include_str!("../../python/pyronova/_async_engine.py"),
                "pyronova/_async_engine.py",
                &engine,
            )
            .map_err(|e| match e {
                ExecError::Nul(source) => AsyncEngineError::Nul { worker, source },
                ExecError::Import(exception) | ExecError::Raised(exception) => {
                    AsyncEngineError::Stopped { worker, exception }
                }
            })
        })
    }

    /// Runs `route` for `request` on this worker's interpreter: acquires its GIL, runs the
    /// hooks and the handler, releases the GIL. A panic comes back as
    /// [`HandlerError::Panic`] with its payload, the thread state put back all the same.
    ///
    /// # Safety
    /// On the thread `rebind_tstate_to_current_thread` bound this worker to, with no thread
    /// state current.
    pub(crate) unsafe fn serve(
        &mut self,
        route: RouteId,
        request: PyronovaRequest,
    ) -> Result<ResponseData, HandlerError> {
        // The guard writes the thread state back here even while a panic unwinds.
        let tstate_cell = std::cell::Cell::new(self.tstate);
        let result = catch_panic(|| {
            let _guard = SubInterpGilGuard::acquire(tstate_cell.get(), &tstate_cell);
            self.call_handler(route, request)
        });
        self.tstate = tstate_cell.get();
        result
    }

    /// Runs what a hook or handler returned to a value: an awaitable is driven to
    /// completion on this interpreter's persistent event loop, an `async def` coroutine in
    /// the request's own context (`rc`). A plain value is returned unchanged.
    ///
    /// An exception the awaitable raises is `stage`'s.
    ///
    /// # Safety
    /// Must be called with this sub-interpreter's GIL held.
    unsafe fn resolve_coroutine(
        &self,
        py: Python<'_>,
        rc: &RequestContext<'_>,
        obj: PyObjRef,
        stage: Stage,
    ) -> Result<PyObjRef, HandlerError> {
        let ptr = obj.as_ptr();
        match Returned::of(&Bound::from_borrowed_ptr(py, ptr)) {
            Returned::Value => Ok(obj),
            Returned::Coroutine => {
                let event_loop = Bound::from_borrowed_ptr(py, self.asyncio_loop);
                let coro = Bound::from_borrowed_ptr(py, ptr);
                let result = rc
                    .run_coroutine(&event_loop, &coro)
                    .map_err(|e| HandlerError::python(py, stage, &e))?;
                PyObjRef::from_owned(result.into_ptr()).ok_or_else(|| raised(py, stage))
            }
            // loop.run_until_complete(awaitable)
            Returned::OtherAwaitable => {
                PyObjRef::from_owned(ffi::PyObject_CallOneArg(self.loop_run_func, ptr))
                    .ok_or_else(|| raised(py, stage))
            }
        }
    }

    /// Runs the hooks and the handler for one request, in its own `contextvars.Context`.
    ///
    /// # Safety
    /// Must be called with this sub-interpreter's GIL held.
    unsafe fn call_handler(
        &mut self,
        route: RouteId,
        request: PyronovaRequest,
    ) -> Result<ResponseData, HandlerError> {
        self.attached(|worker, py| {
            worker.requests_served += 1;
            // The hooks and the handler share one fresh `contextvars.Context`.
            let response = in_request_context(py, |rc| {
                worker.call_handler_attached(py, rc, route.index(), request)
            })
            .unwrap_or_else(|e| Err(HandlerError::python(py, Stage::Setup, &e)));
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
        // SAFETY: attached (`py`); `gc_collect_func` is an owned reference to this
        // interpreter's `gc.collect`. `PyObject_CallNoArgs` skips the empty-tuple alloc.
        let collected = unsafe {
            Bound::from_owned_ptr_or_err(py, ffi::PyObject_CallNoArgs(self.gc_collect_func))
        };
        if let Err(err) = collected {
            let exception = PyException::capture(py, &err);
            tracing::error!(
                target: "pyronova::app",
                worker_id = self.worker_id,
                error = %exception,
                traceback = exception.traceback(),
                "gc.collect() raised; the worker keeps serving, but this signals OOM, heap \
                 corruption or interpreter damage"
            );
        }
    }

    unsafe fn call_handler_attached(
        &mut self,
        py: Python<'_>,
        rc: &RequestContext<'_>,
        handler_idx: usize,
        request: PyronovaRequest,
    ) -> Result<ResponseData, HandlerError> {
        // The worker's table was checked index by index against main's at init, and the
        // route came from main's table.
        let func = self.handlers[handler_idx];

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

        let request = if bisect_mode.as_deref() == Some("skip_build") {
            // Hand the handler Py_None instead of a built request.
            drop(request);
            PyObjRef::from_borrowed(ffi::Py_None()).ok_or_else(|| raised(py, Stage::Setup))?
        } else {
            let built =
                Py::new(py, request).map_err(|e| HandlerError::python(py, Stage::Setup, &e))?;
            PyObjRef::from_owned(built.into_ptr()).ok_or_else(|| raised(py, Stage::Setup))?
        };
        let request_ptr = request.as_ptr();

        if bisect_mode.as_deref() == Some("skip_handler") {
            // Drop request (triggers tp_dealloc) and return a fixed
            // response — skips hooks, Vectorcall, parse_result.
            drop(request);
            return Ok(bisect_response());
        }

        // Run before_request hooks. One that raises fails the request (500): running the
        // handler anyway would bypass an auth / ACL hook that denies by raising.
        for &hook_func in &self.before_hooks {
            let hook_result =
                PyObjRef::from_owned(ffi::PyObject_CallOneArg(hook_func, request_ptr))
                    .ok_or_else(|| raised(py, Stage::BeforeHook))?;
            // Drive async hooks through the event loop so `async def` middleware doesn't
            // leak a bare coroutine object as a "short-circuit response".
            let resolved = self.resolve_coroutine(py, rc, hook_result, Stage::BeforeHook)?;
            if resolved.as_ptr() != ffi::Py_None() {
                return Ok(worker_response(py, resolved)?);
            }
        }

        // Call handler(request).
        let args_arr = [request_ptr];
        let result_obj = PyObjRef::from_owned(ffi::PyObject_Vectorcall(
            func,
            args_arr.as_ptr(),
            1,
            std::ptr::null_mut(),
        ))
        .ok_or_else(|| raised(py, Stage::Handler))?;
        let resolved = self.resolve_coroutine(py, rc, result_obj, Stage::Handler)?;
        let mut response = worker_response(py, resolved)?;

        // Run after_request hooks: hook(request, response) → response. One that raises
        // fails the request, as on the main interpreter.
        for &hook_func in &self.after_hooks {
            let resp_obj = PyObjRef::from_owned(response.to_py(py)?.into_ptr())
                .ok_or_else(|| raised(py, Stage::AfterHook))?;
            let args = [request_ptr, resp_obj.as_ptr()];
            let hook_result = PyObjRef::from_owned(ffi::PyObject_Vectorcall(
                hook_func,
                args.as_ptr(),
                2,
                std::ptr::null_mut(),
            ))
            .ok_or_else(|| raised(py, Stage::AfterHook))?;
            // Drive async after_hooks through the event loop.
            let resolved = self.resolve_coroutine(py, rc, hook_result, Stage::AfterHook)?;
            if resolved.as_ptr() != ffi::Py_None() {
                response = worker_response(py, resolved)?;
            }
        }

        Ok(response)
    }
}

/// The exception a C-API call that returned NULL left pending, as `stage`'s error.
fn raised(py: Python<'_>, stage: Stage) -> HandlerError {
    HandlerError::Python {
        stage,
        exception: PyException::fetch(py),
    }
}

/// A new asyncio event loop, set as this interpreter's current one.
fn new_event_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let asyncio = py.import("asyncio")?;
    let event_loop = asyncio.call_method0("new_event_loop")?;
    asyncio.call_method1("set_event_loop", (&event_loop,))?;
    Ok(event_loop)
}

// ---------------------------------------------------------------------------
// Worker init helpers
// ---------------------------------------------------------------------------

/// A new module named `name`, registered in `sys.modules`, with builtins, `__file__` and,
/// for the bootstrap and async engine, `WORKER_ID` / `POOL_ID`.
fn new_module<'py>(
    py: Python<'py>,
    name: &CStr,
    file: Option<&str>,
    ids: Option<(usize, u64)>,
) -> PyResult<Bound<'py, pyo3::types::PyModule>> {
    let module = pyo3::types::PyModule::new(py, &name.to_string_lossy())?;
    // SAFETY: attached (`py`); `PyEval_GetBuiltins` returns a borrowed reference to the
    // current frame's (or the interpreter's) builtins dict, never NULL.
    let builtins = unsafe { Bound::from_borrowed_ptr(py, ffi::PyEval_GetBuiltins()) };
    module.setattr("__builtins__", builtins)?;
    if let Some(path) = file {
        module.setattr("__file__", PyString::new(py, path))?;
    }
    if let Some((worker_id, pool_id)) = ids {
        module.setattr("WORKER_ID", worker_id)?;
        module.setattr("POOL_ID", pool_id)?;
    }
    py.import("sys")?
        .getattr("modules")?
        .set_item(module.name()?, &module)?;
    Ok(module)
}

/// Why a module's source did not run.
#[derive(Debug)]
enum ExecError {
    /// The source or its file name contains a NUL byte, so it can't be compiled.
    Nul(std::ffi::NulError),
    /// It raised `ImportError` itself (not a subclass such as `ModuleNotFoundError`).
    Import(PyException),
    /// It raised, with this exception.
    Raised(PyException),
}

/// Compiles `src` as `filename` and executes it in `module`'s namespace. An exception is
/// returned with its text and traceback (nothing is printed).
fn exec_in(
    py: Python<'_>,
    src: &str,
    filename: &str,
    module: &Bound<'_, pyo3::types::PyModule>,
) -> Result<(), ExecError> {
    let src_c = CString::new(src).map_err(ExecError::Nul)?;
    let file_c = CString::new(filename).map_err(ExecError::Nul)?;
    let dict = module.dict();
    // SAFETY: attached (`py`); the strings are NUL-terminated; `dict` is a live dict.
    // `Py_CompileString` and `PyEval_EvalCode` return a new reference or NULL with an
    // exception set.
    let ran = unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            ffi::Py_CompileString(src_c.as_ptr(), file_c.as_ptr(), ffi::Py_file_input),
        )
        .and_then(|code| {
            Bound::from_owned_ptr_or_err(
                py,
                ffi::PyEval_EvalCode(code.as_ptr(), dict.as_ptr(), dict.as_ptr()),
            )
        })
    };
    ran.map(drop).map_err(|e| {
        let exception = PyException::capture(py, &e);
        if e.get_type(py).is(py.get_type::<PyImportError>()) {
            ExecError::Import(exception)
        } else {
            ExecError::Raised(exception)
        }
    })
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
) -> Result<ResponseData, ResponseError> {
    let value = Bound::from_owned_ptr(py, result_obj.into_raw());
    if value.is_instance_of::<crate::python::stream::PyronovaStream>() {
        return Err(ResponseError::StreamInWorker);
    }
    crate::response::extract_response_data(py, value)
}

/// The fixed response of the leak-hunt bisection modes.
fn bisect_response() -> ResponseData {
    ResponseData {
        body: bytes::Bytes::from_static(b"ok"),
        content_type: hyper::header::HeaderValue::from_static("text/plain; charset=utf-8"),
        status: hyper::StatusCode::OK,
        headers: crate::types::ResponseHeaders::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refused_interpreter_reports_its_pystatus() {
        let status = ffi::PyStatus {
            _type: ffi::_PyStatus_TYPE::_PyStatus_TYPE_ERROR,
            func: c"init_interp_create_gil".as_ptr(),
            err_msg: c"failed to create a new GIL".as_ptr(),
            exitcode: 0,
        };
        // SAFETY: both strings are NUL-terminated literals.
        let err = unsafe { NewInterpreterError::from_status(&status) };
        assert_eq!(
            err.to_string(),
            "Py_NewInterpreterFromConfig failed in init_interp_create_gil: failed to create \
             a new GIL"
        );
    }

    #[test]
    fn a_status_without_text_says_so() {
        let status = ffi::PyStatus {
            _type: ffi::_PyStatus_TYPE::_PyStatus_TYPE_OK,
            func: std::ptr::null(),
            err_msg: std::ptr::null(),
            exitcode: 0,
        };
        // SAFETY: NULL pointers are allowed.
        let err = unsafe { NewInterpreterError::from_status(&status) };
        assert_eq!(
            err.to_string(),
            "Py_NewInterpreterFromConfig failed: it returned no error message and no thread \
             state"
        );
    }
}
