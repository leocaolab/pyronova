//! `SubInterpreterWorker` — owns one CPython sub-interpreter and runs requests in it. This
//! is the densest concentration of `unsafe` + raw `pyo3::ffi` in the codebase: creating and
//! ending the interpreter, and moving its thread state between OS threads.
//!
//! A worker runs the same program as the main interpreter: its bootstrap sets up
//! logging, the GC policy and C-extension isolation, then the user's script executes as a
//! real module and imports the real `pyronova` package and engine. The worker takes its
//! handlers from the app that script registered routes on, checked index by index against
//! the main interpreter's table.
//!
//! What a worker holds for serving is its [`Role`]: an [`Inline`] worker (the TPC threads,
//! the pool's sync workers) runs the hook chain from Rust on its own event loop; an
//! [`AsyncEngine`] worker runs the async engine, which takes the handlers from the app
//! itself. The role's references belong to the worker's interpreter and are released only
//! by [`SubInterpreterWorker::end`], with its thread state current.

use std::cell::Cell;
use std::ffi::{CStr, CString};
use std::mem::ManuallyDrop;
use std::time::Duration;

use pyo3::exceptions::{PyImportError, PyModuleNotFoundError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyModule, PyString, PyTraceback, PyTuple};

use super::hook_chain::{worker_response, Chain, EventLoop};
use super::request_context::{in_request_context, RequestContext};
use super::worker_api::AsyncInbox;
use super::worker_app::{self, WorkerRoutes};
use crate::body::REQUEST_BUDGET;
use crate::error::{panic_message, HandlerError, PyException, Stage};
use crate::router::{RouteId, RouteSignature};
use crate::types::{PyronovaRequest, ResponseData};

/// Name of the module the user's script executes as in a worker. Not `__main__`, so a script's
/// `if __name__ == "__main__": app.run()` does not run in workers.
const SCRIPT_MODULE: &CStr = c"__pyronova_worker__";
/// Name of the module the bootstrap executes as.
const BOOTSTRAP_MODULE: &CStr = c"__pyronova_bootstrap__";
/// Name of the module the async engine executes as.
const ASYNC_ENGINE_MODULE: &CStr = c"__pyronova_async_engine__";

/// How much sooner than the caller the async engine gives up on a request's task: it
/// cancels the task and answers 504 itself, instead of leaving it computing a result the
/// caller, already answered, never takes.
const ASYNC_TASK_MARGIN: Duration = Duration::from_secs(2);
/// The async engine's budget per request task (its `TASK_TIMEOUT`).
const ASYNC_TASK_BUDGET: Duration = REQUEST_BUDGET.saturating_sub(ASYNC_TASK_MARGIN);

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

pyo3::create_exception!(
    pyronova.engine,
    WorkerException,
    pyo3::exceptions::PyException,
    "An exception a sub-interpreter worker raised, as the traceback it printed there: the \
     exception object itself belongs to the worker's interpreter. The `__cause__` of the \
     error a failed worker start raises on the main interpreter."
);

/// The exception classes the main interpreter re-raises a worker's start failure as;
/// anything else is a `RuntimeError` there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExceptionClass {
    ModuleNotFound,
    Import,
    Other,
}

impl ExceptionClass {
    fn of(py: Python<'_>, err: &PyErr) -> Self {
        if err.is_instance_of::<PyModuleNotFoundError>(py) {
            ExceptionClass::ModuleNotFound
        } else if err.is_instance_of::<PyImportError>(py) {
            ExceptionClass::Import
        } else {
            ExceptionClass::Other
        }
    }
}

/// A Python exception a worker raised while it started: its text (the object itself
/// belongs to the worker's interpreter) and its class, for the main interpreter to re-raise.
#[derive(Debug, thiserror::Error)]
#[error("{exception}")]
pub(crate) struct StartException {
    exception: PyException,
    class: ExceptionClass,
}

impl StartException {
    fn capture(py: Python<'_>, err: &PyErr) -> Self {
        StartException {
            exception: PyException::capture(py, err),
            class: ExceptionClass::of(py, err),
        }
    }
}

/// What a failed import in the user's script was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScriptFailure {
    /// A relative import (`from . import x`): a worker executes the script outside its
    /// package, where it can't resolve.
    RelativeImport,
    Other,
}

impl std::fmt::Display for ScriptFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScriptFailure::RelativeImport => f.write_str(
                "\n(A worker executes the script as a module of its own, \
                 `__pyronova_worker__`, outside any package, so a relative import in it \
                 (`from . import x`, `from .models import X`) cannot resolve there. Import by \
                 absolute name (`from mypkg.models import X`), with the package importable \
                 from sys.path.)",
            ),
            ScriptFailure::Other => Ok(()),
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
    #[error("worker {worker}: {file} raised while the worker started: {raised}{failure}")]
    Script {
        worker: usize,
        file: String,
        raised: StartException,
        failure: ScriptFailure,
    },
    #[error("worker {worker}: {step} failed: {raised}")]
    Setup {
        worker: usize,
        step: &'static str,
        raised: StartException,
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
    /// A module's source did not run.
    fn exec(worker: usize, file: &str, error: ExecError) -> Self {
        match error {
            ExecError::Nul(source) => WorkerStartError::Nul {
                worker,
                file: file.to_string(),
                source,
            },
            ExecError::Raised { raised, failure } => WorkerStartError::Script {
                worker,
                file: file.to_string(),
                raised,
                failure,
            },
        }
    }

    /// The exception the worker raised, if a Python exception is why it didn't start.
    fn raised(&self) -> Option<&StartException> {
        match self {
            WorkerStartError::Script { raised, .. } | WorkerStartError::Setup { raised, .. } => {
                Some(raised)
            }
            _ => None,
        }
    }

    /// This error as the main interpreter raises it: an import failure in the worker is an
    /// `ImportError` (`ModuleNotFoundError`) there too, anything else a `RuntimeError`. The
    /// worker's exception, with its traceback, is the `__cause__` ([`WorkerException`]).
    pub(crate) fn into_pyerr(self, py: Python<'_>) -> PyErr {
        let text = self.to_string();
        let Some(raised) = self.raised() else {
            return PyRuntimeError::new_err(text);
        };
        let err = match raised.class {
            ExceptionClass::ModuleNotFound => PyModuleNotFoundError::new_err(text),
            ExceptionClass::Import => PyImportError::new_err(text),
            ExceptionClass::Other => PyRuntimeError::new_err(text),
        };
        let traceback = raised.exception.traceback().to_owned();
        err.set_cause(py, Some(WorkerException::new_err(traceback)));
        err
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
    #[error("worker {worker}: a Rust panic in the async engine: {payload}")]
    Panic { worker: usize, payload: String },
}

// ---------------------------------------------------------------------------
// What workers are built from
// ---------------------------------------------------------------------------

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
    /// The libraries each worker loads from a private copy (`app.isolate(...)`), cloned by
    /// its bootstrap before the script runs.
    pub(crate) isolate: Vec<String>,
}

impl WorkerProgram {
    /// Reads `script_path` and main's `sys.path` now. An unreadable script is an error.
    pub(crate) fn read(
        py: Python<'_>,
        script_path: String,
        isolate: Vec<String>,
    ) -> PyResult<Self> {
        let script = std::fs::read_to_string(&script_path)
            .map_err(|e| PyRuntimeError::new_err(format!("read script '{script_path}': {e}")))?;
        let import_path = import_path(py)?;
        Ok(WorkerProgram {
            script_path,
            script,
            import_path,
            isolate,
        })
    }
}

/// Main's `sys.path` as text a worker can take. An entry is whatever `os.fsdecode` makes a
/// path of (`str`, `bytes`, a `pathlib.Path`); anything else can't be a path in a worker
/// (the object belongs to main), and is left out with a warning.
fn import_path(py: Python<'_>) -> PyResult<Vec<String>> {
    let fsdecode = py.import("os")?.getattr("fsdecode")?;
    let entries = py.import("sys")?.getattr("path")?;
    let mut path = Vec::new();
    for entry in entries.try_iter()? {
        let entry = entry?;
        match fsdecode
            .call1((&entry,))
            .and_then(|p| p.extract::<String>())
        {
            Ok(p) => path.push(p),
            Err(e) => tracing::warn!(
                target: "pyronova::server",
                entry = %entry.repr().map(|r| r.to_string()).unwrap_or_else(|re| re.to_string()),
                error = %e,
                "a sys.path entry that is not a path is left out of the workers' sys.path"
            ),
        }
    }
    Ok(path)
}

/// What every worker of one server is built from.
#[derive(Clone, Copy)]
pub(crate) struct WorkerSpec<'a> {
    pub(crate) program: &'a WorkerProgram,
    /// The routes the script must register: main's table up to its seal.
    pub(crate) expected: &'a RouteSignature,
    pub(crate) shared_state: &'a crate::state::SharedMap,
    /// Collect once this many requests ran since the last collect; 0 disables the count
    /// trigger. `GcConfig::count_trigger`: the threshold in count mode, the OOM failsafe in
    /// idle mode, 0 in off mode.
    pub(crate) gc_threshold: u64,
    /// The served app's limits; a worker whose script set others says they are ignored.
    pub(crate) limits: crate::site::Limits,
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

/// What a worker holds for the requests it serves.
pub(crate) trait Role: Send + Sized {
    /// Built last in the worker's init, in its interpreter, from the routes its script
    /// registered. `routes` and everything built before an error are `Bound`, so an
    /// error releases them at once.
    fn build(
        py: Python<'_>,
        worker: usize,
        routes: WorkerRoutes<'_>,
        gc_threshold: u64,
    ) -> Result<Self, WorkerStartError>;

    /// Releases every reference it holds, with this interpreter's thread state current.
    fn release(self, py: Python<'_>);
}

/// A worker that runs the hook chain from Rust: the TPC threads and the pool's sync
/// workers.
pub(crate) struct Inline {
    /// Indexed like the main interpreter's route table.
    handlers: Vec<Py<PyAny>>,
    /// `before_request` / `after_request` hooks, in registration order.
    before_hooks: Vec<Py<PyAny>>,
    after_hooks: Vec<Py<PyAny>>,
    /// This interpreter's persistent asyncio event loop, which `async def` hooks and
    /// handlers run on.
    event_loop: Py<PyAny>,
    gc: GcSchedule,
}

/// A worker that runs the async engine (`_async_engine.py`), which takes the handlers and
/// hooks from the app itself (`_worker_app_handlers` / `_worker_app_hooks`) and runs on
/// its own loop: it holds nothing here.
pub(crate) struct AsyncEngine;

impl Role for Inline {
    fn build(
        py: Python<'_>,
        worker: usize,
        routes: WorkerRoutes<'_>,
        gc_threshold: u64,
    ) -> Result<Self, WorkerStartError> {
        let setup = |step| {
            move |e: PyErr| WorkerStartError::Setup {
                worker,
                step,
                raised: StartException::capture(py, &e),
            }
        };
        let event_loop = new_event_loop(py).map_err(setup("creating the asyncio event loop"))?;
        // `_bootstrap.py` has already called gc.disable(): this is the worker's only
        // cycle collection.
        let collect = py
            .import("gc")
            .and_then(|gc| gc.getattr("collect"))
            .map_err(setup("looking up gc.collect"))?;

        let owned = |v: Vec<Bound<'_, PyAny>>| -> Vec<Py<PyAny>> {
            v.into_iter().map(Bound::unbind).collect()
        };
        Ok(Inline {
            handlers: owned(routes.handlers),
            before_hooks: owned(routes.before_hooks),
            after_hooks: owned(routes.after_hooks),
            event_loop: event_loop.unbind(),
            gc: GcSchedule {
                collect: collect.unbind(),
                threshold: gc_threshold,
                served: 0,
                collected_at: 0,
            },
        })
    }

    fn release(self, py: Python<'_>) {
        let owned = self
            .handlers
            .into_iter()
            .chain(self.before_hooks)
            .chain(self.after_hooks)
            .chain([self.event_loop, self.gc.collect]);
        for reference in owned {
            reference.drop_ref(py);
        }
    }
}

impl Role for AsyncEngine {
    fn build(
        _py: Python<'_>,
        _worker: usize,
        _routes: WorkerRoutes<'_>,
        _gc_threshold: u64,
    ) -> Result<Self, WorkerStartError> {
        Ok(AsyncEngine)
    }

    fn release(self, _py: Python<'_>) {}
}

impl EventLoop for Inline {
    fn event_loop<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>, HandlerError> {
        Ok(self.event_loop.bind(py).clone())
    }
}

impl Inline {
    /// Runs `route` for `request`: its hooks and handler in one fresh `contextvars.Context`.
    fn serve(
        &mut self,
        py: Python<'_>,
        route: RouteId,
        request: PyronovaRequest,
    ) -> Result<ResponseData, HandlerError> {
        self.gc.served += 1;
        in_request_context(py, |rc| self.run(rc, route, request))
            .unwrap_or_else(|e| Err(HandlerError::python(py, Stage::Setup, &e)))
    }

    fn run(
        &self,
        rc: &RequestContext<'_>,
        route: RouteId,
        request: PyronovaRequest,
    ) -> Result<ResponseData, HandlerError> {
        let py = rc.py();
        let chain = Chain {
            rc,
            event_loop: self,
            to_response: worker_response,
        };
        let req =
            Bound::new(py, request).map_err(|e| HandlerError::python(py, Stage::Setup, &e))?;

        let short_circuit = chain.before(&self.before_hooks, &req)?;
        let response = match short_circuit {
            Some(response) => response,
            // The worker's table was checked index by index against main's at init, and
            // the route came from main's table.
            None => {
                let value = chain.handler(&self.handlers[route.index()], &req)?;
                chain.after(&self.after_hooks, &req, worker_response(value)?)?
            }
        };

        #[cfg(feature = "leak_detect")]
        crate::leak_detect::record_drop(req.as_any());
        Ok(response)
    }
}

/// When a worker runs `gc.collect()`: every `threshold` requests (0: never by count).
struct GcSchedule {
    /// This interpreter's `gc.collect`, so a collection doesn't re-import.
    collect: Py<PyAny>,
    threshold: u64,
    /// Requests this worker has run, counted as each is dispatched.
    served: u64,
    /// `served` at the last collection.
    collected_at: u64,
}

impl GcSchedule {
    fn since_collect(&self) -> u64 {
        self.served - self.collected_at
    }

    fn due(&self) -> bool {
        self.threshold > 0 && self.since_collect() >= self.threshold
    }

    /// One full `gc.collect()`. A failure is logged with its real error and taken off the
    /// interpreter, so the next request starts with no exception set.
    fn collect(&mut self, py: Python<'_>, worker: usize) {
        self.collected_at = self.served;
        if let Err(err) = self.collect.bind(py).call0() {
            let exception = PyException::capture(py, &err);
            tracing::error!(
                target: "pyronova::app",
                worker_id = worker,
                error = %exception,
                traceback = exception.traceback(),
                "gc.collect() raised; the worker keeps serving, but this signals OOM, heap \
                 corruption or interpreter damage"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The worker
// ---------------------------------------------------------------------------

pub(crate) struct SubInterpreterWorker<R: Role = Inline> {
    /// Its thread state, saved while nothing runs on it.
    tstate: *mut ffi::PyThreadState,
    /// This worker's index: `WORKER_ID` in its bootstrap (log records) and async engine.
    worker_id: usize,
    /// References into this interpreter: released only by [`Self::end`], with its thread
    /// state current. A worker dropped without `end` leaks them rather than decref'ing
    /// into whatever interpreter is current there.
    role: ManuallyDrop<R>,
}

// SAFETY: a worker moves between threads only before it serves: from the thread that built
// it to the one that serves it, which rebinds `tstate` to itself first
// (`bind_to_this_thread`), or back to the building thread's `end_all`. Its thread state and
// the `Py<T>`s in `role` are only used with that thread state current, by one thread at a
// time (`&mut self`).
unsafe impl<R: Role> Send for SubInterpreterWorker<R> {}

impl<R: Role> Drop for SubInterpreterWorker<R> {
    fn drop(&mut self) {
        // `end` and `abandon` consume the worker without running this.
        tracing::error!(
            target: "pyronova::server",
            worker = self.worker_id,
            "sub-interpreter worker dropped without being ended; leaking its interpreter"
        );
    }
}

impl<R: Role> SubInterpreterWorker<R> {
    /// Create a new sub-interpreter, run the bootstrap and the user's script in it, and
    /// build its role from the app the script registered routes on.
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

        // Past this point we own a live sub-interpreter: an error ends it before
        // returning. Everything the init built is `Bound` until it succeeded, so an error
        // has released it by the time the interpreter ends.
        let built = with_current_tstate(|py| prepare::<R>(py, worker_id, spec));
        let result = match built {
            Ok(role) => Ok(SubInterpreterWorker {
                tstate: ffi::PyEval_SaveThread(),
                worker_id,
                role: ManuallyDrop::new(role),
            }),
            Err(e) => {
                ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
                Err(e)
            }
        };
        ffi::PyThreadState_Swap(main_tstate);
        result
    }

    pub(crate) fn worker_id(&self) -> usize {
        self.worker_id
    }

    /// Binds this worker's thread state to the calling thread, which serves it from now on
    /// (see `rebind_tstate_to_current_thread`).
    ///
    /// # Safety
    /// On the thread that will serve the worker, with no thread state current, before
    /// anything else runs on the worker there.
    pub(crate) unsafe fn bind_to_this_thread(&mut self) {
        self.tstate = rebind_tstate_to_current_thread(self.tstate);
    }

    /// Ends this worker's sub-interpreter: with its thread state current, release every
    /// reference the worker holds, then `Py_EndInterpreter`.
    ///
    /// Works from the worker's own thread (normal shutdown, no thread state current) and
    /// from the thread that created it (a failed start, possibly with main's thread state
    /// current, which is detached for the duration and restored afterwards).
    ///
    /// # Safety
    /// The runtime must not be finalized (see [`Self::abandon`]), and `tstate` must be
    /// usable on the calling thread: the worker's rebound thread state on its own thread,
    /// or the creator thread state on the creating thread.
    pub(crate) unsafe fn end(self) {
        let mut this = ManuallyDrop::new(self);
        let previous = ffi::PyThreadState_GetUnchecked();
        if !previous.is_null() {
            ffi::PyEval_SaveThread();
        }
        ffi::PyEval_RestoreThread(this.tstate);
        let role = ManuallyDrop::take(&mut this.role);
        with_current_tstate(|py| role.release(py));
        ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
        if !previous.is_null() {
            ffi::PyEval_RestoreThread(previous);
        }
    }

    /// Gives up on this worker without touching Python: for a worker whose thread outlived
    /// `Py_Finalize` (abandoned after the shutdown grace period), where restoring its
    /// thread state would be a use-after-free. The OS reclaims its memory at exit.
    pub(crate) fn abandon(self) {
        std::mem::forget(self);
    }

    /// Ends the worker on the thread that served it, once nothing on that thread uses it
    /// any more. A thread abandoned past shutdown may run this after
    /// `Py_Finalize`; the worker is abandoned then.
    pub(crate) fn end_on_own_thread(self) {
        // SAFETY: always safe to call.
        if unsafe { ffi::Py_IsInitialized() } != 0 {
            // SAFETY: on the worker's own thread, no thread state current.
            unsafe { self.end() };
        } else {
            self.abandon();
        }
    }

    /// Ends every worker in `workers` on the calling (creating) thread: the clean-up of a
    /// start that failed after some workers were built.
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

    /// Runs `f` on this worker's interpreter: acquires its GIL, runs `f`, releases the GIL.
    /// A panic in `f` comes back as its payload, the thread state put back all the same.
    ///
    /// # Safety
    /// On the thread `bind_to_this_thread` bound this worker to, with no thread state
    /// current.
    unsafe fn with_gil<T>(
        &mut self,
        f: impl for<'py> FnOnce(&mut R, Python<'py>) -> T,
    ) -> std::thread::Result<T> {
        // The guard writes the thread state back here even while a panic unwinds.
        let tstate = Cell::new(self.tstate);
        let role = &mut *self.role;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _gil = SubInterpGilGuard::acquire(tstate.get(), &tstate);
            // Re-entrant: this worker's thread state is current, and it is the thread's
            // gilstate one (`bind_to_this_thread`), so this registers the attach with PyO3
            // without switching thread states.
            Python::attach(|py| f(role, py))
        }));
        self.tstate = tstate.get();
        result
    }
}

impl SubInterpreterWorker<Inline> {
    /// Runs `route` for `request` on this worker's interpreter: the hooks and the handler,
    /// then a `gc.collect()` if one is due. A panic comes back as [`HandlerError::Panic`]
    /// with its payload.
    ///
    /// # Safety
    /// On the thread `bind_to_this_thread` bound this worker to, with no thread state
    /// current.
    pub(crate) unsafe fn serve(
        &mut self,
        route: RouteId,
        request: PyronovaRequest,
    ) -> Result<ResponseData, HandlerError> {
        let worker = self.worker_id;
        self.with_gil(|inline, py| {
            let response = inline.serve(py, route, request);
            if inline.gc.due() {
                inline.gc.collect(py, worker);
            }
            response
        })
        .unwrap_or_else(|payload| Err(HandlerError::panic(payload)))
    }

    /// Runs `gc.collect()` between requests, from the thread this worker is bound to (TPC
    /// idle mode).
    ///
    /// # Safety
    /// As for [`Self::serve`].
    pub(crate) unsafe fn collect_garbage_between_requests(&mut self) {
        let worker = self.worker_id;
        if let Err(payload) = self.with_gil(|inline, py| inline.gc.collect(py, worker)) {
            tracing::error!(
                target: "pyronova::server",
                worker,
                panic = %panic_message(&*payload),
                "a Rust panic in an idle gc.collect()"
            );
        }
    }

    /// Requests this worker has run.
    pub(crate) fn requests_served(&self) -> u64 {
        self.role.gc.served
    }

    /// Requests this worker has run since its last `gc.collect()`.
    pub(crate) fn requests_since_collect(&self) -> u64 {
        self.role.gc.since_collect()
    }
}

impl SubInterpreterWorker<AsyncEngine> {
    /// Runs the async engine in this worker's interpreter until `inbox` closes or the
    /// engine stops. What it ends with is returned: its exception with its traceback, or a
    /// Rust panic's payload.
    ///
    /// # Safety
    /// On the thread `bind_to_this_thread` bound this worker to, with no thread state
    /// current.
    pub(crate) unsafe fn run_async_engine(
        &mut self,
        inbox: AsyncInbox,
    ) -> Result<(), AsyncEngineError> {
        let worker = self.worker_id;
        self.with_gil(|_, py| run_engine(py, worker, inbox))
            .unwrap_or_else(|payload| {
                Err(AsyncEngineError::Panic {
                    worker,
                    payload: panic_message(&*payload),
                })
            })
    }
}

/// Executes the async engine as its own module, with this worker's id, its request inbox
/// (`CHANNEL`) and its task budget (`TASK_TIMEOUT`, seconds).
fn run_engine(py: Python<'_>, worker: usize, inbox: AsyncInbox) -> Result<(), AsyncEngineError> {
    let setup = |e: PyErr| AsyncEngineError::Setup {
        worker,
        exception: PyException::capture(py, &e),
    };
    let engine = new_module(py, ASYNC_ENGINE_MODULE, None)
        .and_then(|m| {
            m.setattr("WORKER_ID", worker)?;
            m.setattr("CHANNEL", Bound::new(py, inbox)?)?;
            m.setattr("TASK_TIMEOUT", ASYNC_TASK_BUDGET.as_secs_f64())?;
            Ok(m)
        })
        .map_err(setup)?;
    exec_in(
        py,
        include_str!("../../python/pyronova/_async_engine.py"),
        "pyronova/_async_engine.py",
        &engine,
    )
    .map_err(|e| match e {
        ExecError::Nul(source) => AsyncEngineError::Nul { worker, source },
        ExecError::Raised { raised, .. } => AsyncEngineError::Stopped {
            worker,
            exception: raised.exception,
        },
    })
}

// ---------------------------------------------------------------------------
// Worker init
// ---------------------------------------------------------------------------

/// The init, in the new worker's interpreter (`py` is its token): the bootstrap, the
/// script, the JSON serializer, then the role from the app the script registered on,
/// checked index by index against main's table.
fn prepare<R: Role>(
    py: Python<'_>,
    worker: usize,
    spec: &WorkerSpec<'_>,
) -> Result<R, WorkerStartError> {
    let setup = |step| {
        move |e: PyErr| WorkerStartError::Setup {
            worker,
            step,
            raised: StartException::capture(py, &e),
        }
    };
    let program = spec.program;

    // Before the script runs: a `PyronovaApp` or `SharedState` it creates in this
    // interpreter must see the running app's map.
    crate::state::hand_to_worker(py, spec.shared_state)?;

    // Before anything is imported: the bootstrap and the script resolve their imports
    // as they do on main.
    PyList::new(py, &program.import_path)
        .and_then(|path| py.import("sys")?.setattr("path", path))
        .map_err(setup("setting sys.path to main's"))?;

    // The bootstrap (logging bridge, GC policy, C-extension isolation) in its own
    // namespace, with this worker's id (for its log records) and the libraries to isolate.
    const BOOTSTRAP_FILE: &str = "pyronova/_bootstrap.py";
    let bootstrap = new_module(py, BOOTSTRAP_MODULE, None)
        .and_then(|m| {
            m.setattr("WORKER_ID", worker)?;
            m.setattr("ISOLATE_LIBS", PyTuple::new(py, &program.isolate)?)?;
            Ok(m)
        })
        .map_err(setup("creating the bootstrap module"))?;
    // The log handler's source runs in the same namespace first: the bootstrap installs
    // it before the package can be imported.
    const LOG_BRIDGE_FILE: &str = "pyronova/_log_bridge.py";
    exec_in(
        py,
        include_str!("../../python/pyronova/_log_bridge.py"),
        LOG_BRIDGE_FILE,
        &bootstrap,
    )
    .map_err(|e| WorkerStartError::exec(worker, LOG_BRIDGE_FILE, e))?;
    exec_in(
        py,
        include_str!("../../python/pyronova/_bootstrap.py"),
        BOOTSTRAP_FILE,
        &bootstrap,
    )
    .map_err(|e| WorkerStartError::exec(worker, BOOTSTRAP_FILE, e))?;

    // The user's script as a real module, compiled with its own path so tracebacks
    // point at it, `from __future__` imports work, and `typing.get_type_hints` finds
    // the module in `sys.modules`.
    let script_module = new_module(py, SCRIPT_MODULE, Some(&program.script_path))
        .map_err(setup("creating the script module"))?;
    exec_in(py, &program.script, &program.script_path, &script_module)
        .map_err(|e| WorkerStartError::exec(worker, &program.script_path, e))?;

    // The JSON serializer (isojson, a hard dependency).
    crate::response::require_json(py).map_err(setup("loading the JSON serializer"))?;

    let routes = registered_routes(py, worker, spec)?;
    R::build(py, worker, routes, spec.gc_threshold)
}

/// The routes of the app the script registered on, which must be main's table.
fn registered_routes<'py>(
    py: Python<'py>,
    worker: usize,
    spec: &WorkerSpec<'_>,
) -> Result<WorkerRoutes<'py>, WorkerStartError> {
    let registered = worker_app::worker_routes(py).map_err(|e| WorkerStartError::Setup {
        worker,
        step: "reading the app the script registered",
        raised: StartException::capture(py, &e),
    })?;
    let expected = spec.expected;
    let routes = match registered {
        Some(r) if r.signature == *expected => r,
        Some(r) => {
            return Err(WorkerStartError::RouteMismatch {
                worker,
                mismatch: RouteSignature::describe_mismatch(expected, &r.signature),
            })
        }
        None if expected.is_empty() => WorkerRoutes::empty(),
        None => {
            return Err(WorkerStartError::NoRoutes {
                worker,
                expected: expected.routes.len(),
            })
        }
    };
    for ignored in spec.limits.ignored_in_worker(&routes.limits) {
        tracing::warn!(target: "pyronova::server", worker, "{ignored}");
    }
    Ok(routes)
}

/// A new asyncio event loop, set as this interpreter's current one.
fn new_event_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let asyncio = py.import("asyncio")?;
    let event_loop = asyncio.call_method0("new_event_loop")?;
    asyncio.call_method1("set_event_loop", (&event_loop,))?;
    Ok(event_loop)
}

/// A new module named `name`, registered in `sys.modules`, with builtins and, for the
/// script, `__file__`.
fn new_module<'py>(
    py: Python<'py>,
    name: &CStr,
    file: Option<&str>,
) -> PyResult<Bound<'py, PyModule>> {
    let module = PyModule::new(py, &name.to_string_lossy())?;
    // SAFETY: attached (`py`); `PyEval_GetBuiltins` returns a borrowed reference to the
    // current frame's (or the interpreter's) builtins dict, never NULL.
    let builtins = unsafe { Bound::from_borrowed_ptr(py, ffi::PyEval_GetBuiltins()) };
    module.setattr("__builtins__", builtins)?;
    if let Some(path) = file {
        module.setattr("__file__", PyString::new(py, path))?;
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
    /// It raised, with this exception.
    Raised {
        raised: StartException,
        failure: ScriptFailure,
    },
}

/// Compiles `src` as `filename` and executes it in `module`'s namespace. An exception is
/// returned with its text and traceback (nothing is printed).
fn exec_in(
    py: Python<'_>,
    src: &str,
    filename: &str,
    module: &Bound<'_, PyModule>,
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
    ran.map(drop).map_err(|e| ExecError::Raised {
        raised: StartException::capture(py, &e),
        failure: script_failure(py, &e, filename, src),
    })
}

/// Whether `err` was raised by a relative import in the module compiled from `src` as
/// `filename`: the innermost traceback frame in that file is on a `from .x import y`
/// statement.
fn script_failure(py: Python<'_>, err: &PyErr, filename: &str, src: &str) -> ScriptFailure {
    if !err.is_instance_of::<PyImportError>(py) {
        return ScriptFailure::Other;
    }
    match failing_line(py, err, filename).and_then(|line| relative_import_at(py, src, line)) {
        Ok(true) => ScriptFailure::RelativeImport,
        Ok(false) => ScriptFailure::Other,
        Err(e) => {
            tracing::warn!(
                target: "pyronova::server",
                error = %e,
                "could not tell whether the import that failed in {filename} was relative"
            );
            ScriptFailure::Other
        }
    }
}

/// The line of the innermost frame of `err`'s traceback that is in `filename`, if any.
fn failing_line(py: Python<'_>, err: &PyErr, filename: &str) -> PyResult<Option<usize>> {
    let mut line = None;
    let mut tb = err.traceback(py);
    while let Some(frame) = tb {
        let code_file: String = frame
            .getattr("tb_frame")?
            .getattr("f_code")?
            .getattr("co_filename")?
            .extract()?;
        if code_file == filename {
            line = Some(frame.getattr("tb_lineno")?.extract()?);
        }
        tb = frame.getattr("tb_next")?.cast_into::<PyTraceback>().ok();
    }
    Ok(line)
}

/// Whether `line` of `src` is inside a relative `from` import statement.
fn relative_import_at(py: Python<'_>, src: &str, line: Option<usize>) -> PyResult<bool> {
    let Some(line) = line else {
        return Ok(false);
    };
    let ast = py.import("ast")?;
    let import_from = ast.getattr("ImportFrom")?;
    let tree = ast.call_method1("parse", (src,))?;
    for node in ast.call_method1("walk", (tree,))?.try_iter()? {
        let node = node?;
        if !node.is_instance(&import_from)? || node.getattr("level")?.extract::<u32>()? == 0 {
            continue;
        }
        let first: usize = node.getattr("lineno")?.extract()?;
        let last: usize = node.getattr("end_lineno")?.extract()?;
        if (first..=last).contains(&line) {
            return Ok(true);
        }
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// Thread states
// ---------------------------------------------------------------------------

/// Runs `f` with a token for the thread state current on this thread, which PyO3 does not
/// know is attached: a worker's init on the main OS thread, whose gilstate thread state is
/// main's (`Python::attach` there would switch to it), and a worker's end. A `Py<T>`
/// dropped in `f` would be deferred, not released: `f` releases with `drop_ref`.
///
/// # Safety
/// A thread state is current on this thread.
unsafe fn with_current_tstate<T>(f: impl for<'py> FnOnce(Python<'py>) -> T) -> T {
    f(Python::assume_attached())
}

/// Holds a sub-interpreter's GIL; releasing it on drop, so a panic mid-handler can't leave
/// the GIL locked (the next request would deadlock). The saved thread state is written
/// back to `tstate_cell` on drop, so the caller has it even after an unwind.
struct SubInterpGilGuard<'a> {
    tstate_cell: &'a Cell<*mut ffi::PyThreadState>,
}

impl<'a> SubInterpGilGuard<'a> {
    /// Acquires the sub-interpreter's GIL.
    ///
    /// # Safety
    /// `tstate` is a saved thread state usable on this thread, and no thread state is
    /// current here.
    unsafe fn acquire(
        tstate: *mut ffi::PyThreadState,
        tstate_cell: &'a Cell<*mut ffi::PyThreadState>,
    ) -> Self {
        ffi::PyEval_RestoreThread(tstate);
        Self { tstate_cell }
    }
}

impl Drop for SubInterpGilGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: the GIL is held while this guard exists.
        unsafe {
            self.tstate_cell.set(ffi::PyEval_SaveThread());
        }
    }
}

/// Replaces the worker's thread state (created on the building thread) with a fresh one
/// bound to the calling thread, and returns it saved.
///
/// A thread state created on one OS thread and attached/detached on another accumulates
/// per-thread bookkeeping: a pure-C reproducer leaks ~1 KB per attach cycle (997 B/iter
/// shared vs 0 B/iter with a `PyThreadState_New` on the serving thread). See
/// docs/memory-leak-investigation-2026-04-19.md.
///
/// # Safety
/// `creator_tstate` is the worker's saved thread state, not yet bound to another thread,
/// and no thread state is current on this thread.
unsafe fn rebind_tstate_to_current_thread(
    creator_tstate: *mut ffi::PyThreadState,
) -> *mut ffi::PyThreadState {
    ffi::PyEval_RestoreThread(creator_tstate);
    let interp = ffi::PyInterpreterState_Get();
    let fresh = ffi::PyThreadState_New(interp);
    if fresh.is_null() {
        // Keep serving on the creator tstate: the per-request leak above comes back on
        // this worker, so say so loudly, with what CPython reported. The error is taken
        // off the interpreter here, or the first request would start with it pending.
        let reported = with_current_tstate(PyErr::take);
        tracing::error!(
            target: "pyronova::server",
            error = ?reported,
            "PyThreadState_New returned NULL while binding a worker to its thread (the \
             interpreter is shutting down, or out of memory); the worker keeps the thread \
             state it was created with, which leaks ~1 KB per request until restart"
        );
        return ffi::PyEval_SaveThread();
    }
    let prev = ffi::PyThreadState_Swap(fresh);
    debug_assert_eq!(prev, creator_tstate);
    ffi::PyThreadState_Clear(creator_tstate);
    ffi::PyThreadState_Delete(creator_tstate);
    ffi::PyEval_SaveThread()
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

    #[test]
    fn the_relative_import_hint_is_only_for_a_relative_import() {
        assert!(ScriptFailure::RelativeImport
            .to_string()
            .contains("outside any package, so a relative import"));
        assert_eq!(ScriptFailure::Other.to_string(), "");
    }

    #[test]
    fn the_async_task_budget_ends_before_the_callers() {
        assert!(ASYNC_TASK_BUDGET < REQUEST_BUDGET);
        assert_eq!(ASYNC_TASK_BUDGET + ASYNC_TASK_MARGIN, REQUEST_BUDGET);
    }
}
