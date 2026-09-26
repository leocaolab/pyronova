//! The app a worker's script registered on (Layer 2, C3).
//!
//! A worker serves the one app its script registers routes or hooks on: the first
//! registration records it here, and the worker reads its routes once the script has run.
//! The app type registers itself with the function that reads it, so this module (and the
//! worker that reads it) does not depend on the app.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

use crate::router::RouteSignature;
use crate::site::Limits;

/// The handlers and hooks of the app a worker's script registered on, indexed like main's
/// table, with their signature. `Bound`, so dropping them where the worker's thread state is
/// current but not registered with PyO3 (its init) releases them at once.
pub(crate) struct WorkerRoutes<'py> {
    pub(crate) signature: RouteSignature,
    pub(crate) handlers: Vec<Bound<'py, PyAny>>,
    pub(crate) before_hooks: Vec<Bound<'py, PyAny>>,
    pub(crate) after_hooks: Vec<Bound<'py, PyAny>>,
    /// The limits the script set on its app (never served: main's app is).
    pub(crate) limits: Limits,
}

impl WorkerRoutes<'_> {
    /// A script that registered nothing (a main table with no routes either).
    pub(crate) fn empty() -> Self {
        WorkerRoutes {
            signature: RouteSignature::default(),
            handlers: Vec::new(),
            before_hooks: Vec::new(),
            after_hooks: Vec::new(),
            limits: Limits::DEFAULT,
        }
    }
}

/// Reads the routes of an app of the type that recorded it.
pub(crate) type ReadRoutes = for<'py> fn(&Bound<'py, PyAny>) -> PyResult<WorkerRoutes<'py>>;

struct Recorded {
    app: Py<PyAny>,
    read: ReadRoutes,
}

/// Per interpreter under the fork: in a worker, the app its script registered on.
static WORKER_APP: PyOnceLock<Recorded> = PyOnceLock::new();

/// Records `app` as the one this worker serves, on its first registration. Registering on
/// a second app is an error (Layer 2, FR-4; decision Q-2 (a)). A no-op on the main
/// interpreter.
pub(crate) fn record(app: &Bound<'_, PyAny>, read: ReadRoutes) -> PyResult<()> {
    let py = app.py();
    if crate::run_context::on_main(py) {
        return Ok(());
    }
    match WORKER_APP.get(py) {
        Some(recorded) if recorded.app.bind(py).is(app) => Ok(()),
        Some(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
            "the script registers routes or hooks on a second app; a worker serves exactly \
             one app per script (create one Pyronova()/PyronovaApp() and register everything \
             on it)",
        )),
        None => WORKER_APP
            .set(
                py,
                Recorded {
                    app: app.clone().unbind(),
                    read,
                },
            )
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("worker app already recorded")),
    }
}

/// The routes of the app this worker's script registered on, or `None` if it registered
/// none. Meaningful only in a worker, after its script ran.
pub(crate) fn worker_routes(py: Python<'_>) -> PyResult<Option<WorkerRoutes<'_>>> {
    WORKER_APP
        .get(py)
        .map(|recorded| (recorded.read)(recorded.app.bind(py)))
        .transpose()
}
