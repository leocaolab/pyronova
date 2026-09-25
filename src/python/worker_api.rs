//! The engine functions a sub-interpreter worker's async engine (`_async_engine.py`) calls
//! (Layer 2, C5): pull the next request from its inbox, answer it (a response, an
//! exception, a timeout), turn a handler's return value into a `Response`, and get the
//! worker app's handlers and hooks.
//!
//! Each async worker's engine gets its own [`AsyncInbox`] (its `CHANNEL`): nothing here is
//! process-global, so two pools in one process never see each other's requests, and a
//! worker of a pool that is gone finds its inbox closed. Each request comes as an
//! [`AsyncJob`] that carries where its answer goes: dropping one unanswered answers it.
//!
//! PyO3 does the argument parsing; a Rust panic becomes a `RuntimeError` (not PyO3's
//! `PanicException`, a `BaseException`), logged here.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::{PyBaseException, PyException as PyExceptionType, PyRuntimeError};
use pyo3::prelude::*;

use super::hook_chain::worker_response;
use super::pool::{WorkReply, WorkRequest};
use super::worker_app;
use crate::handlers::error::{panic_message, HandlerError, RequestTag, Stage};
use crate::request_id::RequestId;
use crate::response::ResponseError;
use crate::types::{PyronovaRequest, ResponseData};

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

// ---------------------------------------------------------------------------
// The inbox and its jobs
// ---------------------------------------------------------------------------

/// One async worker's request queue: the pool's async channel, shared by its async
/// workers, and this worker's own stop signal, which its engine closes when it stops so
/// its fetcher thread returns.
#[pyclass(module = "pyronova.engine", frozen, name = "_AsyncInbox")]
pub(crate) struct AsyncInbox {
    requests: crossbeam_channel::Receiver<WorkRequest>,
    stop: crossbeam_channel::Receiver<()>,
    /// Dropped by `close`, which disconnects `stop`.
    closer: Mutex<Option<crossbeam_channel::Sender<()>>>,
}

impl AsyncInbox {
    pub(crate) fn new(requests: crossbeam_channel::Receiver<WorkRequest>) -> Self {
        let (closer, stop) = crossbeam_channel::bounded(0);
        AsyncInbox {
            requests,
            stop,
            closer: Mutex::new(Some(closer)),
        }
    }

    /// The next request whose caller still waits, or `None` once the pool's channel or
    /// this inbox is closed. A request whose caller gave up (504) meanwhile is dropped
    /// unrun, as the sync workers do.
    fn next_live(&self) -> Option<WorkRequest> {
        loop {
            let request = crossbeam_channel::select! {
                recv(self.requests) -> request => request.ok()?,
                recv(self.stop) -> _ => return None,
            };
            if !request.response_tx.is_closed() {
                return Some(request);
            }
            WorkRequest::inc_completed();
        }
    }
}

/// One request the async engine runs: its route index, its `Request`, and where its answer
/// goes. Answered exactly once: by `_worker_send` / `_worker_fail` / `_worker_timed_out`,
/// or, if the engine drops it unanswered (a task cancelled when the engine stopped), by
/// its drop, with an error.
#[pyclass(module = "pyronova.engine", frozen, name = "_AsyncJob")]
pub(crate) struct AsyncJob {
    #[pyo3(get)]
    route: usize,
    #[pyo3(get)]
    request: Py<PyronovaRequest>,
    reply: Mutex<Option<Pending>>,
}

/// Where a request's answer goes, and the request as its error log line names it.
struct Pending {
    reply: WorkReply,
    request_id: RequestId,
    method: Arc<str>,
    path: Arc<str>,
}

impl Pending {
    /// Sends `result` to the waiting caller, an error logged first with the request it
    /// belongs to. A caller that stopped waiting (504) drops it; an error is logged anyway.
    fn answer(self, result: Result<ResponseData, HandlerError>) {
        let tag = RequestTag {
            id: &self.request_id,
            method: &self.method,
            path: &self.path,
        };
        let reply = result.map_err(|e| e.log(&tag));
        if self.reply.send(reply).is_err() {
            tracing::debug!(target: "pyronova::server", request_id = %self.request_id, "the caller timed out (504); dropping the result");
        }
        WorkRequest::inc_completed();
    }
}

impl AsyncJob {
    /// Answers this job; a second answer is an error of the engine's.
    fn answer(&self, result: Result<ResponseData, HandlerError>) -> PyResult<()> {
        let pending = self
            .reply
            .lock()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("this request was already answered"))?;
        pending.answer(result);
        Ok(())
    }
}

impl Drop for AsyncJob {
    fn drop(&mut self) {
        if let Some(pending) = self.reply.get_mut().take() {
            pending.answer(Err(HandlerError::WorkerLost(
                "the async engine dropped the request without answering it",
            )));
        }
    }
}

/// A handler's return value (or a hook's) that is not a response, raised by
/// `_worker_to_response` inside the engine's task: it carries the typed error to
/// `_worker_fail`, which answers the request with it.
#[pyclass(module = "pyronova.engine", extends = PyExceptionType, frozen, name = "_ResponseInvalid")]
pub(crate) struct ResponseInvalid {
    text: String,
    error: Mutex<Option<ResponseError>>,
}

#[pymethods]
impl ResponseInvalid {
    fn __str__(&self) -> &str {
        &self.text
    }
}

impl ResponseInvalid {
    fn raise(py: Python<'_>, error: ResponseError) -> PyErr {
        let raised = Bound::new(
            py,
            ResponseInvalid {
                text: error.to_string(),
                error: Mutex::new(Some(error)),
            },
        );
        match raised {
            Ok(exception) => PyErr::from_value(exception.into_any()),
            Err(e) => e,
        }
    }
}

// ---------------------------------------------------------------------------
// The functions
// ---------------------------------------------------------------------------

/// The next request from `inbox`, or `None` once its pool is gone or the inbox is closed.
/// Waits with the GIL released.
#[pyfunction]
pub(crate) fn _worker_recv(
    py: Python<'_>,
    inbox: &Bound<'_, AsyncInbox>,
) -> PyResult<Option<AsyncJob>> {
    no_panic("_worker_recv", || {
        let inbox = inbox.get();
        let Some(request) = py.detach(|| inbox.next_live()) else {
            return Ok(None);
        };
        let (route, request, reply) = request.into_request();
        let pending = Pending {
            reply,
            request_id: request.request_id.clone(),
            method: Arc::clone(&request.method),
            path: Arc::clone(&request.path),
        };
        match Py::new(py, request) {
            Ok(request) => Ok(Some(AsyncJob {
                route: route.index(),
                request,
                reply: Mutex::new(Some(pending)),
            })),
            Err(e) => {
                pending.answer(Err(HandlerError::python(py, Stage::Setup, &e)));
                Err(e)
            }
        }
    })
}

/// Closes `inbox`: its fetcher's `_worker_recv` returns `None`. The engine calls it when
/// it stops, so the fetcher thread, which the interpreter waits for at its end, exits.
#[pyfunction]
pub(crate) fn _worker_close(inbox: &Bound<'_, AsyncInbox>) -> PyResult<()> {
    no_panic("_worker_close", || {
        inbox.get().closer.lock().take();
        Ok(())
    })
}

/// Answers `job` with `response` (anything a handler may return; the engine passes a
/// `Response`).
#[pyfunction]
pub(crate) fn _worker_send(job: &Bound<'_, AsyncJob>, response: Bound<'_, PyAny>) -> PyResult<()> {
    no_panic("_worker_send", || {
        job.get()
            .answer(worker_response(response).map_err(HandlerError::from))
    })
}

/// Answers `job` with the exception its task raised (a hook's or the handler's, or a
/// return value that is not a response): logged with its traceback, a generic 500 for the
/// client.
#[pyfunction]
pub(crate) fn _worker_fail(
    py: Python<'_>,
    job: &Bound<'_, AsyncJob>,
    exception: Bound<'_, PyBaseException>,
) -> PyResult<()> {
    no_panic("_worker_fail", || {
        let invalid = exception
            .cast::<ResponseInvalid>()
            .ok()
            .and_then(|invalid| invalid.get().error.lock().take());
        let error = match invalid {
            Some(invalid) => HandlerError::Response(invalid),
            None => HandlerError::python(
                py,
                Stage::AsyncTask,
                &PyErr::from_value(exception.into_any()),
            ),
        };
        job.get().answer(Err(error))
    })
}

/// Answers `job` with a timeout (504): its task ran past the engine's budget and was
/// cancelled.
#[pyfunction]
pub(crate) fn _worker_timed_out(job: &Bound<'_, AsyncJob>) -> PyResult<()> {
    no_panic("_worker_timed_out", || {
        job.get().answer(Err(HandlerError::Timeout))
    })
}

/// A handler's (or hook's) return value as a `Response`, with the one mapping every
/// interpreter uses, so after-request hooks see a `Response` on every path. A value that
/// is no response (a `Stream`: streaming needs `gil=True, stream=True`, FR-16) raises
/// [`ResponseInvalid`] carrying the error.
#[pyfunction]
pub(crate) fn _worker_to_response(py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    no_panic("_worker_to_response", || {
        if value.is_instance_of::<crate::types::PyronovaResponse>() {
            return Ok(value.unbind());
        }
        worker_response(value)
            .and_then(|data| data.to_py(py))
            .map(|response| response.into_any().unbind())
            .map_err(|e| ResponseInvalid::raise(py, e))
    })
}

/// The handlers of the app this worker's script registered routes on, indexed like the
/// main interpreter's route table.
#[pyfunction]
pub(crate) fn _worker_app_handlers(py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
    no_panic("_worker_app_handlers", || {
        let routes = worker_app::worker_routes(py)?;
        Ok(routes
            .map(|r| r.handlers.into_iter().map(Bound::unbind).collect())
            .unwrap_or_default())
    })
}

/// The worker app's hooks, in registration order.
#[pyclass(module = "pyronova.engine", frozen, get_all, name = "_WorkerHooks")]
pub(crate) struct WorkerHooks {
    before: Vec<Py<PyAny>>,
    after: Vec<Py<PyAny>>,
}

/// The worker app's `before_request` and `after_request` hooks.
#[pyfunction]
pub(crate) fn _worker_app_hooks(py: Python<'_>) -> PyResult<WorkerHooks> {
    no_panic("_worker_app_hooks", || {
        let unbind = |hooks: Vec<Bound<'_, PyAny>>| hooks.into_iter().map(Bound::unbind).collect();
        Ok(match worker_app::worker_routes(py)? {
            Some(routes) => WorkerHooks {
                before: unbind(routes.before_hooks),
                after: unbind(routes.after_hooks),
            },
            None => WorkerHooks {
                before: Vec::new(),
                after: Vec::new(),
            },
        })
    })
}
