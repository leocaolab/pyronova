//! Why a request failed, as one typed error, from where it happens to the response.
//!
//! A failure is a [`HandlerError`] holding the raw error: a Python exception's type, text
//! and traceback, a panic's payload, the response-mapping failure. It is logged exactly
//! once, where it happens, with the request's id ([`HandlerError::log`] consumes it into a
//! [`Logged`]), and rendered exactly once, at the pipeline edge
//! (`handlers::error`, `Logged::into_response`): only a `Logged` error can become a
//! response, and logging consumes the error, so neither step can be skipped or repeated.
//!
//! A request the server gives up on without an answer from Python is a [`Refusal`]; the
//! only way to make one is [`refuse`], which counts it in `DROPPED_REQUESTS` there, where
//! the server decides it.
//!
//! The bottom layer: the worker, response and pipeline modules all build on it, and it
//! imports none of them.

use std::any::Any;
use std::fmt;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyString;

use crate::body::{BodyReject, REQUEST_BUDGET};
use crate::python::body_stream;
use crate::request_id::RequestId;

// ─────────────────────────── Python exceptions ───────────────────────────

/// A Python exception as it was raised: its type, its text and its formatted traceback.
#[derive(Debug, Clone)]
pub(crate) struct PyException {
    type_name: String,
    message: String,
    traceback: String,
}

impl PyException {
    /// Captures `err`. Runs only on the error path.
    pub(crate) fn capture(py: Python<'_>, err: &PyErr) -> Self {
        let value = err.value(py);
        let type_name = match value.get_type().name() {
            Ok(name) => py_text(&name),
            Err(e) => format!("<type name unavailable: {e}>"),
        };
        let message = match value.str() {
            Ok(text) => py_text(&text),
            Err(e) => format!("<str() of the exception raised {e}>"),
        };
        PyException {
            type_name,
            message,
            traceback: format_traceback(py, value),
        }
    }

    pub(crate) fn traceback(&self) -> &str {
        &self.traceback
    }
}

impl fmt::Display for PyException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.type_name, self.message)
    }
}

/// `traceback.format_exception(value)`, joined. If formatting itself fails, that failure
/// is what the traceback says.
fn format_traceback(
    py: Python<'_>,
    value: &Bound<'_, pyo3::exceptions::PyBaseException>,
) -> String {
    let formatted = py
        .import("traceback")
        .and_then(|tb| tb.call_method1("format_exception", (value,)))
        .and_then(|lines| PyString::new(py, "").call_method1("join", (lines,)))
        .and_then(|joined| Ok(py_text(joined.cast::<PyString>()?)));
    formatted.unwrap_or_else(|e| format!("<traceback unavailable: {e}>"))
}

/// A Python `str` as Rust text. A lone surrogate (not UTF-8) is kept as its `\udXXX`
/// escape rather than dropped.
fn py_text(s: &Bound<'_, PyString>) -> String {
    if let Ok(text) = s.to_str() {
        return text.to_owned();
    }
    s.call_method1("encode", ("utf-8", "backslashreplace"))
        .and_then(|b| b.extract::<Vec<u8>>())
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_else(|_| s.to_string_lossy().into_owned())
}

// ─────────────────────────── the error ───────────────────────────

/// Why a handler's return value is not a response.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResponseError {
    #[error("the returned value could not be serialized as JSON: {0}")]
    Json(PyException),
    #[error("response text is not valid Unicode: {0}")]
    Text(PyException),
    #[error("str() of the returned {type_name} raised {exception}")]
    Str {
        type_name: String,
        exception: PyException,
    },
    #[error("handler returned invalid HTTP status {0}")]
    Status(u16),
    #[error(
        "a sub-interpreter handler returned a Stream; streaming responses need gil=True, \
         stream=True on the route"
    )]
    StreamInWorker,
    #[error("the returned Stream was already consumed")]
    StreamConsumed,
    #[error("could not build the Response an after_request hook receives: {0}")]
    ToPy(PyException),
}

impl ResponseError {
    /// The Python exception behind this error, if one raised.
    pub(crate) fn exception(&self) -> Option<&PyException> {
        match self {
            ResponseError::Json(e) | ResponseError::Text(e) | ResponseError::ToPy(e) => Some(e),
            ResponseError::Str { exception, .. } => Some(exception),
            ResponseError::Status(_)
            | ResponseError::StreamInWorker
            | ResponseError::StreamConsumed => None,
        }
    }
}

/// Which part of a request raised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    /// Building the request's `Request` or entering its context.
    Setup,
    BeforeHook,
    Handler,
    AfterHook,
    /// The async engine's task for the request (hooks and handler).
    AsyncTask,
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Stage::Setup => "setting up the request",
            Stage::BeforeHook => "a before_request hook",
            Stage::Handler => "the handler",
            Stage::AfterHook => "an after_request hook",
            Stage::AsyncTask => "the request's async task",
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum HandlerError {
    #[error("{stage} raised {exception}")]
    Python {
        stage: Stage,
        exception: PyException,
    },
    #[error("a Rust panic while running the request: {payload}")]
    Panic { payload: String },
    #[error(transparent)]
    Response(#[from] ResponseError),
    /// The server gave up on the request without an answer from Python.
    #[error(transparent)]
    Refused(Refused),
    #[error(transparent)]
    BodyRejected(#[from] BodyReject),
    #[error("could not start the thread that runs the handler: {0}")]
    ThreadSpawn(#[source] std::io::Error),
    /// A main-side thread's cached event loop belongs to another interpreter than the one
    /// the request runs on: running the awaitable there would mix two interpreters'
    /// objects.
    #[error(
        "this thread's event loop belongs to interpreter {loop_interp}, but the request runs \
         on interpreter {current}"
    )]
    ForeignEventLoop { loop_interp: i64, current: i64 },
}

/// Why the server gave up on a request without an answer from Python.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Refusal {
    /// A queue or permit budget is full.
    #[error("no capacity for the request: {0}")]
    Overloaded(&'static str),
    /// The workers that would run it are gone: the server is shutting down.
    #[error("{0}: the server is shutting down")]
    PoolClosed(&'static str),
    /// The worker running it dropped its reply without answering.
    #[error("{0}")]
    WorkerLost(&'static str),
    #[error("the handler did not answer within {REQUEST_BUDGET:?}")]
    Timeout,
    /// A handler run inline on a TPC thread returned, but past the budget: nothing could
    /// answer before it returned, and its late result is not sent. What it raised, if it
    /// raised, is part of this one error, so the request logs once.
    #[error(
        "handler {handler} ran past the {REQUEST_BUDGET:?} request budget ({took:?}), \
         blocking its TPC thread the whole time; answered 504 only once it returned. A sync \
         `def` runs inline and cannot be preempted; make slow work `async def` or gil=True, \
         where the 504 is sent on time{}",
        raised.as_ref().map(|e| format!("; it also raised: {e}")).unwrap_or_default()
    )]
    Overran {
        handler: String,
        took: Duration,
        raised: Option<Box<HandlerError>>,
    },
}

/// A counted [`Refusal`]. Only [`refuse`] makes one, so every refusal is counted in
/// `DROPPED_REQUESTS` exactly once, where the server decides it.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(crate) struct Refused(Refusal);

impl Refused {
    pub(crate) fn refusal(&self) -> &Refusal {
        &self.0
    }
}

/// Gives up on a request: counts it in `DROPPED_REQUESTS` and returns the error that
/// answers it.
pub(crate) fn refuse(refusal: Refusal) -> HandlerError {
    crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    HandlerError::Refused(Refused(refusal))
}

impl HandlerError {
    /// What `stage` raising `err` means for the request: a streamed body's rejection
    /// (`req.stream` raised `BodyRejected` and the handler let it through) is that
    /// rejection, a 4xx like a buffered body's; anything else is the exception.
    pub(crate) fn python(py: Python<'_>, stage: Stage, err: &PyErr) -> Self {
        if let Some(reject) = body_stream::rejection_of(py, err) {
            return HandlerError::BodyRejected(reject);
        }
        HandlerError::Python {
            stage,
            exception: PyException::capture(py, err),
        }
    }

    /// A panic caught by `catch_unwind`, with its payload's text.
    pub(crate) fn panic(payload: Box<dyn Any + Send>) -> Self {
        HandlerError::Panic {
            payload: panic_message(&*payload),
        }
    }

    fn traceback(&self) -> Option<&str> {
        match self {
            HandlerError::Python { exception, .. } => Some(exception.traceback()),
            HandlerError::Response(e) => e.exception().map(PyException::traceback),
            HandlerError::Refused(Refused(Refusal::Overran {
                raised: Some(raised),
                ..
            })) => raised.traceback(),
            _ => None,
        }
    }

    /// Logs this error, once, with the request it failed. Call it where the error
    /// happens: on the thread that ran the handler, so an error nobody waits for any more
    /// (the request already timed out) is still logged.
    pub(crate) fn log(self, request: &RequestTag<'_>) -> Logged {
        let id = request.id;
        let (method, path) = (request.method, request.path);
        let traceback = self.traceback().unwrap_or("");
        match &self {
            HandlerError::Refused(Refused(Refusal::Overloaded(_) | Refusal::PoolClosed(_))) => {
                tracing::warn!(
                    target: "pyronova::handler", request_id = %id, method, path,
                    error = %self, "request refused"
                )
            }
            HandlerError::BodyRejected(BodyReject::Read(_)) => tracing::warn!(
                target: "pyronova::handler", request_id = %id, method, path,
                error = %self, "request body rejected"
            ),
            HandlerError::BodyRejected(_) => tracing::debug!(
                target: "pyronova::handler", request_id = %id, method, path,
                error = %self, "request body rejected"
            ),
            _ => tracing::error!(
                target: "pyronova::handler", request_id = %id, method, path,
                error = %self, traceback, "request failed"
            ),
        }
        Logged(Box::new(LoggedError {
            error: self,
            request_id: id.clone(),
        }))
    }
}

// ─────────────────────────── the request an error belongs to ───────────────────────────

/// The request an error belongs to, as its log line names it.
pub(crate) struct RequestTag<'a> {
    pub(crate) id: &'a RequestId,
    pub(crate) method: &'a str,
    pub(crate) path: &'a str,
}

/// A request's id, method and URI, kept for its log lines after the request itself moved
/// into the handler's `Request`. Every field is a copy or a reference-count increment of
/// the request's own: no allocation.
#[derive(Clone, Debug)]
pub(crate) struct RequestLabel {
    pub(crate) id: RequestId,
    pub(crate) method: hyper::Method,
    pub(crate) uri: hyper::Uri,
}

impl RequestLabel {
    pub(crate) fn tag(&self) -> RequestTag<'_> {
        RequestTag {
            id: &self.id,
            method: self.method.as_str(),
            path: self.uri.path(),
        }
    }
}

/// A [`HandlerError`] that has been logged, with the id it was logged under: the only
/// form an error takes on its way to the response. Boxed, so a `Result<ResponseData,
/// Logged>` costs the success path nothing.
pub(crate) struct Logged(Box<LoggedError>);

pub(crate) struct LoggedError {
    pub(crate) error: HandlerError,
    pub(crate) request_id: RequestId,
}

impl Logged {
    /// The error and the id it was logged under, for rendering.
    pub(crate) fn into_inner(self) -> LoggedError {
        *self.0
    }
}

impl fmt::Debug for Logged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Logged({} [{}])", self.0.error, self.0.request_id)
    }
}

// ─────────────────────────── panics ───────────────────────────

/// A panic payload's text: the `&str` or `String` `panic!` carries, else its type.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<panic payload is neither &str nor String>".to_string())
}

/// Runs `f`, turning a panic into [`HandlerError::Panic`] with its payload. `f` must
/// leave nothing half-updated that the caller reads after a panic (the worker loops put
/// the thread state back through a guard).
pub(crate) fn catch_panic<T>(
    f: impl FnOnce() -> Result<T, HandlerError>,
) -> Result<T, HandlerError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .unwrap_or_else(|payload| Err(HandlerError::panic(payload)))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// The log output of `f`, as the JSON the server writes.
    pub(crate) fn captured_log(f: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(data);
                Ok(data.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let sink = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || sink.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    pub(crate) fn tag(id: &RequestId) -> RequestTag<'_> {
        RequestTag {
            id,
            method: "GET",
            path: "/p",
        }
    }

    #[test]
    fn a_panic_keeps_its_payload() {
        let from_str = catch_panic::<()>(|| panic!("static payload"));
        let from_string = catch_panic::<()>(|| panic!("formatted {}", 42));
        let from_other = catch_panic::<()>(|| std::panic::panic_any(7_u8));
        let text = |r: Result<(), HandlerError>| r.unwrap_err().to_string();
        assert!(text(from_str).ends_with("static payload"));
        assert!(text(from_string).ends_with("formatted 42"));
        assert!(text(from_other).contains("neither &str nor String"));
    }

    #[test]
    fn a_panic_payload_is_logged_with_the_request_id() {
        // The pool and TPC worker loops both turn a handler panic into this error and log
        // it on the worker's thread.
        let id = RequestId::mint();
        let log = captured_log(|| {
            let err = catch_panic::<()>(|| panic!("worker exploded: m4")).unwrap_err();
            let _ = err.log(&tag(&id));
        });
        let line = log
            .lines()
            .find(|l| l.contains("worker exploded: m4"))
            .expect(&log);
        assert!(line.contains(&id.to_string()), "{line}");
        assert!(line.contains("\"level\":\"ERROR\""), "{line}");
    }

    #[test]
    fn refusing_counts_the_request_once() {
        use std::sync::atomic::Ordering::Relaxed;
        // Other tests may refuse concurrently: the counter only grows, by at least one per
        // refusal made here.
        let before = crate::monitor::DROPPED_REQUESTS.load(Relaxed);
        let err = refuse(Refusal::Timeout);
        let after_refuse = crate::monitor::DROPPED_REQUESTS.load(Relaxed);
        assert!(after_refuse > before);
        // Logging (and rendering, in `handlers::error`) no longer counts.
        let id = RequestId::mint();
        let logged = captured_log_value(|| err.log(&tag(&id)));
        assert!(matches!(
            logged.into_inner().error,
            HandlerError::Refused(Refused(Refusal::Timeout))
        ));
    }

    #[test]
    fn an_overrun_logs_once_with_what_the_handler_raised() {
        let id = RequestId::mint();
        let raised = HandlerError::Panic {
            payload: "late and broken".into(),
        };
        let log = captured_log(|| {
            let err = refuse(Refusal::Overran {
                handler: "slow".into(),
                took: Duration::from_secs(31),
                raised: Some(Box::new(raised)),
            });
            let _ = err.log(&tag(&id));
        });
        let lines: Vec<_> = log.lines().collect();
        assert_eq!(lines.len(), 1, "{log}");
        assert!(lines[0].contains("ran past"), "{log}");
        assert!(lines[0].contains("late and broken"), "{log}");
    }

    /// Runs `f` with the log captured (and discarded).
    pub(crate) fn captured_log_value<T>(f: impl FnOnce() -> T) -> T {
        let mut out = None;
        captured_log(|| out = Some(f()));
        out.unwrap()
    }
}
