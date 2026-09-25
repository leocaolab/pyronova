//! Why a request failed, as one typed error, from where it happens to the response.
//!
//! A failure is a [`HandlerError`] holding the raw error: a Python exception's type, text
//! and traceback, a panic's payload, the response-mapping failure. It is logged exactly
//! once, where it happens, with the request's id ([`HandlerError::log`] consumes it into a
//! [`Logged`]), and rendered exactly once, at the pipeline edge ([`Logged::into_response`]):
//! only a `Logged` error can become a response, and logging consumes the error, so neither
//! step can be skipped or repeated.
//!
//! What the client sees (decision D4): a 4xx body carries the reason; a 5xx body is generic
//! plus the request id, which is also on the log line holding the real error.

use std::any::Any;
use std::fmt;

use bytes::Bytes;
use http_body_util::Full;
use hyper::Response;
use pyo3::prelude::*;
use pyo3::types::PyString;

use super::pipeline::{BodyReject, REQUEST_BUDGET};
use crate::request_id::RequestId;
use crate::response::{self, ResponseError};

/// The body text of every 500.
pub(crate) const GENERIC_500: &str = "Internal Server Error";

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

    /// The exception pending on this thread's interpreter (after a C-API call returned
    /// NULL), taken off it. A NULL with nothing pending is itself reported (SystemError).
    pub(crate) fn fetch(py: Python<'_>) -> Self {
        Self::capture(py, &PyErr::fetch(py))
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
pub(crate) fn py_text(s: &Bound<'_, PyString>) -> String {
    if let Ok(text) = s.to_str() {
        return text.to_owned();
    }
    s.call_method1("encode", ("utf-8", "backslashreplace"))
        .and_then(|b| b.extract::<Vec<u8>>())
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_else(|_| s.to_string_lossy().into_owned())
}

// ─────────────────────────── the error ───────────────────────────

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
    #[error(transparent)]
    BodyRejected(#[from] BodyReject),
    #[error("could not start the thread that runs the handler: {0}")]
    ThreadSpawn(#[source] std::io::Error),
}

impl HandlerError {
    pub(crate) fn python(py: Python<'_>, stage: Stage, err: &PyErr) -> Self {
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

    /// A request the server accepted but gave up on without an answer from Python
    /// (counted in `DROPPED_REQUESTS`).
    fn is_refusal(&self) -> bool {
        matches!(
            self,
            HandlerError::Overloaded(_)
                | HandlerError::PoolClosed(_)
                | HandlerError::WorkerLost(_)
                | HandlerError::Timeout
        )
    }

    fn traceback(&self) -> Option<&str> {
        match self {
            HandlerError::Python { exception, .. } => Some(exception.traceback()),
            HandlerError::Response(e) => e.exception().map(PyException::traceback),
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
            HandlerError::Overloaded(_) | HandlerError::PoolClosed(_) => tracing::warn!(
                target: "pyronova::handler", request_id = %id, method, path,
                error = %self, "request refused"
            ),
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

/// The request an error belongs to, as its log line names it.
pub(crate) struct RequestTag<'a> {
    pub(crate) id: &'a RequestId,
    pub(crate) method: &'a str,
    pub(crate) path: &'a str,
}

/// A [`HandlerError`] that has been logged, with the id it was logged under: the only
/// form an error takes on its way to the response. Boxed, so a `Result<ResponseData,
/// Logged>` costs the success path nothing.
pub(crate) struct Logged(Box<LoggedError>);

struct LoggedError {
    error: HandlerError,
    request_id: RequestId,
}

impl fmt::Debug for Logged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Logged({} [{}])", self.0.error, self.0.request_id)
    }
}

impl Logged {
    /// The response for this error: a 4xx carries the reason, a 5xx the generic text and
    /// the request id it was logged under. This match is the one place that maps an error
    /// to its status.
    pub(crate) fn into_response(self) -> Response<Full<Bytes>> {
        let LoggedError { error, request_id } = *self.0;
        if error.is_refusal() {
            crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        match error {
            HandlerError::Overloaded(_) => {
                response::overloaded_response("server overloaded", &request_id)
            }
            HandlerError::PoolClosed(_) => {
                response::unavailable_response("server shutting down", &request_id)
            }
            HandlerError::Timeout => response::gateway_timeout_response(&request_id),
            HandlerError::BodyRejected(BodyReject::TooLarge) => {
                response::payload_too_large_response()
            }
            HandlerError::BodyRejected(BodyReject::TimedOut) => {
                response::request_timeout_response()
            }
            HandlerError::BodyRejected(reject @ BodyReject::Read(_)) => {
                response::bad_request_response(&reject.to_string())
            }
            HandlerError::Python { .. }
            | HandlerError::Panic { .. }
            | HandlerError::Response(_)
            | HandlerError::WorkerLost(_)
            | HandlerError::ThreadSpawn(_) => response::error_response(GENERIC_500, &request_id),
        }
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
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// The log output of `f`, as the JSON the server writes.
    fn captured_log(f: impl FnOnce()) -> String {
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

    fn tag(id: &RequestId) -> RequestTag<'_> {
        RequestTag {
            id,
            method: "GET",
            path: "/p",
        }
    }

    fn body(resp: Response<Full<Bytes>>) -> serde_json::Value {
        use http_body_util::BodyExt;
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let bytes = rt.block_on(resp.into_body().collect()).unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
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
    fn a_5xx_body_is_generic_with_the_logged_id() {
        let id = RequestId::mint();
        let cases = [
            (
                HandlerError::Panic {
                    payload: "secret payload".into(),
                },
                500,
                GENERIC_500,
            ),
            (HandlerError::WorkerLost("secret worker"), 500, GENERIC_500),
            (
                HandlerError::Overloaded("secret queue"),
                503,
                "server overloaded",
            ),
            (
                HandlerError::PoolClosed("secret pool"),
                503,
                "server shutting down",
            ),
            (HandlerError::Timeout, 504, "request timeout"),
        ];
        for (err, status, text) in cases {
            let resp = captured_log_value(|| err.log(&tag(&id)).into_response());
            assert_eq!(resp.status().as_u16(), status);
            let body = body(resp);
            assert_eq!(
                body,
                serde_json::json!({"error": text, "request_id": id.to_string()})
            );
        }
    }

    #[test]
    fn a_4xx_body_carries_the_reason() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            HandlerError::BodyRejected(BodyReject::TooLarge)
                .log(&tag(&id))
                .into_response()
        });
        assert_eq!(resp.status().as_u16(), 413);
        assert_eq!(
            body(resp),
            serde_json::json!({"error": "payload too large"})
        );
        let resp = captured_log_value(|| {
            HandlerError::BodyRejected(BodyReject::TimedOut)
                .log(&tag(&id))
                .into_response()
        });
        assert_eq!(resp.status().as_u16(), 408);
        assert_eq!(
            body(resp),
            serde_json::json!({"error": "request body timeout"})
        );
    }

    /// Runs `f` with the log captured (and discarded).
    fn captured_log_value<T>(f: impl FnOnce() -> T) -> T {
        let mut out = None;
        captured_log(|| out = Some(f()));
        out.unwrap()
    }
}
