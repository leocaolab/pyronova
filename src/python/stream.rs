//! SSE (Server-Sent Events) streaming response support.
//!
//! Handler returns a `PyronovaStream` object, then calls `stream.send("data")`
//! in a loop. Each send pushes a chunk to the HTTP response body.
//!
//! Resource lifecycle: `close()` performs deterministic channel teardown,
//! independent of Python GC timing. This prevents zombie TCP connections
//! when PyronovaStream is held by long-lived Python references.

use bytes::Bytes;
use hyper::header::HeaderValue;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::mpsc;

use crate::types::{header_text, header_value, ResponseHeaders};

/// Upper bound on buffered stream chunks before `send()` rejects.
///
/// Previously the channel was unbounded — a slow client plus a fast
/// producer would buffer forever and OOM the process. Bounded backs
/// that pressure up to the caller, who can slow down, skip, or bail.
const STREAM_CHANNEL_CAP: usize = 1024;

type StreamItem = Result<Bytes, std::convert::Infallible>;

/// Python-facing stream object. Handler calls send()/send_event()/close().
#[pyclass(frozen, name = "Stream", module = "pyronova.engine")]
pub(crate) struct PyronovaStream {
    // Wrapped in Option so close() can deterministically drop the Sender,
    // decoupling channel lifetime from Python GC (Haskell bracket pattern).
    tx: std::sync::Mutex<Option<mpsc::Sender<StreamItem>>>,
    rx: std::sync::Mutex<Option<mpsc::Receiver<StreamItem>>>,
    pub(crate) content_type: HeaderValue,
    #[pyo3(get)]
    pub(crate) status_code: u16,
    /// Extra response headers, validated when the stream is made.
    pub(crate) headers: ResponseHeaders,
}

#[pymethods]
impl PyronovaStream {
    /// Create a new SSE stream. Channel is created immediately so send() works right away.
    #[new]
    /// `headers` as for `Response`: a name maps to a `str` or a list of `str`; a bad one is
    /// a `TypeError` / `ValueError` naming it.
    #[pyo3(signature = (content_type=None, status_code=200, headers=None))]
    fn new(
        content_type: Option<&str>,
        status_code: u16,
        headers: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let content_type = match content_type {
            Some(ct) => header_value("content_type", ct)?,
            None => HeaderValue::from_static("text/event-stream"),
        };
        let headers = headers
            .map(ResponseHeaders::from_py)
            .transpose()?
            .unwrap_or_default();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAP);
        Ok(PyronovaStream {
            tx: std::sync::Mutex::new(Some(tx)),
            rx: std::sync::Mutex::new(Some(rx)),
            content_type,
            status_code,
            headers,
        })
    }

    #[getter]
    fn content_type(&self) -> PyResult<&str> {
        header_text(&self.content_type)
    }

    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.headers.to_py(py)
    }

    /// Send raw data chunk. Returns BlockingIOError when the channel is
    /// full (slow client); the caller should back off before retrying.
    /// Uses try_send to preserve sync semantics — blocking on a Tokio
    /// mpsc.send() from the Python handler thread would require async.
    fn send(&self, data: &str) -> PyResult<()> {
        let tx_guard = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        let tx = tx_guard.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyConnectionError::new_err("stream was explicitly closed")
        })?;
        match tx.try_send(Ok(Bytes::from(data.to_string()))) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err(pyo3::exceptions::PyBlockingIOError::new_err(
                    "stream buffer full (client is slow); retry after a brief pause",
                ))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(
                pyo3::exceptions::PyConnectionError::new_err("client disconnected"),
            ),
        }
    }

    /// Send an SSE event: `event: {event}\ndata: {data}\n\n`
    #[pyo3(signature = (data, event=None, id=None))]
    fn send_event(&self, data: &str, event: Option<&str>, id: Option<&str>) -> PyResult<()> {
        // SSE field values for `id` and `event` must not contain CR or LF —
        // a newline in either injects arbitrary SSE fields (e.g. injecting
        // "data: attacker-controlled" by embedding "\ndata: ..." in an event name).
        // Per RFC 8895 the id and event fields are single-line.
        if let Some(id) = id {
            if id.contains(['\n', '\r']) {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "SSE id must not contain newline or carriage-return characters",
                ));
            }
        }
        if let Some(event) = event {
            if event.contains(['\n', '\r']) {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "SSE event name must not contain newline or carriage-return characters",
                ));
            }
        }
        let mut msg = String::with_capacity(data.len() + 64);
        if let Some(id) = id {
            msg.push_str("id: ");
            msg.push_str(id);
            msg.push('\n');
        }
        if let Some(event) = event {
            msg.push_str("event: ");
            msg.push_str(event);
            msg.push('\n');
        }
        // SSE spec (WHATWG): line endings are \n, \r\n, or bare \r. Rust's
        // str::lines() handles \n and \r\n but preserves bare \r within a line,
        // enabling injection of extra SSE fields via a \r in data. Normalize
        // bare \r to \n first so all three variants split correctly.
        let data_norm;
        let data_ref: &str = if data.contains('\r') {
            data_norm = data.replace("\r\n", "\n").replace('\r', "\n");
            &data_norm
        } else {
            data
        };
        for line in data_ref.lines() {
            msg.push_str("data: ");
            msg.push_str(line);
            msg.push('\n');
        }
        if data_ref.is_empty() {
            msg.push_str("data: \n");
        }
        msg.push('\n'); // End of event
        self.send(&msg)
    }

    /// Deterministic channel teardown — drops the Sender immediately,
    /// causing the Tokio Receiver to see channel-closed and end the HTTP
    /// response. Does not depend on Python GC timing.
    fn close(&self) {
        let mut lock = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        let _ = lock.take();
    }
}

impl PyronovaStream {
    /// Take the receiver (called once by Rust handler to start streaming).
    pub(crate) fn take_rx(&self) -> Option<mpsc::Receiver<StreamItem>> {
        self.rx.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}
