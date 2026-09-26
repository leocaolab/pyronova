//! Streaming responses (Server-Sent Events): a handler returns a `Stream`, and each
//! `send` / `send_event` on it becomes a chunk of the response body.
//!
//! `close()` ends the response at once, whatever still references the stream: waiting
//! for Python's GC to drop it would hold the client's connection open.

use bytes::Bytes;
use hyper::header::HeaderValue;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::mpsc;

use crate::types::{header_text, header_value, ResponseHeaders};

/// Chunks buffered for a slow client before `send()` raises `BlockingIOError`, so a fast
/// producer backs off instead of growing the buffer without bound.
const STREAM_CHANNEL_CAP: usize = 1024;

type StreamItem = Result<Bytes, std::convert::Infallible>;

/// A streamed response body a handler writes to (`send`, `send_event`, `close`).
#[pyclass(frozen, name = "Stream", module = "pyronova.engine")]
pub(crate) struct PyronovaStream {
    /// `None` once `close()` dropped the sender, which ends the response.
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
    /// Creates a stream (`text/event-stream` by default). `headers` as for `Response`: a
    /// name maps to a `str` or a list of `str`; a bad one is a `TypeError` / `ValueError`
    /// naming it.
    #[new]
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

    /// Sends a raw chunk. Never blocks: raises `BlockingIOError` when the buffer is full
    /// (slow client), and the caller backs off before retrying.
    fn send(&self, data: &str) -> PyResult<()> {
        self.push(Bytes::copy_from_slice(data.as_bytes()))
    }

    /// Sends an SSE event: `id:` and `event:` lines if given, one `data:` line per line of
    /// `data`, then a blank line.
    #[pyo3(signature = (data, event=None, id=None))]
    fn send_event(&self, data: &str, event: Option<&str>, id: Option<&str>) -> PyResult<()> {
        // A line break in `id` or `event` would inject arbitrary SSE fields (WHATWG HTML,
        // server-sent events: every field is one line).
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
        for line in sse_lines(data) {
            msg.push_str("data: ");
            msg.push_str(line);
            msg.push('\n');
        }
        msg.push('\n');
        self.push(Bytes::from(msg))
    }

    /// Ends the response now; a later `send` raises `ConnectionError`.
    fn close(&self) {
        drop(self.tx.lock().unwrap_or_else(|e| e.into_inner()).take());
    }
}

/// The lines of an event's `data`, split at every line ending SSE knows (WHATWG: `\r\n`,
/// `\n`, or a bare `\r`, which would otherwise start a field of its own). One trailing line
/// ending ends the last line rather than adding an empty one; empty data is one empty line.
fn sse_lines(data: &str) -> impl Iterator<Item = &str> {
    let body = data
        .strip_suffix("\r\n")
        .or_else(|| data.strip_suffix(['\n', '\r']))
        .unwrap_or(data);
    body.split("\r\n").flat_map(|part| part.split(['\r', '\n']))
}

impl PyronovaStream {
    /// Queues one chunk for the response body. `BlockingIOError` when the channel is full
    /// (slow client), `ConnectionError` once the stream is closed or the client is gone.
    fn push(&self, chunk: Bytes) -> PyResult<()> {
        let tx_guard = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        let tx = tx_guard.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyConnectionError::new_err("stream was explicitly closed")
        })?;
        match tx.try_send(Ok(chunk)) {
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

    /// The body's receiving end; `Some` only the first time.
    pub(crate) fn take_rx(&self) -> Option<mpsc::Receiver<StreamItem>> {
        self.rx.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

#[cfg(test)]
mod tests {
    use super::sse_lines;

    fn lines(data: &str) -> Vec<&str> {
        sse_lines(data).collect()
    }

    #[test]
    fn every_line_ending_splits() {
        assert_eq!(lines("a\r\nb\nc\rd"), ["a", "b", "c", "d"]);
    }

    #[test]
    fn one_trailing_line_ending_ends_the_last_line() {
        assert_eq!(lines("a\n"), ["a"]);
        assert_eq!(lines("a\r\n"), ["a"]);
        assert_eq!(lines("a\n\n"), ["a", ""]);
        assert_eq!(lines("\n"), [""]);
    }

    #[test]
    fn empty_data_is_one_empty_line() {
        assert_eq!(lines(""), [""]);
    }
}
