//! Streaming request body: a handler reads `req.stream`, a sync iterator of `bytes`
//! chunks, as the body's frames arrive, without buffering the whole body.
//!
//! Opt-in per route via `@app.post("/path", gil=True, stream=True)`; only `gil=True`
//! routes stream. A feeder task (`body::stream_body_feeder`) pushes frames into a
//! bounded channel, so a slow handler stops the feeder, which stops polling the body,
//! which closes the client's TCP window. `max_body_size` still bounds the total, and the
//! whole body must arrive within the request budget, as a buffered one does.
//!
//! Error handling: a body the feeder gives up on (larger than `max_body_size`, too slow,
//! a failed read) arrives as the same `BodyReject` a buffered body gets; reading it raises
//! [`BodyRejected`], an `OSError`. A handler that lets it through gets the response a
//! buffered body would (413 / 408 / 400), logged as a rejection, not a server error.
//! A stream that failed keeps failing: every later read raises the same error.

use std::sync::Mutex;

use pyo3::exceptions::{PyOSError, PyRuntimeError, PyStopIteration};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::body::{BodyReceiver, BodyReject, ChunkMsg};

pyo3::create_exception!(
    pyronova.engine,
    BodyRejected,
    PyOSError,
    "A streamed request body was rejected, as a buffered one would be: larger than \
     max_body_size, too slow, or a failed read. An OSError, so `except OSError` still \
     catches it; left uncaught, the request gets the buffered body's 413 / 408 / 400."
);

/// The rejection a `BodyRejected` the server raised stands for, kept on it (as
/// `_rejection`) so the dispatcher can answer it as what it is.
#[pyclass(frozen, name = "_BodyRejection", module = "pyronova.engine")]
struct Rejection(BodyReject);

/// `BodyRejected` for `reject`, carrying it.
fn body_rejected(py: Python<'_>, reject: BodyReject) -> PyErr {
    let err = BodyRejected::new_err(reject.to_string());
    let carried = Bound::new(py, Rejection(reject))
        .and_then(|r| err.value(py).setattr(intern!(py, "_rejection"), r));
    match carried {
        Ok(()) => err,
        Err(failed) => {
            failed.set_cause(py, Some(err));
            failed
        }
    }
}

/// The rejection `err` carries, if it is a `BodyRejected` the server raised (a user's own
/// `raise BodyRejected(...)` carries none).
pub(crate) fn rejection_of(py: Python<'_>, err: &PyErr) -> Option<BodyReject> {
    if !err.is_instance_of::<BodyRejected>(py) {
        return None;
    }
    let carried = err.value(py).getattr(intern!(py, "_rejection")).ok()?;
    let rejection = carried.cast::<Rejection>().ok()?;
    Some(rejection.get().0.clone())
}

/// Where reading a body stream stands.
enum StreamState {
    /// Chunks may still arrive.
    Open(BodyReceiver),
    /// The whole body was read.
    Finished,
    /// The body will never arrive in full; every read raises this.
    Failed(StreamFailure),
}

#[derive(Clone)]
enum StreamFailure {
    /// The server rejected the body (too large, too slow, a failed read).
    Rejected(BodyReject),
    /// The channel closed without the body's end: the feeder stopped (the handler did not
    /// take a chunk within the request budget, or the request was abandoned). Reading on
    /// would silently truncate the body.
    CutOff,
}

impl StreamFailure {
    fn to_py(&self, py: Python<'_>) -> PyErr {
        match self {
            StreamFailure::Rejected(reject) => body_rejected(py, reject.clone()),
            StreamFailure::CutOff => PyOSError::new_err(
                "the request body stream ended before the body did: the server stopped \
                 feeding it (the handler did not read it within the request budget, or the \
                 request was abandoned)",
            ),
        }
    }
}

/// What one receive from the channel means for the stream.
enum Received {
    Chunk(bytes::Bytes),
    End,
    Failed(StreamFailure),
}

impl Received {
    fn of(msg: Option<ChunkMsg>) -> Self {
        match msg {
            Some(ChunkMsg::Data(chunk)) => Received::Chunk(chunk),
            Some(ChunkMsg::Eof) => Received::End,
            Some(ChunkMsg::Err(reject)) => Received::Failed(StreamFailure::Rejected(reject)),
            None => Received::Failed(StreamFailure::CutOff),
        }
    }
}

/// Python-visible iterator over an incoming body's chunks.
///
/// The state is behind a `Mutex` held across each blocking receive, so concurrent
/// readers (two threads, or `drain_count` racing `__next__`) take chunks one at a time in
/// order instead of one of them seeing a spurious end.
#[pyclass(name = "BodyStream", module = "pyronova.engine")]
pub(crate) struct PyronovaBodyStream {
    state: Mutex<StreamState>,
}

impl PyronovaBodyStream {
    pub(crate) fn new(rx: BodyReceiver) -> Self {
        PyronovaBodyStream {
            state: Mutex::new(StreamState::Open(rx)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StreamState> {
        // A panic while the lock was held left the state whole (each transition is one
        // assignment), and unwinding into Python must not happen.
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[pymethods]
impl PyronovaBodyStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Block until the next chunk arrives. Returns `bytes` for data, raises
    /// `StopIteration` at the body's end, [`BodyRejected`] for a rejected body — and the
    /// same error again on every read after one.
    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let mut state = self.lock();
        let rx = match &mut *state {
            StreamState::Open(rx) => rx,
            StreamState::Finished => return Err(PyStopIteration::new_err(py.None())),
            StreamState::Failed(failure) => return Err(failure.to_py(py)),
        };
        match Received::of(py.detach(|| rx.blocking_recv())) {
            Received::Chunk(chunk) => Ok(PyBytes::new(py, &chunk).unbind()),
            Received::End => {
                *state = StreamState::Finished;
                Err(PyStopIteration::new_err(py.None()))
            }
            Received::Failed(failure) => {
                let err = failure.to_py(py);
                *state = StreamState::Failed(failure);
                Err(err)
            }
        }
    }

    /// Reads whole chunks until at least `n` bytes (or the body's end; all of it when `n`
    /// is `None`). Returns `b""` at the end. Chunks are never split, so the result may be
    /// longer than `n`.
    #[pyo3(signature = (n=None))]
    fn read(&self, py: Python<'_>, n: Option<usize>) -> PyResult<Py<PyBytes>> {
        let mut buf = Vec::<u8>::new();
        loop {
            // Checked before pulling a chunk, so `read(0)` consumes nothing (as
            // `file.read(0)`).
            if let Some(limit) = n {
                if buf.len() >= limit {
                    break;
                }
            }
            match self.__next__(py) {
                Ok(chunk) => {
                    buf.extend_from_slice(chunk.bind(py).as_bytes());
                }
                Err(e) if e.is_instance_of::<PyStopIteration>(py) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(PyBytes::new(py, &buf).unbind())
    }

    /// Consume the rest of the stream in Rust and return its byte count.
    ///
    /// For handlers that only need the upload size: the whole consume loop runs under a
    /// single `py.detach()` and never allocates a `PyBytes` per chunk, where iterating in
    /// Python pays a GIL round trip and a `bytes` object per frame.
    ///
    /// Raises [`BodyRejected`] for a rejected body, and `RuntimeError` on a stream already
    /// read to its end: there is nothing left to count, and `0` would read as an empty
    /// body.
    fn drain_count(&self, py: Python<'_>) -> PyResult<u64> {
        let mut state = self.lock();
        let rx = match &mut *state {
            StreamState::Open(rx) => rx,
            StreamState::Finished => {
                return Err(PyRuntimeError::new_err(
                    "the request body stream was already read to its end",
                ))
            }
            StreamState::Failed(failure) => return Err(failure.to_py(py)),
        };
        let drained: Result<u64, StreamFailure> = py.detach(|| {
            let mut total: u64 = 0;
            loop {
                match Received::of(rx.blocking_recv()) {
                    Received::Chunk(chunk) => total += chunk.len() as u64,
                    Received::End => return Ok(total),
                    Received::Failed(failure) => return Err(failure),
                }
            }
        });
        match drained {
            Ok(total) => {
                *state = StreamState::Finished;
                Ok(total)
            }
            Err(failure) => {
                let err = failure.to_py(py);
                *state = StreamState::Failed(failure);
                Err(err)
            }
        }
    }
}
