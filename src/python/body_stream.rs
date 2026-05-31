//! Streaming request body — lets a handler consume `Incoming` body frames
//! as they arrive, without buffering the whole body in memory.
//!
//! Opt-in per route via `@app.post("/path", gil=True, stream=True)`. The
//! accept loop skips `Limited::new().collect()` and instead spawns a feeder
//! task that pushes each body frame into a **bounded** channel; the
//! handler sees `req.stream` as a Python iterator yielding `bytes` chunks
//! and terminating with `StopIteration` on EOF.
//!
//! The channel is `tokio::sync::mpsc` with capacity 8 rather than
//! `std::sync::mpsc` unbounded — the bound propagates backpressure all
//! the way to the TCP stack. If the Python handler is slow (say parsing
//! a 10 GB upload line-by-line), the feeder `.send().await` suspends,
//! which in turn stops `poll_frame` from being driven, and eventually
//! the TCP receive window closes on the client side. No OOM, no silent
//! space leak. The crossover point for "full" is ~8 × typical frame
//! size (~64 KB each) = ~512 KB in flight per connection, which is
//! deliberately modest.
//!
//! Scope (v1):
//!   * Only `gil=True` routes. Sub-interpreter streaming needs a C-FFI
//!     bridge akin to `pyronova_recv`/`pyronova_send` and is deferred.
//!   * Sync iterator only. `async for chunk in req.stream()` is deferred.
//!   * `max_body_size` still bounds total ingest even when streaming.
//!
//! Error handling: if the client disconnects or sends malformed frames,
//! the feeder sends an error message on the channel; `__next__` raises
//! `IOError(msg)` so the handler can terminate cleanly.

use std::sync::Mutex;

use bytes::Bytes;
use pyo3::exceptions::{PyIOError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Bounded-channel capacity. Low enough to enforce real backpressure
/// on a slow consumer, high enough that a steady-state 64 KB-frame
/// stream never stalls on round-trip scheduling latency.
pub(crate) const CHANNEL_CAPACITY: usize = 8;

/// A message on the feeder → handler channel.
pub(crate) enum ChunkMsg {
    Data(Bytes),
    /// Feeder hit an error (body too large, client disconnected, etc.).
    /// The handler will raise IOError on next iteration.
    Err(String),
    /// End of body — channel sender is dropped after sending this, so
    /// subsequent recv() returns Err immediately.
    Eof,
}

/// Standard shape for the per-request body-stream receiver: an Arc'd
/// Mutex that holds `Some(Receiver)` until the handler takes it out
/// with `.stream`, then holds `None`. For non-streaming routes the
/// slot is permanently `None` — use [`empty_body_stream_rx`] to avoid
/// allocating a fresh Mutex on every buffered request.
pub(crate) type BodyStreamRx =
    std::sync::Arc<std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<ChunkMsg>>>>;

/// Shared empty body-stream slot for non-streaming routes. All handlers
/// hold a clone of the same Arc; no writes ever target this slot (the
/// only operation is `take()` on `None`, which is a no-op), so sharing
/// is race-free. Saves one Arc + Mutex heap alloc per buffered request
/// on the hot path.
pub(crate) fn empty_body_stream_rx() -> BodyStreamRx {
    use std::sync::OnceLock;
    static EMPTY: OnceLock<BodyStreamRx> = OnceLock::new();
    EMPTY
        .get_or_init(|| std::sync::Arc::new(std::sync::Mutex::new(None)))
        .clone()
}

/// Python-visible iterator over an incoming body's chunks.
///
/// The receiver is wrapped in `Mutex<Option<...>>` so that:
///   * iteration is thread-safe (Python's GIL doesn't cover Rust channels)
///   * we can drop the receiver on EOF/error to free resources promptly
///
/// Only one logical iterator should consume a given stream — attempting to
/// call `next()` on a stream that's already been drained yields
/// `StopIteration` immediately.
#[pyclass(name = "BodyStream")]
pub(crate) struct PyronovaBodyStream {
    rx: Mutex<Option<tokio::sync::mpsc::Receiver<ChunkMsg>>>,
}

impl PyronovaBodyStream {
    pub(crate) fn new(rx: tokio::sync::mpsc::Receiver<ChunkMsg>) -> Self {
        PyronovaBodyStream {
            rx: Mutex::new(Some(rx)),
        }
    }
}

#[pymethods]
impl PyronovaBodyStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Block until the next chunk arrives. Returns `bytes` for data,
    /// raises `StopIteration` at EOF, `IOError(msg)` on transport error.
    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        // Hold the Mutex guard across the blocking recv(). Concurrent
        // callers (two Python threads iterating the same stream, or an
        // asyncio.gather(...) that races __next__ against drain_count)
        // will serialize on this lock — exactly the semantics a single-
        // owner byte stream demands. Each waiter receives the NEXT
        // chunk in FIFO order once the earlier call completes.
        //
        // The previous implementation `take()`'d the receiver out of
        // the Mutex to avoid holding the guard across the blocking
        // call — on the theory that a held guard would freeze other
        // Python threads. It did freeze them, but the alternative was
        // worse: the second concurrent __next__ observed `None` in
        // the slot and raised a spurious StopIteration, silently
        // truncating the request body without any error. Data loss in
        // a body stream is strictly worse than brief contention.
        //
        // GIL: released across blocking_recv() so unrelated Python
        // threads (different stream objects, ordinary app code)
        // continue to run.
        let mut guard = self.rx.lock().unwrap();
        let Some(rx) = guard.as_mut() else {
            return Err(PyStopIteration::new_err("stream exhausted"));
        };
        let msg = py.detach(|| rx.blocking_recv());
        match msg {
            Some(ChunkMsg::Data(b)) => Ok(PyBytes::new(py, &b).unbind()),
            Some(ChunkMsg::Eof) | None => {
                // Mark exhausted so subsequent calls fast-path without
                // touching the receiver.
                *guard = None;
                Err(PyStopIteration::new_err(py.None()))
            }
            Some(ChunkMsg::Err(e)) => Err(PyIOError::new_err(e)),
        }
    }

    /// Read up to `n` bytes by concatenating chunks. Convenience over the
    /// iterator protocol for code that wants `read(n)` semantics. Returns
    /// `b""` at EOF. Note: may return fewer than `n` bytes if EOF arrives;
    /// may return more than `n` bytes if the buffered chunk is larger (no
    /// attempt to split frames).
    #[pyo3(signature = (n=None))]
    fn read(&self, py: Python<'_>, n: Option<usize>) -> PyResult<Py<PyBytes>> {
        let mut buf = Vec::<u8>::new();
        loop {
            // Check the limit before pulling another chunk so that
            // read(0) returns b"" without consuming any data (matching
            // Python's file.read(0) contract), and so we never fetch a
            // chunk we've already satisfied the request for.
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

    /// Consume the entire stream in Rust and return the total byte count.
    ///
    /// Motivation: handlers that only need the upload size (progress
    /// meters, auditing, "reject if too big" middleware) would otherwise
    /// iterate every chunk through Python's `for chunk in req.stream:`
    /// loop. On a 25 MB upload split into ~1600 16 KB hyper frames,
    /// the Python-side iteration overhead (GIL release/reacquire + a
    /// PyBytes allocation per frame) dominates — ~1.8μs × 1600 ≈ 3ms
    /// per request of pure Python-loop cost.
    ///
    /// `drain_count()` runs the whole consume loop under a single
    /// `py.detach()` (GIL released once, not 1600 times) and never
    /// allocates a `PyBytes`. For the Arena /upload profile this is
    /// ~50% faster than the iterator path.
    ///
    /// Returns the raw byte count; raises IOError on transport error.
    fn drain_count(&self, py: Python<'_>) -> PyResult<u64> {
        // Same serialization guarantee as __next__: hold the Mutex
        // across the drain loop. Concurrent callers (drain_count racing
        // __next__ or drain_count racing drain_count) block on the
        // lock and then observe `None` — returning 0 — because the
        // first caller marked the stream exhausted.
        let mut guard = self.rx.lock().unwrap();
        let Some(rx) = guard.as_mut() else {
            return Ok(0);
        };
        let result: Result<u64, String> = py.detach(|| {
            let mut total: u64 = 0;
            loop {
                match rx.blocking_recv() {
                    Some(ChunkMsg::Data(b)) => total += b.len() as u64,
                    Some(ChunkMsg::Eof) | None => return Ok(total),
                    Some(ChunkMsg::Err(e)) => return Err(e),
                }
            }
        });
        *guard = None; // fully consumed (or errored)
        result.map_err(PyIOError::new_err)
    }
}
