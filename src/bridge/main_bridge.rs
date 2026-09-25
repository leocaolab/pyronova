//! Main-interpreter dispatch bridge for `gil=True` routes under TPC.
//!
//! TPC's default Phase 2 inline handler can only run sub-interpreter-safe
//! code. Legacy C extensions (numpy / pandas / torch / pydantic-core)
//! force `gil=True` on their handlers and require execution on the main
//! interpreter. This bridge makes that compatible with TPC.
//!
//! Architecture:
//!
//!   N TPC threads ──(crossbeam::bounded(cap), MPMC)──▶ M bridge worker threads
//!                            ↑                                          │
//!                            │ bounded, try_send → 503 on full          │ Python::attach
//!                            │                                          │ (main GIL)
//!                            ◀─────────(tokio::sync::oneshot)─────────── ◁
//!                              response via oneshot per request
//!
//! Why M > 1: Python's GIL is a *runtime* lock, not a per-thread one.
//! When a `gil=True` handler does I/O (file read, socket call, sleep,
//! sqlx fetch via `runtime().block_on`) the underlying C call releases
//! the GIL. With a single bridge thread, that I/O blocks the *thread*
//! even though the GIL is free — new GIL work piles up in the channel
//! until it 503s, while the main interp sits idle.
//!
//! With M worker threads, the released GIL is immediately picked up by
//! the next worker. CPU-bound numpy-style handlers still serialize on
//! the GIL (correct), but I/O-bound handlers see real concurrency
//! (limited by the worker count, not by the bridge fan-in width).
//!
//! Worker count defaults to 4 (typical: handlers mix CPU + I/O); set
//! `PYRONOVA_GIL_BRIDGE_WORKERS` to override. Channel capacity defaults
//! to 16 *per worker* so the back-pressure ratio stays the same as the
//! original single-thread design.
//!
//! Throughput contract: the bridge is a *cold path*. CPython's GIL caps
//! parallel CPU at the single-thread rate of whatever the handler's
//! work is (typically ≤ 10k rps for numpy-class workloads, often much
//! less). The channel capacity is deliberately small so that when the
//! bridge is saturated, TPC threads 503 *fast* rather than accumulating
//! memory on a queue that services slower than the fleet's inbound rate.
//! The rest of the fleet (pure sub-interp routes) keeps running at TPC
//! speed — the GIL-bound slowdown stays contained.
//!
//! `dispatch_one` reuses the existing `call_handler_with_hooks` from
//! `handlers.rs` — same semantics as the non-TPC GIL path: before hooks
//! → handler → after hooks → response extraction. Coroutines and
//! streams fall through the same logic.

use std::sync::Arc;

use crossbeam_channel as cbc;
use pyo3::prelude::*;
use tokio::sync::oneshot;

use crate::error::Logged;
use crate::handlers::{call_handler_with_hooks, MainReply};
use crate::router::Target;
use crate::site::SharedSite;
use crate::types::PyronovaRequest;

/// Work request for the main-interp bridge: the handler's `Request` (built on the TPC
/// thread) plus the oneshot reply channel. The reply is a buffered response or a stream
/// (SSE): tokio's mpsc receiver inside a stream is `Send`, so it crosses back to the TPC
/// thread, whose hyper body writer drives it. A `stream=True` route's `Request` carries
/// the receiver the feeder on the TPC thread's LocalSet pushes body frames into.
pub(crate) struct GilWorkItem {
    pub target: Target,
    pub request: PyronovaRequest,
    pub response_tx: oneshot::Sender<Result<MainReply, Logged>>,
}

/// A bridge thread the OS would not start. Serving `gil=True` routes with fewer threads
/// than configured would be a silent capacity cut, so it fails the server's start.
#[derive(Debug, thiserror::Error)]
#[error("could not start main-interpreter bridge thread {worker} of {requested}: {source}")]
pub(crate) struct BridgeSpawnError {
    worker: usize,
    requested: usize,
    #[source]
    source: std::io::Error,
}

impl From<BridgeSpawnError> for PyErr {
    fn from(e: BridgeSpawnError) -> Self {
        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
    }
}

/// Handle returned to callers. Cheap to Arc-share across all TPC
/// threads.
pub(crate) struct MainInterpBridge {
    tx: cbc::Sender<GilWorkItem>,
    /// Bridge threads exit when the Sender drops. `shutdown_join` drops it and joins them,
    /// so a server run returns only after every thread has released its `Py<T>`s and its
    /// main thread state (Layer 2, FR-6).
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl MainInterpBridge {
    /// Spawn `config.workers` main-interp bridge threads sharing a single
    /// crossbeam MPMC channel of `config.capacity`. Returns an
    /// Arc-safe handle for cloning to every TPC thread's dispatch path.
    ///
    /// The capacity is *total* (not per-worker) so the operator's mental
    /// model — "how many requests can queue before 503" — stays simple.
    /// Both are positive (`config::BridgeConfig`): a zero-capacity channel is a
    /// rendezvous, which `try_dispatch` would answer 503 on every request.
    ///
    /// A thread that fails to start fails the whole spawn: the threads already started
    /// are stopped and joined (with the GIL released, since they attach to main on their
    /// way out) before the error returns.
    pub(crate) fn spawn(
        py: Python<'_>,
        site: SharedSite,
        config: crate::config::BridgeConfig,
    ) -> Result<Arc<Self>, BridgeSpawnError> {
        let workers = config.workers.get();
        let (tx, rx) = cbc::bounded::<GilWorkItem>(config.capacity.get());

        // Bridge workers intentionally do NOT spin up a tokio runtime.
        // Python handlers on the main interp may call
        // `req.stream.drain_count()` which internally calls
        // `Receiver::blocking_recv` on the body-stream mpsc — and
        // tokio's blocking_recv panics if invoked from an async runtime
        // context. Plain std::threads with crossbeam recv() keep us
        // outside of any runtime, so the downstream Python-side
        // blocking_recv works correctly.
        let mut handles = Vec::with_capacity(workers);
        for i in 0..workers {
            let (rx, site) = (rx.clone(), Arc::clone(&site));
            let spawned = match injected_spawn_failure(i) {
                Some(injected) => Err(injected),
                None => std::thread::Builder::new()
                    .name(format!("pyronova-main-bridge-{i}"))
                    .stack_size(crate::python::PYTHON_THREAD_STACK)
                    .spawn(move || serve(i, rx, site)),
            };
            match spawned {
                Ok(handle) => handles.push(handle),
                Err(source) => {
                    let started = MainInterpBridge { tx, handles };
                    py.detach(|| started.join_all());
                    return Err(BridgeSpawnError {
                        worker: i,
                        requested: workers,
                        source,
                    });
                }
            }
        }

        tracing::info!(
            target: "pyronova::server",
            workers,
            capacity = config.capacity.get(),
            "main-interp bridge spawned"
        );
        Ok(Arc::new(MainInterpBridge { tx, handles }))
    }

    /// Close the channel and wait for every bridge thread to exit. Call it with the GIL
    /// released (the threads attach to main on their way out), after every other clone of
    /// the bridge is gone, i.e. after the TPC threads have been joined.
    pub(crate) fn shutdown_join(bridge: Arc<Self>) {
        match Arc::try_unwrap(bridge) {
            Ok(bridge) => bridge.join_all(),
            Err(still_shared) => {
                // Joining now would wait forever on a Sender someone else still holds.
                tracing::error!(
                    target: "pyronova::server",
                    other_refs = Arc::strong_count(&still_shared) - 1,
                    "main-interp bridge still referenced at shutdown; its threads are \
                     left to exit when the last reference drops"
                );
            }
        }
    }

    /// Closes the channel and joins every thread. Call it with the GIL released.
    fn join_all(self) {
        let MainInterpBridge { tx, handles } = self;
        drop(tx);
        for (i, h) in handles.into_iter().enumerate() {
            if let Err(payload) = h.join() {
                tracing::error!(
                    target: "pyronova::server",
                    worker = i,
                    panic = %crate::error::panic_message(&*payload),
                    "main-interp bridge worker panicked"
                );
            }
        }
    }

    /// Non-blocking dispatch: `Full` when the queue is (the caller answers 503), `Closed`
    /// when every bridge thread has exited. A refused item is dropped here; its reply
    /// channel closing is what the caller's feeder, if any, is stopped for.
    pub(crate) fn try_dispatch(&self, item: GilWorkItem) -> Result<(), TryDispatchError> {
        self.tx.try_send(item).map_err(|e| match e {
            cbc::TrySendError::Full(_) => TryDispatchError::Full,
            cbc::TrySendError::Disconnected(_) => TryDispatchError::Closed,
        })
    }
}

pub(crate) enum TryDispatchError {
    Full,
    Closed,
}

/// The bridge thread whose spawn fails, for the fault-injection build's tests
/// (`pyronova.engine._fault_fail_bridge_spawn`). Set once per process.
#[cfg(feature = "fault_injection")]
pub(crate) static FAIL_SPAWN_OF: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

#[cfg(feature = "fault_injection")]
fn injected_spawn_failure(worker: usize) -> Option<std::io::Error> {
    (FAIL_SPAWN_OF.get() == Some(&worker))
        .then(|| std::io::Error::other("injected spawn failure (fault_injection build)"))
}

#[cfg(not(feature = "fault_injection"))]
fn injected_spawn_failure(_worker: usize) -> Option<std::io::Error> {
    None
}

/// One bridge thread: runs work items until every sender is gone.
fn serve(worker: usize, rx: cbc::Receiver<GilWorkItem>, site: SharedSite) {
    // Each request attaches through `main_attach`, which gives this thread one main thread
    // state for its life (released by a thread-local destructor before `join` returns).
    // `recv` fails once every sender is gone: the server is shutting down.
    while let Ok(item) = rx.recv() {
        dispatch_one(&site, item);
    }
    // Everything this thread owns that holds Python objects goes while attached, before
    // the thread state does.
    crate::run_context::main_attach(move |py| {
        crate::handlers::close_thread_event_loop(py);
        drop(site);
    });
    tracing::info!(
        target: "pyronova::server",
        worker,
        "main-interp bridge worker exiting (channel closed)"
    );
}

fn dispatch_one(site: &SharedSite, item: GilWorkItem) {
    let GilWorkItem {
        target,
        request,
        response_tx,
    } = item;

    // Fast bailout if the caller's TPC task has already given up
    // waiting (client disconnect, upstream timeout). Don't burn main
    // interp's single GIL on a request nobody is going to read.
    if response_tx.is_closed() {
        return;
    }

    let result = call_handler_with_hooks(site, target, request);
    // The caller gave up (504) while the handler ran: nobody reads this reply.
    let _ = response_tx.send(result);
}
