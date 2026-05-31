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

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::OnceLock;

use bytes::Bytes;
use crossbeam_channel as cbc;
use tokio::sync::oneshot;

use crate::handlers::{call_handler_with_hooks, HandlerResult, StreamInfo};
use crate::router::FrozenRoutes;
use crate::types::{LazyHeaders, PyronovaRequest, ResponseData};

/// Bridge reply can be either a buffered response or a streaming one.
/// Stream handlers (SSE, chunked upload replies, long-poll) need to
/// hand an mpsc::Receiver back to the TPC thread so the hyper body
/// writer can drive it. The oneshot channel is Send + one-shot, and
/// tokio's mpsc::Receiver is Send, so transferring StreamInfo
/// across the main-interp → TPC thread boundary is safe.
pub(crate) enum BridgeResponse {
    Resp(ResponseData),
    Stream(StreamInfo),
}

/// Work request for the main-interp bridge. Carries everything a GIL
/// handler needs plus the oneshot reply channel.
pub(crate) struct GilWorkItem {
    pub method: Arc<str>,
    pub path: Arc<str>,
    pub params: Vec<(String, String)>,
    pub query: String,
    pub body: Bytes,
    pub headers: HashMap<String, String>,
    pub client_ip: IpAddr,
    pub handler_idx: usize,
    /// Body-stream receiver for `stream=True` routes. The feeder task
    /// running on the TPC thread's LocalSet pushes body frames into
    /// this receiver; the bridge thread's handler pulls them. Buffered
    /// routes share the [`crate::python::body_stream::empty_body_stream_rx`]
    /// singleton — `body` carries the collected bytes there.
    pub body_stream_rx: crate::python::body_stream::BodyStreamRx,
    pub response_tx: oneshot::Sender<Result<BridgeResponse, String>>,
}

/// Handle returned to callers. Cheap to Arc-share across all TPC
/// threads.
pub(crate) struct MainInterpBridge {
    tx: cbc::Sender<GilWorkItem>,
    // Worker JoinHandles are deliberately dropped-detached. Workers
    // exit when the crossbeam Sender count hits zero (server shutdown
    // dropping Arcs). At that point main interp cleanup runs naturally.
}

impl MainInterpBridge {
    /// Spawn `workers` main-interp bridge threads sharing a single
    /// crossbeam MPMC channel of the given capacity. Returns an
    /// Arc-safe handle for cloning to every TPC thread's dispatch path.
    ///
    /// `capacity` is *total* (not per-worker) so the operator's mental
    /// model — "how many requests can queue before 503" — stays simple.
    /// Defaults applied at the call site in src/app.rs:
    ///   PYRONOVA_GIL_BRIDGE_WORKERS=4
    ///   PYRONOVA_GIL_BRIDGE_CAPACITY=16 × workers
    pub(crate) fn spawn(routes: FrozenRoutes, capacity: usize, workers: usize) -> Arc<Self> {
        let workers = workers.max(1);
        let (tx, rx) = cbc::bounded::<GilWorkItem>(capacity);

        // Bridge workers intentionally do NOT spin up a tokio runtime.
        // Python handlers on the main interp may call
        // `req.stream.drain_count()` which internally calls
        // `Receiver::blocking_recv` on the body-stream mpsc — and
        // tokio's blocking_recv panics if invoked from an async runtime
        // context. Plain std::threads with crossbeam recv() keep us
        // outside of any runtime, so the downstream Python-side
        // blocking_recv works correctly.
        // Spawn workers best-effort: a thread spawn can fail under OS
        // resource exhaustion (ulimit -u, ENOMEM). Panicking here would
        // abort the whole server process — turning a recoverable
        // partial-capacity situation into a total outage. Instead we log
        // and continue with however many workers did start. The channel
        // back-pressure (try_dispatch → 503) and disconnect handling
        // (→ 500) already degrade gracefully when capacity is reduced or
        // zero, so the rest of the fleet (sub-interp routes) keeps
        // serving regardless.
        let mut spawned = 0usize;
        for i in 0..workers {
            let rx = rx.clone();
            let routes = Arc::clone(&routes);
            let res = std::thread::Builder::new()
                .name(format!("pyronova-main-bridge-{i}"))
                .spawn(move || {
                    loop {
                        // crossbeam recv: blocks until item or all
                        // Senders drop. Disconnected → server shutdown
                        // → worker exit.
                        let item = match rx.recv() {
                            Ok(i) => i,
                            Err(_) => break,
                        };
                        dispatch_one(&routes, item);
                    }
                    tracing::info!(
                        target: "pyronova::server",
                        worker = i,
                        "main-interp bridge worker exiting (channel closed)"
                    );
                });
            match res {
                Ok(_handle) => spawned += 1,
                Err(e) => {
                    tracing::error!(
                        target: "pyronova::server",
                        worker = i,
                        error = %e,
                        "failed to spawn main-interp bridge worker; \
                         continuing with fewer workers"
                    );
                }
            }
        }

        if spawned == 0 {
            tracing::error!(
                target: "pyronova::server",
                requested = workers,
                "no main-interp bridge workers could be spawned; \
                 gil=True routes will respond 500 until resources free up"
            );
        } else if spawned < workers {
            tracing::warn!(
                target: "pyronova::server",
                spawned,
                requested = workers,
                "main-interp bridge started with reduced worker count"
            );
        }

        tracing::info!(
            target: "pyronova::server",
            spawned,
            requested = workers,
            capacity,
            "main-interp bridge spawned"
        );

        Arc::new(MainInterpBridge { tx })
    }

    /// Non-blocking dispatch. Returns Err with the original work item
    /// when the channel is full (caller should respond 503) or when
    /// all bridge workers have exited (caller should respond 500).
    ///
    /// `clippy::result_large_err`: the Err variant carries the unsent
    /// `GilWorkItem` back so the caller can drain it / respond. Boxing
    /// would only move bytes around; this is a cold path (only fires on
    /// 503 / shutdown). Allowed deliberately.
    #[allow(clippy::result_large_err)]
    pub(crate) fn try_dispatch(
        &self,
        item: GilWorkItem,
    ) -> Result<(), (GilWorkItem, TryDispatchError)> {
        match self.tx.try_send(item) {
            Ok(()) => Ok(()),
            Err(cbc::TrySendError::Full(item)) => Err((item, TryDispatchError::Full)),
            Err(cbc::TrySendError::Disconnected(item)) => Err((item, TryDispatchError::Closed)),
        }
    }
}

pub(crate) enum TryDispatchError {
    Full,
    Closed,
}

fn dispatch_one(routes: &FrozenRoutes, item: GilWorkItem) {
    let GilWorkItem {
        method,
        path,
        params,
        query,
        body,
        headers,
        client_ip,
        handler_idx,
        body_stream_rx,
        response_tx,
    } = item;

    // Fast bailout if the caller's TPC task has already given up
    // waiting (client disconnect, upstream timeout). Don't burn main
    // interp's single GIL on a request nobody is going to read.
    if response_tx.is_closed() {
        return;
    }

    // Reconstruct the pyo3 PyronovaRequest from raw parts. Same shape
    // as what `handle_request` builds in non-TPC GIL mode, so the
    // downstream call_handler_with_hooks path is byte-for-byte the
    // same behavior.
    let sky_req = PyronovaRequest {
        method,
        path,
        params,
        query,
        headers_source: LazyHeaders::Converted(headers),
        headers_cache: OnceLock::new(),
        query_cache: OnceLock::new(),
        query_all_cache: OnceLock::new(),
        client_ip_addr: client_ip,
        body_bytes: body,
        body_stream_rx,
    };

    let result = call_handler_with_hooks(routes.clone(), handler_idx, sky_req);
    let resp = match result {
        HandlerResult::PyronovaResponse(Ok(rd)) => Ok(BridgeResponse::Resp(rd)),
        HandlerResult::PyronovaResponse(Err(e)) => Err(e),
        HandlerResult::PyronovaStream(info) => Ok(BridgeResponse::Stream(info)),
    };
    let _ = response_tx.send(resp);
}
