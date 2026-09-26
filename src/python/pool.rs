//! Channel-based interpreter pool: the `WorkRequest` domain type, the
//! `InterpreterPool` a server submits to, the worker threads it joins at shutdown, and
//! the per-OS-thread worker loops (sync + async) that drive `SubInterpreterWorker`s.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pyo3::prelude::*;

use super::worker::{AsyncEngine, SubInterpreterWorker, WorkerSpec, WorkerStartError};
use super::worker_api::AsyncInbox;
use crate::error::Logged;
use crate::types::{PyronovaRequest, ResponseData};

/// What a worker sends back for a request: its response, or its error, logged.
pub(crate) type WorkReply = tokio::sync::oneshot::Sender<Result<ResponseData, Logged>>;

// ---------------------------------------------------------------------------
// Work item for channel-based dispatch
// ---------------------------------------------------------------------------

pub(crate) struct WorkRequest {
    pub route: crate::router::RouteId,
    /// Which worker kind runs it: the sync pool or the async engine's.
    pub kind: crate::router::HandlerKind,
    /// The handler's `Request`, built on the Tokio thread (`request_head`); it moves into
    /// the worker's interpreter as is.
    pub request: PyronovaRequest,
    pub response_tx: WorkReply,
}

impl WorkRequest {
    /// The route, the handler's `Request`, and where its reply goes.
    pub(crate) fn into_request(self) -> (crate::router::RouteId, PyronovaRequest, WorkReply) {
        (self.route, self.request, self.response_tx)
    }
}

// Diagnostic: count WorkRequest creates vs worker-completes. Gated
// behind `leak_detect` because hitting two shared atomics on every
// request is an NUMA disaster on many-core boxes — a single shared
// AtomicU64 pings its cache line across every CCD on a Threadripper /
// EPYC on every `fetch_add`, silently capping throughput regardless of
// how many workers we spawn. The public `workrequest_counts()`
// Python export keeps its shape: returns (0, 0) when the feature is
// off, real values when diagnostics are compiled in.
#[cfg(feature = "leak_detect")]
static WR_CREATED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "leak_detect")]
static WR_COMPLETED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl WorkRequest {
    #[inline(always)]
    pub fn inc_created() {
        #[cfg(feature = "leak_detect")]
        WR_CREATED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    /// A request answered (or dropped because its caller had given up): exactly once per
    /// request, on every path.
    #[inline(always)]
    pub fn inc_completed() {
        #[cfg(feature = "leak_detect")]
        WR_COMPLETED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn created_count() -> u64 {
        #[cfg(feature = "leak_detect")]
        {
            WR_CREATED.load(std::sync::atomic::Ordering::Relaxed)
        }
        #[cfg(not(feature = "leak_detect"))]
        {
            0
        }
    }
    pub fn dropped_count() -> u64 {
        #[cfg(feature = "leak_detect")]
        {
            WR_COMPLETED.load(std::sync::atomic::Ordering::Relaxed)
        }
        #[cfg(not(feature = "leak_detect"))]
        {
            0
        }
    }
}

// ---------------------------------------------------------------------------
// Worker split
// ---------------------------------------------------------------------------

/// How the pool divides its workers: `def` handlers run on sync workers, `async def`
/// handlers on async workers (the async engine's event loop), never the other way round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerSplit {
    pub(crate) sync_workers: usize,
    pub(crate) async_workers: usize,
}

impl WorkerSplit {
    pub(crate) fn total(&self) -> usize {
        self.sync_workers + self.async_workers
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SplitError {
    /// A pool is needed and there are no workers at all.
    #[error("workers=0: the sub-interpreter pool needs at least one worker")]
    NoWorkers,
    /// Sync and async handlers each need a worker of their own, and there is one.
    #[error(
        "workers=1 cannot serve both sync (`def`) and async (`async def`) routes: the \
         sub-interpreter pool runs each kind on workers of its own, so it needs at least 2 \
         workers (or handlers of one kind only)"
    )]
    OneWorkerForBothKinds,
}

/// The split of `n` workers for the handler kinds the pool must serve: half (rounded
/// down) async when both are needed, all of them for the only kind needed.
pub(crate) fn split_workers(
    n: usize,
    has_sync: bool,
    has_async: bool,
) -> Result<WorkerSplit, SplitError> {
    let split = |sync_workers, async_workers| WorkerSplit {
        sync_workers,
        async_workers,
    };
    match (has_sync, has_async) {
        (false, false) => Ok(split(n, 0)),
        _ if n == 0 => Err(SplitError::NoWorkers),
        (true, true) if n == 1 => Err(SplitError::OneWorkerForBothKinds),
        (true, true) => Ok(split(n - n / 2, n / 2)),
        (true, false) => Ok(split(n, 0)),
        (false, true) => Ok(split(0, n)),
    }
}

/// [`split_workers`] for a route table: a `gil=True` route runs on the main interpreter,
/// every other route on a pool worker of its handler's kind.
pub(crate) fn split_workers_for_routes(
    n: usize,
    requires_gil: &[bool],
    is_async: &[bool],
) -> Result<WorkerSplit, SplitError> {
    let pooled_kinds = || {
        requires_gil
            .iter()
            .zip(is_async)
            .filter(|(&gil, _)| !gil)
            .map(|(_, &is_async)| is_async)
    };
    split_workers(
        n,
        pooled_kinds().any(|is_async| !is_async),
        pooled_kinds().any(|is_async| is_async),
    )
}

// ---------------------------------------------------------------------------
// Channel-based Interpreter Pool
// ---------------------------------------------------------------------------

/// Why the sub-interpreter pool could not start.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PoolError {
    #[error(transparent)]
    Worker(#[from] WorkerStartError),
    #[error("could not spawn the thread of worker {index}: {source}")]
    Spawn {
        index: usize,
        source: std::io::Error,
    },
}

impl PoolError {
    /// This error as the main interpreter raises it (see [`WorkerStartError::into_pyerr`]).
    pub(crate) fn into_pyerr(self, py: Python<'_>) -> PyErr {
        match self {
            PoolError::Worker(e) => e.into_pyerr(py),
            spawn @ PoolError::Spawn { .. } => {
                pyo3::exceptions::PyRuntimeError::new_err(spawn.to_string())
            }
        }
    }
}

/// A worker thread a pool shutdown gave up on (still running after the grace period), with
/// what it was running. Its interpreter is still alive, and finalizing with a live
/// sub-interpreter aborts, so `Pyronova.run()` exits non-zero instead (Layer 2, design §12,
/// M4 review N8).
#[pyclass(module = "pyronova.engine", frozen, get_all)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AbandonedWorker {
    /// The worker thread's name.
    pub(crate) thread: String,
    /// `METHOD path` of the route its sync worker was running, if any.
    pub(crate) route: Option<String>,
}

#[pymethods]
impl AbandonedWorker {
    fn __str__(&self) -> String {
        self.to_string()
    }

    fn __repr__(&self) -> String {
        format!("AbandonedWorker({self})")
    }
}

impl std::fmt::Display for AbandonedWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.route {
            Some(route) => write!(f, "{} (running {route})", self.thread),
            None => f.write_str(&self.thread),
        }
    }
}

/// `running` value of a worker that isn't running a handler.
const IDLE: usize = usize::MAX;

/// How long a worker thread gets to finish its request and end its interpreter once the
/// pool is closed.
const JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Where a server submits requests to its sub-interpreter workers. Dropping the last
/// reference closes the channels, which tells the workers to finish; [`PoolThreads::join`]
/// then waits for them.
pub(crate) struct InterpreterPool {
    sync_work_tx: crossbeam_channel::Sender<WorkRequest>,
    async_work_tx: Option<crossbeam_channel::Sender<WorkRequest>>,
    /// Admission-control gate: one permit per slot in the work channels. Callers take one
    /// before collecting a large request body, so an over-capacity surge of uploads
    /// doesn't pile N × max_body_size up in RAM while waiting for a full queue; the permit
    /// is held until the response is ready (`handle_request_subinterp`).
    pub(crate) submit_semaphore: Arc<tokio::sync::Semaphore>,
}

/// A pool's worker threads, joined when its server stops.
pub(crate) struct PoolThreads {
    threads: Vec<WorkerThread>,
    /// `METHOD path` per route index, for naming an abandoned worker's route.
    route_names: Vec<String>,
}

struct WorkerThread {
    handle: std::thread::JoinHandle<()>,
    /// The route index its sync worker is running, or `IDLE`; async workers stay `IDLE`.
    /// Only read when a shutdown abandons the worker.
    running: Arc<AtomicUsize>,
}

impl InterpreterPool {
    /// Create `split.total()` sub-interpreters, each in its own OS thread, connected via
    /// channels: the first `split.sync_workers` serve `def` handlers, the rest `async def`.
    ///
    /// # Safety
    /// Must be called with the main interpreter's thread state current (before
    /// `py.detach()`).
    pub unsafe fn new(
        split: WorkerSplit,
        spec: &WorkerSpec<'_>,
    ) -> Result<(Self, PoolThreads), PoolError> {
        let n = split.total();
        let has_any_async = split.async_workers > 0;
        let (sync_work_tx, sync_work_rx) = crossbeam_channel::bounded::<WorkRequest>(n * 128);
        let (async_work_tx, async_work_rx) = crossbeam_channel::bounded::<WorkRequest>(n * 128);
        // One permit per queue slot of the channels in use, so a permit-holder always finds
        // a slot when it reaches `submit`.
        let total_permits = n * 128 * if has_any_async { 2 } else { 1 };

        let (sync_workers, async_workers) = build_workers(split, spec)?;
        let pool = InterpreterPool {
            sync_work_tx,
            async_work_tx: has_any_async.then_some(async_work_tx),
            submit_semaphore: Arc::new(tokio::sync::Semaphore::new(total_permits)),
        };
        let mut threads = PoolThreads {
            threads: Vec::with_capacity(split.total()),
            route_names: spec
                .expected
                .routes
                .iter()
                .map(|(method, path, _)| format!("{method} {path}"))
                .collect(),
        };

        // If a spawn fails, the workers not yet handed to a thread are ended here (FR-19),
        // and the ones already running are stopped (the pool closes) and joined.
        let mut sync_pending = sync_workers.into_iter().enumerate();
        while let Some((i, worker)) = sync_pending.next() {
            let running = Arc::new(AtomicUsize::new(IDLE));
            let (rx, current) = (sync_work_rx.clone(), Arc::clone(&running));
            match spawn(format!("pyronova-worker-{i}"), move || {
                serve_sync(worker, rx, &current)
            }) {
                Ok(handle) => threads.threads.push(WorkerThread { handle, running }),
                Err(source) => {
                    SubInterpreterWorker::end_all(sync_pending.map(|(_, w)| w));
                    SubInterpreterWorker::end_all(async_workers);
                    return Err(stop_started(pool, threads, i, source));
                }
            }
        }
        let mut async_pending = async_workers.into_iter().enumerate();
        while let Some((j, worker)) = async_pending.next() {
            let i = split.sync_workers + j;
            let inbox = AsyncInbox::new(async_work_rx.clone());
            match spawn(format!("pyronova-async-worker-{i}"), move || {
                serve_async(worker, inbox)
            }) {
                Ok(handle) => threads.threads.push(WorkerThread {
                    handle,
                    running: Arc::new(AtomicUsize::new(IDLE)),
                }),
                Err(source) => {
                    SubInterpreterWorker::end_all(async_pending.map(|(_, w)| w));
                    return Err(stop_started(pool, threads, i, source));
                }
            }
        }

        Ok((pool, threads))
    }

    /// Queue a request on the worker kind that runs it. Never waits: a full queue is an
    /// error the caller answers with 503.
    pub fn submit(&self, req: WorkRequest) -> Result<(), SubmitError> {
        // `async_work_tx` is set iff the split has async workers, which it has whenever a
        // pooled route is `async def` (`split_workers_for_routes`).
        let tx = match (req.kind, self.async_work_tx.as_ref()) {
            (crate::router::HandlerKind::Async, Some(tx)) => tx,
            _ => &self.sync_work_tx,
        };
        tx.try_send(req).map_err(|e| match e {
            crossbeam_channel::TrySendError::Full(_) => SubmitError::Full,
            crossbeam_channel::TrySendError::Disconnected(_) => SubmitError::Closed,
        })
    }
}

/// Builds the sync workers, then the async ones, in index order on this (the main) thread.
/// If one fails, the ones built so far are ended here, on their creating thread (FR-19).
///
/// # Safety
/// As for [`InterpreterPool::new`].
#[allow(clippy::type_complexity)]
unsafe fn build_workers(
    split: WorkerSplit,
    spec: &WorkerSpec<'_>,
) -> Result<
    (
        Vec<SubInterpreterWorker>,
        Vec<SubInterpreterWorker<AsyncEngine>>,
    ),
    PoolError,
> {
    let mut sync_workers = Vec::with_capacity(split.sync_workers);
    for i in 0..split.sync_workers {
        match SubInterpreterWorker::new(i, spec) {
            Ok(worker) => sync_workers.push(worker),
            Err(e) => {
                SubInterpreterWorker::end_all(sync_workers);
                return Err(e.into());
            }
        }
    }
    let mut async_workers = Vec::with_capacity(split.async_workers);
    for i in split.sync_workers..split.total() {
        match SubInterpreterWorker::<AsyncEngine>::new(i, spec) {
            Ok(worker) => async_workers.push(worker),
            Err(e) => {
                SubInterpreterWorker::end_all(sync_workers);
                SubInterpreterWorker::end_all(async_workers);
                return Err(e.into());
            }
        }
    }
    Ok((sync_workers, async_workers))
}

/// A start that failed spawning worker `index`'s thread: closes the pool, so the threads
/// already running end their workers, and waits for them.
fn stop_started(
    pool: InterpreterPool,
    threads: PoolThreads,
    index: usize,
    source: std::io::Error,
) -> PoolError {
    drop(pool);
    for abandoned in threads.join() {
        tracing::error!(
            target: "pyronova::server",
            "worker thread {abandoned} did not stop after a failed pool start"
        );
    }
    PoolError::Spawn { index, source }
}

fn spawn(
    name: String,
    serve: impl FnOnce() + Send + 'static,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name(name)
        .stack_size(crate::python::PYTHON_THREAD_STACK)
        .spawn(serve)
}

impl PoolThreads {
    /// Waits for every worker thread to end its interpreter, each for up to
    /// [`JOIN_TIMEOUT`], and returns the ones still running then. The pool's senders must be
    /// gone (every [`InterpreterPool`] reference dropped), or the workers never finish.
    ///
    /// A worker's handler can block forever (a `requests.get` with no timeout); waiting for
    /// it would hang the process, so it is abandoned instead: its thread is leaked, and the
    /// caller must not finalize the runtime while it lives.
    pub(crate) fn join(self) -> Vec<AbandonedWorker> {
        self.join_within(JOIN_TIMEOUT)
    }

    fn join_within(self, timeout: std::time::Duration) -> Vec<AbandonedWorker> {
        let PoolThreads {
            threads,
            route_names,
        } = self;
        threads
            .into_iter()
            .filter_map(|thread| thread.join_or_abandon(timeout, &route_names))
            .collect()
    }
}

impl WorkerThread {
    fn join_or_abandon(
        self,
        timeout: std::time::Duration,
        route_names: &[String],
    ) -> Option<AbandonedWorker> {
        // `JoinHandle` has no timed join: poll `is_finished` (an atomic read).
        let deadline = std::time::Instant::now() + timeout;
        while !self.handle.is_finished() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        if self.handle.is_finished() {
            if let Err(panic) = self.handle.join() {
                let msg = crate::error::panic_message(&*panic);
                tracing::error!(
                    target: "pyronova::server",
                    "worker thread panicked during shutdown: {msg}",
                );
            }
            return None;
        }
        let abandoned = AbandonedWorker {
            thread: self.handle.thread().name().unwrap_or("worker").to_string(),
            route: route_names
                .get(self.running.load(Ordering::Relaxed))
                .cloned(),
        };
        tracing::error!(
            target: "pyronova::server",
            "worker thread {abandoned} did not exit within {timeout:?}; abandoning it",
        );
        // Leak the JoinHandle — the OS reclaims the thread at process exit.
        std::mem::forget(self.handle);
        Some(abandoned)
    }
}

/// Why [`InterpreterPool::submit`] could not queue a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmitError {
    /// Every queue slot is taken.
    Full,
    /// The workers are gone (shutdown).
    Closed,
}

/// A sync worker's thread: runs each request it takes until the channel closes.
fn serve_sync(
    mut worker: SubInterpreterWorker,
    rx: crossbeam_channel::Receiver<WorkRequest>,
    running: &AtomicUsize,
) {
    // SAFETY: this thread now owns the worker, which no other thread has bound.
    unsafe { worker.bind_to_this_thread() };

    for req in rx.iter() {
        // Skip requests whose caller already timed out (504): don't spend CPU on dead
        // requests during a backlog.
        if req.response_tx.is_closed() {
            WorkRequest::inc_completed();
            continue;
        }

        let (route, request, reply) = req.into_request();
        // The request as its error log line names it; `request` moves into the call.
        let label = request.label();

        running.store(route.index(), Ordering::Relaxed);
        // SAFETY: on the thread this worker was bound to, no thread state current.
        let result = unsafe { worker.serve(route, request) };
        running.store(IDLE, Ordering::Relaxed);

        // The caller may have given up (504) meanwhile; the error is logged either way.
        let _ = reply.send(result.map_err(|e| e.log(&label.tag())));
        WorkRequest::inc_completed();
    }

    worker.end_on_own_thread();
}

/// An async worker's thread: runs the async engine (`_async_engine.py`) on the worker's
/// interpreter until its inbox closes or the engine stops. Requests the engine still held
/// when it stopped were answered as it let go of them (see `AsyncJob`).
fn serve_async(mut worker: SubInterpreterWorker<AsyncEngine>, inbox: AsyncInbox) {
    // SAFETY: this thread now owns the worker, which no other thread has bound.
    unsafe { worker.bind_to_this_thread() };

    // SAFETY: on the thread this worker was bound to, no thread state current.
    if let Err(e) = unsafe { worker.run_async_engine(inbox) } {
        tracing::error!(
            target: "pyronova::server",
            worker = worker.worker_id(),
            error = %e,
            "async worker stopped serving"
        );
    }

    worker.end_on_own_thread();
}

#[cfg(test)]
mod split_tests {
    use super::*;

    fn split(sync_workers: usize, async_workers: usize) -> Result<WorkerSplit, SplitError> {
        Ok(WorkerSplit {
            sync_workers,
            async_workers,
        })
    }

    #[test]
    fn both_kinds_share_the_workers_async_half_rounded_down() {
        assert_eq!(split_workers(0, true, true), Err(SplitError::NoWorkers));
        assert_eq!(
            split_workers(1, true, true),
            Err(SplitError::OneWorkerForBothKinds)
        );
        assert_eq!(split_workers(2, true, true), split(1, 1));
        assert_eq!(split_workers(3, true, true), split(2, 1));
        assert_eq!(split_workers(4, true, true), split(2, 2));
        assert_eq!(split_workers(5, true, true), split(3, 2));
    }

    #[test]
    fn one_kind_gets_every_worker() {
        for n in 1..=4 {
            assert_eq!(split_workers(n, true, false), split(n, 0));
            assert_eq!(split_workers(n, false, true), split(0, n));
            assert_eq!(split_workers(n, false, false), split(n, 0));
        }
        assert_eq!(split_workers(0, true, false), Err(SplitError::NoWorkers));
        assert_eq!(split_workers(0, false, true), Err(SplitError::NoWorkers));
    }

    #[test]
    fn a_needed_kind_never_gets_zero_workers() {
        for n in 0..=8 {
            for (has_sync, has_async) in [(true, true), (true, false), (false, true)] {
                if let Ok(s) = split_workers(n, has_sync, has_async) {
                    assert_eq!(s.total(), n);
                    assert!(!has_sync || s.sync_workers > 0, "n={n}: {s:?}");
                    assert!(!has_async || s.async_workers > 0, "n={n}: {s:?}");
                }
            }
        }
    }

    #[test]
    fn gil_routes_need_no_pool_worker() {
        // sync, async gil=True, sync gil=True
        assert_eq!(
            split_workers_for_routes(1, &[false, true, true], &[false, true, false]),
            split(1, 0)
        );
        // sync, async
        assert_eq!(
            split_workers_for_routes(1, &[false, false], &[false, true]),
            Err(SplitError::OneWorkerForBothKinds)
        );
        // async, async
        assert_eq!(
            split_workers_for_routes(3, &[false, false], &[true, true]),
            split(0, 3)
        );
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    const SHORT: std::time::Duration = std::time::Duration::from_millis(200);

    fn thread_running(
        name: &str,
        release: crossbeam_channel::Receiver<()>,
        route: usize,
    ) -> WorkerThread {
        let handle = std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let _ = release.recv();
            })
            .unwrap();
        WorkerThread {
            handle,
            running: Arc::new(AtomicUsize::new(route)),
        }
    }

    #[test]
    fn join_returns_the_abandoned_workers_to_its_caller() {
        let (release, blocked) = crossbeam_channel::bounded::<()>(0);
        let (_done, finished) = crossbeam_channel::bounded::<()>(0);
        drop(_done);
        let threads = PoolThreads {
            threads: vec![
                thread_running("w-finished", finished, IDLE),
                thread_running("w-stuck", blocked, 1),
            ],
            route_names: vec!["GET /a".into(), "GET /stuck".into()],
        };
        let abandoned = threads.join_within(SHORT);
        assert_eq!(
            abandoned,
            vec![AbandonedWorker {
                thread: "w-stuck".into(),
                route: Some("GET /stuck".into()),
            }]
        );
        assert_eq!(abandoned[0].to_string(), "w-stuck (running GET /stuck)");
        drop(release);
    }

    #[test]
    fn two_pools_report_their_own_abandoned_workers() {
        // Each pool's join returns only its own threads: nothing is shared through a
        // process-wide list, so one server's stuck worker never reaches another's run.
        let (release, blocked) = crossbeam_channel::bounded::<()>(0);
        let stuck = PoolThreads {
            threads: vec![thread_running("pool-a", blocked, IDLE)],
            route_names: Vec::new(),
        };
        let (_done, finished) = crossbeam_channel::bounded::<()>(0);
        drop(_done);
        let clean = PoolThreads {
            threads: vec![thread_running("pool-b", finished, IDLE)],
            route_names: Vec::new(),
        };
        assert_eq!(clean.join_within(SHORT), vec![]);
        assert_eq!(stuck.join_within(SHORT).len(), 1);
        drop(release);
    }

    #[test]
    fn split_errors_render_their_reason() {
        assert!(SplitError::NoWorkers.to_string().starts_with("workers=0"));
        assert!(SplitError::OneWorkerForBothKinds
            .to_string()
            .contains("needs at least 2 workers"));
    }
}
