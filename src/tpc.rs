//! Thread-Per-Core (TPC) accept layer, the default server. See docs/tpc-rearch.md.
//!
//! Each TPC thread owns a pinned OS thread, a
//! `tokio::runtime::Builder::new_current_thread()` runtime, a
//! `LocalSet` for spawn_local tasks, and its own `SO_REUSEPORT`
//! listeners (bound before any thread exists); in sub-interpreter mode it
//! also owns one worker and runs its handlers inline. `PYRONOVA_TPC=0`
//! serves through the multi-thread pool instead (`app.rs`).
//!
//! Why no `Send` bounds on the per-connection future? Because
//! `LocalSet::spawn_local` runs the task on the same OS thread that
//! owns the LocalSet — no cross-thread move ever happens. This is also
//! why we don't pay work-stealing cost on this path: there is no other
//! worker to steal from.

use std::future::Future;
use std::rc::Rc;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::bridge::main_bridge::MainInterpBridge;
use crate::config::{DarwinTopology, GcConfig, GC_MODE_ENV};
use crate::error::panic_message;
use crate::handlers::handle_request;
use crate::python::interp::SubInterpreterWorker;
use crate::server::listener::{
    AcceptSource, Accepted, Bound, BoundListeners, Listener, ListenerError,
};
use crate::site::{SharedSite, Site};
use crate::websocket;
use crate::worker::{drive_connection, drive_tcp_conn, LocalExec, TpcContext};

/// How long a TPC thread's in-flight connections get to finish after a stop.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Pin the current OS thread to a specific CPU core if one is
/// available. Silently no-ops on platforms where `core_affinity`
/// can't enumerate (e.g. restricted containers with no CPU mask
/// visibility); in that case the OS scheduler still gets us
/// statistically-close-to-core-local execution on the per-thread
/// runtime because the runtime never migrates tasks, only the
/// kernel can move the thread.
pub(crate) fn try_pin_current(core_id: Option<core_affinity::CoreId>) {
    if let Some(c) = core_id {
        let _ = core_affinity::set_for_current(c);
    }
}

/// macOS-only: bump the calling thread's QoS class to
/// USER_INTERACTIVE. core_affinity::set_for_current is a silent
/// no-op on Darwin (no public CPU-pinning API), so without this
/// the scheduler is free to park TPC threads on E-cores for
/// power savings — fatal under TPC because there is no work-
/// stealing across threads. USER_INTERACTIVE tells the scheduler
/// to keep us on P-cores and ignore power hints, at the cost of
/// giving up energy-efficiency on idle machines. Acceptable
/// tradeoff for a throughput-first server.
#[cfg(target_os = "macos")]
pub(crate) fn elevate_thread_qos_macos() {
    use std::os::raw::c_int;
    // Opaque qos_class_t. 0x21 == QOS_CLASS_USER_INTERACTIVE per
    // <sys/qos.h>. Keeping the constant inline avoids pulling in
    // the whole qos.h shim; the value has been stable since 10.10.
    const QOS_CLASS_USER_INTERACTIVE: c_int = 0x21;
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: c_int, relative_priority: c_int) -> c_int;
    }
    unsafe {
        let rc = pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        // Silent failure here can park TPC threads on E-cores — fatal
        // for throughput per the doc comment above ("no work-stealing
        // across threads"). Log so the perf regression is visible
        // before it shows up as a benchmark drop (arc finding tpc-1).
        if rc != 0 {
            tracing::warn!(
                target: "pyronova::server",
                rc,
                "pthread_set_qos_class_self_np failed; TPC thread may be \
                 scheduled on E-cores — expect throughput collapse"
            );
        }
    }
}

#[cfg(not(target_os = "macos"))]
#[inline(always)]
pub(crate) fn elevate_thread_qos_macos() {}

/// Log line emitted once on startup so operators can see the TPC topology.
///
/// `site` is used to surface the gil / async / sub-interp split so the
/// operator knows up front how many routes will go through the
/// main_bridge versus the per-thread TPC fleet.
fn log_startup(mode: &str, bound: &[Bound], n_threads: usize, n_cpus: usize, site: &Site) {
    let shape = crate::router::RouteShape::of(&site.routes);
    let gil_count = shape.gil_count();
    let async_count = shape.async_count();
    let stream_count = shape.streamed;
    let total = shape.gil.len();
    let subinterp_count = total - gil_count - async_count;
    tracing::info!(
        target: "pyronova::server",
        version = env!("CARGO_PKG_VERSION"),
        mode,
        tpc = true,
        listening = ?bound,
        tpc_threads = n_threads,
        cpus = n_cpus,
        routes_total = total,
        routes_subinterp = subinterp_count,
        routes_gil = gil_count,
        routes_async = async_count,
        routes_stream = stream_count,
        "Pyronova started"
    );
    println!(
        "\n  Pyronova v{} [TPC mode, {mode}]",
        env!("CARGO_PKG_VERSION")
    );
    for listener in bound {
        println!("  Listening on {listener}");
    }
    println!("  TPC threads: {n_threads} (CPUs: {n_cpus}, pinned)");
    println!(
        "  Routes: {subinterp_count} sub-interp + {gil_count} GIL + {async_count} async{stream_suffix}\n",
        stream_suffix = if stream_count > 0 {
            format!(" ({stream_count} stream)")
        } else {
            String::new()
        }
    );
}

/// The thread that stops a TPC server on SIGINT (`crate::app::until_stopped`). Dropping it
/// cancels the stop token and joins the thread, so a run leaves no watcher behind on any
/// exit path, including accept loops that ended without a stop.
struct StopWatcher {
    stop: CancellationToken,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn spawn_stop_watcher(stop: CancellationToken) -> Result<StopWatcher, ServeError> {
    let watched = stop.clone();
    let thread = std::thread::Builder::new()
        .name("pyronova-stop".into())
        .spawn(move || match RuntimeBuilder::new_current_thread().enable_all().build() {
            Ok(rt) => rt.block_on(crate::app::until_stopped(watched)),
            Err(e) => {
                // Without its runtime the watcher can't see SIGINT; stop rather than
                // serve a server that ignores it.
                tracing::error!(target: "pyronova::server", error = %e, "stop-watcher runtime could not be built; stopping");
                watched.cancel();
            }
        })
        .map_err(|source| ServeError::Spawn {
            thread: "pyronova-stop".into(),
            source,
        })?;
    Ok(StopWatcher {
        stop,
        thread: Some(thread),
    })
}

impl Drop for StopWatcher {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(thread) = self.thread.take() {
            if let Err(payload) = thread.join() {
                tracing::error!(
                    target: "pyronova::server",
                    panic = %panic_message(payload.as_ref()),
                    "stop watcher thread panicked"
                );
            }
        }
    }
}

/// Why a TPC server run could not start, or failed while serving.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ServeError {
    #[error("could not spawn thread {thread}: {source}")]
    Spawn {
        thread: String,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Listener(#[from] ListenerError),
    #[error("thread {thread} could not build its runtime: {source}")]
    Runtime {
        thread: String,
        #[source]
        source: std::io::Error,
    },
    #[error("thread {thread} panicked: {payload}")]
    Panicked { thread: String, payload: String },
    #[error("{workers} TPC workers for {groups} listener groups")]
    WorkerCount { workers: usize, groups: usize },
}

impl From<ServeError> for pyo3::PyErr {
    fn from(e: ServeError) -> Self {
        match e {
            ServeError::Listener(e) => e.into(),
            other => pyo3::exceptions::PyRuntimeError::new_err(other.to_string()),
        }
    }
}

type ThreadHandle = (String, std::thread::JoinHandle<Result<(), ServeError>>);

/// Runs `serve` on this (new) TPC thread: pinned to `core`, QoS raised, on a
/// current-thread runtime and a `LocalSet`, both gone when this returns.
fn serve_on_this_thread<Fut>(
    thread: &str,
    core: Option<core_affinity::CoreId>,
    serve: impl FnOnce() -> Fut,
) -> Result<(), ServeError>
where
    Fut: Future<Output = Result<(), ServeError>>,
{
    try_pin_current(core);
    elevate_thread_qos_macos();
    let rt = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| ServeError::Runtime {
            thread: thread.to_string(),
            source,
        })?;
    let local = LocalSet::new();
    let served = local.block_on(&rt, serve());
    // Its tasks hold the thread's `Rc`s; they go before the caller ends the worker.
    drop(local);
    served
}

/// Joins every thread. Each failure is logged; the first one is returned.
fn join_all(handles: Vec<ThreadHandle>) -> Result<(), ServeError> {
    let mut first = None;
    for (thread, handle) in handles {
        let failure = match handle.join() {
            Ok(Ok(())) => continue,
            Ok(Err(e)) => e,
            Err(payload) => ServeError::Panicked {
                thread,
                payload: panic_message(payload.as_ref()),
            },
        };
        tracing::error!(target: "pyronova::server", error = %failure, "TPC thread failed");
        first.get_or_insert(failure);
    }
    first.map_or(Ok(()), Err)
}

/// Lets the in-flight connections of a stopped accept loop finish, up to
/// [`DRAIN_TIMEOUT`].
async fn drain(tracker: TaskTracker) {
    tracker.close();
    if tokio::time::timeout(DRAIN_TIMEOUT, tracker.wait())
        .await
        .is_err()
    {
        tracing::warn!(
            target: "pyronova::server",
            open = tracker.len(),
            "connections did not finish within {DRAIN_TIMEOUT:?} of the stop; closing them"
        );
    }
}

// ---------------------------------------------------------------------------
// GIL mode — every handler on the main interpreter
// ---------------------------------------------------------------------------

/// One pinned TPC thread per listener group, each serving every handler on the main
/// interpreter. A thread that fails stops the server; its error is returned.
pub(crate) fn run_tpc_gil(
    listeners: BoundListeners,
    n_cpus: usize,
    site: SharedSite,
    shutdown: CancellationToken,
) -> Result<(), ServeError> {
    let n_threads = listeners.groups.len();
    log_startup("gil", &listeners.bound, n_threads, n_cpus, &site);

    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let _watcher = spawn_stop_watcher(shutdown.clone())?;

    let mut handles = Vec::with_capacity(n_threads);
    for (i, group) in listeners.groups.into_iter().enumerate() {
        let thread = format!("pyronova-tpc-{i}");
        let core = core_ids.get(i).copied();
        let (site, stop, label) = (Arc::clone(&site), shutdown.clone(), thread.clone());
        let spawned = std::thread::Builder::new()
            .name(thread.clone())
            .stack_size(crate::python::PYTHON_THREAD_STACK)
            .spawn(move || {
                let served = serve_on_this_thread(&label, core, || {
                    tpc_accept_loop_gil(group, site, stop.clone())
                });
                if served.is_err() {
                    stop.cancel();
                }
                served
            });
        match spawned {
            Ok(handle) => handles.push((thread, handle)),
            Err(source) => {
                shutdown.cancel();
                // Their failures, if any, are logged there; the spawn is the cause.
                let _logged = join_all(handles);
                return Err(ServeError::Spawn { thread, source });
            }
        }
    }
    join_all(handles)
}

async fn tpc_accept_loop_gil(
    group: Vec<Listener>,
    site: SharedSite,
    shutdown: CancellationToken,
) -> Result<(), ServeError> {
    let mut source = AcceptSource::new(group)?;
    let tracker = TaskTracker::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            accepted = source.accept() => {
                tracker.spawn_local(serve_main_conn(accepted, Arc::clone(&site), shutdown.clone()));
            }
        }
    }
    drain(tracker).await;
    Ok(())
}

/// A GIL-mode connection on a TPC thread: every request runs on the main interpreter.
async fn serve_main_conn(accepted: Accepted, site: SharedSite, conn_token: CancellationToken) {
    let client_ip = accepted.remote.ip();
    let Some(stream) = crate::tls::wrap(accepted.stream, accepted.tls.as_deref()).await else {
        return;
    };
    let svc = service_fn(move |req: Request<Incoming>| {
        let site = Arc::clone(&site);
        async move {
            if websocket::wants_websocket(&req, &site) {
                websocket::handle_websocket(req, site, client_ip).await
            } else {
                handle_request(req, site, client_ip).await
            }
        }
    });
    drive_connection(stream, svc, LocalExec, conn_token).await;
}

// ---------------------------------------------------------------------------
// Sub-interpreter mode — each TPC thread owns a worker and runs its handlers inline
// ---------------------------------------------------------------------------

/// A TPC sub-interpreter server: what `run_tpc_subinterp` serves with.
pub(crate) struct TpcServer {
    /// One per TPC thread, built on the main thread.
    pub(crate) workers: Vec<SubInterpreterWorker>,
    pub(crate) site: SharedSite,
    /// Runs `gil=True` routes and the fallback on the main interpreter; `None` when the
    /// table has none.
    pub(crate) bridge: Option<Arc<MainInterpBridge>>,
    pub(crate) gc: GcConfig,
    pub(crate) topology: DarwinTopology,
}

/// Each TPC thread owns one worker and executes handlers synchronously on the accept
/// thread: no shared pool, no channel, no oneshot wake. Every worker ends on the thread
/// that served it, or here if it was never handed to one.
pub(crate) fn run_tpc_subinterp(
    listeners: BoundListeners,
    n_cpus: usize,
    server: TpcServer,
    shutdown: CancellationToken,
) -> Result<(), ServeError> {
    // Darwin: kqueue-backed SO_REUSEPORT routes ~all traffic to one
    // listener (last-socket-wins). We keep the per-thread-listener
    // default anyway because localhost benchmarking shows it still
    // wins: fanout's cross-thread wake cost + client/server CPU
    // contention on a single machine outweighs the distribution
    // benefit. The fanout topology stays behind an env opt-in for
    // real-NIC testing and hardware where the loopback isn't the
    // bottleneck. Set `PYRONOVA_TPC_DARWIN=fanout` to opt in.
    let groups = match server.topology {
        DarwinTopology::PerThreadListener => server.workers.len(),
        DarwinTopology::Fanout => 1,
    };
    if listeners.groups.len() != groups {
        let err = ServeError::WorkerCount {
            workers: server.workers.len(),
            groups: listeners.groups.len(),
        };
        // SAFETY: on the main thread inside `py.detach` (no thread state current); none of
        // the workers was rebound.
        unsafe { SubInterpreterWorker::end_all(server.workers) };
        return Err(err);
    }
    match server.topology {
        #[cfg(target_os = "macos")]
        DarwinTopology::Fanout => run_fanout(listeners, n_cpus, server, shutdown),
        _ => run_per_thread_listener(listeners, n_cpus, server, shutdown),
    }
}

/// The stop watcher, or the run's end: a server that can't watch for SIGINT doesn't
/// start, and its workers end here.
fn watch_or_end(
    shutdown: &CancellationToken,
    workers: &mut Vec<SubInterpreterWorker>,
) -> Result<StopWatcher, ServeError> {
    spawn_stop_watcher(shutdown.clone()).inspect_err(|_| {
        // SAFETY: on the main thread inside `py.detach`; none of the workers was rebound.
        unsafe { SubInterpreterWorker::end_all(workers.drain(..)) };
    })
}

fn run_per_thread_listener(
    listeners: BoundListeners,
    n_cpus: usize,
    mut server: TpcServer,
    shutdown: CancellationToken,
) -> Result<(), ServeError> {
    let n_threads = server.workers.len();
    log_startup(
        "hybrid-inline",
        &listeners.bound,
        n_threads,
        n_cpus,
        &server.site,
    );
    let _watcher = watch_or_end(&shutdown, &mut server.workers)?;

    let gc = server.gc;
    let handoffs = server.workers.into_iter().zip(listeners.groups).collect();
    let handles = spawn_worker_threads(
        "tpc",
        handoffs,
        &server.site,
        &server.bridge,
        &shutdown,
        move |context, group, stop| tpc_accept_loop_inline(group, context, stop, gc),
    )?;
    join_all(handles)
}

/// One pinned thread per `(worker, payload)`. A worker is handed over only after its
/// thread exists: the thread rebinds it, wraps it in its [`TpcContext`], runs `serve`,
/// and ends it there. If a spawn fails, the threads already running are stopped and
/// joined, and every worker not handed over — the failed thread's included — is ended
/// here, on the thread that built them (FR-19). A thread whose `serve` fails stops the
/// server.
fn spawn_worker_threads<T, F, Fut>(
    name: &str,
    handoffs: Vec<(SubInterpreterWorker, T)>,
    site: &SharedSite,
    bridge: &Option<Arc<MainInterpBridge>>,
    shutdown: &CancellationToken,
    serve: F,
) -> Result<Vec<ThreadHandle>, ServeError>
where
    T: Send + 'static,
    F: Fn(Rc<TpcContext>, T, CancellationToken) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), ServeError>> + 'static,
{
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let mut handles = Vec::with_capacity(handoffs.len());
    let mut pending = handoffs.into_iter().enumerate();
    while let Some((i, handoff)) = pending.next() {
        let thread = format!("pyronova-{name}-{i}");
        let core = core_ids.get(i).copied();
        let (tx, rx) = mpsc::sync_channel::<(SubInterpreterWorker, T)>(1);
        let serve = serve.clone();
        let (site, bridge, stop) = (Arc::clone(site), bridge.clone(), shutdown.clone());
        let label = thread.clone();
        let spawned = std::thread::Builder::new()
            .name(thread.clone())
            .stack_size(crate::python::PYTHON_THREAD_STACK)
            .spawn(move || {
                // No worker means the spawner ended it after a failure of its own.
                let Ok((mut worker, payload)) = rx.recv() else {
                    return Ok(());
                };
                // SAFETY: this thread now owns the worker, which no other thread has bound.
                worker.tstate = unsafe {
                    crate::python::interp::rebind_tstate_to_current_thread(worker.tstate)
                };
                let context = Rc::new(TpcContext {
                    worker: std::cell::RefCell::new(worker),
                    site,
                    bridge,
                });
                let served = serve_on_this_thread(&label, core, || {
                    serve(Rc::clone(&context), payload, stop.clone())
                });
                TpcContext::end(context);
                if served.is_err() {
                    stop.cancel();
                }
                served
            });
        match spawned {
            Ok(handle) => {
                handles.push((thread, handle));
                // The thread's only receiver is waiting; a one-slot channel never blocks.
                if let Err(mpsc::SendError(unsent)) = tx.send(handoff) {
                    // SAFETY: on the creating thread inside `py.detach`; never rebound.
                    unsafe { SubInterpreterWorker::end_all([unsent.0]) };
                }
            }
            Err(source) => {
                shutdown.cancel();
                // Their failures, if any, are logged there; the spawn is the cause.
                let _logged = join_all(handles);
                let unsent = std::iter::once(handoff)
                    .chain(pending.map(|(_, h)| h))
                    .map(|(worker, _)| worker);
                // SAFETY: on the creating thread inside `py.detach` (no thread state
                // current); none of these was handed to a thread.
                unsafe { SubInterpreterWorker::end_all(unsent) };
                return Err(ServeError::Spawn { thread, source });
            }
        }
    }
    Ok(handles)
}

/// Accepts on this TPC thread's listeners and serves each connection inline on
/// `context`'s worker, until `shutdown`. In idle GC mode it also runs the idle tick.
pub(crate) async fn tpc_accept_loop_inline(
    group: Vec<Listener>,
    context: Rc<TpcContext>,
    shutdown: CancellationToken,
    gc: GcConfig,
) -> Result<(), ServeError> {
    let mut source = AcceptSource::new(group)?;
    // The worker counts requests as it runs them and fires its own count trigger (the
    // threshold, or idle mode's OOM failsafe); this loop adds the idle tick.
    let mut ticker = (gc.mode == GcMode::Idle).then(|| {
        // First tick one period from now: nothing to collect before any request ran.
        tokio::time::interval_at(tokio::time::Instant::now() + gc.idle_tick, gc.idle_tick)
    });
    let mut idle_gc = IdleGc::default();
    let tracker = TaskTracker::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            accepted = source.accept() => {
                tracker.spawn_local(drive_tcp_conn(accepted, Rc::clone(&context), shutdown.clone()));
            }
            () = next_tick(&mut ticker) => idle_gc_tick(&mut idle_gc, &context.worker),
        }
    }
    drain(tracker).await;
    Ok(())
}

/// The idle GC tick, or never (count and off modes).
async fn next_tick(ticker: &mut Option<tokio::time::Interval>) {
    match ticker {
        Some(ticker) => {
            ticker.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// The idle tick: collect on `worker` if it went quiet (see [`IdleGc`]).
fn idle_gc_tick(idle: &mut IdleGc, worker: &std::cell::RefCell<SubInterpreterWorker>) {
    let due = {
        let w = worker.borrow();
        idle.on_tick(w.requests_served(), w.requests_since_collect())
    };
    if due {
        // SAFETY: this TPC thread is the one the worker was rebound to, and the accept
        // loop runs between requests, so no thread state is current.
        unsafe { worker.borrow_mut().collect_garbage_between_requests() };
    }
}

/// A connection the fanout acceptor hands to a worker thread.
#[cfg(target_os = "macos")]
type FannedOut = (
    std::net::TcpStream,
    std::net::SocketAddr,
    Option<Arc<tokio_rustls::TlsAcceptor>>,
);

/// Darwin-only TPC topology: one accept thread feeds N worker threads
/// through per-worker bounded mpsc queues, round-robin. Preserves the
/// current-thread runtime + LocalSet + sub-interp-per-worker model; the
/// only change is where the TcpStream comes from. Pays one cross-thread
/// wake per TCP connection, which is amortized to ~0 under HTTP keep-
/// alive (one wake serves the connection's full request lifetime).
/// Count or off GC only (`GcServer::DarwinFanout`): there is no idle tick here.
#[cfg(target_os = "macos")]
fn run_fanout(
    listeners: BoundListeners,
    n_cpus: usize,
    mut server: TpcServer,
    shutdown: CancellationToken,
) -> Result<(), ServeError> {
    // Bounded per-worker inbox. Capacity is a load-shedding threshold:
    // when a worker falls behind and its inbox fills, the acceptor
    // drops new connections (TCP RST to the client) rather than
    // hoarding file descriptors or queuing unbounded backlog. 1024
    // gives ample slack for burst smoothing while keeping worst-case
    // FD usage bounded at n_threads * 1024.
    const WORKER_INBOX_CAP: usize = 1024;

    let n_threads = server.workers.len();
    log_startup(
        "hybrid-inline-fanout",
        &listeners.bound,
        n_threads,
        n_cpus,
        &server.site,
    );
    let _watcher = watch_or_end(&shutdown, &mut server.workers)?;

    let (txs, rxs): (Vec<_>, Vec<_>) = (0..n_threads)
        .map(|_| tokio::sync::mpsc::channel::<FannedOut>(WORKER_INBOX_CAP))
        .unzip();
    let handoffs = server.workers.into_iter().zip(rxs).collect();
    let mut handles = spawn_worker_threads(
        "tpc",
        handoffs,
        &server.site,
        &server.bridge,
        &shutdown,
        |context, rx, stop| fanout_worker_loop(rx, context, stop),
    )?;

    // Acceptor — dedicated OS thread with its own current_thread runtime so accept()
    // polling doesn't contend with any worker. Pinned to the core after the workers'
    // so TCP work doesn't fight handler execution for the same L1/L2.
    let group = listeners
        .groups
        .into_iter()
        .next()
        .expect("run_tpc_subinterp checked: one listener group");
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let core = (!core_ids.is_empty()).then(|| core_ids[n_threads % core_ids.len()]);
    let stop = shutdown.clone();
    let thread = "pyronova-acceptor".to_string();
    let spawned = std::thread::Builder::new()
        .name(thread.clone())
        .spawn(move || {
            let served = serve_on_this_thread("pyronova-acceptor", core, || {
                fanout_accept_loop(group, txs, stop.clone())
            });
            if served.is_err() {
                stop.cancel();
            }
            served
        });
    match spawned {
        Ok(handle) => handles.push((thread, handle)),
        Err(source) => {
            shutdown.cancel();
            let _logged = join_all(handles);
            return Err(ServeError::Spawn { thread, source });
        }
    }
    join_all(handles)
}

#[cfg(target_os = "macos")]
async fn fanout_accept_loop(
    group: Vec<Listener>,
    txs: Vec<tokio::sync::mpsc::Sender<FannedOut>>,
    shutdown: CancellationToken,
) -> Result<(), ServeError> {
    use tokio::sync::mpsc::error::TrySendError;

    let mut source = AcceptSource::new(group)?;
    let mut next: usize = 0;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            accepted = source.accept() => {
                // Nonblocking, as `TcpStream::from_std` on the worker requires.
                let stream = match accepted.stream.into_std() {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(target: "pyronova::server", error = %e, "into_std failed; dropping the connection");
                        continue;
                    }
                };
                // Round-robin. try_send with load shedding: if the chosen worker's inbox
                // is full, drop the connection (kernel sends RST). Sheds cleanly under
                // overload instead of hoarding FDs or spawning unbounded pending work.
                match txs[next].try_send((stream, accepted.remote, accepted.tls)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        tracing::warn!(
                            target: "pyronova::server",
                            worker = next,
                            "TPC worker inbox full — dropping connection (load shed)"
                        );
                    }
                    Err(TrySendError::Closed(_)) => {
                        // Worker channel closed — either a clean shutdown or a worker
                        // thread failure. Either way, stop so the other workers exit.
                        tracing::error!(
                            target: "pyronova::server",
                            worker = next,
                            "TPC worker channel closed unexpectedly — triggering shutdown"
                        );
                        shutdown.cancel();
                        break;
                    }
                }
                next = (next + 1) % txs.len();
            }
        }
    }
    // Dropping `txs` hangs up every receiver, the workers' exit alongside the token.
    Ok(())
}

#[cfg(target_os = "macos")]
async fn fanout_worker_loop(
    mut rx: tokio::sync::mpsc::Receiver<FannedOut>,
    context: Rc<TpcContext>,
    shutdown: CancellationToken,
) -> Result<(), ServeError> {
    let tracker = TaskTracker::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            received = rx.recv() => match received {
                Some((stream, remote, tls)) => {
                    let stream = match tokio::net::TcpStream::from_std(stream) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!(target: "pyronova::server", error = %e, "TcpStream::from_std failed; dropping the connection");
                            continue;
                        }
                    };
                    let accepted = Accepted { stream, remote, tls };
                    tracker.spawn_local(drive_tcp_conn(accepted, Rc::clone(&context), shutdown.clone()));
                }
                None => break,
            }
        }
    }
    drain(tracker).await;
    Ok(())
}

/// GC scheduling mode, parsed once at startup from `PYRONOVA_GC_MODE` (unset = count):
///   - `count` — the count trigger inside `SubInterpreterWorker::call_handler` fires
///     `gc.collect()` every `PYRONOVA_GC_THRESHOLD` requests per worker. Simple,
///     predictable, can collide with bursty traffic.
///   - `idle` — the TPC accept loop collects once a worker has run requests and then
///     none for a full `PYRONOVA_GC_IDLE_MS` tick (default 100ms), so the pause lands in
///     a lull. The worker's count trigger becomes the OOM failsafe at
///     `PYRONOVA_GC_OOM_FAILSAFE` requests (default 50_000), so sustained traffic can't
///     starve the collector. Needs the per-thread-listener TPC topology.
///   - `off` — no framework-level triggers at all. `gc.disable()` still runs at sub-interp
///     init; users must call `gc.collect()` themselves or accept ref-count-only cleanup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GcMode {
    Count,
    Idle,
    Off,
}

impl std::fmt::Display for GcMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            GcMode::Count => "count",
            GcMode::Idle => "idle",
            GcMode::Off => "off",
        })
    }
}

impl std::str::FromStr for GcMode {
    type Err = GcModeError;

    fn from_str(raw: &str) -> Result<Self, GcModeError> {
        match raw {
            "count" => Ok(GcMode::Count),
            "idle" => Ok(GcMode::Idle),
            "off" => Ok(GcMode::Off),
            _ => Err(GcModeError::Unknown(raw.to_string())),
        }
    }
}

impl GcMode {
    /// This mode, if `server` can run it.
    pub(crate) fn supported_by(self, server: GcServer) -> Result<Self, GcModeError> {
        let supported = match server {
            #[cfg(target_os = "macos")]
            GcServer::DarwinFanout => matches!(self, GcMode::Count | GcMode::Off),
            GcServer::SubInterpreterPool => self == GcMode::Count,
        };
        if supported {
            Ok(self)
        } else {
            Err(GcModeError::Unsupported { mode: self, server })
        }
    }
}

/// A server shape that runs only some GC modes (the TPC per-thread-listener one runs all).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GcServer {
    #[cfg(target_os = "macos")]
    DarwinFanout,
    SubInterpreterPool,
}

impl std::fmt::Display for GcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            #[cfg(target_os = "macos")]
            GcServer::DarwinFanout => "the Darwin fanout TPC topology (PYRONOVA_TPC_DARWIN=fanout)",
            GcServer::SubInterpreterPool => "the sub-interpreter pool (PYRONOVA_TPC=0)",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum GcModeError {
    Unknown(String),
    Unsupported { mode: GcMode, server: GcServer },
}

impl std::fmt::Display for GcModeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcModeError::Unknown(raw) => write!(
                f,
                "{GC_MODE_ENV}={raw:?} is not a GC mode; expected \"count\", \"idle\" or \"off\""
            ),
            GcModeError::Unsupported { mode, server } => {
                write!(f, "{GC_MODE_ENV}={mode} is not supported by {server}")
            }
        }
    }
}

impl std::error::Error for GcModeError {}

/// The idle-mode trigger: collect at a tick when the worker ran requests since its last
/// collect and none since the previous tick.
#[derive(Default)]
struct IdleGc {
    served_at_last_tick: u64,
}

impl IdleGc {
    /// Whether to collect at this tick, given the worker's counters now.
    fn on_tick(&mut self, served: u64, since_collect: u64) -> bool {
        let quiet = served == self.served_at_last_tick;
        self.served_at_last_tick = served;
        quiet && since_collect > 0
    }
}

/// Count physical CPU cores to size the TPC pool.
///
/// Linux: parses /sys/devices/system/cpu/cpu*/topology/thread_siblings_list —
/// the number of unique sibling groups equals the physical core count,
/// stripping SMT.
///
/// macOS: queries `hw.perflevel0.physicalcpu` via sysctl. On Apple
/// Silicon perflevel0 is the performance-core cluster; the efficiency
/// cores at perflevel1 are deliberately excluded. Running a TPC
/// thread on an E-core tanks single-connection throughput to ~1/3,
/// and with no work-stealing that request is stuck — so the whole
/// tail latency collapses. Sizing to P-core count keeps every TPC
/// thread on a fast cluster.
///
/// Other platforms: falls back to logical core count.
#[cfg(target_os = "linux")]
pub(crate) fn physical_core_count() -> usize {
    use std::collections::HashSet;
    use std::fs;

    let Ok(entries) = fs::read_dir("/sys/devices/system/cpu") else {
        return std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
    };

    let mut sibling_groups: HashSet<String> = HashSet::new();
    for e in entries.flatten() {
        let name = e.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("cpu") || !name[3..].chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let path = e.path().join("topology/thread_siblings_list");
        if let Ok(s) = fs::read_to_string(&path) {
            sibling_groups.insert(s.trim().to_string());
        }
    }
    if sibling_groups.is_empty() {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        sibling_groups.len()
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn physical_core_count() -> usize {
    use std::ffi::CString;
    use std::ptr;
    let name = CString::new("hw.perflevel0.physicalcpu").unwrap();
    let mut count: i32 = 0;
    let mut size = std::mem::size_of::<i32>();
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut count as *mut _ as *mut libc::c_void,
            &mut size,
            ptr::null_mut(),
            0,
        )
    };
    if rc == 0 && count > 0 {
        return count as usize;
    }
    // Pre-Apple-Silicon macOS (no perf levels) or older kernels:
    // fall back to logical count.
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn physical_core_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(test)]
mod gc_tests {
    use super::*;

    #[test]
    fn gc_mode_parses_the_three_modes_and_nothing_else() {
        assert_eq!("count".parse(), Ok(GcMode::Count));
        assert_eq!("idle".parse(), Ok(GcMode::Idle));
        assert_eq!("off".parse(), Ok(GcMode::Off));
        for raw in ["idel", "", "IDLE", " idle"] {
            assert_eq!(
                raw.parse::<GcMode>(),
                Err(GcModeError::Unknown(raw.to_string()))
            );
        }
        let message = "idel".parse::<GcMode>().unwrap_err().to_string();
        assert!(message.contains("PYRONOVA_GC_MODE") && message.contains("\"idel\""));
    }

    #[test]
    fn pool_runs_count_mode_only() {
        let pool = GcServer::SubInterpreterPool;
        assert_eq!(GcMode::Count.supported_by(pool), Ok(GcMode::Count));
        for mode in [GcMode::Idle, GcMode::Off] {
            assert_eq!(
                mode.supported_by(pool),
                Err(GcModeError::Unsupported { mode, server: pool })
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn darwin_fanout_has_no_idle_mode() {
        let fanout = GcServer::DarwinFanout;
        assert_eq!(GcMode::Count.supported_by(fanout), Ok(GcMode::Count));
        assert_eq!(GcMode::Off.supported_by(fanout), Ok(GcMode::Off));
        let err = GcMode::Idle.supported_by(fanout).unwrap_err();
        assert_eq!(
            err,
            GcModeError::Unsupported {
                mode: GcMode::Idle,
                server: fanout
            }
        );
        assert!(err.to_string().contains("idle") && err.to_string().contains("fanout"));
    }

    #[test]
    fn idle_gc_collects_after_a_quiet_tick() {
        let mut idle = IdleGc::default();
        // Nothing served yet: nothing to collect.
        assert!(!idle.on_tick(0, 0));
        // Requests ran during this tick (keep-alive or not): not quiet yet.
        assert!(!idle.on_tick(3, 3));
        // No request since the previous tick: collect.
        assert!(idle.on_tick(3, 3));
        // Collected (since_collect back to 0), still quiet: nothing to do.
        assert!(!idle.on_tick(3, 0));
        // Busy on every tick: the idle trigger never fires (the failsafe covers it).
        assert!(!idle.on_tick(10, 7));
        assert!(!idle.on_tick(20, 17));
        assert!(idle.on_tick(20, 17));
    }
}
