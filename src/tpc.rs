//! Thread-Per-Core (TPC) accept layer. See docs/tpc-rearch.md.
//!
//! Phase 1: scaffolding only. Each TPC thread owns a pinned OS thread, a
//! `tokio::runtime::Builder::new_current_thread()` runtime, a
//! `LocalSet` for spawn_local tasks, and its own `SO_REUSEPORT`
//! listener. Dispatch still flows through the old `InterpreterPool` —
//! Phase 2 replaces that with a per-thread sub-interpreter.
//!
//! Opt-in via `PYRONOVA_TPC=1` (env) or `app.run(tpc=True)` (kwarg).
//! Old multi-thread path remains the default until Phase 1 proves out.
//!
//! Why no `Send` bounds on the per-connection future? Because
//! `LocalSet::spawn_local` runs the task on the same OS thread that
//! owns the LocalSet — no cross-thread move ever happens. This is also
//! why we don't pay work-stealing cost on this path: there is no other
//! worker to steal from.

use std::net::SocketAddr;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use tokio::net::TcpListener;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::handlers::handle_request;
use crate::python::interp::SubInterpreterWorker;
use crate::router::FrozenRoutes;
use crate::server::listener::{create_reuseport_listener, handle_accept_error, setup_tcp_quickack};
use crate::websocket;

/// Custom hyper executor that spawns onto the current thread's
/// `LocalSet`. Required because `hyper_util::rt::TokioExecutor` uses
/// `tokio::spawn` which needs a multi-thread runtime — on a
/// current-thread runtime that call panics. `LocalExec` drops the Send
/// bound on the future, keeping every spawn strictly on the TPC
/// thread.
#[derive(Clone, Copy)]
struct LocalExec;

impl<F> hyper::rt::Executor<F> for LocalExec
where
    F: std::future::Future + 'static,
    F::Output: 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn_local(fut);
    }
}

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
/// `routes` is used to surface the gil / async / sub-interp split so the
/// operator knows up front how many routes will go through the
/// single-thread main_bridge versus the per-thread TPC fleet.
fn log_startup(
    mode: &str,
    addr: &SocketAddr,
    n_threads: usize,
    n_cpus: usize,
    routes: &crate::router::RouteTable,
) {
    let gil_count = routes.requires_gil.iter().filter(|&&g| g).count();
    let async_count = routes.is_async.iter().filter(|&&a| a).count();
    let stream_count = routes.is_stream.iter().filter(|&&s| s).count();
    let total = routes.requires_gil.len();
    let subinterp_count = total.saturating_sub(gil_count).saturating_sub(async_count);
    tracing::info!(
        target: "pyronova::server",
        version = env!("CARGO_PKG_VERSION"),
        mode,
        tpc = true,
        %addr,
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
    println!("  Listening on http://{addr}");
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

// ---------------------------------------------------------------------------
// GIL mode — every handler on the main interpreter
// ---------------------------------------------------------------------------

pub(crate) fn run_tpc_gil(
    addr: SocketAddr,
    n_threads: usize,
    n_cpus: usize,
    routes: FrozenRoutes,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
) -> Result<(), String> {
    log_startup("gil", &addr, n_threads, n_cpus, &routes);

    let shutdown = CancellationToken::new();
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    // ctrl_c watcher runs on its own dedicated thread — it's not TPC
    // traffic, just a signal sink that flips the token.
    let sigint_token = shutdown.clone();
    std::thread::spawn(move || {
        let rt = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("sigint runtime");
        rt.block_on(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!(target: "pyronova::server", "Shutting down gracefully...");
            println!("\n  Shutting down gracefully...");
            crate::monitor::stop_rss_sampler();
            sigint_token.cancel();
        });
    });

    let mut handles = Vec::with_capacity(n_threads);
    for i in 0..n_threads {
        let core_id = core_ids.get(i).copied();
        let routes = Arc::clone(&routes);
        let shutdown_thread = shutdown.clone();
        let tls = tls_acceptor.clone();

        let handle = std::thread::Builder::new()
            .name(format!("pyronova-tpc-{i}"))
            .spawn(move || {
                try_pin_current(core_id);
                elevate_thread_qos_macos();
                let rt = RuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tpc current-thread runtime build");
                let local = LocalSet::new();
                local.block_on(&rt, async move {
                    tpc_accept_loop_gil(addr, routes, shutdown_thread, tls).await;
                });
            });
        match handle {
            Ok(h) => handles.push(h),
            Err(e) => {
                shutdown.cancel();
                return Err(format!("spawn tpc-{i}: {e}"));
            }
        }
    }

    for h in handles {
        if let Err(e) = h.join() {
            let msg = e
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| e.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            tracing::error!(target: "pyronova::server", panic = msg, "TPC GIL worker thread panicked");
        }
    }
    Ok(())
}

async fn tpc_accept_loop_gil(
    addr: SocketAddr,
    routes: FrozenRoutes,
    shutdown: CancellationToken,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
) {
    let std_listener = match create_reuseport_listener(addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(target: "pyronova::server", error = %e, "TPC reuseport listener failed");
            return;
        }
    };
    let listener = match TcpListener::from_std(std_listener) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(target: "pyronova::server", error = %e, "TPC TcpListener::from_std failed");
            return;
        }
    };

    let tracker = TaskTracker::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            res = listener.accept() => {
                match res {
                    Ok((stream, remote_addr)) => {
                        let _ = stream.set_nodelay(true);
                        setup_tcp_quickack(&stream);

                        let routes = Arc::clone(&routes);
                        let conn_token = shutdown.clone();
                        let tls_acc_c = tls_acceptor.clone();
                        tracker.spawn_local(async move {
                            drive_gil_conn(stream, remote_addr, routes, conn_token, tls_acc_c).await;
                        });
                    }
                    Err(e) => handle_accept_error(&e).await,
                }
            }
        }
    }
    tracker.close();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), tracker.wait()).await;
}

async fn drive_gil_conn(
    stream: tokio::net::TcpStream,
    remote_addr: SocketAddr,
    routes: FrozenRoutes,
    conn_token: CancellationToken,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
) {
    let tls_stream = match tls_acceptor {
        Some(acc) => match crate::tls::wrap_tls(&acc, stream).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "pyronova::server", error = %e, "TLS handshake failed");
                return;
            }
        },
        None => crate::tls::wrap_plain(stream),
    };
    let io = TokioIo::new(tls_stream);
    let svc = service_fn(move |req: Request<Incoming>| {
        let routes = Arc::clone(&routes);
        let client_ip_addr = remote_addr.ip();
        async move {
            if websocket::is_websocket_upgrade(&req) {
                websocket::handle_websocket(req, routes).await
            } else {
                handle_request(req, routes, client_ip_addr).await
            }
        }
    });
    let mut builder = AutoBuilder::new(LocalExec);
    builder
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(std::time::Duration::from_secs(10));
    let conn = builder.serve_connection_with_upgrades(io, svc);
    tokio::pin!(conn);
    let mut graceful_sent = false;
    loop {
        tokio::select! {
            res = conn.as_mut() => {
                if let Err(e) = res {
                    let msg = e.to_string();
                    if !msg.contains("connection closed")
                        && !msg.contains("reset by peer")
                        && !msg.contains("broken pipe")
                    {
                        tracing::warn!(target: "pyronova::server", error = %e, "Connection error");
                    }
                }
                break;
            }
            _ = conn_token.cancelled(), if !graceful_sent => {
                conn.as_mut().graceful_shutdown();
                graceful_sent = true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sub-interpreter mode — dispatch still via the old pool for Phase 1
// ---------------------------------------------------------------------------

/// TPC inline mode (Phase 2) — each TPC thread owns its own sub-interp
/// and executes handlers synchronously on the accept thread. No shared
/// pool, no channel, no oneshot wake.
///
/// `workers` must have exactly `n_threads` entries; ownership transfers
/// to the TPC threads. Constraint check: startup must have rejected any
/// gil=True / async def / stream=True route — the inline handler has no
/// path to handle those (see handle_request_tpc_inline).
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_tpc_subinterp(
    addr: SocketAddr,
    n_threads: usize,
    n_cpus: usize,
    mut workers: Vec<SubInterpreterWorker>,
    routes: FrozenRoutes,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
    extra_tls: Vec<(SocketAddr, Arc<tokio_rustls::TlsAcceptor>)>,
) -> Result<(), String> {
    if workers.len() != n_threads {
        return Err(format!(
            "TPC worker count mismatch: expected {n_threads}, got {}",
            workers.len()
        ));
    }

    // Darwin: kqueue-backed SO_REUSEPORT routes ~all traffic to one
    // listener (last-socket-wins). We keep the per-thread-listener
    // default anyway because localhost benchmarking shows it still
    // wins: fanout's cross-thread wake cost + client/server CPU
    // contention on a single machine outweighs the distribution
    // benefit. The fanout topology stays behind an env opt-in for
    // real-NIC testing and hardware where the loopback isn't the
    // bottleneck. Set `PYRONOVA_TPC_DARWIN=fanout` to opt in.
    #[cfg(target_os = "macos")]
    {
        let use_fanout = matches!(
            std::env::var("PYRONOVA_TPC_DARWIN").ok().as_deref(),
            Some("fanout")
        );
        if use_fanout {
            return run_tpc_subinterp_fanout(
                addr,
                n_threads,
                n_cpus,
                workers,
                routes,
                tls_acceptor,
                main_bridge,
                extra_tls,
            );
        }
        run_tpc_subinterp_per_thread_listener(
            addr,
            n_threads,
            n_cpus,
            &mut workers,
            routes,
            tls_acceptor,
            main_bridge,
            extra_tls,
        )
    }

    #[cfg(not(target_os = "macos"))]
    {
        run_tpc_subinterp_per_thread_listener(
            addr,
            n_threads,
            n_cpus,
            &mut workers,
            routes,
            tls_acceptor,
            main_bridge,
            extra_tls,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn run_tpc_subinterp_per_thread_listener(
    addr: SocketAddr,
    n_threads: usize,
    n_cpus: usize,
    workers: &mut Vec<SubInterpreterWorker>,
    routes: FrozenRoutes,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
    extra_tls: Vec<(SocketAddr, Arc<tokio_rustls::TlsAcceptor>)>,
) -> Result<(), String> {
    log_startup("hybrid-inline", &addr, n_threads, n_cpus, &routes);

    let shutdown = CancellationToken::new();
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    let sigint_token = shutdown.clone();
    std::thread::spawn(move || {
        let rt = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("sigint runtime");
        rt.block_on(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!(target: "pyronova::server", "Shutting down gracefully...");
            println!("\n  Shutting down gracefully...");
            crate::monitor::stop_rss_sampler();
            sigint_token.cancel();
        });
    });

    // Leak one Arc into a `&'static RouteTable` shared across workers.
    // The RouteTable is read-only after startup; leaking the Arc means
    // per-request dispatch on the hot path does zero refcount ops. The
    // memory cost is one RouteTable worth, held for the server's life
    // — acceptable for a long-running server. `Arc::into_raw` + deref
    // is the stable-since-1.0 way to get this; the pointer is never
    // reclaimed on the success path, which is the whole point. We keep
    // the raw pointer so the error path below can reclaim it instead of
    // leaking on every failed start.
    let routes_raw: *const crate::router::RouteTable = Arc::into_raw(Arc::clone(&routes));
    let routes_static: &'static crate::router::RouteTable = unsafe { &*routes_raw };

    let mut handles = Vec::with_capacity(n_threads);
    for i in 0..n_threads {
        let core_id = core_ids.get(i).copied();
        let worker = workers.remove(0);
        let routes_arc = Arc::clone(&routes);
        let shutdown = shutdown.clone();
        let tls = tls_acceptor.clone();

        let bridge = main_bridge.clone();
        let shutdown_thread = shutdown.clone();
        let extra_tls_clone = extra_tls.clone();
        let handle = std::thread::Builder::new()
            .name(format!("pyronova-tpc-{i}"))
            .spawn(move || {
                try_pin_current(core_id);
                elevate_thread_qos_macos();
                let rt = RuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tpc current-thread runtime build");
                let local = LocalSet::new();
                // Rebind the sub-interp's tstate to THIS OS thread so
                // PyEval_RestoreThread during call_handler targets it.
                // Must happen on the TPC thread itself, not on main.
                let mut worker = worker;
                worker.tstate = unsafe {
                    crate::python::interp::rebind_tstate_to_current_thread(worker.tstate)
                };
                let worker = std::rc::Rc::new(std::cell::RefCell::new(worker));
                local.block_on(&rt, async move {
                    tpc_accept_loop_inline(
                        addr,
                        extra_tls_clone,
                        worker,
                        routes_static,
                        routes_arc,
                        shutdown_thread,
                        tls,
                        bridge,
                    )
                    .await;
                });
            });
        match handle {
            Ok(h) => handles.push(h),
            Err(e) => {
                // Tell the threads that did spawn to exit, join them so
                // none still reference routes_static, then reclaim the
                // leaked Arc before returning — otherwise repeated failed
                // starts (restart, tests) accumulate leaked RouteTables.
                shutdown.cancel();
                for h in handles {
                    let _ = h.join();
                }
                unsafe { drop(Arc::from_raw(routes_raw)) };
                return Err(format!("spawn tpc-{i}: {e}"));
            }
        }
    }

    for h in handles {
        if let Err(e) = h.join() {
            let msg = e
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| e.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            tracing::error!(target: "pyronova::server", panic = msg, "TPC inline worker thread panicked");
        }
    }
    Ok(())
}

/// Darwin-only TPC topology: one accept thread feeds N worker threads
/// through per-worker unbounded mpsc queues, round-robin. Preserves the
/// current-thread runtime + LocalSet + sub-interp-per-worker model; the
/// only change is where the TcpStream comes from. Pays one cross-thread
/// wake per TCP connection, which is amortized to ~0 under HTTP keep-
/// alive (one wake serves the connection's full request lifetime).
#[cfg(target_os = "macos")]
#[allow(clippy::too_many_arguments)]
fn run_tpc_subinterp_fanout(
    addr: SocketAddr,
    n_threads: usize,
    n_cpus: usize,
    mut workers: Vec<SubInterpreterWorker>,
    routes: FrozenRoutes,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
    _extra_tls: Vec<(SocketAddr, Arc<tokio_rustls::TlsAcceptor>)>,
) -> Result<(), String> {
    log_startup("hybrid-inline-fanout", &addr, n_threads, n_cpus, &routes);

    let shutdown = CancellationToken::new();
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    let sigint_token = shutdown.clone();
    std::thread::spawn(move || {
        let rt = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("sigint runtime");
        rt.block_on(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!(target: "pyronova::server", "Shutting down gracefully...");
            println!("\n  Shutting down gracefully...");
            crate::monitor::stop_rss_sampler();
            sigint_token.cancel();
        });
    });

    type Conn = (std::net::TcpStream, SocketAddr);

    // Bounded per-worker inbox. Capacity is a load-shedding threshold:
    // when a worker falls behind and its inbox fills, the acceptor
    // drops new connections (TCP RST to the client) rather than
    // hoarding file descriptors or queuing unbounded backlog. 1024
    // gives ample slack for burst smoothing while keeping worst-case
    // FD usage bounded at n_threads * 1024.
    const WORKER_INBOX_CAP: usize = 1024;

    let mut worker_txs: Vec<tokio::sync::mpsc::Sender<Conn>> = Vec::with_capacity(n_threads);
    let mut worker_rxs: Vec<Option<tokio::sync::mpsc::Receiver<Conn>>> =
        Vec::with_capacity(n_threads);
    for _ in 0..n_threads {
        let (tx, rx) = tokio::sync::mpsc::channel(WORKER_INBOX_CAP);
        worker_txs.push(tx);
        worker_rxs.push(Some(rx));
    }

    // Leak once for a shared &'static, same rationale as the per-thread
    // listener path. All workers read from the same static, no Arc ops.
    // Keep the raw pointer so the error path can reclaim it on a failed
    // start instead of leaking.
    let routes_raw: *const crate::router::RouteTable = Arc::into_raw(Arc::clone(&routes));
    let routes_static: &'static crate::router::RouteTable = unsafe { &*routes_raw };

    let mut handles = Vec::with_capacity(n_threads + 1);

    for (i, rx_slot) in worker_rxs.iter_mut().enumerate().take(n_threads) {
        let core_id = core_ids.get(i).copied();
        let worker = workers.remove(0);
        let routes_arc = Arc::clone(&routes);
        let shutdown_w = shutdown.clone();
        let tls = tls_acceptor.clone();
        let bridge = main_bridge.clone();
        let rx = rx_slot
            .take()
            .expect("worker_rxs slot must be populated");

        let handle = std::thread::Builder::new()
            .name(format!("pyronova-tpc-{i}"))
            .spawn(move || {
                try_pin_current(core_id);
                elevate_thread_qos_macos();
                let rt = RuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tpc current-thread runtime build");
                let local = LocalSet::new();
                let mut worker = worker;
                worker.tstate = unsafe {
                    crate::python::interp::rebind_tstate_to_current_thread(worker.tstate)
                };
                let worker = std::rc::Rc::new(std::cell::RefCell::new(worker));
                local.block_on(&rt, async move {
                    tpc_worker_loop_fanout(
                        rx,
                        worker,
                        routes_static,
                        routes_arc,
                        shutdown_w,
                        tls,
                        bridge,
                    )
                    .await;
                });
            });
        match handle {
            Ok(h) => handles.push(h),
            Err(e) => {
                // Reclaim the leaked Arc on a failed start: cancel so the
                // already-spawned threads exit, join them so none still
                // reference routes_static, then drop the reclaimed Arc.
                shutdown.cancel();
                for h in handles {
                    let _ = h.join();
                }
                unsafe { drop(Arc::from_raw(routes_raw)) };
                return Err(format!("spawn tpc-{i}: {e}"));
            }
        }
    }

    // Acceptor thread — dedicated OS thread with its own current_thread
    // runtime so accept() polling doesn't contend with any worker.
    let shutdown_a = shutdown.clone();
    let acceptor = std::thread::Builder::new()
        .name("pyronova-acceptor".into())
        .spawn(move || {
            // Pin the acceptor to the last P-core if available — keeps
            // it off the cores handling handlers so TCP softirq-style
            // work doesn't fight for the same L1/L2 as handler execution.
            if !core_ids.is_empty() {
                try_pin_current(core_ids.get(n_threads % core_ids.len().max(1)).copied());
            }
            elevate_thread_qos_macos();
            let rt = RuntimeBuilder::new_current_thread()
                .enable_all()
                .build()
                .expect("acceptor runtime");
            rt.block_on(async move {
                let std_listener = match crate::server::listener::create_reuseport_listener(addr) {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!(target: "pyronova::server", error = %e, "TPC fanout listener failed");
                        return;
                    }
                };
                let listener = match TcpListener::from_std(std_listener) {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!(target: "pyronova::server", error = %e, "TPC fanout TcpListener::from_std failed");
                        return;
                    }
                };
                let mut next: usize = 0;
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown_a.cancelled() => break,
                        res = listener.accept() => match res {
                            Ok((stream, remote_addr)) => {
                                let _ = stream.set_nodelay(true);
                                setup_tcp_quickack(&stream);
                                let std_stream = match stream.into_std() {
                                    Ok(s) => s,
                                    Err(e) => {
                                        tracing::warn!(target: "pyronova::server", error = %e, "into_std failed");
                                        continue;
                                    }
                                };
                                // TcpStream::from_std requires nonblocking.
                                if std_stream.set_nonblocking(true).is_err() {
                                    continue;
                                }
                                // Round-robin. try_send with load shedding:
                                // if the chosen worker's inbox is full, drop
                                // the connection (kernel sends RST). Sheds
                                // cleanly under overload instead of hoarding
                                // FDs or spawning unbounded pending work.
                                // See WORKER_INBOX_CAP comment above.
                                use tokio::sync::mpsc::error::TrySendError;
                                match worker_txs[next].try_send((std_stream, remote_addr)) {
                                    Ok(()) => {}
                                    Err(TrySendError::Full(_)) => {
                                        tracing::warn!(
                                            target: "pyronova::server",
                                            worker = next,
                                            "TPC worker inbox full — dropping connection (load shed)"
                                        );
                                    }
                                    Err(TrySendError::Closed(_)) => {
                                        // Worker channel closed — either a clean shutdown or a
                                        // worker thread panic. Either way, trigger orderly
                                        // shutdown so remaining workers are notified.
                                        tracing::error!(
                                            target: "pyronova::server",
                                            worker = next,
                                            "TPC worker channel closed unexpectedly — triggering shutdown"
                                        );
                                        shutdown_a.cancel();
                                        break;
                                    }
                                }
                                next += 1;
                                if next >= worker_txs.len() {
                                    next = 0;
                                }
                            }
                            Err(e) => crate::server::listener::handle_accept_error(&e).await,
                        }
                    }
                }
                // Dropping worker_txs here hangs up every receiver,
                // giving workers a clean exit signal alongside the
                // shutdown token.
                drop(worker_txs);
            });
        });
    match acceptor {
        Ok(h) => handles.push(h),
        Err(e) => {
            shutdown.cancel();
            return Err(format!("spawn acceptor: {e}"));
        }
    }

    for h in handles {
        if let Err(e) = h.join() {
            let msg = e
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| e.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            tracing::error!(target: "pyronova::server", panic = msg, "TPC fanout worker/acceptor thread panicked");
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
async fn tpc_worker_loop_fanout(
    mut rx: tokio::sync::mpsc::Receiver<(std::net::TcpStream, SocketAddr)>,
    worker: std::rc::Rc<std::cell::RefCell<SubInterpreterWorker>>,
    routes_static: &'static crate::router::RouteTable,
    routes_arc: FrozenRoutes,
    shutdown: CancellationToken,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
) {
    // Fanout path supports the Count GC mode only. Idle mode's drained-
    // queue signal lives in the acceptor now, not the worker, and the
    // count-based trigger inside call_handler covers the common case.
    // Off mode still silences the trigger.
    let gc_mode = gc_mode_from_env();
    if matches!(gc_mode, GcMode::Off) {
        worker.borrow_mut().gc_threshold = 0;
    }

    let tracker = TaskTracker::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            maybe = rx.recv() => match maybe {
                Some((std_stream, remote_addr)) => {
                    let stream = match tokio::net::TcpStream::from_std(std_stream) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!(target: "pyronova::server", error = %e, "TcpStream::from_std failed");
                            continue;
                        }
                    };
                    let worker_clone = std::rc::Rc::clone(&worker);
                    let routes_arc_c = Arc::clone(&routes_arc);
                    let conn_token = shutdown.clone();
                    let tls_acc_c = tls_acceptor.clone();
                    let bridge_c = main_bridge.clone();
                    tracker.spawn_local(async move {
                        crate::worker::drive_tcp_conn(stream, remote_addr, worker_clone, routes_static, routes_arc_c, conn_token, tls_acc_c, bridge_c).await;
                    });
                }
                None => break,
            }
        }
    }
    tracker.close();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), tracker.wait()).await;
}

/// GC scheduling mode. Read once at startup from `PYRONOVA_GC_MODE`:
///   - `"count"` (default) — the count-based trigger inside
///     `SubInterpreterWorker::call_handler` fires `gc.collect()` every
///     `PYRONOVA_GC_THRESHOLD` requests per worker. Simple, predictable,
///     can collide with bursty traffic.
///   - `"idle"` — the TPC accept loop fires `gc.collect()` on a
///     100ms-cadence timer, but ONLY when the accept queue drained to
///     empty since the previous tick. Traffic-density-adaptive. A
///     hard failsafe at `PYRONOVA_GC_OOM_FAILSAFE` requests (default
///     50_000) forces a collect even under sustained load so sustained
///     bursts can't starve the collector into OOM. The per-call_handler
///     count trigger is disabled while in this mode.
///   - `"off"` — no framework-level triggers at all. `gc.disable()`
///     still runs at sub-interp init; users must call `gc.collect()`
///     themselves or accept ref-count-only cleanup.
#[derive(Clone, Copy, Debug)]
enum GcMode {
    Count,
    Idle,
    Off,
}

fn gc_mode_from_env() -> GcMode {
    match std::env::var("PYRONOVA_GC_MODE").ok().as_deref() {
        Some("idle") => GcMode::Idle,
        Some("off") => GcMode::Off,
        _ => GcMode::Count,
    }
}

fn oom_failsafe_from_env() -> u64 {
    std::env::var("PYRONOVA_GC_OOM_FAILSAFE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000)
}

fn idle_tick_ms_from_env() -> u64 {
    std::env::var("PYRONOVA_GC_IDLE_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
        .max(1) // tokio::time::interval panics on Duration::ZERO
}

/// Fire a single `gc.collect()` on the worker's sub-interpreter.
/// Acquires + releases the sub-interp GIL for the duration of the call.
/// Cheap no-op when `gc_collect_func` is null (e.g. `gc` module failed
/// to import at sub-interp init — unreachable in practice).
fn fire_gc(worker: &std::rc::Rc<std::cell::RefCell<SubInterpreterWorker>>) {
    use pyo3::ffi;
    let mut w = worker.borrow_mut();
    if w.gc_collect_func.is_null() {
        return;
    }
    let tstate_cell = std::cell::Cell::new(w.tstate);
    unsafe {
        let _guard =
            crate::python::interp::SubInterpGilGuard::acquire(tstate_cell.get(), &tstate_cell);
        // PyObject_CallNoArgs: skip the empty-tuple alloc the generic
        // PyObject_Call path requires. Idiomatic 3.9+ invocation.
        let res = ffi::PyObject_CallNoArgs(w.gc_collect_func);
        if !res.is_null() {
            ffi::Py_DECREF(res);
        } else {
            // gc.collect() failing is a serious signal (OOM, heap
            // corruption, interp state damage). Pre-fix the exception
            // was cleared silently — issue cascades into mysterious
            // later crashes (arc tpc-2). Log the exception value
            // before clearing so the root cause is captured. Uses
            // 3.12+ PyErr_GetRaisedException (the deprecated triple
            // PyErr_Fetch was replaced).
            if !ffi::PyErr_Occurred().is_null() {
                let exc = ffi::PyErr_GetRaisedException();
                let msg = if !exc.is_null() {
                    let s = ffi::PyObject_Str(exc);
                    let owned = if !s.is_null() {
                        let cs = ffi::PyUnicode_AsUTF8(s);
                        let m = if !cs.is_null() {
                            std::ffi::CStr::from_ptr(cs).to_string_lossy().into_owned()
                        } else {
                            "<repr failed>".to_string()
                        };
                        ffi::Py_DECREF(s);
                        m
                    } else {
                        "<str failed>".to_string()
                    };
                    ffi::Py_DECREF(exc);
                    owned
                } else {
                    "<no value>".to_string()
                };
                tracing::error!(
                    target: "pyronova::app",
                    error = %msg,
                    "gc.collect() raised — clearing exception to keep worker \
                     alive, but this signals OOM / heap corruption / interp \
                     damage and will likely cascade"
                );
            }
        }
    }
    w.tstate = tstate_cell.get();
}

/// Accept from `slot` if Some, otherwise pend forever.
async fn tls_accept_or_pending(
    slot: Option<&(TcpListener, Arc<tokio_rustls::TlsAcceptor>)>,
) -> (
    std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>,
    Option<Arc<tokio_rustls::TlsAcceptor>>,
) {
    match slot {
        Some((l, acc)) => (l.accept().await, Some(Arc::clone(acc))),
        None => std::future::pending().await,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn tpc_accept_loop_inline(
    addr: SocketAddr,
    extra_tls: Vec<(SocketAddr, Arc<tokio_rustls::TlsAcceptor>)>,
    worker: std::rc::Rc<std::cell::RefCell<SubInterpreterWorker>>,
    routes_static: &'static crate::router::RouteTable,
    routes_arc: FrozenRoutes,
    shutdown: CancellationToken,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
) {
    let std_listener = match create_reuseport_listener(addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(target: "pyronova::server", error = %e, "TPC reuseport listener failed");
            return;
        }
    };
    let listener = match TcpListener::from_std(std_listener) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(target: "pyronova::server", error = %e, "TPC TcpListener::from_std failed");
            return;
        }
    };

    let extra_listeners: Vec<(TcpListener, Arc<tokio_rustls::TlsAcceptor>)> = extra_tls
        .into_iter()
        .filter_map(|(tls_addr, acc)| {
            let std_sock = match create_reuseport_listener(tls_addr) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "pyronova::server", error = %e, addr = %tls_addr, "TPC extra TLS listener failed");
                    return None;
                }
            };
            match TcpListener::from_std(std_sock) {
                Ok(l) => Some((l, acc)),
                Err(e) => {
                    tracing::error!(target: "pyronova::server", error = %e, addr = %tls_addr, "TPC extra TLS TcpListener::from_std failed");
                    None
                }
            }
        })
        .collect();
    let tls_slot0 = extra_listeners.first();
    let tls_slot1 = extra_listeners.get(1);

    let gc_mode = gc_mode_from_env();
    // In idle mode, disable the count-based trigger inside call_handler —
    // we drive GC from the accept loop instead. Each TPC thread owns its
    // worker exclusively so the borrow_mut is free of contention.
    if matches!(gc_mode, GcMode::Idle | GcMode::Off) {
        worker.borrow_mut().gc_threshold = 0;
    }
    let oom_failsafe = oom_failsafe_from_env();
    let idle_ms = idle_tick_ms_from_env();

    let tracker = TaskTracker::new();
    match gc_mode {
        GcMode::Idle => {
            // Hybrid idle trigger: collect either on every idle tick
            // (when the accept queue drained since last tick) or on a
            // hard failsafe to prevent OOM during sustained bursts.
            //
            // `requests_since_last_gc` is written from the accept path
            // (one TPC thread — no atomics needed; the borrow is single-
            // threaded on this current_thread runtime).
            let mut requests_since_last_gc: u64 = 0;
            let mut gc_timer = tokio::time::interval(std::time::Duration::from_millis(idle_ms));
            // First tick fires immediately — skip it so we don't collect
            // an empty heap before any requests have run.
            gc_timer.tick().await;
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    res = listener.accept() => {
                        match res {
                            Ok((stream, remote_addr)) => {
                                let _ = stream.set_nodelay(true);
                                setup_tcp_quickack(&stream);
                                requests_since_last_gc += 1;

                                let worker_clone = std::rc::Rc::clone(&worker);
                                let routes_arc_c = Arc::clone(&routes_arc);
                                let conn_token = shutdown.clone();
                                let tls_acc_c = tls_acceptor.clone();
                                let bridge_c = main_bridge.clone();
                                tracker.spawn_local(async move {
                                    crate::worker::drive_tcp_conn(stream, remote_addr, worker_clone, routes_static, routes_arc_c, conn_token, tls_acc_c, bridge_c).await;
                                });

                                // Failsafe: sustained burst — accept
                                // queue never drains, so the idle tick
                                // never gets its chance. Force a
                                // collect here to keep RSS bounded.
                                if requests_since_last_gc >= oom_failsafe {
                                    fire_gc(&worker);
                                    requests_since_last_gc = 0;
                                    gc_timer.reset();
                                }
                            }
                            Err(e) => handle_accept_error(&e).await,
                        }
                    }
                    (res, tls_acc) = tls_accept_or_pending(tls_slot0) => {
                        match res {
                            Ok((stream, remote_addr)) => {
                                let _ = stream.set_nodelay(true);
                                setup_tcp_quickack(&stream);
                                requests_since_last_gc += 1;
                                let worker_clone = std::rc::Rc::clone(&worker);
                                let routes_arc_c = Arc::clone(&routes_arc);
                                let conn_token = shutdown.clone();
                                let bridge_c = main_bridge.clone();
                                tracker.spawn_local(async move {
                                    crate::worker::drive_tcp_conn(stream, remote_addr, worker_clone, routes_static, routes_arc_c, conn_token, tls_acc, bridge_c).await;
                                });
                                if requests_since_last_gc >= oom_failsafe {
                                    fire_gc(&worker);
                                    requests_since_last_gc = 0;
                                    gc_timer.reset();
                                }
                            }
                            Err(e) => handle_accept_error(&e).await,
                        }
                    }
                    (res, tls_acc) = tls_accept_or_pending(tls_slot1) => {
                        match res {
                            Ok((stream, remote_addr)) => {
                                let _ = stream.set_nodelay(true);
                                setup_tcp_quickack(&stream);
                                requests_since_last_gc += 1;
                                let worker_clone = std::rc::Rc::clone(&worker);
                                let routes_arc_c = Arc::clone(&routes_arc);
                                let conn_token = shutdown.clone();
                                let bridge_c = main_bridge.clone();
                                tracker.spawn_local(async move {
                                    crate::worker::drive_tcp_conn(stream, remote_addr, worker_clone, routes_static, routes_arc_c, conn_token, tls_acc, bridge_c).await;
                                });
                                if requests_since_last_gc >= oom_failsafe {
                                    fire_gc(&worker);
                                    requests_since_last_gc = 0;
                                    gc_timer.reset();
                                }
                            }
                            Err(e) => handle_accept_error(&e).await,
                        }
                    }
                    _ = gc_timer.tick(), if requests_since_last_gc > 0 => {
                        // Idle tick: the select! raced the tick against
                        // accept() and the tick won — the accept queue
                        // was quiet for at least the tick period. Fire
                        // the collect without interrupting any user-
                        // visible request.
                        fire_gc(&worker);
                        requests_since_last_gc = 0;
                    }
                }
            }
        }
        GcMode::Count | GcMode::Off => loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                res = listener.accept() => {
                    match res {
                        Ok((stream, remote_addr)) => {
                            let _ = stream.set_nodelay(true);
                            setup_tcp_quickack(&stream);

                            let worker_clone = std::rc::Rc::clone(&worker);
                            let routes_arc_c = Arc::clone(&routes_arc);
                            let conn_token = shutdown.clone();
                            let tls_acc_c = tls_acceptor.clone();
                            let bridge_c = main_bridge.clone();
                            tracker.spawn_local(async move {
                                crate::worker::drive_tcp_conn(stream, remote_addr, worker_clone, routes_static, routes_arc_c, conn_token, tls_acc_c, bridge_c).await;
                            });
                        }
                        Err(e) => handle_accept_error(&e).await,
                    }
                }
                (res, tls_acc) = tls_accept_or_pending(tls_slot0) => {
                    match res {
                        Ok((stream, remote_addr)) => {
                            let _ = stream.set_nodelay(true);
                            setup_tcp_quickack(&stream);
                            let worker_clone = std::rc::Rc::clone(&worker);
                            let routes_arc_c = Arc::clone(&routes_arc);
                            let conn_token = shutdown.clone();
                            let bridge_c = main_bridge.clone();
                            tracker.spawn_local(async move {
                                crate::worker::drive_tcp_conn(stream, remote_addr, worker_clone, routes_static, routes_arc_c, conn_token, tls_acc, bridge_c).await;
                            });
                        }
                        Err(e) => handle_accept_error(&e).await,
                    }
                }
                (res, tls_acc) = tls_accept_or_pending(tls_slot1) => {
                    match res {
                        Ok((stream, remote_addr)) => {
                            let _ = stream.set_nodelay(true);
                            setup_tcp_quickack(&stream);
                            let worker_clone = std::rc::Rc::clone(&worker);
                            let routes_arc_c = Arc::clone(&routes_arc);
                            let conn_token = shutdown.clone();
                            let bridge_c = main_bridge.clone();
                            tracker.spawn_local(async move {
                                crate::worker::drive_tcp_conn(stream, remote_addr, worker_clone, routes_static, routes_arc_c, conn_token, tls_acc, bridge_c).await;
                            });
                        }
                        Err(e) => handle_accept_error(&e).await,
                    }
                }
            }
        },
    }
    tracker.close();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), tracker.wait()).await;
}

// drive_inline_conn moved to worker::drive_tcp_conn.

// ---------------------------------------------------------------------------
// Flag
// ---------------------------------------------------------------------------

/// Env-var gate for Phase 1. The Python-side `app.run(tpc=True)` kwarg
/// goes through a different path in `PyronovaApp` and does not consult
/// this function.
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

pub(crate) fn env_enabled() -> bool {
    matches!(
        std::env::var("PYRONOVA_TPC").ok().as_deref(),
        Some("1") | Some("true") | Some("yes") | Some("on")
    )
}
