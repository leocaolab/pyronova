//! In-process benchmark harnesses.
//!
//! Two modes, both driven from the `bench_inmem` / `bench_loopback`
//! pyo3 methods on `PyronovaApp`:
//!
//! - `run_inmem_bench`: virtual connections via `tokio::io::duplex`.
//!   Exercises the full per-request pipeline (Hyper parse → routing →
//!   handler → response build → Hyper write) with zero network cost.
//!   Bounds the framework ceiling, since there's no kernel socket, no
//!   loopback copy, no syscall.
//!
//! - `run_loopback_bench`: real TCP on 127.0.0.1, client + server in
//!   the same process. Isolates the kernel loopback cost from
//!   external-client CPU contention. The gap vs `run_inmem_bench`
//!   measures syscall + TCP state + loopback copy + kqueue/epoll
//!   wakeup.
//!
//! Kept out of `tpc.rs` so the production file reads as pure
//! topology + dispatch. The shared worker-spawn pattern
//! (pin → QoS → current_thread runtime → rebind tstate → LocalSet)
//! is duplicated between the two bench entrypoints and the
//! production TPC path; that's deliberate — a common helper would
//! need to thread generic closures through and the bench loops
//! need bespoke wiring (virtual-conn spawn, client-runtime boot)
//! that the prod path doesn't.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use crate::python::interp::SubInterpreterWorker;
use crate::router::{FrozenRoutes, RouteTable};
use crate::tpc::{elevate_thread_qos_macos, tpc_accept_loop_inline, try_pin_current};

const INMEM_REQ: &[u8] = b"GET / HTTP/1.1\r\nHost: inmem\r\nConnection: keep-alive\r\n\r\n";
const INMEM_BATCH: usize = 32;

// ---------------------------------------------------------------------------
// In-memory benchmark (no TCP)
// ---------------------------------------------------------------------------

pub(crate) fn run_inmem_bench(
    n_threads: usize,
    conns_per_worker: usize,
    duration_s: u64,
    mut workers: Vec<SubInterpreterWorker>,
    mut per_worker_routes: Vec<FrozenRoutes>,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
) -> Result<(u64, f64), String> {
    if workers.len() != n_threads {
        return Err(format!(
            "inmem bench worker count mismatch: expected {n_threads}, got {}",
            workers.len()
        ));
    }
    if per_worker_routes.len() != n_threads {
        return Err(format!(
            "inmem bench routes count mismatch: expected {n_threads}, got {}",
            per_worker_routes.len()
        ));
    }

    println!(
        "\n  Pyronova v{} [in-memory bench] — {n_threads} workers × {conns_per_worker} virtual conns, {duration_s}s",
        env!("CARGO_PKG_VERSION")
    );

    let shutdown = CancellationToken::new();
    let total = Arc::new(AtomicU64::new(0));
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    // Turn each per-worker Arc into a raw pointer up-front. Each worker
    // reads its route table through a &'static derived from the pointer
    // so the read-only route data lives on its own cacheline and is
    // referenced without any refcount ops per request. The matching
    // Arc::from_raw runs after every worker thread is joined (see the
    // reclaim loop before the Ok return) so the allocations are freed
    // rather than leaked — at which point no live &'static remains.
    let per_worker_ptrs: Vec<*const RouteTable> =
        per_worker_routes.drain(..).map(Arc::into_raw).collect();

    let mut handles: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(n_threads);
    #[allow(clippy::needless_range_loop)] // i indexes 3 parallel slices
    for i in 0..n_threads {
        let core_id = core_ids.get(i).copied();
        let worker = workers.remove(0);
        let routes: &'static RouteTable = unsafe { &*per_worker_ptrs[i] };
        // Extra clone for spawn-failure cleanup path (arc bench-2).
        // Closure consumes its own `shutdown` clone; this one stays in
        // the outer scope so map_err can cancel before joining handles.
        let shutdown_cleanup = shutdown.clone();
        let shutdown = shutdown.clone();
        let bridge = main_bridge.clone();
        let total = Arc::clone(&total);

        let handle = std::thread::Builder::new()
            .name(format!("pyronova-inmem-{i}"))
            .spawn(move || {
                try_pin_current(core_id);
                elevate_thread_qos_macos();
                let rt = RuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("inmem runtime build");
                let local = LocalSet::new();
                let mut worker = worker;
                worker.tstate = unsafe {
                    crate::python::interp::rebind_tstate_to_current_thread(worker.tstate)
                };
                let worker = std::rc::Rc::new(std::cell::RefCell::new(worker));
                local.block_on(&rt, async move {
                    // Spawn K virtual connections on this worker's LocalSet.
                    for _ in 0..conns_per_worker {
                        let (server_half, client_half) = tokio::io::duplex(64 * 1024);
                        let worker_c = std::rc::Rc::clone(&worker);
                        let shutdown_c = shutdown.clone();
                        let bridge_c = bridge.clone();
                        tokio::task::spawn_local(async move {
                            crate::worker::drive_conn(
                                server_half,
                                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                                worker_c,
                                routes,
                                None, // bench: no WS upgrades
                                shutdown_c,
                                bridge_c,
                            )
                            .await;
                        });
                        let total_c = Arc::clone(&total);
                        let shutdown_c = shutdown.clone();
                        tokio::task::spawn_local(async move {
                            inmem_generator(client_half, total_c, shutdown_c).await;
                        });
                    }
                    shutdown.cancelled().await;
                });
            })
            .map_err(|e| {
                // Pre-fix the `?` propagated immediately, leaving
                // threads 0..i-1 blocked on shutdown.cancelled().await
                // forever (arc bench-2). Cancel before bailing so
                // already-spawned workers can exit, then join them.
                shutdown_cleanup.cancel();
                for h in handles.drain(..) {
                    if let Err(panic) = h.join() {
                        tracing::error!(target: "pyronova::server", ?panic,
                            "bench worker thread panicked during spawn-failure cleanup");
                    }
                }
                format!("spawn inmem-{i}: {e}")
            })?;
        handles.push(handle);
    }

    // Warmup then measure.
    std::thread::sleep(std::time::Duration::from_secs(1));
    let start = total.load(Ordering::Relaxed);
    let t0 = std::time::Instant::now();
    std::thread::sleep(std::time::Duration::from_secs(duration_s));
    let end = total.load(Ordering::Relaxed);
    let elapsed = t0.elapsed().as_secs_f64();
    shutdown.cancel();

    for h in handles {
        // Log a panicked bench worker rather than silently dropping the
        // Err — pre-fix a worker crash made the bench report "success"
        // with N-1 actual workers (arc bench-1).
        if let Err(panic) = h.join() {
            tracing::error!(
                target: "pyronova::server",
                ?panic,
                "bench worker thread panicked"
            );
        }
    }

    // Reclaim the per-worker route tables now that every worker thread
    // has been joined — no live &'static references remain, so the
    // Arc::from_raw here balances the Arc::into_raw above and frees the
    // allocations instead of leaking them.
    for ptr in per_worker_ptrs {
        drop(unsafe { Arc::from_raw(ptr) });
    }
    Ok((end - start, elapsed))
}

async fn inmem_generator(
    mut stream: tokio::io::DuplexStream,
    counter: Arc<AtomicU64>,
    shutdown: CancellationToken,
) {
    // Probe: one request, parse Content-Length, compute response size.
    if stream.write_all(INMEM_REQ).await.is_err() {
        return;
    }
    let mut buf = vec![0u8; 4096];
    let mut filled = 0usize;
    let resp_size = loop {
        let n = match stream.read(&mut buf[filled..]).await {
            Ok(n) => n,
            Err(_) => return,
        };
        if n == 0 {
            return;
        }
        filled += n;
        let data = &buf[..filled];
        if let Some(hdr_end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            let mut content_len: Option<usize> = None;
            for line in data[..hdr_end].split(|&b| b == b'\n') {
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                if let Some(i) = line.iter().position(|&b| b == b':') {
                    let (k, v) = (&line[..i], &line[i + 1..]);
                    if k.eq_ignore_ascii_case(b"content-length") {
                        if let Ok(s) = std::str::from_utf8(v) {
                            if let Ok(parsed) = s.trim().parse::<usize>() {
                                content_len = Some(parsed);
                                break;
                            }
                        }
                    }
                }
            }
            let body_len = match content_len {
                Some(n) => n,
                None => return,
            };
            let total = hdr_end + 4 + body_len;
            if buf.len() < total {
                buf.resize(total + 128, 0);
            }
            while filled < total {
                let n = match stream.read(&mut buf[filled..]).await {
                    Ok(n) => n,
                    Err(_) => return,
                };
                if n == 0 {
                    return;
                }
                filled += n;
            }
            break total;
        }
        if filled == buf.len() {
            buf.resize(filled * 2, 0);
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);

    // Steady-state: pipeline BATCH requests, read exactly BATCH responses
    // worth of bytes. Every response is byte-identical at this route, so
    // counting bytes is sound.
    let mut batch = Vec::with_capacity(INMEM_REQ.len() * INMEM_BATCH);
    for _ in 0..INMEM_BATCH {
        batch.extend_from_slice(INMEM_REQ);
    }
    let need = resp_size * INMEM_BATCH;
    let mut rbuf = vec![0u8; need.max(8192)];

    while !shutdown.is_cancelled() {
        if stream.write_all(&batch).await.is_err() {
            break;
        }
        let mut got = 0usize;
        while got < need {
            let n = match stream.read(&mut rbuf[..]).await {
                Ok(n) => n,
                Err(_) => return,
            };
            if n == 0 {
                return;
            }
            got += n;
        }
        counter.fetch_add(INMEM_BATCH as u64, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Loopback benchmark (real TCP, in-process client)
// ---------------------------------------------------------------------------

pub(crate) fn run_loopback_bench(
    n_threads: usize,
    client_conns: usize,
    duration_s: u64,
    mut workers: Vec<SubInterpreterWorker>,
    routes: FrozenRoutes,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
) -> Result<(u64, f64, u16), String> {
    if workers.len() != n_threads {
        return Err(format!(
            "loopback bench worker count mismatch: expected {n_threads}, got {}",
            workers.len()
        ));
    }

    // Bind an ephemeral port on the main thread so we can report it
    // back to the client before server threads start accepting. Using
    // SO_REUSEPORT on this probe socket means the TPC worker threads
    // can bind the same port without EADDRINUSE.
    let probe = crate::server::listener::create_reuseport_listener("127.0.0.1:0".parse().unwrap())?;
    let port = probe
        .local_addr()
        .map_err(|e| format!("local_addr: {e}"))?
        .port();
    drop(probe);
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    println!(
        "\n  Pyronova v{} [loopback bench] — {n_threads} workers, {client_conns} client conns, port {port}, {duration_s}s",
        env!("CARGO_PKG_VERSION")
    );

    let shutdown = CancellationToken::new();
    let total = Arc::new(AtomicU64::new(0));
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    // Shared &'static route table across all server workers, reclaimed
    // after the workers are joined (see the Arc::from_raw before Ok).
    let routes_ptr: *const RouteTable = Arc::into_raw(Arc::clone(&routes));
    let routes_static: &'static RouteTable = unsafe { &*routes_ptr };

    // --- Server threads (mirrors run_tpc_subinterp_per_thread_listener) ---
    let mut handles: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(n_threads + 1);
    for i in 0..n_threads {
        let core_id = core_ids.get(i).copied();
        let worker = workers.remove(0);
        let routes_arc_c = Arc::clone(&routes);
        let shutdown_c = shutdown.clone();
        // Extra clone retained in outer scope for spawn-failure cleanup
        // (arc bench-2 — lb-srv variant).
        let shutdown_cleanup = shutdown.clone();
        let bridge = main_bridge.clone();

        let h = std::thread::Builder::new()
            .name(format!("pyronova-lb-srv-{i}"))
            .spawn(move || {
                try_pin_current(core_id);
                elevate_thread_qos_macos();
                let rt = RuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("lb srv rt");
                let local = LocalSet::new();
                let mut worker = worker;
                worker.tstate = unsafe {
                    crate::python::interp::rebind_tstate_to_current_thread(worker.tstate)
                };
                let worker = std::rc::Rc::new(std::cell::RefCell::new(worker));
                local.block_on(&rt, async move {
                    tpc_accept_loop_inline(
                        addr,
                        vec![],
                        worker,
                        routes_static,
                        routes_arc_c,
                        shutdown_c,
                        None,
                        bridge,
                    )
                    .await;
                });
            })
            .map_err(|e| {
                // Cancel + join already-spawned threads on spawn fail
                // so they don't block forever on shutdown.cancelled()
                // (arc bench-2 — loopback variant).
                shutdown_cleanup.cancel();
                for h in handles.drain(..) {
                    if let Err(panic) = h.join() {
                        tracing::error!(target: "pyronova::server", ?panic,
                            "bench lb-srv thread panicked during spawn-failure cleanup");
                    }
                }
                format!("spawn lb-srv-{i}: {e}")
            })?;
        handles.push(h);
    }

    // --- Client thread: a multi-thread tokio runtime hosting N client tasks ---
    let shutdown_cli = shutdown.clone();
    let total_cli = Arc::clone(&total);
    let cli = std::thread::Builder::new()
        .name("pyronova-lb-client".into())
        .spawn(move || {
            // Multi-thread runtime but conservative worker count so it
            // doesn't crowd out server cores. Use 2 client threads —
            // pipelining means a handful of client tasks saturate many
            // server workers.
            let rt = RuntimeBuilder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("lb client rt");
            rt.block_on(async move {
                // Stagger client connect to after the server listeners
                // are certain to be up. 100ms is plenty at release mode.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let mut tasks = Vec::with_capacity(client_conns);
                for _ in 0..client_conns {
                    let total = Arc::clone(&total_cli);
                    let shutdown = shutdown_cli.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = tcp_client_conn(port, total, shutdown).await;
                    }));
                }
                for t in tasks {
                    let _ = t.await;
                }
            });
        })
        .map_err(|e| format!("spawn lb-client: {e}"))?;
    handles.push(cli);

    // Warmup + measure.
    std::thread::sleep(std::time::Duration::from_secs(1));
    let start = total.load(Ordering::Relaxed);
    let t0 = std::time::Instant::now();
    std::thread::sleep(std::time::Duration::from_secs(duration_s));
    let end = total.load(Ordering::Relaxed);
    let elapsed = t0.elapsed().as_secs_f64();
    shutdown.cancel();

    for h in handles {
        // Log a panicked bench worker rather than silently dropping the
        // Err — pre-fix a worker crash made the bench report "success"
        // with N-1 actual workers (arc bench-1).
        if let Err(panic) = h.join() {
            tracing::error!(
                target: "pyronova::server",
                ?panic,
                "bench worker thread panicked"
            );
        }
    }

    // Reclaim the shared route table now that all server + client
    // threads are joined — balances the Arc::into_raw above.
    drop(unsafe { Arc::from_raw(routes_ptr) });
    Ok((end - start, elapsed, port))
}

async fn tcp_client_conn(
    port: u16,
    counter: Arc<AtomicU64>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    stream.set_nodelay(true)?;

    // Probe once to get response size.
    stream.write_all(INMEM_REQ).await?;
    let mut buf = vec![0u8; 4096];
    let mut filled = 0usize;
    let resp_size = loop {
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            return Ok(());
        }
        filled += n;
        if let Some(hdr_end) = buf[..filled].windows(4).position(|w| w == b"\r\n\r\n") {
            let mut content_len: Option<usize> = None;
            for line in buf[..hdr_end].split(|&b| b == b'\n') {
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                if let Some(i) = line.iter().position(|&b| b == b':') {
                    let (k, v) = (&line[..i], &line[i + 1..]);
                    if k.eq_ignore_ascii_case(b"content-length") {
                        if let Ok(s) = std::str::from_utf8(v) {
                            if let Ok(parsed) = s.trim().parse::<usize>() {
                                content_len = Some(parsed);
                                break;
                            }
                        }
                    }
                }
            }
            let total = hdr_end + 4 + content_len.unwrap_or(0);
            if buf.len() < total {
                buf.resize(total + 128, 0);
            }
            while filled < total {
                let n = stream.read(&mut buf[filled..]).await?;
                if n == 0 {
                    return Ok(());
                }
                filled += n;
            }
            break total;
        }
        if filled == buf.len() {
            buf.resize(filled * 2, 0);
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);

    let mut batch = Vec::with_capacity(INMEM_REQ.len() * INMEM_BATCH);
    for _ in 0..INMEM_BATCH {
        batch.extend_from_slice(INMEM_REQ);
    }
    let need = resp_size * INMEM_BATCH;
    let mut rbuf = vec![0u8; need.max(8192)];

    while !shutdown.is_cancelled() {
        stream.write_all(&batch).await?;
        let mut got = 0usize;
        while got < need {
            let n = stream.read(&mut rbuf[..]).await?;
            if n == 0 {
                return Ok(());
            }
            got += n;
        }
        counter.fetch_add(INMEM_BATCH as u64, Ordering::Relaxed);
    }
    Ok(())
}
