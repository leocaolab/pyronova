use std::sync::Arc;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use pyo3::prelude::*;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::signal;

use crate::handlers::{handle_request, handle_request_subinterp};
use crate::python::interp;
use crate::router::{FrozenRoutes, MutableRoutes, RouteTable};
use crate::server::listener::{create_reuseport_listener, handle_accept_error, setup_tcp_quickack};
use crate::state::SharedState;
use crate::websocket;

#[pyclass]
pub(crate) struct PyronovaApp {
    routes: MutableRoutes,
    script_path: Option<String>,
    shared_state: Arc<dashmap::DashMap<String, bytes::Bytes>>,
    /// Per-instance CORS configuration (None = disabled).
    cors_config: Option<crate::router::CorsConfig>,
    /// Per-instance request logging flag.
    request_logging: bool,
    /// Per-instance access-log sampling. See RouteTable for semantics.
    request_log_sample_n: u64,
    request_log_always_status: u16,
    /// Shared counter for sampled-request rolls. Cloned (Arc) into every
    /// RouteTable snapshot so all worker copies share the same roll.
    request_log_counter: Arc<std::sync::atomic::AtomicU64>,
    /// Opt into Thread-Per-Core mode. See docs/tpc-rearch.md. Can also
    /// be flipped via the `PYRONOVA_TPC=1` env var; either is sufficient.
    tpc: bool,
}

#[pymethods]
impl PyronovaApp {
    #[new]
    fn new() -> Self {
        PyronovaApp {
            routes: Arc::new(parking_lot::RwLock::new(RouteTable::new())),
            script_path: None,
            shared_state: Arc::new(dashmap::DashMap::new()),
            cors_config: None,
            request_logging: false,
            request_log_sample_n: 1,
            request_log_always_status: 0,
            request_log_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            tpc: false,
        }
    }

    /// Enable Thread-Per-Core mode (Phase 1 scaffolding).
    fn set_tpc(&mut self, enabled: bool) {
        self.tpc = enabled;
    }

    /// Set per-instance CORS origin (legacy setter — disables advanced CORS
    /// features. Prefer `set_cors_config` which propagates credentials and
    /// expose-headers to every response per W3C CORS spec.)
    fn set_cors_origin(&mut self, origin: String) {
        self.cors_config = Some(crate::router::CorsConfig {
            origin,
            methods: "GET, POST, PUT, DELETE, PATCH, OPTIONS".to_string(),
            headers: "*".to_string(),
            expose_headers: None,
            allow_credentials: false,
        });
    }

    /// Set full per-instance CORS configuration. All fields are applied to
    /// every response (GET/POST/etc.), not just OPTIONS preflight.
    #[pyo3(signature = (origin, methods, headers, expose_headers=None, allow_credentials=false))]
    fn set_cors_config(
        &mut self,
        origin: String,
        methods: String,
        headers: String,
        expose_headers: Option<String>,
        allow_credentials: bool,
    ) {
        // W3C Fetch / CORS forbids `Access-Control-Allow-Origin: *`
        // together with `Access-Control-Allow-Credentials: true` —
        // browsers reject the response client-side regardless of
        // what the server sends. The server still returns 200 which
        // makes this a particularly nasty debugging pit (200 logs,
        // client-visible failure). Warn at config time so the
        // misconfiguration is visible in the logs where the user
        // looks first.
        if allow_credentials && origin.trim() == "*" {
            tracing::warn!(
                target: "pyronova::server",
                "CORS misconfiguration: origin=\"*\" with allow_credentials=true is rejected by all \
                 major browsers (W3C Fetch spec). Configure a concrete origin (e.g. \"https://app.example.com\") \
                 when credentials are enabled."
            );
        }
        self.cors_config = Some(crate::router::CorsConfig {
            origin,
            methods,
            headers,
            expose_headers: expose_headers.filter(|s| !s.is_empty()),
            allow_credentials,
        });
    }

    /// Enable/disable per-instance request logging.
    fn enable_request_logging(&mut self, enabled: bool) {
        self.request_logging = enabled;
    }

    /// Configure access-log sampling. `sample_n=1` (default) logs every
    /// request; `sample_n=100` logs ~1% of requests. `always_status` is
    /// the lower bound for "always log regardless of sampling" — set to
    /// `400` to keep full visibility of 4xx/5xx while sampling 2xx, or
    /// `0` (default) to apply sampling uniformly.
    ///
    /// Has no effect unless `enable_request_logging(True)` is also set.
    #[pyo3(signature = (sample_n=1, always_status=0))]
    fn set_request_log_sampling(&mut self, sample_n: u64, always_status: u16) {
        self.request_log_sample_n = sample_n.max(1);
        self.request_log_always_status = always_status;
    }

    /// Set max request body size in bytes. Default: 10 MB.
    fn set_max_body_size(&self, size: usize) {
        crate::handlers::set_max_body_size(size);
    }

    /// Register a fast-path route — a response that never enters Python.
    ///
    /// For routes with a constant body (health checks, `/robots.txt`,
    /// `/pipeline` probe endpoints, maintenance pages) the Python handler
    /// dispatch is pure overhead: GIL acquisition, handler call,
    /// serialization, all for the same bytes every time. `add_fast_response`
    /// stores the fully-built response at registration time; the accept
    /// loop serves it directly without any Python involvement.
    ///
    /// The match is exact `(method, path)` — no path params, no glob.
    /// Path-parameterized routes still need a real handler.
    #[pyo3(signature = (
        method,
        path,
        body,
        content_type="text/plain".to_string(),
        status_code=200,
        headers=None
    ))]
    fn add_fast_response(
        &mut self,
        method: &str,
        path: &str,
        body: Vec<u8>,
        content_type: String,
        status_code: u16,
        headers: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<()> {
        let method_key = method.to_ascii_uppercase();
        let path_key = path.to_string();
        let resp = crate::router::FastResponse {
            body: bytes::Bytes::from(body),
            content_type,
            status: status_code,
            headers: headers.unwrap_or_default(),
        };
        let mut routes = self.routes.write();
        let bucket = routes.fast_responses.entry(method_key.clone()).or_default();
        // Reject duplicate (method, path) registrations instead of silently
        // discarding the previous one. add_route already errors on duplicate
        // routes; mirror that so a double-registration (hot reload, plugin)
        // surfaces a clear error rather than losing the first one with no
        // diagnostic (arc finding app-53).
        if bucket.contains_key(&path_key) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "fast response already registered for {method_key} {path_key}"
            )));
        }
        bucket.insert(path_key, resp);
        Ok(())
    }

    /// Configure response compression. Disabled by default; opt-in only.
    ///
    /// Args:
    ///   enabled: master switch — when false, compression logic is a
    ///     single relaxed-atomic load + branch-not-taken.
    ///   min_size: responses smaller than this (in bytes) are not compressed.
    ///   gzip / brotli: enable each algorithm. Server prefers brotli when both
    ///     are enabled and the client accepts it.
    ///   gzip_level: 1..=9, default 6. Higher = better ratio, more CPU.
    ///   brotli_quality: 0..=11, default 4. Production sweet spot is 4–6.
    #[pyo3(signature = (
        enabled,
        min_size = crate::compression::DEFAULT_MIN_SIZE,
        gzip = true,
        brotli = true,
        gzip_level = 6,
        brotli_quality = 4,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn configure_compression(
        &self,
        enabled: bool,
        min_size: usize,
        gzip: bool,
        brotli: bool,
        gzip_level: u32,
        brotli_quality: u32,
    ) {
        crate::compression::configure(enabled, min_size, gzip, brotli, gzip_level, brotli_quality);
    }

    /// Access the shared state (cross-sub-interpreter, nanosecond latency).
    #[getter]
    fn state(&self) -> SharedState {
        SharedState::with_inner(Arc::clone(&self.shared_state))
    }

    #[pyo3(signature = (path, handler, gil=false))]
    fn get(&mut self, path: &str, handler: Py<PyAny>, gil: bool, py: Python<'_>) -> PyResult<()> {
        let name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        self.add_route("GET", path, handler, name, gil, false, py)
    }

    #[pyo3(signature = (path, handler, gil=false, stream=false))]
    fn post(
        &mut self,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
        py: Python<'_>,
    ) -> PyResult<()> {
        let name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        self.add_route("POST", path, handler, name, gil, stream, py)
    }

    #[pyo3(signature = (path, handler, gil=false, stream=false))]
    fn put(
        &mut self,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
        py: Python<'_>,
    ) -> PyResult<()> {
        let name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        self.add_route("PUT", path, handler, name, gil, stream, py)
    }

    #[pyo3(signature = (path, handler, gil=false))]
    fn delete(
        &mut self,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        py: Python<'_>,
    ) -> PyResult<()> {
        let name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        self.add_route("DELETE", path, handler, name, gil, false, py)
    }

    #[pyo3(signature = (method, path, handler, gil=false, stream=false))]
    fn route(
        &mut self,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
        py: Python<'_>,
    ) -> PyResult<()> {
        let name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        self.add_route(method, path, handler, name, gil, stream, py)
    }

    fn before_request(&mut self, handler: Py<PyAny>, py: Python<'_>) -> PyResult<()> {
        let name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        let mut routes = self.routes.write();
        routes.before_hooks.push(handler);
        routes.before_hook_names.push(name);
        Ok(())
    }

    fn after_request(&mut self, handler: Py<PyAny>, py: Python<'_>) -> PyResult<()> {
        let name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        let mut routes = self.routes.write();
        routes.after_hooks.push(handler);
        routes.after_hook_names.push(name);
        Ok(())
    }

    fn fallback(&mut self, handler: Py<PyAny>, py: Python<'_>) -> PyResult<()> {
        let name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        let mut routes = self.routes.write();
        routes.fallback_handler = Some(handler);
        routes.fallback_handler_name = Some(name);
        Ok(())
    }

    fn websocket(&mut self, path: &str, handler: Py<PyAny>) -> PyResult<()> {
        let mut routes = self.routes.write();
        routes.ws_handlers.insert(path.to_string(), handler);
        Ok(())
    }

    fn static_dir(&mut self, prefix: &str, directory: &str) -> PyResult<()> {
        let prefix = if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };
        let dir = std::path::Path::new(directory)
            .canonicalize()
            .map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "static directory '{directory}' not found: {e}"
                ))
            })?
            .to_string_lossy()
            .to_string();
        let mut routes = self.routes.write();
        routes.static_dirs.push((prefix, dir));
        Ok(())
    }

    #[pyo3(signature = (
        host=None, port=None, workers=None, mode=None, io_workers=None,
        tls_cert=None, tls_key=None, tpc=None, extra_tls_ports=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn run(
        &self,
        py: Python<'_>,
        host: Option<&str>,
        port: Option<u16>,
        workers: Option<usize>,
        mode: Option<&str>,
        io_workers: Option<usize>,
        tls_cert: Option<&str>,
        tls_key: Option<&str>,
        tpc: Option<bool>,
        extra_tls_ports: Option<Vec<u16>>,
    ) -> PyResult<()> {
        // Refresh the metrics kill-switch from the current env every
        // run() — process-level state, but tests / hot-reload may flip
        // PYRONOVA_METRICS between runs and we want each new server to
        // honor the latest value.
        crate::monitor::init_metrics_flag();
        // RSS sampler is a real OS thread; spawning it twice would
        // leak. Once-protect the spawn (and the log line) but leave
        // the flag refresh above unguarded.
        use std::sync::Once;
        static RSS_SAMPLER_INIT: Once = Once::new();
        RSS_SAMPLER_INIT.call_once(|| {
            if std::env::var("PYRONOVA_METRICS").unwrap_or_default() == "1" {
                crate::monitor::spawn_rss_sampler();
                tracing::info!(target: "pyronova::server", "Metrics enabled (PYRONOVA_METRICS=1): passive GIL monitor + RSS sampler");
            }
        });

        let host = host.unwrap_or("127.0.0.1");
        let port = port.unwrap_or(8000);
        let mode = mode.unwrap_or("default");
        let addr: SocketAddr =
            format!("{host}:{port}")
                .parse()
                .map_err(|e: std::net::AddrParseError| {
                    pyo3::exceptions::PyValueError::new_err(e.to_string())
                })?;

        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        // workers = TPC thread count (TPC mode) or sub-interpreter count (non-TPC).
        // Default: physical_core_count() in TPC mode (avoids SMT thrash), num_cpus otherwise.
        let workers_explicit = workers.is_some();
        let workers = workers.unwrap_or(num_cpus);
        // io_workers = Tokio async thread count + accept loop count (non-TPC mode).
        let io_workers = io_workers.unwrap_or(num_cpus);

        // Freeze route table: extract from RwLock into read-only Arc.
        // After this point, no more route registration — zero-lock reads.
        let frozen: FrozenRoutes = {
            let table = self.routes.read();
            // Clone the RouteTable into a new Arc (no lock needed at runtime)
            Arc::new(RouteTable {
                handlers: table.handlers.iter().map(|h| h.clone_ref(py)).collect(),
                handler_names: table.handler_names.clone(),
                requires_gil: table.requires_gil.clone(),
                is_async: table.is_async.clone(),
                is_stream: table.is_stream.clone(),
                routers: table.routers.clone(),
                ws_handlers: table
                    .ws_handlers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                    .collect(),
                before_hooks: table.before_hooks.iter().map(|h| h.clone_ref(py)).collect(),
                after_hooks: table.after_hooks.iter().map(|h| h.clone_ref(py)).collect(),
                before_hook_names: table.before_hook_names.clone(),
                after_hook_names: table.after_hook_names.clone(),
                fallback_handler: table.fallback_handler.as_ref().map(|h| h.clone_ref(py)),
                fallback_handler_name: table.fallback_handler_name.clone(),
                static_dirs: table.static_dirs.clone(),
                cors_config: self.cors_config.clone(),
                request_logging: self.request_logging,
                request_log_sample_n: self.request_log_sample_n,
                request_log_always_status: self.request_log_always_status,
                request_log_counter: Arc::clone(&self.request_log_counter),
                fast_responses: table.fast_responses.clone(),
            })
        };

        // Build TLS acceptor once at startup if both paths are provided.
        // Either both or neither — single path is a configuration error.
        let tls_acceptor = match (tls_cert, tls_key) {
            (Some(cert), Some(key)) => Some(
                crate::tls::build_acceptor(cert, key)
                    .map_err(pyo3::exceptions::PyValueError::new_err)?,
            ),
            (None, None) => None,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "tls_cert and tls_key must be provided together",
                ))
            }
        };

        // Extra TLS ports: read from PYRONOVA_TLS_PORTS env if not provided.
        let extra_tls_ports: Vec<u16> = extra_tls_ports
            .or_else(|| {
                std::env::var("PYRONOVA_TLS_PORTS")
                    .ok()
                    .map(|s| s.split(',').filter_map(|p| p.trim().parse().ok()).collect())
            })
            .unwrap_or_default();

        let extra_tls: Vec<(SocketAddr, Arc<tokio_rustls::TlsAcceptor>)> =
            if let Some(ref acc) = tls_acceptor {
                extra_tls_ports
                    .iter()
                    .map(|&p| -> PyResult<(SocketAddr, Arc<tokio_rustls::TlsAcceptor>)> {
                        // arc src-app-3: a TLS port that fails to parse must NOT
                        // be silently dropped. A warn-and-drop leaves the operator
                        // believing the port is TLS-protected when it was never
                        // opened at all — a security-UX trap. Fail fast at startup
                        // (mirrors the cert/key validation above) so the
                        // misconfiguration is impossible to miss.
                        let sa = format!("{host}:{p}")
                            .parse::<SocketAddr>()
                            .map_err(|e: std::net::AddrParseError| {
                                pyo3::exceptions::PyValueError::new_err(format!(
                                    "extra TLS port {p} could not be parsed into a socket \
                                     address (\"{host}:{p}\"): {e}. Refusing to start: an \
                                     unparseable TLS port must never be silently dropped, \
                                     since the operator expects this port to be TLS-protected."
                                ))
                            })?;
                        Ok((sa, Arc::clone(acc)))
                    })
                    .collect::<PyResult<Vec<_>>>()?
            } else {
                if !extra_tls_ports.is_empty() {
                    // arc src-app-4: TLS ports configured but no cert =
                    // ports would never be opened. Operators expect TLS on
                    // these; a silent (or merely warned) no-op leaves them
                    // believing the ports are protected when they are not.
                    // This is a security misconfiguration — fail fast at
                    // startup rather than serve in a misleading state.
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "extra_tls_ports {extra_tls_ports:?} configured but tls_cert/tls_key \
                         not set; these ports cannot be opened as TLS. Provide tls_cert+tls_key \
                         or remove the port list."
                    )));
                }
                vec![]
            };

        // TPC is now the default — automatic when the route set is
        // compatible (no gil=True / async def / stream=True routes).
        // Incompatible workloads fall back silently to the old
        // multi-thread InterpreterPool. Opt out explicitly via
        // `tpc=False`, `PYRONOVA_TPC=0`, or `PYRONOVA_TPC=off`.
        //
        // Why default-on: measured +7% throughput and ~10× P99
        // improvement over the multi_thread path on the baseline
        // test. Kernel SO_REUSEPORT + per-core current_thread runtime
        // + physical-core pinning + zero cross-thread handler dispatch
        // is a strict win for the common "sync handler" shape.
        // After Phase 3+4+5 TPC covers every route shape:
        //   gil=True        → main-interp bridge
        //   async def       → sub-interp asyncio loop (inline, blocking)
        //   response stream → main-interp bridge BridgeResponse::Stream
        //   stream=True     → body feeder on TPC LocalSet, receiver
        //                     forwarded to bridge via GilWorkItem.body_stream_rx
        //
        // `PYRONOVA_TPC=0` remains as an escape hatch for unforeseen bugs
        // or niche C-extension loading issues.
        let tpc_incompatible = false;
        let tpc_forced_off = std::env::var("PYRONOVA_TPC")
            .map(|v| {
                matches!(
                    v.to_ascii_lowercase().as_str(),
                    "0" | "off" | "no" | "false"
                )
            })
            .unwrap_or(false);
        let tpc_explicit_opt_in = tpc.unwrap_or(false) || self.tpc || crate::tpc::env_enabled();
        // Explicit opt-in on an incompatible route set is a startup
        // error (existing behavior in run_tpc_subinterp). Implicit
        // default on incompatible set silently falls back — this is
        // the whole point of the auto path.
        let tpc_enabled = if tpc_forced_off {
            false
        } else if tpc_explicit_opt_in {
            true // run_tpc_subinterp will error if incompatible
        } else {
            !tpc_incompatible
        };

        // TPC defaults to PHYSICAL core count, not logical. On SMT
        // systems, pinning 2 TPC threads to sibling hyperthreads
        // thrashes shared L1i/L1d and kills per-thread throughput
        // (measured -50% on 7840HS baseline). Work-stealing on
        // non-TPC can exploit SMT because one sibling does IO while
        // the other does Python bytecode — different cache footprints.
        // TPC runs the SAME codepath on every thread, so SMT siblings
        // step on each other. Explicit override via PYRONOVA_WORKERS still honored.
        let workers = if tpc_enabled && !workers_explicit {
            crate::tpc::physical_core_count()
        } else {
            workers
        };

        if mode == "subinterp" || mode == "auto" {
            if tpc_enabled {
                // When extra_tls is non-empty the TLS ports are handled by
                // those extra listeners; the main addr is plain HTTP.
                let main_tls = if extra_tls.is_empty() {
                    tls_acceptor
                } else {
                    None
                };
                self.run_tpc_subinterp(
                    py, addr, workers, io_workers, num_cpus, frozen, main_tls, extra_tls,
                )
            } else {
                self.run_subinterp(
                    py,
                    addr,
                    workers,
                    io_workers,
                    num_cpus,
                    frozen,
                    tls_acceptor,
                )
            }
        } else if tpc_enabled {
            self.run_tpc_gil(py, addr, workers, num_cpus, frozen, tls_acceptor)
        } else {
            self.run_gil(py, addr, io_workers, num_cpus, frozen, tls_acceptor)
        }
    }

    /// In-memory benchmark: spin up N TPC sub-interp workers, feed
    /// them virtual connections via `tokio::io::duplex` (no TCP).
    /// Bypasses the kernel network stack entirely — used to bound
    /// the pure-framework ceiling (Hyper parse → routing → handler
    /// → response build). Only supports sync, non-GIL, non-streaming
    /// routes. Returns `(total_requests, elapsed_s)`.
    #[pyo3(signature = (duration_s=10, workers=None, conns_per_worker=8))]
    fn bench_inmem(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        conns_per_worker: usize,
    ) -> PyResult<(u64, f64)> {
        self.__bench_inmem_impl(py, duration_s, workers, conns_per_worker)
    }

    /// Loopback bench: real TCP on 127.0.0.1, server + client in the
    /// same process. Measures the framework ceiling with the kernel
    /// network stack included, but zero external-client CPU
    /// contention (unlike wrk). Returns (total_requests, elapsed_s, port).
    #[pyo3(signature = (duration_s=10, workers=None, client_conns=32))]
    fn bench_loopback(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        client_conns: usize,
    ) -> PyResult<(u64, f64, u16)> {
        self.__bench_loopback_impl(py, duration_s, workers, client_conns)
    }
}

/// Drive a single HTTP/1+2 connection to completion, then drain it on
/// shutdown. Shared by `run_gil` and `run_subinterp`: the accept layer
/// and the per-request service closure differ, but the AutoBuilder setup
/// (Slowloris header-read timeout) and the graceful-shutdown lifecycle
/// loop are byte-identical. `tpc.rs::drive_gil_conn` runs the same shape
/// on a `current_thread` runtime with `LocalExec` rather than
/// `TokioExecutor`, so it cannot reuse this `TokioExecutor`-specialized
/// helper without an executor-generic bound.
async fn serve_connection<S>(
    io: TokioIo<crate::tls::MaybeTlsStream>,
    svc: S,
    conn_token: tokio_util::sync::CancellationToken,
) where
    S: hyper::service::Service<
            Request<Incoming>,
            Response = hyper::Response<crate::handlers::BoxBody>,
            Error = hyper::Error,
        > + Send
        + 'static,
    S::Future: Send + 'static,
{
    let mut builder = AutoBuilder::new(hyper_util::rt::TokioExecutor::new());
    // Slowloris defense: cap how long hyper waits for the client to finish
    // sending request headers. Without this a client that opens a TCP
    // connection and dribbles one header byte per minute holds a Tokio
    // task + fd forever. TLS handshake is already bounded in
    // src/tls.rs::wrap_tls; this closes the analogous hole on the
    // plaintext HTTP path (and on HTTP-after-TLS). HTTP/2 has its own
    // internal frame/settings timeouts via the h2 crate, so we only
    // configure H/1 here. Requires a Timer — TokioTimer ties it to the runtime.
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
                // Shutdown: tell hyper to stop accepting new requests on
                // this connection and drain in-flight ones. Keep driving
                // the connection future until it completes.
                conn.as_mut().graceful_shutdown();
                graceful_sent = true;
            }
        }
    }
}

impl PyronovaApp {
    #[allow(clippy::too_many_arguments)]
    fn add_route(
        &mut self,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        handler_name: String,
        gil: bool,
        stream: bool,
        py: Python<'_>,
    ) -> PyResult<()> {
        // Auto-detect if handler is async def (also check __call__ for class-based views)
        let inspect = py.import("inspect")?;
        let is_async = inspect
            .call_method1("iscoroutinefunction", (&handler,))?
            .extract::<bool>()
            .unwrap_or(false)
            || handler
                .bind(py)
                .getattr("__call__")
                .and_then(|c| {
                    inspect
                        .call_method1("iscoroutinefunction", (c,))
                        .and_then(|r| r.extract::<bool>())
                })
                .unwrap_or(false);

        // Streaming constraints (v1): GIL-only, sync handlers only.
        // Sub-interp streaming needs a C-FFI bridge; async streaming needs
        // awaitable support. Both deferred to v2.
        if stream && !gil {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "stream=True requires gil=True (v1 limitation)",
            ));
        }
        if stream && is_async {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "stream=True is not yet supported on async def handlers (v1 limitation)",
            ));
        }

        let mut routes = self.routes.write();
        routes
            .insert(method, path, handler, handler_name, gil, is_async, stream)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("route error: {e}")))?;
        Ok(())
    }

    fn run_gil(
        &self,
        py: Python<'_>,
        addr: SocketAddr,
        io_workers: usize,
        num_cpus: usize,
        routes: FrozenRoutes,
        tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    ) -> PyResult<()> {
        let scheme = if tls_acceptor.is_some() {
            "https"
        } else {
            "http"
        };
        tracing::info!(
            target: "pyronova::server",
            version = env!("CARGO_PKG_VERSION"),
            %addr,
            io_workers,
            cpus = num_cpus,
            mode = "gil",
            tls = tls_acceptor.is_some(),
            "Pyronova started"
        );
        println!("\n  Pyronova v{}", env!("CARGO_PKG_VERSION"));
        println!("  Listening on {scheme}://{addr}");
        println!("  IO workers: {io_workers} (CPUs: {num_cpus})\n");

        py.detach(move || -> PyResult<()> {
            let rt = RuntimeBuilder::new_multi_thread()
                .worker_threads(io_workers)
                .enable_all()
                .build()
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("tokio runtime error: {e}"))
                })?;

            rt.block_on(async move {
                // Multi-accept: N listeners on same port via SO_REUSEPORT.
                // Linux kernel load-balances connections across accept loops.
                // macOS SO_REUSEPORT doesn't do kernel LB, so use 1 acceptor.
                #[cfg(target_os = "linux")]
                let n_accept = io_workers.min(num_cpus);
                #[cfg(not(target_os = "linux"))]
                let n_accept = 1;
                let shutdown_token = tokio_util::sync::CancellationToken::new();
                // TaskTracker: collects every per-connection spawn so we can
                // `.wait()` for them on shutdown. Without this, `rt.block_on`
                // returning after `shutdown_token.cancel()` drops the Tokio
                // Runtime, which aborts every spawned connection mid-request
                // (clients see TCP RST). graceful_shutdown() on each conn is
                // necessary but insufficient — it only signals hyper to stop
                // accepting NEW keep-alive requests; the drain still needs
                // time on the runtime.
                let conn_tracker = tokio_util::task::TaskTracker::new();

                for _ in 0..n_accept {
                    let std_listener = create_reuseport_listener(addr).map_err(|e| {
                        pyo3::exceptions::PyOSError::new_err(e)
                    })?;
                    let listener = TcpListener::from_std(std_listener).map_err(|e| {
                        pyo3::exceptions::PyOSError::new_err(format!("TcpListener::from_std error: {e}"))
                    })?;
                    let routes = Arc::clone(&routes);
                    let token = shutdown_token.clone();
                    let tracker = conn_tracker.clone();
                    let tls_acc = tls_acceptor.clone();

                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                result = listener.accept() => {
                                    let (stream, remote_addr) = match result {
                                        Ok(v) => v,
                                        Err(e) => {
                                            handle_accept_error(&e).await;
                                            continue;
                                        }
                                    };
                                    let routes = Arc::clone(&routes);
                                    let _ = stream.set_nodelay(true);
                                    setup_tcp_quickack(&stream);

                                    let conn_token = token.clone();
                                    let tls_acc_c = tls_acc.clone();
                                    tracker.spawn(async move {
                                        // TLS handshake happens here (inside the
                                        // spawned connection task) so it doesn't
                                        // block the accept loop from taking more
                                        // connections.
                                        let tls_stream = match tls_acc_c {
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
                                        serve_connection(io, svc, conn_token).await;
                                    });
                                }
                                _ = token.cancelled() => break,
                            }
                        }
                    });
                }

                // ctrl_c() returns Err if signal handler registration
                // failed (e.g. main thread can't take SIGINT in some
                // embedded contexts). Pre-fix this silently fell through
                // to immediate shutdown — server appeared to start then
                // die seconds later with no diagnostic (arc app-1/-2).
                if let Err(e) = signal::ctrl_c().await {
                    tracing::error!(
                        target: "pyronova::server",
                        error = %e,
                        "ctrl_c signal handler registration failed; cannot \
                         receive SIGINT. Accept loops are live and serving — \
                         parking instead of tearing down. Terminate via \
                         SIGTERM/SIGKILL."
                    );
                    // Do NOT fall through to shutdown: the accept loops were
                    // already spawned above and are serving traffic. Falling
                    // through here is what made the server "start then die
                    // seconds later" (arc app-1/-2/-52). Park forever so the
                    // server keeps running; the OS can still SIGKILL it.
                    std::future::pending::<()>().await;
                }
                tracing::info!(target: "pyronova::server", "Shutting down gracefully...");
                println!("\n  Shutting down gracefully...");
                crate::monitor::stop_rss_sampler();
                shutdown_token.cancel();
                // Close the tracker (no more spawns) and wait for every
                // in-flight connection to finish its hyper drain. Bound
                // the wait at 30 s so a pathological client can't hold
                // shutdown hostage forever.
                conn_tracker.close();
                const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
                if tokio::time::timeout(DRAIN_TIMEOUT, conn_tracker.wait()).await.is_err() {
                    tracing::warn!(
                        target: "pyronova::server",
                        "{} in-flight connections did not drain within {:?} — exiting anyway",
                        conn_tracker.len(),
                        DRAIN_TIMEOUT,
                    );
                }

                Ok(())
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn run_subinterp(
        &self,
        py: Python<'_>,
        addr: SocketAddr,
        workers: usize,
        io_workers: usize,
        num_cpus: usize,
        routes: FrozenRoutes,
        tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    ) -> PyResult<()> {
        let script_path = if let Some(ref p) = self.script_path {
            p.clone()
        } else {
            let main_mod = py.import("__main__")?;
            main_mod.getattr("__file__")?.extract::<String>()?
        };

        let (
            handler_names,
            routers,
            before_hook_names,
            after_hook_names,
            static_dirs,
            requires_gil,
        ) = (
            routes.handler_names.clone(),
            routes.routers.clone(),
            routes.before_hook_names.clone(),
            routes.after_hook_names.clone(),
            routes.static_dirs.clone(),
            routes.requires_gil.clone(),
        );

        let gil_count = requires_gil.iter().filter(|&&g| g).count();
        let subinterp_count = requires_gil.len() - gil_count;

        let async_count_routes = routes.is_async.iter().filter(|&&a| a).count();
        let has_async = async_count_routes > 0;
        let mode_label = if has_async { "hybrid-async" } else { "hybrid" };
        tracing::info!(
            target: "pyronova::server",
            version = env!("CARGO_PKG_VERSION"),
            mode = mode_label,
            %addr,
            workers,
            cpus = num_cpus,
            subinterp_routes = subinterp_count,
            gil_routes = gil_count,
            async_routes = async_count_routes,
            "Pyronova started"
        );
        println!(
            "\n  Pyronova v{} [{mode_label} mode]",
            env!("CARGO_PKG_VERSION")
        );
        let scheme = if tls_acceptor.is_some() {
            "https"
        } else {
            "http"
        };
        println!("  Listening on {scheme}://{addr}");
        println!("  Sub-interpreters: {workers} | IO threads: {io_workers} (CPUs: {num_cpus})");
        if has_async {
            // Cap async at workers-1 so sync always keeps at least one
            // slot; on tiny pools (workers<=2) async gets 1, sync gets
            // the rest. Plain subtraction here panics on usize underflow
            // in debug builds and wraps to garbage in release builds
            // when workers < 2.
            let async_w = (workers / 2).max(2).min(workers.saturating_sub(1).max(1));
            let sync_w = workers.saturating_sub(async_w);
            println!("  Workers: {sync_w} sync + {async_w} async");
        }
        println!(
            "  Routes: {subinterp_count} sub-interp + {gil_count} GIL + {async_count_routes} async"
        );
        println!("  Script: {script_path}\n");

        let pool = unsafe {
            interp::InterpreterPool::new(
                workers,
                py,
                &script_path,
                &handler_names,
                routers,
                &before_hook_names,
                &after_hook_names,
                static_dirs,
                requires_gil,
                routes.is_async.clone(),
                self.cors_config.clone(),
                self.request_logging,
            )
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "sub-interpreter pool error: {e}"
                ))
            })?
        };
        let pool = Arc::new(pool);

        py.detach(move || -> PyResult<()> {
            let rt = RuntimeBuilder::new_multi_thread()
                .worker_threads(io_workers)
                .enable_all()
                .build()
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("tokio runtime error: {e}"))
                })?;

            rt.block_on(async move {
                #[cfg(target_os = "linux")]
                let n_accept = io_workers.min(num_cpus);
                #[cfg(not(target_os = "linux"))]
                let n_accept = 1;
                let shutdown_token = tokio_util::sync::CancellationToken::new();
                // See comment in run_gil — same contract.
                let conn_tracker = tokio_util::task::TaskTracker::new();

                for _ in 0..n_accept {
                    let std_listener = create_reuseport_listener(addr).map_err(|e| {
                        pyo3::exceptions::PyOSError::new_err(e)
                    })?;
                    let listener = TcpListener::from_std(std_listener).map_err(|e| {
                        pyo3::exceptions::PyOSError::new_err(format!("TcpListener::from_std error: {e}"))
                    })?;
                    let pool = Arc::clone(&pool);
                    let routes = Arc::clone(&routes);
                    let token = shutdown_token.clone();
                    let tracker = conn_tracker.clone();
                    let tls_acc = tls_acceptor.clone();

                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                result = listener.accept() => {
                                    let (stream, remote_addr) = match result {
                                        Ok(v) => v,
                                        Err(e) => {
                                            handle_accept_error(&e).await;
                                            continue;
                                        }
                                    };
                                    let pool = Arc::clone(&pool);
                                    let routes = Arc::clone(&routes);
                                    let _ = stream.set_nodelay(true);
                                    setup_tcp_quickack(&stream);

                                    let conn_token = token.clone();
                                    let tls_acc_c = tls_acc.clone();
                                    tracker.spawn(async move {
                                        let tls_stream = match tls_acc_c {
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
                                            let pool = Arc::clone(&pool);
                                            let routes = Arc::clone(&routes);
                                            let client_ip_addr = remote_addr.ip();
                                            async move {
                                                if websocket::is_websocket_upgrade(&req) {
                                                    websocket::handle_websocket(req, routes).await
                                                } else {
                                                    handle_request_subinterp(req, pool, routes, client_ip_addr).await
                                                }
                                            }
                                        });
                                        serve_connection(io, svc, conn_token).await;
                                    });
                                }
                                _ = token.cancelled() => break,
                            }
                        }
                    });
                }

                // ctrl_c() returns Err if signal handler registration
                // failed (e.g. main thread can't take SIGINT in some
                // embedded contexts). Pre-fix this silently fell through
                // to immediate shutdown — server appeared to start then
                // die seconds later with no diagnostic (arc app-1/-2).
                if let Err(e) = signal::ctrl_c().await {
                    tracing::error!(
                        target: "pyronova::server",
                        error = %e,
                        "ctrl_c signal handler registration failed; cannot \
                         receive SIGINT. Accept loops are live and serving — \
                         parking instead of tearing down. Terminate via \
                         SIGTERM/SIGKILL."
                    );
                    // Do NOT fall through to shutdown: the accept loops were
                    // already spawned above and are serving traffic. Falling
                    // through here is what made the server "start then die
                    // seconds later" (arc app-1/-2/-52). Park forever so the
                    // server keeps running; the OS can still SIGKILL it.
                    std::future::pending::<()>().await;
                }
                tracing::info!(target: "pyronova::server", "Shutting down gracefully...");
                println!("\n  Shutting down gracefully...");
                crate::monitor::stop_rss_sampler();
                shutdown_token.cancel();
                conn_tracker.close();
                const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
                if tokio::time::timeout(DRAIN_TIMEOUT, conn_tracker.wait()).await.is_err() {
                    tracing::warn!(
                        target: "pyronova::server",
                        "{} in-flight connections did not drain within {:?} — exiting anyway",
                        conn_tracker.len(),
                        DRAIN_TIMEOUT,
                    );
                }

                Ok(())
            })
        })
    }

    /// TPC GIL entry — Phase 1 scaffolding. Same dispatch semantics as
    /// `run_gil` (every handler on the main interpreter), different
    /// accept layer (N pinned OS threads × current_thread runtime ×
    /// SO_REUSEPORT, no cross-core task migration).
    #[allow(clippy::too_many_arguments)]
    fn run_tpc_gil(
        &self,
        py: Python<'_>,
        addr: SocketAddr,
        io_workers: usize,
        num_cpus: usize,
        routes: FrozenRoutes,
        tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    ) -> PyResult<()> {
        py.detach(move || -> PyResult<()> {
            crate::tpc::run_tpc_gil(addr, io_workers, num_cpus, routes, tls_acceptor)
                .map_err(pyo3::exceptions::PyRuntimeError::new_err)
        })
    }

    /// TPC sub-interp inline mode (Phase 2) — each TPC thread owns its
    /// own sub-interp and runs handlers synchronously on the accept
    /// thread. No pool, no channel, no oneshot wake.
    ///
    /// Phase 2 constraint: every route must be sync + non-GIL. Any
    /// route with `gil=True`, `async def`, or `stream=True` causes this
    /// to bail at startup with a clear error. Users with such routes
    /// should stay on the old multi_thread path (drop `tpc=True`).
    #[allow(clippy::too_many_arguments)]
    fn run_tpc_subinterp(
        &self,
        py: Python<'_>,
        addr: SocketAddr,
        workers: usize,
        _io_workers: usize,
        num_cpus: usize,
        routes: FrozenRoutes,
        tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
        extra_tls: Vec<(SocketAddr, Arc<tokio_rustls::TlsAcceptor>)>,
    ) -> PyResult<()> {
        // gil=True routes: main-interp bridge (Phase 3).
        // async def: sub-interp path already drives coroutines via the
        //   persistent asyncio loop (SubInterpreterWorker::resolve_coroutine
        //   fires when call_handler returns an awaitable — line 1768 in
        //   interp.rs). Each sub-interp already has its own asyncio event
        //   loop cached at init. No extra work needed for correctness.
        //   Note: this is "blocking async" — the TPC thread is blocked
        //   for the coroutine's entire execution. Awaits inside the
        //   coroutine still run via asyncio's event loop on that same
        //   thread, so `await asyncio.sleep()` or `await client.get()`
        //   work correctly; they just don't yield to OTHER requests on
        //   the same TPC thread. Since SO_REUSEPORT distributes new
        //   connections across threads, this is fine for throughput —
        //   a slow async handler only blocks its one thread.
        // stream=True: already gated by gil=True in route registration,
        //   so all stream routes flow through the main-interp bridge.
        //   Phase 5 wires stream responses back through the bridge
        //   oneshot (BridgeResponse enum).
        let gil_count = routes.requires_gil.iter().filter(|&&g| g).count();
        let _async_count = routes.is_async.iter().filter(|&&a| a).count();
        let _stream_count = routes.is_stream.iter().filter(|&&s| s).count();

        let script_path = if let Some(ref p) = self.script_path {
            p.clone()
        } else {
            let main_mod = py.import("__main__")?;
            main_mod.getattr("__file__")?.extract::<String>()?
        };

        // Combine handler + hook names — same pattern as InterpreterPool::new.
        let mut all_func_names: Vec<String> = routes.handler_names.clone();
        all_func_names.extend(routes.before_hook_names.iter().cloned());
        all_func_names.extend(routes.after_hook_names.iter().cloned());

        // PYRONOVA_WORKER=1 so sub-interps know they're replays.
        std::env::set_var("PYRONOVA_WORKER", "1");

        // Read the user script once.
        let raw_script = std::fs::read_to_string(&script_path).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("read script '{script_path}': {e}"))
        })?;

        // Allocate a pool_id for this TPC server — still used by the
        // C-FFI bridge's zombie guard (cheap compatibility).
        let pool_id = interp::next_pool_id();

        // Build N sub-interpreters on the MAIN thread (main tstate current).
        // Each SubInterpreterWorker::new swaps to a fresh sub-interp, runs
        // the bootstrap script, then swaps back. Returned workers have
        // `tstate` saved (GIL released) — the TPC thread will rebind it
        // via rebind_tstate_to_current_thread before use.
        let n_threads = workers;
        let mut sub_workers = Vec::with_capacity(n_threads);
        for i in 0..n_threads {
            let w = unsafe {
                interp::SubInterpreterWorker::new(
                    &raw_script,
                    &script_path,
                    &all_func_names,
                    pool_id,
                )
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "TPC sub-interp {i} init: {e}"
                    ))
                })?
            };
            sub_workers.push(w);
        }

        // Phase 3 gil=True bridge: spawn a dedicated main-interp thread
        // when at least one route is gil=True. The bridge serves those
        // routes on a single MPSC-fed worker with the main GIL, while
        // TPC threads handle the rest inline. See src/main_bridge.rs.
        let main_bridge = if gil_count > 0 {
            // 4 workers default — handlers mix CPU + I/O. Pure-CPU
            // (numpy) workloads serialize on the GIL anyway so extra
            // workers cost only thread-stack memory; I/O-bound (DB,
            // file, sleep, sqlx-via-runtime) workloads gain real
            // concurrency because each worker can pick up the GIL the
            // moment a peer's handler releases it.
            let workers: usize = std::env::var("PYRONOVA_GIL_BRIDGE_WORKERS")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&n: &usize| n > 0)
                .unwrap_or(4);
            // Capacity scales with workers so per-worker queue depth
            // stays at 16 (matches the original single-thread design's
            // back-pressure behavior).
            let capacity: usize = std::env::var("PYRONOVA_GIL_BRIDGE_CAPACITY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(16 * workers);
            Some(crate::bridge::main_bridge::MainInterpBridge::spawn(
                Arc::clone(&routes),
                capacity,
                workers,
            ))
        } else {
            None
        };

        py.detach(move || -> PyResult<()> {
            crate::tpc::run_tpc_subinterp(
                addr,
                n_threads,
                num_cpus,
                sub_workers,
                routes,
                tls_acceptor,
                main_bridge,
                extra_tls,
            )
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)
        })
    }

    #[allow(dead_code)]
    fn __bench_inmem_impl(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        conns_per_worker: usize,
    ) -> PyResult<(u64, f64)> {
        let n_threads = workers.unwrap_or_else(crate::tpc::physical_core_count);

        // Build N independent FrozenRoutes — one per TPC worker. Each
        // gets its own Arc allocation so the refcount cacheline is
        // exclusive to the worker's P-core. Removes the cross-core
        // ping-pong from per-request Arc::clone(&routes) at the cost
        // of N × Py handler IncRefs at startup (one-time).
        let build_one = |py: Python<'_>| -> FrozenRoutes {
            let table = self.routes.read();
            Arc::new(RouteTable {
                handlers: table.handlers.iter().map(|h| h.clone_ref(py)).collect(),
                handler_names: table.handler_names.clone(),
                requires_gil: table.requires_gil.clone(),
                is_async: table.is_async.clone(),
                is_stream: table.is_stream.clone(),
                routers: table.routers.clone(),
                ws_handlers: table
                    .ws_handlers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                    .collect(),
                before_hooks: table.before_hooks.iter().map(|h| h.clone_ref(py)).collect(),
                after_hooks: table.after_hooks.iter().map(|h| h.clone_ref(py)).collect(),
                before_hook_names: table.before_hook_names.clone(),
                after_hook_names: table.after_hook_names.clone(),
                fallback_handler: table.fallback_handler.as_ref().map(|h| h.clone_ref(py)),
                fallback_handler_name: table.fallback_handler_name.clone(),
                static_dirs: table.static_dirs.clone(),
                cors_config: self.cors_config.clone(),
                request_logging: self.request_logging,
                request_log_sample_n: self.request_log_sample_n,
                request_log_always_status: self.request_log_always_status,
                request_log_counter: Arc::clone(&self.request_log_counter),
                fast_responses: table.fast_responses.clone(),
            })
        };

        // Route-shape validation uses one sample.
        let sample = build_one(py);
        if sample.requires_gil.iter().any(|&g| g) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "bench_inmem does not support gil=True routes",
            ));
        }
        if sample.is_async.iter().any(|&a| a) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "bench_inmem does not support async def routes",
            ));
        }
        if sample.is_stream.iter().any(|&s| s) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "bench_inmem does not support stream=True routes",
            ));
        }
        let handler_names_from_routes = sample.handler_names.clone();
        let before_hook_names = sample.before_hook_names.clone();
        let after_hook_names = sample.after_hook_names.clone();

        let mut per_worker_routes: Vec<FrozenRoutes> = Vec::with_capacity(n_threads);
        per_worker_routes.push(sample);
        for _ in 1..n_threads {
            per_worker_routes.push(build_one(py));
        }

        let script_path = if let Some(ref p) = self.script_path {
            p.clone()
        } else {
            let main_mod = py.import("__main__")?;
            main_mod.getattr("__file__")?.extract::<String>()?
        };
        let mut all_func_names: Vec<String> = handler_names_from_routes;
        all_func_names.extend(before_hook_names);
        all_func_names.extend(after_hook_names);

        crate::monitor::init_metrics_flag();
        std::env::set_var("PYRONOVA_WORKER", "1");
        let raw_script = std::fs::read_to_string(&script_path).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("read script '{script_path}': {e}"))
        })?;
        let pool_id = interp::next_pool_id();
        let mut built_workers = Vec::with_capacity(n_threads);
        for i in 0..n_threads {
            let w = unsafe {
                interp::SubInterpreterWorker::new(
                    &raw_script,
                    &script_path,
                    &all_func_names,
                    pool_id,
                )
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "bench_inmem sub-interp {i} init: {e}"
                    ))
                })?
            };
            built_workers.push(w);
        }

        py.detach(move || -> PyResult<(u64, f64)> {
            crate::bench::run_inmem_bench(
                n_threads,
                conns_per_worker,
                duration_s,
                built_workers,
                per_worker_routes,
                None,
            )
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)
        })
    }

    #[allow(dead_code)]
    fn __bench_loopback_impl(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        client_conns: usize,
    ) -> PyResult<(u64, f64, u16)> {
        let routes: FrozenRoutes = {
            let table = self.routes.read();
            Arc::new(RouteTable {
                handlers: table.handlers.iter().map(|h| h.clone_ref(py)).collect(),
                handler_names: table.handler_names.clone(),
                requires_gil: table.requires_gil.clone(),
                is_async: table.is_async.clone(),
                is_stream: table.is_stream.clone(),
                routers: table.routers.clone(),
                ws_handlers: table
                    .ws_handlers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                    .collect(),
                before_hooks: table.before_hooks.iter().map(|h| h.clone_ref(py)).collect(),
                after_hooks: table.after_hooks.iter().map(|h| h.clone_ref(py)).collect(),
                before_hook_names: table.before_hook_names.clone(),
                after_hook_names: table.after_hook_names.clone(),
                fallback_handler: table.fallback_handler.as_ref().map(|h| h.clone_ref(py)),
                fallback_handler_name: table.fallback_handler_name.clone(),
                static_dirs: table.static_dirs.clone(),
                cors_config: self.cors_config.clone(),
                request_logging: self.request_logging,
                request_log_sample_n: self.request_log_sample_n,
                request_log_always_status: self.request_log_always_status,
                request_log_counter: Arc::clone(&self.request_log_counter),
                fast_responses: table.fast_responses.clone(),
            })
        };

        if routes.requires_gil.iter().any(|&g| g)
            || routes.is_async.iter().any(|&a| a)
            || routes.is_stream.iter().any(|&s| s)
        {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "bench_loopback requires all routes to be sync, non-GIL, non-stream",
            ));
        }

        let n_threads = workers.unwrap_or_else(crate::tpc::physical_core_count);
        let script_path = if let Some(ref p) = self.script_path {
            p.clone()
        } else {
            let main_mod = py.import("__main__")?;
            main_mod.getattr("__file__")?.extract::<String>()?
        };
        let mut all_func_names: Vec<String> = routes.handler_names.clone();
        all_func_names.extend(routes.before_hook_names.iter().cloned());
        all_func_names.extend(routes.after_hook_names.iter().cloned());

        crate::monitor::init_metrics_flag();
        std::env::set_var("PYRONOVA_WORKER", "1");
        let raw_script = std::fs::read_to_string(&script_path).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("read script '{script_path}': {e}"))
        })?;
        let pool_id = interp::next_pool_id();
        let mut built_workers = Vec::with_capacity(n_threads);
        for i in 0..n_threads {
            let w = unsafe {
                interp::SubInterpreterWorker::new(
                    &raw_script,
                    &script_path,
                    &all_func_names,
                    pool_id,
                )
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "bench_loopback sub-interp {i} init: {e}"
                    ))
                })?
            };
            built_workers.push(w);
        }

        py.detach(move || -> PyResult<(u64, f64, u16)> {
            crate::bench::run_loopback_bench(
                n_threads,
                client_conns,
                duration_s,
                built_workers,
                routes,
                None,
            )
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)
        })
    }
}
