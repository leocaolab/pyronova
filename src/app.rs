use std::sync::Arc;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use pyo3::prelude::*;
use std::net::SocketAddr;

use tokio::runtime::Builder as RuntimeBuilder;
use tokio::signal;
use tokio_util::sync::CancellationToken;

use crate::config::{ConfigError, EnvConfig, Mode};
use crate::conn_driver::drive_connection;
use crate::handlers::{handle_request, handle_request_subinterp};
use crate::python::pool::{AbandonedWorker, InterpreterPool};
use crate::python::worker::{SubInterpreterWorker, WorkerProgram, WorkerSpec};
use crate::python::worker_app::{self, WorkerRoutes};
use crate::router::{Dispatch, HandlerKind, MutableRoutes, RequestBody, RouteTable, Sealed};
use crate::server::listener::{AcceptSource, Accepted, BoundListeners, Listener, ListenerSpec};
use crate::site::{AccessLog, Cors, CorsSpec, Limits, SharedSite, Site, SiteConfig};
use crate::state::SharedState;
use crate::websocket;
use hyper_util::rt::TokioExecutor;

#[pyclass(module = "pyronova.engine")]
pub(crate) struct PyronovaApp {
    routes: MutableRoutes,
    script_path: Option<String>,
    shared_state: Arc<dashmap::DashMap<String, bytes::Bytes>>,
    /// Per-instance CORS configuration (None = disabled), parsed when it is set.
    cors: Option<Cors>,
    /// Per-instance access log. Its sampling counter is shared by every copy served.
    access_log: AccessLog,
    /// Answer the built-in gRPC benchmark method (`enable_grpc_benchmark`).
    grpc_benchmark: bool,
    /// The header a client's request id arrives in (`set_request_id_header`).
    request_id_header: Option<hyper::header::HeaderName>,
    /// This app's request-body and WebSocket limits, served by its runs.
    limits: Limits,
    /// The libraries each worker loads from a private copy (`isolate`).
    isolated: Vec<String>,
    /// The server `run` is serving; `None` while nothing is serving.
    serving: parking_lot::Mutex<Option<Serving>>,
    /// The worker threads this app's last `run()` abandoned at its shutdown (still running
    /// past the grace period), until taken (`_take_abandoned_workers`).
    abandoned: parking_lot::Mutex<Vec<AbandonedWorker>>,
}

/// The server one `run()` call is serving.
struct Serving {
    /// Cancelled to stop it (`shutdown()`, or SIGINT through `until_stopped`).
    stop: CancellationToken,
    /// Where it listens; empty until its listeners are bound.
    bound: Vec<crate::server::listener::Bound>,
}

#[pymethods]
impl PyronovaApp {
    #[new]
    fn new(py: Python<'_>) -> Self {
        PyronovaApp {
            routes: Arc::new(parking_lot::RwLock::new(RouteTable::new())),
            script_path: None,
            shared_state: crate::state::map_for_new(py),
            cors: None,
            access_log: AccessLog::disabled(),
            grpc_benchmark: false,
            request_id_header: None,
            limits: Limits::DEFAULT,
            isolated: Vec::new(),
            serving: parking_lot::Mutex::new(None),
            abandoned: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Gives each sub-interpreter worker its own private copy of these C-extension
    /// libraries, cloned by the worker's bootstrap before the script runs. `pyronova`
    /// itself can't be isolated: one shared copy of it and its engine is required
    /// (FR-11).
    fn isolate(&mut self, libraries: Vec<String>) -> PyResult<()> {
        if libraries.iter().any(|lib| lib == "pyronova") {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "pyronova cannot be isolated: one shared copy of pyronova and its engine is \
                 required (it keeps process-wide state). Remove it from app.isolate(...).",
            ));
        }
        for lib in libraries {
            if !self.isolated.contains(&lib) {
                self.isolated.push(lib);
            }
        }
        Ok(())
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
    ) -> PyResult<()> {
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
        self.cors = Some(parse_cors(&CorsSpec {
            origin: &origin,
            methods: &methods,
            headers: &headers,
            expose_headers: expose_headers.as_deref().filter(|s| !s.is_empty()),
            allow_credentials,
        })?);
        Ok(())
    }

    /// Answer HttpArena's `benchmark.BenchmarkService/GetSum` gRPC method. Only a POST to
    /// that exact path with an `application/grpc*` content-type reaches it; every other
    /// request is routed as usual.
    fn enable_grpc_benchmark(&mut self) {
        self.grpc_benchmark = true;
    }

    /// Take a request's id from the client's `header` when it sends a usable one (visible
    /// ASCII, at most 128 bytes); otherwise the server mints one. Either way the id is
    /// `req.request_id`, and a 5xx reports it.
    fn set_request_id_header(&mut self, header: &str) -> PyResult<()> {
        let name = hyper::header::HeaderName::from_bytes(header.as_bytes()).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "invalid request-id header name {header:?}: {e}"
            ))
        })?;
        self.request_id_header = Some(name);
        Ok(())
    }

    /// Enable/disable per-instance request logging.
    fn enable_request_logging(&mut self, enabled: bool) {
        self.access_log.enabled = enabled;
    }

    /// Configure access-log sampling. `sample_n=1` (default) logs every
    /// request; `sample_n=100` logs ~1% of requests; `0` raises `ValueError`.
    /// `always_status` is the lower bound for "always log regardless of sampling" —
    /// `400` keeps full visibility of 4xx/5xx while sampling 2xx; `None` (default)
    /// applies sampling uniformly. A value that isn't an HTTP status raises `ValueError`.
    ///
    /// Has no effect unless `enable_request_logging(True)` is also set.
    #[pyo3(signature = (sample_n=1, always_status=None))]
    fn set_request_log_sampling(
        &mut self,
        sample_n: u64,
        always_status: Option<u16>,
    ) -> PyResult<()> {
        let sample_n = std::num::NonZeroU64::new(sample_n).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "sample_n must be at least 1 (1 logs every request)",
            )
        })?;
        let always_status = always_status
            .map(|code| {
                hyper::StatusCode::from_u16(code).map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "always_status must be an HTTP status (100-999) or None, got {code}"
                    ))
                })
            })
            .transpose()?;
        self.access_log.sample_n = sample_n;
        self.access_log.always_status = always_status;
        Ok(())
    }

    /// Set this app's max request body size in bytes; a larger body is answered 413.
    /// Default: 10 MB. Per app: another app in the process keeps its own.
    fn set_max_body_size(&mut self, size: usize) {
        self.limits.max_body_bytes = size;
    }

    fn max_body_size(&self) -> usize {
        self.limits.max_body_bytes
    }

    /// Largest WebSocket message (and frame), in bytes, in either direction. Default 1 MiB.
    fn set_max_websocket_message_size(&mut self, size: i64) -> PyResult<()> {
        self.limits.ws = self.limits.ws.with_max_message_bytes(size)?;
        Ok(())
    }

    fn max_websocket_message_size(&self) -> u32 {
        self.limits.ws.max_message_bytes
    }

    /// Concurrent WebSocket connections; an upgrade beyond it is answered 503. Default 1024.
    fn set_max_websocket_connections(&mut self, count: i64) -> PyResult<()> {
        self.limits.ws = self.limits.ws.with_max_connections(count)?;
        Ok(())
    }

    fn max_websocket_connections(&self) -> usize {
        self.limits.ws.max_connections
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
        let resp = crate::router::FastResponse::parse(
            bytes::Bytes::from(body),
            &content_type,
            status_code,
            &headers.unwrap_or_default(),
        )
        .map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "fast response {method_key} {path_key}: {e}"
            ))
        })?;
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
        py: Python<'_>,
        enabled: bool,
        min_size: u32,
        gzip: bool,
        brotli: bool,
        gzip_level: u32,
        brotli_quality: u32,
    ) {
        let wanted = crate::compression::requested(
            enabled,
            min_size,
            gzip,
            brotli,
            gzip_level,
            brotli_quality,
        );
        // Process-wide (unlike the per-app limits): main sets it, a worker only warns (FR-17).
        if crate::run_context::on_main(py) {
            crate::compression::configure(
                enabled,
                min_size,
                gzip,
                brotli,
                gzip_level,
                brotli_quality,
            );
            return;
        }
        let current = crate::compression::current();
        if wanted != current {
            tracing::warn!(
                target: "pyronova::server",
                "configure_compression({wanted:?}) in a worker is ignored: compression is \
                 process-wide, and the main interpreter set {current:?}"
            );
        }
    }

    /// Marks the end of the script's registrations. `Pyronova` calls it once, on the main
    /// interpreter, when it prepares its first server and before anything registered at
    /// run time (`/mcp`, logging hooks, startup hooks). Idempotent, because the engine's
    /// own `run()` seals an unsealed table too: the first boundary stays (Layer 2, FR-2).
    /// A worker is never sealed: its table is the script's registrations.
    fn _seal_registrations(&self, py: Python<'_>) -> PyResult<()> {
        if !crate::run_context::on_main(py) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "_seal_registrations runs on the main interpreter only",
            ));
        }
        self.seal_if_unsealed();
        Ok(())
    }

    /// The script workers execute, when it isn't `__main__.__file__`: `pyronova run
    /// module:app` runs `cli.py` as `__main__`, and workers must execute the app's module
    /// instead (Layer 2, N15).
    fn set_script_path(&mut self, path: String) {
        self.script_path = Some(path);
    }

    /// Access the shared state (cross-sub-interpreter, nanosecond latency).
    #[getter]
    fn state(&self) -> SharedState {
        SharedState::with_inner(Arc::clone(&self.shared_state))
    }

    #[pyo3(signature = (path, handler, gil=false))]
    fn get(slf: &Bound<'_, Self>, path: &str, handler: Py<PyAny>, gil: bool) -> PyResult<()> {
        Self::register_route(slf, "GET", path, handler, gil, false)
    }

    #[pyo3(signature = (path, handler, gil=false, stream=false))]
    fn post(
        slf: &Bound<'_, Self>,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
    ) -> PyResult<()> {
        Self::register_route(slf, "POST", path, handler, gil, stream)
    }

    #[pyo3(signature = (path, handler, gil=false, stream=false))]
    fn put(
        slf: &Bound<'_, Self>,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
    ) -> PyResult<()> {
        Self::register_route(slf, "PUT", path, handler, gil, stream)
    }

    #[pyo3(signature = (path, handler, gil=false))]
    fn delete(slf: &Bound<'_, Self>, path: &str, handler: Py<PyAny>, gil: bool) -> PyResult<()> {
        Self::register_route(slf, "DELETE", path, handler, gil, false)
    }

    #[pyo3(signature = (method, path, handler, gil=false, stream=false))]
    fn route(
        slf: &Bound<'_, Self>,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
    ) -> PyResult<()> {
        Self::register_route(slf, method, path, handler, gil, stream)
    }

    fn before_request(slf: &Bound<'_, Self>, handler: Py<PyAny>) -> PyResult<()> {
        Self::serve_in_worker(slf)?;
        slf.borrow().routes.write().before_hooks.push(handler);
        Ok(())
    }

    fn after_request(slf: &Bound<'_, Self>, handler: Py<PyAny>) -> PyResult<()> {
        Self::serve_in_worker(slf)?;
        slf.borrow().routes.write().after_hooks.push(handler);
        Ok(())
    }

    fn fallback(slf: &Bound<'_, Self>, handler: Py<PyAny>) -> PyResult<()> {
        Self::serve_in_worker(slf)?;
        slf.borrow().routes.write().set_fallback(handler);
        Ok(())
    }

    /// Register the WebSocket handler for `path`. A second handler for the same path
    /// raises `ValueError`, as a duplicate route does, instead of replacing the first.
    fn websocket(&mut self, path: &str, handler: Py<PyAny>) -> PyResult<()> {
        let mut routes = self.routes.write();
        match routes.ws_handlers.entry(path.to_string()) {
            std::collections::hash_map::Entry::Occupied(_) => {
                Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "websocket handler already registered for {path}"
                )))
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(handler);
                Ok(())
            }
        }
    }

    fn static_dir(&mut self, prefix: &str, directory: &str) -> PyResult<()> {
        let mount = crate::static_fs::StaticMount::new(prefix, directory)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        self.routes.write().static_dirs.push(mount);
        Ok(())
    }

    #[pyo3(signature = (
        host=None, port=None, workers=None, mode=None, io_workers=None,
        tls_cert=None, tls_key=None, extra_tls_ports=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn run(
        &self,
        py: Python<'_>,
        host: Option<&str>,
        port: Option<u16>,
        workers: Option<usize>,
        mode: Option<&Bound<'_, PyAny>>,
        io_workers: Option<usize>,
        tls_cert: Option<&str>,
        tls_key: Option<&str>,
        extra_tls_ports: Option<Vec<u16>>,
    ) -> PyResult<()> {
        // A worker executes the whole script, so an unguarded `app.run()` in a raw-engine
        // script reaches here inside the worker's init. Serving from there would start a
        // server inside a worker (Layer 2, N10); `Pyronova.run()` returns before this.
        if !crate::run_context::on_main(py) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "PyronovaApp.run() was called inside a sub-interpreter worker. Workers execute \
                 the whole script to register its routes; start the server only in the main \
                 interpreter, e.g. under `if __name__ == \"__main__\":` or with \
                 `if not pyronova.engine._in_worker():`",
            ));
        }
        // isojson is a hard dependency: without it the server doesn't start.
        crate::response::require_json(py)?;
        // Everything this run takes from the environment, parsed once; a bad value stops
        // it here, before anything is built.
        let env = EnvConfig::from_env()?;
        let mode = mode.map(Mode::from_arg).transpose()?.unwrap_or(Mode::Gil);
        // Process-level, refreshed every run: tests / hot-reload may flip PYRONOVA_METRICS
        // between runs, and each new server honours the latest value.
        crate::monitor::init_metrics_flag(env.metrics);
        if env.metrics {
            // The RSS sampler is a real OS thread; spawning it twice would leak one.
            static RSS_SAMPLER_INIT: std::sync::Once = std::sync::Once::new();
            RSS_SAMPLER_INIT.call_once(|| {
                crate::monitor::spawn_rss_sampler();
                tracing::info!(target: "pyronova::server", "Metrics enabled (PYRONOVA_METRICS=1): passive GIL monitor + RSS sampler");
            });
        }

        let host = host.unwrap_or("127.0.0.1");
        let port = port.unwrap_or(8000);
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

        // A raw-engine script never calls `_seal_registrations`; seal here so every served
        // table has the script's boundary that workers compare against (Layer 2, FR-2).
        self.seal_if_unsealed();
        // Freeze route table: extract from RwLock into read-only Arc.
        // After this point, no more route registration — zero-lock reads.
        let frozen: SharedSite = Arc::new(self.snapshot(py));

        // The route table holds main-interpreter `Py<T>`s. Worker, bridge and Tokio threads
        // hold clones and may drop theirs anywhere, including at runtime shutdown; this one
        // is the last, dropped when `run` returns, on the main thread, attached (FR-6).
        let _routes_keepalive = Arc::clone(&frozen);

        // Build TLS acceptor once at startup if both paths are provided.
        // Either both or neither — single path is a configuration error.
        let tls_acceptor = match (tls_cert, tls_key) {
            (Some(cert), Some(key)) => Some(
                crate::tls::build_acceptor(cert, key)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
            ),
            (None, None) => None,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "tls_cert and tls_key must be provided together",
                ))
            }
        };
        // Every run path serves this one listener set. `Pyronova.run()` resolves
        // PYRONOVA_TLS_PORTS into `extra_tls_ports` (one parser). Extra TLS ports without a
        // certificate are refused: the operator would believe them protected.
        let specs = ListenerSpec::set(addr, tls_acceptor, &extra_tls_ports.unwrap_or_default())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        // Thread-per-core serves every route shape (gil=True and response streams through
        // the main-interpreter bridge, async def on the worker's own loop, stream=True
        // bodies fed from the TPC thread) and measured +7% throughput and ~10× better P99
        // than the multi-thread pool, so it is the default. `PYRONOVA_TPC=0` keeps the
        // pool path as an escape hatch for niche C-extension loading issues.
        let tpc_enabled = env.tpc;

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

        // The GC modes a path can't run are refused now, before anything is bound or built.
        let fanout = env.darwin_topology == crate::config::DarwinTopology::Fanout;
        match (mode, tpc_enabled) {
            (Mode::Subinterp, false) => {
                env.gc
                    .mode
                    .supported_by(crate::tpc::GcServer::SubInterpreterPool)
                    .map_err(ConfigError::from)?;
            }
            #[cfg(target_os = "macos")]
            (Mode::Subinterp, true) if fanout => {
                env.gc
                    .mode
                    .supported_by(crate::tpc::GcServer::DarwinFanout)
                    .map_err(ConfigError::from)?;
            }
            _ => {}
        }

        // One socket per listener for each accept loop: every TPC thread, the fanout
        // acceptor, or the multi-thread runtime's accept tasks.
        let accept_loops = match (mode, tpc_enabled) {
            (Mode::Subinterp, true) if fanout => 1,
            (_, true) => workers,
            (_, false) => multi_thread_accept_loops(io_workers, num_cpus),
        };

        let stop = CancellationToken::new();
        *self.serving.lock() = Some(Serving {
            stop: stop.clone(),
            bound: Vec::new(),
        });
        let served = (|| -> PyResult<Vec<AbandonedWorker>> {
            // Every listener is bound before any thread or worker exists: a port in use is
            // this call's `OSError(EADDRINUSE)`, not a log line from a serving thread.
            let listeners = BoundListeners::bind(&specs, accept_loops)?;
            if let Some(serving) = self.serving.lock().as_mut() {
                serving.bound = listeners.bound.clone();
            }
            let run = ServerRun {
                listeners,
                site: frozen,
                stop,
                workers,
                io_workers,
                num_cpus,
            };
            match (mode, tpc_enabled) {
                (Mode::Subinterp, true) => {
                    self.run_tpc_subinterp(py, run, &env).map(|()| Vec::new())
                }
                (Mode::Subinterp, false) => self.run_subinterp(py, run, &env),
                (Mode::Gil, true) => py
                    .detach(move || {
                        crate::tpc::run_tpc_gil(run.listeners, run.num_cpus, run.site, run.stop)
                    })
                    .map_err(PyErr::from)
                    .map(|()| Vec::new()),
                (Mode::Gil, false) => run_gil(py, run).map(|()| Vec::new()),
            }
        })();
        *self.serving.lock() = None;
        // Kept for the caller, not returned: a SIGINT's KeyboardInterrupt can land in
        // Python before a return value is stored, and the caller must still see them.
        served.map(|abandoned| *self.abandoned.lock() = abandoned)
    }

    /// The worker threads this app's last `run()` abandoned (still running past the
    /// shutdown grace period), taken: a second call returns none. Their interpreters are
    /// alive, and finalizing with a live sub-interpreter aborts, so `Pyronova.run()` exits
    /// non-zero without finalizing when there are any (Layer 2, N8).
    fn _take_abandoned_workers(&self) -> Vec<AbandonedWorker> {
        std::mem::take(&mut *self.abandoned.lock())
    }

    /// The port the server `run()` is serving listens on (its first listener's; a
    /// `port=0` resolved to the one the kernel picked). `None` until the listeners are
    /// bound, and after the server stopped.
    fn bound_port(&self) -> Option<u16> {
        self.serving
            .lock()
            .as_ref()
            .and_then(|serving| serving.bound.first())
            .map(|bound| bound.addr.port())
    }

    /// Stops the server `run()` is serving, the way SIGINT does: stop accepting, drain the
    /// in-flight connections, return from `run()`. Callable from any thread; a no-op while
    /// nothing is serving.
    fn shutdown(&self) {
        if let Some(serving) = self.serving.lock().as_ref() {
            serving.stop.cancel();
        }
    }

    /// In-memory benchmark (`--features bench` builds only): N TPC sub-interpreter
    /// workers serve virtual connections (`tokio::io::duplex`, no TCP) that pipeline
    /// `GET /`. The pure-framework ceiling: hyper parse → routing → handler → response.
    /// Every route must be sync, non-GIL, non-stream. Returns
    /// `(requests, elapsed_s)` over the measured window; any failed worker or client
    /// raises instead.
    #[cfg(feature = "bench")]
    #[pyo3(signature = (duration_s=10, workers=None, conns_per_worker=8))]
    fn bench_inmem(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        conns_per_worker: usize,
    ) -> PyResult<(u64, f64)> {
        let n = bench_worker_count(workers)?;
        // One copy of the site per worker, so no two cores share its refcount cacheline.
        let sites: Vec<SharedSite> = (0..n)
            .map(|_| self.bench_site(py).map(Arc::new))
            .collect::<PyResult<_>>()?;
        let env = EnvConfig::from_env()?;
        crate::monitor::init_metrics_flag(env.metrics);
        let workers = self.build_workers(py, n, &sites[0], env.gc.count_trigger())?;

        // The sites hold main-interpreter `Py<T>`s: the last reference to each is this
        // one, dropped here, attached, after the bench threads are joined (FR-6).
        let sites_keepalive = sites.clone();
        let paired = workers.into_iter().zip(sites).collect();
        let duration = std::time::Duration::from_secs(duration_s);
        let measured = py
            .detach(move || crate::bench::run_inmem_bench(conns_per_worker, duration, paired))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        drop(sites_keepalive);
        Ok((measured.requests, measured.elapsed.as_secs_f64()))
    }

    /// Loopback benchmark (`--features bench` builds only): N TPC sub-interpreter
    /// workers accept real TCP on an ephemeral 127.0.0.1 port, and `client_conns`
    /// pipelining clients run in this process. The ceiling with the kernel network stack
    /// but no external-client CPU contention (unlike wrk). Every route must be sync,
    /// non-GIL, non-stream. Returns `(requests, elapsed_s, port)`; any failed worker or
    /// client raises instead.
    #[cfg(feature = "bench")]
    #[pyo3(signature = (duration_s=10, workers=None, client_conns=32))]
    fn bench_loopback(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        client_conns: usize,
    ) -> PyResult<(u64, f64, u16)> {
        let n = bench_worker_count(workers)?;
        let site: SharedSite = Arc::new(self.bench_site(py)?);
        let env = EnvConfig::from_env()?;
        crate::monitor::init_metrics_flag(env.metrics);
        let workers = self.build_workers(py, n, &site, env.gc.count_trigger())?;

        // As in `bench_inmem`: the site's last reference is dropped here, attached.
        let site_keepalive = Arc::clone(&site);
        let duration = std::time::Duration::from_secs(duration_s);
        let (measured, port) = py
            .detach(move || {
                crate::bench::run_loopback_bench(client_conns, duration, workers, site, env.gc)
            })
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        drop(site_keepalive);
        Ok((measured.requests, measured.elapsed.as_secs_f64(), port))
    }
}

/// `workers=None` means one per physical core; zero workers has nothing to measure.
#[cfg(feature = "bench")]
fn bench_worker_count(workers: Option<usize>) -> PyResult<usize> {
    match workers {
        None => Ok(crate::tpc::physical_core_count()),
        Some(0) => Err(pyo3::exceptions::PyValueError::new_err(
            "bench workers must be at least 1",
        )),
        Some(n) => Ok(n),
    }
}

/// Resolves once the server should stop, then cancels `stop` so every accept loop and
/// connection sees it: on SIGINT, or when `PyronovaApp.shutdown()` cancelled `stop`. If the
/// SIGINT handler cannot be installed, the server keeps serving until `shutdown()`.
pub(crate) async fn until_stopped(stop: CancellationToken) {
    tokio::select! {
        signalled = signal::ctrl_c() => match signalled {
            // SIGINT ends the process, so the process-wide RSS sampler stops with it.
            Ok(()) => crate::monitor::stop_rss_sampler(),
            Err(e) => {
                tracing::error!(
                    target: "pyronova::server",
                    error = %e,
                    "ctrl_c signal handler registration failed; cannot receive SIGINT. \
                     Serving until PyronovaApp.shutdown(), SIGTERM or SIGKILL."
                );
                stop.cancelled().await;
            }
        },
        () = stop.cancelled() => {}
    }
    tracing::info!(target: "pyronova::server", "Shutting down gracefully...");
    println!("\n  Shutting down gracefully...");
    stop.cancel();
}

/// One server run as `run()` resolved it: what each serving path starts from.
struct ServerRun {
    /// Bound before the path starts; one group per accept loop.
    listeners: BoundListeners,
    site: SharedSite,
    /// Cancelled by `shutdown()` or SIGINT.
    stop: CancellationToken,
    /// TPC threads, or the pool's sub-interpreters.
    workers: usize,
    /// Tokio threads of the multi-thread paths.
    io_workers: usize,
    num_cpus: usize,
}

/// Accept loops of the multi-thread (`PYRONOVA_TPC=0`) server. Linux's `SO_REUSEPORT`
/// load-balances connections across several loops; macOS's doesn't, so it gets one.
#[cfg(target_os = "linux")]
fn multi_thread_accept_loops(io_workers: usize, num_cpus: usize) -> usize {
    io_workers.min(num_cpus)
}

#[cfg(not(target_os = "linux"))]
fn multi_thread_accept_loops(_io_workers: usize, _num_cpus: usize) -> usize {
    1
}

/// How long the multi-thread server's in-flight connections get to finish after a stop.
const MULTI_THREAD_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The multi-thread (`PYRONOVA_TPC=0`) server, for both modes: one accept loop per
/// listener group on a Tokio runtime of `io_workers` threads; `serve_conn` serves each
/// connection. Runs until SIGINT or `shutdown()` (`until_stopped`), then lets the
/// in-flight connections drain for up to [`MULTI_THREAD_DRAIN_TIMEOUT`]: returning drops
/// the runtime, which would abort them mid-request.
fn serve_multi_thread<F, Fut>(
    groups: Vec<Vec<Listener>>,
    io_workers: usize,
    stop: CancellationToken,
    serve_conn: F,
) -> PyResult<()>
where
    F: Fn(Accepted, CancellationToken) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let rt = RuntimeBuilder::new_multi_thread()
        .worker_threads(io_workers)
        .enable_all()
        .build()
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("tokio runtime error: {e}"))
        })?;
    rt.block_on(async move {
        let conn_tracker = tokio_util::task::TaskTracker::new();
        for group in groups {
            let mut source = AcceptSource::new(group)?;
            let (token, tracker, serve_conn) =
                (stop.clone(), conn_tracker.clone(), serve_conn.clone());
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        accepted = source.accept() => {
                            tracker.spawn(serve_conn(accepted, token.clone()));
                        }
                        _ = token.cancelled() => break,
                    }
                }
            });
        }

        until_stopped(stop).await;
        conn_tracker.close();
        if tokio::time::timeout(MULTI_THREAD_DRAIN_TIMEOUT, conn_tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!(
                target: "pyronova::server",
                "{} in-flight connections did not drain within {:?} — exiting anyway",
                conn_tracker.len(),
                MULTI_THREAD_DRAIN_TIMEOUT,
            );
        }
        Ok(())
    })
}

/// The multi-thread GIL-mode server (`PYRONOVA_TPC=0`): every handler on the main
/// interpreter.
fn run_gil(py: Python<'_>, run: ServerRun) -> PyResult<()> {
    let ServerRun {
        listeners,
        site,
        stop,
        io_workers,
        num_cpus,
        ..
    } = run;
    tracing::info!(
        target: "pyronova::server",
        version = env!("CARGO_PKG_VERSION"),
        listening = ?listeners.bound,
        io_workers,
        cpus = num_cpus,
        mode = "gil",
        "Pyronova started"
    );
    println!("\n  Pyronova v{}", env!("CARGO_PKG_VERSION"));
    for listener in &listeners.bound {
        println!("  Listening on {listener}");
    }
    println!("  IO workers: {io_workers} (CPUs: {num_cpus})\n");

    py.detach(move || {
        serve_multi_thread(
            listeners.groups,
            io_workers,
            stop,
            move |accepted, token| {
                let site = Arc::clone(&site);
                async move {
                    let client_ip = accepted.remote.ip();
                    let Some(stream) =
                        crate::tls::wrap(accepted.stream, accepted.tls.as_deref()).await
                    else {
                        return;
                    };
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let site = Arc::clone(&site);
                        async move {
                            if websocket::is_websocket_upgrade(&req) {
                                websocket::handle_websocket(req, site, client_ip).await
                            } else {
                                handle_request(req, site, client_ip).await
                            }
                        }
                    });
                    drive_connection(stream, svc, TokioExecutor::new(), token).await;
                }
            },
        )
    })
}

impl PyronovaApp {
    /// In a worker, the app the script registers routes or hooks on is the one the worker
    /// serves: the first registration records it, and a registration on a second app is an
    /// error (Layer 2, FR-4; decision Q-2 (a)). Raw `PyronovaApp()` scripts and `Pyronova`
    /// apps work the same way. A no-op on the main interpreter.
    fn serve_in_worker(slf: &Bound<'_, Self>) -> PyResult<()> {
        worker_app::record(slf.as_any(), Self::worker_routes)
    }

    /// The routes and limits of `app` (a `PyronovaApp`), as its worker serves them.
    fn worker_routes<'py>(app: &Bound<'py, PyAny>) -> PyResult<WorkerRoutes<'py>> {
        let py = app.py();
        let app = app.cast::<Self>()?.borrow();
        let table = app.routes.read();
        let bind = |hooks: &[Py<PyAny>]| hooks.iter().map(|h| h.bind(py).clone()).collect();
        Ok(WorkerRoutes {
            signature: crate::router::RouteSignature::of(&table),
            handlers: table
                .routes()
                .iter()
                .map(|r| r.handler.bind(py).clone())
                .collect(),
            before_hooks: bind(&table.before_hooks),
            after_hooks: bind(&table.after_hooks),
            limits: app.limits,
        })
    }

    fn register_route(
        slf: &Bound<'_, Self>,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
    ) -> PyResult<()> {
        let py = slf.py();
        let name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        Self::serve_in_worker(slf)?;
        slf.borrow_mut()
            .add_route(method, path, handler, name, gil, stream, py)
    }

    /// Records the script's registration boundary unless one is already set; idempotent
    /// (Layer 2, FR-2).
    fn seal_if_unsealed(&self) {
        let mut routes = self.routes.write();
        if routes.sealed.is_none() {
            routes.sealed = Some(Sealed {
                routes: routes.routes().len(),
                before_hooks: routes.before_hooks.len(),
                after_hooks: routes.after_hooks.len(),
            });
        }
    }

    /// What a server run serves: a copy of the route table (holding its own references to
    /// the handlers) with this app's CORS and access-log settings.
    fn snapshot(&self, py: Python<'_>) -> Site {
        Site {
            routes: self.routes.read().clone_ref(py),
            config: SiteConfig {
                cors: self.cors.clone(),
                access_log: self.access_log.clone(),
                grpc_benchmark: self.grpc_benchmark,
                request_id_header: self.request_id_header.clone(),
                limits: self.limits,
                ws_connections: crate::websocket::OpenConnections::default(),
            },
        }
    }

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
        // Auto-detect if handler is async def (also check __call__ for class-based views).
        // A failing check fails the registration: guessing "sync" would dispatch an async
        // handler to the sync pool.
        let inspect = py.import("inspect")?;
        let is_coroutine_function = |f: &Bound<'_, PyAny>| -> PyResult<bool> {
            inspect.call_method1("iscoroutinefunction", (f,))?.extract()
        };
        let handler_obj = handler.bind(py);
        let is_async = is_coroutine_function(handler_obj)?
            || match handler_obj.getattr(pyo3::intern!(py, "__call__")) {
                Ok(call) => is_coroutine_function(&call)?,
                // Not callable through `__call__`: nothing more to inspect.
                Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => false,
                Err(e) => return Err(e),
            };

        // Streaming constraints (v1): GIL-only, sync handlers only.
        // Sub-interp streaming isn't supported (a worker handler returning a
        // Stream gets a 500); async streaming needs awaitable support.
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
        // Workers only know the routes the script registered (Layer 2, FR-2). A route added
        // after `run()` began, e.g. from an `on_startup` hook, exists only on main.
        if routes.sealed.is_some() && !gil {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "{} {path} is registered after app.run() started (for example from an on_startup \
                 hook). Such a route exists only in the main interpreter, so it needs gil=True.",
                method.to_uppercase()
            )));
        }
        let dispatch = match (gil, stream, is_async) {
            (true, true, _) => Dispatch::Main(RequestBody::Streamed),
            (true, false, _) => Dispatch::Main(RequestBody::Buffered),
            (false, _, false) => Dispatch::Worker(HandlerKind::Sync),
            (false, _, true) => Dispatch::Worker(HandlerKind::Async),
        };
        routes
            .insert(method, path, handler, handler_name, dispatch)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("route error: {e}")))?;
        Ok(())
    }

    /// The multi-thread sub-interpreter server (`PYRONOVA_TPC=0`): a channel pool of
    /// workers behind Tokio accept loops.
    fn run_subinterp(
        &self,
        py: Python<'_>,
        run: ServerRun,
        env: &EnvConfig,
    ) -> PyResult<Vec<AbandonedWorker>> {
        let ServerRun {
            listeners,
            site: routes,
            stop,
            workers,
            io_workers,
            num_cpus,
        } = run;
        let program = self.worker_program(py)?;

        // What every worker's script must register (Layer 2, C3), as plain values.
        let expected = crate::router::RouteSignature::of(&routes.routes);
        let shape = crate::router::RouteShape::of(&routes.routes);
        let split =
            crate::python::pool::split_workers_for_routes(workers, &shape.gil, &shape.is_async)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let gil_count = shape.gil_count();
        let async_count_routes = shape.async_count();
        let subinterp_count = shape.gil.len() - gil_count;
        let has_async = async_count_routes > 0;
        let mode_label = if has_async { "hybrid-async" } else { "hybrid" };
        tracing::info!(
            target: "pyronova::server",
            version = env!("CARGO_PKG_VERSION"),
            mode = mode_label,
            listening = ?listeners.bound,
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
        for listener in &listeners.bound {
            println!("  Listening on {listener}");
        }
        println!("  Sub-interpreters: {workers} | IO threads: {io_workers} (CPUs: {num_cpus})");
        if split.async_workers > 0 {
            println!(
                "  Workers: {} sync + {} async",
                split.sync_workers, split.async_workers
            );
        }
        println!(
            "  Routes: {subinterp_count} sub-interp + {gil_count} GIL + {async_count_routes} async"
        );
        println!("  Script: {}\n", program.script_path);

        let spec = WorkerSpec {
            program: &program,
            expected: &expected,
            shared_state: &self.shared_state,
            gc_threshold: env.gc.threshold,
            limits: routes.config.limits,
        };
        // SAFETY: on the main thread with main's thread state current (`py`).
        let (pool, threads) =
            unsafe { InterpreterPool::new(split, &spec) }.map_err(|e| e.into_pyerr(py))?;
        let pool = Arc::new(pool);

        let served = py.detach(move || {
            serve_multi_thread(
                listeners.groups,
                io_workers,
                stop,
                move |accepted, token| {
                    let (pool, site) = (Arc::clone(&pool), Arc::clone(&routes));
                    async move {
                        let client_ip = accepted.remote.ip();
                        let Some(stream) =
                            crate::tls::wrap(accepted.stream, accepted.tls.as_deref()).await
                        else {
                            return;
                        };
                        let svc = service_fn(move |req: Request<Incoming>| {
                            let (pool, site) = (Arc::clone(&pool), Arc::clone(&site));
                            async move {
                                if websocket::is_websocket_upgrade(&req) {
                                    websocket::handle_websocket(req, site, client_ip).await
                                } else {
                                    handle_request_subinterp(req, pool, site, client_ip).await
                                }
                            }
                        });
                        drive_connection(stream, svc, TokioExecutor::new(), token).await;
                    }
                },
            )
        });
        // Every pool reference was in the serving closures, gone with them: the workers
        // finish. The ones that don't within the grace period go back to this run's caller.
        let abandoned = py.detach(|| threads.join());
        if served.is_err() && !abandoned.is_empty() {
            tracing::error!(
                target: "pyronova::server",
                "the server failed with worker(s) still running: {}",
                abandoned.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
            );
        }
        served.map(|()| abandoned)
    }

    /// TPC sub-interp inline mode — each TPC thread owns its
    /// own sub-interp and runs handlers synchronously on the accept
    /// thread. No pool, no channel, no oneshot wake.
    fn run_tpc_subinterp(&self, py: Python<'_>, run: ServerRun, env: &EnvConfig) -> PyResult<()> {
        let ServerRun {
            listeners,
            site: routes,
            stop,
            workers: n_threads,
            num_cpus,
            ..
        } = run;
        // gil=True routes and response streams: main-interp bridge.
        // async def: the worker drives the coroutine on its own persistent asyncio loop.
        //   This is "blocking async": the TPC thread is blocked for the coroutine's
        //   entire execution. Awaits inside the coroutine still run on that loop, so
        //   `await asyncio.sleep()` or `await client.get()` work; they just don't yield
        //   to OTHER requests on the same TPC thread. SO_REUSEPORT spreads connections
        //   across threads, so a slow async handler only blocks its one thread.
        // stream=True: gated by gil=True at registration, so it flows through the bridge.
        let workers = self.build_workers(py, n_threads, &routes, env.gc.count_trigger())?;

        // The main-interp bridge serves `gil=True` routes and the fallback with the main
        // GIL, while TPC threads handle the rest inline. See src/bridge/main_bridge.rs.
        let bridge = routes.routes.uses_main().then(|| {
            crate::bridge::main_bridge::MainInterpBridge::spawn(Arc::clone(&routes), env.bridge)
        });
        let server = crate::tpc::TpcServer {
            workers,
            site: routes,
            bridge: bridge.clone(),
            gc: env.gc,
            topology: env.darwin_topology,
        };

        py.detach(move || {
            let served = crate::tpc::run_tpc_subinterp(listeners, num_cpus, server, stop);
            // The TPC threads are joined, so this is the last bridge reference: close it and
            // wait for its threads to release their Python objects (FR-6).
            if let Some(bridge) = bridge {
                crate::bridge::main_bridge::MainInterpBridge::shutdown_join(bridge);
            }
            served
        })
        .map_err(PyErr::from)
    }

    /// The program workers run: the script `set_script_path` named, else
    /// `__main__.__file__`, with main's `sys.path` as it is now.
    fn worker_program(&self, py: Python<'_>) -> PyResult<WorkerProgram> {
        let script_path = match &self.script_path {
            Some(path) => path.clone(),
            None => py.import("__main__")?.getattr("__file__")?.extract()?,
        };
        WorkerProgram::read(py, script_path, self.isolated.clone())
    }

    /// Builds `n` TPC sub-interpreter workers, in order, on the main thread: each runs the
    /// app's script and must register `site`'s routes. If worker `i` fails, the workers already
    /// built are ended here, on the thread that created them (FR-19), before the error is
    /// raised. The workers come back with their thread state saved; the thread that
    /// serves one rebinds it first.
    fn build_workers(
        &self,
        py: Python<'_>,
        n: usize,
        site: &Site,
        gc_threshold: u64,
    ) -> PyResult<Vec<SubInterpreterWorker>> {
        if !crate::run_context::on_main(py) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "sub-interpreter workers are built on the main interpreter only",
            ));
        }
        let program = self.worker_program(py)?;

        let expected = crate::router::RouteSignature::of(&site.routes);
        let spec = WorkerSpec {
            program: &program,
            expected: &expected,
            shared_state: &self.shared_state,
            gc_threshold,
            limits: site.config.limits,
        };
        let mut built = Vec::with_capacity(n);
        for i in 0..n {
            // SAFETY: on the main thread with main's thread state current (checked above).
            let worker = unsafe { SubInterpreterWorker::new(i, &spec) };
            match worker {
                Ok(w) => built.push(w),
                Err(e) => {
                    // SAFETY: as above; none of `built` was rebound to another thread.
                    unsafe { SubInterpreterWorker::end_all(built) };
                    return Err(e.into_pyerr(py));
                }
            }
        }
        Ok(built)
    }
}

#[cfg(feature = "bench")]
impl PyronovaApp {
    /// What a bench serves: the sealed route table, which must be all sync, non-GIL,
    /// non-stream (the TPC inline path with no main-interpreter bridge).
    fn bench_site(&self, py: Python<'_>) -> PyResult<Site> {
        self.seal_if_unsealed();
        let site = self.snapshot(py);
        if !crate::router::RouteShape::of(&site.routes).all_inline_sync() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "the benches support only sync, non-GIL, non-stream routes",
            ));
        }
        Ok(site)
    }
}

fn parse_cors(spec: &CorsSpec<'_>) -> PyResult<Cors> {
    Cors::parse(spec).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}
