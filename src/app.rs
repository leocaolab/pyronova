use std::sync::Arc;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use pyo3::prelude::*;
use std::net::SocketAddr;
use std::num::NonZeroUsize;

use tokio::runtime::Builder as RuntimeBuilder;
use tokio::signal;
use tokio_util::sync::CancellationToken;

use crate::config::{ConfigError, EnvConfig, Mode, Sizing, Topology};
use crate::handlers::{handle_request, handle_request_subinterp};
use crate::python::interp;
use crate::router::{Dispatch, HandlerKind, MutableRoutes, RequestBody, RouteTable, Sealed};
use crate::server::cpu::Cpus;
use crate::server::listener::{AcceptSource, Accepted, BoundListeners, Listener, ListenerSpec};
use crate::site::{AccessLog, Cors, CorsSpec, Limits, SharedSite, Site, SiteConfig};
use crate::state::SharedState;
use crate::websocket;
use crate::worker::drive_connection;
use hyper_util::rt::TokioExecutor;

/// The `PyronovaApp` a worker's script created: one per worker interpreter (Layer 2, C3).
/// The worker takes its handlers from it after the script has run.
static WORKER_APP: pyo3::sync::PyOnceLock<Py<PyronovaApp>> = pyo3::sync::PyOnceLock::new();

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
    /// This app's response compression, served by its runs; `None` = off.
    compression: Option<crate::compression::Settings>,
}

pyo3::create_exception!(
    pyronova.engine,
    RegistrationSealed,
    pyo3::exceptions::PyValueError,
    "A route or hook registered after the app's registrations were sealed (the first \
     server's start). Workers rebuild the app by running its script, so they only have \
     what the script registered: a late hook would silently skip every worker route. A \
     ValueError."
);

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
            compression: None,
        }
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

    /// This app's response compression: a `Compression` to turn it on, `None` to turn it
    /// off (the default). Per app, like the limits.
    #[pyo3(signature = (settings))]
    fn configure_compression(
        &mut self,
        settings: Option<&Bound<'_, crate::compression::Settings>>,
    ) {
        self.compression = settings.map(|s| *s.get());
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
        let app = slf.borrow();
        let mut routes = app.routes.write();
        refuse_sealed_hook(&routes, "a before_request hook")?;
        routes.before_hooks.push(handler);
        Ok(())
    }

    fn after_request(slf: &Bound<'_, Self>, handler: Py<PyAny>) -> PyResult<()> {
        Self::serve_in_worker(slf)?;
        let app = slf.borrow();
        let mut routes = app.routes.write();
        refuse_sealed_hook(&routes, "an after_request hook")?;
        routes.after_hooks.push(handler);
        Ok(())
    }

    fn fallback(slf: &Bound<'_, Self>, handler: Py<PyAny>) -> PyResult<()> {
        Self::serve_in_worker(slf)?;
        let app = slf.borrow();
        let mut routes = app.routes.write();
        refuse_sealed_hook(&routes, "a fallback handler")?;
        routes.set_fallback(handler);
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

    /// Prepares one server of this app: resolves its configuration, freezes what it serves
    /// and binds its listeners (a port in use is `OSError(EADDRINUSE)` here). Nothing
    /// serves until the returned `Server`'s `serve()`; its `shutdown()` stops that one
    /// server, so several servers of one app stop independently.
    #[pyo3(signature = (
        host=None, port=None, workers=None, mode=None, io_workers=None,
        tls_cert=None, tls_key=None, extra_tls_ports=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn start(
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
    ) -> PyResult<Server> {
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
        // Everything this run takes from its arguments and the environment, parsed once; a
        // bad value stops it here, before anything is built.
        let sizing = Sizing {
            workers: positive("workers", workers)?,
            io_workers: positive("io_workers", io_workers)?,
        };
        let addr = socket_addr(host.unwrap_or("127.0.0.1"), port.unwrap_or(8000))?;
        let env = EnvConfig::from_env()?;
        let mode = mode.map(Mode::from_arg).transpose()?.unwrap_or(Mode::Gil);
        let cpus = Cpus::detect();
        let topology = Topology::resolve(mode, &env, sizing, cpus).map_err(ConfigError::from)?;

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

        // Process-level, refreshed every start: tests / hot-reload may flip PYRONOVA_METRICS
        // between runs, and each new server honours the latest value.
        crate::monitor::init_metrics_flag(env.metrics);

        // A raw-engine script never calls `_seal_registrations`; seal here so every served
        // table has the script's boundary that workers compare against (Layer 2, FR-2).
        self.seal_if_unsealed();
        // Freeze route table: extract from RwLock into read-only Arc.
        // After this point, no more route registration — zero-lock reads.
        let site: SharedSite = Arc::new(self.snapshot(py));

        // Every listener is bound before any thread or worker exists: a port in use is
        // this call's `OSError(EADDRINUSE)`, not a log line from a serving thread.
        let listeners = BoundListeners::bind(&specs, topology.accept_loops().get())?;
        let port = listeners
            .bound
            .first()
            .map(|bound| bound.addr.port())
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("the server has no listener")
            })?;
        let stop = CancellationToken::new();
        Ok(Server {
            stop: stop.clone(),
            port,
            run: parking_lot::Mutex::new(Some(ServerRun {
                listeners,
                site,
                stop,
                topology,
                env,
                cpus,
                app: self.app_source(),
            })),
        })
    }

    /// `start()` then `serve()`: binds, then serves until SIGINT; for raw-engine scripts.
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
        self.start(
            py,
            host,
            port,
            workers,
            mode,
            io_workers,
            tls_cert,
            tls_key,
            extra_tls_ports,
        )?
        .serve(py)
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
        let workers = build_workers(
            py,
            n,
            &sites[0],
            env.gc.count_trigger(),
            &self.app_source().program(py)?,
            &self.shared_state,
        )?;

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
        let workers = build_workers(
            py,
            n,
            &site,
            env.gc.count_trigger(),
            &self.app_source().program(py)?,
            &self.shared_state,
        )?;

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
        None => Ok(crate::server::cpu::physical_core_count().get()),
        Some(0) => Err(pyo3::exceptions::PyValueError::new_err(
            "bench workers must be at least 1",
        )),
        Some(n) => Ok(n),
    }
}

/// A count argument of `run()`: `None` takes the default, 0 is a `ValueError` (a server
/// with no worker or no IO thread serves nothing).
fn positive(name: &str, value: Option<usize>) -> PyResult<Option<std::num::NonZeroUsize>> {
    value
        .map(|n| {
            std::num::NonZeroUsize::new(n).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "{name} must be at least 1, got 0 (leave it unset for the default)"
                ))
            })
        })
        .transpose()
}

/// The address `run(host=…, port=…)` names. The host is an IP address, v4 or v6 (bare or
/// in brackets); a name such as `localhost` is not resolved.
fn socket_addr(host: &str, port: u16) -> Result<SocketAddr, BadHost> {
    let ip = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    ip.parse::<std::net::IpAddr>()
        .map(|ip| SocketAddr::new(ip, port))
        .map_err(|source| BadHost {
            host: host.to_string(),
            source,
        })
}

#[derive(Debug, thiserror::Error)]
#[error(
    "host {host:?} is not an IP address ({source}); use e.g. \"127.0.0.1\", \"0.0.0.0\" or \
     \"::1\" (host names such as \"localhost\" are not resolved)"
)]
struct BadHost {
    host: String,
    source: std::net::AddrParseError,
}

impl From<BadHost> for PyErr {
    fn from(e: BadHost) -> Self {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
}

/// Resolves once the server should stop, then cancels `stop` so every accept loop and
/// connection sees it: on SIGINT, or when `Server.shutdown()` cancelled `stop`. If the
/// SIGINT handler cannot be installed, the server keeps serving until `shutdown()`.
pub(crate) async fn until_stopped(stop: CancellationToken) {
    tokio::select! {
        signalled = signal::ctrl_c() => {
            if let Err(e) = signalled {
                tracing::error!(
                    target: "pyronova::server",
                    error = %e,
                    "ctrl_c signal handler registration failed; cannot receive SIGINT. \
                     Serving until Server.shutdown(), SIGTERM or SIGKILL."
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

/// One server of an app, bound by `PyronovaApp.start()`. `serve()` runs it until SIGINT
/// or this server's `shutdown()`; its listeners are released when it returns.
#[pyclass(module = "pyronova.engine", frozen)]
pub(crate) struct Server {
    /// Cancelled by `shutdown()` or SIGINT; shared with the run.
    stop: CancellationToken,
    /// The first listener's port (a `port=0` resolved to the kernel's pick).
    port: u16,
    /// What `serve()` runs; taken by it, so a server serves once.
    run: parking_lot::Mutex<Option<ServerRun>>,
}

#[pymethods]
impl Server {
    /// Serves until SIGINT or `shutdown()`, blocking this thread; then drains the in-flight
    /// connections and returns. A server stopped before it served returns at once. Serving
    /// a server a second time raises `RuntimeError`.
    pub(crate) fn serve(&self, py: Python<'_>) -> PyResult<()> {
        let run = self.run.lock().take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "this server has already served; start() another",
            )
        })?;
        run.serve(py)
    }

    /// Stops this server the way SIGINT does: stop accepting, drain the in-flight
    /// connections, return from `serve()`. Callable from any thread, before or during
    /// `serve()`; idempotent. Other servers of the same app keep serving.
    fn shutdown(&self) {
        self.stop.cancel();
    }

    /// The port this server listens on (its first listener's).
    #[getter]
    fn port(&self) -> u16 {
        self.port
    }
}

/// The app a server's workers rebuild by executing its script.
struct AppSource {
    /// `set_script_path`'s script, else `__main__.__file__` (read when the workers are
    /// built).
    script_path: Option<String>,
    shared_state: Arc<dashmap::DashMap<String, bytes::Bytes>>,
}

impl AppSource {
    /// The program workers run: the script, with main's `sys.path` as it is now.
    fn program(&self, py: Python<'_>) -> PyResult<interp::WorkerProgram> {
        let script_path = match &self.script_path {
            Some(path) => path.clone(),
            None => py.import("__main__")?.getattr("__file__")?.extract()?,
        };
        interp::WorkerProgram::read(py, script_path)
    }
}

/// One server as `start()` resolved it: what `serve()` runs.
struct ServerRun {
    /// One group per accept loop (`Topology::accept_loops`).
    listeners: BoundListeners,
    site: SharedSite,
    /// Cancelled by `shutdown()` or SIGINT.
    stop: CancellationToken,
    topology: Topology,
    env: EnvConfig,
    cpus: Cpus,
    app: AppSource,
}

impl ServerRun {
    fn serve(self, py: Python<'_>) -> PyResult<()> {
        // The route table holds main-interpreter `Py<T>`s. Worker, bridge and Tokio
        // threads hold clones and may drop theirs anywhere, including at runtime shutdown;
        // this one is the last, dropped when `serve` returns, on this thread, attached
        // (FR-6).
        let _site_keepalive = Arc::clone(&self.site);
        if self.stop.is_cancelled() {
            // Stopped before it served (e.g. `TestClient.close()` during its start):
            // nothing to build, the listeners are released here.
            return Ok(());
        }
        // Sampled while this server serves; stopped on every way out of here.
        let _rss = self.env.metrics.then(|| {
            tracing::info!(target: "pyronova::server", "Metrics enabled (PYRONOVA_METRICS=1): passive GIL monitor + RSS sampler");
            crate::monitor::RssSampling::start()
        });
        match self.topology {
            Topology::TpcGil { .. } => {
                let ServerRun {
                    listeners,
                    site,
                    stop,
                    cpus,
                    ..
                } = self;
                py.detach(move || {
                    crate::tpc::run_tpc_gil(listeners, cpus.logical.get(), site, stop)
                })
                .map_err(PyErr::from)
            }
            Topology::TpcWorkers { threads } => {
                serve_tpc_workers(py, self, threads, crate::tpc::run_per_thread_listener)
            }
            #[cfg(target_os = "macos")]
            Topology::TpcFanout { threads } => {
                serve_tpc_workers(py, self, threads, crate::tpc::run_fanout)
            }
            Topology::MultiThreadGil { io_threads, .. } => run_gil(py, self, io_threads),
            Topology::Pool {
                workers,
                io_threads,
                ..
            } => run_subinterp(py, self, workers, io_threads),
        }
    }
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
fn run_gil(py: Python<'_>, run: ServerRun, io_threads: NonZeroUsize) -> PyResult<()> {
    let ServerRun {
        listeners,
        site,
        stop,
        cpus,
        ..
    } = run;
    let (io_workers, num_cpus) = (io_threads.get(), cpus.logical.get());
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

/// The handlers and hooks of the app a worker's script registered on, indexed like main's
/// table, with their signature (Layer 2, C3).
pub(crate) struct WorkerRoutes {
    pub(crate) signature: crate::router::RouteSignature,
    pub(crate) handlers: Vec<Py<PyAny>>,
    pub(crate) before_hooks: Vec<Py<PyAny>>,
    pub(crate) after_hooks: Vec<Py<PyAny>>,
    /// The limits the script set on its app (never served: main's app is).
    pub(crate) limits: Limits,
}

impl WorkerRoutes {
    /// A script that registered nothing (a main table with no routes either).
    pub(crate) fn empty() -> Self {
        WorkerRoutes {
            signature: crate::router::RouteSignature::default(),
            handlers: Vec::new(),
            before_hooks: Vec::new(),
            after_hooks: Vec::new(),
            limits: Limits::DEFAULT,
        }
    }
}

/// The routes of the app this worker's script registered on, or `None` if it registered
/// none. Meaningful only in a worker, after its script ran.
pub(crate) fn worker_routes(py: Python<'_>) -> Option<WorkerRoutes> {
    let app = WORKER_APP.get(py)?.bind(py).borrow();
    let table = app.routes.read();
    Some(WorkerRoutes {
        signature: crate::router::RouteSignature::of(&table),
        handlers: table
            .routes()
            .iter()
            .map(|r| r.handler.clone_ref(py))
            .collect(),
        before_hooks: table.before_hooks.iter().map(|h| h.clone_ref(py)).collect(),
        after_hooks: table.after_hooks.iter().map(|h| h.clone_ref(py)).collect(),
        limits: app.limits,
    })
}

impl PyronovaApp {
    /// In a worker, the app the script registers routes or hooks on is the one the worker
    /// serves: the first registration records it, and a registration on a second app is an
    /// error (Layer 2, FR-4; decision Q-2 (a)). Raw `PyronovaApp()` scripts and `Pyronova`
    /// apps work the same way. A no-op on the main interpreter.
    fn serve_in_worker(slf: &Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        if crate::run_context::on_main(py) {
            return Ok(());
        }
        match WORKER_APP.get(py) {
            Some(app) if app.bind(py).is(slf) => Ok(()),
            Some(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "the script registers routes or hooks on a second app; a worker serves \
                 exactly one app per script (create one Pyronova()/PyronovaApp() and register \
                 everything on it)",
            )),
            None => WORKER_APP.set(py, slf.clone().unbind()).map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err("worker app already recorded")
            }),
        }
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

    fn app_source(&self) -> AppSource {
        AppSource {
            script_path: self.script_path.clone(),
            shared_state: Arc::clone(&self.shared_state),
        }
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
                compression: self.compression,
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

        let dispatch = route_dispatch(gil, stream, is_async).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "{} {path}: {e}",
                method.to_uppercase()
            ))
        })?;

        let mut routes = self.routes.write();
        // Workers only know the routes the script registered (Layer 2, FR-2). A route added
        // after `run()` began, e.g. from an `on_startup` hook, exists only on main.
        if routes.sealed.is_some() && !gil {
            return Err(RegistrationSealed::new_err(format!(
                "{} {path} is registered after app.run() started (for example from an on_startup \
                 hook). Such a route exists only in the main interpreter, so it needs gil=True.",
                method.to_uppercase()
            )));
        }
        routes
            .insert(method, path, handler, handler_name, dispatch)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("route error: {e}")))?;
        Ok(())
    }
}

/// The multi-thread sub-interpreter server (`PYRONOVA_TPC=0`): a channel pool of
/// workers behind Tokio accept loops.
fn run_subinterp(
    py: Python<'_>,
    run: ServerRun,
    workers: NonZeroUsize,
    io_threads: NonZeroUsize,
) -> PyResult<()> {
    let ServerRun {
        listeners,
        site: routes,
        stop,
        env,
        cpus,
        app,
        ..
    } = run;
    let (workers, io_workers, num_cpus) = (workers.get(), io_threads.get(), cpus.logical.get());
    let program = app.program(py)?;

    let shape = crate::router::RouteShape::of(&routes.routes);
    let split = interp::split_workers_for_routes(workers, &shape.gil, &shape.is_async)
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

    let pool = Arc::new(build_pool(
        py,
        split,
        &routes,
        env.gc.threshold,
        &program,
        &app,
    )?);

    py.detach(move || {
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
    })
}

/// A TPC sub-interpreter server: how it takes connections (`Topology`).
type TpcServe = fn(
    BoundListeners,
    usize,
    crate::tpc::TpcServer,
    CancellationToken,
) -> Result<(), crate::tpc::ServeError>;

/// The TPC sub-interpreter server: `threads` TPC threads, each owning a worker that runs
/// the `def` routes inline; `async def` routes on an async pool (decision Q1), so their
/// awaits overlap and their 504 is sent on time; `gil=True` routes and the fallback on the
/// main-interpreter bridge.
fn serve_tpc_workers(
    py: Python<'_>,
    run: ServerRun,
    threads: NonZeroUsize,
    serve: TpcServe,
) -> PyResult<()> {
    let ServerRun {
        listeners,
        site: routes,
        stop,
        env,
        cpus,
        app,
        ..
    } = run;
    let program = app.program(py)?;

    let shape = crate::router::RouteShape::of(&routes.routes);
    let async_routes = shape.async_count();
    let sync_routes = shape.gil.len() - shape.gil_count() - async_routes;
    let async_workers = if async_routes > 0 {
        crate::config::tpc_async_workers(threads, sync_routes > 0).get()
    } else {
        0
    };
    // Built first: if the TPC workers then fail, dropping the pool ends its workers on
    // their own threads.
    let async_pool = if async_workers > 0 {
        let split = interp::WorkerSplit {
            sync_workers: 0,
            async_workers,
        };
        let pool = build_pool(py, split, &routes, env.gc.count_trigger(), &program, &app)?;
        Some(Arc::new(pool))
    } else {
        None
    };
    let workers = match build_workers(
        py,
        threads.get(),
        &routes,
        env.gc.count_trigger(),
        &program,
        &app.shared_state,
    ) {
        Ok(workers) => workers,
        Err(e) => {
            py.detach(move || drop(async_pool));
            return Err(e);
        }
    };

    // The main-interp bridge serves `gil=True` routes and the fallback with the main
    // GIL, while TPC threads handle the rest. See src/bridge/main_bridge.rs.
    let bridge = routes.routes.uses_main().then(|| {
        crate::bridge::main_bridge::MainInterpBridge::spawn(Arc::clone(&routes), env.bridge)
    });
    let server = crate::tpc::TpcServer {
        workers,
        site: routes,
        bridge: bridge.clone(),
        async_pool: async_pool.clone(),
        async_workers,
        gc: env.gc,
    };

    py.detach(move || {
        let served = serve(listeners, cpus.logical.get(), server, stop);
        // The TPC threads are joined, so these are the last references: the async pool
        // joins its workers, the bridge its threads (FR-6).
        drop(async_pool);
        if let Some(bridge) = bridge {
            crate::bridge::main_bridge::MainInterpBridge::shutdown_join(bridge);
        }
        served
    })
    .map_err(PyErr::from)
}

/// What every worker of a server is built from: the app's program, the routes it must
/// register (`site`'s, up to the seal), the shared state and the served limits.
fn worker_spec<'a>(
    program: &'a interp::WorkerProgram,
    expected: &'a crate::router::RouteSignature,
    site: &Site,
    gc_threshold: u64,
    shared_state: &'a crate::state::SharedMap,
) -> interp::WorkerSpec<'a> {
    interp::WorkerSpec {
        program,
        expected,
        // Each worker gets it as `POOL_ID` (the async engine's zombie guard).
        pool_id: interp::next_pool_id(),
        shared_state,
        gc_threshold,
        limits: site.config.limits,
    }
}

/// A channel pool of `split` workers, each on its own thread, serving `site`.
fn build_pool(
    py: Python<'_>,
    split: interp::WorkerSplit,
    site: &Site,
    gc_threshold: u64,
    program: &interp::WorkerProgram,
    app: &AppSource,
) -> PyResult<interp::InterpreterPool> {
    let expected = crate::router::RouteSignature::of(&site.routes);
    let spec = worker_spec(program, &expected, site, gc_threshold, &app.shared_state);
    // SAFETY: called with the main interpreter attached (`py`), before `py.detach`.
    unsafe { interp::InterpreterPool::new(split, py, &spec) }.map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("sub-interpreter pool error: {e}"))
    })
}

/// Builds `n` TPC sub-interpreter workers, in order, on the main thread: each runs the
/// app's script and must register `site`'s routes. If worker `i` fails, the workers already
/// built are ended here, on the thread that created them (FR-19), before the error is
/// raised. The workers come back with their thread state saved; the thread that
/// serves one rebinds it first.
fn build_workers(
    py: Python<'_>,
    n: usize,
    site: &Site,
    gc_threshold: u64,
    program: &interp::WorkerProgram,
    shared_state: &crate::state::SharedMap,
) -> PyResult<Vec<interp::SubInterpreterWorker>> {
    if !crate::run_context::on_main(py) {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "sub-interpreter workers are built on the main interpreter only",
        ));
    }
    let expected = crate::router::RouteSignature::of(&site.routes);
    let spec = worker_spec(program, &expected, site, gc_threshold, shared_state);
    let mut built = Vec::with_capacity(n);
    for i in 0..n {
        // SAFETY: on the main thread with main's thread state current (checked above).
        let worker = unsafe { interp::SubInterpreterWorker::new(i, &spec) };
        match worker {
            Ok(w) => built.push(w),
            Err(e) => {
                // SAFETY: as above; none of `built` was rebound to another thread.
                unsafe { interp::SubInterpreterWorker::end_all(built) };
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "sub-interp {i} init: {e}"
                )));
            }
        }
    }
    Ok(built)
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

/// A route's `gil` / `stream` / handler-kind combination that no serving path runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
enum UnservableRoute {
    /// A streamed body is fed from the connection's thread to a handler on the main
    /// interpreter; a worker handler has no such feed and would find `req.stream` empty.
    #[error("stream=True needs gil=True: a streamed request body is only fed to handlers on the main interpreter")]
    StreamOffMain,
    #[error("stream=True is not supported on `async def` handlers; use a `def` handler")]
    StreamAsync,
}

/// Where a route with these registration flags runs, or why none can run it.
fn route_dispatch(gil: bool, stream: bool, is_async: bool) -> Result<Dispatch, UnservableRoute> {
    match (gil, stream, is_async) {
        (_, true, true) => Err(UnservableRoute::StreamAsync),
        (false, true, false) => Err(UnservableRoute::StreamOffMain),
        (true, true, false) => Ok(Dispatch::Main(RequestBody::Streamed)),
        (true, false, _) => Ok(Dispatch::Main(RequestBody::Buffered)),
        (false, false, false) => Ok(Dispatch::Worker(HandlerKind::Sync)),
        (false, false, true) => Ok(Dispatch::Worker(HandlerKind::Async)),
    }
}

/// A hook (or the fallback) registered once the registrations are sealed would exist
/// only on main, so every worker route would skip it — e.g. an auth `before_request` added
/// by an `on_startup` hook. Refused with the error a late worker route gets.
fn refuse_sealed_hook(routes: &RouteTable, what: &str) -> PyResult<()> {
    match routes.sealed {
        None => Ok(()),
        Some(_) => Err(RegistrationSealed::new_err(format!(
            "{what} is registered after app.run() started (for example from an on_startup \
             hook, or enable_cors() there). Workers only run what the script registered, so \
             every worker route would skip it; register it at module level."
        ))),
    }
}

fn parse_cors(spec: &CorsSpec<'_>) -> PyResult<Cors> {
    Cors::parse(spec).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_streamed_body_needs_a_sync_main_interpreter_handler() {
        use Dispatch::{Main, Worker};
        assert_eq!(
            route_dispatch(true, true, false),
            Ok(Main(RequestBody::Streamed))
        );
        assert_eq!(
            route_dispatch(false, true, false),
            Err(UnservableRoute::StreamOffMain)
        );
        assert_eq!(
            route_dispatch(false, true, true),
            Err(UnservableRoute::StreamAsync)
        );
        assert_eq!(
            route_dispatch(true, true, true),
            Err(UnservableRoute::StreamAsync)
        );
        assert_eq!(
            route_dispatch(true, false, true),
            Ok(Main(RequestBody::Buffered))
        );
        assert_eq!(
            route_dispatch(false, false, false),
            Ok(Worker(HandlerKind::Sync))
        );
        assert_eq!(
            route_dispatch(false, false, true),
            Ok(Worker(HandlerKind::Async))
        );
    }

    #[test]
    fn a_host_is_an_ip_address_and_its_error_names_it() {
        assert_eq!(
            socket_addr("127.0.0.1", 80).unwrap(),
            "127.0.0.1:80".parse().unwrap()
        );
        assert_eq!(socket_addr("::1", 80).unwrap(), "[::1]:80".parse().unwrap());
        assert_eq!(
            socket_addr("[::1]", 80).unwrap(),
            "[::1]:80".parse().unwrap()
        );
        for host in ["localhost", "", "1.2.3", "[127.0.0.1"] {
            let text = socket_addr(host, 80).unwrap_err().to_string();
            assert!(text.starts_with(&format!("host {host:?} ")), "{text}");
        }
    }
}
