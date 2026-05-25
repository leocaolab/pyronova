use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use matchit::Router;
use parking_lot::RwLock;
use pyo3::prelude::*;

/// A response entirely built at registration time — no Python call,
/// no serialization, no allocation on the request path. Served directly
/// from the accept loop for exact-match (method, path) lookups. Use
/// cases: `/pipeline "ok"`, `/health`, `/robots.txt`, maintenance
/// pages, any constant-body endpoint.
#[derive(Clone)]
pub(crate) struct FastResponse {
    pub(crate) body: Bytes,
    pub(crate) content_type: String,
    pub(crate) status: u16,
    pub(crate) headers: HashMap<String, String>,
}

/// Full CORS configuration. When present, applied to every response —
/// not just OPTIONS preflight — per W3C CORS spec requirements for
/// Access-Control-Allow-Credentials and Access-Control-Expose-Headers.
#[derive(Clone, Default)]
pub(crate) struct CorsConfig {
    pub(crate) origin: String,
    pub(crate) methods: String,
    pub(crate) headers: String,
    pub(crate) expose_headers: Option<String>,
    pub(crate) allow_credentials: bool,
}

pub(crate) struct RouteTable {
    pub(crate) handlers: Vec<Py<PyAny>>,
    pub(crate) handler_names: Vec<String>,
    pub(crate) requires_gil: Vec<bool>,
    pub(crate) is_async: Vec<bool>,
    /// Per-route streaming flag. When true, the accept loop skips the
    /// body collect and attaches a `PyronovaBodyStream` to the request.
    pub(crate) is_stream: Vec<bool>,
    pub(crate) routers: HashMap<String, Router<usize>>,
    pub(crate) ws_handlers: HashMap<String, Py<PyAny>>,
    pub(crate) before_hooks: Vec<Py<PyAny>>,
    pub(crate) after_hooks: Vec<Py<PyAny>>,
    pub(crate) before_hook_names: Vec<String>,
    pub(crate) after_hook_names: Vec<String>,
    pub(crate) fallback_handler: Option<Py<PyAny>>,
    pub(crate) fallback_handler_name: Option<String>,
    pub(crate) static_dirs: Vec<(String, String)>,
    pub(crate) cors_config: Option<CorsConfig>,
    pub(crate) request_logging: bool,
    /// Sample 1-in-N requests when access logging is enabled. `1` (the
    /// default) logs every request; `100` keeps roughly 1% of normal
    /// traffic to keep observability without paying full log cost. The
    /// `request_log_always_status` floor is checked first — a 5xx
    /// always logs even if it loses the sampling roll.
    pub(crate) request_log_sample_n: u64,
    /// Bypass sampling for responses with status >= this value. Set to
    /// 400 to "always log errors, sample successes". `0` (default)
    /// disables the bypass — sampling applies to every status.
    pub(crate) request_log_always_status: u16,
    /// Atomic counter advanced once per sampled request. Held in an
    /// Arc so route-table clones share the same sample roll — without
    /// this, each TPC worker's per-thread copy would have its own
    /// counter and `sample_n=100` would log N% of every worker's
    /// traffic = effectively N × workers % overall.
    pub(crate) request_log_counter: Arc<std::sync::atomic::AtomicU64>,
    /// Exact-match (METHOD, path) → pre-built response, served from
    /// the accept loop before any Python dispatch. Nested map keyed
    /// by method then path so the lookup accepts `&str` directly via
    /// the `Borrow<str>` impl on `String` — zero allocation on the
    /// hot path. At 2M+ req/s the old `(method.to_string(), path.to_string())`
    /// key cost two heap allocations per request.
    pub(crate) fast_responses: HashMap<String, HashMap<String, FastResponse>>,
}

impl RouteTable {
    pub(crate) fn new() -> Self {
        RouteTable {
            handlers: Vec::new(),
            handler_names: Vec::new(),
            requires_gil: Vec::new(),
            is_async: Vec::new(),
            is_stream: Vec::new(),
            routers: HashMap::new(),
            ws_handlers: HashMap::new(),
            before_hooks: Vec::new(),
            after_hooks: Vec::new(),
            before_hook_names: Vec::new(),
            after_hook_names: Vec::new(),
            fallback_handler: None,
            fallback_handler_name: None,
            static_dirs: Vec::new(),
            cors_config: None,
            request_logging: false,
            request_log_sample_n: 1,
            request_log_always_status: 0,
            request_log_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            fast_responses: HashMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert(
        &mut self,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        handler_name: String,
        gil: bool,
        is_async: bool,
        is_stream: bool,
    ) -> Result<(), String> {
        let idx = self.handlers.len();
        self.handlers.push(handler);
        self.handler_names.push(handler_name);
        self.requires_gil.push(gil);
        self.is_async.push(is_async);
        self.is_stream.push(is_stream);
        let router = self.routers.entry(method.to_uppercase()).or_default();
        router.insert(path, idx).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub(crate) fn lookup(
        &self,
        method: &str,
        path: &str,
    ) -> Option<(usize, Vec<(String, String)>)> {
        // `insert` stores methods uppercased; lookup must match — clients
        // sending `get` / `Get` previously silently missed routes even
        // though HTTP methods are case-insensitive per RFC 9110 §9.1.
        //
        // Fast path: hyper hands us canonical (already-uppercase) methods
        // for every standard verb, so the vast majority of requests can
        // reuse `method` without allocation. Only fall back to allocating
        // a normalized copy when we actually see lowercase bytes.
        let router = if method.bytes().any(|b| b.is_ascii_lowercase()) {
            self.routers.get(&method.to_ascii_uppercase())?
        } else {
            self.routers.get(method)?
        };
        let matched = router.at(path).ok()?;
        // Path params from matchit are raw URI segments — percent-encoded.
        // Every web framework's users expect `/user/{name}` + `/user/john%20doe`
        // to yield `name = "john doe"`, not `"john%20doe"`. Decode here so
        // Python handlers don't have to import urllib.parse for every route.
        // Key names are route-template identifiers and are always ASCII;
        // we only decode values.
        let params: Vec<(String, String)> = matched
            .params
            .iter()
            .map(|(k, v)| {
                let decoded = percent_encoding::percent_decode_str(v)
                    .decode_utf8()
                    .map(|c| c.into_owned())
                    .unwrap_or_else(|_| v.to_string());
                (k.to_string(), decoded)
            })
            .collect();
        Some((*matched.value, params))
    }
}

// SAFETY: RouteTable contains Py<PyAny> handles (handlers, hooks, ws_handlers,
// fallback) which PyO3 does NOT impl Send/Sync on, because Python object
// access is GIL-protected. We assert Send + Sync here because:
//
// 1. RouteTable is read-only after pyronova's startup: routes are pushed
//    in `register_route` during app construction, then frozen by
//    `FrozenRoutes` / `Arc<RouteTable>` for the runtime. There is no
//    interior mutability of the Py<PyAny> fields during request handling.
// 2. Every read site that calls into a Python object via these handles
//    holds the GIL (either main interp for gil=True routes, or the
//    sub-interp GIL for the rest). Crossing-thread access to Py<PyAny>
//    is therefore always GIL-protected at the use site.
// 3. The Arc reference-count operations on Arc<RouteTable> are themselves
//    atomic and don't touch Python.
//
// Per arc finding router-3: the previous `unsafe impl` lacked this
// rationale, leaving future readers to re-derive safety from scratch.
unsafe impl Send for RouteTable {}
unsafe impl Sync for RouteTable {}

/// Mutable during registration (before run).
pub(crate) type MutableRoutes = Arc<RwLock<RouteTable>>;

/// Frozen after startup — zero-lock reads on the hot path.
pub(crate) type FrozenRoutes = Arc<RouteTable>;
