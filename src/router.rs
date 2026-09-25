use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE, SERVER};
use hyper::{Response, StatusCode};
use matchit::Router;
use parking_lot::RwLock;
use pyo3::prelude::*;

/// A response entirely built at registration time — no Python call, no serialization on
/// the request path. Served for exact-match (method, path) lookups: `/health`,
/// `/robots.txt`, maintenance pages, any constant-body endpoint.
///
/// Parsed once, when it is registered: an invalid status or header is a registration
/// error, so serving it can't fail.
#[derive(Clone, Debug)]
pub(crate) struct FastResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FastResponseError {
    #[error("invalid status code {0}: an HTTP status is 100..=999")]
    Status(u16),
    #[error("invalid header name {name:?}: {source}")]
    HeaderName {
        name: String,
        source: hyper::header::InvalidHeaderName,
    },
    #[error("invalid value for header {name:?}: {source}")]
    HeaderValue {
        name: String,
        source: hyper::header::InvalidHeaderValue,
    },
}

impl FastResponse {
    pub(crate) fn parse(
        body: Bytes,
        content_type: &str,
        status: u16,
        headers: &HashMap<String, String>,
    ) -> Result<Self, FastResponseError> {
        let status = StatusCode::from_u16(status).map_err(|_| FastResponseError::Status(status))?;
        let mut map = HeaderMap::with_capacity(headers.len() + 2);
        map.insert(CONTENT_TYPE, header_value("content-type", content_type)?);
        map.insert(
            SERVER,
            HeaderValue::from_static(crate::response::SERVER_HEADER),
        );
        for (name, value) in headers {
            let header = HeaderName::from_bytes(name.as_bytes()).map_err(|source| {
                FastResponseError::HeaderName {
                    name: name.clone(),
                    source,
                }
            })?;
            map.append(header, header_value(name, value)?);
        }
        Ok(FastResponse {
            status,
            headers: map,
            body,
        })
    }

    #[inline]
    pub(crate) fn to_response(&self) -> Response<Full<Bytes>> {
        let mut resp = Response::new(Full::new(self.body.clone()));
        *resp.status_mut() = self.status;
        *resp.headers_mut() = self.headers.clone();
        resp
    }
}

fn header_value(name: &str, value: &str) -> Result<HeaderValue, FastResponseError> {
    HeaderValue::from_str(value).map_err(|source| FastResponseError::HeaderValue {
        name: name.to_string(),
        source,
    })
}

/// A route the table rejected (a duplicate, a conflicting or malformed path).
#[derive(Debug, thiserror::Error)]
#[error("{method} {path}: {source}")]
pub(crate) struct RouteError {
    method: String,
    path: String,
    source: matchit::InsertError,
}

/// A registered route's index in its [`RouteTable`]. Only the table hands them out, so an
/// id always names one of its routes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RouteId(usize);

impl RouteId {
    /// The position in registration order, which is the same in every interpreter (the
    /// worker tables are checked against main's; Layer 2, C3).
    pub(crate) fn index(self) -> usize {
        self.0
    }
}

/// Which handler a request runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    Route(RouteId),
    /// The `app.fallback()` handler, for a request no route matched.
    Fallback,
}

/// Where a route's handler runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Dispatch {
    /// A sub-interpreter worker of the handler's kind.
    Worker(HandlerKind),
    /// `gil=True`: the main interpreter.
    Main(RequestBody),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HandlerKind {
    /// `def`
    Sync,
    /// `async def`
    Async,
}

/// How a main-interpreter handler receives the request body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestBody {
    /// Collected in full before the handler runs.
    Buffered,
    /// `stream=True`: fed to `req.stream` chunk by chunk while the handler runs.
    Streamed,
}

/// What a request resolved to: the handler, and where it runs. A worker only ever runs a
/// registered route; the fallback always runs on main.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Call {
    Worker(RouteId, HandlerKind),
    Main(Target, RequestBody),
}

pub(crate) struct Route {
    pub(crate) handler: Py<PyAny>,
    pub(crate) name: String,
    pub(crate) key: RouteKey,
    pub(crate) dispatch: Dispatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RouteKey {
    /// Uppercased.
    pub(crate) method: String,
    pub(crate) path: String,
}

/// Path parameters of a matched route, percent-decoded.
pub(crate) type Params = Vec<(String, String)>;

pub(crate) struct RouteTable {
    routes: Vec<Route>,
    routers: HashMap<String, Router<RouteId>>,
    fallback: Option<Py<PyAny>>,
    pub(crate) ws_handlers: HashMap<String, Py<PyAny>>,
    pub(crate) before_hooks: Vec<Py<PyAny>>,
    pub(crate) after_hooks: Vec<Py<PyAny>>,
    pub(crate) static_dirs: Vec<crate::static_fs::StaticMount>,
    /// Exact-match (METHOD, path) → pre-built response, served before any Python dispatch.
    /// Nested by method then path so the lookup takes `&str` through `String: Borrow<str>`
    /// — no allocation on the hot path.
    pub(crate) fast_responses: HashMap<String, HashMap<String, FastResponse>>,
    /// Route, before-hook and after-hook counts when `Pyronova.run()` began, i.e. the part
    /// of the table the script registered. Everything after it (`/mcp`, logging hooks,
    /// startup-hook routes) exists only on main. Set once, on main (Layer 2, FR-2).
    pub(crate) sealed: Option<Sealed>,
}

/// See [`RouteTable::sealed`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Sealed {
    pub(crate) routes: usize,
    pub(crate) before_hooks: usize,
    pub(crate) after_hooks: usize,
}

/// The routes and hook counts a worker's script must register, as plain values (Layer 2,
/// C3). Main computes it from its sealed table before creating workers; a worker compares
/// its own whole table against it. Plain values, so a worker's init, which runs with the
/// worker's thread state current, never touches main's `Py<T>` handlers (M4 review N3).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RouteSignature {
    /// Per route index: `(METHOD, path, gil)`.
    pub(crate) routes: Vec<(String, String, bool)>,
    pub(crate) before_hooks: usize,
    pub(crate) after_hooks: usize,
}

impl RouteSignature {
    /// `table` up to its seal, or the whole table if it has none (a worker's table: a
    /// worker is never sealed).
    pub(crate) fn of(table: &RouteTable) -> Self {
        let sealed = table.sealed.unwrap_or(Sealed {
            routes: table.routes.len(),
            before_hooks: table.before_hooks.len(),
            after_hooks: table.after_hooks.len(),
        });
        RouteSignature {
            routes: table.routes[..sealed.routes]
                .iter()
                .map(|r| {
                    let gil = matches!(r.dispatch, Dispatch::Main(_));
                    (r.key.method.clone(), r.key.path.clone(), gil)
                })
                .collect(),
            before_hooks: sealed.before_hooks,
            after_hooks: sealed.after_hooks,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.routes.is_empty() && self.before_hooks == 0 && self.after_hooks == 0
    }

    /// Both signatures, one route per line, with the first differing index marked.
    pub(crate) fn describe_mismatch(expected: &Self, got: &Self) -> String {
        let first_diff = expected
            .routes
            .iter()
            .zip(&got.routes)
            .position(|(a, b)| a != b)
            .unwrap_or_else(|| expected.routes.len().min(got.routes.len()));
        let list = |s: &Self| {
            let mut out = String::new();
            for (i, (m, p, gil)) in s.routes.iter().enumerate() {
                let mark = if i == first_diff {
                    "  <-- first difference"
                } else {
                    ""
                };
                let gil = if *gil { " gil=True" } else { "" };
                out.push_str(&format!("    [{i}] {m} {p}{gil}{mark}\n"));
            }
            out.push_str(&format!(
                "    before_request hooks: {}, after_request hooks: {}\n",
                s.before_hooks, s.after_hooks
            ));
            out
        };
        format!(
            "the main interpreter registered:\n{}this worker's script registered:\n{}",
            list(expected),
            list(got)
        )
    }
}

impl RouteTable {
    pub(crate) fn new() -> Self {
        RouteTable {
            routes: Vec::new(),
            routers: HashMap::new(),
            fallback: None,
            ws_handlers: HashMap::new(),
            before_hooks: Vec::new(),
            after_hooks: Vec::new(),
            static_dirs: Vec::new(),
            fast_responses: HashMap::new(),
            sealed: None,
        }
    }

    /// A copy holding its own references to every Python object.
    pub(crate) fn clone_ref(&self, py: Python<'_>) -> Self {
        let clone_all = |v: &[Py<PyAny>]| v.iter().map(|h| h.clone_ref(py)).collect();
        RouteTable {
            routes: self
                .routes
                .iter()
                .map(|r| Route {
                    handler: r.handler.clone_ref(py),
                    name: r.name.clone(),
                    key: r.key.clone(),
                    dispatch: r.dispatch,
                })
                .collect(),
            routers: self.routers.clone(),
            fallback: self.fallback.as_ref().map(|h| h.clone_ref(py)),
            ws_handlers: self
                .ws_handlers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                .collect(),
            before_hooks: clone_all(&self.before_hooks),
            after_hooks: clone_all(&self.after_hooks),
            static_dirs: self.static_dirs.clone(),
            fast_responses: self.fast_responses.clone(),
            sealed: self.sealed,
        }
    }

    pub(crate) fn insert(
        &mut self,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        name: String,
        dispatch: Dispatch,
    ) -> Result<(), RouteError> {
        // The fallible router insert goes first, so a rejected route (e.g. a duplicate)
        // leaves the table untouched.
        let id = RouteId(self.routes.len());
        let method = method.to_uppercase();
        let router = self.routers.entry(method.clone()).or_default();
        router.insert(path, id).map_err(|source| RouteError {
            method: method.clone(),
            path: path.to_string(),
            source,
        })?;
        self.routes.push(Route {
            handler,
            name,
            key: RouteKey {
                method,
                path: path.to_string(),
            },
            dispatch,
        });
        Ok(())
    }

    pub(crate) fn set_fallback(&mut self, handler: Py<PyAny>) {
        self.fallback = Some(handler);
    }

    pub(crate) fn routes(&self) -> &[Route] {
        &self.routes
    }

    pub(crate) fn route(&self, id: RouteId) -> &Route {
        &self.routes[id.0]
    }

    /// Whether any request can run on the main interpreter: a `gil=True` route or the
    /// fallback.
    pub(crate) fn uses_main(&self) -> bool {
        self.fallback.is_some()
            || self
                .routes
                .iter()
                .any(|r| matches!(r.dispatch, Dispatch::Main(_)))
    }

    /// The handler `target` names. `Target::Fallback` only comes from
    /// [`Self::fallback_call`] on a table with a fallback, and tables are frozen while
    /// serving.
    pub(crate) fn handler(&self, target: Target) -> &Py<PyAny> {
        match target {
            Target::Route(id) => &self.route(id).handler,
            Target::Fallback => self
                .fallback
                .as_ref()
                .expect("Target::Fallback resolved on a table without a fallback"),
        }
    }

    /// The route `(method, path)` matches, where it runs, and its path parameters.
    pub(crate) fn resolve(&self, method: &str, path: &str) -> Option<(Call, Params)> {
        let (id, params) = self.lookup(method, path)?;
        let call = match self.route(id).dispatch {
            Dispatch::Worker(kind) => Call::Worker(id, kind),
            Dispatch::Main(body) => Call::Main(Target::Route(id), body),
        };
        Some((call, params))
    }

    /// The fallback handler's call, for a request no route (and no static file) matched.
    pub(crate) fn fallback_call(&self) -> Option<Call> {
        self.fallback
            .as_ref()
            .map(|_| Call::Main(Target::Fallback, RequestBody::Buffered))
    }

    /// The pre-built response registered for exactly `(method, path)`.
    #[inline]
    pub(crate) fn fast_response(&self, method: &str, path: &str) -> Option<&FastResponse> {
        if self.fast_responses.is_empty() {
            return None;
        }
        self.fast_responses.get(method)?.get(path)
    }

    fn lookup(&self, method: &str, path: &str) -> Option<(RouteId, Params)> {
        // `insert` stores methods uppercased and HTTP methods are case-insensitive (RFC
        // 9110 §9.1). hyper hands us canonical uppercase methods for every standard verb,
        // so only a lowercase method pays for a normalized copy.
        let router = if method.bytes().any(|b| b.is_ascii_lowercase()) {
            self.routers.get(&method.to_ascii_uppercase())?
        } else {
            self.routers.get(method)?
        };
        let matched = router.at(path).ok()?;
        // Path params from matchit are raw URI segments. Decode every value uniformly, with
        // invalid UTF-8 mapped to U+FFFD, so a literal `%` (from `%25`) can't be confused
        // with a failed decode. Keys are route-template identifiers, always ASCII.
        let params: Params = matched
            .params
            .iter()
            .map(|(k, v)| {
                let decoded = percent_encoding::percent_decode_str(v)
                    .decode_utf8_lossy()
                    .into_owned();
                (k.to_string(), decoded)
            })
            .collect();
        Some((*matched.value, params))
    }
}

/// Per route (registration order): whether it runs on main, and whether it is `async def`
/// on a worker. The shape the worker split and the startup banners read.
pub(crate) struct RouteShape {
    pub(crate) gil: Vec<bool>,
    pub(crate) is_async: Vec<bool>,
    pub(crate) streamed: usize,
}

impl RouteShape {
    pub(crate) fn of(table: &RouteTable) -> Self {
        let dispatches = || table.routes().iter().map(|r| r.dispatch);
        RouteShape {
            gil: dispatches()
                .map(|d| matches!(d, Dispatch::Main(_)))
                .collect(),
            is_async: dispatches()
                .map(|d| matches!(d, Dispatch::Worker(HandlerKind::Async)))
                .collect(),
            streamed: dispatches()
                .filter(|d| matches!(d, Dispatch::Main(RequestBody::Streamed)))
                .count(),
        }
    }

    pub(crate) fn gil_count(&self) -> usize {
        self.gil.iter().filter(|&&g| g).count()
    }

    pub(crate) fn async_count(&self) -> usize {
        self.is_async.iter().filter(|&&a| a).count()
    }

    /// Every route runs inline on a TPC worker: what the benches serve.
    #[cfg(feature = "bench")]
    pub(crate) fn all_inline_sync(&self) -> bool {
        self.gil_count() == 0 && self.async_count() == 0
    }
}

/// Mutable during registration (before run).
pub(crate) type MutableRoutes = Arc<RwLock<RouteTable>>;
