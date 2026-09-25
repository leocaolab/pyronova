//! What one server run serves: the frozen route table plus the per-run settings applied
//! to every response (CORS, access log). Built once when `run()` starts; read-only after.

use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use hyper::StatusCode;

use crate::router::RouteTable;

pub(crate) struct Site {
    pub(crate) routes: RouteTable,
    pub(crate) config: SiteConfig,
}

pub(crate) type SharedSite = Arc<Site>;

pub(crate) struct SiteConfig {
    pub(crate) cors: Option<Cors>,
    pub(crate) access_log: AccessLog,
    /// Answer HttpArena's `benchmark.BenchmarkService/GetSum` gRPC method; off unless the
    /// app enables it.
    pub(crate) grpc_benchmark: bool,
    /// The header a client's request id arrives in (`app.enable_request_id()`); `None`
    /// means every request id is minted by the server.
    pub(crate) request_id_header: Option<HeaderName>,
}

/// CORS response headers, parsed once at configuration. Applied to every response, not
/// only the OPTIONS preflight: `Access-Control-Allow-Credentials` and
/// `Access-Control-Expose-Headers` must be on the actual response (W3C CORS).
#[derive(Clone, Debug)]
pub(crate) struct Cors {
    headers: Vec<(HeaderName, HeaderValue)>,
}

/// The CORS settings as configured, before parsing.
pub(crate) struct CorsSpec<'a> {
    pub(crate) origin: &'a str,
    pub(crate) methods: &'a str,
    pub(crate) headers: &'a str,
    pub(crate) expose_headers: Option<&'a str>,
    pub(crate) allow_credentials: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid CORS {setting} {value:?}: {source}")]
pub(crate) struct CorsError {
    setting: &'static str,
    value: String,
    source: hyper::header::InvalidHeaderValue,
}

impl Cors {
    pub(crate) fn parse(spec: &CorsSpec<'_>) -> Result<Self, CorsError> {
        let value = |setting: &'static str, value: &str| {
            HeaderValue::from_str(value).map_err(|source| CorsError {
                setting,
                value: value.to_string(),
                source,
            })
        };
        let mut headers = vec![
            (
                hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN,
                value("origin", spec.origin)?,
            ),
            (
                hyper::header::ACCESS_CONTROL_ALLOW_METHODS,
                value("methods", spec.methods)?,
            ),
            (
                hyper::header::ACCESS_CONTROL_ALLOW_HEADERS,
                value("headers", spec.headers)?,
            ),
        ];
        if spec.allow_credentials {
            headers.push((
                hyper::header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            ));
        }
        if let Some(expose) = spec.expose_headers {
            headers.push((
                hyper::header::ACCESS_CONTROL_EXPOSE_HEADERS,
                value("expose_headers", expose)?,
            ));
        }
        Ok(Cors { headers })
    }

    /// Sets (not appends) each header, so a handler's own value can't be duplicated.
    #[inline]
    pub(crate) fn apply(&self, map: &mut HeaderMap) {
        for (name, value) in &self.headers {
            map.insert(name.clone(), value.clone());
        }
    }
}

/// The `pyronova::access` log: whether it is on, and which responses it samples.
#[derive(Clone, Debug)]
pub(crate) struct AccessLog {
    pub(crate) enabled: bool,
    /// Log 1 in N responses; `1` logs every one.
    pub(crate) sample_n: NonZeroU64,
    /// Responses with a status at or above this always log, sampled or not.
    pub(crate) always_status: Option<StatusCode>,
    /// One sampling roll shared by every copy of the settings (every TPC thread), so
    /// `sample_n = 100` keeps 1% overall rather than 1% per thread.
    pub(crate) counter: Arc<AtomicU64>,
}

impl AccessLog {
    pub(crate) fn disabled() -> Self {
        AccessLog {
            enabled: false,
            sample_n: NonZeroU64::MIN,
            always_status: None,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Cheapest check first: off, then the status floor, then the 1-in-N roll (`1` never
    /// touches the shared counter).
    #[inline]
    pub(crate) fn samples(&self, status: StatusCode) -> bool {
        if !self.enabled {
            return false;
        }
        if self.always_status.is_some_and(|floor| status >= floor) {
            return true;
        }
        let n = self.sample_n.get();
        n == 1
            || self
                .counter
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(n)
    }
}
