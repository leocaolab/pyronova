use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// PyronovaRequest
// ---------------------------------------------------------------------------

/// Two ways to construct headers:
/// - GIL path: store raw `hyper::HeaderMap`, convert lazily on first Python access.
/// - Sub-interp path: pre-converted `HashMap` (needed for C-FFI bridge).
pub(crate) enum LazyHeaders {
    /// Raw hyper HeaderMap — O(1) construction, deferred conversion.
    Raw(hyper::HeaderMap),
    /// Pre-converted (sub-interp path where FFI needs HashMap anyway).
    Converted(HashMap<String, String>),
}

#[pyclass(frozen, skip_from_py_object, name = "Request")]
pub(crate) struct PyronovaRequest {
    /// Arc<str> — shared with access log, zero-cost clone.
    pub(crate) method: Arc<str>,
    /// Arc<str> — shared with access log, zero-cost clone.
    pub(crate) path: Arc<str>,
    /// Stored as Vec for small-count path params (typically 1-2).
    pub(crate) params: Vec<(String, String)>,
    #[pyo3(get)]
    pub(crate) query: String,
    /// Lazy headers: raw HeaderMap or pre-converted HashMap.
    pub(crate) headers_source: LazyHeaders,
    /// Cached conversion — computed once on first Python access.
    pub(crate) headers_cache: OnceLock<HashMap<String, String>>,
    /// Raw IP — zero allocation. `.to_string()` only when Python accesses it.
    pub(crate) client_ip_addr: IpAddr,
    /// Stored as Bytes (ref-counted, zero-copy from hyper).
    pub(crate) body_bytes: Bytes,
    /// For streaming routes (`stream=True`), this holds the feeder channel's
    /// receiver end, shared across all clones of this request so the
    /// handler (which receives a clone from `call_handler_with_hooks`) can
    /// take ownership. The first `.stream` access wins; subsequent calls
    /// return None. Stored as raw receiver (not `Py<PyronovaBodyStream>`) to
    /// keep `drop_in_place::<PyronovaRequest>` free of `_Py_Dealloc` — that
    /// would break `cargo test` linking for the pure-Rust unit tests.
    pub(crate) body_stream_rx: Arc<
        std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<crate::python::body_stream::ChunkMsg>>>,
    >,
    /// Cached parse of the query string. `form_urlencoded::parse + collect`
    /// costs ~100-200 ns for a two-param query and building a fresh
    /// Python dict on top is another ~500 ns. OnceLock matches the
    /// headers_cache pattern: parse once, return ref on subsequent
    /// accesses.
    pub(crate) query_cache: OnceLock<HashMap<String, String>>,
    /// Cached multi-value parse — same rationale as `query_cache`.
    pub(crate) query_all_cache: OnceLock<HashMap<String, Vec<String>>>,
}

/// Manual Clone: OnceLock doesn't impl Clone, so we reset the cache on clone.
/// Cloned requests lazily recompute headers if accessed.
impl Clone for PyronovaRequest {
    fn clone(&self) -> Self {
        // body_stream_rx is `Arc<Mutex<Option<Receiver>>>`, so clones share the
        // *same* receiver slot rather than getting an independent one. This is
        // deliberate and required: the handler runs on a clone (handed down by
        // `call_handler_with_hooks`), so it must be able to take the receiver
        // out of the shared slot. `.stream` is take-once — the first access
        // (whichever clone) wins and later accesses see None. If two clones
        // race for `.stream` only one gets the receiver; that's a caller bug,
        // not a framework one, since streaming is expected on a single copy.
        Self {
            method: self.method.clone(),
            path: self.path.clone(),
            params: self.params.clone(),
            query: self.query.clone(),
            headers_source: self.headers_source.clone(),
            headers_cache: OnceLock::new(),
            client_ip_addr: self.client_ip_addr,
            body_bytes: self.body_bytes.clone(),
            body_stream_rx: Arc::clone(&self.body_stream_rx),
            query_cache: OnceLock::new(),
            query_all_cache: OnceLock::new(),
        }
    }
}

impl Clone for LazyHeaders {
    fn clone(&self) -> Self {
        match self {
            Self::Raw(hm) => Self::Raw(hm.clone()),
            Self::Converted(m) => Self::Converted(m.clone()),
        }
    }
}

impl PyronovaRequest {
    /// Resolve headers to HashMap (lazy for Raw, immediate for Converted).
    pub(crate) fn resolved_headers(&self) -> &HashMap<String, String> {
        self.headers_cache
            .get_or_init(|| match &self.headers_source {
                LazyHeaders::Raw(hm) => extract_headers(hm),
                LazyHeaders::Converted(m) => m.clone(),
            })
    }
}

#[pymethods]
impl PyronovaRequest {
    #[getter]
    fn method(&self) -> &str {
        &self.method
    }

    #[getter]
    fn path(&self) -> &str {
        &self.path
    }

    /// Converts Vec<(String, String)> → Python dict on access.
    #[getter]
    fn params(&self) -> HashMap<String, String> {
        self.params.iter().cloned().collect()
    }

    /// Lazy headers: converts raw HeaderMap → dict only on first access.
    #[getter]
    fn headers(&self) -> HashMap<String, String> {
        self.resolved_headers().clone()
    }

    /// Lazy: heap-allocates the IP string only when Python reads `req.client_ip`.
    #[getter]
    fn client_ip(&self) -> String {
        self.client_ip_addr.to_string()
    }

    #[getter]
    fn body(&self) -> &[u8] {
        &self.body_bytes
    }

    /// Streaming body iterator. Only populated on routes registered with
    /// `stream=True`; returns `None` otherwise so code that doesn't opt-in
    /// never sees a stream object.
    ///
    /// **Consumed on first access.** The receiver is taken out of the
    /// shared slot, so a second call to `req.stream` in the same request
    /// lifecycle returns `None`. Before/after hooks that clone the request
    /// and read `.stream` will steal chunks from the handler — that's the
    /// caller's bug, not ours.
    ///
    /// Usage in a `@app.post(..., gil=True, stream=True)` handler:
    ///
    /// ```python
    /// for chunk in req.stream:
    ///     process(chunk)
    /// ```
    #[getter]
    fn stream(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<Py<crate::python::body_stream::PyronovaBodyStream>>> {
        let rx = { self.body_stream_rx.lock().unwrap().take() };
        match rx {
            Some(rx) => Ok(Some(Py::new(
                py,
                crate::python::body_stream::PyronovaBodyStream::new(rx),
            )?)),
            None => Ok(None),
        }
    }

    /// Zero-copy: validates UTF-8 on the Bytes slice, creates Python str directly.
    fn text<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyString>> {
        let s = std::str::from_utf8(&self.body_bytes)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(pyo3::types::PyString::new(py, s))
    }

    fn json<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let parsed: serde_json::Value = serde_json::from_slice(&self.body_bytes).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("JSON parse error: {e}"))
        })?;
        pythonize::pythonize(py, &parsed)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("pythonize error: {e}")))
    }

    /// Look up a single query parameter by name without cloning the full map.
    ///
    /// Uses the already-computed cache when available. On a cold cache this
    /// scans the raw query string once — cheaper than building a HashMap for
    /// a single lookup. First-wins on duplicate keys (same policy as
    /// `query_params`).
    fn query_param(&self, key: &str) -> Option<String> {
        if let Some(cache) = self.query_cache.get() {
            return cache.get(key).cloned();
        }
        form_urlencoded::parse(self.query.as_bytes())
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.into_owned())
    }

    #[getter]
    fn query_params(&self) -> HashMap<String, String> {
        // Parse once, reuse on subsequent accesses. On duplicate keys
        // (`?a=1&a=2`) we keep the FIRST value, not the last. Rationale:
        // HTTP parameter pollution (HPP). WAFs, rate limiters, and
        // reverse proxies almost always inspect the first occurrence of
        // a duplicated param; a web framework that then silently picks
        // the last one opens a classic security-policy bypass
        // (`?role=user&role=admin` reaches business logic as admin
        // while the WAF approved it as user). First-wins lines up
        // with those upstream components.
        //
        // If a handler legitimately needs all values, use
        // `query_params_all()` which returns Dict[str, List[str]].
        self.query_cache
            .get_or_init(|| {
                let mut map: HashMap<String, String> = HashMap::new();
                for (k, v) in form_urlencoded::parse(self.query.as_bytes()) {
                    map.entry(k.into_owned()).or_insert_with(|| v.into_owned());
                }
                map
            })
            .clone()
    }

    /// Full query-parameter access preserving duplicate keys.
    /// Returns Dict[str, List[str]] in insertion order.
    fn query_params_all(&self) -> HashMap<String, Vec<String>> {
        self.query_all_cache
            .get_or_init(|| {
                let mut map: HashMap<String, Vec<String>> = HashMap::new();
                for (k, v) in form_urlencoded::parse(self.query.as_bytes()) {
                    map.entry(k.into_owned()).or_default().push(v.into_owned());
                }
                map
            })
            .clone()
    }
}

// ---------------------------------------------------------------------------
// PyronovaResponse
// ---------------------------------------------------------------------------

#[pyclass(frozen, name = "Response")]
pub(crate) struct PyronovaResponse {
    #[pyo3(get)]
    pub(crate) body: Py<PyAny>,
    #[pyo3(get)]
    pub(crate) status_code: u16,
    #[pyo3(get)]
    pub(crate) content_type: Option<String>,
    #[pyo3(get)]
    pub(crate) headers: HashMap<String, String>,
}

#[pymethods]
impl PyronovaResponse {
    #[new]
    #[pyo3(signature = (body, status_code=200, content_type=None, headers=None))]
    fn new(
        py: Python<'_>,
        body: Py<PyAny>,
        status_code: u16,
        content_type: Option<String>,
        headers: Option<Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Self> {
        let headers_map = if let Some(dict) = headers {
            let mut map = HashMap::with_capacity(dict.len());
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                if let Ok(s) = v.extract::<String>() {
                    map.insert(key, s);
                } else if let Ok(list) = v.extract::<Vec<String>>() {
                    // Multiple values (e.g. multiple Set-Cookie headers):
                    // encode as NUL-separated string. build_response splits on '\0'
                    // to emit separate HTTP header lines.
                    if !list.is_empty() {
                        map.insert(key, list.join("\0"));
                    }
                }
                // Values of unexpected types are silently dropped.
            }
            map
        } else {
            let _ = py;
            HashMap::new()
        };
        Ok(PyronovaResponse {
            body,
            status_code,
            content_type,
            headers: headers_map,
        })
    }
}

// ---------------------------------------------------------------------------
// ResponseData (Rust-internal, not exposed to Python)
// ---------------------------------------------------------------------------

pub(crate) struct ResponseData {
    pub(crate) body: Bytes,
    pub(crate) content_type: String,
    pub(crate) status: u16,
    pub(crate) headers: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// extract_headers
// ---------------------------------------------------------------------------

pub fn extract_headers(header_map: &hyper::HeaderMap) -> HashMap<String, String> {
    let mut headers = HashMap::with_capacity(header_map.len());
    for (name, value) in header_map.iter() {
        let key = name.as_str().to_string();
        // Fast path: valid UTF-8 (99.99% of headers) avoids Cow→String deep copy.
        let val = match std::str::from_utf8(value.as_bytes()) {
            Ok(s) => s.to_string(),
            Err(_) => String::from_utf8_lossy(value.as_bytes()).into_owned(),
        };
        headers
            .entry(key)
            .and_modify(|existing: &mut String| {
                existing.push_str(", ");
                existing.push_str(&val);
            })
            .or_insert(val);
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_headers_basic() {
        let mut hm = hyper::HeaderMap::new();
        hm.insert("content-type", "application/json".parse().unwrap());
        hm.insert("x-custom", "hello".parse().unwrap());
        let h = extract_headers(&hm);
        assert_eq!(h["content-type"], "application/json");
        assert_eq!(h["x-custom"], "hello");
    }

    #[test]
    fn extract_headers_empty() {
        let hm = hyper::HeaderMap::new();
        let h = extract_headers(&hm);
        assert!(h.is_empty());
    }

    #[test]
    fn extract_headers_multi_value() {
        let mut hm = hyper::HeaderMap::new();
        hm.append("accept", "text/html".parse().unwrap());
        hm.append("accept", "application/json".parse().unwrap());
        let h = extract_headers(&hm);
        assert!(h["accept"].contains("text/html"));
        assert!(h["accept"].contains("application/json"));
        assert!(h["accept"].contains(", "));
    }

    #[test]
    fn query_params_parsing() {
        let req = PyronovaRequest {
            method: Arc::from("GET"),
            path: Arc::from("/search"),
            params: Vec::new(),
            query: "q=hello+world&page=2&lang=en".to_string(),
            headers_source: LazyHeaders::Raw(hyper::HeaderMap::new()),
            headers_cache: std::sync::OnceLock::new(),
            client_ip_addr: IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            body_bytes: Bytes::new(),
            body_stream_rx: Arc::new(std::sync::Mutex::new(None)),
            query_cache: OnceLock::new(),
            query_all_cache: OnceLock::new(),
        };
        let qp = req.query_params();
        assert_eq!(qp["q"], "hello world");
        assert_eq!(qp["page"], "2");
        assert_eq!(qp["lang"], "en");
    }

    #[test]
    fn query_params_empty() {
        let req = PyronovaRequest {
            method: Arc::from("GET"),
            path: Arc::from("/"),
            params: Vec::new(),
            query: "".to_string(),
            headers_source: LazyHeaders::Raw(hyper::HeaderMap::new()),
            headers_cache: std::sync::OnceLock::new(),
            client_ip_addr: IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            body_bytes: Bytes::new(),
            body_stream_rx: Arc::new(std::sync::Mutex::new(None)),
            query_cache: OnceLock::new(),
            query_all_cache: OnceLock::new(),
        };
        assert!(req.query_params().is_empty());
    }

    #[test]
    fn query_params_percent_encoded() {
        let req = PyronovaRequest {
            method: Arc::from("GET"),
            path: Arc::from("/"),
            params: Vec::new(),
            query: "name=%E4%B8%AD%E6%96%87".to_string(),
            headers_source: LazyHeaders::Raw(hyper::HeaderMap::new()),
            headers_cache: std::sync::OnceLock::new(),
            client_ip_addr: IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            body_bytes: Bytes::new(),
            body_stream_rx: Arc::new(std::sync::Mutex::new(None)),
            query_cache: OnceLock::new(),
            query_all_cache: OnceLock::new(),
        };
        assert_eq!(req.query_params()["name"], "中文");
    }

    // Note: text() and json() require Python GIL, tested via Python tests.
}
