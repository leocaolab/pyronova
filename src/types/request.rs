//! `Request` (what a handler receives) and `Headers`, its read-only view of the header
//! fields.

use std::borrow::Cow;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::OnceLock;

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};

use super::response::{header_name, header_value};
use crate::request_id::RequestId;

// ---------------------------------------------------------------------------
// PyronovaRequest
// ---------------------------------------------------------------------------

#[pyclass(frozen, name = "Request", module = "pyronova.engine")]
pub(crate) struct PyronovaRequest {
    /// Moved from hyper's request: a standard method is a plain value, no allocation.
    pub(crate) method: hyper::Method,
    /// Moved from hyper's request: path and query are views into the bytes hyper read.
    pub(crate) uri: hyper::Uri,
    /// Stored as Vec for small-count path params (typically 1-2).
    pub(crate) params: Vec<(String, String)>,
    /// The header fields as received, one entry per field line. Python reads them through
    /// the `Headers` view (`req.headers`), which converts only what it is asked for.
    pub(crate) headers: HeaderMap,
    /// Raw IP — zero allocation. `.to_string()` only when Python accesses it.
    pub(crate) client_ip_addr: IpAddr,
    /// The request's correlation id (`req.request_id`), written once by the pipeline.
    pub(crate) request_id: RequestId,
    /// Stored as Bytes (ref-counted, zero-copy from hyper).
    pub(crate) body_bytes: Bytes,
    /// `req.stream`: the streamed body of a `stream=True` route, handed out once.
    pub(crate) body_stream: crate::body::StreamSlot,
    /// Cached parse of the query string. `form_urlencoded::parse + collect`
    /// costs ~100-200 ns for a two-param query and building a fresh
    /// Python dict on top is another ~500 ns. OnceLock matches the
    /// `query_all_cache` pattern: parse once, return ref on subsequent
    /// accesses.
    pub(crate) query_cache: OnceLock<HashMap<String, String>>,
    /// Cached multi-value parse — same rationale as `query_cache`.
    pub(crate) query_all_cache: OnceLock<HashMap<String, Vec<String>>>,
}

impl PyronovaRequest {
    fn query_str(&self) -> &str {
        self.uri.query().unwrap_or("")
    }
}

#[pymethods]
impl PyronovaRequest {
    /// Python-side constructor: `Request(method, path, params, query,
    /// body_bytes, headers, client_ip)`, through the same constructor the server uses
    /// (`request_head`).
    ///
    /// `params` / `headers` arrive as `dict[str, str]`, `body_bytes` as `bytes`, and
    /// `client_ip` as a string. A method, path or query that is not valid in a request
    /// line, a header that is not a valid field, or a `client_ip` that is not an IP
    /// address is a `ValueError` naming it. The request gets a fresh `request_id`.
    #[new]
    fn py_new(
        method: &str,
        path: &str,
        params: HashMap<String, String>,
        query: &str,
        body_bytes: Vec<u8>,
        headers: HashMap<String, String>,
        client_ip: &str,
    ) -> PyResult<Self> {
        let headers = headers
            .iter()
            .map(|(name, value)| Ok((header_name(name)?, header_value(name, value)?)))
            .collect::<PyResult<HeaderMap>>()?;
        let client_ip_addr = client_ip.parse::<IpAddr>().map_err(|e| {
            PyValueError::new_err(format!("client_ip {client_ip:?} is not an IP address: {e}"))
        })?;
        let target = if query.is_empty() {
            path.to_string()
        } else {
            format!("{path}?{query}")
        };
        let (mut parts, ()) = hyper::Request::builder()
            .method(method)
            .uri(&target)
            .body(())
            .map_err(|e| {
                PyValueError::new_err(format!(
                    "method {method:?} with target {target:?} is not a valid request line: {e}"
                ))
            })?
            .into_parts();
        parts.headers = headers;
        let head = crate::request_head::RequestHead {
            parts,
            params: params.into_iter().collect(),
            client_ip: client_ip_addr,
            request_id: RequestId::mint(),
        };
        Ok(PyronovaRequest::new(
            head,
            crate::request_head::Body::Buffered(Bytes::from(body_bytes)),
        ))
    }

    #[getter]
    fn method(&self) -> &str {
        self.method.as_str()
    }

    #[getter]
    fn path(&self) -> &str {
        self.uri.path()
    }

    /// The raw query string, without the `?`; `""` when the request has none.
    #[getter]
    fn query(&self) -> &str {
        self.query_str()
    }

    /// The `host[:port]` the request was sent to: the target's authority when it has one
    /// (HTTP/2 `:authority`, an absolute-form target — RFC 9112 §3.2.2 has it win over
    /// `Host`), else the `Host` header; `None` with neither.
    #[getter]
    fn authority(&self) -> Option<Cow<'_, str>> {
        match self.uri.authority() {
            Some(authority) => Some(Cow::Borrowed(authority.as_str())),
            None => self.headers.get(hyper::header::HOST).map(field_text),
        }
    }

    /// A new `dict` of the path params on every access.
    #[getter]
    fn params<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, value) in &self.params {
            dict.set_item(name, value)?;
        }
        Ok(dict)
    }

    /// The request's headers as a read-only, case-insensitive mapping (`Headers`). No
    /// copy: the view reads this request's fields.
    #[getter]
    fn headers(slf: &Bound<'_, Self>) -> PyResult<Py<PyronovaHeaders>> {
        Py::new(
            slf.py(),
            PyronovaHeaders {
                req: slf.clone().unbind(),
            },
        )
    }

    /// Lazy: heap-allocates the IP string only when Python reads `req.client_ip`.
    #[getter]
    fn client_ip(&self) -> String {
        self.client_ip_addr.to_string()
    }

    /// The request's correlation id: the one a 5xx body reports and the error log line
    /// carries. The client's own id when the app enabled request ids and the request sent
    /// one, else minted by the server.
    #[getter]
    fn request_id(&self) -> String {
        self.request_id.to_string()
    }

    #[getter]
    fn body(&self) -> &[u8] {
        &self.body_bytes
    }

    /// Streaming body iterator. Only populated on routes registered with
    /// `stream=True`; `None` otherwise, so code that doesn't opt in never sees a stream
    /// object.
    ///
    /// **Taken on first access.** A second `req.stream` in the same request raises
    /// `RuntimeError`: one stream reads the body, so a hook that took it would leave the
    /// handler none.
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
        let rx = self
            .body_stream
            .take()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        rx.map(|rx| Py::new(py, crate::python::body_stream::PyronovaBodyStream::new(rx)))
            .transpose()
    }

    /// Zero-copy: validates UTF-8 on the Bytes slice, creates Python str directly.
    fn text<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyString>> {
        let s = std::str::from_utf8(&self.body_bytes)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(pyo3::types::PyString::new(py, s))
    }

    /// The body decoded as JSON, with the codec responses use (see `crate::json`): an
    /// integer of any size stays an exact `int`, a float reads back as the value sent. A
    /// body that isn't JSON raises `ValueError`.
    fn json<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        crate::json::loads(py, &self.body_bytes)
    }

    /// Look up a single query parameter by name without building the full map.
    ///
    /// Uses the already-computed cache when available. On a cold cache this
    /// scans the raw query string once — cheaper than building a HashMap for
    /// a single lookup. First-wins on duplicate keys (same policy as
    /// `query_params`).
    fn query_param(&self, key: &str) -> Option<Cow<'_, str>> {
        if let Some(cache) = self.query_cache.get() {
            return cache.get(key).map(|v| Cow::Borrowed(v.as_str()));
        }
        form_urlencoded::parse(self.query_str().as_bytes())
            .find(|(k, _)| k == key)
            .map(|(_, v)| Cow::Owned(v.into_owned()))
    }

    /// A new `dict` on every access, built from the parse cached on the first.
    #[getter(query_params)]
    fn py_query_params<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, value) in self.query_params() {
            dict.set_item(key, value)?;
        }
        Ok(dict)
    }

    /// Full query-parameter access preserving duplicate keys: a new
    /// `dict[str, list[str]]` on every call.
    fn query_params_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, values) in self.query_params_all_map() {
            dict.set_item(key, PyList::new(py, values)?)?;
        }
        Ok(dict)
    }

    // ── Buffer protocol: zero-copy `memoryview(req)` into the body ──────
    //
    // Lets big-body handlers view the Rust-owned body `Bytes` with no copy:
    // `memoryview(req)` / `np.frombuffer(req, dtype=...)` point straight at
    // our buffer, unlike `req.body` which materializes a `PyBytes` (a
    // memcpy). Read-only, 1-D, itemsize 1, format "B". The view takes a
    // strong ref to `req` (`view.obj`), so the body outlives the view — our
    // instance (and its `Bytes`) can only drop after the memoryview
    // releases that ref. This is the on-ramp for large / columnar payloads
    // (file uploads, AI-agent bodies, and a future Arrow zero-copy body).
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: std::ffi::c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(pyo3::exceptions::PyBufferError::new_err("NULL view"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            // Zero the view so a caller that ignores the error and reads
            // buf/len sees nulls, not uninitialized stack data.
            std::ptr::write_bytes(view, 0, 1);
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "pyronova Request body is readonly",
            ));
        }

        // `frozen` ⇒ `Bound::get` yields `&Self` without a borrow guard.
        // `body_bytes` is heap-owned by this instance at a stable address,
        // kept alive by the strong ref we stash in `view.obj` below, so the
        // raw pointer stays valid for the view's whole lifetime.
        let data: &[u8] = &slf.get().body_bytes;
        (*view).buf = data.as_ptr() as *mut std::ffi::c_void;
        (*view).len = data.len() as ffi::Py_ssize_t;
        (*view).itemsize = 1;
        (*view).readonly = 1;
        (*view).ndim = 1;
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            c"B".as_ptr() as *mut std::ffi::c_char
        } else {
            std::ptr::null_mut()
        };
        // For a 1-D contiguous buffer, shape/strides (when requested) point
        // at the view's own `len`/`itemsize` fields — the CPython
        // "stored by value in the Py_buffer" convention.
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            &mut (*view).len
        } else {
            std::ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            &mut (*view).itemsize
        } else {
            std::ptr::null_mut()
        };
        (*view).suboffsets = std::ptr::null_mut();
        (*view).internal = std::ptr::null_mut();
        // Hand the view a strong ref to keep `req` (and its body) alive.
        (*view).obj = slf.into_ptr();
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {
        // Nothing to free: the buffer points into `self.body_bytes`, owned
        // by this instance. CPython's PyBuffer_Release DECREFs `view.obj`
        // (the ref we handed out) on its own; we must not double-free it.
    }
}

impl PyronovaRequest {
    /// The query string's parameters, parsed on the first access. On duplicate keys
    /// (`?a=1&a=2`) the FIRST value wins: WAFs, rate limiters and proxies inspect the first
    /// occurrence, so taking the last would let `?role=user&role=admin` reach the handler
    /// as the value the WAF never approved (HTTP parameter pollution). All values:
    /// `query_params_all()`.
    pub(crate) fn query_params(&self) -> &HashMap<String, String> {
        self.query_cache.get_or_init(|| {
            let mut map: HashMap<String, String> = HashMap::new();
            for (k, v) in form_urlencoded::parse(self.query_str().as_bytes()) {
                map.entry(k.into_owned()).or_insert_with(|| v.into_owned());
            }
            map
        })
    }

    fn query_params_all_map(&self) -> &HashMap<String, Vec<String>> {
        self.query_all_cache.get_or_init(|| {
            let mut map: HashMap<String, Vec<String>> = HashMap::new();
            for (k, v) in form_urlencoded::parse(self.query_str().as_bytes()) {
                map.entry(k.into_owned()).or_default().push(v.into_owned());
            }
            map
        })
    }
}

// ---------------------------------------------------------------------------
// Headers: the request's header fields, as Python reads them
// ---------------------------------------------------------------------------

/// `req.headers`: a read-only mapping over the request's header fields, names
/// case-insensitive. A name sent on several field lines reads as one value, joined per
/// RFC 9110 §5.3 with `", "` — except `cookie`, joined with `"; "` (RFC 9113 §8.2.3, which
/// lets an HTTP/2 client send one `cookie` field per crumb). `get_all(name)` gives each
/// field line as sent.
#[pyclass(frozen, name = "Headers", module = "pyronova.engine", mapping)]
pub(crate) struct PyronovaHeaders {
    /// The request whose fields this reads; `frozen`, so read without a borrow guard.
    req: Py<PyronovaRequest>,
}

impl PyronovaHeaders {
    fn map<'a>(&'a self, py: Python<'a>) -> &'a HeaderMap {
        &self.req.bind(py).get().headers
    }
}

#[pymethods]
impl PyronovaHeaders {
    fn __getitem__(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        joined_value(self.map(py), name).ok_or_else(|| PyKeyError::new_err(name.to_string()))
    }

    #[pyo3(signature = (name, default=None))]
    fn get(&self, py: Python<'_>, name: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        match joined_value(self.map(py), name) {
            Some(value) => PyString::new(py, &value).into_any().unbind(),
            None => default.unwrap_or_else(|| py.None()),
        }
    }

    /// Every field line named `name`, in the order received; `[]` if there is none.
    fn get_all(&self, py: Python<'_>, name: &str) -> Vec<String> {
        self.map(py)
            .get_all(name)
            .iter()
            .map(|v| field_text(v).into_owned())
            .collect()
    }

    fn __contains__(&self, py: Python<'_>, name: &Bound<'_, PyAny>) -> bool {
        match name.cast::<PyString>().map(|s| s.to_str()) {
            Ok(Ok(name)) => self.map(py).contains_key(name),
            _ => false,
        }
    }

    fn __len__(&self, py: Python<'_>) -> usize {
        self.map(py).keys_len()
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(self.keys(py)?.try_iter()?.into_any())
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, self.map(py).keys().map(HeaderName::as_str))
    }

    fn values(&self, py: Python<'_>) -> Vec<String> {
        joined_fields(self.map(py))
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }

    fn items(&self, py: Python<'_>) -> Vec<(String, String)> {
        joined_fields(self.map(py))
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        format!("Headers({:?})", joined_fields(self.map(py)))
    }
}

/// A field value as text: UTF-8 when it is, else ISO-8859-1 (RFC 9110 §5.5 `obs-text`),
/// which maps every byte to a character, so no byte is lost or replaced.
fn field_text(value: &HeaderValue) -> Cow<'_, str> {
    match std::str::from_utf8(value.as_bytes()) {
        Ok(s) => Cow::Borrowed(s),
        Err(_) => Cow::Owned(value.as_bytes().iter().map(|&b| char::from(b)).collect()),
    }
}

/// The separator that combines several field lines of `name` into one value.
fn field_separator(name: &str) -> &'static str {
    if name.eq_ignore_ascii_case(hyper::header::COOKIE.as_str()) {
        "; "
    } else {
        ", "
    }
}

/// `name`'s field lines combined into one value, or `None` if there is none.
fn joined_value(map: &HeaderMap, name: &str) -> Option<String> {
    let separator = field_separator(name);
    let mut lines = map.get_all(name).iter().map(field_text);
    let first = lines.next()?.into_owned();
    Some(lines.fold(first, |mut joined, line| {
        joined.push_str(separator);
        joined.push_str(&line);
        joined
    }))
}

/// Every field name with its combined value, in the order the names first arrived.
pub(crate) fn joined_fields(map: &HeaderMap) -> Vec<(String, String)> {
    map.keys()
        .filter_map(|name| {
            let value = joined_value(map, name.as_str())?;
            Some((name.as_str().to_string(), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_headers_basic() {
        let mut hm = hyper::HeaderMap::new();
        hm.insert("content-type", "application/json".parse().unwrap());
        hm.insert("x-custom", "hello".parse().unwrap());
        let h: HashMap<String, String> = joined_fields(&hm).into_iter().collect();
        assert_eq!(h["content-type"], "application/json");
        assert_eq!(h["x-custom"], "hello");
    }

    #[test]
    fn extract_headers_empty() {
        let hm = hyper::HeaderMap::new();
        let h: HashMap<String, String> = joined_fields(&hm).into_iter().collect();
        assert!(h.is_empty());
    }

    #[test]
    fn extract_headers_multi_value() {
        let mut hm = hyper::HeaderMap::new();
        hm.append("accept", "text/html".parse().unwrap());
        hm.append("accept", "application/json".parse().unwrap());
        let h: HashMap<String, String> = joined_fields(&hm).into_iter().collect();
        assert!(h["accept"].contains("text/html"));
        assert!(h["accept"].contains("application/json"));
        assert!(h["accept"].contains(", "));
    }

    #[test]
    fn repeated_cookie_fields_join_with_semicolon() {
        let mut hm = HeaderMap::new();
        hm.append("cookie", "a=1".parse().unwrap());
        hm.append("cookie", "b=2".parse().unwrap());
        assert_eq!(joined_value(&hm, "Cookie").as_deref(), Some("a=1; b=2"));
    }

    #[test]
    fn non_utf8_field_reads_as_latin1() {
        let mut hm = HeaderMap::new();
        hm.append("x-name", HeaderValue::from_bytes(b"caf\xe9").unwrap());
        assert_eq!(joined_value(&hm, "x-name").as_deref(), Some("café"));
    }

    fn request_with_query(query: &str) -> PyronovaRequest {
        PyronovaRequest::py_new(
            "GET",
            "/search",
            HashMap::new(),
            query,
            Vec::new(),
            HashMap::new(),
            "127.0.0.1",
        )
        .unwrap()
    }

    #[test]
    fn query_params_parsing() {
        let req = request_with_query("q=hello+world&page=2&lang=en");
        let qp = req.query_params();
        assert_eq!(qp["q"], "hello world");
        assert_eq!(qp["page"], "2");
        assert_eq!(qp["lang"], "en");
    }

    #[test]
    fn query_params_empty() {
        let req = request_with_query("");
        assert!(req.query_params().is_empty());
    }

    #[test]
    fn query_params_percent_encoded() {
        let req = request_with_query("name=%E4%B8%AD%E6%96%87");
        assert_eq!(req.query_params()["name"], "中文");
    }

    // Note: text() and json() require Python GIL, tested via Python tests.
}
