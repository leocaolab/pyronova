//! `Response` (what a handler returns), its header lines, and `ResponseData` (what the
//! HTTP layer sends).

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

/// A response's header lines: a real multimap, one entry per line sent, names and values
/// already validated. Built once, from the handler's `headers=`; the HTTP response takes
/// the map as is. `content-type` and `server` here replace the defaults.
#[derive(Clone, Debug, Default)]
pub(crate) struct ResponseHeaders(HeaderMap);

impl ResponseHeaders {
    pub(crate) fn new() -> Self {
        Self(HeaderMap::new())
    }

    /// `{name: str | list[str]}` as header lines, a list giving one line per item. A
    /// non-`str` name or value is a `TypeError`, an invalid one a `ValueError`; each names
    /// the header.
    pub(crate) fn from_py(dict: &Bound<'_, PyDict>) -> PyResult<Self> {
        let mut map = HeaderMap::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let key = key.cast::<PyString>().map_err(|_| {
                PyTypeError::new_err(format!(
                    "response header name must be str, got {}",
                    type_name(&key)
                ))
            })?;
            let key = key.to_str()?;
            let name = header_name(key)?;
            for item in header_items(key, &value)? {
                map.append(name.clone(), header_value(key, str_item(key, &item)?)?);
            }
        }
        Ok(Self(map))
    }

    /// `{name: str}`, or `{name: [str, ...]}` for a name with several lines.
    pub(crate) fn to_py<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for name in self.0.keys() {
            let lines = self
                .0
                .get_all(name)
                .iter()
                .map(header_text)
                .collect::<PyResult<Vec<_>>>()?;
            match lines.as_slice() {
                [one] => dict.set_item(name.as_str(), *one)?,
                many => dict.set_item(name.as_str(), many)?,
            }
        }
        Ok(dict)
    }

    /// The first line named `name`, as text.
    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).and_then(|v| v.to_str().ok())
    }

    pub(crate) fn contains_key(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    pub(crate) fn as_map_mut(&mut self) -> &mut HeaderMap {
        &mut self.0
    }

    pub(crate) fn into_map(self) -> HeaderMap {
        self.0
    }
}

/// The lines of header `key`: the items of a list or tuple, else the value itself.
fn header_items<'py>(key: &str, value: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if let Ok(list) = value.cast::<PyList>() {
        return Ok(list.iter().collect());
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        return Ok(tuple.iter().collect());
    }
    str_item(key, value)?;
    Ok(vec![value.clone()])
}

fn str_item<'a>(key: &str, item: &'a Bound<'_, PyAny>) -> PyResult<&'a str> {
    let text = item.cast::<PyString>().map_err(|_| {
        PyTypeError::new_err(format!(
            "response header {key:?}: value must be str or a list of str, got {}",
            type_name(item)
        ))
    })?;
    text.to_str()
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    match obj.get_type().name() {
        Ok(name) => name.to_string(),
        Err(e) => format!("<type name unavailable: {e}>"),
    }
}

/// `name` as a header name; an invalid one is a `ValueError` naming it.
pub(crate) fn header_name(name: &str) -> PyResult<HeaderName> {
    HeaderName::from_bytes(name.as_bytes())
        .map_err(|e| PyValueError::new_err(format!("invalid header name {name:?}: {e}")))
}

/// `value` as the value of header `name`; an invalid one (CR, LF, NUL, other controls) is
/// a `ValueError` naming the header.
pub(crate) fn header_value(name: &str, value: &str) -> PyResult<HeaderValue> {
    HeaderValue::from_str(value).map_err(|e| {
        PyValueError::new_err(format!("header {name:?}: invalid value {value:?}: {e}"))
    })
}

/// A header value Rust built from a `&str`, as text again.
pub(crate) fn header_text(value: &HeaderValue) -> PyResult<&str> {
    std::str::from_utf8(value.as_bytes())
        .map_err(|e| PyValueError::new_err(format!("header value is not UTF-8: {e}")))
}

// ---------------------------------------------------------------------------
// PyronovaResponse
// ---------------------------------------------------------------------------

#[pyclass(frozen, name = "Response", module = "pyronova.engine")]
pub(crate) struct PyronovaResponse {
    #[pyo3(get)]
    pub(crate) body: Py<PyAny>,
    #[pyo3(get)]
    pub(crate) status_code: u16,
    /// `content_type=`, validated; `None` = derived from the body.
    pub(crate) content_type: Option<HeaderValue>,
    pub(crate) headers: ResponseHeaders,
}

#[pymethods]
impl PyronovaResponse {
    /// `headers` maps a name to a `str`, or to a list of `str` for a header sent on
    /// several lines (e.g. `Set-Cookie`). A `Content-Type` or `Server` in it replaces the
    /// default one.
    #[new]
    #[pyo3(signature = (body, status_code=200, content_type=None, headers=None))]
    fn new(
        body: Py<PyAny>,
        status_code: u16,
        content_type: Option<&str>,
        headers: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        Ok(PyronovaResponse {
            body,
            status_code,
            content_type: content_type
                .map(|ct| header_value("content_type", ct))
                .transpose()?,
            headers: headers
                .map(ResponseHeaders::from_py)
                .transpose()?
                .unwrap_or_default(),
        })
    }

    #[getter]
    fn content_type(&self) -> PyResult<Option<&str>> {
        self.content_type.as_ref().map(header_text).transpose()
    }

    /// `{name: str}`, or `{name: [str, ...]}` for a header on several lines; names are
    /// lower-case, as they go out on the wire.
    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.headers.to_py(py)
    }
}

// ---------------------------------------------------------------------------
// ResponseData (Rust-internal, not exposed to Python)
// ---------------------------------------------------------------------------

/// A handler's result as the HTTP layer sends it.
pub(crate) struct ResponseData {
    pub(crate) body: Bytes,
    /// The body's type: from `content_type=`, else from what the handler returned. A
    /// `content-type` in `headers` replaces it.
    pub(crate) content_type: HeaderValue,
    pub(crate) status: hyper::StatusCode,
    pub(crate) headers: ResponseHeaders,
}
