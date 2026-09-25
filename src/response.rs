//! A handler's return value → [`ResponseData`] → HTTP response. One mapping for every
//! interpreter: the main one (GIL mode, `gil=True` routes, the TPC bridge) and the
//! sub-interpreter workers (pool, TPC inline, the async engine).

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{HeaderValue, CONTENT_TYPE, SERVER};
use hyper::{Response, StatusCode};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyList, PyString};

use crate::types::{PyronovaResponse, ResponseData, ResponseHeaders};

pub(crate) const SERVER_HEADER: &str = concat!("Pyronova/", env!("CARGO_PKG_VERSION"));

const JSON: HeaderValue = HeaderValue::from_static("application/json");
const TEXT: HeaderValue = HeaderValue::from_static("text/plain; charset=utf-8");
const OCTET_STREAM: HeaderValue = HeaderValue::from_static("application/octet-stream");

// ---------------------------------------------------------------------------
// isojson-backed serializer (required dep: always available)
// ---------------------------------------------------------------------------

// Cached `dumps(obj)` wrapper compiled once per interpreter via PyModule::from_code.
//
// Per interpreter: `PyOnceLock` is per-interpreter with the PyO3 fork, so each sub-interpreter
// caches its own function. A process-global `std::sync::OnceLock<Py<_>>` here handed the first
// interpreter's function (and module) to every other one.
static JSON_HELPER: pyo3::sync::PyOnceLock<pyo3::Py<pyo3::PyAny>> = pyo3::sync::PyOnceLock::new();

fn get_or_init_json_dumps(py: Python<'_>) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> {
    JSON_HELPER
        .get_or_try_init(py, || {
            // PyModule::from_code gives correct module-level scoping: _isojson and _default are
            // in the module's __dict__, so the dumps closure sees them.
            let module = pyo3::types::PyModule::from_code(
                py,
                c"import isojson as _isojson\n\ndef _default(obj):\n    if isinstance(obj, (set, frozenset)):\n        return list(obj)\n    raise TypeError(f'not serializable: {type(obj).__name__}')\n\ndef dumps(obj):\n    return _isojson.dumps(obj, default=_default)\n",
                c"pyronova_json",
                c"pyronova_json",
            )?;
            Ok::<_, pyo3::PyErr>(module.getattr("dumps")?.unbind())
        })
        .map(|f| f.bind(py).clone())
}

fn json_dumps(py: Python<'_>, obj: &pyo3::Bound<'_, pyo3::PyAny>) -> Result<Bytes, String> {
    let dumps = get_or_init_json_dumps(py).map_err(|e| format!("json init: {e}"))?;
    let result = dumps
        .call1((obj,))
        .map_err(|e| format!("json error: {e}"))?;
    let bytes = result
        .cast::<PyBytes>()
        .map_err(|e| format!("json error: {e}"))?;
    Ok(Bytes::copy_from_slice(bytes.as_bytes()))
}

// ---------------------------------------------------------------------------
// Handler return value → ResponseData
// ---------------------------------------------------------------------------

/// What a handler (or a hook) returned, as a response. The type comes from the value,
/// never from the text:
///
/// - `dict` / `list` → JSON;
/// - `str` → `text/plain`;
/// - `bytes` / `bytearray` → `application/octet-stream`;
/// - `None` → an empty 200;
/// - a `Response` → its status and headers, its body mapped by the same rules unless it
///   names its `content_type`;
/// - anything else → `str(value)` as text.
///
/// A value that can't be turned into a body (a `str` that isn't valid Unicode, a failing
/// `__str__`, an unserializable dict) is an error, never an empty or lossy body.
pub(crate) fn extract_response_data(
    py: Python<'_>,
    obj: Bound<'_, PyAny>,
) -> Result<ResponseData, String> {
    // The common returns skip the `Response` type lookup.
    if is_json_value(&obj) || obj.cast::<PyString>().is_ok() {
        return plain_response(py, &obj);
    }
    match obj.cast::<PyronovaResponse>() {
        Ok(resp) => from_response(py, resp.get()),
        Err(_) => plain_response(py, &obj),
    }
}

/// A value that isn't a `Response`: a 200 with the value as its body.
fn plain_response(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<ResponseData, String> {
    let (body, content_type) = body_of(py, obj)?;
    Ok(ResponseData {
        body,
        content_type,
        status: 200,
        headers: ResponseHeaders::new(),
    })
}

fn from_response(py: Python<'_>, resp: &PyronovaResponse) -> Result<ResponseData, String> {
    let (body, derived_type) = body_of(py, resp.body.bind(py))?;
    Ok(ResponseData {
        body,
        content_type: resp.content_type.clone().unwrap_or(derived_type),
        status: resp.status_code,
        headers: resp.headers.clone(),
    })
}

fn is_json_value(obj: &Bound<'_, PyAny>) -> bool {
    obj.cast::<PyDict>().is_ok() || obj.cast::<PyList>().is_ok()
}

/// A value as a body, and the type that body has.
fn body_of(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<(Bytes, HeaderValue), String> {
    if is_json_value(obj) {
        return Ok((json_dumps(py, obj)?, JSON));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok((text_bytes(s)?, TEXT));
    }
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok((Bytes::copy_from_slice(b.as_bytes()), OCTET_STREAM));
    }
    if let Ok(b) = obj.cast::<PyByteArray>() {
        return Ok((Bytes::from(b.to_vec()), OCTET_STREAM));
    }
    if obj.is_none() {
        return Ok((Bytes::new(), TEXT));
    }
    let text = obj
        .str()
        .map_err(|e| format!("str() of the returned {} failed: {e}", type_name(obj)))?;
    Ok((text_bytes(&text)?, TEXT))
}

/// A `str`'s UTF-8 bytes; a lone surrogate is an error, not a replacement character.
fn text_bytes(s: &Bound<'_, PyString>) -> Result<Bytes, String> {
    let text = s
        .to_str()
        .map_err(|e| format!("response text is not valid Unicode: {e}"))?;
    Ok(Bytes::copy_from_slice(text.as_bytes()))
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    match obj.get_type().name() {
        Ok(name) => name.to_string(),
        Err(e) => format!("<type name unavailable: {e}>"),
    }
}

impl ResponseData {
    /// This response as a `Response`, for an `after_request` hook: a body that is UTF-8
    /// text is a `str`, any other a `bytes`.
    pub(crate) fn to_py<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyronovaResponse>> {
        let body = match std::str::from_utf8(&self.body) {
            Ok(text) => PyString::new(py, text).into_any(),
            Err(_) => PyBytes::new(py, &self.body).into_any(),
        };
        Bound::new(
            py,
            PyronovaResponse {
                body: body.unbind(),
                status_code: self.status,
                content_type: Some(self.content_type.clone()),
                headers: self.headers.clone(),
            },
        )
    }
}

// ---------------------------------------------------------------------------
// HTTP response builders
// ---------------------------------------------------------------------------

/// The HTTP response for a handler's result; an error becomes a logged 500. The headers
/// were validated when the `Response` was made, so building can't fail: the handler's
/// header map becomes the response's, and `content-type` / `server` are added only if the
/// handler didn't set them.
pub(crate) fn build_response(result: Result<ResponseData, String>) -> Response<Full<Bytes>> {
    let data = match result {
        Ok(data) => data,
        Err(e) => {
            tracing::error!(target: "pyronova::handler", error = %e, "handler failed; responding 500");
            return error_response(&e);
        }
    };
    let mut headers = data.headers.into_map();
    headers.entry(CONTENT_TYPE).or_insert(data.content_type);
    headers
        .entry(SERVER)
        .or_insert(HeaderValue::from_static(SERVER_HEADER));
    let mut resp = Response::new(Full::new(data.body));
    *resp.status_mut() = status_or_500(data.status);
    *resp.headers_mut() = headers;
    resp
}

/// A handler's status code, or 500 (logged) if it isn't an HTTP status.
pub(crate) fn status_or_500(code: u16) -> StatusCode {
    StatusCode::from_u16(code).unwrap_or_else(|_| {
        tracing::error!(
            target: "pyronova::handler",
            status = code,
            "handler returned invalid HTTP status {code}; responding 500"
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

pub(crate) fn error_response(msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header("content-type", "application/json")
        .header("server", SERVER_HEADER)
        .body(Full::new(Bytes::from(error_json_body(msg))))
        .unwrap()
}

/// Serialize a `{"error": msg}` JSON body via serde_json. Hand-rolling the
/// escape (only handling `"`) would leak backslashes, control chars, and
/// newlines into the payload — the classic "minimal escape hides a JSON
/// injection" bug. `serde_json::to_vec` is the only safe source.
fn error_json_body(msg: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "error": msg }))
        .unwrap_or_else(|_| br#"{"error":"serialization failed"}"#.to_vec())
}

#[inline]
pub(crate) fn overloaded_response(msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header("content-type", "application/json")
        .header("server", SERVER_HEADER)
        .header("retry-after", "1")
        .body(Full::new(Bytes::from(error_json_body(msg))))
        .unwrap()
}

/// 503 for a request whose workers are gone (the server is shutting down).
#[inline]
pub(crate) fn unavailable_response(msg: &str) -> Response<Full<Bytes>> {
    json_error(StatusCode::SERVICE_UNAVAILABLE, msg)
}

/// 400 for a request whose body could not be read.
#[inline]
pub(crate) fn bad_request_response(msg: &str) -> Response<Full<Bytes>> {
    json_error(StatusCode::BAD_REQUEST, msg)
}

/// 408 for a request body that did not arrive within the request budget.
#[inline]
pub(crate) fn request_timeout_response() -> Response<Full<Bytes>> {
    json_error(StatusCode::REQUEST_TIMEOUT, "request body timeout")
}

fn json_error(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    let mut resp = Response::new(Full::new(Bytes::from(error_json_body(msg))));
    *resp.status_mut() = status;
    let headers = resp.headers_mut();
    headers.insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    headers.insert(
        hyper::header::SERVER,
        hyper::header::HeaderValue::from_static(SERVER_HEADER),
    );
    resp
}

#[inline]
pub(crate) fn payload_too_large_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::PAYLOAD_TOO_LARGE)
        .header("content-type", "application/json")
        .header("server", SERVER_HEADER)
        .body(Full::new(Bytes::from_static(
            b"{\"error\":\"payload too large\"}",
        )))
        .unwrap()
}

#[inline]
pub(crate) fn gateway_timeout_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::GATEWAY_TIMEOUT)
        .header("content-type", "application/json")
        .header("server", SERVER_HEADER)
        .body(Full::new(Bytes::from_static(
            b"{\"error\":\"request timeout\"}",
        )))
        .unwrap()
}

#[inline]
pub(crate) fn not_found_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("content-type", "application/json")
        .header("server", SERVER_HEADER)
        .body(Full::new(Bytes::from_static(b"{\"error\":\"not found\"}")))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn body_bytes(resp: Response<Full<Bytes>>) -> Vec<u8> {
        use http_body_util::BodyExt;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let collected = resp.into_body().collect().await.unwrap();
            collected.to_bytes().to_vec()
        })
    }

    #[test]
    fn not_found_status_and_body() {
        let resp = not_found_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(resp.headers()["content-type"], "application/json");
        assert!(resp.headers()["server"]
            .to_str()
            .unwrap()
            .starts_with("Pyronova/"));
        assert_eq!(body_bytes(resp), b"{\"error\":\"not found\"}");
    }

    #[test]
    fn error_response_500() {
        let resp = error_response("something broke");
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp.headers()["content-type"], "application/json");
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("something broke"));
    }

    #[test]
    fn error_response_escapes_quotes() {
        let resp = error_response(r#"bad "input""#);
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains(r#"bad \"input\""#));
    }

    #[test]
    fn error_response_escapes_control_chars_and_backslashes() {
        // Previously the hand-rolled escape only handled `"`; a backslash
        // or a newline in `msg` produced invalid JSON. serde_json fixes it.
        let resp = error_response("back\\slash\nnewline\ttab");
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("must parse");
        assert_eq!(parsed["error"], "back\\slash\nnewline\ttab");
    }

    #[test]
    fn overloaded_503_with_retry_after() {
        let resp = overloaded_response("too busy");
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers()["retry-after"], "1");
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("too busy"));
    }

    #[test]
    fn payload_too_large_413() {
        let resp = payload_too_large_response();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("payload too large"));
    }

    #[test]
    fn gateway_timeout_504() {
        let resp = gateway_timeout_response();
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("request timeout"));
    }

    #[test]
    fn build_response_ok() {
        let data = ResponseData {
            body: Bytes::from("hello"),
            content_type: HeaderValue::from_static("text/plain"),
            status: 200,
            headers: ResponseHeaders::new(),
        };
        let resp = build_response(Ok(data));
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "text/plain");
    }

    #[test]
    fn build_response_custom_status_and_headers() {
        let mut headers = ResponseHeaders::new();
        headers
            .as_map_mut()
            .insert("x-custom", HeaderValue::from_static("value"));
        let data = ResponseData {
            body: Bytes::from("created"),
            content_type: JSON,
            status: 201,
            headers,
        };
        let resp = build_response(Ok(data));
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(resp.headers()["x-custom"], "value");
    }

    #[test]
    fn build_response_error_falls_back_to_500() {
        let resp = build_response(Err("oops".to_string()));
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn handler_content_type_and_server_replace_the_defaults() {
        let mut headers = ResponseHeaders::new();
        let map = headers.as_map_mut();
        map.insert(CONTENT_TYPE, HeaderValue::from_static("text/csv"));
        map.insert(SERVER, HeaderValue::from_static("mine"));
        let resp = build_response(Ok(ResponseData {
            body: Bytes::from("a,b"),
            content_type: TEXT,
            status: 200,
            headers,
        }));
        let all = |name| {
            resp.headers()
                .get_all(name)
                .iter()
                .map(|v| v.to_str().unwrap().to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(all(CONTENT_TYPE), ["text/csv"]);
        assert_eq!(all(SERVER), ["mine"]);
    }

    #[test]
    fn repeated_header_lines_stay_separate() {
        let mut headers = ResponseHeaders::new();
        let map = headers.as_map_mut();
        map.append("set-cookie", HeaderValue::from_static("a=1"));
        map.append("set-cookie", HeaderValue::from_static("b=2"));
        let resp = build_response(Ok(ResponseData {
            body: Bytes::new(),
            content_type: TEXT,
            status: 200,
            headers,
        }));
        let cookies: Vec<_> = resp.headers().get_all("set-cookie").iter().collect();
        assert_eq!(cookies, ["a=1", "b=2"]);
    }
}
