//! A handler's return value → [`ResponseData`] → HTTP response. One mapping for every
//! interpreter: the main one (GIL mode, `gil=True` routes, the TPC bridge) and the
//! sub-interpreter workers (pool, TPC inline, the async engine).

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{HeaderValue, CONTENT_TYPE, SERVER};
use hyper::{Response, StatusCode};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyList, PyString};

use crate::error::{PyException, ResponseError};
use crate::request_id::RequestId;
use crate::types::{PyronovaResponse, ResponseData, ResponseHeaders};

pub(crate) const SERVER_HEADER: &str = concat!("Pyronova/", env!("CARGO_PKG_VERSION"));

const JSON: HeaderValue = HeaderValue::from_static("application/json");
const TEXT: HeaderValue = HeaderValue::from_static("text/plain; charset=utf-8");
const OCTET_STREAM: HeaderValue = HeaderValue::from_static("application/octet-stream");
/// `Retry-After` on an overload 503, in seconds: overload is transient.
const OVERLOAD_RETRY_AFTER_SECS: HeaderValue = HeaderValue::from_static("1");

// ---------------------------------------------------------------------------
// isojson-backed serializer (required dep: always available)
// ---------------------------------------------------------------------------

// The `dumps(obj)` wrapper, compiled once per interpreter: `PyOnceLock` is per-interpreter
// with the PyO3 fork, and one interpreter's function must never be called from another.
static JSON_HELPER: pyo3::sync::PyOnceLock<pyo3::Py<pyo3::PyAny>> = pyo3::sync::PyOnceLock::new();

fn get_or_init_json_dumps(py: Python<'_>) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> {
    JSON_HELPER
        .get_or_try_init(py, || {
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

/// Loads the JSON serializer. isojson is a hard dependency: every interpreter loads it when
/// the server starts, so a missing one stops the start instead of failing every `dict`.
pub(crate) fn require_json(py: Python<'_>) -> PyResult<()> {
    get_or_init_json_dumps(py).map(|_| ())
}

fn json_dumps(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<Bytes, ResponseError> {
    let json = |e: PyErr| ResponseError::Json(PyException::capture(py, &e));
    let dumps = get_or_init_json_dumps(py).map_err(json)?;
    let result = dumps.call1((obj,)).map_err(json)?;
    let bytes = result.cast::<PyBytes>().map_err(|e| json(e.into()))?;
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
/// A value that can't be turned into a response (a `str` that isn't valid Unicode, a
/// failing `__str__`, an unserializable dict, a status that isn't an HTTP status) is an
/// error, never an empty or lossy body.
pub(crate) fn extract_response_data(
    py: Python<'_>,
    obj: Bound<'_, PyAny>,
) -> Result<ResponseData, ResponseError> {
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
fn plain_response(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<ResponseData, ResponseError> {
    let (body, content_type) = body_of(py, obj)?;
    Ok(ResponseData {
        body,
        content_type,
        status: StatusCode::OK,
        headers: ResponseHeaders::new(),
    })
}

fn from_response(py: Python<'_>, resp: &PyronovaResponse) -> Result<ResponseData, ResponseError> {
    let status = http_status(resp.status_code)?;
    let (body, derived_type) = body_of(py, resp.body.bind(py))?;
    Ok(ResponseData {
        body,
        content_type: resp.content_type.clone().unwrap_or(derived_type),
        status,
        headers: resp.headers.clone(),
    })
}

/// A handler's status code as an HTTP status.
pub(crate) fn http_status(code: u16) -> Result<StatusCode, ResponseError> {
    StatusCode::from_u16(code).map_err(|_| ResponseError::Status(code))
}

fn is_json_value(obj: &Bound<'_, PyAny>) -> bool {
    obj.cast::<PyDict>().is_ok() || obj.cast::<PyList>().is_ok()
}

/// A value as a body, and the type that body has.
fn body_of(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<(Bytes, HeaderValue), ResponseError> {
    if is_json_value(obj) {
        return Ok((json_dumps(py, obj)?, JSON));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok((text_bytes(py, s)?, TEXT));
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
    let text = obj.str().map_err(|e| ResponseError::Str {
        type_name: type_name(obj),
        exception: PyException::capture(py, &e),
    })?;
    Ok((text_bytes(py, &text)?, TEXT))
}

/// A `str`'s UTF-8 bytes; a lone surrogate is an error, not a replacement character.
fn text_bytes(py: Python<'_>, s: &Bound<'_, PyString>) -> Result<Bytes, ResponseError> {
    let text = s
        .to_str()
        .map_err(|e| ResponseError::Text(PyException::capture(py, &e)))?;
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
    pub(crate) fn to_py<'py>(
        &self,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyronovaResponse>, ResponseError> {
        let body = match std::str::from_utf8(&self.body) {
            Ok(text) => PyString::new(py, text).into_any(),
            Err(_) => PyBytes::new(py, &self.body).into_any(),
        };
        Bound::new(
            py,
            PyronovaResponse {
                body: body.unbind(),
                status_code: self.status.as_u16(),
                content_type: Some(self.content_type.clone()),
                headers: self.headers.clone(),
            },
        )
        .map_err(|e| ResponseError::ToPy(PyException::capture(py, &e)))
    }
}

// ---------------------------------------------------------------------------
// HTTP response builders
// ---------------------------------------------------------------------------

/// The HTTP response for a handler's response. The headers were validated when the
/// `Response` was made, so building can't fail: the handler's header map becomes the
/// response's, and `content-type` / `server` are added only if the handler didn't set
/// them.
pub(crate) fn build_response(data: ResponseData) -> Response<Full<Bytes>> {
    let mut headers = data.headers.into_map();
    headers.entry(CONTENT_TYPE).or_insert(data.content_type);
    headers
        .entry(SERVER)
        .or_insert(HeaderValue::from_static(SERVER_HEADER));
    let mut resp = Response::new(Full::new(data.body));
    *resp.status_mut() = data.status;
    *resp.headers_mut() = headers;
    resp
}

/// 500 with `msg` (the generic text) and the request id: the client can quote the id to
/// an operator, who finds the real error on the log line carrying it.
pub(crate) fn error_response(msg: &str, request_id: &RequestId) -> Response<Full<Bytes>> {
    json_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        server_error_body(msg, request_id),
    )
}

/// `{"error": msg}` for a 4xx; serde_json escapes `msg`, which may carry client input.
fn error_json_body(msg: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "error": msg }))
        .unwrap_or_else(|_| br#"{"error":"serialization failed"}"#.to_vec())
}

/// `{"error": msg, "request_id": id}` for a 5xx.
fn server_error_body(msg: &str, request_id: &RequestId) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "error": msg,
        "request_id": request_id.to_string(),
    }))
    .unwrap_or_else(|_| br#"{"error":"serialization failed"}"#.to_vec())
}

/// 503 for a request no queue slot or permit was free for, with `retry-after`.
#[inline]
pub(crate) fn overloaded_response(msg: &str, request_id: &RequestId) -> Response<Full<Bytes>> {
    let mut resp = json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        server_error_body(msg, request_id),
    );
    resp.headers_mut()
        .insert(hyper::header::RETRY_AFTER, OVERLOAD_RETRY_AFTER_SECS);
    resp
}

/// 503 for a request whose workers are gone (the server is shutting down).
#[inline]
pub(crate) fn unavailable_response(msg: &str, request_id: &RequestId) -> Response<Full<Bytes>> {
    json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        server_error_body(msg, request_id),
    )
}

/// 400 for a request whose body could not be read.
#[inline]
pub(crate) fn bad_request_response(msg: &str) -> Response<Full<Bytes>> {
    json_error(StatusCode::BAD_REQUEST, error_json_body(msg))
}

/// 408 for a request body that did not arrive within the request budget.
#[inline]
pub(crate) fn request_timeout_response() -> Response<Full<Bytes>> {
    json_error(
        StatusCode::REQUEST_TIMEOUT,
        error_json_body("request body timeout"),
    )
}

fn json_error(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    let mut resp = Response::new(Full::new(body.into()));
    *resp.status_mut() = status;
    let headers = resp.headers_mut();
    headers.insert(CONTENT_TYPE, JSON);
    headers.insert(SERVER, HeaderValue::from_static(SERVER_HEADER));
    resp
}

#[inline]
pub(crate) fn payload_too_large_response() -> Response<Full<Bytes>> {
    json_error(
        StatusCode::PAYLOAD_TOO_LARGE,
        Bytes::from_static(b"{\"error\":\"payload too large\"}"),
    )
}

/// 504 for a handler that did not answer within the request budget.
#[inline]
pub(crate) fn gateway_timeout_response(request_id: &RequestId) -> Response<Full<Bytes>> {
    json_error(
        StatusCode::GATEWAY_TIMEOUT,
        server_error_body("request timeout", request_id),
    )
}

#[inline]
pub(crate) fn not_found_response() -> Response<Full<Bytes>> {
    json_error(
        StatusCode::NOT_FOUND,
        Bytes::from_static(b"{\"error\":\"not found\"}"),
    )
}

/// 405 for a path that has routes, none for the request's method; `allow` lists the
/// methods it has.
pub(crate) fn method_not_allowed_response(allow: HeaderValue) -> Response<Full<Bytes>> {
    let mut resp = json_error(
        StatusCode::METHOD_NOT_ALLOWED,
        Bytes::from_static(b"{\"error\":\"method not allowed\"}"),
    );
    resp.headers_mut().insert(hyper::header::ALLOW, allow);
    resp
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
        let resp = error_response("something broke", &RequestId::mint());
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp.headers()["content-type"], "application/json");
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("something broke"));
    }

    #[test]
    fn error_response_escapes_quotes() {
        let resp = error_response(r#"bad "input""#, &RequestId::mint());
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains(r#"bad \"input\""#));
    }

    #[test]
    fn error_response_escapes_control_chars_and_backslashes() {
        let resp = error_response("back\\slash\nnewline\ttab", &RequestId::mint());
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("must parse");
        assert_eq!(parsed["error"], "back\\slash\nnewline\ttab");
    }

    #[test]
    fn overloaded_503_with_retry_after() {
        let resp = overloaded_response("too busy", &RequestId::mint());
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
        let resp = gateway_timeout_response(&RequestId::mint());
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("request timeout"));
    }

    #[test]
    fn build_response_ok() {
        let data = ResponseData {
            body: Bytes::from("hello"),
            content_type: HeaderValue::from_static("text/plain"),
            status: StatusCode::OK,
            headers: ResponseHeaders::new(),
        };
        let resp = build_response(data);
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
            status: StatusCode::CREATED,
            headers,
        };
        let resp = build_response(data);
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(resp.headers()["x-custom"], "value");
    }

    #[test]
    fn handler_content_type_and_server_replace_the_defaults() {
        let mut headers = ResponseHeaders::new();
        let map = headers.as_map_mut();
        map.insert(CONTENT_TYPE, HeaderValue::from_static("text/csv"));
        map.insert(SERVER, HeaderValue::from_static("mine"));
        let resp = build_response(ResponseData {
            body: Bytes::from("a,b"),
            content_type: TEXT,
            status: StatusCode::OK,
            headers,
        });
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
        let resp = build_response(ResponseData {
            body: Bytes::new(),
            content_type: TEXT,
            status: StatusCode::OK,
            headers,
        });
        let cookies: Vec<_> = resp.headers().get_all("set-cookie").iter().collect();
        assert_eq!(cookies, ["a=1", "b=2"]);
    }

    #[test]
    fn an_invalid_status_is_a_mapping_error() {
        assert!(matches!(
            http_status(1000),
            Err(ResponseError::Status(1000))
        ));
        assert!(ResponseError::Status(1000)
            .to_string()
            .contains("invalid HTTP status 1000"));
        assert_eq!(http_status(201).unwrap(), StatusCode::CREATED);
    }
}
