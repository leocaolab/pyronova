use std::collections::HashMap;

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

pub(crate) const SERVER_HEADER: &str = concat!("Pyronova/", env!("CARGO_PKG_VERSION"));
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString};

use crate::types::{PyronovaResponse, ResponseData};
use pyo3::types::PyBytes;

// ---------------------------------------------------------------------------
// orjson-backed serializer (required dep: always available)
// ---------------------------------------------------------------------------

// Cached `_dumps(obj)` wrapper compiled once via PyModule::from_code.
// Py<T> is Send+Sync; GIL is held whenever this is accessed.
static ORJSON_HELPER: std::sync::OnceLock<pyo3::Py<pyo3::PyAny>> = std::sync::OnceLock::new();

fn get_or_init_json_dumps(py: Python<'_>) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> {
    if let Some(f) = ORJSON_HELPER.get() {
        return Ok(f.bind(py).clone());
    }
    // PyModule::from_code gives correct module-level scoping: _orjson and
    // _default are in the module's __dict__, so the dumps closure sees them.
    let module = pyo3::types::PyModule::from_code(
        py,
        c"import orjson as _orjson\n\ndef _default(obj):\n    if isinstance(obj, (set, frozenset)):\n        return list(obj)\n    raise TypeError(f'not serializable: {type(obj).__name__}')\n\ndef dumps(obj):\n    return _orjson.dumps(obj, default=_default)\n",
        c"pyronova_json",
        c"pyronova_json",
    )?;
    let f = module.getattr("dumps")?;
    // Resolve the race: if another thread won the `set`, discard our local `f`
    // and return the cached winner so every caller sees the same singleton.
    match ORJSON_HELPER.set(f.clone().unbind()) {
        Ok(()) => Ok(f),
        Err(_) => Ok(ORJSON_HELPER.get().unwrap().bind(py).clone()),
    }
}

fn orjson_dumps(py: Python<'_>, obj: &pyo3::Bound<'_, pyo3::PyAny>) -> Result<Bytes, String> {
    let dumps = get_or_init_json_dumps(py).map_err(|e| format!("json init: {e}"))?;
    let result = dumps.call1((obj,)).map_err(|e| format!("json error: {e}"))?;
    let bytes = result
        .cast::<PyBytes>()
        .map_err(|e| format!("json error: {e}"))?;
    Ok(Bytes::copy_from_slice(bytes.as_bytes()))
}

// ---------------------------------------------------------------------------
// Extract handler return value → ResponseData
// ---------------------------------------------------------------------------

pub(crate) fn extract_response_data(
    py: Python<'_>,
    obj: Bound<'_, pyo3::PyAny>,
) -> Result<ResponseData, String> {
    // dict / list → JSON — most common return type; checked first to skip
    // the PyronovaResponse cast on the hot path.
    if obj.cast::<PyDict>().is_ok() || obj.cast::<PyList>().is_ok() {
        let json_bytes = orjson_dumps(py, &obj)?;
        return Ok(ResponseData {
            body: json_bytes,
            content_type: "application/json".to_string(),
            status: 200,
            headers: HashMap::new(),
        });
    }

    // Plain string
    if let Ok(s) = obj.cast::<PyString>() {
        let st = s.to_string();
        let ct = if st.starts_with('{') || (st.starts_with('[') && st.trim_end().ends_with(']')) {
            "application/json"
        } else {
            "text/plain; charset=utf-8"
        };
        return Ok(ResponseData {
            body: Bytes::from(st),
            content_type: ct.to_string(),
            status: 200,
            headers: HashMap::new(),
        });
    }

    // PyronovaResponse — custom status / headers / content-type
    if let Ok(resp) = obj.cast::<PyronovaResponse>() {
        let resp = resp.get();
        let body_bound = resp.body.bind(py);

        let (body_bytes, auto_ct) = if let Ok(s) = body_bound.cast::<PyString>() {
            let st = s.to_string();
            let ct = if st.starts_with('{') || (st.starts_with('[') && st.trim_end().ends_with(']'))
            {
                "application/json"
            } else {
                "text/plain; charset=utf-8"
            };
            (Bytes::from(st), ct)
        } else if body_bound.cast::<PyDict>().is_ok() || body_bound.cast::<PyList>().is_ok() {
            let json_bytes = orjson_dumps(py, body_bound)?;
            (json_bytes, "application/json")
        } else if let Ok(pb) = body_bound.cast::<PyBytes>() {
            // Fast path for PyBytes: one copy (PyBytes buffer → Bytes
            // heap) instead of PyBytes::extract which builds a
            // transient Vec<u8> before we wrap it — same number of
            // allocations on the surface, but skips the iteration.
            (
                Bytes::copy_from_slice(pb.as_bytes()),
                "application/octet-stream",
            )
        } else if let Ok(b) = body_bound.extract::<Vec<u8>>() {
            (Bytes::from(b), "application/octet-stream")
        } else {
            let st = body_bound.str().map_err(|e| e.to_string())?.to_string();
            (Bytes::from(st), "text/plain; charset=utf-8")
        };

        let content_type = resp
            .content_type
            .clone()
            .unwrap_or_else(|| auto_ct.to_string());

        return Ok(ResponseData {
            body: body_bytes,
            content_type,
            status: resp.status_code,
            headers: resp.headers.clone(),
        });
    }

    // bytes — fast path for PyBytes skips the Vec<u8> extraction step
    if let Ok(pb) = obj.cast::<PyBytes>() {
        return Ok(ResponseData {
            body: Bytes::copy_from_slice(pb.as_bytes()),
            content_type: "application/octet-stream".to_string(),
            status: 200,
            headers: HashMap::new(),
        });
    }
    if let Ok(b) = obj.extract::<Vec<u8>>() {
        return Ok(ResponseData {
            body: Bytes::from(b),
            content_type: "application/octet-stream".to_string(),
            status: 200,
            headers: HashMap::new(),
        });
    }

    // fallback: str()
    let st = obj.str().map_err(|e| e.to_string())?.to_string();
    Ok(ResponseData {
        body: Bytes::from(st),
        content_type: "text/plain; charset=utf-8".to_string(),
        status: 200,
        headers: HashMap::new(),
    })
}

// ---------------------------------------------------------------------------
// HTTP response builders
// ---------------------------------------------------------------------------

pub(crate) fn build_response(
    result: Result<ResponseData, String>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    match result {
        Ok(data) => {
            let status = StatusCode::from_u16(data.status).unwrap_or_else(|_| {
                tracing::warn!(
                    target: "pyronova::response",
                    status = data.status,
                    "handler returned invalid HTTP status code, using 500"
                );
                StatusCode::INTERNAL_SERVER_ERROR
            });
            let mut builder = Response::builder()
                .status(status)
                .header("content-type", &data.content_type)
                .header("server", SERVER_HEADER);
            for (k, v) in &data.headers {
                // NUL-separated values encode multiple occurrences of the same
                // header key (e.g. multiple Set-Cookie lines set via cookies.py).
                for part in v.split('\0') {
                    builder = builder.header(k.as_str(), part);
                }
            }
            Ok(builder
                .body(Full::new(data.body))
                .unwrap_or_else(|_| error_response("invalid response headers")))
        }
        Err(e) => Ok(error_response(&e)),
    }
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
            content_type: "text/plain".to_string(),
            status: 200,
            headers: HashMap::new(),
        };
        let resp = build_response(Ok(data)).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "text/plain");
    }

    #[test]
    fn build_response_custom_status_and_headers() {
        let mut headers = HashMap::new();
        headers.insert("x-custom".to_string(), "value".to_string());
        let data = ResponseData {
            body: Bytes::from("created"),
            content_type: "application/json".to_string(),
            status: 201,
            headers,
        };
        let resp = build_response(Ok(data)).unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(resp.headers()["x-custom"], "value");
    }

    #[test]
    fn build_response_error_falls_back_to_500() {
        let resp = build_response(Err("oops".to_string())).unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
