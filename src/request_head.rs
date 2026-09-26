//! The one place a handler's `Request` is made.
//!
//! Every path — the TPC inline worker, the sub-interpreter pool, the main interpreter
//! (GIL mode, `gil=True` routes, the TPC bridge), a WebSocket upgrade, and the Python-side
//! `Request(...)` constructor — builds its `Request` with [`PyronovaRequest::new`] from a
//! [`RequestHead`] and a [`Body`].
//!
//! Nothing is copied: the method, URI and headers move out of hyper's request parts, the
//! body is hyper's `Bytes` or the streamed body's receiver. The [`RequestLabel`] a path
//! keeps for its log lines is a copy of the method and reference-count increments of the
//! URI and id.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::OnceLock;

use bytes::Bytes;
use hyper::http::request::Parts;

use crate::body::{BodyReceiver, StreamSlot};
use crate::error::RequestLabel;
use crate::request_id::RequestId;
use crate::router::Params;
use crate::types::PyronovaRequest;

/// Everything a `Request` is made of besides its body.
pub(crate) struct RequestHead {
    /// Method, URI and headers as hyper parsed them; they move into the `Request`.
    pub(crate) parts: Parts,
    pub(crate) params: Params,
    pub(crate) client_ip: IpAddr,
    pub(crate) request_id: RequestId,
}

impl RequestHead {
    /// The request as its log lines name it.
    pub(crate) fn label(&self) -> RequestLabel {
        RequestLabel {
            id: self.request_id.clone(),
            method: self.parts.method.clone(),
            uri: self.parts.uri.clone(),
        }
    }
}

/// A request's body as its handler receives it.
pub(crate) enum Body {
    /// Collected in full before the handler runs.
    Buffered(Bytes),
    /// `stream=True`: fed to `req.stream` while the handler runs.
    Streamed(BodyReceiver),
}

impl PyronovaRequest {
    pub(crate) fn new(head: RequestHead, body: Body) -> Self {
        let RequestHead {
            parts,
            params,
            client_ip,
            request_id,
        } = head;
        let (body_bytes, body_stream) = match body {
            Body::Buffered(bytes) => (bytes, StreamSlot::Buffered),
            Body::Streamed(rx) => (Bytes::new(), StreamSlot::streamed(rx)),
        };
        PyronovaRequest {
            method: parts.method,
            uri: parts.uri,
            params,
            headers: parts.headers,
            client_ip_addr: client_ip,
            request_id,
            body_bytes,
            body_stream,
            query_cache: OnceLock::<HashMap<String, String>>::new(),
            query_all_cache: OnceLock::new(),
        }
    }

    /// The request as its log lines name it.
    pub(crate) fn label(&self) -> RequestLabel {
        RequestLabel {
            id: self.request_id.clone(),
            method: self.method.clone(),
            uri: self.uri.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(uri: &str) -> RequestHead {
        let (parts, ()) = hyper::Request::builder()
            .method("POST")
            .uri(uri)
            .header("x-a", "1")
            .body(())
            .unwrap()
            .into_parts();
        RequestHead {
            parts,
            params: vec![("id".into(), "7".into())],
            client_ip: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            request_id: RequestId::mint(),
        }
    }

    #[test]
    fn the_request_takes_the_head_as_is() {
        let request = PyronovaRequest::new(head("/items/7?q=1"), Body::Buffered(Bytes::new()));
        assert_eq!(request.method, hyper::Method::POST);
        assert_eq!(request.uri.path(), "/items/7");
        assert_eq!(request.uri.query(), Some("q=1"));
        assert_eq!(request.headers["x-a"], "1");
        assert!(matches!(request.body_stream, StreamSlot::Buffered));
    }

    #[test]
    fn the_label_shares_the_uri_bytes() {
        let head = head("/items/7?q=1");
        let label = head.label();
        let request = PyronovaRequest::new(head, Body::Buffered(Bytes::new()));
        // The label's path is the request's own bytes, not a copy.
        assert_eq!(label.uri.path().as_ptr(), request.uri.path().as_ptr());
        assert_eq!(label.tag().method, "POST");
    }
}
