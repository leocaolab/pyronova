//! GIL-mode request handler.
//!
//! Default path when TPC is off — handlers run on the main Python
//! interpreter via `tokio::task::spawn_blocking`. Supports the full
//! route feature set (async def, streaming, C-extensions, gil=True
//! is moot here since everything already runs on the main interp).
//!
//! Extracted out of the monolithic `src/handlers.rs`. Shared helpers
//! (`apply_cors`, `full_body`, `build_fast_response`,
//! `stream_body_feeder`, `call_handler_with_hooks`,
//! `build_stream_response`, `HandlerResult`) live in the parent and
//! are imported via `super::`.

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::response::{build_response, not_found_response, payload_too_large_response};
use crate::router::FrozenRoutes;
use crate::static_fs::try_static_file;
use crate::types::PyronovaRequest;

use super::{
    apply_cors, build_fast_response, build_stream_response, call_handler_with_hooks, full_body,
    max_body_size, stream_body_feeder, BoxBody, HandlerResult,
};

pub(crate) async fn handle_request(
    req: Request<Incoming>,
    routes: FrozenRoutes,
    client_ip_addr: std::net::IpAddr,
) -> Result<Response<BoxBody>, hyper::Error> {
    // gRPC short-circuit: if the request looks like gRPC
    // (`application/grpc*` content-type on POST), it goes to the
    // hand-rolled unary dispatcher instead of the normal routing
    // pipeline. gRPC responses need HTTP/2 trailers with `grpc-status`,
    // which the regular Response<Full<Bytes>> path doesn't model.
    if crate::grpc::is_grpc_request(&req) {
        return crate::grpc::handle_grpc(req).await;
    }
    crate::monitor::count_request();
    let start = std::time::Instant::now();

    // Fast-path: zero-alloc lookup. Borrows method/path from hyper
    // directly; the nested map accepts `&str` via `String: Borrow<str>`.
    if !routes.fast_responses.is_empty() {
        if let Some(fr) = routes
            .fast_responses
            .get(req.method().as_str())
            .and_then(|m| m.get(req.uri().path()))
        {
            return Ok(full_body(build_fast_response(
                fr,
                routes.cors_config.as_ref(),
            )));
        }
    }

    let method: Arc<str> = Arc::from(req.method().as_str());
    let uri = req.uri().clone();
    let path: Arc<str> = Arc::from(uri.path());
    let query = uri.query().unwrap_or("").to_string();

    // Lazy headers: store raw HeaderMap, convert only if Python accesses req.headers.
    let raw_headers = req.headers().clone();
    // Capture Accept-Encoding before raw_headers is moved into the PyronovaRequest.
    let accept_encoding = raw_headers
        .get(hyper::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // Take ownership of the body before the route lookup — we may either
    // collect it synchronously (buffered path) or spawn a feeder task
    // (streaming path) based on the matched route's `stream` flag.
    let body_obj = req.into_body();

    let lookup = routes.lookup(&method, &path);
    let has_fallback = routes.fallback_handler.is_some();

    if lookup.is_none() && (method.as_ref() == "GET" || method.as_ref() == "HEAD") {
        if let Some(resp) = try_static_file(&path, &routes.static_dirs).await {
            // Static file hit: apply CORS before returning. Previously
            // this early-return path skipped `apply_cors` at the function
            // bottom, so CORS-configured apps served static assets
            // without the allow-origin / allow-methods headers.
            let mut r = full_body(resp);
            apply_cors(&mut r, routes.cors_config.as_ref());
            return Ok(r);
        }
    }

    let (handler_idx, params) = match lookup {
        Some(v) => v,
        None if has_fallback => (usize::MAX, Vec::new()),
        // 404 must still carry CORS headers — browsers running an
        // OPTIONS preflight against an unknown path would otherwise
        // block the real cross-origin request with a generic "CORS
        // policy" error that's painful to debug. The normal dispatch
        // path applies CORS at function-exit; we replicate that here.
        None => {
            let mut r = full_body(not_found_response());
            apply_cors(&mut r, routes.cors_config.as_ref());
            return Ok(r);
        }
    };

    let is_stream_route =
        handler_idx != usize::MAX && routes.is_stream.get(handler_idx).copied().unwrap_or(false);

    let (body_bytes, body_stream_rx) = if is_stream_route {
        // Streaming path: spawn a feeder that pushes body frames into a
        // channel. The handler takes the receiver out via `req.stream`
        // on first access.
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::python::body_stream::ChunkMsg>(
            crate::python::body_stream::CHANNEL_CAPACITY,
        );
        let cap = max_body_size();
        tokio::spawn(stream_body_feeder(body_obj, tx, cap));
        (Bytes::new(), Arc::new(std::sync::Mutex::new(Some(rx))))
    } else {
        // Buffered path (default): collect the whole body with size + time limits.
        use http_body_util::Limited;
        let limited = Limited::new(body_obj, max_body_size());
        let bytes =
            match tokio::time::timeout(std::time::Duration::from_secs(30), limited.collect()).await
            {
                Ok(Ok(c)) => c.to_bytes(),
                Ok(Err(_)) => {
                    let mut r = full_body(payload_too_large_response());
                    apply_cors(&mut r, routes.cors_config.as_ref());
                    return Ok(r);
                }
                Err(_) => {
                    let mut r = full_body(crate::response::gateway_timeout_response());
                    apply_cors(&mut r, routes.cors_config.as_ref());
                    return Ok(r);
                }
            };
        (bytes, Arc::new(std::sync::Mutex::new(None)))
    };

    let method_log = Arc::clone(&method);
    let path_log = Arc::clone(&path);
    let sky_req = PyronovaRequest {
        method,
        path,
        params,
        query,
        headers_source: crate::types::LazyHeaders::Raw(raw_headers),
        headers_cache: std::sync::OnceLock::new(),
        client_ip_addr,
        body_bytes,
        body_stream_rx,
        query_cache: std::sync::OnceLock::new(),
        query_all_cache: std::sync::OnceLock::new(),
    };

    // spawn_blocking: prevent GIL acquisition from starving Tokio workers
    let routes_ref = Arc::clone(&routes);
    let handler_result = tokio::task::spawn_blocking(move || {
        call_handler_with_hooks(routes_ref, handler_idx, sky_req)
    })
    .await
    .unwrap_or_else(|_| {
        HandlerResult::PyronovaResponse(Err("handler thread panicked".to_string()))
    });

    let mut resp = match handler_result {
        HandlerResult::PyronovaResponse(mut result) => {
            if let Ok(data) = result.as_mut() {
                crate::compression::maybe_compress(data, &accept_encoding);
            }
            // Don't propagate build_response errors via `?` — that would
            // skip apply_cors below, breaking CORS-protected APIs when
            // response construction fails (e.g. invalid header chars
            // from a handler). Convert to a 500 with CORS still applied,
            // matching the 404/413/504 paths above.
            match build_response(result) {
                Ok(r) => full_body(r),
                Err(e) => {
                    let mut r = full_body(crate::response::error_response(&e.to_string()));
                    apply_cors(&mut r, routes.cors_config.as_ref());
                    return Ok(r);
                }
            }
        }
        HandlerResult::PyronovaStream(info) => build_stream_response(info),
    };

    apply_cors(&mut resp, routes.cors_config.as_ref());
    let latency_us = start.elapsed().as_micros() as u64;
    let status = resp.status().as_u16();
    if super::should_log_request(&routes, status) {
        tracing::info!(
            target: "pyronova::access",
            method = %method_log,
            path = %path_log,
            status,
            latency_us,
            mode = "gil",
            "PyronovaRequest handled"
        );
    }
    Ok(resp)
}
