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

use crate::response::{build_response, payload_too_large_response};
use crate::router::FrozenRoutes;
use crate::types::PyronovaRequest;

use super::{
    apply_cors, build_stream_response, call_handler_with_hooks, full_body, max_body_size,
    preprocess_request, stream_body_feeder, BoxBody, HandlerResult, Prepared, Preprocessed,
};

pub(crate) async fn handle_request(
    req: Request<Incoming>,
    routes: FrozenRoutes,
    client_ip_addr: std::net::IpAddr,
) -> Result<Response<BoxBody>, hyper::Error> {
    // Shared preprocessing: gRPC short-circuit, fast-path, header/body
    // extraction, route lookup, and the static-file / 404 fallback. Any
    // short-circuit returns an early response; otherwise we get the
    // prepared request state and continue with GIL-specific dispatch.
    let Prepared {
        method,
        path,
        query,
        raw_headers,
        accept_encoding,
        body: body_obj,
        handler_idx,
        params,
        start,
    } = match preprocess_request(
        req,
        &routes,
        routes.cors_config.as_ref(),
        &routes.static_dirs,
        |m, p| routes.lookup(m, p),
    )
    .await?
    {
        Preprocessed::Respond(r) => return Ok(r),
        Preprocessed::Dispatch(p) => p,
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
