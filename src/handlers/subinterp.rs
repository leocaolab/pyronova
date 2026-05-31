//! Sub-interpreter pool handler.
//!
//! Non-TPC dispatch: hands work to a shared `InterpreterPool` of
//! sub-interpreter workers. Each worker runs on its own OS thread
//! and pulls from a crossbeam MPMC channel. Used when TPC is off
//! (opt-out) or not applicable.
//!
//! Extracted out of the monolithic `src/handlers.rs`. Shared helpers
//! live in the parent and are imported via `super::`.

use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::python::interp;
use crate::response::{
    build_response, error_response, gateway_timeout_response, overloaded_response,
    payload_too_large_response,
};
use crate::router::FrozenRoutes;
use crate::types::PyronovaRequest;

use super::{
    apply_cors, build_stream_response, call_handler_with_hooks, full_body, max_body_size,
    preprocess_request, stream_body_feeder, BoxBody, HandlerResult, Prepared, Preprocessed,
    SharedPool,
};

pub(crate) async fn handle_request_subinterp(
    req: Request<Incoming>,
    pool: SharedPool,
    routes: FrozenRoutes,
    client_ip_addr: std::net::IpAddr,
) -> Result<Response<BoxBody>, hyper::Error> {
    // Shared preprocessing (see `handle_request`). Route lookup, static
    // dirs, and CORS for the static-file / 404 fallback come from the
    // pool; the fast-path table and fallback flag come from `routes`.
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
        pool.cors_config.as_ref(),
        &pool.static_dirs,
        |m, p| pool.lookup(m, p),
    )
    .await?
    {
        Preprocessed::Respond(r) => return Ok(r),
        Preprocessed::Dispatch(p) => p,
    };

    // ── Hybrid dispatch: GIL routes use main interpreter ──
    let is_gil_route =
        handler_idx == usize::MAX || pool.requires_gil.get(handler_idx).copied().unwrap_or(false);

    // Admission gate (sub-interp, large-body path only): take a permit
    // BEFORE a potentially-large body collect so that N concurrent
    // uploads can't pile N × max_body_size into RAM. The gate is
    // deliberately SKIPPED for small/no-body requests — HTTP/2
    // multiplexes hundreds of streams per connection and gcannon's
    // baseline-h2 profile easily puts 25k+ concurrent small requests
    // in flight at once. A blanket permit budget sized for "the
    // queue" (n × 128) would reject 99% of them and destroy h2
    // throughput; a budget sized for "the worst-case RAM cost of a
    // body flood" would be too large to protect against it. The
    // split: small bodies (<= ADMISSION_SKIP_BYTES) pass through,
    // large bodies (> threshold) require a permit.
    const ADMISSION_SKIP_BYTES: u64 = 64 * 1024; // 64 KiB
    let content_length = raw_headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    // Fast path: an *honestly* declared large body takes its permit
    // upfront so we can reject before reading a byte. A client that
    // under-declares (or omits Content-Length via chunked / HTTP-2
    // framing, which parses to 0) slips past here — the buffered collect
    // below re-checks against bytes actually received and acquires the
    // permit lazily, so the memory bound holds regardless of the header.
    let mut submit_permit = if !is_gil_route && content_length > ADMISSION_SKIP_BYTES {
        match pool.submit_semaphore.clone().try_acquire_owned() {
            Ok(p) => Some(p),
            Err(_) => {
                crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut r = full_body(overloaded_response("server overloaded"));
                apply_cors(&mut r, pool.cors_config.as_ref());
                return Ok(r);
            }
        }
    } else {
        None
    };

    // Streaming is only honored on GIL routes (v1). A sub-interp route
    // with stream=True falls through to the buffered path — but that's
    // impossible by construction because add_route rejects non-GIL
    // streaming at registration time.
    let is_stream_route = is_gil_route
        && handler_idx != usize::MAX
        && routes.is_stream.get(handler_idx).copied().unwrap_or(false);

    // Invariant guard: registering a stream=True route requires gil=True
    // (`add_route` enforces this). If somehow that constraint is bypassed
    // — a future refactor, a router hack — we'd spawn the feeder task and
    // then send the resulting empty `body_bytes` to a sub-interpreter
    // worker, silently discarding the client's upload. Fail closed with
    // a 500 instead of the black hole.
    let is_stream_on_subinterp = handler_idx != usize::MAX
        && !is_gil_route
        && routes.is_stream.get(handler_idx).copied().unwrap_or(false);
    if is_stream_on_subinterp {
        let mut r = full_body(error_response(
            "stream=True routes must be registered with gil=True (framework invariant violated)",
        ));
        apply_cors(&mut r, pool.cors_config.as_ref());
        return Ok(r);
    }

    // Decide body handling: stream-capable GIL routes bypass the collect
    // (proper streaming), everyone else collects up front.
    let (body_bytes, body_stream_rx_early) = if is_stream_route {
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::python::body_stream::ChunkMsg>(
            crate::python::body_stream::CHANNEL_CAPACITY,
        );
        let cap = max_body_size();
        tokio::spawn(stream_body_feeder(body_obj, tx, cap));
        (
            Bytes::new(),
            Some(Arc::new(std::sync::Mutex::new(Some(rx)))),
        )
    } else {
        // Buffered collect with lazy admission: the permit is acquired
        // here if the body's *actual* size crosses the threshold, even
        // when Content-Length claimed otherwise. GIL routes never gate.
        use super::AdmissionCollect;
        let outcome = super::collect_body_with_admission(
            body_obj,
            max_body_size(),
            !is_gil_route,
            ADMISSION_SKIP_BYTES,
            &pool.submit_semaphore,
            &mut submit_permit,
        )
        .await;
        match outcome {
            AdmissionCollect::Body(b) => (b, None),
            AdmissionCollect::Overloaded => {
                crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut r = full_body(overloaded_response("server overloaded"));
                apply_cors(&mut r, pool.cors_config.as_ref());
                return Ok(r);
            }
            AdmissionCollect::TooLarge => {
                let mut r = full_body(payload_too_large_response());
                apply_cors(&mut r, pool.cors_config.as_ref());
                return Ok(r);
            }
            AdmissionCollect::ReadError(e) => {
                tracing::warn!(target: "pyronova::handler", error = %e, "body read error");
                let mut r = full_body(crate::response::error_response("body read failed"));
                apply_cors(&mut r, pool.cors_config.as_ref());
                return Ok(r);
            }
            AdmissionCollect::Timeout => {
                let mut r = full_body(crate::response::gateway_timeout_response());
                apply_cors(&mut r, pool.cors_config.as_ref());
                return Ok(r);
            }
        }
    };

    if is_gil_route {
        let method_log = Arc::clone(&method);
        let path_log = Arc::clone(&path);
        let body_stream_rx = if let Some(rx) = body_stream_rx_early.clone() {
            rx
        } else if is_stream_route {
            // Defensive dead-branch: the `is_stream_route` arm above
            // always populates body_stream_rx_early, so control never
            // reaches here. Keep it type-correct and `try_send` (non-
            // awaiting) so a future refactor doesn't accidentally
            // reintroduce a pre-awaited send on a channel nobody reads.
            let (tx, rx) = tokio::sync::mpsc::channel::<crate::python::body_stream::ChunkMsg>(
                crate::python::body_stream::CHANNEL_CAPACITY,
            );
            if !body_bytes.is_empty() {
                let _ = tx.try_send(crate::python::body_stream::ChunkMsg::Data(
                    body_bytes.clone(),
                ));
            }
            let _ = tx.try_send(crate::python::body_stream::ChunkMsg::Eof);
            Arc::new(std::sync::Mutex::new(Some(rx)))
        } else {
            Arc::new(std::sync::Mutex::new(None))
        };
        // For stream routes, the body is served through the stream —
        // keep body_bytes empty so handler code that reads `.body`
        // doesn't double-consume.
        let body_bytes_for_req = if is_stream_route {
            Bytes::new()
        } else {
            body_bytes
        };
        let sky_req = PyronovaRequest {
            method,
            path,
            params,
            query,
            headers_source: crate::types::LazyHeaders::Raw(raw_headers),
            headers_cache: std::sync::OnceLock::new(),
            client_ip_addr,
            body_bytes: body_bytes_for_req,
            body_stream_rx,
            query_cache: std::sync::OnceLock::new(),
            query_all_cache: std::sync::OnceLock::new(),
        };

        let routes_ref = Arc::clone(&routes);
        let task = tokio::task::spawn_blocking(move || {
            call_handler_with_hooks(routes_ref, handler_idx, sky_req)
        });
        let handler_result = match tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                tracing::error!(
                    target: "pyronova::handler",
                    error = %e,
                    "GIL handler thread panicked or was cancelled"
                );
                HandlerResult::PyronovaResponse(Err("handler thread panicked".to_string()))
            }
            Err(_) => {
                crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut r = full_body(crate::response::gateway_timeout_response());
                apply_cors(&mut r, routes.cors_config.as_ref());
                return Ok(r);
            }
        };

        let mut resp = match handler_result {
            HandlerResult::PyronovaResponse(mut result) => {
                if let Ok(data) = result.as_mut() {
                    crate::compression::maybe_compress(data, &accept_encoding);
                }
                full_body(build_response(result)?)
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
        return Ok(resp);
    }

    // ── Default: sub-interpreter (fast path) ──
    // extract_headers / to_string deferred to the worker thread — see WorkRequest fields.
    let method_log = Arc::clone(&method);
    let path_log = Arc::clone(&path);
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();

    if let Err(e) = pool.submit(interp::WorkRequest {
        handler_idx,
        method: Arc::clone(&method),
        path: Arc::clone(&path),
        params,
        query,
        body: body_bytes,
        headers: raw_headers,
        client_ip: client_ip_addr,
        response_tx,
    }) {
        crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut r = full_body(overloaded_response(&e));
        apply_cors(&mut r, pool.cors_config.as_ref());
        return Ok(r);
    }
    interp::WorkRequest::inc_created();

    let result = match tokio::time::timeout(std::time::Duration::from_secs(30), response_rx).await {
        Ok(Ok(r)) => r,
        Ok(Err(_)) => Err("worker thread dropped response".to_string()),
        Err(_) => {
            crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut r = full_body(gateway_timeout_response());
            apply_cors(&mut r, pool.cors_config.as_ref());
            return Ok(r);
        }
    };

    let mut http_resp = super::build_subinterp_http_response(result, &accept_encoding, None);
    // Apply CORS uniformly to both Ok and Err paths. Previously the Err
    // path returned a bare 500 with no CORS headers, so a browser would
    // surface the real error as an opaque CORS failure — a classic
    // debugging trap where the server error is invisible client-side.
    apply_cors(&mut http_resp, pool.cors_config.as_ref());
    let latency_us = start.elapsed().as_micros() as u64;
    let status = http_resp.status().as_u16();
    if super::should_log_request(&routes, status) {
        tracing::info!(
            target: "pyronova::access",
            method = %method_log,
            path = %path_log,
            status,
            latency_us,
            mode = "subinterp",
            "PyronovaRequest handled"
        );
    }
    Ok(http_resp)
}
