//! TPC inline request handler.
//!
//! Hot path for the default (sync handler, non-GIL, non-streaming)
//! dispatch. Runs on the TPC worker's own OS thread, directly calls
//! the sub-interpreter's Python handler, writes the response back
//! through hyper. No cross-thread wakes, no mpsc channels.
//!
//! Extracted out of the monolithic `src/handlers.rs` so the hot path
//! has its own file; the GIL-mode and sub-interp-pool handlers stay
//! next door in the parent module. Shared helpers (`apply_cors`,
//! `full_body`, `build_fast_response`, `collect_body_bounded`,
//! `build_stream_response`) live in the parent and are imported via
//! `super::`.

use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::python::interp;
use crate::response::{
    build_response, error_response, gateway_timeout_response, not_found_response,
    overloaded_response,
};
use crate::static_fs::try_static_file;
use crate::types::extract_headers;

use super::{
    apply_cors, build_fast_response, build_stream_response, collect_body_bounded, full_body,
    max_body_size, stream_body_feeder, BoxBody,
};

pub(crate) async fn handle_request_tpc_inline(
    req: Request<Incoming>,
    routes: &'static crate::router::RouteTable,
    worker: std::rc::Rc<std::cell::RefCell<interp::SubInterpreterWorker>>,
    client_ip_addr: std::net::IpAddr,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
) -> Result<Response<BoxBody>, hyper::Error> {
    // gRPC short-circuit — fully handled in Rust, doesn't touch the sub-interp.
    if crate::grpc::is_grpc_request(&req) {
        return crate::grpc::handle_grpc(req).await;
    }
    crate::monitor::count_request();
    let start = std::time::Instant::now();

    // Fast-path: pre-built response. Zero Python, zero allocation.
    // Borrows method/path directly from hyper's request — no Arc::from,
    // no uri.clone, no String allocations. Nested map lookup uses
    // `String: Borrow<str>` so both levels accept `&str` directly.
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

    // Decompose with `into_parts` — no uri/method/headers clone, no
    // Arc::from, no `query.to_string()`. `hyper::Method` / `hyper::Uri`
    // / `HeaderMap` are all Arc-backed internally; moving them out of
    // Parts is refcount-free. The sub-interp hot path below runs
    // entirely on `&str` borrows from these locals — zero heap
    // allocations for method/path/query on the common route.
    let (parts, body_obj) = req.into_parts();
    let method = parts.method;
    let uri = parts.uri;
    let raw_headers = parts.headers;

    let accept_encoding = raw_headers
        .get(hyper::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let method_str: &str = method.as_str();
    let path: &str = uri.path();
    let query: &str = uri.query().unwrap_or("");

    // Router lookup. Use RouteTable::lookup to get percent-decoding
    // of path params for free — raw matchit param iteration would
    // leave `john%20doe` undecoded. (This was the bug in v2.3.0's
    // first TPC draft; test_path_params_are_url_decoded caught it.)
    let lookup = routes.lookup(method_str, path);

    // Static files — only on GET/HEAD miss.
    if lookup.is_none() && (method_str == "GET" || method_str == "HEAD") {
        if let Some(resp) = try_static_file(path, &routes.static_dirs).await {
            let mut r = full_body(resp);
            apply_cors(&mut r, routes.cors_config.as_ref());
            return Ok(r);
        }
    }

    let (handler_idx, params) = match lookup {
        Some(v) => v,
        None => {
            let mut r = full_body(not_found_response());
            apply_cors(&mut r, routes.cors_config.as_ref());
            return Ok(r);
        }
    };

    // Body handling depends on the route's stream flag:
    //   stream=True  → spawn a feeder task on this TPC thread's LocalSet
    //                  so the handler can drain the mpsc receiver chunk
    //                  by chunk. `body_bytes` stays empty. Route-
    //                  registration layer already enforces stream=True
    //                  implies gil=True, so this path feeds straight
    //                  into the main-interp bridge below.
    //   stream=False → collect the whole body with size + timeout
    //                  limits before dispatch.
    // Stream-route body handling. Route registration enforces
    // `stream=True ⇒ gil=True`, so a streaming route is always dispatched
    // through the main-interp bridge below — we build the mpsc receiver
    // here (so the feeder can run on this TPC thread's LocalSet) and
    // hand it to `GilWorkItem`. Non-stream routes skip all of this: the
    // TPC inline hot path (sync + non-gil + non-stream) never touches
    // `body_stream_rx` and pays no Arc::clone for a value it won't use.
    let is_stream_route = routes.is_stream[handler_idx];
    let (body_bytes, stream_rx): (
        Vec<u8>,
        Option<tokio::sync::mpsc::Receiver<crate::python::body_stream::ChunkMsg>>,
    ) = if is_stream_route {
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::python::body_stream::ChunkMsg>(
            crate::python::body_stream::CHANNEL_CAPACITY,
        );
        tokio::task::spawn_local(stream_body_feeder(body_obj, tx, max_body_size()));
        (Vec::new(), Some(rx))
    } else {
        match collect_body_bounded(body_obj).await {
            Ok(b) => (b, None),
            Err(mut r) => {
                apply_cors(&mut r, routes.cors_config.as_ref());
                return Ok(r);
            }
        }
    };

    let headers = extract_headers(&raw_headers);

    // ── Phase 3: gil=True route → main-interp bridge ──────────────────
    // If the matched route is registered with gil=True (C-extension /
    // pydantic-core / numpy / torch etc. that can't run in a sub-interp),
    // send the work to the dedicated main-interp bridge thread and await
    // its oneshot reply. TPC accept thread pays an MPSC try_send +
    // oneshot.await — everything else stays on the main interp. Full
    // MPSC queue → 503 fast (GIL-bound path mustn't back up and stall
    // the 440k rps sub-interp fleet). See src/main_bridge.rs for design.
    let is_gil_route = routes
        .requires_gil
        .get(handler_idx)
        .copied()
        .unwrap_or(false);
    if is_gil_route {
        let bridge = match main_bridge {
            Some(b) => b,
            None => {
                // Shouldn't happen: accept-loop bootstrap builds the
                // bridge iff `routes.requires_gil` contains any true.
                // If we got here with None, startup wiring is wrong.
                let mut r = full_body(error_response(
                    "gil=True route requested but main-interp bridge is not initialized",
                ));
                apply_cors(&mut r, routes.cors_config.as_ref());
                return Ok(r);
            }
        };
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        // body_stream_rx materialized lazily: wrap the per-request
        // receiver for streaming routes, fall back to the shared
        // singleton for buffered gil routes. Non-gil TPC inline
        // dispatch never runs this line — zero Arc::clone on that path.
        let body_stream_rx = match stream_rx {
            Some(rx) => std::sync::Arc::new(std::sync::Mutex::new(Some(rx))),
            None => crate::python::body_stream::empty_body_stream_rx(),
        };
        // Only here do we materialize owned strings — the bridge ships
        // the item cross-thread so it must outlive these stack locals.
        let item = crate::bridge::main_bridge::GilWorkItem {
            method: Arc::from(method_str),
            path: Arc::from(path),
            params,
            query: query.to_string(),
            body: Bytes::from(body_bytes),
            headers,
            client_ip: client_ip_addr,
            handler_idx,
            body_stream_rx,
            response_tx,
        };
        if let Err((_dropped_item, err)) = bridge.try_dispatch(item) {
            crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let resp = match err {
                crate::bridge::main_bridge::TryDispatchError::Full => {
                    overloaded_response("gil=True bridge queue full")
                }
                crate::bridge::main_bridge::TryDispatchError::Closed => {
                    error_response("gil=True bridge thread stopped")
                }
            };
            let mut r = full_body(resp);
            apply_cors(&mut r, routes.cors_config.as_ref());
            return Ok(r);
        }
        let result = match tokio::time::timeout(std::time::Duration::from_secs(30), response_rx)
            .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => {
                crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err("gil=True bridge dropped response (worker panicked or shut down)".to_string())
            }
            Err(_) => {
                crate::monitor::DROPPED_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut r = full_body(gateway_timeout_response());
                apply_cors(&mut r, routes.cors_config.as_ref());
                return Ok(r);
            }
        };
        // Bridge can return either a buffered response or a stream.
        // Buffered path: compress, build response, wrap. Stream path:
        // hand the mpsc receiver to hyper via build_stream_response,
        // which returns a Response<BoxBody> backed by StreamBody.
        let mut http_resp = match result {
            Ok(crate::bridge::main_bridge::BridgeResponse::Resp(mut resp_data)) => {
                crate::compression::maybe_compress(&mut resp_data, &accept_encoding);
                match build_response(Ok(resp_data)) {
                    Ok(r) => full_body(r),
                    Err(e) => {
                        tracing::error!(
                            target: "pyronova::handler",
                            error = %e,
                            "GIL bridge response build failed"
                        );
                        full_body(error_response("response build failed"))
                    }
                }
            }
            Ok(crate::bridge::main_bridge::BridgeResponse::Stream(info)) => {
                build_stream_response(info)
            }
            Err(e) => full_body(error_response(&e)),
        };
        apply_cors(&mut http_resp, routes.cors_config.as_ref());
        let latency_us = start.elapsed().as_micros() as u64;
        let status = http_resp.status().as_u16();
        if super::should_log_request(routes, status) {
            tracing::info!(
                target: "pyronova::access",
                method = %method,
                path = %path,
                status,
                latency_us,
                mode = "tpc-gil-bridge",
                "PyronovaRequest handled"
            );
        }
        return Ok(http_resp);
    }

    // Resolve the handler name up front via a checked lookup. handler_idx
    // comes from the router, which registers names/flags in lockstep, so
    // this should always succeed — but if the route table is ever
    // inconsistent we surface it as an explicit 500 here rather than
    // letting an out-of-range index panic *inside* the catch_unwind below,
    // where it would be misreported as a handler panic at line ~304.
    // call_handler is sync, so the &'static-derived borrow lives long enough.
    let handler_name = match routes.handler_names.get(handler_idx) {
        Some(name) => name,
        None => {
            tracing::error!(
                target: "pyronova::handler",
                handler_idx,
                handler_count = routes.handler_names.len(),
                "route table inconsistency: handler_idx out of range"
            );
            let mut r = full_body(error_response(
                "internal error: route table inconsistency",
            ));
            apply_cors(&mut r, routes.cors_config.as_ref());
            return Ok(r);
        }
    };

    // The inline dispatch: acquire the TPC thread's sub-interp GIL,
    // run the handler, release. Blocks the thread (which is the point —
    // peer TPC threads continue serving via SO_REUSEPORT). Non-Send
    // because Rc<RefCell<_>> and *mut PyThreadState cross no await.
    let result = {
        let mut worker_ref = worker.borrow_mut();
        let tstate_cell = std::cell::Cell::new(worker_ref.tstate);
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            let _guard = interp::SubInterpGilGuard::acquire(tstate_cell.get(), &tstate_cell);
            worker_ref.call_handler(
                handler_name,
                &routes.before_hook_names,
                &routes.after_hook_names,
                method_str,
                path,
                &params,
                query,
                &body_bytes,
                &headers,
                &client_ip_addr.to_string(),
            )
        }));
        worker_ref.tstate = tstate_cell.get();
        match res {
            Ok(r) => r,
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("unknown panic");
                tracing::error!(
                    target: "pyronova::handler",
                    panic = msg,
                    handler = %handler_name,
                    "TPC handler panicked"
                );
                Err(format!("internal error: TPC handler panic: {msg}"))
            }
        }
    };

    let mut http_resp = super::build_subinterp_http_response(
        result,
        &accept_encoding,
        Some(handler_name.as_str()),
    );
    apply_cors(&mut http_resp, routes.cors_config.as_ref());
    let latency_us = start.elapsed().as_micros() as u64;
    let status = http_resp.status().as_u16();
    if super::should_log_request(routes, status) {
        tracing::info!(
            target: "pyronova::access",
            method = %method,
            path = %path,
            status,
            latency_us,
            mode = "tpc-inline",
            "PyronovaRequest handled"
        );
    }
    Ok(http_resp)
}
