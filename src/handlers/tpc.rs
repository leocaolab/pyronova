//! TPC request handler.
//!
//! A worker route runs inline: on the TPC thread's own OS thread, in its own
//! sub-interpreter, with no cross-thread wake. A `gil=True` route or the fallback goes to
//! the main-interpreter bridge.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::http::request::Parts;
use hyper::{Request, Response};

use crate::bridge::main_bridge::{GilWorkItem, MainInterpBridge, TryDispatchError};
use crate::python::interp::SubInterpreterWorker;
use crate::request_id::RequestId;
use crate::router::{Call, Params, RequestBody, RouteId, Target};
use crate::site::Site;
use crate::types::PyronovaRequest;
use crate::worker::TpcContext;

use super::error::{HandlerError, RequestTag};
use super::pipeline::{
    await_reply, collect_body, fail, finish, preprocess, AcceptEncoding, Prepared, Preprocessed,
    RequestLine, Served, REQUEST_BUDGET,
};
use super::{build_main_http_response, http_response, stream_body_feeder, BoxBody};

pub(crate) async fn handle_request_tpc_inline(
    req: Request<Incoming>,
    context: Rc<TpcContext>,
    client_ip_addr: std::net::IpAddr,
) -> Result<Response<BoxBody>, hyper::Error> {
    let site = &*context.site;
    let prepared = match preprocess(req, site).await? {
        Preprocessed::Respond(r) => return Ok(r),
        Preprocessed::Dispatch(p) => p,
    };
    let resp = match prepared.call {
        Call::Worker(route, _) => {
            run_inline(site, &context.worker, prepared, route, client_ip_addr).await
        }
        Call::Main(target, body) => {
            let bridge = context.bridge.as_deref();
            run_on_bridge(site, bridge, prepared, target, body, client_ip_addr).await
        }
    };
    Ok(resp)
}

/// Runs a worker route on this thread's sub-interpreter.
///
/// The call blocks this thread (which is the point: peer TPC threads keep serving), so
/// nothing on it can answer before the handler returns. The request budget is enforced
/// when it does: a handler that ran past [`REQUEST_BUDGET`] gets the same 504 as on the
/// other paths, not its late result.
async fn run_inline(
    site: &Site,
    worker: &RefCell<SubInterpreterWorker>,
    prepared: Prepared,
    route: RouteId,
    client_ip_addr: std::net::IpAddr,
) -> Response<BoxBody> {
    let Prepared {
        mut parts,
        body,
        params,
        start,
        request_id,
        ..
    } = prepared;
    let headers = std::mem::take(&mut parts.headers);
    let line = RequestLine {
        method: parts.method.as_str(),
        path: parts.uri.path(),
        start,
    };
    let tag = RequestTag {
        id: &request_id,
        method: parts.method.as_str(),
        path: parts.uri.path(),
    };
    let resp = match collect_body(body, site.config.limits.max_body_bytes).await {
        Ok(body) => {
            let request = InlineRequest {
                parts: &parts,
                headers,
                params,
                body,
                client_ip: client_ip_addr,
                request_id: request_id.clone(),
            };
            call_inline(site, worker, route, request, &tag)
        }
        Err(e) => fail(e, &tag),
    };
    finish(resp, site, &line, Served::Inline)
}

/// What an inline handler's `Request` is made of; the headers are moved out of `parts`.
struct InlineRequest<'a> {
    parts: &'a Parts,
    headers: hyper::HeaderMap,
    params: Params,
    body: Bytes,
    client_ip: std::net::IpAddr,
    request_id: RequestId,
}

impl InlineRequest<'_> {
    fn into_request(self) -> PyronovaRequest {
        PyronovaRequest {
            method: Arc::from(self.parts.method.as_str()),
            path: Arc::from(self.parts.uri.path()),
            params: self.params,
            query: self.parts.uri.query().unwrap_or("").to_string(),
            headers: self.headers,
            client_ip_addr: self.client_ip,
            request_id: self.request_id,
            body_bytes: self.body,
            body_stream_rx: Arc::new(std::sync::Mutex::new(None)),
            query_cache: std::sync::OnceLock::new(),
            query_all_cache: std::sync::OnceLock::new(),
        }
    }
}

fn call_inline(
    site: &Site,
    worker: &RefCell<SubInterpreterWorker>,
    route: RouteId,
    request: InlineRequest<'_>,
    tag: &RequestTag<'_>,
) -> Response<BoxBody> {
    let accept_encoding = AcceptEncoding::of(&request.headers);
    let called = Instant::now();

    // Acquire the TPC thread's sub-interp GIL, run the handler, release. A failure is
    // logged here, on this thread.
    // SAFETY: this TPC thread is the one its worker was rebound to, and no thread state is
    // current between requests.
    let result =
        unsafe { worker.borrow_mut().serve(route, request.into_request()) }.map_err(|e| e.log(tag));

    let took = called.elapsed();
    if took > REQUEST_BUDGET {
        tracing::error!(
            target: "pyronova::handler",
            handler = %site.routes.route(route).name,
            took_ms = took.as_millis() as u64,
            "handler ran past the {REQUEST_BUDGET:?} request budget, blocking its TPC thread \
             the whole time; answered 504. Use `async def` or gil=True for slow work"
        );
        return fail(HandlerError::Timeout, tag);
    }
    http_response(result, accept_encoding.as_str())
}

/// Runs a main-interpreter call (a `gil=True` route or the fallback) through the bridge.
async fn run_on_bridge(
    site: &Site,
    bridge: Option<&MainInterpBridge>,
    prepared: Prepared,
    target: Target,
    body: RequestBody,
    client_ip_addr: std::net::IpAddr,
) -> Response<BoxBody> {
    let Prepared {
        mut parts,
        body: incoming,
        params,
        start,
        request_id,
        ..
    } = prepared;
    let headers = std::mem::take(&mut parts.headers);
    let line = RequestLine {
        method: parts.method.as_str(),
        path: parts.uri.path(),
        start,
    };
    let tag = RequestTag {
        id: &request_id,
        method: parts.method.as_str(),
        path: parts.uri.path(),
    };
    let resp = match bridge {
        Some(bridge) => {
            let call = BridgeCall {
                target,
                body,
                params,
                headers,
                client_ip: client_ip_addr,
                max_body: site.config.limits.max_body_bytes,
            };
            dispatch_to_bridge(bridge, &parts, incoming, call, &tag).await
        }
        // The bridge is spawned whenever the table has a main-interpreter call.
        None => fail(
            HandlerError::WorkerLost("the main-interpreter bridge is not running"),
            &tag,
        ),
    };
    finish(resp, site, &line, Served::Bridge)
}

struct BridgeCall {
    target: Target,
    body: RequestBody,
    params: Params,
    headers: hyper::HeaderMap,
    client_ip: std::net::IpAddr,
    /// The app's `max_body_size`.
    max_body: usize,
}

async fn dispatch_to_bridge(
    bridge: &MainInterpBridge,
    parts: &Parts,
    incoming: Incoming,
    call: BridgeCall,
    tag: &RequestTag<'_>,
) -> Response<BoxBody> {
    // A streamed body is fed on this thread's LocalSet while the bridge's handler reads it.
    // The feeder's handle is kept so a rejected dispatch can stop it: dropping the receiver
    // alone isn't seen while the feeder waits on the client's next frame.
    let (body_bytes, body_stream_rx, feeder) = match call.body {
        RequestBody::Streamed => {
            let (tx, rx) = tokio::sync::mpsc::channel(crate::python::body_stream::CHANNEL_CAPACITY);
            let feeder = tokio::task::spawn_local(stream_body_feeder(incoming, tx, call.max_body));
            (
                Bytes::new(),
                Arc::new(std::sync::Mutex::new(Some(rx))),
                Some(feeder),
            )
        }
        RequestBody::Buffered => match collect_body(incoming, call.max_body).await {
            Ok(bytes) => (
                bytes,
                crate::python::body_stream::empty_body_stream_rx(),
                None,
            ),
            Err(e) => return fail(e, tag),
        },
    };

    let accept_encoding = AcceptEncoding::of(&call.headers);
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let item = GilWorkItem {
        method: Arc::from(parts.method.as_str()),
        path: Arc::from(parts.uri.path()),
        params: call.params,
        query: parts.uri.query().unwrap_or("").to_string(),
        body: body_bytes,
        headers: call.headers,
        client_ip: call.client_ip,
        request_id: tag.id.clone(),
        target: call.target,
        body_stream_rx,
        response_tx,
    };
    if let Err((rejected, err)) = bridge.try_dispatch(item) {
        drop(rejected);
        if let Some(feeder) = feeder {
            feeder.abort();
        }
        let error = match err {
            TryDispatchError::Full => HandlerError::Overloaded("gil=True bridge queue"),
            TryDispatchError::Closed => HandlerError::PoolClosed("gil=True bridge"),
        };
        return fail(error, tag);
    }

    let lost = |_| HandlerError::WorkerLost("the gil=True bridge dropped the request");
    match await_reply(response_rx, lost).await {
        Ok(result) => build_main_http_response(result, accept_encoding.as_str()),
        Err(e) => fail(e, tag),
    }
}
