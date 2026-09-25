//! TPC request handler.
//!
//! A worker route runs inline: on the TPC thread's own OS thread, in its own
//! sub-interpreter, with no cross-thread wake. A `gil=True` route or the fallback goes to
//! the main-interpreter bridge.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::http::request::Parts;
use hyper::{Request, Response};

use crate::bridge::main_bridge::{GilWorkItem, MainInterpBridge, TryDispatchError};
use crate::python::interp::{self, SubInterpreterWorker};
use crate::router::{Call, Params, RequestBody, RouteId, Target};
use crate::site::Site;

use super::pipeline::{
    await_reply, collect_body, finish, preprocess, refuse, AcceptEncoding, Prepared, Preprocessed,
    Refusal, RequestLine, Served, REQUEST_BUDGET,
};
use super::{
    build_main_http_response, full_body, http_response, max_body_size, stream_body_feeder, BoxBody,
};

pub(crate) async fn handle_request_tpc_inline(
    req: Request<Incoming>,
    site: &'static Site,
    worker: Rc<RefCell<SubInterpreterWorker>>,
    client_ip_addr: std::net::IpAddr,
    main_bridge: Option<Arc<MainInterpBridge>>,
) -> Result<Response<BoxBody>, hyper::Error> {
    let prepared = match preprocess(req, site).await? {
        Preprocessed::Respond(r) => return Ok(r),
        Preprocessed::Dispatch(p) => p,
    };
    let resp = match prepared.call {
        Call::Worker(route, _) => run_inline(site, &worker, prepared, route, client_ip_addr).await,
        Call::Main(target, body) => {
            run_on_bridge(site, main_bridge, prepared, target, body, client_ip_addr).await
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
    worker: &Rc<RefCell<SubInterpreterWorker>>,
    prepared: Prepared,
    route: RouteId,
    client_ip_addr: std::net::IpAddr,
) -> Response<BoxBody> {
    let Prepared {
        mut parts,
        body,
        params,
        start,
        ..
    } = prepared;
    let headers = std::mem::take(&mut parts.headers);
    let line = RequestLine {
        method: parts.method.as_str(),
        path: parts.uri.path(),
        start,
    };
    let resp = match collect_body(body, max_body_size()).await {
        Ok(body) => {
            let request = InlineRequest {
                parts: &parts,
                headers,
                params: &params,
                body,
                client_ip: client_ip_addr,
            };
            call_inline(site, worker, route, request)
        }
        Err(reject) => reject.into_response(),
    };
    finish(resp, site, &line, Served::Inline)
}

/// What an inline handler's `Request` is made of; the headers are moved out of `parts`.
struct InlineRequest<'a> {
    parts: &'a Parts,
    headers: hyper::HeaderMap,
    params: &'a Params,
    body: Bytes,
    client_ip: std::net::IpAddr,
}

fn call_inline(
    site: &Site,
    worker: &Rc<RefCell<SubInterpreterWorker>>,
    route: RouteId,
    request: InlineRequest<'_>,
) -> Response<BoxBody> {
    let name = &site.routes.route(route).name;
    let accept_encoding = AcceptEncoding::of(&request.headers);
    let InlineRequest {
        parts,
        headers,
        params,
        body,
        client_ip,
    } = request;
    let called = Instant::now();

    // Acquire the TPC thread's sub-interp GIL, run the handler, release. Non-Send because
    // Rc<RefCell<_>> and *mut PyThreadState cross no await.
    let result = {
        let mut worker_ref = worker.borrow_mut();
        let tstate_cell = Cell::new(worker_ref.tstate);
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            let _guard = interp::SubInterpGilGuard::acquire(tstate_cell.get(), &tstate_cell);
            worker_ref.call_handler(
                route,
                parts.method.as_str(),
                parts.uri.path(),
                params,
                parts.uri.query().unwrap_or(""),
                body,
                headers,
                client_ip,
            )
        }));
        worker_ref.tstate = tstate_cell.get();
        res.unwrap_or_else(|payload| {
            let msg = payload
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            tracing::error!(target: "pyronova::handler", panic = msg, handler = %name, "TPC handler panicked");
            Err(format!("internal error: TPC handler panic: {msg}"))
        })
    };

    let took = called.elapsed();
    if took > REQUEST_BUDGET {
        tracing::error!(
            target: "pyronova::handler",
            handler = %name,
            took_ms = took.as_millis() as u64,
            "handler ran past the {REQUEST_BUDGET:?} request budget, blocking its TPC thread \
             the whole time; answered 504. Use `async def` or gil=True for slow work"
        );
        return refuse(Refusal::TimedOut);
    }
    http_response(result, accept_encoding.as_str())
}

/// Runs a main-interpreter call (a `gil=True` route or the fallback) through the bridge.
async fn run_on_bridge(
    site: &Site,
    bridge: Option<Arc<MainInterpBridge>>,
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
        ..
    } = prepared;
    let headers = std::mem::take(&mut parts.headers);
    let line = RequestLine {
        method: parts.method.as_str(),
        path: parts.uri.path(),
        start,
    };
    let resp = match bridge {
        Some(bridge) => {
            let call = BridgeCall {
                target,
                body,
                params,
                headers,
                client_ip: client_ip_addr,
            };
            dispatch_to_bridge(&bridge, &parts, incoming, call).await
        }
        // The bridge is spawned whenever the table has a main-interpreter call.
        None => full_body(crate::response::error_response(
            "main-interpreter call requested but the main-interp bridge is not running",
        )),
    };
    finish(resp, site, &line, Served::Bridge)
}

struct BridgeCall {
    target: Target,
    body: RequestBody,
    params: Params,
    headers: hyper::HeaderMap,
    client_ip: std::net::IpAddr,
}

async fn dispatch_to_bridge(
    bridge: &MainInterpBridge,
    parts: &Parts,
    incoming: Incoming,
    call: BridgeCall,
) -> Response<BoxBody> {
    // A streamed body is fed on this thread's LocalSet while the bridge's handler reads it.
    // The feeder's handle is kept so a rejected dispatch can stop it: dropping the receiver
    // alone isn't seen while the feeder waits on the client's next frame.
    let (body_bytes, body_stream_rx, feeder) = match call.body {
        RequestBody::Streamed => {
            let (tx, rx) = tokio::sync::mpsc::channel(crate::python::body_stream::CHANNEL_CAPACITY);
            let feeder =
                tokio::task::spawn_local(stream_body_feeder(incoming, tx, max_body_size()));
            (
                Bytes::new(),
                Arc::new(std::sync::Mutex::new(Some(rx))),
                Some(feeder),
            )
        }
        RequestBody::Buffered => match collect_body(incoming, max_body_size()).await {
            Ok(bytes) => (
                bytes,
                crate::python::body_stream::empty_body_stream_rx(),
                None,
            ),
            Err(reject) => return reject.into_response(),
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
        target: call.target,
        body_stream_rx,
        response_tx,
    };
    if let Err((rejected, err)) = bridge.try_dispatch(item) {
        drop(rejected);
        if let Some(feeder) = feeder {
            feeder.abort();
        }
        return refuse(match err {
            TryDispatchError::Full => Refusal::Overloaded("gil=True bridge queue full"),
            TryDispatchError::Closed => Refusal::ShuttingDown("gil=True bridge stopped"),
        });
    }

    match await_reply(response_rx, "gil=True bridge dropped the request").await {
        Ok(result) => build_main_http_response(result, accept_encoding.as_str()),
        Err(refusal) => refuse(refusal),
    }
}
