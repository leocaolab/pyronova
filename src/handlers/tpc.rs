//! TPC request handler.
//!
//! A worker route runs inline: on the TPC thread's own OS thread, in its own
//! sub-interpreter, with no cross-thread wake. A `gil=True` route or the fallback goes to
//! the main-interpreter bridge.
//!
//! The inline path allocates nothing of its own per request: the handler's `Request` takes
//! the method, URI, headers and body bytes hyper already holds, and the log line keeps a
//! [`RequestLabel`] (a copy of the method, reference-count increments of the URI and id).

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::body::{body_channel, stream_body_feeder, BoxBody, REQUEST_BUDGET};
use crate::bridge::main_bridge::{GilWorkItem, MainInterpBridge, TryDispatchError};
use crate::error::{refuse, Refusal, RequestLabel, RequestTag};
use crate::python::interp::SubInterpreterWorker;
use crate::request_head::{Body, RequestHead};
use crate::router::{Call, RequestBody, RouteId, Target};
use crate::site::Site;
use crate::types::PyronovaRequest;
use crate::worker::TpcContext;

use super::pipeline::{
    await_reply, await_streamed_reply, collect_body, fail, finish, preprocess, AcceptEncoding,
    Prepared, Preprocessed, RequestLine, Served,
};
use super::{build_main_http_response, http_response};

pub(crate) async fn handle_request_tpc_inline(
    req: Request<Incoming>,
    context: Rc<TpcContext>,
    client_ip_addr: std::net::IpAddr,
) -> Result<Response<BoxBody>, hyper::Error> {
    let site = &*context.site;
    let prepared = match preprocess(req, site, client_ip_addr).await? {
        Preprocessed::Respond(r) => return Ok(r),
        Preprocessed::Dispatch(p) => p,
    };
    let resp = match prepared.call {
        Call::Worker(route, _) => run_inline(site, &context.worker, prepared, route).await,
        Call::Main(target, body) => {
            let bridge = context.bridge.as_deref();
            run_on_bridge(site, bridge, prepared, target, body).await
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
) -> Response<BoxBody> {
    let Prepared {
        head, body, start, ..
    } = prepared;
    let label = head.label();
    let resp = match collect_body(body, site.config.limits.max_body_bytes).await {
        Ok(bytes) => call_inline(site, worker, route, head, Body::Buffered(bytes), &label),
        Err(e) => fail(e, &label.tag()),
    };
    finish(resp, site, &RequestLine::of(&label, start), Served::Inline)
}

fn call_inline(
    site: &Site,
    worker: &RefCell<SubInterpreterWorker>,
    route: RouteId,
    head: RequestHead,
    body: Body,
    label: &RequestLabel,
) -> Response<BoxBody> {
    let accept_encoding = AcceptEncoding::of(&head);
    let request = PyronovaRequest::new(head, body);
    let called = Instant::now();

    // Acquire the TPC thread's sub-interp GIL, run the handler, release.
    // SAFETY: this TPC thread is the one its worker was rebound to, and no thread state is
    // current between requests.
    let result = unsafe { worker.borrow_mut().serve(route, request) };

    // A late result is not sent; what the handler raised, if it did, is part of the one
    // error the overrun logs.
    let took = called.elapsed();
    let result = if took > REQUEST_BUDGET {
        Err(refuse(Refusal::Overran {
            handler: site.routes.route(route).name.clone(),
            took,
            raised: result.err().map(Box::new),
        }))
    } else {
        result
    };
    // A failure is logged here, on this thread, once.
    http_response(result.map_err(|e| e.log(&label.tag())), &accept_encoding)
}

/// Runs a main-interpreter call (a `gil=True` route or the fallback) through the bridge.
async fn run_on_bridge(
    site: &Site,
    bridge: Option<&MainInterpBridge>,
    prepared: Prepared,
    target: Target,
    body: RequestBody,
) -> Response<BoxBody> {
    let Prepared {
        head,
        body: incoming,
        start,
        ..
    } = prepared;
    let label = head.label();
    let resp = match bridge {
        Some(bridge) => {
            let call = BridgeCall {
                target,
                body,
                max_body: site.config.limits.max_body_bytes,
            };
            dispatch_to_bridge(bridge, head, incoming, call, &label.tag()).await
        }
        // The bridge is spawned whenever the table has a main-interpreter call.
        None => fail(
            refuse(Refusal::WorkerLost(
                "the main-interpreter bridge is not running",
            )),
            &label.tag(),
        ),
    };
    finish(resp, site, &RequestLine::of(&label, start), Served::Bridge)
}

struct BridgeCall {
    target: Target,
    body: RequestBody,
    /// The app's `max_body_size`.
    max_body: usize,
}

async fn dispatch_to_bridge(
    bridge: &MainInterpBridge,
    head: RequestHead,
    incoming: Incoming,
    call: BridgeCall,
    tag: &RequestTag<'_>,
) -> Response<BoxBody> {
    // A streamed body is fed on this thread's LocalSet while the bridge's handler reads it.
    // The feeder's handle is kept so a rejected dispatch can stop it: dropping the receiver
    // alone isn't seen while the feeder waits on the client's next frame.
    let (body, feeder) = match call.body {
        RequestBody::Streamed => {
            let (tx, rx) = body_channel();
            let feeder = tokio::task::spawn_local(stream_body_feeder(incoming, tx, call.max_body));
            (Body::Streamed(rx), Some(feeder))
        }
        RequestBody::Buffered => match collect_body(incoming, call.max_body).await {
            Ok(bytes) => (Body::Buffered(bytes), None),
            Err(e) => return fail(e, tag),
        },
    };

    let accept_encoding = AcceptEncoding::of(&head);
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let item = GilWorkItem {
        target: call.target,
        request: PyronovaRequest::new(head, body),
        response_tx,
    };
    if let Err(err) = bridge.try_dispatch(item) {
        if let Some(feeder) = feeder {
            feeder.abort();
        }
        let refusal = match err {
            TryDispatchError::Full => Refusal::Overloaded("gil=True bridge queue"),
            TryDispatchError::Closed => Refusal::PoolClosed("gil=True bridge"),
        };
        return fail(refuse(refusal), tag);
    }

    let lost = |_| {
        refuse(Refusal::WorkerLost(
            "the gil=True bridge dropped the request",
        ))
    };
    let reply = match feeder {
        Some(mut feeder) => await_streamed_reply(&mut feeder, response_rx, lost).await,
        None => await_reply(response_rx, lost).await,
    };
    match reply {
        Ok(result) => build_main_http_response(result, &accept_encoding),
        Err(e) => fail(e, tag),
    }
}
