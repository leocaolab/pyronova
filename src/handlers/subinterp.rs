//! Sub-interpreter pool dispatch (TPC off): a worker route goes to the shared
//! `InterpreterPool`, whose workers each run on their own OS thread and pull from a
//! crossbeam MPMC channel; `gil=True` routes and the fallback run on main.

use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::body::BoxBody;
use crate::error::{refuse, Refusal, RequestTag};
use crate::python::interp;
use crate::request_head::Body;
use crate::router::{Call, HandlerKind, RouteId};
use crate::site::{SharedSite, Site};
use crate::types::PyronovaRequest;

use super::gil::run_on_main;
use super::pipeline::{
    await_reply, collect_body_with_admission, fail, finish, overloaded, preprocess, AcceptEncoding,
    Admission, Prepared, Preprocessed, RequestLine, Served,
};
use super::{http_response, SharedPool};

/// Bodies up to this size skip the admission gate. HTTP/2 multiplexes hundreds of streams
/// per connection, so tens of thousands of small requests can be in flight at once: a
/// permit budget sized for "the queue" would reject most of them, and one sized for the
/// worst-case RAM of a body flood would not protect against it. Only large bodies need a
/// permit.
const ADMISSION_SKIP_BYTES: u64 = 64 * 1024;

pub(crate) async fn handle_request_subinterp(
    req: Request<Incoming>,
    pool: SharedPool,
    site: SharedSite,
    client_ip_addr: std::net::IpAddr,
) -> Result<Response<BoxBody>, hyper::Error> {
    let prepared = match preprocess(req, &site, client_ip_addr).await? {
        Preprocessed::Respond(r) => return Ok(r),
        Preprocessed::Dispatch(p) => p,
    };
    let resp = match prepared.call {
        Call::Main(target, body) => run_on_main(&site, prepared, target, body, Served::Main).await,
        Call::Worker(route, kind) => serve_on_pool(&pool, &site, prepared, route, kind).await,
    };
    Ok(resp)
}

/// Runs a worker route on `pool` and finishes its response. A TPC thread serves its
/// `async def` routes this way too, on the async pool.
pub(crate) async fn serve_on_pool(
    pool: &SharedPool,
    site: &Site,
    prepared: Prepared,
    route: RouteId,
    kind: HandlerKind,
) -> Response<BoxBody> {
    let label = prepared.head.label();
    let start = prepared.start;
    let work = PoolWork {
        route,
        kind,
        max_body: site.config.limits.max_body_bytes,
        compression: site.config.compression,
    };
    let resp = run_on_pool(pool, prepared, work, &label.tag()).await;
    finish(resp, site, &RequestLine::of(&label, start), Served::Pool)
}

/// What the pool runs, besides the request itself.
struct PoolWork {
    route: RouteId,
    kind: HandlerKind,
    /// The app's `max_body_size`.
    max_body: usize,
    /// The app's compression settings; `None` = off.
    compression: Option<crate::compression::Settings>,
}

async fn run_on_pool(
    pool: &SharedPool,
    prepared: Prepared,
    work: PoolWork,
    tag: &RequestTag<'_>,
) -> Response<BoxBody> {
    let Prepared { head, body, .. } = prepared;
    // An honestly declared large body takes its permit before a byte is read, so it can be
    // rejected upfront. One that under-declares is caught by the collector.
    let content_length = head
        .parts
        .headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let upfront = if content_length > ADMISSION_SKIP_BYTES {
        match pool.submit_semaphore.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => return fail(overloaded(), tag),
        }
    } else {
        None
    };
    let admission = Admission {
        semaphore: &pool.submit_semaphore,
        skip_bytes: ADMISSION_SKIP_BYTES,
        permit: upfront,
    };
    let accept_encoding = AcceptEncoding::of(&head);
    let admitted = match collect_body_with_admission(body, work.max_body, admission).await {
        Ok(admitted) => admitted,
        Err(e) => return fail(e, tag),
    };
    // Held until the response is ready.
    let _permit = admitted.permit;

    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let submitted = pool.submit(interp::WorkRequest {
        route: work.route,
        kind: work.kind,
        request: PyronovaRequest::new(head, Body::Buffered(admitted.body)),
        response_tx,
    });
    if let Err(e) = submitted {
        let refusal = match e {
            interp::SubmitError::Full => Refusal::Overloaded("sub-interpreter work queue"),
            interp::SubmitError::Closed => Refusal::PoolClosed("sub-interpreter pool"),
        };
        return fail(refuse(refusal), tag);
    }
    interp::WorkRequest::inc_created();

    let lost = |_| {
        refuse(Refusal::WorkerLost(
            "the sub-interpreter worker dropped the request",
        ))
    };
    match await_reply(response_rx, lost).await {
        Ok(result) => http_response(result, &accept_encoding, work.compression.as_ref()).await,
        Err(e) => fail(e, tag),
    }
}
