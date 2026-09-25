//! Sub-interpreter pool dispatch (TPC off): a worker route goes to the shared
//! `InterpreterPool`, whose workers each run on their own OS thread and pull from a
//! crossbeam MPMC channel; `gil=True` routes and the fallback run on main.

use std::sync::Arc;

use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::python::interp;
use crate::router::{Call, HandlerKind, RouteId};
use crate::site::{SharedSite, Site};

use super::error::{HandlerError, RequestTag};
use super::gil::run_on_main;
use super::pipeline::{
    await_reply, collect_body_with_admission, fail, finish, preprocess, Admission, Prepared,
    Preprocessed, RequestLine, Served, OVERLOADED,
};
use super::{http_response, BoxBody, SharedPool};

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
    let prepared = match preprocess(req, &site).await? {
        Preprocessed::Respond(r) => return Ok(r),
        Preprocessed::Dispatch(p) => p,
    };
    let resp = match prepared.call {
        Call::Main(target, body) => {
            run_on_main(&site, prepared, target, body, client_ip_addr, Served::Main).await
        }
        Call::Worker(route, kind) => {
            serve_on_pool(&pool, &site, prepared, route, kind, client_ip_addr).await
        }
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
    client_ip_addr: std::net::IpAddr,
) -> Response<BoxBody> {
    let method: Arc<str> = Arc::from(prepared.parts.method.as_str());
    let path: Arc<str> = Arc::from(prepared.parts.uri.path());
    let line_start = prepared.start;
    let id = prepared.request_id.clone();
    let tag = RequestTag {
        id: &id,
        method: &method,
        path: &path,
    };
    let work = PoolWork {
        route,
        kind,
        method: Arc::clone(&method),
        path: Arc::clone(&path),
        client_ip: client_ip_addr,
        max_body: site.config.limits.max_body_bytes,
    };
    let resp = run_on_pool(pool, prepared, work, &tag).await;
    let line = RequestLine {
        method: &method,
        path: &path,
        start: line_start,
    };
    finish(resp, site, &line, Served::Pool)
}

/// What the pool runs, besides the request itself.
struct PoolWork {
    route: RouteId,
    kind: HandlerKind,
    method: Arc<str>,
    path: Arc<str>,
    client_ip: std::net::IpAddr,
    /// The app's `max_body_size`.
    max_body: usize,
}

async fn run_on_pool(
    pool: &SharedPool,
    prepared: Prepared,
    work: PoolWork,
    tag: &RequestTag<'_>,
) -> Response<BoxBody> {
    // An honestly declared large body takes its permit before a byte is read, so it can be
    // rejected upfront. One that under-declares is caught by the collector.
    let content_length = prepared
        .parts
        .headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let upfront = if content_length > ADMISSION_SKIP_BYTES {
        match pool.submit_semaphore.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => return fail(OVERLOADED, tag),
        }
    } else {
        None
    };
    let admission = Admission {
        semaphore: &pool.submit_semaphore,
        skip_bytes: ADMISSION_SKIP_BYTES,
        permit: upfront,
    };
    let accept_encoding = prepared.accept_encoding();
    let query = prepared.query().to_owned();
    let admitted = match collect_body_with_admission(prepared.body, work.max_body, admission).await
    {
        Ok(admitted) => admitted,
        Err(e) => return fail(e, tag),
    };
    // Held until the response is ready.
    let _permit = admitted.permit;

    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let submitted = pool.submit(interp::WorkRequest {
        route: work.route,
        kind: work.kind,
        method: work.method,
        path: work.path,
        params: prepared.params,
        query,
        body: admitted.body,
        headers: prepared.parts.headers,
        client_ip: work.client_ip,
        request_id: prepared.request_id,
        response_tx,
    });
    if let Err(e) = submitted {
        let error = match e {
            interp::SubmitError::Full => HandlerError::Overloaded("sub-interpreter work queue"),
            interp::SubmitError::Closed => HandlerError::PoolClosed("sub-interpreter pool"),
        };
        return fail(error, tag);
    }
    interp::WorkRequest::inc_created();

    let lost = |_| HandlerError::WorkerLost("the sub-interpreter worker dropped the request");
    match await_reply(response_rx, lost).await {
        Ok(result) => http_response(result, accept_encoding.as_str()),
        Err(e) => fail(e, tag),
    }
}
