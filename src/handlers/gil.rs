//! Main-interpreter dispatch from a Tokio thread.
//!
//! GIL mode (`mode="gil"`, TPC or not) runs every handler this way; the sub-interpreter
//! pool runs its `gil=True` routes and the fallback this way too ([`run_on_main`]).

use std::sync::Arc;

use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::body::{body_channel, stream_body_feeder, BoxBody};
use crate::error::HandlerError;
use crate::request_head::{Body, RequestHead};
use crate::router::{Call, RequestBody, Target};
use crate::site::SharedSite;
use crate::types::PyronovaRequest;

use super::pipeline::{
    await_reply, await_streamed_reply, collect_body, fail, finish, preprocess, task_lost,
    AcceptEncoding, Prepared, Preprocessed, RequestLine, Served,
};
use super::{build_main_http_response, call_handler_with_hooks};

pub(crate) async fn handle_request(
    req: Request<Incoming>,
    site: SharedSite,
    client_ip_addr: std::net::IpAddr,
) -> Result<Response<BoxBody>, hyper::Error> {
    let prepared = match preprocess(req, &site, client_ip_addr).await? {
        Preprocessed::Respond(r) => return Ok(r),
        Preprocessed::Dispatch(p) => p,
    };
    // In GIL mode every handler runs on main; a worker route's body is buffered.
    let (target, body) = match prepared.call {
        Call::Worker(id, _) => (Target::Route(id), RequestBody::Buffered),
        Call::Main(target, body) => (target, body),
    };
    Ok(run_on_main(&site, prepared, target, body, Served::Main).await)
}

/// Runs `target`'s handler on the main interpreter from a blocking-pool thread, within the
/// request budget, and finishes the response.
pub(crate) async fn run_on_main(
    site: &SharedSite,
    prepared: Prepared,
    target: Target,
    body: RequestBody,
    served: Served,
) -> Response<BoxBody> {
    let Prepared {
        head,
        body: incoming,
        start,
        ..
    } = prepared;
    // The `Request` takes the head; the log lines and the response keep their own copies.
    let label = head.label();
    let accept_encoding = AcceptEncoding::of(&head);
    let tag = label.tag();

    let max_body = site.config.limits.max_body_bytes;
    let resp = match main_request(head, incoming, body, max_body).await {
        Ok((sky_req, feeder)) => {
            let site_ref = Arc::clone(site);
            let task = tokio::task::spawn_blocking(move || {
                call_handler_with_hooks(&site_ref, target, sky_req)
            });
            let reply = match feeder {
                Some(mut feeder) => await_streamed_reply(&mut feeder, task, task_lost).await,
                None => await_reply(task, task_lost).await,
            };
            match reply {
                Ok(result) => build_main_http_response(result, &accept_encoding),
                Err(e) => fail(e, &tag),
            }
        }
        Err(e) => fail(e, &tag),
    };
    finish(resp, site, &RequestLine::of(&label, start), served)
}

/// The `Request` a main-interpreter handler gets: the body collected, or fed through
/// `req.stream` for a `stream=True` route (with the task feeding it).
async fn main_request(
    head: RequestHead,
    incoming: Incoming,
    body: RequestBody,
    max_body: usize,
) -> Result<(PyronovaRequest, Option<tokio::task::JoinHandle<()>>), HandlerError> {
    let (body, feeder) = match body {
        RequestBody::Streamed => {
            let (tx, rx) = body_channel();
            let feeder = tokio::spawn(stream_body_feeder(incoming, tx, max_body));
            (Body::Streamed(rx), Some(feeder))
        }
        RequestBody::Buffered => (
            Body::Buffered(collect_body(incoming, max_body).await?),
            None,
        ),
    };
    Ok((PyronovaRequest::new(head, body), feeder))
}
