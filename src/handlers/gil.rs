//! Main-interpreter dispatch from a Tokio thread.
//!
//! GIL mode (`mode="gil"`, TPC or not) runs every handler this way; the sub-interpreter
//! pool runs its `gil=True` routes and the fallback this way too ([`run_on_main`]).

use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::router::{Call, RequestBody, Target};
use crate::site::SharedSite;
use crate::types::PyronovaRequest;

use super::error::{HandlerError, RequestTag};
use super::pipeline::{
    await_reply, await_streamed_reply, collect_body, fail, finish, preprocess, task_lost, Prepared,
    Preprocessed, RequestLine, Served,
};
use super::{
    build_main_http_response, call_handler_with_hooks, max_body_size, stream_body_feeder, BoxBody,
};

pub(crate) async fn handle_request(
    req: Request<Incoming>,
    site: SharedSite,
    client_ip_addr: std::net::IpAddr,
) -> Result<Response<BoxBody>, hyper::Error> {
    let prepared = match preprocess(req, &site).await? {
        Preprocessed::Respond(r) => return Ok(r),
        Preprocessed::Dispatch(p) => p,
    };
    // In GIL mode every handler runs on main; a worker route's body is buffered.
    let (target, body) = match prepared.call {
        Call::Worker(id, _) => (Target::Route(id), RequestBody::Buffered),
        Call::Main(target, body) => (target, body),
    };
    Ok(run_on_main(&site, prepared, target, body, client_ip_addr, Served::Main).await)
}

/// Runs `target`'s handler on the main interpreter from a blocking-pool thread, within the
/// request budget, and finishes the response.
pub(crate) async fn run_on_main(
    site: &SharedSite,
    prepared: Prepared,
    target: Target,
    body: RequestBody,
    client_ip_addr: std::net::IpAddr,
    served: Served,
) -> Response<BoxBody> {
    // The `Request` takes the method, path, headers and id; the log line and the response
    // keep their own copies.
    let method: Arc<str> = Arc::from(prepared.parts.method.as_str());
    let path: Arc<str> = Arc::from(prepared.parts.uri.path());
    let id = prepared.request_id.clone();
    let accept_encoding = prepared.accept_encoding();
    let line = RequestLine {
        method: &method,
        path: &path,
        start: prepared.start,
    };
    let tag = RequestTag {
        id: &id,
        method: &method,
        path: &path,
    };

    let resp = match main_request(prepared, &method, &path, body, client_ip_addr).await {
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
                Ok(result) => build_main_http_response(result, accept_encoding.as_str()),
                Err(e) => fail(e, &tag),
            }
        }
        Err(e) => fail(e, &tag),
    };
    finish(resp, site, &line, served)
}

/// The `Request` a main-interpreter handler gets: the body collected, or fed through
/// `req.stream` for a `stream=True` route (with the task feeding it).
async fn main_request(
    prepared: Prepared,
    method: &Arc<str>,
    path: &Arc<str>,
    body: RequestBody,
    client_ip_addr: std::net::IpAddr,
) -> Result<(PyronovaRequest, Option<tokio::task::JoinHandle<()>>), HandlerError> {
    let query = prepared.query().to_owned();
    let (body_bytes, body_stream_rx, feeder) = match body {
        RequestBody::Streamed => {
            let (tx, rx) = tokio::sync::mpsc::channel(crate::python::body_stream::CHANNEL_CAPACITY);
            let feeder = tokio::spawn(stream_body_feeder(prepared.body, tx, max_body_size()));
            (
                Bytes::new(),
                Arc::new(std::sync::Mutex::new(Some(rx))),
                Some(feeder),
            )
        }
        RequestBody::Buffered => (
            collect_body(prepared.body, max_body_size()).await?,
            crate::python::body_stream::empty_body_stream_rx(),
            None,
        ),
    };
    let request = PyronovaRequest {
        method: Arc::clone(method),
        path: Arc::clone(path),
        params: prepared.params,
        query,
        headers: prepared.parts.headers,
        client_ip_addr,
        request_id: prepared.request_id,
        body_bytes,
        body_stream_rx,
        query_cache: std::sync::OnceLock::new(),
        query_all_cache: std::sync::OnceLock::new(),
    };
    Ok((request, feeder))
}
