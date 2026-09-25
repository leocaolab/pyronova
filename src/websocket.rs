//! PyronovaWebSocket support — async Tokio ↔ sync Python bridge via channels.
//!
//! Every resource a client can make the server hold is bounded: connections (each
//! holds one OS thread for its Python handler) by `max_connections`, message and frame
//! size by `max_message_bytes`, and the bytes queued in each direction of a connection
//! by a per-connection byte budget.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::HeaderValue;
use hyper::http::request::Parts;
use hyper::{Request, Response, StatusCode};
use parking_lot::Mutex;
use pyo3::exceptions::{PyBlockingIOError, PyConnectionError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::WebSocketStream;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tungstenite::Message;

use crate::body::{full_body, BoxBody};
use crate::error::{
    refuse, HandlerError, Logged, PyException, Refusal, RequestLabel, RequestTag, Stage,
};
use crate::handlers::pipeline::{await_reply, fail, finish, AcceptEncoding, RequestLine, Served};
use crate::handlers::{http_response, run_before_hooks};
use crate::request_head::{Body, RequestHead};
use crate::request_id::RequestId;
use crate::site::{SharedSite, Site};
use crate::types::{PyronovaRequest, ResponseData};

// ---------------------------------------------------------------------------
// Limits (per app: `site::Limits`)
// ---------------------------------------------------------------------------

/// Largest message (and frame) accepted from a client or queued by a handler, in bytes.
/// tungstenite's own default is 64 MiB per message; 1 MiB matches the `websockets` library.
const DEFAULT_MAX_MESSAGE_BYTES: u32 = 1024 * 1024;
/// Concurrent WebSocket connections. Each holds one OS thread for its Python handler.
const DEFAULT_MAX_CONNECTIONS: usize = 1024;
/// Bytes charged per queued message on top of its payload, for the allocation and
/// queue slot it occupies, so a flood of empty messages is bounded too.
const MESSAGE_OVERHEAD_BYTES: u32 = 64;
/// A budget of `max_message_bytes + MESSAGE_OVERHEAD_BYTES` must fit the `u32` permit
/// count a semaphore acquire takes.
const MAX_MESSAGE_BYTES_LIMIT: u32 = u32::MAX - MESSAGE_OVERHEAD_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WsLimits {
    pub(crate) max_message_bytes: u32,
    pub(crate) max_connections: usize,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WsLimitError {
    #[error(
        "max_websocket_message_size must be between 1 and {MAX_MESSAGE_BYTES_LIMIT} bytes, got {0}"
    )]
    MessageSize(i64),
    #[error("max_websocket_connections must be at least 1, got {0}")]
    Connections(i64),
}

impl From<WsLimitError> for PyErr {
    fn from(e: WsLimitError) -> Self {
        PyValueError::new_err(e.to_string())
    }
}

impl WsLimits {
    pub(crate) const DEFAULT: Self = Self {
        max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        max_connections: DEFAULT_MAX_CONNECTIONS,
    };

    pub(crate) fn with_max_message_bytes(self, bytes: i64) -> Result<Self, WsLimitError> {
        let max_message_bytes = u32::try_from(bytes)
            .ok()
            .filter(|b| (1..=MAX_MESSAGE_BYTES_LIMIT).contains(b))
            .ok_or(WsLimitError::MessageSize(bytes))?;
        Ok(Self {
            max_message_bytes,
            ..self
        })
    }

    pub(crate) fn with_max_connections(self, connections: i64) -> Result<Self, WsLimitError> {
        let max_connections = usize::try_from(connections)
            .ok()
            .filter(|c| *c >= 1)
            .ok_or(WsLimitError::Connections(connections))?;
        Ok(Self {
            max_connections,
            ..self
        })
    }

    fn tungstenite_config(self) -> WebSocketConfig {
        let max = Some(self.max_message_bytes as usize);
        WebSocketConfig::default()
            .max_message_size(max)
            .max_frame_size(max)
    }
}

/// The WebSocket connections one server run has open (`SiteConfig::ws_connections`).
#[derive(Clone, Default)]
pub(crate) struct OpenConnections(Arc<AtomicUsize>);

/// One of the `max_connections` slots, held for the connection's whole life (until its
/// Python handler thread has been joined) and released on drop.
struct ConnectionSlot(Arc<AtomicUsize>);

impl ConnectionSlot {
    fn try_acquire(open: &OpenConnections, max_connections: usize) -> Option<Self> {
        open.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < max_connections).then_some(n + 1)
            })
            .ok()
            .map(|_| Self(Arc::clone(&open.0)))
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Bytes queued in one direction of one connection. A message holds its share until it
/// leaves the queue. The budget fits one maximum-size message, so every allowed
/// message can always be queued once the queue drains.
#[derive(Clone)]
struct ByteBudget(Arc<Semaphore>);

impl ByteBudget {
    fn new(max_message_bytes: u32) -> Self {
        let permits = max_message_bytes + MESSAGE_OVERHEAD_BYTES;
        Self(Arc::new(Semaphore::new(permits as usize)))
    }

    /// `len` never exceeds `max_message_bytes`: tungstenite enforces it on input and
    /// `send` checks it on output, so the sum cannot overflow `MAX_MESSAGE_BYTES_LIMIT`.
    fn cost(len: usize) -> u32 {
        len as u32 + MESSAGE_OVERHEAD_BYTES
    }

    async fn reserve(&self, len: usize) -> Option<OwnedSemaphorePermit> {
        self.0
            .clone()
            .acquire_many_owned(Self::cost(len))
            .await
            .ok()
    }

    fn try_reserve(&self, len: usize) -> Option<OwnedSemaphorePermit> {
        self.0.clone().try_acquire_many_owned(Self::cost(len)).ok()
    }
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

enum WsMsg {
    Text(String),
    Binary(Vec<u8>),
}

impl WsMsg {
    fn kind(&self) -> &'static str {
        match self {
            Self::Text(_) => "text",
            Self::Binary(_) => "binary",
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Text(s) => s.len(),
            Self::Binary(b) => b.len(),
        }
    }

    fn into_text(self) -> Result<String, Self> {
        match self {
            Self::Text(s) => Ok(s),
            other => Err(other),
        }
    }

    fn into_binary(self) -> Result<Vec<u8>, Self> {
        match self {
            Self::Binary(b) => Ok(b),
            other => Err(other),
        }
    }

    fn into_message(self) -> Message {
        match self {
            Self::Text(s) => Message::Text(s.into()),
            Self::Binary(b) => Message::Binary(b.into()),
        }
    }
}

/// A message in a connection queue, holding its share of that direction's byte budget.
struct Queued {
    msg: WsMsg,
    _budget: OwnedSemaphorePermit,
}

/// `recv()` / `recv_bytes()` found the other kind of message at the head of the queue.
/// The message stays queued; the error says how to read it.
struct KindMismatch {
    method: &'static str,
    got: &'static str,
    len: usize,
}

impl From<KindMismatch> for PyErr {
    fn from(m: KindMismatch) -> Self {
        PyTypeError::new_err(format!(
            "{}() found a {} message ({} bytes) next; it is still queued: read it with \
             recv_message(), which returns str or bytes",
            m.method, m.got, m.len
        ))
    }
}

struct Inbox {
    rx: UnboundedReceiver<Queued>,
    /// A message a typed receive refused, returned first by the next receive.
    held: Option<WsMsg>,
}

impl Inbox {
    /// The next message converted by `extract`, or `None` once the peer has closed. A
    /// message `extract` hands back stays at the head of the queue.
    fn next_as<T>(
        &mut self,
        method: &'static str,
        extract: impl FnOnce(WsMsg) -> Result<T, WsMsg>,
    ) -> Result<Option<T>, KindMismatch> {
        let Some(msg) = self
            .held
            .take()
            .or_else(|| self.rx.blocking_recv().map(|q| q.msg))
        else {
            return Ok(None);
        };
        extract(msg).map(Some).map_err(|msg| {
            let mismatch = KindMismatch {
                method,
                got: msg.kind(),
                len: msg.len(),
            };
            self.held = Some(msg);
            mismatch
        })
    }
}

struct Outbox {
    tx: UnboundedSender<Queued>,
    budget: ByteBudget,
    max_message_bytes: u32,
}

impl Outbox {
    fn push(&self, msg: WsMsg) -> PyResult<()> {
        if msg.len() > self.max_message_bytes as usize {
            return Err(PyValueError::new_err(format!(
                "{} message of {} bytes exceeds max_websocket_message_size ({} bytes)",
                msg.kind(),
                msg.len(),
                self.max_message_bytes
            )));
        }
        let budget = self.budget.try_reserve(msg.len()).ok_or_else(|| {
            PyBlockingIOError::new_err(
                "WebSocket send buffer full (client is slow); retry after a brief pause",
            )
        })?;
        self.tx
            .send(Queued {
                msg,
                _budget: budget,
            })
            .map_err(|_| PyConnectionError::new_err("WebSocket closed"))
    }
}

// ---------------------------------------------------------------------------
// PyronovaWebSocket — the Python-facing connection object
// ---------------------------------------------------------------------------

#[pyclass(name = "WebSocket", module = "pyronova.engine")]
pub(crate) struct PyronovaWebSocket {
    inbox: Mutex<Inbox>,
    outbox: Mutex<Option<Outbox>>,
    request: Py<PyronovaRequest>,
}

#[pymethods]
impl PyronovaWebSocket {
    /// The upgrade request: method, path, query, headers (Origin, cookies), client IP. The
    /// same object the `before_request` hooks saw.
    #[getter]
    fn request(&self, py: Python<'_>) -> Py<PyronovaRequest> {
        self.request.clone_ref(py)
    }

    /// Receive the next message as `str` (text) or `bytes` (binary); `None` once the
    /// connection is closed.
    ///
    /// Releases the GIL while blocking on the queue so other Python threads are not
    /// frozen for an unbounded wait.
    fn recv_message<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let msg = py.detach(|| self.inbox.lock().next_as("recv_message", Ok))?;
        Ok(msg.map(|msg| match msg {
            WsMsg::Text(s) => PyString::new(py, &s).into_any(),
            WsMsg::Binary(b) => PyBytes::new(py, &b).into_any(),
        }))
    }

    /// Receive the next text message; `None` once the connection is closed. Raises
    /// `TypeError` if the next message is binary, leaving it queued.
    fn recv(&self, py: Python<'_>) -> PyResult<Option<String>> {
        Ok(py.detach(|| self.inbox.lock().next_as("recv", WsMsg::into_text))?)
    }

    /// Receive the next binary message; `None` once the connection is closed. Raises
    /// `TypeError` if the next message is text, leaving it queued.
    fn recv_bytes<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let data = py.detach(|| self.inbox.lock().next_as("recv_bytes", WsMsg::into_binary))?;
        Ok(data.map(|b| PyBytes::new(py, &b)))
    }

    /// Send a text message to the client.
    ///
    /// Non-blocking: a message over `max_websocket_message_size` raises `ValueError`;
    /// a full send buffer (slow client) raises `BlockingIOError`; a closed connection
    /// raises `ConnectionError`.
    fn send(&self, msg: &str) -> PyResult<()> {
        self.push(WsMsg::Text(msg.to_string()))
    }

    /// Send a binary message to the client. See `send` for semantics.
    fn send_bytes(&self, data: Vec<u8>) -> PyResult<()> {
        self.push(WsMsg::Binary(data))
    }

    /// Close the connection.
    fn close(&self) {
        *self.outbox.lock() = None;
    }
}

impl PyronovaWebSocket {
    fn push(&self, msg: WsMsg) -> PyResult<()> {
        self.outbox
            .lock()
            .as_ref()
            .ok_or_else(|| PyConnectionError::new_err("WebSocket closed"))?
            .push(msg)
    }
}

// ---------------------------------------------------------------------------
// Upgrade
// ---------------------------------------------------------------------------

/// Whether `req` asks for a WebSocket on one of the site's WebSocket routes. Any other
/// request — `Upgrade: websocket` on an ordinary route included — is routed normally.
pub(crate) fn wants_websocket(req: &Request<Incoming>, site: &Site) -> bool {
    let upgrade = req
        .headers()
        .get(hyper::header::UPGRADE)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"));
    upgrade && site.routes.ws_handlers.contains_key(req.uri().path())
}

/// The WebSocket protocol version this server speaks (RFC 6455).
const WS_VERSION: &str = "13";

/// A valid WebSocket opening handshake (RFC 6455 §4.2.1): checked before a connection
/// slot, a hook or a thread is spent on the request.
struct WsHandshake {
    key: HeaderValue,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum HandshakeError {
    #[error("a WebSocket upgrade needs HTTP/1.1, got {0:?}")]
    HttpVersion(hyper::Version),
    #[error("a WebSocket upgrade must be a GET, got {0}")]
    Method(hyper::Method),
    #[error("a WebSocket upgrade needs `Connection: upgrade`")]
    Connection,
    #[error("a WebSocket upgrade needs a Sec-WebSocket-Key")]
    MissingKey,
    #[error("this server speaks WebSocket version {WS_VERSION}")]
    WsVersion,
}

impl WsHandshake {
    fn parse(parts: &Parts) -> Result<Self, HandshakeError> {
        if parts.version != hyper::Version::HTTP_11 {
            return Err(HandshakeError::HttpVersion(parts.version));
        }
        if parts.method != hyper::Method::GET {
            return Err(HandshakeError::Method(parts.method.clone()));
        }
        let headers = &parts.headers;
        let connection_upgrade = headers
            .get_all(hyper::header::CONNECTION)
            .iter()
            .flat_map(|v| v.as_bytes().split(|&b| b == b','))
            .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"upgrade"));
        if !connection_upgrade {
            return Err(HandshakeError::Connection);
        }
        if headers
            .get(hyper::header::SEC_WEBSOCKET_VERSION)
            .is_none_or(|v| v.as_bytes() != WS_VERSION.as_bytes())
        {
            return Err(HandshakeError::WsVersion);
        }
        let key = headers
            .get(hyper::header::SEC_WEBSOCKET_KEY)
            .filter(|k| !k.is_empty())
            .ok_or(HandshakeError::MissingKey)?;
        Ok(WsHandshake { key: key.clone() })
    }
}

impl HandshakeError {
    /// 426 naming the version this server speaks for an unsupported version (RFC 6455
    /// §4.2.2), 400 with the reason for anything else.
    fn response(&self) -> Response<BoxBody> {
        let mut resp = Response::new(Full::new(Bytes::from(self.to_string())));
        if *self == HandshakeError::WsVersion {
            *resp.status_mut() = StatusCode::UPGRADE_REQUIRED;
            resp.headers_mut().insert(
                hyper::header::SEC_WEBSOCKET_VERSION,
                HeaderValue::from_static(WS_VERSION),
            );
        } else {
            *resp.status_mut() = StatusCode::BAD_REQUEST;
        }
        full_body(resp)
    }
}

/// Build the 101 Switching Protocols response for a WebSocket upgrade.
fn ws_upgrade_response(key: &HeaderValue) -> Response<BoxBody> {
    let accept = tungstenite::handshake::derive_accept_key(key.as_bytes());
    let mut resp = Response::new(Full::new(Bytes::new()));
    *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let headers = resp.headers_mut();
    headers.insert(
        hyper::header::UPGRADE,
        HeaderValue::from_static("websocket"),
    );
    headers.insert(
        hyper::header::CONNECTION,
        HeaderValue::from_static("Upgrade"),
    );
    // Base64 of a SHA-1: always a valid field value.
    if let Ok(accept) = HeaderValue::from_str(&accept) {
        headers.insert(hyper::header::SEC_WEBSOCKET_ACCEPT, accept);
    }
    full_body(resp)
}

/// Answers a WebSocket upgrade request: the 101, or the response that refused it. Either
/// is finished like any other response (CORS, access log).
pub(crate) async fn handle_websocket(
    mut req: Request<Incoming>,
    site: SharedSite,
    client_ip: std::net::IpAddr,
) -> Result<Response<BoxBody>, hyper::Error> {
    crate::monitor::count_request();
    let start = std::time::Instant::now();
    let upgrade = hyper::upgrade::on(&mut req);
    let (parts, _body) = req.into_parts();
    let request_id = RequestId::of(&parts.headers, site.config.request_id_header.as_ref());
    let head = RequestHead {
        parts,
        params: Vec::new(),
        client_ip,
        request_id,
    };
    let label = head.label();
    let resp = match WsHandshake::parse(&head.parts) {
        Ok(handshake) => answer_upgrade(&site, handshake, head, upgrade, &label).await,
        Err(e) => {
            tracing::debug!(target: "pyronova::server", request_id = %label.id,
                path = label.uri.path(), error = %e, "WebSocket handshake refused");
            e.response()
        }
    };
    Ok(finish(
        resp,
        &site,
        &RequestLine::of(&label, start),
        Served::WebSocket,
    ))
}

/// What the `before_request` hooks decided about an upgrade.
enum Verdict {
    Accept,
    /// A hook returned this response (or raised: `Err`, logged).
    Reject(Result<ResponseData, Logged>),
}

/// The connection's handler thread and the connection slot it holds: the slot is released
/// only once the thread has been joined, on every path (the 101, a hook's rejection, the
/// handshake giving up while a hook still runs).
struct HandlerThread {
    handle: std::thread::JoinHandle<()>,
    slot: ConnectionSlot,
}

impl HandlerThread {
    /// `JoinHandle::join()` blocks, so it runs on the blocking pool: a hung handler must
    /// not pin a Tokio worker thread.
    async fn join(self) {
        let HandlerThread { handle, slot } = self;
        match tokio::task::spawn_blocking(move || handle.join()).await {
            Ok(Ok(())) => {}
            Ok(Err(payload)) => {
                tracing::error!(target: "pyronova::server",
                    panic = %crate::error::panic_message(&*payload),
                    "WebSocket handler thread panicked");
            }
            Err(e) => {
                tracing::error!(target: "pyronova::server", error = %e,
                    "WebSocket handler thread join failed");
            }
        }
        drop(slot);
    }
}

async fn answer_upgrade(
    site: &SharedSite,
    handshake: WsHandshake,
    head: RequestHead,
    upgrade: hyper::upgrade::OnUpgrade,
    label: &RequestLabel,
) -> Response<BoxBody> {
    let tag = label.tag();
    let limits = site.config.limits.ws;
    let Some(slot) =
        ConnectionSlot::try_acquire(&site.config.ws_connections, limits.max_connections)
    else {
        tracing::debug!(target: "pyronova::server", path = tag.path, max_connections = limits.max_connections,
            "WebSocket connection limit reached; answered 503");
        return fail(refuse(Refusal::Overloaded("websocket connections")), &tag);
    };

    // The hooks run on the connection's thread before the 101; the handler runs there
    // after it, in the same request context. In TPC mode this function runs on a worker's
    // thread, bound to that worker's interpreter; the handler and hooks are main's, and run
    // on the connection's own thread, attached to main (Layer 2, C4).
    let (ends, handler_ends) = connection_channels(limits);
    let (verdict_tx, verdict_rx) = oneshot::channel();
    let accept_encoding = AcceptEncoding::of(&head);
    let request = PyronovaRequest::new(head, Body::Buffered(Bytes::new()));
    let handle = match spawn_handler_thread(Arc::clone(site), request, handler_ends, verdict_tx) {
        Ok(handle) => handle,
        Err(e) => return fail(HandlerError::ThreadSpawn(e), &tag),
    };
    let thread = HandlerThread { handle, slot };

    let lost = |_| {
        refuse(Refusal::WorkerLost(
            "the websocket handler thread exited before the upgrade",
        ))
    };
    match await_reply(verdict_rx, lost).await {
        Ok(Verdict::Accept) => {}
        Ok(Verdict::Reject(response)) => {
            thread.join().await;
            return http_response(response, &accept_encoding);
        }
        Err(e) => {
            // The thread sees the verdict go unread and exits without running the
            // handler; its slot is held until then.
            tokio::spawn(thread.join());
            return fail(e, &tag);
        }
    }

    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let ws_stream = WebSocketStream::from_raw_socket(
                    hyper_util::rt::TokioIo::new(upgraded),
                    tungstenite::protocol::Role::Server,
                    Some(limits.tungstenite_config()),
                )
                .await;
                run_ws_connection(ws_stream, ends, limits).await;
            }
            Err(e) => {
                tracing::error!(target: "pyronova::server", error = %e, "WebSocket upgrade error");
                // Dropping the ends makes the handler's recv() return None.
                drop(ends);
            }
        }
        thread.join().await;
    });

    ws_upgrade_response(&handshake.key)
}

// ---------------------------------------------------------------------------
// Connection: Python handler thread + message pump
// ---------------------------------------------------------------------------

/// The pump's ends of a connection's two queues.
struct ConnEnds {
    incoming: UnboundedSender<Queued>,
    incoming_budget: ByteBudget,
    outgoing: UnboundedReceiver<Queued>,
}

/// The handler's ends: what `recv()` reads and `send()` writes.
struct HandlerEnds {
    inbox: Inbox,
    outbox: Outbox,
}

fn connection_channels(limits: WsLimits) -> (ConnEnds, HandlerEnds) {
    let (incoming_tx, incoming_rx) = unbounded_channel::<Queued>();
    let (outgoing_tx, outgoing_rx) = unbounded_channel::<Queued>();
    (
        ConnEnds {
            incoming: incoming_tx,
            incoming_budget: ByteBudget::new(limits.max_message_bytes),
            outgoing: outgoing_rx,
        },
        HandlerEnds {
            inbox: Inbox {
                rx: incoming_rx,
                held: None,
            },
            outbox: Outbox {
                tx: outgoing_tx,
                budget: ByteBudget::new(limits.max_message_bytes),
                max_message_bytes: limits.max_message_bytes,
            },
        },
    )
}

/// The connection's OS thread, attached to main: the `before_request` hooks, then (if they
/// let the upgrade through) the handler, all in one request context.
///
/// Contract: WebSocket handlers and hooks live in the main interpreter. The thread has no
/// thread state, so it attaches to main explicitly, with one thread state for the
/// connection's life (Layer 2, C4). Not `main_attach`: this thread can outlive the server
/// run whose context `main_attach` reads. The site clone is dropped inside the attach.
fn spawn_handler_thread(
    site: SharedSite,
    request: PyronovaRequest,
    ends: HandlerEnds,
    verdict: oneshot::Sender<Verdict>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let main = crate::run_context::main_interp();
    std::thread::Builder::new()
        .name("pyronova-ws".to_string())
        .spawn(move || {
            crate::run_context::attach_to(main, move |py| {
                let served = crate::python::request_context::in_request_context(py, |rc| {
                    serve_connection(py, rc, &site, request, ends, verdict)
                });
                if let Err(e) = served {
                    tracing::error!(target: "pyronova::server", error = %e,
                        "WebSocket connection could not enter its contextvars.Context");
                }
                drop(site);
            });
        })
}

fn serve_connection(
    py: Python<'_>,
    rc: &crate::python::request_context::RequestContext<'_>,
    site: &Site,
    request: PyronovaRequest,
    ends: HandlerEnds,
    verdict: oneshot::Sender<Verdict>,
) {
    // Checked when the request was routed here; the table is frozen.
    let Some(handler) = site
        .routes
        .ws_handlers
        .get(request.uri.path())
        .map(|h| h.clone_ref(py))
    else {
        return;
    };
    // The request as its log lines name it; `request` moves into its `Request`.
    let label = request.label();
    let hooks = Py::new(py, request)
        .map_err(|e| HandlerError::python(py, Stage::Setup, &e))
        .and_then(|request| {
            run_before_hooks(py, rc, &site.routes.before_hooks, &request).map(|r| (request, r))
        });
    // A verdict goes unread only if the handshake already gave up.
    let request = match hooks {
        Ok((request, None)) => request,
        Ok((_, Some(response))) => {
            let _ = verdict.send(Verdict::Reject(Ok(response)));
            return;
        }
        Err(e) => {
            let _ = verdict.send(Verdict::Reject(Err(e.log(&label.tag()))));
            return;
        }
    };
    if verdict.send(Verdict::Accept).is_err() {
        // The handshake gave up waiting (request budget): no 101 was sent.
        return;
    }
    let ws = PyronovaWebSocket {
        inbox: Mutex::new(ends.inbox),
        outbox: Mutex::new(Some(ends.outbox)),
        request,
    };
    if let Err(e) = run_handler(py, &handler, ws) {
        log_handler_error(&e, &label.tag());
    }
    // Drop the handler under the GIL, not via PyO3's pending-drop path.
    drop(handler);
}

/// Why a WebSocket handler ended with an error. The connection was already upgraded, so
/// there is no response to render: the error is logged, with the request.
enum HandlerFailure {
    /// The handler (or the coroutine it returned) raised.
    Raised(PyException),
    /// Making the `WebSocket` object, or driving the returned coroutine, failed.
    Setup(PyException),
}

fn log_handler_error(failure: &HandlerFailure, tag: &RequestTag<'_>) {
    let (what, exception) = match failure {
        HandlerFailure::Raised(e) => ("WebSocket handler raised", e),
        HandlerFailure::Setup(e) => ("WebSocket handler could not be run", e),
    };
    tracing::error!(target: "pyronova::handler", request_id = %tag.id, method = tag.method,
        path = tag.path, error = %exception, traceback = exception.traceback(), "{what}");
}

fn run_handler(
    py: Python<'_>,
    handler: &Py<PyAny>,
    ws: PyronovaWebSocket,
) -> Result<(), HandlerFailure> {
    let setup = |e: PyErr| HandlerFailure::Setup(PyException::capture(py, &e));
    let raised = |e: PyErr| HandlerFailure::Raised(PyException::capture(py, &e));
    let ws_obj = Py::new(py, ws).map_err(setup)?;
    let result = handler.call1(py, (ws_obj,)).map_err(raised)?;
    // An `async def` handler returns a coroutine that must be driven on a fresh loop.
    let asyncio = py.import("asyncio").map_err(setup)?;
    let is_coro = asyncio
        .call_method1("iscoroutine", (&result,))
        .and_then(|b| b.extract::<bool>())
        .map_err(setup)?;
    if is_coro {
        asyncio.call_method1("run", (&result,)).map_err(raised)?;
    }
    Ok(())
}

async fn run_ws_connection<S>(ws_stream: WebSocketStream<S>, ends: ConnEnds, limits: WsLimits)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (ws_sink, ws_source) = ws_stream.split();
    let mut pump_ends = PumpEnds {
        sink: ws_sink,
        source: ws_source,
        incoming: ends.incoming,
        incoming_budget: ends.incoming_budget,
        outgoing: ends.outgoing,
    };
    let close_frame = pump(&mut pump_ends, limits).await;

    let PumpEnds {
        sink,
        source,
        incoming,
        ..
    } = pump_ends;
    // Dropping the sender makes Python's pending recv return None.
    drop(incoming);
    close_connection(sink, source, close_frame).await;
}

type WsSink<S> = SplitSink<WebSocketStream<S>, Message>;
type WsSource<S> = SplitStream<WebSocketStream<S>>;

struct PumpEnds<S> {
    sink: WsSink<S>,
    source: WsSource<S>,
    incoming: UnboundedSender<Queued>,
    incoming_budget: ByteBudget,
    outgoing: UnboundedReceiver<Queued>,
}

/// Move messages between the socket and Python until either side ends. Returns the
/// close frame the server owes the client, if the server is the one closing.
///
/// The channels are unbounded in count but every queued message holds its bytes from
/// a `ByteBudget`, so each direction is bounded in memory.
async fn pump<S>(ends: &mut PumpEnds<S>, limits: WsLimits) -> Option<CloseFrame>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            msg = ends.source.next() => {
                let queued = match msg {
                    Some(Ok(Message::Text(text))) => WsMsg::Text(text.to_string()),
                    Some(Ok(Message::Binary(data))) => WsMsg::Binary(data.into()),
                    // tungstenite queues the Pong for a Ping itself and flushes it with
                    // the next read or write.
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                    Some(Ok(Message::Close(_))) | None => return None,
                    Some(Err(tungstenite::Error::Capacity(e))) => {
                        tracing::warn!(target: "pyronova::server", error = %e,
                            max_message_bytes = limits.max_message_bytes,
                            "WebSocket message over max_websocket_message_size; closing with 1009");
                        return Some(CloseFrame { code: CloseCode::Size, reason: e.to_string().into() });
                    }
                    Some(Err(e)) => {
                        tracing::warn!(target: "pyronova::server", error = %e, "WebSocket read error");
                        return None;
                    }
                };
                if forward(&ends.incoming, &ends.incoming_budget, queued).await.is_break() {
                    return None;
                }
            }
            queued = ends.outgoing.recv() => {
                // The queue slot's budget is released once the message is on the wire.
                let Queued { msg, _budget } = queued?;
                if let Err(e) = ends.sink.send(msg.into_message()).await {
                    tracing::debug!(target: "pyronova::server", error = %e, "WebSocket send failed");
                    return None;
                }
            }
        }
    }
}

/// Queue a client message for Python. While the byte budget is spent this waits,
/// which stops this select arm from reading the socket: flow control reaches the
/// client through the TCP window.
async fn forward(tx: &UnboundedSender<Queued>, budget: &ByteBudget, msg: WsMsg) -> ControlFlow<()> {
    let Some(permit) = budget.reserve(msg.len()).await else {
        return ControlFlow::Break(());
    };
    match tx.send(Queued {
        msg,
        _budget: permit,
    }) {
        Ok(()) => ControlFlow::Continue(()),
        // The Python side is gone.
        Err(_) => ControlFlow::Break(()),
    }
}

/// How long a closing connection waits for the peer to close its side.
const CLOSE_LINGER: Duration = Duration::from_secs(2);
/// Scratch buffer for discarding input while lingering.
const LINGER_CHUNK_BYTES: usize = 8 * 1024;

/// Close so the client reliably sees the close frame: send it, flush, shut down our
/// write side, then read and discard until the peer closes (at most `CLOSE_LINGER`).
/// Dropping a socket that still has unread input — such as the rest of a message just
/// refused as too big — makes the kernel send RST, which can destroy the close frame
/// before the client reads it.
///
/// Failures here mean the peer is already gone, the normal end of many connections,
/// so they log at debug.
async fn close_connection<S>(mut sink: WsSink<S>, source: WsSource<S>, frame: Option<CloseFrame>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if let Some(frame) = frame {
        if let Err(e) = sink.send(Message::Close(Some(frame))).await {
            tracing::debug!(target: "pyronova::server", error = %e, "WebSocket close frame not sent");
        }
    }
    if let Err(e) = sink.close().await {
        tracing::debug!(target: "pyronova::server", error = %e, "WebSocket close failed");
    }

    let mut stream = match sink.reunite(source) {
        Ok(stream) => stream,
        Err(e) => {
            tracing::debug!(target: "pyronova::server", error = %e, "WebSocket halves did not reunite");
            return;
        }
    };
    let io = stream.get_mut();
    if let Err(e) = io.shutdown().await {
        tracing::debug!(target: "pyronova::server", error = %e, "WebSocket write shutdown failed");
    }
    if tokio::time::timeout(CLOSE_LINGER, discard_until_eof(io))
        .await
        .is_err()
    {
        tracing::debug!(target: "pyronova::server", linger = ?CLOSE_LINGER,
            "WebSocket peer did not close in time; dropping the connection");
    }
}

async fn discard_until_eof<R: tokio::io::AsyncRead + Unpin>(io: &mut R) {
    let mut scratch = [0u8; LINGER_CHUNK_BYTES];
    loop {
        // EOF or a read error both mean the peer has gone, which is what we wait for.
        match io.read(&mut scratch).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handshake_parts(
        edit: impl FnOnce(hyper::http::request::Builder) -> hyper::http::request::Builder,
    ) -> Parts {
        let valid = hyper::Request::builder()
            .method("GET")
            .uri("/ws")
            .version(hyper::Version::HTTP_11)
            .header("upgrade", "websocket")
            .header("connection", "keep-alive, Upgrade")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==");
        edit(valid).body(()).unwrap().into_parts().0
    }

    #[test]
    fn a_valid_handshake_parses() {
        let parsed = WsHandshake::parse(&handshake_parts(|b| b)).unwrap();
        assert_eq!(parsed.key, "dGhlIHNhbXBsZSBub25jZQ==");
    }

    #[test]
    fn an_invalid_handshake_is_refused_with_its_reason() {
        let refused = |edit: fn(hyper::http::request::Builder) -> hyper::http::request::Builder| {
            WsHandshake::parse(&handshake_parts(edit)).err().unwrap()
        };
        assert_eq!(
            refused(|b| b.method("POST")),
            HandshakeError::Method(hyper::Method::POST)
        );
        assert_eq!(
            refused(|b| b.version(hyper::Version::HTTP_10)),
            HandshakeError::HttpVersion(hyper::Version::HTTP_10)
        );
        assert_eq!(
            refused(|b| b.version(hyper::Version::HTTP_2)),
            HandshakeError::HttpVersion(hyper::Version::HTTP_2)
        );
        let mut no_connection = handshake_parts(|b| b);
        no_connection.headers.remove("connection");
        assert_eq!(
            WsHandshake::parse(&no_connection).err(),
            Some(HandshakeError::Connection)
        );
        let mut old_version = handshake_parts(|b| b);
        old_version
            .headers
            .insert("sec-websocket-version", HeaderValue::from_static("8"));
        assert_eq!(
            WsHandshake::parse(&old_version).err(),
            Some(HandshakeError::WsVersion)
        );
        let mut no_key = handshake_parts(|b| b);
        no_key.headers.remove("sec-websocket-key");
        assert_eq!(
            WsHandshake::parse(&no_key).err(),
            Some(HandshakeError::MissingKey)
        );
    }

    #[test]
    fn an_unsupported_version_is_426_naming_13_and_the_rest_400() {
        let resp = HandshakeError::WsVersion.response();
        assert_eq!(resp.status(), StatusCode::UPGRADE_REQUIRED);
        assert_eq!(resp.headers()["sec-websocket-version"], "13");
        let resp = HandshakeError::Connection.response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn limits_reject_out_of_range_values() {
        let l = WsLimits::DEFAULT;
        assert!(l.with_max_message_bytes(0).is_err());
        assert!(l.with_max_message_bytes(-1).is_err());
        assert!(l
            .with_max_message_bytes(MAX_MESSAGE_BYTES_LIMIT as i64 + 1)
            .is_err());
        assert_eq!(
            l.with_max_message_bytes(4096).unwrap().max_message_bytes,
            4096
        );
        assert!(l.with_max_connections(0).is_err());
        assert_eq!(l.with_max_connections(3).unwrap().max_connections, 3);
    }

    #[tokio::test]
    async fn byte_budget_counts_bytes_not_messages() {
        let budget = ByteBudget::new(1000);
        let big = budget.try_reserve(900).expect("fits");
        // 900 + 64 of 1064 used: a 100-byte message (164) no longer fits, a 0-byte one does.
        assert!(budget.try_reserve(100).is_none());
        let small = budget.try_reserve(0).expect("fits");
        drop(big);
        assert!(budget.try_reserve(900).is_some());
        drop(small);
    }

    #[test]
    fn a_max_size_message_always_fits_an_empty_budget() {
        let budget = ByteBudget::new(1000);
        assert!(budget.try_reserve(1000).is_some());
    }

    #[test]
    fn connection_slots_are_bounded_and_released() {
        // A fresh run's counter starts at 0.
        let open = OpenConnections::default();
        let first = ConnectionSlot::try_acquire(&open, 2).expect("slot 1");
        let second = ConnectionSlot::try_acquire(&open, 2).expect("slot 2");
        assert!(ConnectionSlot::try_acquire(&open, 2).is_none());
        drop(first);
        let third = ConnectionSlot::try_acquire(&open, 2).expect("released slot");
        drop((second, third));
    }

    #[test]
    fn typed_receive_keeps_the_other_kind_queued() {
        let (tx, rx) = unbounded_channel();
        let budget = ByteBudget::new(1000);
        for msg in [WsMsg::Binary(vec![1, 2, 3]), WsMsg::Text("hi".into())] {
            let permit = budget.try_reserve(msg.len()).unwrap();
            tx.send(Queued {
                msg,
                _budget: permit,
            })
            .unwrap();
        }
        let mut inbox = Inbox { rx, held: None };
        let mismatch = inbox.next_as("recv", WsMsg::into_text).err().unwrap();
        assert_eq!((mismatch.got, mismatch.len), ("binary", 3));
        assert_eq!(
            inbox
                .next_as("recv_bytes", WsMsg::into_binary)
                .ok()
                .flatten(),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            inbox.next_as("recv", WsMsg::into_text).ok().flatten(),
            Some("hi".to_string())
        );
        drop(tx);
        assert!(inbox
            .next_as("recv", WsMsg::into_text)
            .ok()
            .unwrap()
            .is_none());
    }
}
