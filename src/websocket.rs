//! PyronovaWebSocket support — async Tokio ↔ sync Python bridge via channels.

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use pyo3::prelude::*;
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message;

use crate::router::FrozenRoutes;

// ---------------------------------------------------------------------------
// Channel message type
// ---------------------------------------------------------------------------

enum WsMsg {
    Text(String),
    Binary(Vec<u8>),
}

// ---------------------------------------------------------------------------
// PyronovaWebSocket — Python-facing PyronovaWebSocket connection object
// ---------------------------------------------------------------------------

#[pyclass(name = "WebSocket")]
pub(crate) struct PyronovaWebSocket {
    // Bounded tokio channel so the hyper → Python path has TCP-level
    // backpressure: if the Python handler falls behind, the tokio
    // reader's `send().await` suspends, hyper stops reading the
    // socket, the kernel closes the receive window, the client slows
    // down. The previous `std::sync::mpsc::channel()` was unbounded
    // and turned that backpressure chain into an unbounded memory
    // sink — a single fast client could drive a multi-GB queue while
    // the Python handler ran a slow computation. 256 slots matches
    // OUTGOING_CAP order of magnitude and is small enough that a
    // stuck consumer notices immediately.
    incoming_rx: std::sync::Mutex<tokio::sync::mpsc::Receiver<WsMsg>>,
    outgoing_tx: std::sync::Mutex<Option<tokio::sync::mpsc::Sender<WsMsg>>>,
}

#[pymethods]
impl PyronovaWebSocket {
    /// Receive next text message. Returns None if connection closed.
    ///
    /// Releases the GIL while blocking on the channel so other Python
    /// threads (e.g. a second task reading from another ws, or the
    /// application's worker threads) are not frozen. Holding the GIL
    /// across a potentially unbounded channel wait is a single-threaded
    /// Python server in disguise.
    fn recv(&self, py: Python<'_>) -> Option<String> {
        py.detach(|| {
            // Recover from poisoning (a previous holder panicked) — channel
// data is intact, only the poison flag is set. Panicking across
// FFI into Python is UB per PyO3 (arc websocket-1).
let mut rx = self.incoming_rx.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                match rx.blocking_recv()? {
                    WsMsg::Text(s) => return Some(s),
                    WsMsg::Binary(_) => continue,
                }
            }
        })
    }

    /// Receive next binary message. Returns None if connection closed.
    /// Releases the GIL while waiting — see `recv` for rationale.
    fn recv_bytes(&self, py: Python<'_>) -> Option<Vec<u8>> {
        py.detach(|| {
            // Recover from poisoning (a previous holder panicked) — channel
// data is intact, only the poison flag is set. Panicking across
// FFI into Python is UB per PyO3 (arc websocket-1).
let mut rx = self.incoming_rx.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                match rx.blocking_recv()? {
                    WsMsg::Binary(b) => return Some(b),
                    WsMsg::Text(_) => continue,
                }
            }
        })
    }

    /// Receive next message as (type, data). type is "text" or "binary".
    /// Returns None if connection closed.
    fn recv_message<'py>(&self, py: Python<'py>) -> Option<(String, Py<PyAny>)> {
        // Release the GIL across the blocking recv; re-acquire to build the
        // Python-typed return value.
        let msg = py.detach(|| {
            // Recover from poisoning (a previous holder panicked) — channel
// data is intact, only the poison flag is set. Panicking across
// FFI into Python is UB per PyO3 (arc websocket-1).
let mut rx = self.incoming_rx.lock().unwrap_or_else(|e| e.into_inner());
            rx.blocking_recv()
        })?;
        match msg {
            WsMsg::Text(s) => Some((
                "text".to_string(),
                s.into_pyobject(py).unwrap().into_any().unbind(),
            )),
            WsMsg::Binary(b) => Some((
                "binary".to_string(),
                pyo3::types::PyBytes::new(py, &b).into_any().unbind(),
            )),
        }
    }

    /// Send a text message to the client.
    ///
    /// Uses try_send to stay sync — the outgoing channel is bounded, so a
    /// slow or disconnected client surfaces as BlockingIOError (buffer
    /// full) or ConnectionError (channel closed). Callers typically either
    /// pause / drop events or abort the connection.
    fn send(&self, msg: &str) -> PyResult<()> {
        self.try_send_outgoing(WsMsg::Text(msg.to_string()))
    }

    /// Send a binary message to the client. See `send` for semantics.
    fn send_bytes(&self, data: Vec<u8>) -> PyResult<()> {
        self.try_send_outgoing(WsMsg::Binary(data))
    }

    /// Close the PyronovaWebSocket connection.
    fn close(&self) {
        // Recover from poisoning — see recv() rationale (arc websocket-1).
        let mut tx = self.outgoing_tx.lock().unwrap_or_else(|e| e.into_inner());
        *tx = None;
    }
}

impl PyronovaWebSocket {
    fn try_send_outgoing(&self, msg: WsMsg) -> PyResult<()> {
        use tokio::sync::mpsc::error::TrySendError;
        // Convert poisoning to PyConnectionError — this method already
        // returns PyResult, so failure can flow back to Python normally
        // (arc websocket-1). Panicking across FFI is UB per PyO3.
        let guard = self.outgoing_tx.lock().map_err(|e| {
            pyo3::exceptions::PyConnectionError::new_err(format!(
                "PyronovaWebSocket outgoing mutex poisoned: {e}"
            ))
        })?;
        let tx = guard.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyConnectionError::new_err("PyronovaWebSocket closed")
        })?;
        match tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(pyo3::exceptions::PyBlockingIOError::new_err(
                "PyronovaWebSocket send buffer full (client is slow); retry after a brief pause",
            )),
            Err(TrySendError::Closed(_)) => Err(pyo3::exceptions::PyConnectionError::new_err(
                "PyronovaWebSocket closed",
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// PyronovaWebSocket upgrade detection
// ---------------------------------------------------------------------------

pub(crate) fn is_websocket_upgrade(req: &Request<Incoming>) -> bool {
    req.headers()
        .get("upgrade")
        .map(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"))
        .unwrap_or(false)
}

/// Build the 101 Switching Protocols response for PyronovaWebSocket upgrade.
fn ws_upgrade_response(key: &[u8]) -> Response<Full<Bytes>> {
    let accept = tungstenite::handshake::derive_accept_key(key);

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-accept", accept)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

// ---------------------------------------------------------------------------
// Handle PyronovaWebSocket upgrade + message pump
// ---------------------------------------------------------------------------

pub(crate) async fn handle_websocket(
    mut req: Request<Incoming>,
    routes: FrozenRoutes,
) -> Result<Response<crate::handlers::BoxBody>, hyper::Error> {
    let path = req.uri().path().to_string();

    // Look up PyronovaWebSocket handler (need GIL to clone Py<PyAny>)
    let handler = Python::attach(|py| routes.ws_handlers.get(&path).map(|h| h.clone_ref(py)));

    let handler = match handler {
        Some(h) => h,
        None => {
            return Ok(crate::handlers::full_body(
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from_static(b"no websocket handler")))
                    .unwrap(),
            ));
        }
    };

    // Extract the key for the handshake
    let key = match req.headers().get("sec-websocket-key") {
        Some(k) => k.as_bytes().to_vec(),
        None => {
            return Ok(crate::handlers::full_body(
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from_static(b"missing sec-websocket-key")))
                    .unwrap(),
            ));
        }
    };

    // Set up the upgrade
    let upgrade = hyper::upgrade::on(&mut req);

    // Spawn the PyronovaWebSocket handler task
    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let ws_stream = WebSocketStream::from_raw_socket(
                    hyper_util::rt::TokioIo::new(upgraded),
                    tungstenite::protocol::Role::Server,
                    None,
                )
                .await;

                run_ws_connection(ws_stream, handler).await;
            }
            Err(e) => {
                tracing::error!(target: "pyronova::server", error = %e, "PyronovaWebSocket upgrade error");
            }
        }
    });

    // Return the 101 response
    Ok(crate::handlers::full_body(ws_upgrade_response(&key)))
}

/// Run a PyronovaWebSocket connection — bridges async Tokio with sync Python handler.
async fn run_ws_connection<S>(ws_stream: WebSocketStream<S>, handler: Py<PyAny>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut ws_sink, mut ws_source) = ws_stream.split();

    // Bounded — see PyronovaWebSocket::incoming_rx docstring for why.
    const INCOMING_CAP: usize = 256;
    let (incoming_tx, incoming_rx) = tokio::sync::mpsc::channel::<WsMsg>(INCOMING_CAP);
    // Bounded outgoing: if the Python handler produces faster than the
    // client reads (TCP backpressure builds in ws_sink), we must stop
    // accepting new messages rather than buffer to OOM. `try_send` on
    // full raises ConnectionError to the Python sender, which can back
    // off or abort. 1024 matches the SSE stream default.
    const OUTGOING_CAP: usize = 1024;
    let (outgoing_tx, mut outgoing_rx) = tokio::sync::mpsc::channel::<WsMsg>(OUTGOING_CAP);

    let sky_ws = PyronovaWebSocket {
        incoming_rx: std::sync::Mutex::new(incoming_rx),
        outgoing_tx: std::sync::Mutex::new(Some(outgoing_tx)),
    };

    // Spawn a thread for the Python handler (blocks on ws.recv()).
    //
    // Contract: PyronovaWebSocket handlers live in the main interpreter (they
    // are registered via `@app.websocket(...)` at import time, which
    // runs in the main interp). `Python::attach` on a fresh OS thread
    // binds to the main interp's tstate, which matches. This is NOT a
    // PEP 684 cross-interp boundary — sub-interpreters aren't involved
    // for WS handlers in the current design.
    let py_handle = std::thread::spawn(move || {
        Python::attach(|py| {
            let ws_obj = match Py::new(py, sky_ws) {
                Ok(o) => o,
                Err(e) => {
                    tracing::error!(target: "pyronova::server", error = %e, "PyronovaWebSocket alloc failed");
                    // Drop handler under GIL before returning.
                    drop(handler);
                    return;
                }
            };
            match handler.call1(py, (ws_obj,)) {
                Ok(result) => {
                    // If the handler is `async def`, call1 returns a
                    // coroutine that must be driven. Detect via
                    // `asyncio.iscoroutine`; if so, run it on a fresh
                    // event loop. Otherwise drop the result.
                    // If the iscoroutine check fails (asyncio import,
                    // attribute lookup, or call), pre-fix this silently
                    // returned `false` and the coroutine was dropped
                    // un-awaited — handler appears to succeed but does
                    // nothing (arc finding websocket-3). Log so the
                    // silent no-op is observable.
                    let is_coro = match py
                        .import("asyncio")
                        .and_then(|m| m.getattr("iscoroutine"))
                        .and_then(|f| f.call1((&result,)))
                        .and_then(|r| r.extract::<bool>())
                    {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::warn!(
                                target: "pyronova::app",
                                error = %e,
                                "websocket handler: asyncio.iscoroutine check \
                                 failed; treating result as non-coroutine — \
                                 if the handler is async def its body will \
                                 not have run"
                            );
                            false
                        }
                    };
                    if is_coro {
                        if let Err(e) = py
                            .import("asyncio")
                            .and_then(|m| m.getattr("run"))
                            .and_then(|f| f.call1((&result,)))
                        {
                            tracing::error!(
                                target: "pyronova::server",
                                error = %e,
                                "PyronovaWebSocket async handler error",
                            );
                        }
                    }
                    // `result` drops here under GIL — safe.
                    drop(result);
                }
                Err(e) => {
                    tracing::error!(target: "pyronova::server", error = %e, "PyronovaWebSocket handler error");
                }
            }
            // Drop handler explicitly under GIL. Without this, the
            // Py<PyAny> would be dropped after the `attach` scope
            // closes, triggering PyO3's GIL-less pending-drop path —
            // harmless for ref counting but avoids the indirection.
            drop(handler);
        });
    });

    // Message pump: forward between PyronovaWebSocket and Python channels
    loop {
        tokio::select! {
            // Client → Python (via incoming_tx). `send().await` applies
            // backpressure: if Python's handler is slow and the channel
            // fills, the await suspends, which stops this select arm
            // from re-polling ws_source, which lets hyper's receive
            // buffer fill, which closes the TCP window — flow control
            // reaches all the way to the wire.
            msg = ws_source.next() => {
                match msg {
                    // clippy::collapsible_match (added in Rust 1.95) wants
                    // the inner `if .is_err() { break }` rewritten as a
                    // match guard. That works but requires duplicating the
                    // arm pattern (one with the side-effect-bearing guard,
                    // one bare to swallow the success case) — uglier than
                    // the original nested if. Keep the readable form.
                    #[allow(clippy::collapsible_match)]
                    Some(Ok(Message::Text(text))) => {
                        if incoming_tx.send(WsMsg::Text(text.to_string())).await.is_err() {
                            break;
                        }
                    }
                    #[allow(clippy::collapsible_match)]
                    Some(Ok(Message::Binary(data))) => {
                        if incoming_tx.send(WsMsg::Binary(data.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if ws_sink.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        tracing::warn!(target: "pyronova::server", error = %e, "PyronovaWebSocket read error");
                        break;
                    }
                    _ => {} // Pong
                }
            }
            // Python → Client (via outgoing_rx)
            msg = outgoing_rx.recv() => {
                match msg {
                    Some(WsMsg::Text(text)) => {
                        if ws_sink.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(WsMsg::Binary(data)) => {
                        if ws_sink.send(Message::Binary(data.into())).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }

    // Drop incoming_tx to unblock Python's ws.recv() → returns None
    drop(incoming_tx);

    // Close PyronovaWebSocket
    let _ = ws_sink.close().await;

    // Wait for Python handler thread. `JoinHandle::join()` blocks, so
    // calling it directly from this async fn would pin a Tokio worker
    // thread until the Python handler finishes — a trivial DoS vector
    // when a handler hangs. Dispatch to the blocking-thread pool so
    // async workers stay free.
    let _ = tokio::task::spawn_blocking(move || py_handle.join()).await;
}
