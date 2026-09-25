//! Per-connection drivers: every path that feeds a hyper connection converges here.
//!
//! - [`drive_connection`] runs one HTTP/1+2 connection to completion on any executor —
//!   the multi-thread pool paths (`TokioExecutor`) and the TPC threads ([`LocalExec`]) —
//!   with the Slowloris header-read timeout and graceful drain on shutdown.
//! - [`drive_conn`] / [`drive_tcp_conn`] serve a TPC sub-interpreter thread's
//!   connections through its [`TpcContext`]: real TCP (after the TLS handshake), and the
//!   in-memory bench's duplex streams. One hot path, so a bench number is the production
//!   per-request cost.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::{Builder as AutoBuilder, HttpServerConnExec};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use crate::bridge::main_bridge::MainInterpBridge;
use crate::handlers::{handle_request_tpc_inline, BoxBody};
use crate::python::interp::SubInterpreterWorker;
use crate::server::listener::Accepted;
use crate::site::SharedSite;
use crate::websocket;

/// How long a client gets to finish sending a request's headers (HTTP/1). Without it a
/// client that dribbles one header byte per minute holds a task and an fd forever.
/// HTTP/2 has its own frame/settings timeouts in the h2 crate.
pub(crate) const HEADER_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// LocalSet-compatible hyper executor. `spawn_local` means the
/// spawned future doesn't need `Send` — which is the whole point of
/// TPC (one OS thread, no work stealing, `Rc<RefCell<_>>` handler
/// state).
#[derive(Clone, Copy)]
pub(crate) struct LocalExec;

impl<F> hyper::rt::Executor<F> for LocalExec
where
    F: std::future::Future + 'static,
    F::Output: 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn_local(fut);
    }
}

/// What one TPC sub-interpreter thread serves with: its worker, the site, the
/// main-interpreter bridge. Shared (`Rc`) by the thread's connection tasks; never leaves
/// the thread. One non-atomic `Rc` clone per request.
pub(crate) struct TpcContext {
    pub(crate) worker: RefCell<SubInterpreterWorker>,
    pub(crate) site: SharedSite,
    pub(crate) bridge: Option<Arc<MainInterpBridge>>,
}

impl TpcContext {
    /// Ends the worker on this, its own, thread once every connection task holding the
    /// context is gone (FR-19). A context still shared is leaked, with an error.
    pub(crate) fn end(context: Rc<TpcContext>) {
        match Rc::try_unwrap(context) {
            Ok(context) => context.worker.into_inner().end_served(),
            Err(still_shared) => tracing::error!(
                target: "pyronova::server",
                worker = still_shared.worker.borrow().worker_id,
                "a worker is still referenced after its thread's runtime ended; leaking its \
                 interpreter"
            ),
        }
    }
}

/// Classify a connection-driver error as a benign client disconnect:
/// the peer closed/reset/aborted the socket, or hyper saw a partial
/// message because the peer went away. These are normal under load
/// and shouldn't be logged as server errors.
///
/// Walks the error source chain and matches on the typed
/// `hyper::Error` predicates and `io::ErrorKind` rather than
/// substring-matching the `Display` text. Substring matching is
/// fragile across hyper/dependency versions and, worse, suppresses
/// unrelated compound errors that merely contain one of the magic
/// phrases (e.g. `"TLS handshake failed: connection reset by peer"`).
fn is_benign_disconnect(err: &(dyn std::error::Error + 'static)) -> bool {
    use std::io::ErrorKind;
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = source {
        if let Some(hyper_err) = e.downcast_ref::<hyper::Error>() {
            if hyper_err.is_incomplete_message()
                || hyper_err.is_closed()
                || hyper_err.is_canceled()
                || hyper_err.is_body_write_aborted()
            {
                return true;
            }
        }
        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
            if matches!(
                io_err.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
            ) {
                return true;
            }
        }
        source = e.source();
    }
    false
}

/// Serves one HTTP/1+2 connection (with upgrades, for WebSocket) until it ends. When
/// `conn_token` is cancelled, hyper stops taking new requests on it and the in-flight
/// ones drain. A disconnect by the client is not an error; anything else is logged.
pub(crate) async fn drive_connection<IO, S, E>(
    io: IO,
    svc: S,
    exec: E,
    conn_token: CancellationToken,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: hyper::service::Service<
        Request<Incoming>,
        Response = hyper::Response<BoxBody>,
        Error = hyper::Error,
    >,
    S::Future: 'static,
    E: HttpServerConnExec<S::Future, BoxBody>,
{
    let mut builder = AutoBuilder::new(exec);
    builder
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT);
    let conn = builder.serve_connection_with_upgrades(TokioIo::new(io), svc);
    tokio::pin!(conn);
    let mut graceful_sent = false;
    loop {
        tokio::select! {
            res = conn.as_mut() => {
                if let Err(e) = res {
                    if !is_benign_disconnect(e.as_ref()) {
                        tracing::warn!(target: "pyronova::server", error = %e, "Connection error");
                    }
                }
                break;
            }
            _ = conn_token.cancelled(), if !graceful_sent => {
                conn.as_mut().graceful_shutdown();
                graceful_sent = true;
            }
        }
    }
}

/// Whether a TPC connection answers WebSocket upgrades: production does, the in-memory
/// bench (no upgrades to serve) doesn't, so its per-request closure has no WS branch.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Upgrades {
    WebSocket,
    #[cfg(feature = "bench")]
    Off,
}

/// Serves one connection of a TPC sub-interpreter thread: worker routes run inline on
/// `context`'s worker, main-interpreter calls go to its bridge.
pub(crate) async fn drive_conn<IO>(
    io: IO,
    remote_addr: std::net::IpAddr,
    context: Rc<TpcContext>,
    upgrades: Upgrades,
    conn_token: CancellationToken,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Split by upgrade support at connection start so the per-request closure is fully
    // specialized: no runtime `if upgrades` branch on the bench path.
    match upgrades {
        Upgrades::WebSocket => {
            let svc = service_fn(move |req: Request<Incoming>| {
                let context = Rc::clone(&context);
                async move {
                    if websocket::is_websocket_upgrade(&req) {
                        let site = Arc::clone(&context.site);
                        websocket::handle_websocket(req, site, remote_addr).await
                    } else {
                        handle_request_tpc_inline(req, context, remote_addr).await
                    }
                }
            });
            drive_connection(io, svc, LocalExec, conn_token).await;
        }
        #[cfg(feature = "bench")]
        Upgrades::Off => {
            let svc = service_fn(move |req: Request<Incoming>| {
                handle_request_tpc_inline(req, Rc::clone(&context), remote_addr)
            });
            drive_connection(io, svc, LocalExec, conn_token).await;
        }
    }
}

/// TCP adapter: TLS handshake (if the listener has an acceptor), then the generic
/// driver. Keeps the TLS decision at the transport boundary.
pub(crate) async fn drive_tcp_conn(
    accepted: Accepted,
    context: Rc<TpcContext>,
    conn_token: CancellationToken,
) {
    let Some(stream) = crate::tls::wrap(accepted.stream, accepted.tls.as_deref()).await else {
        return;
    };
    drive_conn(
        stream,
        accepted.remote.ip(),
        context,
        Upgrades::WebSocket,
        conn_token,
    )
    .await;
}
