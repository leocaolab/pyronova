//! Generic per-connection driver for the TPC inline path.
//!
//! Every path that feeds a hyper connection — real TCP accept loop,
//! in-memory duplex bench, loopback bench — converges here. The heavy
//! lifting is a single generic `drive_conn<IO>` that takes any
//! `AsyncRead + AsyncWrite`, wraps it in `TokioIo`, and runs the
//! hyper auto-builder on this worker's LocalSet.
//!
//! Why extract this out of `tpc.rs`: before this split, `tpc.rs` held
//! three near-identical copies of the same service_fn closure + drive
//! loop (`drive_inline_conn`, `drive_inmem_conn`, the GIL-only
//! variant in `drive_gil_conn`). Any hot-path optimization had to be
//! landed three times, and the bench path was a second source of
//! truth for "what a request costs". One generic function = one hot
//! path = measurements on the bench directly reflect the production
//! cost.
//!
//! The `drive_tcp_conn` wrapper below is just TLS-wrap + call. Other
//! transports (in-memory `DuplexStream`) call `drive_conn` directly.

use std::sync::Arc;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use crate::handlers::handle_request_tpc_inline;
use crate::python::interp::SubInterpreterWorker;
use crate::router::{FrozenRoutes, RouteTable};
use crate::websocket;

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

/// Generic connection driver.
///
/// - `io`: any `AsyncRead + AsyncWrite + Unpin + 'static + Send`
///   (TLS-wrapped TCP, plain TCP, duplex for bench).
/// - `routes`: leaked `&'static RouteTable` — zero atomic per request.
/// - `routes_for_ws`: `Some(arc)` enables WS upgrades (production
///   path); `None` for paths that don't need WS (bench). Stored in
///   the closure at connection scope; an Arc clone only fires inside
///   the `is_ws` branch, so the hot non-WS path is atomic-free.
pub(crate) async fn drive_conn<IO>(
    io: IO,
    remote_addr: std::net::IpAddr,
    worker: std::rc::Rc<std::cell::RefCell<SubInterpreterWorker>>,
    routes: &'static RouteTable,
    routes_for_ws: Option<FrozenRoutes>,
    conn_token: CancellationToken,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let io = TokioIo::new(io);
    let mut builder = AutoBuilder::new(LocalExec);
    builder
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(std::time::Duration::from_secs(10));

    // Drive-to-completion loop shared by both connection kinds. Macro
    // rather than generic fn because hyper's Connection /
    // UpgradeableConnection expose `graceful_shutdown` as inherent
    // methods rather than through a common trait, and writing the
    // trait wrapper costs more lines than a one-place macro.
    macro_rules! drive_to_completion {
        ($conn:ident) => {{
            tokio::pin!($conn);
            let mut graceful_sent = false;
            loop {
                tokio::select! {
                    res = $conn.as_mut() => {
                        if let Err(e) = res {
                            if !is_benign_disconnect(e.as_ref()) {
                                tracing::warn!(target: "pyronova::server", error = %e, "Connection error");
                            }
                        }
                        break;
                    }
                    _ = conn_token.cancelled(), if !graceful_sent => {
                        $conn.as_mut().graceful_shutdown();
                        graceful_sent = true;
                    }
                }
            }
        }};
    }

    // Split by ws support at connection start so the per-request
    // closure is fully specialized — no runtime `if ws_supported`
    // branch, no `Option<FrozenRoutes>` state to carry through every
    // future. The bench path's closure is byte-identical to the old
    // hand-written drive_inmem_conn, so no per-request regression.
    match routes_for_ws {
        Some(ws_routes_conn) => {
            let svc = service_fn(move |req: Request<Incoming>| {
                let worker = std::rc::Rc::clone(&worker);
                let bridge = main_bridge.clone();
                let is_ws = websocket::is_websocket_upgrade(&req);
                let ws_routes = if is_ws {
                    Some(Arc::clone(&ws_routes_conn))
                } else {
                    None
                };
                async move {
                    if is_ws {
                        websocket::handle_websocket(req, ws_routes.expect("ws routes set")).await
                    } else {
                        handle_request_tpc_inline(req, routes, worker, remote_addr, bridge).await
                    }
                }
            });
            let conn = builder.serve_connection_with_upgrades(io, svc);
            drive_to_completion!(conn);
        }
        None => {
            let svc = service_fn(move |req: Request<Incoming>| {
                let worker = std::rc::Rc::clone(&worker);
                let bridge = main_bridge.clone();
                async move { handle_request_tpc_inline(req, routes, worker, remote_addr, bridge).await }
            });
            let conn = builder.serve_connection(io, svc);
            drive_to_completion!(conn);
        }
    }
}

/// TCP adapter: TLS handshake (if configured), then hand off to the
/// generic driver. Keeps the TLS decision at the transport boundary
/// so the inner driver stays transport-agnostic.
#[allow(clippy::too_many_arguments)] // single-call adapter, args mirror the per-thread context
pub(crate) async fn drive_tcp_conn(
    stream: tokio::net::TcpStream,
    remote_addr: std::net::SocketAddr,
    worker: std::rc::Rc<std::cell::RefCell<SubInterpreterWorker>>,
    routes: &'static RouteTable,
    routes_for_ws: FrozenRoutes,
    conn_token: CancellationToken,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    main_bridge: Option<Arc<crate::bridge::main_bridge::MainInterpBridge>>,
) {
    let tls_stream = match tls_acceptor {
        Some(acc) => match crate::tls::wrap_tls(&acc, stream).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "pyronova::server", error = %e, "TLS handshake failed");
                return;
            }
        },
        None => crate::tls::wrap_plain(stream),
    };
    drive_conn(
        tls_stream,
        remote_addr.ip(),
        worker,
        routes,
        Some(routes_for_ws),
        conn_token,
        main_bridge,
    )
    .await;
}
