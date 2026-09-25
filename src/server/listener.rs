//! TCP listener configuration: SO_REUSEPORT + TCP_DEFER_ACCEPT +
//! TCP_QUICKACK + accept-error backoff.
//!
//! Extracted out of `app.rs` so the 1500-line pymethods block doesn't
//! carry 120 lines of socket-layer config that has nothing to do with
//! the Python-facing app surface. Every TPC spawn path (production
//! `run_tpc_subinterp`, both bench harnesses) calls these helpers;
//! centralizing them here also makes platform-specific tuning easy
//! to find — `#[cfg(target_os = "linux")]` for the two Linux-only
//! knobs (TCP_QUICKACK, TCP_DEFER_ACCEPT) lives in one file now.

use std::net::SocketAddr;

/// Enable TCP_QUICKACK on a stream (Linux only, no-op elsewhere).
#[allow(unused_variables)]
pub(crate) fn setup_tcp_quickack(stream: &tokio::net::TcpStream) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = stream.as_raw_fd();
        let val: libc::c_int = 1;
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_TCP,
                libc::TCP_QUICKACK,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            )
        };
        // Capture errno immediately after the syscall, before the branch test
        // or anything else can clobber the thread-local errno.
        let errno = std::io::Error::last_os_error();
        // Mirror the TCP_DEFER_ACCEPT handling in create_reuseport_listener:
        // a silent failure here disables the latency optimization (delayed
        // ACKs creep back in) with no trace, making it look mysteriously
        // absent under load. Log so the missing knob is observable.
        if rc != 0 {
            tracing::warn!(
                target: "pyronova::server",
                ?errno,
                "setsockopt(TCP_QUICKACK) failed; delayed-ACK latency \
                 optimization is disabled on this socket"
            );
        }
    }
}

/// A listening socket that could not be set up: the step that failed, on which address,
/// with the OS error (`source().kind()` tells e.g. `AddrInUse`).
#[derive(Debug, thiserror::Error)]
#[error("{step} for {addr} failed: {source}")]
pub(crate) struct ListenerError {
    pub(crate) step: &'static str,
    pub(crate) addr: SocketAddr,
    #[source]
    pub(crate) source: std::io::Error,
}

/// Backlog of the listening socket: large, to avoid SYN drops at 200k+ QPS.
const LISTEN_BACKLOG: i32 = 8192;

/// Create a TCP listener with SO_REUSEPORT (kernel load-balanced accept)
/// and a large backlog to avoid SYN drops under extreme load.
pub(crate) fn create_reuseport_listener(
    addr: SocketAddr,
) -> Result<std::net::TcpListener, ListenerError> {
    use socket2::{Domain, Protocol, Socket, Type};

    let failed = |step: &'static str| move |source| ListenerError { step, addr, source };
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(failed("socket creation"))?;

    socket
        .set_reuse_address(true)
        .map_err(failed("set_reuse_address"))?;

    // SO_REUSEPORT: allows multiple listeners on the same port.
    // Kernel distributes incoming connections across all listeners.
    #[cfg(not(windows))]
    socket
        .set_reuse_port(true)
        .map_err(failed("set_reuse_port"))?;

    socket
        .set_nonblocking(true)
        .map_err(failed("set_nonblocking"))?;

    socket.bind(&addr.into()).map_err(failed("bind"))?;

    // TCP_DEFER_ACCEPT (Linux only): don't wake the accept loop on the
    // bare three-way handshake — wait until the client actually sends
    // the first byte of the HTTP request. A cold-connect flood
    // otherwise spins up Tokio tasks that immediately block in hyper's
    // header-read (or, if no data ever arrives, burn a file descriptor
    // until the header_read_timeout fires 10s later — see app.rs's
    // AutoBuilder config). Timeout arg is seconds after SYN-ACK before
    // the kernel gives up and delivers the bare accept anyway; keeping
    // it modest so half-open connections still surface within the
    // header-read budget.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = socket.as_raw_fd();
        let secs: libc::c_int = 10;
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_DEFER_ACCEPT,
                &secs as *const _ as *const libc::c_void,
                std::mem::size_of_val(&secs) as libc::socklen_t,
            )
        };
        // Capture errno immediately after the syscall, before the branch test
        // or anything else can clobber the thread-local errno.
        let errno = std::io::Error::last_os_error();
        // Silent failure here disables the DoS mitigation the doc
        // comment above describes (cold-connect floods burning FDs).
        // Log so the missing optimization is observable instead of
        // mysteriously absent at scale (arc finding listener-2).
        if rc != 0 {
            tracing::warn!(
                target: "pyronova::server",
                ?errno,
                "setsockopt(TCP_DEFER_ACCEPT) failed; cold-connect DoS \
                 mitigation is disabled on this socket"
            );
        }
    }

    socket.listen(LISTEN_BACKLOG).map_err(failed("listen"))?;

    Ok(socket.into())
}

/// Back off when accept() fails. Critical for EMFILE/ENFILE (file-descriptor
/// exhaustion) — a bare `continue` on these errors spins the accept loop at
/// 100% CPU because the next accept() call fails immediately. Sleeping a few
/// hundred ms lets short-lived fds close and gives the OS room to recover.
/// Transient per-connection errors (ECONNABORTED etc.) get a tiny yield to
/// avoid degenerate tight loops without meaningfully delaying legitimate traffic.
pub(crate) async fn handle_accept_error(e: &std::io::Error) {
    let backoff_ms = if is_resource_exhaustion(e) {
        tracing::error!(
            target: "pyronova::server",
            error = %e,
            "accept() resource exhaustion — backing off 250ms",
        );
        250
    } else {
        tracing::warn!(target: "pyronova::server", error = %e, "accept() error");
        10
    };
    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
}

/// Whether an accept() error signals resource exhaustion (out of file
/// descriptors / socket handles / kernel buffers) versus a transient
/// per-connection error. `raw_os_error()` returns platform-native codes,
/// so the constants must be matched per-platform: Unix errnos here, the
/// `WSAE*` WinSock codes on Windows (e.g. WSAEMFILE=10024, *not* the CRT
/// EMFILE=24 returned for non-socket errors).
fn is_resource_exhaustion(e: &std::io::Error) -> bool {
    match e.raw_os_error() {
        Some(code) => {
            #[cfg(unix)]
            {
                matches!(
                    code,
                    libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM
                )
            }
            #[cfg(windows)]
            {
                // WSAEMFILE (no more socket handles), WSAENOBUFS (no buffer
                // space). Windows has no socket-level ENFILE/ENOMEM analogue.
                const WSAEMFILE: i32 = 10024;
                const WSAENOBUFS: i32 = 10055;
                matches!(code, WSAEMFILE | WSAENOBUFS)
            }
            #[cfg(not(any(unix, windows)))]
            {
                let _ = code;
                false
            }
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_bind_names_the_step_and_keeps_the_os_error() {
        // TEST-NET-1 (RFC 5737): never an address of this host.
        let addr: SocketAddr = "192.0.2.1:0".parse().unwrap();
        let err = create_reuseport_listener(addr).expect_err("not a local address");
        assert_eq!(err.step, "bind");
        assert_eq!(err.addr, addr);
        assert_eq!(err.source.kind(), std::io::ErrorKind::AddrNotAvailable);
        assert!(
            err.to_string().starts_with("bind for 192.0.2.1:0 failed: "),
            "{err}"
        );
    }
}
