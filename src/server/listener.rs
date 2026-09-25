//! TCP listener configuration: the free-port probe, SO_REUSEPORT + TCP_DEFER_ACCEPT +
//! TCP_QUICKACK + accept-error backoff. Every serving path and both bench harnesses bind
//! through [`BoundListeners::bind`]; the Linux-only knobs (TCP_QUICKACK,
//! TCP_DEFER_ACCEPT) live here.

use std::net::SocketAddr;
use std::sync::Arc;

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

impl ListenerError {
    /// The OS error number, when the OS reported one (e.g. `EADDRINUSE`).
    pub(crate) fn errno(&self) -> Option<i32> {
        self.source.raw_os_error()
    }
}

/// `OSError(errno, text)`, so Python sees e.g. `errno.EADDRINUSE` for a port in use.
impl From<ListenerError> for pyo3::PyErr {
    fn from(e: ListenerError) -> Self {
        match e.errno() {
            Some(errno) => pyo3::exceptions::PyOSError::new_err((errno, e.to_string())),
            None => pyo3::exceptions::PyOSError::new_err(e.to_string()),
        }
    }
}

type Acceptor = Arc<tokio_rustls::TlsAcceptor>;

/// One address the server listens on, and whether it speaks TLS there.
#[derive(Clone)]
pub(crate) struct ListenerSpec {
    pub(crate) addr: SocketAddr,
    pub(crate) tls: Option<Acceptor>,
}

impl ListenerSpec {
    /// The listener set of one server: `addr`, then one TLS listener per extra port on
    /// the same host. With extra TLS ports, `addr` serves plain HTTP and TLS is on the
    /// extra ports; without them, `addr` speaks TLS when an acceptor is configured.
    pub(crate) fn set(
        addr: SocketAddr,
        tls: Option<Acceptor>,
        extra_tls_ports: &[u16],
    ) -> Result<Vec<ListenerSpec>, ExtraTlsWithoutCert> {
        match (tls, extra_tls_ports) {
            (tls, []) => Ok(vec![ListenerSpec { addr, tls }]),
            (Some(acceptor), ports) => Ok(std::iter::once(ListenerSpec { addr, tls: None })
                .chain(ports.iter().map(|&port| ListenerSpec {
                    addr: SocketAddr::new(addr.ip(), port),
                    tls: Some(Arc::clone(&acceptor)),
                }))
                .collect()),
            (None, ports) => Err(ExtraTlsWithoutCert(ports.to_vec())),
        }
    }
}

/// Extra TLS ports configured without a certificate: they could never be opened as TLS,
/// and serving without them would leave the operator believing they are protected.
#[derive(Debug, thiserror::Error)]
#[error(
    "extra_tls_ports {0:?} configured but tls_cert/tls_key not set; these ports cannot be \
     opened as TLS. Provide tls_cert+tls_key or remove the port list."
)]
pub(crate) struct ExtraTlsWithoutCert(pub(crate) Vec<u16>);

/// A bound, listening socket (not yet registered with a runtime) and its TLS.
pub(crate) struct Listener {
    pub(crate) socket: std::net::TcpListener,
    /// The address it is bound to.
    pub(crate) addr: SocketAddr,
    pub(crate) tls: Option<Acceptor>,
}

/// Where one spec of a server listens, once bound (a port 0 resolved to the kernel's pick).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Bound {
    pub(crate) addr: SocketAddr,
    pub(crate) tls: bool,
}

impl std::fmt::Display for Bound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scheme = if self.tls { "https" } else { "http" };
        write!(f, "{scheme}://{}", self.addr)
    }
}

/// Every socket of one server, bound before any serving thread exists.
pub(crate) struct BoundListeners {
    /// One group per accept loop; each group has one socket per spec, in spec order.
    pub(crate) groups: Vec<Vec<Listener>>,
    /// Where each spec is bound, in spec order.
    pub(crate) bound: Vec<Bound>,
}

/// Proves `addr` free before the `SO_REUSEPORT` set joins it: a socket without
/// `SO_REUSEPORT` can't bind a port any other socket listens on, whether or not that one
/// set `SO_REUSEPORT`, so a port held by another server (in this process or another) is
/// `AddrInUse` here instead of silently shared. `SO_REUSEADDR` is set, as on the set, so a
/// port left in TIME_WAIT by an earlier server still counts as free. Returns the bound
/// address (a port 0 resolved to the kernel's pick); the probe is released on return.
fn probe_free(addr: SocketAddr) -> Result<SocketAddr, ListenerError> {
    use socket2::{Domain, Protocol, Socket, Type};

    let failed = |step: &'static str| move |source| ListenerError { step, addr, source };
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))
        .map_err(failed("socket creation"))?;
    socket
        .set_reuse_address(true)
        .map_err(failed("set_reuse_address"))?;
    socket.bind(&addr.into()).map_err(failed("bind"))?;
    socket
        .local_addr()
        .ok()
        .and_then(|local| local.as_socket())
        .ok_or_else(|| ListenerError {
            step: "local_addr",
            addr,
            source: std::io::Error::other("the probe socket has no IP address"),
        })
}

impl BoundListeners {
    /// Binds `copies` `SO_REUSEPORT` sockets for each spec: one group per accept loop, so
    /// the kernel spreads connections over the loops. Each port is first proven free
    /// ([`probe_free`], decision G4); a port 0 is resolved by that probe and every copy
    /// joins the port the kernel picked. Any failure (e.g. `AddrInUse`) is returned here,
    /// before a thread or worker exists.
    pub(crate) fn bind(specs: &[ListenerSpec], copies: usize) -> Result<Self, ListenerError> {
        let mut groups: Vec<Vec<Listener>> = (0..copies).map(|_| Vec::new()).collect();
        let mut bound = Vec::with_capacity(specs.len());
        for spec in specs {
            let addr = probe_free(spec.addr)?;
            for group in &mut groups {
                let socket = create_reuseport_listener(addr)?;
                group.push(Listener {
                    socket,
                    addr,
                    tls: spec.tls.clone(),
                });
            }
            bound.push(Bound {
                addr,
                tls: spec.tls.is_some(),
            });
        }
        Ok(BoundListeners { groups, bound })
    }
}

/// A connection one of a group's listeners accepted, configured for serving.
pub(crate) struct Accepted {
    pub(crate) stream: tokio::net::TcpStream,
    pub(crate) remote: SocketAddr,
    /// The acceptor of the listener it arrived on; `None` for plain HTTP.
    pub(crate) tls: Option<Acceptor>,
}

/// One accept loop's listeners (a group of [`BoundListeners`]) as one stream of
/// connections. Replaces one `select!` arm per listener.
pub(crate) struct AcceptSource {
    listeners: Vec<(tokio::net::TcpListener, Option<Acceptor>)>,
    /// Where the next poll starts, so a busy listener can't starve the others.
    next: usize,
}

impl AcceptSource {
    /// Registers the group's sockets with the current runtime; call on the thread (and
    /// runtime) that accepts from them.
    pub(crate) fn new(group: Vec<Listener>) -> Result<Self, ListenerError> {
        let listeners = group
            .into_iter()
            .map(|l| {
                let socket = tokio::net::TcpListener::from_std(l.socket).map_err(|source| {
                    ListenerError {
                        step: "register with the runtime",
                        addr: l.addr,
                        source,
                    }
                })?;
                Ok((socket, l.tls))
            })
            .collect::<Result<_, ListenerError>>()?;
        Ok(AcceptSource { listeners, next: 0 })
    }

    /// The next connection from any listener, with `TCP_NODELAY` (and `TCP_QUICKACK` on
    /// Linux) set. An accept error is logged and backed off here
    /// ([`handle_accept_error`]); it never ends the stream.
    pub(crate) async fn accept(&mut self) -> Accepted {
        loop {
            let (result, index) = std::future::poll_fn(|cx| {
                let n = self.listeners.len();
                for k in 0..n {
                    let i = (self.next + k) % n;
                    if let std::task::Poll::Ready(r) = self.listeners[i].0.poll_accept(cx) {
                        return std::task::Poll::Ready((r, i));
                    }
                }
                std::task::Poll::Pending
            })
            .await;
            self.next = (index + 1) % self.listeners.len();
            match result {
                Ok((stream, remote)) => {
                    if let Err(e) = stream.set_nodelay(true) {
                        tracing::warn!(
                            target: "pyronova::server",
                            error = %e,
                            %remote,
                            "set_nodelay failed; this connection keeps Nagle's algorithm"
                        );
                    }
                    setup_tcp_quickack(&stream);
                    return Accepted {
                        stream,
                        remote,
                        tls: self.listeners[index].1.clone(),
                    };
                }
                Err(e) => handle_accept_error(&e).await,
            }
        }
    }
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

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn copies_of_a_port_zero_listener_share_the_port_the_kernel_picked() {
        let spec = ListenerSpec {
            addr: loopback(0),
            tls: None,
        };
        let bound = BoundListeners::bind(&[spec], 3).expect("loopback binds");
        assert_eq!(bound.groups.len(), 3);
        let port = bound.bound[0].addr.port();
        assert_ne!(port, 0);
        for group in &bound.groups {
            assert_eq!(group.len(), 1);
            assert_eq!(group[0].socket.local_addr().unwrap().port(), port);
            assert_eq!(group[0].addr.port(), port);
        }
        assert_eq!(
            bound.bound[0].to_string(),
            format!("http://127.0.0.1:{port}")
        );
    }

    #[test]
    fn a_port_in_use_is_a_bind_error_with_its_errno() {
        // No SO_REUSEPORT on the holder, so the server's socket can't join it.
        let holder = std::net::TcpListener::bind(loopback(0)).unwrap();
        let addr = holder.local_addr().unwrap();
        let spec = ListenerSpec { addr, tls: None };
        let err = BoundListeners::bind(&[spec], 2)
            .err()
            .expect("the port is taken");
        assert_eq!(err.step, "bind");
        assert_eq!(err.source.kind(), std::io::ErrorKind::AddrInUse);
        assert_eq!(err.errno(), Some(libc::EADDRINUSE));
    }

    #[test]
    fn a_port_another_reuseport_server_listens_on_is_in_use() {
        // Another server's SO_REUSEPORT socket: without the probe, ours would join it and
        // the kernel would split the traffic between the two servers.
        let other = create_reuseport_listener(loopback(0)).unwrap();
        let addr = other.local_addr().unwrap();
        let spec = ListenerSpec { addr, tls: None };
        let err = BoundListeners::bind(&[spec], 2)
            .err()
            .expect("the port is another server's");
        assert_eq!(err.step, "bind");
        assert_eq!(err.errno(), Some(libc::EADDRINUSE));
        assert_eq!(err.addr, addr);
    }

    #[test]
    fn a_released_port_binds_again() {
        let first = BoundListeners::bind(
            &[ListenerSpec {
                addr: loopback(0),
                tls: None,
            }],
            2,
        )
        .unwrap();
        let addr = first.bound[0].addr;
        drop(first);
        let again = BoundListeners::bind(&[ListenerSpec { addr, tls: None }], 2)
            .expect("a port its last server released is free");
        assert_eq!(again.bound[0].addr, addr);
    }

    #[test]
    fn extra_tls_ports_need_a_certificate() {
        let err = ListenerSpec::set(loopback(8000), None, &[8443])
            .err()
            .expect("no certificate");
        assert_eq!(err.0, vec![8443]);
        let plain = ListenerSpec::set(loopback(8000), None, &[]).unwrap();
        assert_eq!(plain.len(), 1);
        assert!(plain[0].tls.is_none());
    }

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
