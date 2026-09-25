//! In-process benchmark harnesses behind `PyronovaApp.bench_inmem` / `bench_loopback`.
//! Compiled only with the `bench` cargo feature.
//!
//! - [`run_inmem_bench`]: virtual connections via `tokio::io::duplex`. The full
//!   per-request pipeline (hyper parse → routing → handler → response write) with no
//!   kernel socket: the framework ceiling.
//! - [`run_loopback_bench`]: real TCP on 127.0.0.1, client in the same process. The gap
//!   to the in-memory number is the kernel's share (syscalls, TCP state, loopback copy,
//!   kqueue/epoll wakeups).
//!
//! Both serve through the production TPC connection driver, so a bench number is the
//! production per-request cost.

use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::{JoinHandle, LocalSet};
use tokio_util::sync::CancellationToken;

use crate::config::GcConfig;
use crate::python::interp::SubInterpreterWorker;
use crate::site::{SharedSite, Site};
use crate::tpc::{elevate_thread_qos_macos, tpc_accept_loop_inline, try_pin_current};

/// The request every client connection sends, pipelined [`PIPELINE_DEPTH`] deep.
const BENCH_REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: bench\r\nConnection: keep-alive\r\n\r\n";
const PIPELINE_DEPTH: usize = 32;
/// Served but not measured, so every connection is past its probe when counting starts.
const WARMUP: Duration = Duration::from_secs(1);
/// In-memory connection buffer, per direction.
const DUPLEX_BUFFER_BYTES: usize = 64 * 1024;
/// A loopback client retries a refused `connect` this long while the server threads bind.
const CONNECT_DEADLINE: Duration = Duration::from_secs(5);
const CONNECT_RETRY: Duration = Duration::from_millis(10);
/// Client runtime threads: a few pipelined connections saturate many server workers, and
/// more threads would take cores from the servers.
const LOOPBACK_CLIENT_THREADS: usize = 2;
/// Response headers the probe parses; a bench route's response has far fewer.
const MAX_RESPONSE_HEADERS: usize = 64;

/// Requests completed within the measured window.
pub(crate) struct Measured {
    pub(crate) requests: u64,
    pub(crate) elapsed: Duration,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BenchError {
    #[error("spawn thread {thread}: {source}")]
    Spawn {
        thread: String,
        #[source]
        source: io::Error,
    },
    #[error("bind a loopback port: {0}")]
    Bind(#[source] crate::server::listener::ListenerError),
    #[error("read the bound loopback port: {0}")]
    LocalAddr(#[source] io::Error),
    #[error("the bench failed: {}", join_failures(.0))]
    Failed(Vec<Failure>),
}

/// Something that went wrong on a bench thread. Any failure fails the whole bench: a
/// number measured with a dead worker or client is not the number asked for.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Failure {
    #[error("thread {thread} panicked: {payload}")]
    Panicked { thread: String, payload: String },
    #[error("thread {thread} could not build its runtime: {source}")]
    Runtime {
        thread: String,
        #[source]
        source: io::Error,
    },
    #[error("thread {thread} was never handed its worker")]
    NoWorker { thread: String },
    #[error("client connection {conn}: {source}")]
    Client {
        conn: usize,
        #[source]
        source: ClientError,
    },
    #[error("client connection {conn} panicked: {payload}")]
    ClientPanicked { conn: usize, payload: String },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("the server closed the connection")]
    Closed,
    #[error(transparent)]
    Response(#[from] ResponseError),
}

/// Why the probe response cannot be benched against.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ResponseError {
    #[error("unparseable response head: {0}")]
    Parse(httparse::Error),
    #[error("GET / answered {0}, not 200")]
    Status(u16),
    #[error("the response has no Content-Length")]
    NoContentLength,
    #[error("invalid Content-Length {0:?}")]
    BadContentLength(String),
}

fn join_failures(failures: &[Failure]) -> String {
    failures
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

// ---------------------------------------------------------------------------
// Benches
// ---------------------------------------------------------------------------

/// Each worker serves `conns_per_worker` in-memory connections, each driven by a client
/// task on the same thread. `workers` pairs every worker with its own copy of the site,
/// so no two cores share a refcount cacheline.
pub(crate) fn run_inmem_bench(
    conns_per_worker: usize,
    duration: Duration,
    workers: Vec<(SubInterpreterWorker, SharedSite)>,
) -> Result<Measured, BenchError> {
    println!(
        "\n  Pyronova v{} [in-memory bench] — {} workers × {conns_per_worker} virtual conns, {}s",
        env!("CARGO_PKG_VERSION"),
        workers.len(),
        duration.as_secs()
    );
    let counter = Arc::new(AtomicU64::new(0));

    let mut threads = Threads::new();
    let lent: Vec<(SubInterpreterWorker, &'static Site)> = workers
        .into_iter()
        // SAFETY: each reference goes only into the closure `spawn_workers` runs on a
        // thread of `threads`.
        .map(|(worker, site)| (worker, unsafe { threads.lend(site) }))
        .collect();
    let serve_counter = Arc::clone(&counter);
    spawn_workers(&mut threads, "inmem", lent, move |worker, site, stop| {
        serve_inmem(
            worker,
            site,
            conns_per_worker,
            Arc::clone(&serve_counter),
            stop,
        )
    })?;

    let measured = measure(&counter, duration);
    fail_on(threads.finish())?;
    Ok(measured)
}

/// The workers accept real TCP connections on one ephemeral loopback port
/// (`SO_REUSEPORT`); `client_conns` pipelined clients run on a separate runtime.
/// Returns the measurement and the port.
pub(crate) fn run_loopback_bench(
    client_conns: usize,
    duration: Duration,
    workers: Vec<SubInterpreterWorker>,
    site: SharedSite,
    gc: GcConfig,
) -> Result<(Measured, u16), BenchError> {
    let addr = free_loopback_addr()?;
    println!(
        "\n  Pyronova v{} [loopback bench] — {} workers, {client_conns} client conns, port {}, {}s",
        env!("CARGO_PKG_VERSION"),
        workers.len(),
        addr.port(),
        duration.as_secs()
    );
    let counter = Arc::new(AtomicU64::new(0));

    // Declared before `clients`, so on an early return the clients stop first.
    let mut servers = Threads::new();
    // SAFETY: the reference goes only into the closure `spawn_workers` runs on threads of
    // `servers`.
    let site_ref = unsafe { servers.lend(Arc::clone(&site)) };
    let workers = workers.into_iter().map(|w| (w, site_ref)).collect();
    spawn_workers(
        &mut servers,
        "lb-srv",
        workers,
        move |worker, site_ref, stop| {
            let site = Arc::clone(&site);
            async move {
                tpc_accept_loop_inline(addr, vec![], worker, site_ref, site, stop, None, None, gc)
                    .await;
                Vec::new()
            }
        },
    )?;
    let mut clients = Threads::new();
    let client_counter = Arc::clone(&counter);
    clients.spawn("pyronova-lb-client".to_string(), move |stop| {
        run_loopback_clients(addr, client_conns, client_counter, stop)
    })?;

    let measured = measure(&counter, duration);
    // Clients first: the servers keep serving until every client has finished its batch.
    let failures: Vec<Failure> = clients
        .finish()
        .into_iter()
        .chain(servers.finish())
        .collect();
    fail_on(failures)?;
    Ok((measured, addr.port()))
}

fn fail_on(failures: Vec<Failure>) -> Result<(), BenchError> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(BenchError::Failed(failures))
    }
}

fn measure(counter: &AtomicU64, duration: Duration) -> Measured {
    std::thread::sleep(WARMUP);
    let start = counter.load(Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(duration);
    Measured {
        requests: counter.load(Ordering::Relaxed) - start,
        elapsed: t0.elapsed(),
    }
}

fn free_loopback_addr() -> Result<SocketAddr, BenchError> {
    // Bound with SO_REUSEPORT and dropped: the server threads then bind the same port.
    let probe = crate::server::listener::create_reuseport_listener((Ipv4Addr::LOCALHOST, 0).into())
        .map_err(BenchError::Bind)?;
    probe.local_addr().map_err(BenchError::LocalAddr)
}

/// One worker's LocalSet: `conns` server connections, each with its client task. Ends
/// when every client has stopped; the server side is dropped with the LocalSet.
async fn serve_inmem(
    worker: Rc<RefCell<SubInterpreterWorker>>,
    site: &'static Site,
    conns: usize,
    counter: Arc<AtomicU64>,
    stop: CancellationToken,
) -> Vec<Failure> {
    // Never cancelled: a server connection shutting down mid-batch would fail its client.
    let server_stop = CancellationToken::new();
    let clients = (0..conns)
        .map(|_| {
            let (server_io, client_io) = tokio::io::duplex(DUPLEX_BUFFER_BYTES);
            tokio::task::spawn_local(crate::worker::drive_conn(
                server_io,
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                Rc::clone(&worker),
                site,
                None,
                server_stop.clone(),
                None,
            ));
            let (counter, stop) = (Arc::clone(&counter), stop.clone());
            tokio::task::spawn_local(async move { drive_client(client_io, &counter, &stop).await })
        })
        .collect();
    client_failures(clients).await
}

fn run_loopback_clients(
    addr: SocketAddr,
    conns: usize,
    counter: Arc<AtomicU64>,
    stop: CancellationToken,
) -> Vec<Failure> {
    let rt = match RuntimeBuilder::new_multi_thread()
        .worker_threads(LOOPBACK_CLIENT_THREADS)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(source) => {
            return vec![Failure::Runtime {
                thread: "pyronova-lb-client".to_string(),
                source,
            }]
        }
    };
    rt.block_on(async move {
        let clients = (0..conns)
            .map(|_| {
                let (counter, stop) = (Arc::clone(&counter), stop.clone());
                tokio::spawn(async move {
                    let stream = connect(addr).await?;
                    stream.set_nodelay(true)?;
                    drive_client(stream, &counter, &stop).await
                })
            })
            .collect();
        client_failures(clients).await
    })
}

async fn client_failures(clients: Vec<JoinHandle<Result<(), ClientError>>>) -> Vec<Failure> {
    futures_util::future::join_all(clients)
        .await
        .into_iter()
        .enumerate()
        .filter_map(|(conn, joined)| match joined {
            Ok(Ok(())) => None,
            Ok(Err(source)) => Some(Failure::Client { conn, source }),
            Err(e) => Some(Failure::ClientPanicked {
                conn,
                payload: e.to_string(),
            }),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Probes one response to learn its size, then pipelines [`PIPELINE_DEPTH`] requests at a
/// time and reads exactly that many responses' bytes, until `stop`. Every response of the
/// bench route is byte-identical, so counting bytes counts responses.
async fn drive_client<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    counter: &AtomicU64,
    stop: &CancellationToken,
) -> Result<(), ClientError> {
    let response_len = probe(&mut stream).await?;
    counter.fetch_add(1, Ordering::Relaxed);

    let batch = BENCH_REQUEST.repeat(PIPELINE_DEPTH);
    let mut responses = vec![0u8; response_len * PIPELINE_DEPTH];
    while !stop.is_cancelled() {
        stream.write_all(&batch).await?;
        stream.read_exact(&mut responses).await?;
        counter.fetch_add(PIPELINE_DEPTH as u64, Ordering::Relaxed);
    }
    Ok(())
}

/// Sends one request and reads its whole response; returns the response's length.
async fn probe<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> Result<usize, ClientError> {
    stream.write_all(BENCH_REQUEST).await?;
    let mut buf = Vec::with_capacity(4096);
    loop {
        if stream.read_buf(&mut buf).await? == 0 {
            return Err(ClientError::Closed);
        }
        if let Some(len) = response_len(&buf)? {
            let read = buf.len();
            if read < len {
                buf.resize(len, 0);
                stream.read_exact(&mut buf[read..]).await?;
            }
            return Ok(len);
        }
    }
}

/// The length of the `200` response at the start of `buf` (head plus `Content-Length`
/// body), or `None` while its head is incomplete.
fn response_len(buf: &[u8]) -> Result<Option<usize>, ResponseError> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
    let mut response = httparse::Response::new(&mut headers);
    let head_len = match response.parse(buf).map_err(ResponseError::Parse)? {
        httparse::Status::Partial => return Ok(None),
        httparse::Status::Complete(len) => len,
    };
    let status = response
        .code
        .ok_or(ResponseError::Parse(httparse::Error::Status))?;
    if status != 200 {
        return Err(ResponseError::Status(status));
    }
    let raw = response
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
        .ok_or(ResponseError::NoContentLength)?
        .value;
    let body_len = std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .ok_or_else(|| ResponseError::BadContentLength(String::from_utf8_lossy(raw).into()))?;
    Ok(Some(head_len + body_len))
}

async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
    let deadline = Instant::now() + CONNECT_DEADLINE;
    loop {
        match TcpStream::connect(addr).await {
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused && Instant::now() < deadline => {
                tokio::time::sleep(CONNECT_RETRY).await
            }
            connected => return connected,
        }
    }
}

// ---------------------------------------------------------------------------
// Threads
// ---------------------------------------------------------------------------

/// A group of bench threads sharing one stop token. Finishing or dropping the group stops
/// and joins every thread, and only then releases what was lent to them.
struct Threads {
    stop: CancellationToken,
    running: Vec<(String, std::thread::JoinHandle<Vec<Failure>>)>,
    /// Dropped after `Drop for Threads` has joined `running`.
    lent: Vec<Arc<dyn std::any::Any + Send + Sync>>,
}

impl Threads {
    fn new() -> Self {
        Threads {
            stop: CancellationToken::new(),
            running: Vec::new(),
            lent: Vec::new(),
        }
    }

    /// `value` as a `&'static T`: the group keeps it alive until every thread is joined.
    /// The TPC connection driver takes the site this way, so the per-request path does no
    /// refcount op (as in production TPC).
    ///
    /// # Safety
    /// The reference must only be moved into closures run by this group's threads: it is
    /// valid until the group is finished or dropped.
    unsafe fn lend<T: Send + Sync + 'static>(&mut self, value: Arc<T>) -> &'static T {
        let lent: *const T = Arc::as_ptr(&value);
        self.lent.push(value);
        // SAFETY: `self.lent` holds the allocation until after every thread of this group
        // is joined; the caller keeps the reference inside those threads.
        unsafe { &*lent }
    }

    fn spawn(
        &mut self,
        thread: String,
        body: impl FnOnce(CancellationToken) -> Vec<Failure> + Send + 'static,
    ) -> Result<(), BenchError> {
        let stop = self.stop.clone();
        let handle = std::thread::Builder::new()
            .name(thread.clone())
            .stack_size(crate::python::PYTHON_THREAD_STACK)
            .spawn(move || body(stop))
            .map_err(|source| BenchError::Spawn {
                thread: thread.clone(),
                source,
            })?;
        self.running.push((thread, handle));
        Ok(())
    }

    /// Stops and joins every thread; what went wrong on them, if anything.
    fn finish(mut self) -> Vec<Failure> {
        self.stop_and_join()
    }

    fn stop_and_join(&mut self) -> Vec<Failure> {
        self.stop.cancel();
        self.running
            .drain(..)
            .flat_map(|(thread, handle)| match handle.join() {
                Ok(failures) => failures,
                Err(panic) => vec![Failure::Panicked {
                    thread,
                    payload: panic_text(panic.as_ref()),
                }],
            })
            .collect()
    }
}

impl Drop for Threads {
    /// Reached with threads still running only when the bench is already returning an
    /// error; what else failed goes to the log.
    fn drop(&mut self) {
        for failure in self.stop_and_join() {
            tracing::error!(target: "pyronova::server", %failure, "bench thread failed while the bench was aborting");
        }
    }
}

fn panic_text(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "a non-string panic payload".to_string())
}

/// One pinned thread per worker: the worker's interpreter is rebound to that thread,
/// `serve` runs on its current-thread runtime + LocalSet, then the interpreter is ended
/// there. A worker is handed over only after its thread exists, so when a spawn fails,
/// every worker not yet handed over (the one meant for that thread included) is ended
/// here, on the thread that built them.
fn spawn_workers<T, F, Fut>(
    threads: &mut Threads,
    name: &str,
    workers: Vec<(SubInterpreterWorker, T)>,
    serve: F,
) -> Result<(), BenchError>
where
    T: Send + 'static,
    F: Fn(Rc<RefCell<SubInterpreterWorker>>, T, CancellationToken) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Vec<Failure>> + 'static,
{
    // No core list (unsupported platform) means no pinning.
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let mut pending = workers.into_iter().enumerate();
    while let Some((i, handoff)) = pending.next() {
        let thread = format!("pyronova-{name}-{i}");
        let core = core_ids.get(i).copied();
        let serve = serve.clone();
        let (tx, rx) = mpsc::sync_channel(1);
        let name = thread.clone();
        let spawned = threads.spawn(thread, move |stop| {
            try_pin_current(core);
            elevate_thread_qos_macos();
            match rx.recv() {
                Ok((worker, payload)) => serve_worker(name, worker, |w| serve(w, payload, stop)),
                Err(_) => vec![Failure::NoWorker { thread: name }],
            }
        });
        if let Err(e) = spawned {
            let unserved = std::iter::once(handoff)
                .chain(pending.map(|(_, h)| h))
                .map(|(w, _)| w);
            // SAFETY: on the thread that built the workers, inside `py.detach` (no thread
            // state current); none of these was rebound.
            unsafe { SubInterpreterWorker::end_all(unserved) };
            return Err(e);
        }
        // The thread's only receiver is waiting; a one-slot channel never blocks here.
        if let Err(mpsc::SendError(unsent)) = tx.send(handoff) {
            // The thread is gone before taking its worker (it panicked in pinning).
            // SAFETY: as above; this worker was never rebound.
            unsafe { SubInterpreterWorker::end_all([unsent.0]) };
        }
    }
    Ok(())
}

fn serve_worker<Fut: Future<Output = Vec<Failure>>>(
    thread: String,
    mut worker: SubInterpreterWorker,
    serve: impl FnOnce(Rc<RefCell<SubInterpreterWorker>>) -> Fut,
) -> Vec<Failure> {
    // SAFETY: this thread now owns the worker, which no other thread has bound.
    worker.tstate =
        unsafe { crate::python::interp::rebind_tstate_to_current_thread(worker.tstate) };
    let worker = Rc::new(RefCell::new(worker));
    let failures = match RuntimeBuilder::new_current_thread().enable_all().build() {
        Ok(rt) => {
            let local = LocalSet::new();
            let failures = local.block_on(&rt, serve(Rc::clone(&worker)));
            // Its tasks hold the other `Rc`s to the worker.
            drop(local);
            failures
        }
        Err(source) => vec![Failure::Runtime { thread, source }],
    };
    SubInterpreterWorker::end_shared(worker);
    failures
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK_HEAD: &[u8] =
        b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\n\r\n";

    #[test]
    fn response_len_is_head_plus_content_length() {
        let mut full = OK_HEAD.to_vec();
        full.extend_from_slice(b"ok");
        assert_eq!(response_len(&full), Ok(Some(OK_HEAD.len() + 2)));
        // Only the head is needed.
        assert_eq!(response_len(OK_HEAD), Ok(Some(OK_HEAD.len() + 2)));
    }

    #[test]
    fn response_len_waits_for_the_whole_head() {
        assert_eq!(response_len(&OK_HEAD[..20]), Ok(None));
    }

    #[test]
    fn missing_content_length_is_an_error_not_an_empty_body() {
        let head = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
        assert_eq!(response_len(head), Err(ResponseError::NoContentLength));
    }

    #[test]
    fn bad_content_length_is_reported_with_its_value() {
        let head = b"HTTP/1.1 200 OK\r\ncontent-length: two\r\n\r\n";
        assert_eq!(
            response_len(head),
            Err(ResponseError::BadContentLength("two".into()))
        );
    }

    #[test]
    fn a_non_200_response_is_an_error() {
        let head = b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nNot Found";
        assert_eq!(response_len(head), Err(ResponseError::Status(404)));
    }

    #[test]
    fn a_panicking_thread_fails_the_group() {
        let mut threads = Threads::new();
        threads
            .spawn("ok".into(), |stop| {
                while !stop.is_cancelled() {
                    std::thread::yield_now();
                }
                Vec::new()
            })
            .unwrap();
        threads
            .spawn("boom".into(), |_| panic!("worker exploded"))
            .unwrap();
        let failures = threads.finish();
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert_eq!(
            failures[0].to_string(),
            "thread boom panicked: worker exploded"
        );
    }

    #[test]
    fn a_thread_reporting_failures_fails_the_group() {
        let mut threads = Threads::new();
        threads
            .spawn("client".into(), |_| {
                vec![Failure::Client {
                    conn: 3,
                    source: ClientError::Closed,
                }]
            })
            .unwrap();
        let failures = threads.finish();
        assert_eq!(
            join_failures(&failures),
            "client connection 3: the server closed the connection"
        );
    }

    #[test]
    fn a_lent_value_is_released_after_its_threads_are_joined() {
        // A stand-in for the site: `Site` holds Python handlers, which a Rust unit test
        // cannot link against.
        let value = Arc::new(AtomicU64::new(0));
        let mut threads = Threads::new();
        // SAFETY: the reference only goes into the thread below.
        let lent = unsafe { threads.lend(Arc::clone(&value)) };
        threads
            .spawn("reader".into(), move |stop| {
                while !stop.is_cancelled() {
                    lent.fetch_add(1, Ordering::Relaxed);
                }
                Vec::new()
            })
            .unwrap();
        assert_eq!(Arc::strong_count(&value), 2);
        drop(threads); // the early-return path: no `finish`
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[tokio::test]
    async fn the_client_reports_a_404_instead_of_counting_it() {
        let (client_io, mut server_io) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move {
            let mut request = vec![0u8; BENCH_REQUEST.len()];
            server_io.read_exact(&mut request).await.unwrap();
            server_io
                .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nNot Found")
                .await
                .unwrap();
            server_io
        });
        let counter = AtomicU64::new(0);
        let result = drive_client(client_io, &counter, &CancellationToken::new()).await;
        assert!(
            matches!(
                result,
                Err(ClientError::Response(ResponseError::Status(404)))
            ),
            "{result:?}"
        );
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        drop(server.await.unwrap());
    }
}
