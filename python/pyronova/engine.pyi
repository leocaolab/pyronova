"""Type stubs for pyronova.engine (Rust extension module)."""

from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Tuple, Union

class LogLevel:
    """The minimum level of what is logged."""

    Off: LogLevel
    Error: LogLevel
    Warn: LogLevel
    Info: LogLevel
    Debug: LogLevel
    Trace: LogLevel
    @staticmethod
    def parse(name: str) -> LogLevel:
        """``"OFF" | "ERROR" | "WARN" | "WARNING" | "INFO" | "DEBUG" | "TRACE"``, in any
        case; anything else raises ``ValueError``."""
        ...

def init_logger(level: LogLevel | str, access_log: bool, format: str) -> None:
    """Install the Rust tracing engine, or reconfigure it if already installed.

    :param level: a ``LogLevel``, or its name (see ``LogLevel.parse``).
    :param format: ``"text"`` (human-readable) or ``"json"`` (structured).
    :raises ValueError: on an unknown level or format.
    :raises RuntimeError: if the subscriber cannot be installed because a
        foreign global tracing subscriber already holds the slot.

    tracing allows one global subscriber per process; a later call applies its
    level, access-log switch and format to that subscriber.
    """
    ...

def emit_python_log(
    levelno: int,
    name: str,
    message: str,
    pathname: str,
    lineno: int,
    worker_id: Optional[int] = None,
) -> None:
    """Route a Python log record through Rust tracing.

    ``levelno`` is the record's numeric level; it maps to the highest standard
    threshold it reaches, so a custom level 25 logs at INFO.
    """
    ...

def _python_log_level() -> Optional[int]:
    """The Python ``logging`` level matching the level ``init_logger`` applied, or
    ``None`` before any ``init_logger``. Every interpreter gates its root logger on it."""
    ...

class Metrics:
    """Snapshot of the engine counters. Times are in microseconds."""

    gil_wait_last_us: int
    gil_wait_peak_us: int
    """Longest GIL acquisition wait since the last ``reset_peaks()``."""
    gil_wait_count: int
    gil_wait_total_us: int
    gil_queue_length: int
    gil_hold_peak_us: int
    """Longest handler GIL hold since the last ``reset_peaks()``."""
    rss_bytes: Optional[int]
    """Process RSS; ``None`` until the sampler (``PYRONOVA_METRICS=1``) reads one."""
    dropped_requests: int
    total_requests: Optional[int]
    """Requests counted; ``None`` while hot-path metrics are off (``PYRONOVA_METRICS`` unset)."""

def get_gil_metrics() -> Metrics:
    """Read every counter. Has no side effect."""
    ...

def reset_peaks() -> None:
    """Clear ``gil_wait_peak_us`` and ``gil_hold_peak_us``."""
    ...

class Headers:
    """The request's header fields: a read-only mapping, names case-insensitive.

    A name sent on several field lines reads as one value joined with ``", "``
    (RFC 9110 §5.3), except ``cookie``, joined with ``"; "`` (RFC 9113
    §8.2.3). ``get_all`` returns each field line as sent.
    """

    def __getitem__(self, name: str) -> str: ...
    def get(self, name: str, default: Optional[str] = None) -> Optional[str]: ...
    def get_all(self, name: str) -> list[str]:
        """Every field line named ``name``, in the order received; ``[]`` if none."""
        ...
    def __contains__(self, name: object) -> bool: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[str]: ...
    def keys(self) -> list[str]: ...
    def values(self) -> list[str]: ...
    def items(self) -> list[tuple[str, str]]: ...

class BodyStream:
    """A ``stream=True`` route's request body, read as it arrives (``req.stream``).

    Iterating yields each chunk as ``bytes``, blocking until it arrives, and stops at the
    body's end. A body the server rejects (larger than ``max_body_size``, too slow, a
    failed read) raises ``BodyRejected``, and every later read raises it again. Two
    threads reading one stream take chunks one at a time, in order.
    """

    def __iter__(self) -> BodyStream: ...
    def __next__(self) -> bytes: ...
    def read(self, n: Optional[int] = None) -> bytes:
        """Reads whole chunks until at least ``n`` bytes, or to the end (all of it when
        ``n`` is ``None``); ``b""`` at the end. Chunks are never split, so the result may
        be longer than ``n``."""
        ...
    def drain_count(self) -> int:
        """Reads the rest of the body without handing it to Python and returns its size
        in bytes.

        :raises BodyRejected: the server rejected the body.
        :raises RuntimeError: the stream was already read to its end.
        """
        ...

class Request:
    """An HTTP request, as a handler and its hooks see it. Immutable."""

    def __init__(
        self,
        method: str,
        path: str,
        params: dict[str, str],
        query: str,
        body_bytes: bytes,
        headers: dict[str, str],
        client_ip: str,
    ) -> None:
        """Builds a request as the server does, with a fresh ``request_id``.

        :raises ValueError: a method, path or query that is not valid in a request line,
            a header that is not a valid field, or a ``client_ip`` that is not an IP
            address.
        """
        ...
    method: str
    path: str
    params: dict[str, str]
    query: str
    authority: str | None
    """The ``host[:port]`` the request was sent to: the HTTP/2 ``:authority`` or an
    absolute-form target's authority, else the ``Host`` header; ``None`` with neither."""
    headers: Headers
    client_ip: str
    request_id: str
    """The request's correlation id: the client's own (``app.enable_request_id()``)
    or one the server minted. A 5xx body and the error's log line carry it."""
    body: bytes
    """The whole body, copied into ``bytes``; ``memoryview(req)`` reads it in place."""
    stream: Optional[BodyStream]
    """The body as it arrives, on a ``stream=True`` route; ``None`` on any other. Taken
    on first access: a second access in the same request raises ``RuntimeError``."""
    query_params: dict[str, str]
    """Query parameters, a new dict on every access. On a duplicate key the FIRST value
    wins (as most proxies and WAFs read it); ``query_params_all()`` keeps every value."""
    def query_params_all(self) -> dict[str, list[str]]:
        """Query parameters with every value of a repeated key, in order."""
        ...
    def query_param(self, key: str) -> Optional[str]:
        """One query parameter (the first value of a repeated key), or ``None``."""
        ...
    def __buffer__(self, flags: int) -> memoryview: ...
    def text(self) -> str:
        """Decode the request body as UTF-8 text.

        Raises ``ValueError`` if the body is not valid UTF-8.
        """
        ...
    def json(self) -> Any:
        """Parse the request body as JSON: an integer of any size stays an exact
        ``int``, a float reads back as the value sent.

        :raises ValueError: the body is not well-formed JSON.
        """
        ...

class Response:
    """A handler's response.

    The body's content type comes from what it is, never from its text: a
    ``dict`` / ``list`` is JSON, a ``str`` is ``text/plain``, ``bytes`` are
    ``application/octet-stream`` — unless ``content_type`` names one.

    A header value is a ``str``, or a list of ``str`` to send the name on
    several lines (e.g. ``Set-Cookie``). A ``Content-Type`` or ``Server`` in
    ``headers`` replaces the default. A non-``str`` value raises
    ``TypeError`` and an invalid one (CR, LF, NUL) ``ValueError``, naming the
    header. ``headers`` reads back with lower-case names.
    """

    body: object
    status_code: int
    content_type: Optional[str]
    headers: dict[str, str | list[str]]
    def __init__(
        self,
        body: object,
        status_code: int = 200,
        content_type: Optional[str] = None,
        headers: Optional[dict[str, str | list[str]]] = None,
    ) -> None: ...

class WebSocket:
    """One WebSocket connection, handed to an ``@app.websocket`` handler.

    Every receive returns ``None`` once the connection has ended (the peer
    closed it, or it was dropped after a read error, which the server logs).

    The ``before_request`` hooks run on the upgrade request before the 101 is
    sent; a hook that returns a response refuses the upgrade with it.
    """

    request: Request
    """The upgrade request (method, path, query, headers such as ``Origin``
    and ``Cookie``, client IP); the object the ``before_request`` hooks saw."""

    def recv_message(self) -> Optional[str | bytes]:
        """Receive the next message: ``str`` for text, ``bytes`` for binary."""
        ...
    def recv(self) -> Optional[str]:
        """Receive the next text message.

        :raises TypeError: if the next message is binary; it stays queued for
            ``recv_bytes()`` / ``recv_message()``.
        """
        ...
    def recv_bytes(self) -> Optional[bytes]:
        """Receive the next binary message.

        :raises TypeError: if the next message is text; it stays queued for
            ``recv()`` / ``recv_message()``.
        """
        ...
    def send(self, msg: str) -> None:
        """Queue a text message. Never blocks.

        :raises ValueError: the message exceeds ``max_websocket_message_size``.
        :raises BlockingIOError: the send buffer is full (the client reads
            slowly); retry after a pause.
        :raises ConnectionError: the connection is closed.
        """
        ...
    def send_bytes(self, data: bytes) -> None:
        """Queue a binary message. Same errors as ``send``."""
        ...
    def close(self) -> None: ...

class Stream:
    """A streamed response body (e.g. server-sent events) a handler returns and then
    writes to. Needs a ``gil=True`` route."""

    status_code: int
    content_type: str
    headers: dict[str, str | list[str]]
    def __init__(
        self,
        content_type: Optional[str] = None,
        status_code: int = 200,
        headers: Optional[dict[str, str | list[str]]] = None,
    ) -> None:
        """``content_type`` defaults to ``text/event-stream``; ``headers`` as for
        ``Response``."""
        ...
    def send(self, data: str) -> None:
        """Sends a raw chunk. Never blocks.

        :raises BlockingIOError: the buffer is full (a slow client); retry after a pause.
        :raises ConnectionError: the stream is closed.
        """
        ...
    def send_event(self, data: str, event: Optional[str] = None, id: Optional[str] = None) -> None:
        """Sends one SSE event: ``id:`` / ``event:`` lines if given, a ``data:`` line per
        line of ``data``. ``ValueError`` for a line break in ``id`` or ``event``."""
        ...
    def close(self) -> None:
        """Ends the response; a later ``send`` raises ``ConnectionError``."""
        ...

class SharedState:
    """Concurrent key-value store shared across all workers / sub-interpreters.

    Backed by a single lock-free concurrent map (Rust ``DashMap``) held behind
    an ``Arc``, so every worker observes the same state in shared memory.

    Concurrency contract:

    - Each individual method call (``__getitem__``, ``__setitem__``,
      ``__delitem__``, ``get``, ``__contains__``, ``incr``, ``decr``, ...) is
      atomic with respect to other calls; there is no need for external
      locking around a single operation.
    - Compound *check-then-act* sequences across two or more calls are **not**
      atomic. For example ``if key not in state: state[key] = v`` can race
      another worker between the membership test and the assignment.
    - For atomic counters use ``incr`` / ``decr`` instead of read-modify-write
      via ``__getitem__`` + ``__setitem__``.
    - Snapshot methods (``keys`` / ``values`` / ``items`` / ``__len__``)
      return a point-in-time view; concurrent writers may change the map
      immediately after the snapshot is taken.
    """

    def __init__(self) -> None:
        """A new, empty map; in a sub-interpreter worker, the running app's map."""
        ...
    def __getitem__(self, key: str) -> str:
        """``state[key]`` — raises ``KeyError`` if the key is absent
        (standard mapping semantics). Use ``get`` for a default instead.

        Every text read (``[]``, ``get``, ``values``, ``items``) raises
        ``TypeError`` naming the key when its value is not UTF-8 (set with
        ``set_bytes``); read it with ``get_bytes``."""
        ...
    def __setitem__(self, key: str, value: str) -> None: ...
    def __delitem__(self, key: str) -> None:
        """``del state[key]`` — raises ``KeyError`` if the key is absent."""
        ...
    def __contains__(self, key: str) -> bool:
        """Whether the key exists, whatever its value."""
        ...
    def get(self, key: str, default: str | None = None) -> str | None: ...
    def set(self, key: str, value: str) -> None: ...
    def set_bytes(self, key: str, value: bytes) -> None: ...
    def get_bytes(self, key: str) -> Optional[bytes]:
        """The raw value, or ``None`` if the key is absent."""
        ...
    def delete(self, key: str) -> bool:
        """Remove the key; whether it existed."""
        ...
    def incr(self, key: str, amount: int) -> int:
        """Atomically add ``amount`` and return the new value; a missing key is created
        with ``amount``. Raises ``TypeError`` if the stored value is not an integer (it is
        never reset)."""
        ...
    def decr(self, key: str, amount: int) -> int:
        """Atomically subtract ``amount`` and return the new value."""
        ...
    def keys(self) -> list[str]: ...
    def values(self) -> list[str]: ...
    def items(self) -> list[tuple[str, str]]: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class Mode:
    """Where non-``gil=True`` handlers run."""

    Gil: Mode
    """Every handler on the main interpreter."""
    Subinterp: Mode
    """Handlers in sub-interpreter workers; ``gil=True`` routes on the main interpreter."""
    @staticmethod
    def parse(name: str) -> Mode:
        """``"gil"`` (or ``"default"``) / ``"subinterp"`` (or ``"auto"``); anything else
        raises ``ValueError``."""
        ...
    @property
    def uses_workers(self) -> bool: ...

class Compression:
    """An app's response-compression settings, validated when made."""

    def __init__(
        self,
        *,
        min_size: int = 512,
        gzip: bool = True,
        brotli: bool = True,
        gzip_level: int = 6,
        brotli_quality: int = 4,
    ) -> None:
        """:raises ValueError: ``gzip_level`` outside 1..=9, ``brotli_quality`` outside
        0..=11, a negative ``min_size``, or neither algorithm enabled."""
        ...
    @property
    def min_size(self) -> int: ...
    @property
    def gzip(self) -> bool: ...
    @property
    def brotli(self) -> bool: ...
    @property
    def gzip_level(self) -> int: ...
    @property
    def brotli_quality(self) -> int: ...

class PyronovaApp:
    """The engine app ``Pyronova`` wraps: routes, hooks, limits, and ``start``/``run``.

    Route registration raises ``ValueError`` for a bad path, a duplicate route, or
    ``stream=True`` without ``gil=True`` or on an ``async def`` handler (a streamed body
    is fed only to a sync handler on the main interpreter); ``RegistrationSealed`` for a
    route other than ``gil=True`` registered after the first server started.
    """

    def __init__(self) -> None: ...
    def get(self, path: str, handler: Callable[..., Any], gil: bool = False) -> None: ...
    def post(self, path: str, handler: Callable[..., Any], gil: bool = False, stream: bool = False) -> None: ...
    def put(self, path: str, handler: Callable[..., Any], gil: bool = False, stream: bool = False) -> None: ...
    def delete(self, path: str, handler: Callable[..., Any], gil: bool = False) -> None: ...
    def route(self, method: str, path: str, handler: Callable[..., Any], gil: bool = False, stream: bool = False) -> None: ...
    def add_fast_response(
        self,
        method: str,
        path: str,
        body: bytes,
        content_type: str = "text/plain",
        status_code: int = 200,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        """Serve a constant response for exactly ``(method, path)`` without entering
        Python. ``ValueError`` for an invalid status or header, or a pair already
        registered."""
        ...
    def before_request(self, handler: Callable[..., Any]) -> None:
        """:raises RegistrationSealed: registered after the first server started."""
        ...
    def after_request(self, handler: Callable[..., Any]) -> None:
        """:raises RegistrationSealed: registered after the first server started."""
        ...
    def fallback(self, handler: Callable[..., Any]) -> None:
        """:raises RegistrationSealed: registered after the first server started."""
        ...
    def websocket(self, path: str, handler: Callable[..., Any]) -> None:
        """:raises ValueError: a handler is already registered for ``path``."""
        ...
    def set_max_body_size(self, size: int) -> None:
        """This app's largest request body, in bytes (413 above it). Per app."""
        ...
    def configure_compression(self, settings: Compression | None) -> None:
        """This app's response compression; ``None`` turns it off. Per app."""
        ...
    def max_body_size(self) -> int: ...
    def set_max_websocket_message_size(self, size: int) -> None:
        """Per app; raises ``ValueError`` outside ``1..=2**32-65``."""
        ...
    def max_websocket_message_size(self) -> int: ...
    def set_max_websocket_connections(self, count: int) -> None:
        """Per app (counted per server run); raises ``ValueError`` below 1."""
        ...
    def max_websocket_connections(self) -> int: ...
    def static_dir(self, prefix: str, directory: str) -> None:
        """Serve files under ``directory`` at URL ``prefix``.

        :raises ValueError: ``prefix`` does not start with ``/``, or
            ``directory`` does not resolve to a directory.

        The request path is percent-decoded, then refused with 403 if it
        climbs out with ``..`` (literal or encoded) or resolves outside
        ``directory`` through a symlink. A missing file falls through to
        routing (404); an unreadable one is 403; any other IO error is
        logged and answered 500.
        """
        ...
    def set_cors_config(
        self,
        origin: str,
        methods: str,
        headers: str,
        expose_headers: Optional[str] = None,
        allow_credentials: bool = False,
    ) -> None:
        """Apply these CORS headers to every response.

        :raises ValueError: a value is not a valid header value (e.g. contains
            a newline).
        """
        ...
    def enable_request_logging(self, enabled: bool) -> None: ...
    def set_request_log_sampling(
        self, sample_n: int = 1, always_status: Optional[int] = None
    ) -> None:
        """Log 1 in ``sample_n`` requests; responses with a status at or above
        ``always_status`` always log. ``ValueError`` for ``sample_n < 1`` or an
        ``always_status`` that isn't an HTTP status (100-999)."""
        ...
    def set_request_id_header(self, header: str) -> None:
        """Take ``req.request_id`` from the client's ``header`` when it sends a
        usable one (visible ASCII, at most 128 bytes); otherwise the server
        mints one. ``ValueError`` for an invalid header name."""
        ...
    def enable_grpc_benchmark(self) -> None:
        """Answer ``POST /benchmark.BenchmarkService/GetSum`` (``application/grpc*``)
        with the built-in HttpArena service. Off by default."""
        ...
    @property
    def state(self) -> SharedState: ...
    def _seal_registrations(self) -> None:
        """Main interpreter only, idempotent: marks the end of the script's registrations.
        A route registered after it must be ``gil=True``."""
        ...
    def set_script_path(self, path: str) -> None:
        """The script sub-interpreter workers execute, when it isn't
        ``__main__.__file__`` (the CLI sets it to the app's module)."""
        ...
    def isolate(self, libraries: List[str]) -> None:
        """Give each sub-interpreter worker its own private copy of these C-extension
        libraries, cloned before the script runs in it. ``ValueError`` for
        ``pyronova``, which is never isolated."""
        ...
    def _take_abandoned_workers(self) -> List[AbandonedWorker]:
        """The worker threads this app's servers abandoned at shutdown (still
        running past the grace period); taking them clears the list."""
        ...
    def start(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        workers: Optional[int] = None,
        mode: Union[Mode, str, None] = None,
        io_workers: Optional[int] = None,
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
        extra_tls_ports: Optional[List[int]] = None,
    ) -> Server:
        """Prepare one server of this app and bind its listeners; ``Server.serve()`` runs
        it. ``mode`` defaults to ``Mode.Gil``.

        ``host`` is an IP address (``"127.0.0.1"``, ``"::1"``, ``"[::1]"``); anything
        else raises ``ValueError`` naming it. ``workers`` / ``io_workers`` of 0 raise
        ``ValueError``. The engine's environment variables (``PYRONOVA_TPC``,
        ``PYRONOVA_GC_*``, ``PYRONOVA_GIL_BRIDGE_*``, ``PYRONOVA_METRICS``,
        ``PYRONOVA_TPC_DARWIN``) are parsed once here; an invalid mode or value raises
        ``ValueError`` before anything starts.

        With ``extra_tls_ports``, ``port`` serves plain HTTP and each extra port serves
        TLS (``tls_cert``/``tls_key`` required); without them ``port`` serves TLS when a
        certificate is given. Every listener is bound here, before any thread or worker
        starts: a port another server listens on (in this process or another, with or
        without ``SO_REUSEPORT``) raises ``OSError`` (``errno.EADDRINUSE``)."""
        ...
    def run(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        workers: Optional[int] = None,
        mode: Union[Mode, str, None] = None,
        io_workers: Optional[int] = None,
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
        extra_tls_ports: Optional[List[int]] = None,
    ) -> None:
        """``start(...).serve()``: bind, then serve until SIGINT."""
        ...
    # Feature-gated: present only in an engine built with
    # `maturin develop --release --features bench` (absent from the default build and
    # from published wheels).
    def bench_inmem(
        self,
        duration_s: int = 10,
        workers: Optional[int] = None,
        conns_per_worker: int = 8,
    ) -> Tuple[int, float]:
        """``--features bench`` only. In-memory bench (no TCP): pipelines ``GET /``.
        Returns ``(requests, elapsed_s)``.

        :raises ValueError: ``workers`` is 0.
        :raises RuntimeError: a route is ``gil=True``, ``async def`` or ``stream=True``;
            a worker failed to start; or a worker or client failed during the run (a
            non-200 response included).
        """
        ...
    def bench_loopback(
        self,
        duration_s: int = 10,
        workers: Optional[int] = None,
        client_conns: int = 32,
    ) -> Tuple[int, float, int]:
        """``--features bench`` only. Real TCP on an ephemeral 127.0.0.1 port, client in
        this process: pipelines ``GET /``. Returns ``(requests, elapsed_s, port)``.

        :raises ValueError: ``workers`` is 0.
        :raises RuntimeError: as for :meth:`bench_inmem`.
        """
        ...

# --- Postgres (see pyronova.db) ---------------------------------------------

class DatabaseError(RuntimeError):
    """A query failed. ``sqlstate`` is the server's SQLSTATE code, or None when the
    server never reported one (pool timeout, dropped connection)."""

    sqlstate: Optional[str]

class IntegrityError(DatabaseError):
    """An integrity constraint was violated (SQLSTATE class 23)."""

class UniqueViolation(IntegrityError):
    """A unique or primary-key constraint was violated (SQLSTATE 23505)."""

class ParamError(TypeError, ValueError):
    """A value a statement parameter can't take (wrong type, out of range, not
    encodable), refused before the query is sent."""

class PgCursor:
    """Streaming result set from ``PgPool.fetch_iter``; yields one dict per row."""

    def __iter__(self) -> "PgCursor": ...
    def __next__(self) -> Dict[str, Any]: ...

class PgPool:
    """The process's Postgres pool. Parameters are encoded as the statement declares
    them; a query failure raises ``DatabaseError``."""

    @classmethod
    def connect(
        cls,
        dsn: str,
        max_connections: Optional[int] = None,
        acquire_timeout_secs: Optional[int] = None,
    ) -> "PgPool":
        """Open the pool, or return the open one. Raises ``ValueError`` if it is open
        with another DSN or with settings that differ from the ones asked for; settings
        left out match the open pool. Defaults on the first call: 10 connections, 30 s.
        A failed connect raises ``ConnectionError`` with ``.sqlstate``: the server's code
        when it answered (``28P01`` bad password, ``3D000`` no such database), else ``None``."""
        ...
    def fetch_one(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]: ...
    def fetch_all(self, sql: str, *params: Any) -> List[Dict[str, Any]]: ...
    def fetch_scalar(self, sql: str, *params: Any) -> Any: ...
    def execute(self, sql: str, *params: Any) -> int: ...
    def fetch_iter(self, sql: str, *params: Any) -> PgCursor: ...
    def fetch_one_async(self, sql: str, *params: Any) -> Awaitable[Optional[Dict[str, Any]]]: ...
    def fetch_all_async(self, sql: str, *params: Any) -> Awaitable[List[Dict[str, Any]]]: ...
    def fetch_scalar_async(self, sql: str, *params: Any) -> Awaitable[Any]: ...
    def execute_async(self, sql: str, *params: Any) -> Awaitable[int]: ...

def _in_worker() -> bool:
    """Whether this code runs in a sub-interpreter worker (not the main interpreter)."""
    ...

class Server:
    """One server of an app, bound by ``PyronovaApp.start()``."""

    @property
    def port(self) -> int:
        """The port it listens on (its first listener; a ``port=0`` resolved to the
        kernel's pick)."""
        ...
    def serve(self) -> None:
        """Serve until SIGINT or ``shutdown()``, blocking this thread; then drain the
        in-flight connections and return. A server stopped before it served returns at
        once. :raises RuntimeError: it has already served."""
        ...
    def shutdown(self) -> None:
        """Stop this server as SIGINT does. Callable from any thread, before or during
        ``serve()``; idempotent. Other servers of the same app keep serving."""
        ...

class RegistrationSealed(ValueError):
    """A route (other than ``gil=True``), hook or fallback registered after the app's
    registrations were sealed (the first server's start). Workers rebuild the app by
    running its script, so they would never see it."""

class BodyRejected(OSError):
    """A streamed request body (``req.stream``) was rejected as a buffered one would be:
    larger than ``max_body_size``, too slow, or a failed read. Left uncaught, the request
    gets the buffered body's 413 / 408 / 400."""

def _route_params(path: str) -> List[str]:
    """The parameter names of a route path, in order (``{*rest}`` gives ``rest``).
    ``ValueError`` for a ``:name`` segment, which the router would take literally."""
    ...


class AbandonedWorker:
    """A sub-interpreter worker thread a server's shutdown gave up on: still running past
    the grace period. ``str()`` gives ``"<thread> (running <METHOD path>)"``."""

    @property
    def thread(self) -> str: ...
    @property
    def route(self) -> Optional[str]:
        """``METHOD path`` of the route its sync worker was running, if any."""
        ...

class WorkerException(Exception):
    """An exception a sub-interpreter worker raised, as the traceback it printed there.
    The ``__cause__`` of the error a failed worker start raises on the main interpreter
    (``ImportError`` / ``ModuleNotFoundError`` for an import that failed in the worker,
    ``RuntimeError`` otherwise)."""


def workrequest_counts() -> Tuple[int, int]:
    """``(created, completed)`` worker requests, for leak checks; ``(0, 0)`` unless the
    engine was built with ``--features leak_detect``."""
    ...

# Called by the async engine in sub-interpreter workers; not a public API.
class _AsyncInbox: ...
class _AsyncJob:
    @property
    def route(self) -> int: ...
    @property
    def request(self) -> Request: ...
class _WorkerHooks:
    @property
    def before(self) -> List[Callable[..., Any]]: ...
    @property
    def after(self) -> List[Callable[..., Any]]: ...
def _worker_recv(inbox: _AsyncInbox) -> Optional[_AsyncJob]: ...
def _worker_close(inbox: _AsyncInbox) -> None: ...
def _worker_send(job: _AsyncJob, response: Any) -> None: ...
def _worker_fail(job: _AsyncJob, exception: BaseException) -> None: ...
def _worker_timed_out(job: _AsyncJob) -> None: ...
def _worker_to_response(value: Any) -> Response: ...
def _worker_app_handlers() -> List[Callable[..., Any]]: ...
def _worker_app_hooks() -> _WorkerHooks: ...
