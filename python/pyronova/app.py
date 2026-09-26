"""High-level Pyronova application class with decorator syntax."""

from __future__ import annotations

import time
import sys
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Literal, TypedDict
import inspect
import json as _json_module

import os

from pyronova.engine import Compression, LogLevel, Mode, PyronovaApp as _PyronovaApp, Response, SharedState, init_logger, emit_python_log, _in_worker, _python_log_level, _route_params
from pyronova._log_bridge import RustLogHandler, root_level
from pyronova import _csrf
from pyronova._csrf import OriginPolicy
from pyronova.mcp import MCPServer
from pyronova import _reload
import logging as _logging

if TYPE_CHECKING:
    from pyronova.health import ReadinessCheck


# A level by name, in either case; `LogLevel.parse` reads it.
LogLevelName = Literal[
    "off", "error", "warn", "warning", "info", "debug", "trace",
    "OFF", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "TRACE",
]


class LogConfig(TypedDict, total=False):
    """Logging configuration dictionary.

    Keys:
        level: a ``LogLevel``, or its name ("OFF", "ERROR", "WARN", "INFO", "DEBUG",
            "TRACE"); an unknown name raises ``ValueError`` in ``Pyronova()``
        access_log: Whether to log every HTTP request (method, path, status, latency)
        format: "text" (human-readable) or "json" (structured, for ELK/Datadog)
    """
    level: LogLevel | LogLevelName
    access_log: bool
    format: Literal["text", "json"]


# `/mcp` takes a JSON-RPC body only (MCP's HTTP transport).
_MCP_BODY_TYPES = {"application/json": "application/json"}


def _log_level(level: LogLevel | LogLevelName) -> LogLevel:
    return level if isinstance(level, LogLevel) else LogLevel.parse(level)

@dataclass(frozen=True)
class RouteInfo:
    """A registered route, as ``app.routes`` lists it."""

    method: str
    path: str
    handler: str  # the handler's qualified name
    gil: bool
    stream: bool
    model: str | None  # the ``model=`` class's name
    is_async: bool


@dataclass(frozen=True)
class FastRouteInfo:
    """A route registered with ``add_fast_response``, as ``app.fast_routes`` lists it."""

    method: str
    path: str
    status_code: int
    content_type: str
    body_bytes: int


def _is_worker() -> bool:
    """Whether this code runs in a sub-interpreter worker (not the main interpreter)."""
    return _in_worker()


# Env vars BLAS libraries read for their thread-pool size. If the user set any
# of them, their choice stands and pyronova changes nothing.
_BLAS_THREAD_VARS = (
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)


def _limit_blas_threads() -> str:
    """Give each worker a single-threaded BLAS; return what was done, for the banner.

    Every worker calls into the same BLAS library, and by default it runs its
    own thread pool sized to all cores, so N workers put N × cores threads on
    the cores. Measured on 16 cores, 4 own-GIL workers each running
    `np.linalg.inv`: 82 inversions in 25 s, against 128,873 with one BLAS
    thread. The same holds for gunicorn/uvicorn multi-process deployments.

    BLAS reads the env vars when it loads, so setting them covers libraries
    loaded later; a library already loaded (numpy imported at the top of the
    app) is changed at run time through threadpoolctl."""
    user_set = [v for v in _BLAS_THREAD_VARS if v in os.environ]
    if user_set:
        return f"left as set by {', '.join(user_set)}"
    for v in _BLAS_THREAD_VARS:
        os.environ[v] = "1"
    try:
        from threadpoolctl import threadpool_limits
    except ImportError:
        if "numpy" in sys.modules or "scipy" in sys.modules:
            _logging.getLogger("pyronova.app").warning(
                "numpy/scipy was imported before app.run(), so its BLAS already "
                "started a thread pool sized to all cores, and without threadpoolctl "
                "pyronova can't shrink it; N workers will contend for the cores. "
                "Fix: `pip install threadpoolctl`, or start the app with "
                "OPENBLAS_NUM_THREADS=1."
            )
            return "1 thread per worker for BLAS loaded from now on (already-loaded BLAS unchanged: no threadpoolctl)"
        return "1 thread per worker"
    threadpool_limits(limits=1, user_api="blas")
    return "1 thread per worker"


def _level_with_access_log(
    current: LogLevel, requested: LogLevel | None, pinned: bool
) -> LogLevel:
    """The log level once the access log is on: the level asked for; else the current
    one, raised to INFO when it would hide the access lines (ERROR, OFF), unless an
    explicit ``enable_logging(level=...)`` chose it."""
    if requested is not None:
        return requested
    if pinned or current not in (LogLevel.Error, LogLevel.Off):
        return current
    return LogLevel.Info


def _require_int(name: str, value: object) -> None:
    """``bool`` is an ``int`` subclass; a limit set to ``True`` is a bug, not 1."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be int, got {type(value).__name__}")


def _setup_python_logging_bridge() -> None:
    """Route the root logger through Rust tracing, gated at the level ``init_logger``
    applied (``_python_log_level()``, the same source every worker reads).

    Adds the one ``RustLogHandler`` (not twice), and keeps the user's other handlers
    (Sentry, DataDog...). Filtering, formatting and I/O happen in Rust; a record below
    the level is rejected by Python before ``getMessage()`` or the FFI call.
    """
    root = _logging.getLogger()
    if not any(isinstance(h, RustLogHandler) for h in root.handlers):
        root.addHandler(RustLogHandler(None, emit_python_log))
    root.setLevel(root_level(_python_log_level()))


class Pyronova:
    """Pyronova web framework — decorator-friendly wrapper around the Rust engine.

    Usage::

        from pyronova import Pyronova

        app = Pyronova()

        @app.get("/")
        def index(req):
            return "Hello from Pyronova!"

        app.run()

    Auto-detects ``def`` vs ``async def`` and routes to the right pool::

        @app.get("/fast")
        def fast(req):              # → sync pool (220k req/s)
            return "hello"

        @app.get("/io")
        async def io(req):          # → async pool (133k req/s)
            await asyncio.sleep(0.1)
            return "done"

        @app.get("/numpy", gil=True)
        def compute(req):           # → GIL main interpreter
            import numpy as np
            return {"mean": float(np.mean([1,2,3]))}

        app.run()                   # zero config, auto dual-pool
    """

    def __init__(
        self,
        debug: bool = False,
        log_config: LogConfig | None = None,
    ) -> None:
        self._engine = _PyronovaApp()
        self._fallback_handler: Callable | None = None
        self._fallback_name: str | None = None
        self._mcp = MCPServer()
        self.debug = debug
        self._startup_hooks: list[Callable] = []
        self._shutdown_hooks: list[Callable] = []
        self._routes_meta: list[RouteInfo] = []
        self._fast_routes_meta: list[FastRouteInfo] = []
        self._readiness_checks: list[ReadinessCheck] = []
        self._health_probes_enabled: bool = False
        self._app_file_path: str | None = None
        self._origin_policy = OriginPolicy()

        # Resolve final logging config: debug mode defaults vs production defaults.
        # Actual init_logger call is deferred to run() so enable_logging() can
        # adjust the config before the tracing subscriber is locked in.
        user = log_config or {}
        if self.debug:
            self._log_config: LogConfig = {
                "level": _log_level(user.get("level", LogLevel.Debug)),
                "access_log": user.get("access_log", True),
                "format": user.get("format", "text"),
            }
        else:
            self._log_config: LogConfig = {
                "level": _log_level(user.get("level", LogLevel.Error)),
                "access_log": user.get("access_log", False),
                "format": user.get("format", "json"),
            }
        # Whether enable_logging(level=...) chose the level (it then keeps it).
        self._log_level_pinned = False
        # _prepare()'s state, under its lock: two servers of one app may start at once
        # (nested TestClients), and each must find the app prepared exactly once.
        self._prepare_lock = threading.Lock()
        self._prepared = False
        self._mcp_route_registered = False
        # The servers this app is serving (engine `Server`s), for `_stop()`.
        self._servers_lock = threading.Lock()
        self._servers: set = set()
        self._defined_in = _defining_module_file(self)
        # Guards the idempotency check-then-set in the enable_* helpers so
        # concurrent startup hooks/threads can't both pass the "already
        # enabled?" check and double-register routes/hooks (arc findings
        # app-36/37/38).
        self._enable_lock = threading.Lock()

    @property
    def mcp(self) -> MCPServer:
        """MCP (Model Context Protocol) server for AI tool integration."""
        return self._mcp

    @property
    def max_body_size(self) -> int:
        """Max request body size in bytes; a larger body is answered 413. Default: 10 MB.
        Per app: another app in the same process keeps its own."""
        return self._engine.max_body_size()

    @max_body_size.setter
    def max_body_size(self, size: int) -> None:
        """Set max request body size. Example: ``app.max_body_size = 50 * 1024 * 1024``"""
        # Type hint is unenforced; explicit check so a non-int / negative
        # value fails here (clear ValueError) instead of passing through
        # to Rust and producing opaque behavior (arc finding app-10).
        if not isinstance(size, int) or isinstance(size, bool):
            raise TypeError(
                f"max_body_size must be int, got {type(size).__name__}"
            )
        if size < 0:
            raise ValueError(f"max_body_size must be non-negative, got {size}")
        self._engine.set_max_body_size(size)

    @property
    def trusted_origins(self) -> list[str]:
        """Other sites whose pages may call ``/mcp`` and ``@app.rpc`` endpoints from a
        browser, as ``scheme://host[:port]``. Default: none.

        Those endpoints act on a JSON (or MsgPack / Protobuf) POST, so they refuse a
        request whose ``Origin`` is another site with 403, and any other body type with
        415: a page can't make a visitor's browser call them (CSRF). Requests without an
        ``Origin`` (curl, SDKs, servers) and from this server's own host are always
        admitted. A bad entry raises ``ValueError``.
        """
        return sorted(
            f"{o.scheme}://{o.host}:{o.port}" for o in self._origin_policy.trusted
        )

    @trusted_origins.setter
    def trusted_origins(self, origins: list[str]) -> None:
        self._origin_policy = OriginPolicy.of(origins)

    @property
    def max_websocket_message_size(self) -> int:
        """Largest WebSocket message, in bytes, in either direction. Default: 1 MiB.

        A bigger client message closes the connection with 1009 (Message Too Big);
        a bigger ``ws.send`` raises ``ValueError``. Each connection also buffers at
        most about this many bytes per direction.
        """
        return self._engine.max_websocket_message_size()

    @max_websocket_message_size.setter
    def max_websocket_message_size(self, size: int) -> None:
        _require_int("max_websocket_message_size", size)
        self._engine.set_max_websocket_message_size(size)

    @property
    def max_websocket_connections(self) -> int:
        """Concurrent WebSocket connections (one handler thread each). Default: 1024.

        An upgrade beyond the cap is answered ``503 Service Unavailable``.
        """
        return self._engine.max_websocket_connections()

    @max_websocket_connections.setter
    def max_websocket_connections(self, count: int) -> None:
        _require_int("max_websocket_connections", count)
        self._engine.set_max_websocket_connections(count)

    def enable_compression(
        self,
        *,
        min_size: int = 512,
        gzip: bool = True,
        brotli: bool = True,
        gzip_level: int = 6,
        brotli_quality: int = 4,
    ) -> None:
        """Enable gzip / brotli response compression for this app.

        Disabled by default; call once at startup to turn on. Per app: another app in
        the same process keeps its own setting. The server negotiates with the client's
        ``Accept-Encoding`` header and prefers brotli when both are enabled. Skips
        responses under ``min_size``, non-text content types (images, octet-stream),
        streaming responses (SSE), and responses that set ``Content-Encoding``
        explicitly. A large body is compressed off the I/O threads.

        Args:
            min_size: minimum body size (bytes) to compress. Default 512.
            gzip: enable gzip (``Content-Encoding: gzip``). Default True.
            brotli: enable brotli (``Content-Encoding: br``). Default True.
            gzip_level: 1..=9, default 6 (balanced speed/ratio).
            brotli_quality: 0..=11, default 4 (production sweet spot).

        :raises ValueError: a level out of its range, a negative ``min_size``, or
            ``gzip=False, brotli=False`` (nothing to enable; use
            ``disable_compression()``).
        """
        self._engine.configure_compression(
            Compression(
                min_size=min_size,
                gzip=gzip,
                brotli=brotli,
                gzip_level=gzip_level,
                brotli_quality=brotli_quality,
            )
        )

    def disable_compression(self) -> None:
        """Disable response compression. No-op if already disabled."""
        self._engine.configure_compression(None)

    def enable_grpc_benchmark(self) -> None:
        """Serve HttpArena's ``benchmark.BenchmarkService/GetSum`` gRPC method.

        Off by default. When enabled, only a ``POST`` to exactly
        ``/benchmark.BenchmarkService/GetSum`` with an ``application/grpc*``
        content-type is answered by the built-in service; every other request,
        gRPC or not, is routed to your handlers as usual.
        """
        self._engine.enable_grpc_benchmark()

    def add_fast_response(
        self,
        method: str,
        path: str,
        body: bytes | str,
        *,
        content_type: str = "text/plain",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Register a route that returns a pre-built response without
        entering Python.

        Use for constant-body endpoints — health checks, ``/robots.txt``,
        ``/pipeline`` probe routes, maintenance pages. The accept loop
        serves the exact bytes stored here on every match, skipping GIL
        acquisition, handler dispatch, and response serialization.

        Match is exact ``(method, path)`` — no path parameters, no
        globbing. If you need those, register a normal decorator route.

        Example::

            app.add_fast_response("GET", "/health", b'{"ok":true}',
                                  content_type="application/json")
            app.add_fast_response("GET", "/robots.txt",
                                  b"User-agent: *\\nDisallow: /\\n")

        :raises ValueError: ``status_code`` is not an HTTP status (100-999),
            or a header name or value (``content_type`` included) is invalid.
        """
        if isinstance(body, str):
            body = body.encode("utf-8")
        self._engine.add_fast_response(
            method.upper(),
            path,
            body,
            content_type=content_type,
            status_code=status_code,
            headers=headers,
        )
        self._fast_routes_meta.append(FastRouteInfo(
            method=method.upper(),
            path=path,
            status_code=status_code,
            content_type=content_type,
            body_bytes=len(body),
        ))

    @property
    def state(self) -> SharedState:
        """Shared state across all sub-interpreters (nanosecond latency).

        Usage::

            app.state["session:user_1"] = json.dumps({"role": "admin"})
            data = json.loads(app.state["session:user_1"])
        """
        return self._engine.state

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # C-extension isolation (per-worker library copies)
    # ------------------------------------------------------------------

    def isolate(self, *libraries: str) -> None:
        """Give each own-GIL sub-interpreter worker its OWN private copy of these
        C-extension libraries (e.g. ``numpy``, ``scipy``, ``scikit-learn``,
        ``orjson``), so extensions holding process-global state can run isolated
        across workers instead of colliding on "cannot load module more than once".

        Call it at module top-level — it runs inside every sub-interpreter at
        worker init (and is a no-op in the main interpreter). Declare a library's
        C dependencies too, e.g. ``app.isolate("numpy", "scipy", "scikit-learn")``,
        since each copy resolves imports from its own path first.

        Copies are cloned copy-on-write (APFS ``cp -c`` / Linux ``cp --reflink=auto``),
        so disk is near-free; memory is ~one full lib set per worker
        (see docs/subinterp-c-extension-status.md).
        """
        # This records the libraries on the engine app; the workers of its runs get the
        # list at init, and their bootstrap clones them before the script runs. A worker
        # executing this again records it on its own app, which nothing reads.
        self._engine.isolate(list(libraries))

    # ------------------------------------------------------------------
    # Route registration (decorator + direct call)
    # ------------------------------------------------------------------

    def get(self, path: str, handler: Callable | None = None, *, gil: bool = False, model: type | None = None) -> Callable:
        return self._route("GET", path, handler, gil=gil, model=model)

    def post(self, path: str, handler: Callable | None = None, *, gil: bool = False, model: type | None = None, stream: bool = False) -> Callable:
        return self._route("POST", path, handler, gil=gil, model=model, stream=stream)

    def put(self, path: str, handler: Callable | None = None, *, gil: bool = False, model: type | None = None, stream: bool = False) -> Callable:
        return self._route("PUT", path, handler, gil=gil, model=model, stream=stream)

    def delete(self, path: str, handler: Callable | None = None, *, gil: bool = False, model: type | None = None) -> Callable:
        return self._route("DELETE", path, handler, gil=gil, model=model)

    def patch(self, path: str, handler: Callable | None = None, *, gil: bool = False, model: type | None = None, stream: bool = False) -> Callable:
        return self._route("PATCH", path, handler, gil=gil, model=model, stream=stream)

    def options(self, path: str, handler: Callable | None = None, *, gil: bool = False, model: type | None = None) -> Callable:
        return self._route("OPTIONS", path, handler, gil=gil, model=model)

    def head(self, path: str, handler: Callable | None = None, *, gil: bool = False, model: type | None = None) -> Callable:
        return self._route("HEAD", path, handler, gil=gil, model=model)

    def route(self, method: str, path: str, handler: Callable | None = None, *, gil: bool = False, model: type | None = None, stream: bool = False) -> Callable:
        return self._route(method.upper(), path, handler, gil=gil, model=model, stream=stream)

    def _route(self, method: str, path: str, handler: Callable | None, *, gil: bool = False, model: type | None = None, stream: bool = False) -> Callable:
        def register(fn: Callable) -> Callable:
            bound = _bind_handler(fn, path, model)
            self._engine.route(method, path, bound, gil, stream)
            self._routes_meta.append(RouteInfo(
                method=method,
                path=path,
                handler=getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn))),
                gil=gil,
                stream=stream,
                model=model.__name__ if model is not None else None,
                is_async=inspect.iscoroutinefunction(fn),
            ))
            # The bound callable, not fn: sub-interp workers find a route's
            # handler by its module-global name, which must be what the route calls.
            return bound

        return register(handler) if handler is not None else register

    # ------------------------------------------------------------------
    # Middleware
    # ------------------------------------------------------------------

    def enable_cors(
        self,
        *,
        allow_origins: str | list[str] = "*",
        allow_methods: str | list[str] = "GET, POST, PUT, DELETE, PATCH, OPTIONS",
        allow_headers: str | list[str] = "*",
        expose_headers: str | list[str] = "",
        allow_credentials: bool = False,
        max_age: int = 86400,
    ) -> None:
        """Enable CORS (Cross-Origin Resource Sharing).

        Usage::

            app.enable_cors()  # Allow all origins

            app.enable_cors(
                allow_origins=["https://example.com", "https://app.example.com"],
                allow_credentials=True,
            )
        """
        def _normalize_cors_value(name: str, value: object) -> str:
            # Contract is `str | list[str]`, but accept any non-str
            # sequence (e.g. tuple) too. Reject everything else loudly
            # so a bad value can never reach `.split(",")` below as a
            # non-str and blow up with an opaque AttributeError.
            if isinstance(value, str):
                return value
            if isinstance(value, (list, tuple)):
                # str(o) would silently coerce ints/None/objects into
                # syntactically-valid-but-wrong header tokens (e.g.
                # [None, 8080] -> "None, 8080"). Reject mixed types loudly
                # so a typo fails at config time, not as a broken header.
                if not all(isinstance(o, str) for o in value):
                    bad = next(o for o in value if not isinstance(o, str))
                    raise TypeError(
                        f"CORS: {name} list/tuple must contain only str, "
                        f"got {type(bad).__name__}"
                    )
                return ", ".join(value)
            raise TypeError(
                f"CORS: {name} must be a str or list/tuple of str, "
                f"got {type(value).__name__}"
            )

        allow_origins = _normalize_cors_value("allow_origins", allow_origins)
        allow_methods = _normalize_cors_value("allow_methods", allow_methods)
        allow_headers = _normalize_cors_value("allow_headers", allow_headers)
        expose_headers = _normalize_cors_value("expose_headers", expose_headers)

        # W3C CORS spec forbids `Access-Control-Allow-Origin: *` together
        # with `Access-Control-Allow-Credentials: true` — browsers silently
        # drop credentials when this pair is emitted, turning the
        # misconfiguration into an invisible auth outage. Fail loudly at
        # enable_cors time instead of shipping a broken preflight.
        if allow_credentials and any(
            o.strip() == "*" for o in allow_origins.split(",")
        ):
            raise ValueError(
                "CORS: allow_origins='*' is incompatible with "
                "allow_credentials=True. W3C spec forbids this pair — "
                "browsers drop credentials silently. Specify explicit "
                "origins, or set allow_credentials=False."
            )

        cors_headers = {
            "access-control-allow-origin": allow_origins,
            "access-control-allow-methods": allow_methods,
            "access-control-allow-headers": allow_headers,
        }
        if expose_headers:
            cors_headers["access-control-expose-headers"] = expose_headers
        if allow_credentials:
            cors_headers["access-control-allow-credentials"] = "true"
        if max_age:
            cors_headers["access-control-max-age"] = str(max_age)

        # Handle preflight OPTIONS + add CORS headers to all responses
        def _cors_before(req):
            if req.method == "OPTIONS":
                return Response(body="", status_code=204, headers=cors_headers)
            return None

        self._engine.before_request(_cors_before)

        # CORS response headers are applied in Rust layer only (handlers.rs)
        # to avoid duplicate headers which violate W3C CORS spec.
        # Pass full config so allow_credentials + expose_headers appear on
        # every response (GET/POST/etc.), not just OPTIONS preflight.
        self._engine.set_cors_config(
            allow_origins,
            allow_methods,
            allow_headers,
            expose_headers or None,
            allow_credentials,
        )

    # ------------------------------------------------------------------

    def rpc(self, path: str, *, proto_model: type | None = None) -> Callable:
        """Register an RPC endpoint with content negotiation.

        Supports MsgPack, JSON, and optional Protobuf auto-decode/encode.

        Usage::

            @app.rpc("/rpc/get_data")
            def get_data(req):
                return {"prices": [150.1, 150.2]}
        """
        from pyronova.rpc import rpc_decorator
        return rpc_decorator(self, path, proto_model)

    # ------------------------------------------------------------------

    def before_request(self, handler: Callable | None = None) -> Callable:
        """Register a before-request hook. Use as decorator or direct call.

        The hook receives ``(request)`` and should return ``None`` to continue
        or a response to short-circuit.
        """
        if handler is not None:
            self._engine.before_request(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._engine.before_request(fn)
            return fn

        return decorator

    def after_request(self, handler: Callable | None = None) -> Callable:
        """Register an after-request hook. Use as decorator or direct call.

        The hook receives ``(request, response)`` and must return a
        ``Response``.
        """
        if handler is not None:
            self._engine.after_request(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._engine.after_request(fn)
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    def on_startup(self, handler: Callable | None = None) -> Callable:
        """Register a startup hook. Called before the server starts accepting requests.

        Usage::

            @app.on_startup
            def init_db():
                app.state["db"] = create_pool()
        """
        if handler is not None:
            self._startup_hooks.append(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._startup_hooks.append(fn)
            return fn

        return decorator

    def on_shutdown(self, handler: Callable | None = None) -> Callable:
        """Register a shutdown hook. Called after the server stops.

        Usage::

            @app.on_shutdown
            def cleanup():
                close_pool(app.state["db"])
        """
        if handler is not None:
            self._shutdown_hooks.append(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._shutdown_hooks.append(fn)
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Observability — X-Request-ID + /metrics
    # ------------------------------------------------------------------

    def enable_request_id(self, header: str = "X-Request-ID") -> None:
        """Guarantee every response carries an ``X-Request-ID`` header.

        If the client sent one (visible ASCII, at most 128 bytes), it's
        echoed back verbatim, so trace IDs propagated from an upstream proxy
        survive. If not, the server's own id (32 hex digits) is used. Either
        way it is ``req.request_id``, and a 5xx response and its error log
        line report the same id. Idempotent.
        """
        with self._enable_lock:
            if getattr(self, "_request_id_enabled", False):
                return
            from pyronova.observability import install_request_id
            self._engine.set_request_id_header(header)
            install_request_id(self, header)
            self._request_id_enabled = True

    def enable_metrics(self, path: str = "/metrics") -> None:
        """Expose Prometheus metrics at ``path`` (default ``/metrics``).

        Registers a ``GET /metrics`` route plus before/after-request hooks
        that maintain counters in ``app.state``. The scrape endpoint does
        not count itself. Idempotent.
        """
        with self._enable_lock:
            if getattr(self, "_metrics_enabled", False):
                return
            from pyronova.observability import install_metrics
            install_metrics(self, path)
            self._metrics_enabled = True

    # ------------------------------------------------------------------
    # Health probes — /livez + /readyz
    # ------------------------------------------------------------------

    def enable_health_probes(
        self,
        *,
        livez_path: str = "/livez",
        readyz_path: str = "/readyz",
        gil: bool = True,
    ) -> None:
        """Register Kubernetes-style ``/livez`` and ``/readyz`` routes.

        ``/livez`` always returns 200 — the process is alive. ``/readyz``
        runs every function registered with ``@app.readiness_check(name)``
        and returns 200 only when all pass; any failure produces 503 with a
        JSON body listing the failing check.

        Call once at startup, typically right after ``Pyronova()``. Idempotent —
        calling again is a no-op.

        Args:
            livez_path: route for the liveness probe (default ``/livez``).
            readyz_path: route for the readiness probe (default ``/readyz``).
            gil: whether probes run on the main interpreter. Default True
                 because most readiness checks touch globals (DB pools,
                 cache clients) that live on the main interp.
        """
        with self._enable_lock:
            if self._health_probes_enabled:
                return
            from pyronova.health import _build_livez_handler, _build_readyz_handler

            self._route("GET", livez_path, _build_livez_handler(), gil=gil)
            self._route(
                "GET",
                readyz_path,
                _build_readyz_handler(self._readiness_checks),
                gil=gil,
            )
            self._health_probes_enabled = True

    def readiness_check(self, name: str) -> Callable:
        """Register a readiness check. Sync or async. Decorator form::

            @app.readiness_check("db")
            def _db_ready():
                pool.fetch_scalar("SELECT 1")

        A check fails when it raises, takes longer than the probe timeout (10 s),
        or returns ``False``; any other value, ``None`` included, passes. A failing
        check is reported in ``/readyz`` and the whole probe returns 503.
        """
        from pyronova.health import ReadinessCheck

        def decorator(fn: Callable) -> Callable:
            self._readiness_checks.append(ReadinessCheck.of(name, fn))
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Fallback (custom 404)
    # ------------------------------------------------------------------

    def fallback(self, handler: Callable | None = None) -> Callable:
        """Register a fallback handler for unmatched routes."""
        if handler is not None:
            self._engine.fallback(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._engine.fallback(fn)
            return fn

        return decorator

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    def websocket(self, path: str, handler: Callable | None = None) -> Callable:
        """Register a WebSocket handler. Use as decorator or direct call.

        The handler receives a ``WebSocket`` object with ``recv()``,
        ``send(msg)``, and ``close()`` methods::

            @app.websocket("/ws")
            def ws_handler(ws):
                while True:
                    msg = ws.recv()
                    if msg is None:
                        break
                    ws.send(f"echo: {msg}")
        """
        if handler is not None:
            self._engine.websocket(path, handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._engine.websocket(path, fn)
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Static files
    # ------------------------------------------------------------------

    def static(self, prefix: str, directory: str) -> None:
        """Serve static files from *directory* under URL *prefix*.

        Example::

            app.static("/static", "./public")
        """
        self._engine.static_dir(prefix, directory)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def enable_logging(
        self,
        level: LogLevel | LogLevelName | None = None,
        sample: int = 1,
        always_log_status: int | None = None,
    ) -> None:
        """Enable the per-request access log (``pyronova::access``).

        One line per request from the Rust side, in every mode and for every
        response — including 404s, static files, fast-path routes and 5xx that
        never reach a Python handler::

            INFO  pyronova::access Request handled method=GET path=/ status=200 latency_us=198 mode="gil"

        :param level: minimum log level — a ``LogLevel``, or its name ("debug" /
            "info" / "warn" / "error" / ...). Given, it is the level, whatever
            ``log_config`` or ``debug=True`` set, and a later call without one
            (``PYRONOVA_LOG=1``, ``debug=True`` at ``run()``) keeps it. Left out, the
            level stays as configured, raised to "info" when it is "error" or "off"
            (the access lines are INFO). An unknown name raises ``ValueError`` here.
        :param sample: log 1 in every ``sample`` requests. ``1`` (default)
            logs every request. ``100`` keeps roughly 1% — production knob
            to recover the 25-30% throughput tax of full access logging
            while retaining a usable observability sample. Each serving
            thread keeps 1 in ``sample`` of its own responses (no shared
            counter), so the log keeps about 1 in ``sample`` overall.
        :param always_log_status: bypass sampling for responses whose
            status is >= this value. ``400`` keeps full visibility of
            4xx/5xx errors while sampling 2xx success traffic. ``None``
            (default) applies sampling uniformly.

        ``sample < 1`` or an ``always_log_status`` that isn't an HTTP status
        raises ``ValueError``.
        """
        # Validated first, so a bad value leaves the logging settings as they were.
        requested = None if level is None else _log_level(level)
        self._engine.set_request_log_sampling(sample, always_log_status)
        self._engine.enable_request_logging(True)

        # The deferred init_logger picks these up.
        self._log_level_pinned = self._log_level_pinned or requested is not None
        self._log_config["level"] = _level_with_access_log(
            self._log_config["level"], requested, self._log_level_pinned
        )
        self._log_config["access_log"] = True

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def _set_app_file(self, path: str) -> None:
        """The source file that defines this app: workers execute it, and the
        reloader watches its directory. Defaults to ``__main__``'s file."""
        self._app_file_path = path
        self._engine.set_script_path(path)

    def _app_file(self) -> str:
        if self._app_file_path is not None:
            return self._app_file_path
        main_file = getattr(sys.modules["__main__"], "__file__", None)
        if main_file is None:
            raise RuntimeError(
                "reload needs the app's source file, but __main__ has no __file__ "
                "(interactive session?); start the app from a file or with `pyronova dev`"
            )
        return os.path.abspath(main_file)

    @property
    def routes(self) -> list[RouteInfo]:
        """The registered routes, in registration order.

        Populated as routes are registered via decorators or direct calls.
        Fast-path routes (``add_fast_response``) appear in ``fast_routes``.
        """
        return list(self._routes_meta)

    @property
    def fast_routes(self) -> list[FastRouteInfo]:
        """Routes registered via ``add_fast_response``."""
        return list(self._fast_routes_meta)

    def run(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        workers: int | None = None,
        mode: str | None = None,
        reload: bool = False,
        io_workers: int | None = None,
        tls_cert: str | None = None,
        tls_key: str | None = None,
        extra_tls_ports: list[int] | None = None,
    ) -> None:
        """Start the Pyronova server.

        Args:
            workers: Python sub-interpreter count (default: CPU count).
            io_workers: Tokio async I/O thread count + accept loop count
                        (default: CPU count). Independent of workers.
            tls_cert: Path to PEM certificate chain (enables HTTPS).
                      Required together with ``tls_key``. Default None → plain HTTP.
            tls_key:  Path to PEM private key. Required together with ``tls_cert``.
        """
        # In a worker the script only registers routes; the server runs on main.
        if _is_worker():
            return
        settings = _ServeSettings.resolve(
            host=host,
            port=port,
            workers=workers,
            mode=mode,
            io_workers=io_workers,
            tls_cert=tls_cert,
            tls_key=tls_key,
            extra_tls_ports=extra_tls_ports,
        )

        reload = reload or os.environ.get("PYRONOVA_RELOAD") == "1"
        if reload and not _reload.is_reload_child():
            _reload.run_with_reload(_reload.ReloadTarget.of_this_process(self._app_file()))
            return

        try:
            self._serve(settings, self._start)
        except WorkersAbandoned as e:
            # A live sub-interpreter makes finalization abort: this process can only
            # exit, non-zero, without finalizing (Layer 2, design §12).
            print(f"pyronova: {e}; exiting without finalization", file=sys.stderr, flush=True)
            sys.stdout.flush()
            os._exit(1)

    def _serve(self, settings: _ServeSettings, start: Callable[[_ServeSettings], None]) -> None:
        """One server's lifetime: prepare the app (once), run the startup hooks, ``start``
        it (bind and serve until it stops), run the shutdown hooks."""
        self._prepare(settings)

        graceful = False
        run_error = None
        try:
            # Run startup hooks inside the try so shutdown hooks still run on failure
            for hook in self._startup_hooks:
                hook()
            start(settings)
            graceful = True  # engine returned after its own drain (SIGINT or _stop())
        except KeyboardInterrupt:
            # SIGINT reaches BOTH Rust (which drains connections and returns) and
            # Python's main thread (which raises KeyboardInterrupt — here, or a
            # few bytecodes later). Either way it's a graceful stop.
            graceful = True
        except BaseException as e:  # noqa: BLE001 — real failure, re-raised below
            run_error = e

        # On a graceful stop, neutralize the triggering SIGINT before running
        # shutdown hooks. The pending signal from ctrl-C often fires only once
        # we're back in Python bytecode (i.e. on the FIRST shutdown hook,
        # interrupting it) or after run() has returned (a KeyboardInterrupt in the
        # caller). Ignoring SIGINT here lets the hooks run to completion and
        # run() return cleanly. Retry through a KeyboardInterrupt that fires
        # while we're installing the handler (`signal.signal` delivers a pending
        # signal first, so once it returns none is left). The previous handler is
        # put back after the hooks: a program that goes on after run() returns
        # (e.g. stopped with _stop()) still gets KeyboardInterrupt on ctrl-C.
        import signal as _signal

        previous_sigint = None
        if graceful:
            while True:
                try:
                    previous_sigint = _signal.signal(_signal.SIGINT, _signal.SIG_IGN)
                    break
                except KeyboardInterrupt:
                    continue
                except (ValueError, OSError):
                    break  # not the main thread / no handler slot — best effort

        # Run shutdown hooks (best-effort teardown barrier): one hook failing
        # (e.g. a pool already closed) must not stop the rest, so log each failure
        # with its traceback and continue.
        for hook in self._shutdown_hooks:
            try:
                hook()
            except Exception:
                _logging.getLogger("pyronova.app").exception(
                    "shutdown hook %s raised", getattr(hook, "__name__", repr(hook))
                )
        if previous_sigint is not None:
            _signal.signal(_signal.SIGINT, previous_sigint)

        # A worker thread that outlived the shutdown grace period (a handler that ignores
        # shutdown) still has a live interpreter, and finalizing with one aborts. Say which;
        # the caller decides what the process does (`run()` exits, a TestClient raises).
        abandoned = [str(w) for w in self._engine._take_abandoned_workers()]
        if abandoned:
            _logging.getLogger("pyronova.app").error(
                "worker(s) %s did not stop within the shutdown grace period",
                ", ".join(abandoned),
            )
            raise WorkersAbandoned(abandoned)

        # Not a graceful stop (real startup/run error): surface it normally.
        if run_error is not None:
            raise run_error

    def _prepare(self, settings: _ServeSettings) -> None:
        """Get the app ready for this server. Once per app, by its first server: seal the
        script's registrations and set up logging. Per server, as that server needs it:
        the ``/mcp`` route once there are MCP tools (a later server still gets it when the
        first had none), and the process's BLAS thread limit when this server runs workers
        (whatever the first server's mode was). Everything registered here exists only on
        main. Locked: two servers of one app may start at once."""
        with self._prepare_lock:
            if not self._prepared:
                self._engine._seal_registrations()

                if os.environ.get("PYRONOVA_LOG") == "1" or self.debug:
                    self.enable_logging()
                # Deferred from __init__ so enable_logging() can adjust the config first.
                # Main and every worker gate Python logging on the level applied here
                # (`_python_log_level`).
                init_logger(
                    self._log_config["level"],
                    self._log_config["access_log"],
                    self._log_config["format"],
                )
                _setup_python_logging_bridge()
                self._prepared = True

            if not self._mcp_route_registered and not self._mcp.is_empty():
                mcp = self._mcp

                def _mcp_handler(req):
                    refused = _csrf.check(req, self._origin_policy, _MCP_BODY_TYPES)
                    if isinstance(refused, _csrf.Refused):
                        return Response(
                            body=mcp.refusal(refused.reason),
                            status_code=refused.status,
                            content_type="application/json",
                        )
                    return Response(
                        body=mcp.handle_request(req.body, request_id=req.request_id),
                        content_type="application/json",
                    )

                self._route("POST", "/mcp", _mcp_handler, gil=True)
                self._mcp_route_registered = True
                print(f"  MCP: {len(mcp._tools)} tools, {len(mcp._resources)} resources, {len(mcp._prompts)} prompts → POST /mcp")

        if settings.mode.uses_workers and settings.workers != 1:
            print(f"  BLAS: {_blas_threads_limited()} (override: set OPENBLAS_NUM_THREADS)", flush=True)

    def _start(
        self,
        settings: _ServeSettings,
        on_bound: Callable[[object], None] | None = None,
    ) -> None:
        """Bind a server (an ``OSError`` if its port is taken), hand it to ``on_bound``,
        then serve until it stops: SIGINT, its ``shutdown()``, or ``_stop()``."""
        server = self._engine.start(
            host=settings.host,
            port=settings.port,
            workers=settings.workers,
            mode=settings.mode,
            io_workers=settings.io_workers,
            tls_cert=settings.tls_cert,
            tls_key=settings.tls_key,
            extra_tls_ports=settings.extra_tls_ports,
        )
        with self._servers_lock:
            self._servers.add(server)
        try:
            if on_bound is not None:
                on_bound(server)
            server.serve()
        finally:
            with self._servers_lock:
                self._servers.discard(server)

    def _stop(self) -> None:
        """Stop every server this app is serving, as SIGINT does: each ``_start`` drains
        and returns. A no-op while nothing is serving. To stop one server of several,
        call that server's ``shutdown()``."""
        with self._servers_lock:
            servers = list(self._servers)
        for server in servers:
            server.shutdown()


class WorkersAbandoned(RuntimeError):
    """Worker threads outlived the shutdown grace period (a handler that ignores the
    stop). Their interpreters are still alive, and finalizing the process with a live
    sub-interpreter aborts, so the process must exit without finalizing
    (``os._exit``): ``Pyronova.run()`` does; a ``TestClient`` raises this instead, and
    the test process will abort at exit."""

    def __init__(self, workers: list[str]):
        self.workers = list(workers)
        super().__init__(
            "worker(s) " + ", ".join(self.workers) + " did not stop within the "
            "shutdown grace period"
        )


# What `_limit_blas_threads` did, once per process: BLAS is process-wide, so the first
# server that runs workers limits it and later ones report the same outcome.
_BLAS_LOCK = threading.Lock()
_blas_outcome: str | None = None


def _blas_threads_limited() -> str:
    global _blas_outcome
    with _BLAS_LOCK:
        if _blas_outcome is None:
            _blas_outcome = _limit_blas_threads()
        return _blas_outcome


@dataclass(frozen=True)
class _ServeSettings:
    """Where and how one server runs, resolved once: explicit argument, else environment
    variable, else default."""

    host: str
    port: int
    mode: Mode
    workers: int | None
    io_workers: int | None
    tls_cert: str | None
    tls_key: str | None
    extra_tls_ports: list[int] | None

    @classmethod
    def resolve(
        cls,
        *,
        host: str | None = None,
        port: int | None = None,
        workers: int | None = None,
        mode: str | Mode | None = None,
        io_workers: int | None = None,
        tls_cert: str | None = None,
        tls_key: str | None = None,
        extra_tls_ports: list[int] | None = None,
    ) -> _ServeSettings:
        if not isinstance(mode, Mode):
            mode = Mode.parse(mode or "subinterp")
        if port is None:
            port = _env_int("PYRONOVA_PORT", "8000")
        if workers is None:
            workers = _env_int("PYRONOVA_WORKERS")
        if io_workers is None:
            io_workers = _env_int("PYRONOVA_IO_WORKERS")
        if workers is not None and workers < 1:
            raise ValueError(f"workers must be >= 1, got {workers}")
        if io_workers is not None and io_workers < 1:
            raise ValueError(f"io_workers must be >= 1, got {io_workers}")
        tls_cert = tls_cert or os.environ.get("PYRONOVA_TLS_CERT")
        tls_key = tls_key or os.environ.get("PYRONOVA_TLS_KEY")
        if bool(tls_cert) != bool(tls_key):
            raise ValueError(
                f"tls_cert and tls_key must be provided together: "
                f"tls_cert={'set' if tls_cert else 'missing'}, "
                f"tls_key={'set' if tls_key else 'missing'}"
            )
        if extra_tls_ports is None:
            extra_tls_ports = _env_ports("PYRONOVA_TLS_PORTS")
        else:
            for p in extra_tls_ports:
                _check_port("extra_tls_ports", p)
        return cls(
            host=host or os.environ.get("PYRONOVA_HOST", "127.0.0.1"),
            port=port,
            mode=mode,
            workers=workers,
            io_workers=io_workers,
            tls_cert=tls_cert,
            tls_key=tls_key,
            extra_tls_ports=extra_tls_ports,
        )


def _env_int(name: str, default: str | None = None) -> int | None:
    """An integer environment variable; the error names the variable (arc app-1, app-7)."""
    v = os.environ.get(name, default)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        raise ValueError(
            f"environment variable {name}={v!r} is not a valid integer"
        ) from None


def _env_ports(name: str) -> list[int] | None:
    """A comma-separated port list environment variable; a bad entry is named (arc app-7)."""
    raw = os.environ.get(name)
    if not raw:
        return None
    ports = []
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            port = int(p)
        except ValueError:
            raise ValueError(f"{name} contains non-integer port {p!r}") from None
        ports.append(_check_port(name, port))
    return ports


def _check_port(source: str, port: int) -> int:
    """An extra listening port: 1-65535 (an ephemeral port 0 could not be reached)."""
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError(f"{source} contains {port!r}, which is not a port (1-65535)")
    return port


def _defining_module_file(app: object) -> str | None:
    """The file whose module-level code created ``app`` — the script sub-interpreter
    workers can execute to rebuild it — or None when a function created it."""
    frame = sys._getframe(1)
    # Step out of Pyronova.__init__ and any subclass __init__ that called it.
    while frame is not None and frame.f_locals.get("self") is app:
        frame = frame.f_back
    if frame is None or frame.f_code.co_name != "<module>":
        return None
    file = frame.f_globals.get("__file__")
    return os.path.abspath(file) if file else None


def _bind_handler(fn: Callable, path: str, model: type | None) -> Callable:
    """The callable the engine dispatches for a route: ``fn`` itself when it
    takes only the request, else a wrapper that validates the body against
    ``model`` and injects the path params ``fn`` declares. Signature mistakes
    and a path the router would not take as written (``:name``) raise here, at
    registration, not on every request."""
    template = frozenset(_route_params(path))
    if model is None:
        return _bind_path_params(fn, path, template)
    return _bind_model(fn, path, template, model)


def _bind_path_params(fn: Callable, path: str, template: frozenset[str]) -> Callable:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return fn  # a builtin/C callable: nothing to inject
    names = _path_param_names(fn, sig, path, template, leading=1)
    if not names:
        return fn  # hot path — the handler is registered as is
    _require_accepts(fn, leading=1, names=names)

    # Every name is in the template, so the router always fills it: `p[n]`, never a
    # silent None.
    if inspect.iscoroutinefunction(fn):
        async def bound(req):
            p = req.params
            return await fn(req, **{n: p[n] for n in names})
    else:
        def bound(req):
            p = req.params
            return fn(req, **{n: p[n] for n in names})
    return _named_like(bound, fn)


def _bind_model(fn: Callable, path: str, template: frozenset[str], model: type) -> Callable:
    """``fn(req, body, **path_params)`` or ``fn(body, **path_params)``, by the rule
    ``_model_takes_request`` checks at registration."""
    # Imported here, only for routes that declare model=: importing pydantic at
    # module level would load pydantic_core in every worker of every app. If it
    # can't be imported, route registration fails with the ImportError.
    from pydantic import BaseModel, ValidationError

    if not (isinstance(model, type) and issubclass(model, BaseModel)):
        raise TypeError(f"model= must be a pydantic BaseModel subclass, got {model!r}")
    sig = inspect.signature(fn)
    takes_request = _model_takes_request(fn, sig, template, model)
    names = _path_param_names(fn, sig, path, template, leading=2 if takes_request else 1)
    if names:
        _require_accepts(fn, leading=2 if takes_request else 1, names=names)

    def args(req, body):
        return (req, body) if takes_request else (body,)

    def kwargs(req):
        p = req.params
        return {n: p[n] for n in names}

    if inspect.iscoroutinefunction(fn):
        async def bound(req):
            try:
                body = model.model_validate_json(req.body)
            except ValidationError as e:
                return _validation_error_response(e)
            return await fn(*args(req, body), **kwargs(req))
    else:
        def bound(req):
            try:
                body = model.model_validate_json(req.body)
            except ValidationError as e:
                return _validation_error_response(e)
            return fn(*args(req, body), **kwargs(req))
    return _named_like(bound, fn)


def _model_takes_request(
    fn: Callable, sig: inspect.Signature, template: frozenset[str], model: type
) -> bool:
    """The ``model=`` rule: a handler's leading positional parameters that are not path
    params are the request and the validated body, ``(req, body)``, or the body alone,
    ``(body)``; every other parameter names a path param. Whether it takes the request
    follows from that count, never from a guess. A signature the rule rejects raises here:
    no parameter for the body, or the parameter annotated as ``model`` in the request's
    place. (More than two is left to the path-param check, which names the extras.)"""
    leading = []
    for param in sig.parameters.values():
        positional = param.kind in (
            inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        if not positional or param.name in template:
            break
        leading.append(param)
    if not leading:
        raise TypeError(
            f"handler {fn.__name__!r} has no parameter for the validated "
            f"{model.__name__} body: with model=, a handler is "
            f"fn(req, body, **path_params) or fn(body, **path_params)"
        )
    takes_request = len(leading) >= 2
    if takes_request and _annotated_as(leading[0], model):
        raise TypeError(
            f"handler {fn.__name__!r}: parameter {leading[0].name!r} is annotated "
            f"{model.__name__} but stands where the request goes (with model=, a handler "
            f"is fn(req, body, **path_params) or fn(body, **path_params); is "
            f"{leading[1].name!r} a path param missing from the URL template?)"
        )
    return takes_request


def _annotated_as(param: inspect.Parameter, cls: type) -> bool:
    """Whether ``param`` is annotated as ``cls``: the class itself, or its name as a string
    (``from __future__ import annotations``)."""
    return param.annotation is cls or param.annotation in (cls.__name__, cls.__qualname__)


def _validation_error_response(e) -> Response:
    _logging.getLogger("pyronova.validation").warning(
        "request body validation failed: %s", type(e).__name__, exc_info=True
    )
    return Response(
        body=_json_module.dumps({"detail": e.errors(include_url=False, include_input=False)}),
        status_code=422,
        content_type="application/json",
    )


def _path_param_names(
    fn: Callable, sig: inspect.Signature, path: str, template: frozenset[str], leading: int
) -> tuple[str, ...]:
    """The parameters after the ``leading`` ones the dispatcher fills (request,
    body), each of which must name a param in the URL template."""
    names = tuple(
        p.name for p in list(sig.parameters.values())[leading:]
        if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    )
    missing = [n for n in names if n not in template]
    if missing:
        raise ValueError(
            f"handler {fn.__name__!r} declares parameter(s) {missing!r} "
            f"that are not in the URL template {path!r}. Path-param "
            f"injection only fills names that appear as `{{name}}` "
            f"or `{{*name}}` in the route path."
        )
    return names


def _require_accepts(fn: Callable, leading: int, names: tuple[str, ...]) -> None:
    """The path params are read off ``inspect.signature(fn)``, which follows a decorator's
    ``__wrapped__`` to the function it wraps. The call goes to ``fn`` itself, so it must take
    them too: a wrapper that doesn't (``def wrapper(req)``) is a registration error here,
    not a TypeError on every request."""
    try:
        own = inspect.signature(fn, follow_wrapped=False)
    except (TypeError, ValueError):
        return  # a builtin/C callable: its signature can't be read
    try:
        own.bind(*([None] * leading), **dict.fromkeys(names))
    except TypeError as e:
        raise TypeError(
            f"handler {getattr(fn, '__qualname__', fn)!r} declares path param(s) "
            f"{list(names)!r} through __wrapped__, but the wrapper the route calls has "
            f"signature {own} and can't take them ({e}); have the wrapper accept and pass "
            "them on (e.g. **path_params)"
        ) from None


def _named_like(wrapper: Callable, fn: Callable) -> Callable:
    wrapper.__name__ = fn.__name__
    wrapper.__qualname__ = fn.__qualname__
    wrapper.__wrapped__ = fn  # Pylance / static analyzers see the original signature
    return wrapper
