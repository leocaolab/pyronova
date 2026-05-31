"""High-level Pyronova application class with decorator syntax."""

from __future__ import annotations

import time
import sys
from typing import Callable, TypedDict
import inspect
import json as _json_module

import os

from pyronova.engine import PyronovaApp as _PyronovaApp, Response, SharedState, init_logger, emit_python_log
from pyronova.mcp import MCPServer
import logging as _logging

try:
    from pydantic import ValidationError as _PydanticValidationError
except ImportError:
    _PydanticValidationError = Exception  # type: ignore[assignment,misc]


class LogConfig(TypedDict, total=False):
    """Logging configuration dictionary.

    Keys:
        level: "OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"
        access_log: Whether to log every HTTP request (method, path, status, latency)
        format: "text" (human-readable) or "json" (structured, for ELK/Datadog)
    """
    level: str
    access_log: bool
    format: str

def _is_worker() -> bool:
    """Check if we're running inside a sub-interpreter worker."""
    return os.environ.get("PYRONOVA_WORKER") == "1"


class _PyronovaRustHandler(_logging.Handler):
    """logging.Handler that bridges to Rust tracing via FFI.

    Defined at module level so the isinstance() dedup check in
    _setup_python_logging_bridge works across repeated calls.
    """

    def emit(self, record: _logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
            # Preserve exception tracebacks (logger.exception / exc_info=True).
            # Use a local variable rather than mutating record.exc_text so the
            # same LogRecord can safely be routed to multiple handlers.
            if record.exc_info:
                exc_text = record.exc_text or self.formatException(record.exc_info)
                msg = f"{msg}\n{exc_text}"
            emit_python_log(
                level=record.levelname,
                name=record.name,
                message=msg,
                pathname=record.pathname or "",
                lineno=record.lineno or 0,
            )
        except Exception:
            # Fallback to Python's built-in error handler — prints to stderr
            self.handleError(record)


_LOGGING_LEVEL_MAP = {
    "TRACE": _logging.DEBUG,
    "DEBUG": _logging.DEBUG,
    "INFO": _logging.INFO,
    "WARN": _logging.WARNING,
    "WARNING": _logging.WARNING,
    "ERROR": _logging.ERROR,
    "CRITICAL": _logging.CRITICAL,
    "OFF": _logging.CRITICAL + 10,  # above CRITICAL — blocks everything
}


def _setup_python_logging_bridge(rust_level: str = "DEBUG") -> None:
    """Hijack Python's root logger to route all logs through Rust tracing.

    Replaces default StreamHandler (synchronous, GIL-blocking I/O) with a
    lightweight handler that crosses FFI into Rust's tracing system.
    The actual filtering, formatting, and I/O happen in Rust — Python only
    does the minimal work of extracting the log record fields.

    The ``rust_level`` parameter syncs Rust's EnvFilter level to Python's
    root logger, so calls below the threshold (e.g. logger.debug() when
    level=ERROR) are rejected by Python's own level check *before* any
    getMessage() formatting or FFI crossing occurs.
    """
    root = _logging.getLogger()
    # Don't clear existing handlers — user may have Sentry, DataDog, etc.
    # Only add Pyronova bridge if not already present.
    if not any(isinstance(h, _PyronovaRustHandler) for h in root.handlers):
        root.addHandler(_PyronovaRustHandler())
    # Sync Python's level gate with Rust's EnvFilter — avoids wasted
    # getMessage() + FFI calls for records that Rust would discard anyway
    level_key = rust_level.upper()
    if level_key not in _LOGGING_LEVEL_MAP:
        import sys
        print(
            f"pyronova: unrecognized log level {rust_level!r}, defaulting to DEBUG",
            file=sys.stderr,
        )
    root.setLevel(_LOGGING_LEVEL_MAP.get(level_key, _logging.DEBUG))


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
        self._routes_meta: list[dict] = []
        self._fast_routes_meta: list[dict] = []
        self._readiness_checks: list[tuple[str, Callable]] = []
        self._health_probes_enabled: bool = False

        # Resolve final logging config: debug mode defaults vs production defaults.
        # Actual init_logger call is deferred to run() so enable_logging() can
        # adjust the config before the tracing subscriber is locked in.
        user = log_config or {}
        if self.debug:
            self._log_config: LogConfig = {
                "level": user.get("level", "DEBUG"),
                "access_log": user.get("access_log", True),
                "format": user.get("format", "text"),
            }
        else:
            self._log_config: LogConfig = {
                "level": user.get("level", "ERROR"),
                "access_log": user.get("access_log", False),
                "format": user.get("format", "json"),
            }
        self._logger_initialized = False
        # Guards the idempotency check-then-set in the enable_* helpers so
        # concurrent startup hooks/threads can't both pass the "already
        # enabled?" check and double-register routes/hooks (arc findings
        # app-36/37/38).
        import threading as _threading
        self._enable_lock = _threading.Lock()

    @property
    def mcp(self) -> MCPServer:
        """MCP (Model Context Protocol) server for AI tool integration."""
        return self._mcp

    @property
    def max_body_size(self) -> int:
        """Max request body size in bytes. Default: 10 MB."""
        return getattr(self, "_max_body_size", 10 * 1024 * 1024)

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
        self._max_body_size = size
        self._engine.set_max_body_size(size)

    def enable_compression(
        self,
        *,
        min_size: int = 512,
        gzip: bool = True,
        brotli: bool = True,
        gzip_level: int = 6,
        brotli_quality: int = 4,
    ) -> None:
        """Enable gzip / brotli response compression.

        Disabled by default; call once at startup to turn on. The server
        negotiates with the client's ``Accept-Encoding`` header and prefers
        brotli when both are enabled. Skips responses under ``min_size``,
        non-text content types (images, octet-stream), streaming responses
        (SSE), and responses that set ``Content-Encoding`` explicitly.

        Args:
            min_size: minimum body size (bytes) to compress. Default 512.
            gzip: enable gzip (``Content-Encoding: gzip``). Default True.
            brotli: enable brotli (``Content-Encoding: br``). Default True.
            gzip_level: 1..=9, default 6 (balanced speed/ratio).
            brotli_quality: 0..=11, default 4 (production sweet spot).
        """
        self._engine.configure_compression(
            True, min_size, gzip, brotli, gzip_level, brotli_quality
        )

    def disable_compression(self) -> None:
        """Disable response compression. No-op if already disabled."""
        self._engine.configure_compression(False)

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
        self._fast_routes_meta.append({
            "method": method.upper(),
            "path": path,
            "status_code": status_code,
            "content_type": content_type,
            "bytes": len(body),
        })

    @property
    def state(self) -> SharedState:
        """Shared state across all sub-interpreters (nanosecond latency).

        Usage::

            app.state["session:user_1"] = json.dumps({"role": "admin"})
            data = json.loads(app.state["session:user_1"])
        """
        return self._engine.state

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
        def _maybe_inject_path_params(fn: Callable) -> Callable:
            """Wrap *fn* so path-template params land in matching kwargs.

            Hot path stays untouched: handlers whose signature is exactly
            ``(req)`` are returned unchanged — no shim, no extra frame, no
            new code path. Only when the signature declares additional
            parameters do we build a wrapper that pulls them from
            ``req.params``.

            Sub-interp note: each worker re-execs the user script and
            looks up handlers by ``__name__`` from module globals, so we
            return the shim (not the original) when wrapping — that's the
            object the global binding must point to.
            """
            try:
                sig = inspect.signature(fn)
            except (TypeError, ValueError):
                return fn
            params = list(sig.parameters.values())
            # First positional param is always the request — skip it.
            extras = [
                p for p in params[1:]
                if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
            ]
            if not extras:
                return fn  # hot path — byte-identical to today
            names = tuple(p.name for p in extras)
            # Cross-check against the URL template so a typo'd kwarg fails
            # loudly at registration instead of silently injecting None at
            # request time. matchit accepts both `{id}` and `:id`; we strip
            # the wrappers and collect param names from the path.
            template_names = set()
            i = 0
            while i < len(path):
                ch = path[i]
                if ch == "{":
                    end = path.find("}", i + 1)
                    if end == -1:
                        break
                    template_names.add(path[i + 1 : end].split(":")[0])
                    i = end + 1
                elif ch == ":":
                    j = i + 1
                    while j < len(path) and (path[j].isalnum() or path[j] == "_"):
                        j += 1
                    if j > i + 1:
                        template_names.add(path[i + 1 : j])
                    i = j
                else:
                    i += 1
            missing = [n for n in names if n not in template_names]
            if missing:
                raise ValueError(
                    f"handler {fn.__name__!r} declares parameter(s) {missing!r} "
                    f"that are not in the URL template {path!r}. Path-param "
                    f"injection only fills names that appear as `{{name}}` "
                    f"or `:name` in the route path."
                )

            is_async = inspect.iscoroutinefunction(fn)
            if is_async:
                async def shim(req):
                    p = req.params
                    return await fn(req, **{n: p.get(n) for n in names})
            else:
                def shim(req):
                    p = req.params
                    return fn(req, **{n: p.get(n) for n in names})
            shim.__name__ = fn.__name__
            shim.__qualname__ = fn.__qualname__
            shim.__wrapped__ = fn  # Pylance / static analyzers see original sig
            return shim

        def _wrap_with_model(fn: Callable, mdl: type) -> Callable:
            """Wrap handler to auto-validate request body with Pydantic model."""
            import inspect
            sig = inspect.signature(fn)
            params = list(sig.parameters.values())
            is_async = inspect.iscoroutinefunction(fn)

            import logging as _logging
            _vlog = _logging.getLogger("pyronova.validation")

            def _validation_error_response(e: Exception) -> "Response":
                _vlog.warning("request body validation failed: %s", type(e).__name__, exc_info=True)
                if hasattr(e, "errors"):
                    import json as _json
                    try:
                        errs = e.errors(include_url=False, include_input=False)
                    except TypeError:
                        errs = e.errors()
                    return Response(
                        body=_json.dumps({"detail": errs}),
                        status_code=422,
                        content_type="application/json",
                    )
                return Response(body="Request body validation failed", status_code=422, content_type="text/plain")

            # model_validate_json raises ValidationError for schema
            # mismatches, but malformed JSON syntax (or a non-decodable
            # body) surfaces as json.JSONDecodeError / ValueError / TypeError.
            # All of these are client-side "bad body" → 422, never 500.
            # _validation_error_response degrades to a generic 422 for the
            # non-pydantic cases via its hasattr(e, "errors") guard.
            _BODY_ERRORS = (_PydanticValidationError, ValueError, TypeError)
            if is_async:
                async def wrapper(req):
                    try:
                        validated = mdl.model_validate_json(req.body)
                    except _BODY_ERRORS as e:
                        return _validation_error_response(e)
                    if len(params) >= 2:
                        return await fn(req, validated)
                    return await fn(validated)
            else:
                def wrapper(req):
                    try:
                        validated = mdl.model_validate_json(req.body)
                    except _BODY_ERRORS as e:
                        return _validation_error_response(e)
                    if len(params) >= 2:
                        return fn(req, validated)
                    return fn(validated)

            wrapper.__name__ = fn.__name__
            wrapper.__qualname__ = fn.__qualname__
            return wrapper

        def _record(fn: Callable) -> None:
            self._routes_meta.append({
                "method": method,
                "path": path,
                "handler": getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn))),
                "gil": gil,
                "stream": stream,
                "model": model.__name__ if model is not None else None,
                "async": inspect.iscoroutinefunction(fn),
            })

        if handler is not None:
            if model is not None:
                handler = _wrap_with_model(handler, model)
            else:
                handler = _maybe_inject_path_params(handler)
            self._engine.route(method, path, handler, gil, stream)
            _record(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            if model is not None:
                wrapped = _wrap_with_model(fn, model)
            else:
                wrapped = _maybe_inject_path_params(fn)
            self._engine.route(method, path, wrapped, gil, stream)
            _record(fn)
            # When wrapping was a no-op `wrapped is fn` — return fn for
            # type hints (today's behavior). When we injected a shim,
            # return the shim so the module-global binding points at it;
            # sub-interp workers look handlers up by __name__ from globals
            # and would otherwise resurrect the original on each worker.
            return fn if wrapped is fn else wrapped

        return decorator

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

        If the client sent one, it's echoed back verbatim (so trace IDs
        propagated from an upstream proxy survive). If not, a fresh UUID
        v4 hex is minted. Idempotent.
        """
        with self._enable_lock:
            if getattr(self, "_request_id_enabled", False):
                return
            from pyronova.observability import install_request_id
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

        A check passes when it returns any value that isn't ``False`` and
        doesn't raise. ``False`` or an exception → the check is reported
        as failing in ``/readyz`` and the whole probe returns 503.
        """
        def decorator(fn: Callable) -> Callable:
            self._readiness_checks.append((name, fn))
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
        level: str = "info",
        sample: int = 1,
        always_log_status: int = 0,
    ) -> None:
        """Enable structured request/response logging.

        This activates both:
        - Rust-side access log (method, path, status, latency_us) via tracing
        - Python-side formatted output for GIL mode (human-readable)

        :param level: minimum log level — "debug" / "info" / "warn" / "error".
        :param sample: log 1 in every ``sample`` requests. ``1`` (default)
            logs every request. ``100`` keeps roughly 1% — production knob
            to recover the 25-30% throughput tax of full access logging
            while retaining a usable observability sample. Per-route
            atomic counter; sampling decision is global.
        :param always_log_status: bypass sampling for responses whose
            status is >= this value. ``400`` keeps full visibility of
            4xx/5xx errors while sampling 2xx success traffic. ``0``
            (default) applies sampling uniformly.

        Output format (GIL mode, text format)::

            2026-03-24 17:30:01 [INFO]  GET /api/trade → 200 (2.3ms)
            2026-03-24 17:30:01 [ERROR] POST /rpc/add → 500 (0.4ms) TypeError: ...

        Idempotent and thread-safe (matches the other ``enable_*`` helpers):
        the before/after hooks below are *appended* to the engine, not
        deduped, so a second call — e.g. a manual ``enable_logging()`` racing
        the ``run()`` auto-enable, or two startup threads — would log every
        request twice. Claim the right to initialize under ``_enable_lock``;
        any subsequent call returns a no-op.
        """
        with self._enable_lock:
            if getattr(self, "_logging_enabled", False):
                return
            self._logging_enabled = True

        from datetime import datetime

        import threading as _threading

        _timings: dict[int, float] = {}
        _MAX_TIMINGS = 10000  # Cap to prevent memory leak from SSE/stream requests
        # before/after hooks run concurrently across requests on multiple
        # worker threads. The len()-then-clear() check-then-act and the
        # set/pop interleave can race; serialize all _timings access so we
        # never hit "dict changed size during iteration"/KeyError (arc app-40).
        _timings_lock = _threading.Lock()
        _min_level = {"debug": 0, "info": 1, "warn": 2, "error": 3}.get(level.lower(), 1)

        def _log_before(req):
            with _timings_lock:
                if len(_timings) > _MAX_TIMINGS:
                    _timings.clear()  # Emergency cleanup — stream requests skip after_hook
                _timings[id(req)] = time.monotonic()
            return None

        def _log_after(req, resp):
            with _timings_lock:
                start = _timings.pop(id(req), None)
            elapsed = (time.monotonic() - start) * 1000 if start else 0
            status = getattr(resp, "status_code", 200)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Determine log level by status code
            if status >= 500:
                tag, lvl = "ERROR", 3
            elif status >= 400:
                tag, lvl = "WARN ", 2
            else:
                tag, lvl = "INFO ", 1

            if lvl >= _min_level:
                # Extract error message from body if 500
                err = ""
                if status >= 500:
                    body = getattr(resp, "body", "")
                    if isinstance(body, str) and "error" in body:
                        # Try to extract error from JSON response
                        try:
                            import json
                            err = " " + json.loads(body).get("error", "")[:100]
                        except Exception:
                            pass

                print(f"  {ts} [{tag}] {req.method} {req.path} → {status} ({elapsed:.1f}ms){err}", flush=True)

            return resp

        self._engine.before_request(_log_before)
        self._engine.after_request(_log_after)

        # Also enable Rust-level logging for sub-interpreter mode (per-instance)
        self._engine.enable_request_logging(True)
        # Sampling / always-log knobs land on the Rust route table; the
        # decision happens inside each handler's logging path.
        if sample > 1 or always_log_status > 0:
            self._engine.set_request_log_sampling(sample, always_log_status)

        # Upgrade log config so the deferred init_logger picks up access_log
        level_map = {"debug": "DEBUG", "info": "INFO", "warn": "WARN", "error": "ERROR"}
        rust_level = level_map.get(level.lower(), "INFO")
        if self._log_config.get("level", "ERROR") in ("ERROR", "OFF"):
            self._log_config["level"] = rust_level
        self._log_config["access_log"] = True

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    @property
    def routes(self) -> list[dict]:
        """List of registered routes (dicts with method/path/handler/gil/stream/async/model).

        Populated as routes are registered via decorators or direct calls.
        Fast-path routes (``add_fast_response``) appear in ``fast_routes``.
        """
        return list(self._routes_meta)

    @property
    def fast_routes(self) -> list[dict]:
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
        # Priority: param > env var > default
        host = host or os.environ.get("PYRONOVA_HOST", "127.0.0.1")

        # Env-var int parsing surfaces actionable errors. Bare int()
        # raises "invalid literal for int()" — opaque about which env
        # var was bad (arc findings app-1, app-7).
        def _env_int(name: str, default: str | None = None) -> int | None:
            v = os.environ.get(name, default)
            if v is None:
                return None
            try:
                return int(v)
            except ValueError:
                raise ValueError(
                    f"environment variable {name}={v!r} is not a valid integer"
                ) from None

        if port is None:
            port = _env_int("PYRONOVA_PORT", "8000")
        # Guarantee an int reaches the Rust FFI boundary. _env_int already
        # has a "8000" default, but pin the invariant explicitly so a None
        # can never flow into self._engine.run(port=...) and produce an
        # opaque type error deep in Rust (arc finding app-39).
        if port is None:
            port = 8000
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
            _ep = os.environ.get("PYRONOVA_TLS_PORTS")
            if _ep:
                # Surface bad entries with the offending value, not a
                # bare "invalid literal for int()" (arc app-7).
                parsed = []
                for p in _ep.split(","):
                    p = p.strip()
                    if not p:
                        continue
                    try:
                        parsed.append(int(p))
                    except ValueError:
                        raise ValueError(
                            f"PYRONOVA_TLS_PORTS contains non-integer port {p!r}"
                        ) from None
                extra_tls_ports = parsed

        # Hot reload: watch .py files, restart on change
        reload = reload or os.environ.get("PYRONOVA_RELOAD") == "1"
        if reload and os.environ.get("_PYRONOVA_RELOAD_CHILD") != "1":
            self._run_with_reload()
            return

        # Auto-enable logging if PYRONOVA_LOG=1 or debug=True.
        # enable_logging() is itself idempotent + lock-guarded, so no
        # external lock or flag bookkeeping is needed here.
        if os.environ.get("PYRONOVA_LOG") == "1" or self.debug:
            self.enable_logging()

        # Initialize Rust tracing engine (deferred from __init__ so
        # enable_logging() can adjust the config first)
        if not self._logger_initialized:
            self._logger_initialized = True
            # Expose level to sub-interpreter bootstrap via env var
            os.environ["PYRONOVA_LOG_LEVEL"] = self._log_config["level"]
            init_logger(
                self._log_config["level"],
                self._log_config["access_log"],
                self._log_config["format"],
            )
            _setup_python_logging_bridge(self._log_config["level"])
        # Auto-register /mcp endpoint if any MCP handlers exist
        if self._mcp._tools or self._mcp._resources or self._mcp._prompts:
            mcp = self._mcp

            def _mcp_handler(req):
                try:
                    body = req.text()
                    result = mcp.handle_request(body)
                except Exception:
                    _logging.getLogger("pyronova.mcp").exception("MCP handler error")
                    result = _json_module.dumps({
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": -32603, "message": "Internal error"},
                    })
                return Response(
                    body=result,
                    content_type="application/json",
                )

            self._engine.route("POST", "/mcp", _mcp_handler, True)  # gil=True
            print(f"  MCP: {len(mcp._tools)} tools, {len(mcp._resources)} resources, {len(mcp._prompts)} prompts → POST /mcp")

        # Auto-detect best mode if not explicitly set
        if mode is None:
            mode = "subinterp"

        # In worker mode (sub-interpreter), don't start the server —
        # just loading the script to register routes is enough.
        if _is_worker():
            return

        import threading

        try:
            # Run startup hooks inside the try so shutdown hooks still run on failure
            for hook in self._startup_hooks:
                hook()

            self._engine.run(
                host=host,
                port=port,
                workers=workers,
                mode=mode,
                io_workers=io_workers,
                tls_cert=tls_cert,
                tls_key=tls_key,
                extra_tls_ports=extra_tls_ports,
            )
        finally:
            # Run shutdown hooks (even if server exits abnormally).
            # [arc:intentional-handle] reason: shutdown is a best-effort
            # teardown barrier — one hook failing (e.g. a pool that's
            # already closed) must not prevent the remaining hooks from
            # running their own cleanup. We log each failure with the
            # exception object (full traceback via .exception) so the
            # cleanup error is observable, then continue; there is no
            # caller left to propagate to at process exit.
            for hook in self._shutdown_hooks:
                try:
                    hook()
                except Exception:
                    _logging.getLogger("pyronova.app").exception(
                        "shutdown hook %s raised", getattr(hook, "__name__", repr(hook))
                    )

    def _run_with_reload(self):
        """Watch .py files and restart server on changes using OS-native events."""
        import subprocess

        script = sys.argv[0] if sys.argv else None
        if not script:
            print("  [reload] Cannot determine script path, running without reload")
            return

        watch_dir = os.path.dirname(os.path.abspath(script)) or "."

        try:
            import watchfiles
        except ImportError:
            print("  [reload] Install 'watchfiles' for efficient file watching:")
            print("           pip install watchfiles")
            print("  [reload] Falling back to polling mode...")
            return self._run_with_reload_poll(watch_dir, script)

        print(f"  [reload] Watching {watch_dir} for .py changes (watchfiles)...")

        while True:
            env = {**os.environ, "_PYRONOVA_RELOAD_CHILD": "1"}
            proc = subprocess.Popen([sys.executable, script], env=env)

            try:
                for changes in watchfiles.watch(
                    watch_dir,
                    watch_filter=watchfiles.PythonFilter(),
                    stop_event=None,
                    debounce=500,  # 500ms debounce — wait for IDE to finish writing
                ):
                    changed = [os.path.basename(c[1]) for c in list(changes)[:3]]
                    print(f"\n  [reload] File changed: {', '.join(changed)}")
                    print(f"  [reload] Restarting...\n")
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    break
                else:
                    break
            except KeyboardInterrupt:
                proc.terminate()
                # Bound the wait so a child that ignores SIGTERM can't hang
                # the reloader forever — escalate to SIGKILL like the
                # file-change branch above (arc finding app-42).
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                break

    def _run_with_reload_poll(self, watch_dir: str, script: str):
        """Fallback polling watcher when watchfiles is not installed."""
        import subprocess
        import hashlib
        import glob

        print(f"  [reload] Watching {watch_dir} for .py changes (polling)...")

        def _snapshot():
            files = {}
            for f in glob.glob(os.path.join(watch_dir, "**/*.py"), recursive=True):
                # Skip common large directories
                if "/.venv/" in f or "/node_modules/" in f or "/__pycache__/" in f:
                    continue
                try:
                    with open(f, "rb") as fh:
                        files[f] = hashlib.md5(fh.read()).hexdigest()
                except Exception:
                    pass
            return files

        while True:
            env = {**os.environ, "_PYRONOVA_RELOAD_CHILD": "1"}
            proc = subprocess.Popen([sys.executable, script], env=env)
            snap = _snapshot()

            try:
                while proc.poll() is None:
                    time.sleep(1)
                    current = _snapshot()
                    if current != snap:
                        # Debounce: wait 0.5s for IDE to finish writing all files
                        time.sleep(0.5)
                        snap = _snapshot()  # Re-snapshot after debounce
                        changed = [f for f in snap if snap.get(f) != current.get(f)]
                        print(f"\n  [reload] File changed: {', '.join(os.path.basename(f) for f in changed[:3])}")
                        print(f"  [reload] Restarting...\n")
                        proc.terminate()
                        try:
                            proc.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        break
                else:
                    break
            except KeyboardInterrupt:
                proc.terminate()
                # Bound the wait so a child that ignores SIGTERM can't hang
                # the reloader forever — escalate to SIGKILL like the
                # file-change branch above (arc finding app-42).
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                break
