"""Bootstrap script injected into each sub-interpreter worker.

Provides mock pyronova modules so user scripts can be executed in isolated
sub-interpreters without importing the real Rust extension (which doesn't
support PEP 684 multi-interpreter loading).

Also installs PyronovaRustHandler to hijack Python's logging module — all log
records are routed through the _pyronova_emit_log C-FFI function into Rust's
tracing system for zero-GIL-blocking I/O.

WARNING: This replaces `sys.modules["pydantic"]` with a no-op stub.
Pydantic validation is only available on routes with `gil=True`.
Sub-interpreter routes get a stub that lets `from pydantic import BaseModel`
succeed but does NOT perform real validation.
"""

# -- Python logging bridge to Rust tracing -----------------------------------

import logging as _logging
import os as _os

class _PyronovaRustHandler(_logging.Handler):
    """Routes Python logging records through Rust tracing via C-FFI.

    _pyronova_emit_log is injected into globals by Rust (interp.rs) before
    this bootstrap script runs. It accepts (level, name, message, pathname,
    lineno, worker_id) and dispatches to tracing macros with near-zero cost.
    """

    def __init__(self, worker_id=0):
        super().__init__()
        self._worker_id = worker_id

    def emit(self, record):
        try:
            msg = record.getMessage()
            # Preserve exception tracebacks (logger.exception / exc_info=True).
            # Use a local variable rather than mutating record.exc_text so the
            # same LogRecord can safely be routed to multiple handlers.
            if record.exc_info:
                exc_text = record.exc_text or self.formatException(record.exc_info)
                msg = f"{msg}\n{exc_text}"
            _pyronova_emit_log(
                record.levelname,
                record.name,
                msg,
                record.pathname or "",
                record.lineno or 0,
                self._worker_id,
            )
        except Exception:
            # Never crash business logic due to logging. `handleError` is
            # Python's own "I tried to log and it blew up" hook — it
            # respects `logging.raiseExceptions` (False in production) and
            # writes a diagnostic to sys.stderr with the failing record,
            # which `pass` silently discarded. Upstream handlers on every
            # stdlib logging class use this exact pattern.
            self.handleError(record)

_root = _logging.getLogger()
_root.handlers.clear()
# Verify the Rust→Python FFI contract before installing the bridge handler.
# The docstring promises `_pyronova_emit_log` is injected into globals by
# Rust (interp.rs) before this script runs. If that injection failed or
# timing changed, the handler's emit() would NameError on every record and
# handleError() would silently eat all logs — invisible log loss. Fail
# loud at install time instead, and fall back to stderr so logs survive.
if "_pyronova_emit_log" not in globals():
    import sys as _sys_boot
    print(
        "pyronova: _pyronova_emit_log FFI not injected before bootstrap; "
        "logging bridge unavailable, falling back to stderr",
        file=_sys_boot.stderr,
    )
    _root.addHandler(_logging.StreamHandler())
else:
    _root.addHandler(_PyronovaRustHandler())
# Sync Python's level gate with Rust's EnvFilter — rejects calls below
# threshold *before* getMessage() formatting or FFI crossing occurs.
# e.g. level=ERROR → logger.debug() returns immediately, no FFI overhead.
_PYRONOVA_LEVEL_MAP = {
    "TRACE": _logging.DEBUG, "DEBUG": _logging.DEBUG,
    "INFO": _logging.INFO, "WARN": _logging.WARNING, "WARNING": _logging.WARNING,
    "ERROR": _logging.ERROR, "CRITICAL": _logging.CRITICAL,
    "OFF": _logging.CRITICAL + 10,
}
_log_level_str = _os.environ.get("PYRONOVA_LOG_LEVEL", "DEBUG").upper()
if _log_level_str not in _PYRONOVA_LEVEL_MAP:
    import sys as _sys
    print(
        f"pyronova: unrecognized PYRONOVA_LOG_LEVEL={_log_level_str!r}, defaulting to DEBUG",
        file=_sys.stderr,
    )
_root.setLevel(_PYRONOVA_LEVEL_MAP.get(_log_level_str, _logging.DEBUG))

# -- Request / Response stubs ------------------------------------------------
#
# `_Request` is defined later by Rust (src/pyronova_request_type.rs) via
# PyType_FromSpec — it's a raw C heap type with __slots__-equivalent C
# members, a custom tp_dealloc, and helper methods (.text/.json/.body/
# .query_params) monkey-patched on at sub-interp init. The Python class
# that used to live here has been removed; see commit `d4bce1c` for the
# Route B migration and commit `fc45a7f` for the tstate fix that made
# it safe to rely solely on the Rust type.

class _Response:
    # Strict __slots__: no __dict__, no dynamic attributes. Paired with
    # Rust's SlotClearer, this makes the per-request cleanup exhaustive —
    # user code cannot stash an object on the response and leak it past
    # the sub-interpreter dealloc bug. If someone writes
    # `response.my_thing = x`, Python raises AttributeError at runtime
    # rather than silently hiding the ref from the Rust-side cleaner.
    __slots__ = ("body", "status_code", "content_type", "headers")
    def __init__(self, body="", status_code=200, content_type=None, headers=None):
        self.body = body
        self.status_code = status_code
        self.content_type = content_type
        self.headers = headers or {}

# -- Mock pyronova modules ----------------------------------------------------

import sys, types, os
os.environ["PYRONOVA_WORKER"] = "1"

# -- Smart GC: hand Python GC scheduling off to the Rust engine --------------
#
# CPython's default GC triggers on a per-generation allocation threshold
# (gc.get_threshold() = (700, 10, 10) by default). At 400k+ rps that
# threshold is tripped HUNDREDS of times per second, each hit blocking
# the current thread for generation-0 scan + possibly escalating to gen-1
# or gen-2. On the request hot path this translates into P99 tail
# latency spikes of 10-50ms even on an otherwise well-behaved workload.
#
# Fix: turn off CPython's automatic trigger entirely. The Rust engine
# holds a cached `gc.collect` function pointer per sub-interp and fires
# it at a configurable request-count interval (default 5000, control
# via `PYRONOVA_GC_THRESHOLD=N` — set 0 to disable scheduled collection
# entirely on workloads that never accrete cycles).
#
# Ref counting still runs on every DECREF to zero, so non-cyclic garbage
# is collected instantly. Only cycle-collection waits for the timer.
# For the standard Pyronova request path (where Request + Response are
# ref-counted to zero by tp_dealloc at the end of each handler), there
# are effectively no cycles to collect — gc.collect() becomes a
# zero-cost safety valve.
try:
    import gc as _gc
    _gc.disable()
except Exception:
    # NEVER silently swallow. If gc.disable() fails, automatic GC stays
    # on — at 400k RPS that means hundreds of generation-0 collections
    # per second, each adding 10-50ms to P99 tail latency. Without this
    # log the regression is invisible until production monitoring catches
    # the tail spike. Emit at ERROR so it shows up in default log filters.
    _logging.getLogger("pyronova.bootstrap").error(
        "gc.disable() failed — CPython auto-GC remains active; "
        "expect P99 tail spikes at high RPS",
        exc_info=True,
    )

_mock_engine = types.ModuleType("pyronova.engine")
_mock_engine.PyronovaApp = type("PyronovaApp", (), {
    "__init__": lambda self: None,
    "get": lambda self, *a, **kw: (lambda f: f) if len(a) < 2 else None,
    "post": lambda self, *a, **kw: (lambda f: f) if len(a) < 2 else None,
    "put": lambda self, *a, **kw: (lambda f: f) if len(a) < 2 else None,
    "delete": lambda self, *a, **kw: (lambda f: f) if len(a) < 2 else None,
    "route": lambda self, *a, **kw: None,
    "before_request": lambda self, f: f,
    "after_request": lambda self, f: f,
    "fallback": lambda self, f: f,
    "websocket": lambda self, *a: (lambda f: f),
    "static_dir": lambda self, *a: None,
    "run": lambda self, **kw: None,
})
# Request is bound to the raw-C Rust type by interp.rs after this
# bootstrap script finishes running — the Rust injection overwrites
# both `globals()["_Request"]` and these module-level references.
# Until then it's None; user code should not import Request at
# module-load time anyway (the type is only meaningful inside a
# running sub-interp handler).
_mock_engine.Request = None
_mock_engine.Response = _Response
_mock_engine.WebSocket = type("WebSocket", (), {})
_mock_engine.SharedState = type("SharedState", (), {})
_mock_engine.Stream = type("Stream", (), {})
_mock_engine.get_gil_metrics = lambda: (0,0,0,0,0,0,0,0,0)

_mock_pyron = types.ModuleType("pyronova")
_mock_pyron.engine = _mock_engine
_mock_pyron.PyronovaApp = _mock_engine.PyronovaApp
_mock_pyron.Request = None  # overwritten by interp.rs post-bootstrap
_mock_pyron.Response = _Response
_mock_pyron.WebSocket = _mock_engine.WebSocket
_mock_pyron.SharedState = _mock_engine.SharedState
_mock_pyron.Stream = _mock_engine.Stream
_mock_pyron.get_gil_metrics = _mock_engine.get_gil_metrics
def _redirect(url, status_code=302):
    # Mirror pyronova.__init__.redirect's CRLF guard so sub-interp mode
    # has the same HTTP Response Splitting defence as GIL mode.
    for _ch in ("\r", "\n", "\0"):
        if _ch in url:
            raise ValueError(
                f"redirect url contains forbidden control character "
                f"{_ch!r}; refusing to emit (HTTP response splitting risk)"
            )
    return _Response(body="", status_code=status_code, headers={"location": url})
_mock_pyron.redirect = _redirect

# Pyronova wrapper (no-op in worker mode)
class _MockPyron:
    def __init__(self, debug=False, log_config=None): pass
    # Route decorators accept whatever kwargs the real API takes (gil,
    # model, stream, ...); swallow them so adding a new flag to the real
    # decorator doesn't break sub-interp replay until we update this mock.
    def get(self, path, handler=None, **kw):
        if handler: return handler
        return lambda f: f
    def post(self, path, handler=None, **kw):
        if handler: return handler
        return lambda f: f
    def put(self, path, handler=None, **kw):
        if handler: return handler
        return lambda f: f
    def delete(self, path, handler=None, **kw):
        if handler: return handler
        return lambda f: f
    def patch(self, path, handler=None, **kw):
        if handler: return handler
        return lambda f: f
    def route(self, *a, **kw):
        return lambda f: f
    def before_request(self, f=None):
        return f if f else lambda fn: fn
    def after_request(self, f=None):
        return f if f else lambda fn: fn
    def fallback(self, f=None):
        return f if f else lambda fn: fn
    def rpc(self, path, **kw):
        return lambda f: f
    def websocket(self, path, handler=None):
        if handler: return handler
        return lambda f: f
    def static(self, *a): pass
    def on_startup(self, f=None):
        return f if f else lambda fn: fn
    def on_shutdown(self, f=None):
        return f if f else lambda fn: fn
    def enable_logging(self): pass
    def enable_cors(self, **kw): pass
    def run(self, **kw): pass
    def __getattr__(self, name):
        # Fallback: any unrecognized attribute access (e.g. future
        # feature toggles like enable_compression, enable_request_logging,
        # set_max_body_size, set_cors_config, etc.) resolves to a harmless
        # no-op. Sub-interp replay only needs route decorators to succeed —
        # runtime feature calls are ignored here and applied once on the
        # main interpreter.
        return lambda *a, **kw: None
    @property
    def max_body_size(self):
        return 10 * 1024 * 1024
    @max_body_size.setter
    def max_body_size(self, _v):
        pass
    @property
    def state(self):
        return {}
    @property
    def mcp(self):
        return type("MCP", (), {"tool": lambda s, *a, **kw: (lambda f: f), "resource": lambda s, *a, **kw: (lambda f: f), "prompt": lambda s, *a, **kw: (lambda f: f)})()

_mock_pyron.Pyronova = _MockPyron

# App module mock
_mock_app = types.ModuleType("pyronova.app")
_mock_app.Pyronova = _MockPyron
# pyronova.cache imports `from .app import Response`, so the mock
# `pyronova.app` must expose a Response symbol too.
_mock_app.Response = _Response

sys.modules["pyronova"] = _mock_pyron
sys.modules["pyronova.engine"] = _mock_engine
sys.modules["pyronova.app"] = _mock_app
sys.modules["pyronova.mcp"] = types.ModuleType("pyronova.mcp")

# -- pyronova.cache — @cached_json decorator (pure Python, sub-interp safe) --
#
# Inlined rather than imported: the real `pyronova.cache` module sits
# under `pyronova/`, which the sub-interp's mock `pyronova` ModuleType
# doesn't carry a `__path__` for, so importlib can't resolve it. Keep
# this in sync with python/pyronova/cache.py — the behavior must match.

import functools as _functools
import json as _json
import threading as _threading
import time as _time

def _cached_json(ttl, key=None):
    if ttl <= 0:
        raise ValueError("cached_json ttl must be > 0")
    _key_fn = key if key is not None else (lambda req: req.path)
    def decorator(handler):
        _cache = {}
        _lock = _threading.Lock()
        @_functools.wraps(handler)
        def wrapper(req):
            now = _time.monotonic()
            try:
                k = _key_fn(req)
            except Exception:
                import logging as _log_cache
                _log_cache.getLogger("pyronova.cache").exception(
                    "cached_json key function raised; bypassing cache"
                )
                return handler(req)
            entry = _cache.get(k)
            if entry is not None and entry[1] > now:
                return _Response(body=entry[0], content_type="application/json")
            result = handler(req)
            if isinstance(result, _Response):
                return result
            if isinstance(result, (bytes, bytearray)):
                body = bytes(result)
            elif isinstance(result, str):
                body = result.encode("utf-8")
            else:
                try:
                    body = _json.dumps(result, separators=(",", ":")).encode("utf-8")
                except (TypeError, ValueError):
                    # Non-serializable handler result: don't cache, hand the
                    # raw result back so the engine's normal response path
                    # (src/response.rs extract_response_data) handles it. That
                    # path runs the same json.dumps and surfaces the original
                    # exception as "handler returned non-serializable type: {e}"
                    # — NOT a lenient swallow — so the diagnostic survives there.
                    # We still log here so the failure isn't silently discarded,
                    # mirroring the key-function branch above. arc finding bootstrap-16.
                    import logging as _log_cache
                    _log_cache.getLogger("pyronova.cache").exception(
                        "cached_json: handler result is not JSON-serializable; "
                        "bypassing cache and deferring to engine response path"
                    )
                    return result
            with _lock:
                _cache[k] = (body, now + ttl)
            return _Response(body=body, content_type="application/json")
        return wrapper
    return decorator

_mock_pyron.cached_json = _cached_json
_cache_mod = types.ModuleType("pyronova.cache")
_cache_mod.cached_json = _cached_json
sys.modules["pyronova.cache"] = _cache_mod

# -- Cookie utilities (pure Python) -------------------------------------------

_cookies_mod = types.ModuleType("pyronova.cookies")
def _get_cookies(req):
    h = req.headers.get("cookie", "") if hasattr(req, "headers") else ""
    if not h: return {}
    r = {}
    for p in h.split(";"):
        p = p.strip()
        if "=" in p:
            n, _, v = p.partition("=")
            v = v.strip()
            # RFC 6265 allows DQUOTE-wrapped cookie values
            if v.startswith('"') and v.endswith('"') and len(v) >= 2:
                v = v[1:-1]
            r[n.strip()] = v
    return r
def _get_cookie(req, name, default=None):
    return _get_cookies(req).get(name, default)
_COOKIE_FORBIDDEN = ("\r", "\n", "\0", ";", ",")
def _reject_cookie_crlf(field, value):
    if value is None:
        return
    for ch in _COOKIE_FORBIDDEN:
        if ch in value:
            raise ValueError(
                f"cookie {field} contains forbidden control character "
                f"{ch!r}; refusing to emit (HTTP response splitting risk)"
            )
def _set_cookie(resp, name, value, **kw):
    # Mirror the real pyronova.cookies check so sub-interp mode has
    # the same HTTP Response Splitting defence as GIL mode. Without this
    # the sub-interp mock would silently emit attacker-controlled bytes
    # into the Set-Cookie header, defeating the v1.4.5 fix.
    _reject_cookie_crlf("name", name)
    # RFC 6265 §4.1.1: cookie-name is a token; '=' is the name/value
    # separator and MUST NOT appear in the name itself. Without this
    # check, name="foo=bar" produces `Set-Cookie: foo=bar=value` which
    # browsers parse as name="foo" value="bar=value" — a session-id
    # bypass surface if the name is attacker-influenced (arc finding
    # bootstrap-2). '=' is not added to _COOKIE_FORBIDDEN because it
    # IS valid in values (base64 padding, etc.) — name-specific check.
    if name is not None and "=" in name:
        raise ValueError(
            f"cookie name {name!r} contains '=' which is forbidden by "
            "RFC 6265 §4.1.1 (would corrupt browser parsing)"
        )
    _reject_cookie_crlf("value", value)
    _reject_cookie_crlf("path", kw.get("path"))
    _reject_cookie_crlf("domain", kw.get("domain"))
    _reject_cookie_crlf("samesite", kw.get("samesite"))
    parts = [f"{name}={value}"]
    if kw.get("max_age") is not None: parts.append(f"Max-Age={kw['max_age']}")
    if kw.get("path", "/"): parts.append(f"Path={kw.get('path','/')}")
    if kw.get("httponly"): parts.append("HttpOnly")
    if kw.get("secure"): parts.append("Secure")
    _samesite = kw.get("samesite", "Lax")
    if _samesite is not None:
        _samesite_norm = str(_samesite).strip().title()
        if _samesite_norm not in ("Strict", "Lax", "None"):
            raise ValueError(
                f"invalid samesite={_samesite!r}; must be 'Strict', 'Lax', or 'None'"
            )
        if _samesite_norm == "None" and not kw.get("secure", False):
            raise ValueError(
                "SameSite=None requires Secure=True; browsers silently drop "
                "SameSite=None cookies that are not Secure (Chrome 80+, Firefox, Safari)"
            )
        parts.append(f"SameSite={_samesite_norm}")
    cookie_str = "; ".join(parts)
    hdrs = dict(getattr(resp, "headers", {}) or {})
    existing = hdrs.get("set-cookie")
    if existing is None:
        hdrs["set-cookie"] = cookie_str
    elif isinstance(existing, list):
        hdrs["set-cookie"] = existing + [cookie_str]
    else:
        hdrs["set-cookie"] = [existing, cookie_str]
    return _Response(body=resp.body, status_code=getattr(resp,"status_code",200), content_type=getattr(resp,"content_type",None), headers=hdrs)
def _delete_cookie(resp, name, **kw):
    # Browsers require the delete Set-Cookie to match the original cookie's
    # Domain/Path/Secure/SameSite attributes; forwarding them all ensures
    # the deletion actually reaches the right cookie jar entry.
    return _set_cookie(resp, name, "", max_age=0,
                       path=kw.get("path", "/"),
                       domain=kw.get("domain"),
                       secure=kw.get("secure", False),
                       httponly=kw.get("httponly", False),
                       samesite=kw.get("samesite", "Lax"))
_cookies_mod.get_cookies = _get_cookies
_cookies_mod.get_cookie = _get_cookie
_cookies_mod.set_cookie = _set_cookie
_cookies_mod.delete_cookie = _delete_cookie
sys.modules["pyronova.cookies"] = _cookies_mod
sys.modules["pyronova.rpc"] = types.ModuleType("pyronova.rpc")
sys.modules["pyronova.testing"] = types.ModuleType("pyronova.testing")

# -- pyronova.db — bridge-backed PgPool proxy ---------------------------
#
# The #[pymodule] engine does not carry a Py_mod_multiple_interpreters
# slot (CPython 3.12+ refuses to load such modules in a sub-interp), so
# the real `PgPool` pyclass is not reachable from this interpreter.
# Rust injects four C-FFI entry points into each sub-interp's globals
# (`_pyronova_db_fetch_all`, `_pyronova_db_fetch_one`,
# `_pyronova_db_fetch_scalar`, `_pyronova_db_execute`) that forward to
# the main-process sqlx pool while releasing the calling sub-interp's
# GIL. See src/db_bridge.rs for the full rationale.

_db_mod = types.ModuleType("pyronova.db")

class _MockPgCursor:
    # fetch_iter is still mock — cursor streaming across the interp
    # boundary needs a per-worker mpsc channel and we haven't wired
    # that yet. Handlers that rely on streaming should stay gil=True.
    def __iter__(self): return self
    def __next__(self): raise StopIteration
    def to_list(self): return []

def _db_ffi(name):
    # Resolve a DB C-FFI entry point injected by Rust (src/interp.rs)
    # before this bootstrap runs. Mirrors the logging bridge's
    # `_pyronova_emit_log` presence check (above): if injection failed
    # or timing changed, the function is absent from globals() and a
    # bare call would raise a cryptic `NameError` from inside the pool
    # proxy — indistinguishable from a pyronova bug. Raise a clear
    # RuntimeError instead so callers know the DB bridge is unavailable.
    fn = globals().get(name)
    if fn is None:
        raise RuntimeError(
            f"pyronova.db is unavailable: the {name!r} C-FFI entry point was "
            "not injected into this sub-interpreter. The Rust engine injects "
            "the DB bridge at worker init; if you see this, DB access from "
            "sub-interpreter workers is not configured. Route DB handlers "
            "with gil=True to use the pool from the main interpreter."
        )
    return fn

class _PgPool:
    """Sub-interp proxy for the Rust-side sqlx pool.

    Zero-state; methods forward into the four C-FFI functions injected
    by src/interp.rs before this bootstrap runs. The functions drop the
    GIL during the sqlx round-trip, so many workers can have queries
    in flight simultaneously on the same shared pool.
    """
    @classmethod
    def connect(cls, *a, **kw):
        # Sub-interp side: no-op. The main interp owns init of the
        # Rust-side static PG_POOL: OnceLock. This handle is stateless
        # and every method call reads that global directly.
        return cls()
    def fetch_all(self, sql, *params):
        return _db_ffi("_pyronova_db_fetch_all")(sql, params)
    def fetch_one(self, sql, *params):
        return _db_ffi("_pyronova_db_fetch_one")(sql, params)
    def fetch_scalar(self, sql, *params):
        return _db_ffi("_pyronova_db_fetch_scalar")(sql, params)
    def execute(self, sql, *params):
        return _db_ffi("_pyronova_db_execute")(sql, params)
    def fetch_iter(self, *a, **kw):
        return _MockPgCursor()
    # Async variants — not yet implemented. Silently returning falsy
    # values (None / [] / 0) would masquerade as "no rows" / "success"
    # and corrupt caller logic. Raise so callers know immediately that
    # async DB is unavailable in sub-interpreter mode.
    async def fetch_one_async(self, *a, **kw):
        raise NotImplementedError(
            "fetch_one_async is not available in sub-interpreter workers; "
            "use fetch_one (sync) or route with gil=True"
        )
    async def fetch_all_async(self, *a, **kw):
        raise NotImplementedError(
            "fetch_all_async is not available in sub-interpreter workers; "
            "use fetch_all (sync) or route with gil=True"
        )
    async def fetch_scalar_async(self, *a, **kw):
        raise NotImplementedError(
            "fetch_scalar_async is not available in sub-interpreter workers; "
            "use fetch_scalar (sync) or route with gil=True"
        )
    async def execute_async(self, *a, **kw):
        raise NotImplementedError(
            "execute_async is not available in sub-interpreter workers; "
            "use execute (sync) or route with gil=True"
        )

_db_mod.PgPool = _PgPool
_db_mod.PgCursor = _MockPgCursor
sys.modules["pyronova.db"] = _db_mod

_crud_mod = types.ModuleType("pyronova.crud")
_crud_mod.register_crud = lambda *a, **kw: None
sys.modules["pyronova.crud"] = _crud_mod

# -- Pydantic stub (WARNING: replaces real pydantic in sub-interpreters) ------
# Pydantic V2's pydantic-core is a Rust/PyO3 extension that cannot load in
# sub-interpreters (no PEP 684 support yet). This stub lets user scripts that
# do `from pydantic import BaseModel` import without error. Real validation
# only runs on `gil=True` routes in the main interpreter.

class _FakeBaseModel:
    def __init_subclass__(cls, **kw): pass
    def __init__(self, **kw):
        for k, v in kw.items(): setattr(self, k, v)
    @classmethod
    def model_validate_json(cls, data): return cls()
    @classmethod
    def model_json_schema(cls): return {}
class _FakeField:
    def __init__(self, **kw): pass
    def __call__(self, **kw): return self
_pydantic_mod = types.ModuleType("pydantic")
_pydantic_mod.BaseModel = _FakeBaseModel
_pydantic_mod.Field = _FakeField(**{})
_pydantic_mod.field_validator = lambda *a, **kw: (lambda f: f)
sys.modules["pydantic"] = _pydantic_mod
# Also mock pydantic sub-modules that get imported
for _pm in ("pydantic.fields", "pydantic.main", "pydantic._migration",
            "pydantic.warnings", "pydantic.version", "pydantic_core"):
    sys.modules[_pm] = types.ModuleType(_pm)

# -- Upload utilities (pure Python) -------------------------------------------

_uploads_mod = types.ModuleType("pyronova.uploads")
class _UploadFile:
    def __init__(self, name, filename, content_type, data):
        self.name = name
        self.filename = filename
        self.content_type = content_type
        self.data = data
    @property
    def text(self):
        # Uploaded bytes are arbitrary user content — may not be valid UTF-8
        # (binary files, mojibake, partial buffers). Use errors='replace' so
        # calling .text on a binary upload yields a lossy string instead of
        # crashing the request with UnicodeDecodeError. Callers needing strict
        # decoding should work with .data directly. Matches uploads.py.
        return self.data.decode('utf-8', errors='replace')
    @property
    def size(self): return len(self.data)
def _parse_multipart(req):
    import urllib.parse as _urlparse
    ct = req.headers.get("content-type", "")
    if "multipart/form-data" not in ct:
        raise ValueError(f"not multipart: content-type={ct!r}")
    boundary = None
    for p in ct.split(";"):
        p = p.strip()
        if p.startswith("boundary="): boundary = p[9:].strip().strip('"')
    if not boundary:
        raise ValueError(f"no multipart boundary in content-type: {ct!r}")
    body = req.body if isinstance(req.body, bytes) else req.body.encode()
    parts = body.split(f"--{boundary}".encode())
    result = {}
    for part in parts:
        if not part or part.strip() in (b"--", b""): continue
        if b"\r\n\r\n" in part: hdr, data = part.split(b"\r\n\r\n", 1)
        elif b"\n\n" in part: hdr, data = part.split(b"\n\n", 1)
        else:
            import logging as _log_mp
            _log_mp.getLogger("pyronova.uploads").warning(
                "multipart: skipping part with no header/body separator"
            )
            continue
        if data.endswith(b"\r\n"): data = data[:-2]
        elif data.endswith(b"\n"): data = data[:-1]
        headers = {}
        for line in hdr.decode('utf-8', errors='replace').split("\n"):
            line = line.strip()
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        disp = headers.get("content-disposition", "")
        fname = ffilename = None
        for pp in disp.split(";"):
            pp = pp.strip()
            if pp.lower().startswith("name="): fname = pp[5:].strip('"')
            elif pp.startswith("filename*="):
                # RFC 5987: filename*=charset'language'encoded-value
                raw = pp[10:].strip().strip('"')
                try:
                    _, _, encoded = raw.partition("'"); _, _, encoded = encoded.partition("'")
                    ffilename = _urlparse.unquote(encoded, errors="replace")
                except Exception:
                    import logging as _log_mp2
                    _log_mp2.getLogger("pyronova.uploads").warning(
                        "multipart: failed to decode RFC 5987 filename*= value %r", raw
                    )
                    ffilename = raw
            elif pp.startswith("filename="):
                ffilename = _urlparse.unquote(pp[9:].strip('"'), errors="replace")
        if fname:
            ctype = headers.get("content-type", "application/octet-stream" if ffilename else "text/plain")
            upload = _UploadFile(fname, ffilename, ctype, data)
            if fname in result:
                existing = result[fname]
                if isinstance(existing, list):
                    existing.append(upload)
                else:
                    result[fname] = [existing, upload]
            else:
                result[fname] = upload
    return result
_uploads_mod.parse_multipart = _parse_multipart
_uploads_mod.UploadFile = _UploadFile
sys.modules["pyronova.uploads"] = _uploads_mod
