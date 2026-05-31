"""Type stubs for pyronova.engine (Rust extension module)."""

from typing import Any, Optional

def init_logger(level: str, access_log: bool, format: str) -> None:
    """Initialize the Rust tracing engine. Call once at startup.

    :param level: one of ``"TRACE" | "DEBUG" | "INFO" | "WARN" |
        "ERROR" | "OFF"`` (case-insensitive). An unrecognized value is
        treated as ``"INFO"``.
    :param format: ``"json"`` for structured logs, anything else for the
        human-readable text formatter.
    Calling more than once is a no-op after the first successful init
    (the global subscriber can only be installed once); the later call
    does not raise but also does not re-configure the level/format.
    """
    ...

def emit_python_log(
    level: str,
    name: str,
    message: str,
    pathname: str,
    lineno: int,
    worker_id: Optional[int] = None,
) -> None:
    """Route a Python log record through Rust tracing."""
    ...

class Request:
    method: str
    path: str
    params: dict[str, str]
    query: str
    headers: dict[str, str]
    client_ip: str
    body: bytes
    query_params: dict[str, str]
    """Query parameters as Dict[str, str]. On duplicate keys, the FIRST
    value wins (HTTP parameter pollution defense — aligns with common
    WAF / reverse-proxy behavior). Use `query_params_all` for duplicates."""
    query_params_all: dict[str, list[str]]
    """Query parameters preserving all values per key. Use when the
    handler legitimately accepts multiple occurrences of the same key."""
    def text(self) -> str:
        """Decode the request body as UTF-8 text.

        Raises ``ValueError`` if the body is not valid UTF-8.
        """
        ...
    def json(self) -> Any:
        """Parse the request body as JSON.

        Raises ``ValueError`` if the body is not well-formed JSON. The
        return is typed ``Any`` because the deserialized value depends on
        the payload: a JSON object becomes a ``dict``, while a JSON
        array/scalar body deserializes to the corresponding Python type
        (``list``/``str``/``int``/...). Guard with try/except ``ValueError``.
        """
        ...

class Response:
    body: object
    status_code: int
    content_type: Optional[str]
    # A header value may be a single string, or a list of strings to emit
    # the same header name multiple times (e.g. multiple ``Set-Cookie``
    # lines — see pyronova.cookies.set_cookie). The runtime accepts both.
    headers: dict[str, str | list[str]]
    def __init__(
        self,
        body: object,
        status_code: int = 200,
        content_type: Optional[str] = None,
        headers: Optional[dict[str, str | list[str]]] = None,
    ) -> None: ...

class WebSocket:
    def recv(self) -> Optional[str]:
        """Receive the next text message.

        Returns ``None`` when the peer has closed the connection (no more
        messages). Protocol errors, transport failures, and non-UTF-8
        frames surface as exceptions, not as ``None`` — distinguish a clean
        close (``None``) from an error (raised) accordingly.
        """
        ...
    def send(self, msg: str) -> None: ...
    def close(self) -> None: ...

class SharedState:
    def __getitem__(self, key: str) -> str:
        """``state[key]`` — raises ``KeyError`` if the key is absent
        (standard mapping semantics). Use ``get`` for a default instead."""
        ...
    def __setitem__(self, key: str, value: str) -> None: ...
    def __delitem__(self, key: str) -> None:
        """``del state[key]`` — raises ``KeyError`` if the key is absent."""
        ...
    def __contains__(self, key: str) -> bool: ...
    def get(self, key: str, default: str | None = None) -> str | None: ...
    def keys(self) -> list[str]: ...
    def values(self) -> list[str]: ...
    def items(self) -> list[tuple[str, str]]: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class PyronovaApp:
    def __init__(self) -> None: ...
    def get(self, path: str, handler: object, gil: bool = False) -> None: ...
    def post(self, path: str, handler: object, gil: bool = False) -> None: ...
    def put(self, path: str, handler: object, gil: bool = False) -> None: ...
    def delete(self, path: str, handler: object, gil: bool = False) -> None: ...
    def route(self, method: str, path: str, handler: object, gil: bool = False) -> None: ...
    def before_request(self, handler: object) -> None: ...
    def after_request(self, handler: object) -> None: ...
    def fallback(self, handler: object) -> None: ...
    def websocket(self, path: str, handler: object) -> None: ...
    def static_dir(self, prefix: str, directory: str) -> None:
        """Serve files under ``directory`` at URL ``prefix``.

        Requested paths are canonicalized and confirmed to stay within
        ``directory`` (path-traversal / symlink-escape attempts are
        rejected with 404, not served). A non-existent or unreadable
        ``directory`` does not raise here; matching requests simply 404 at
        serve time.
        """
        ...
    def set_cors_origin(self, origin: str) -> None: ...
    def set_cors_config(
        self,
        origin: str,
        methods: str,
        headers: str,
        expose_headers: Optional[str] = None,
        allow_credentials: bool = False,
    ) -> None: ...
    def enable_request_logging(self, enabled: bool) -> None: ...
    @property
    def state(self) -> SharedState: ...
    def run(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        workers: Optional[int] = None,
        mode: Optional[str] = None,
    ) -> None: ...
