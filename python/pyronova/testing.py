"""TestClient — exercise a Pyronova app without manually managing a server.

Starts the app in a background thread, in the mode ``app.run()`` uses by
default, and talks to it over a real TCP socket, so every piece of the stack
(Rust accept loop, sub-interp dispatch, CORS, compression, hooks) is exercised
as it would be in production. Define the app at module level: sub-interpreter
workers rebuild it by executing that module. Pure stdlib for HTTP; WebSocket
support requires the ``websockets`` package.

Basics::

    from pyronova.testing import TestClient

    with TestClient(app) as c:
        r = c.get("/users", params={"limit": 10})
        assert r.ok
        assert r.json()["count"] == 10

Persistent cookies::

    with TestClient(app) as c:
        c.post("/login", body={"user": "alice"})
        # Cookies set by /login are resent automatically on later calls.
        r = c.get("/me")

WebSocket::

    with TestClient(app) as c, c.websocket_connect("/ws") as ws:
        ws.send("ping")
        assert ws.recv() == "pong"
"""

from __future__ import annotations

import json
import logging as _logging
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from collections import defaultdict
from dataclasses import dataclass, field
from http.cookiejar import CookieJar
from typing import Any, Iterator, NoReturn

from pyronova.app import Pyronova, WorkersAbandoned, _ServeSettings

_logger = _logging.getLogger("pyronova.testing")

# How long the server may take to accept its first connection; building the
# sub-interpreter workers (each executes the app's module) dominates it.
_READY_POLL_S = 0.1
_READY_POLLS = 300
# How long close() waits for the server to drain and run its shutdown hooks.
_STOP_TIMEOUT_S = 60


def _collapse_headers(msg) -> "dict[str, str | list[str]]":
    """Build a headers dict that preserves multi-valued entries (e.g. Set-Cookie).
    Single-valued headers remain plain strings; repeated headers become lists."""
    raw: dict[str, list[str]] = defaultdict(list)
    for k, v in msg.items():
        raw[k.lower()].append(v)
    return {k: (vs[0] if len(vs) == 1 else vs) for k, vs in raw.items()}


@dataclass
class TestResponse:
    """Response from TestClient.

    Attributes:
        status_code: HTTP status code.
        body: raw response body bytes.
        headers: response headers (case-sensitive dict from urllib).
    """

    status_code: int
    body: bytes
    # Multi-valued headers (e.g. Set-Cookie) are stored as lists; all
    # others remain plain strings. Use get_header_list() for the raw list.
    headers: dict[str, "str | list[str]"] = field(default_factory=dict)

    def get_header_list(self, name: str) -> list[str]:
        """Return all values for a header as a list (always a list, even for single values)."""
        v = self.headers.get(name.lower())
        if v is None:
            return []
        return v if isinstance(v, list) else [v]

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    @property
    def ok(self) -> bool:
        """True when the response is a success (2xx/3xx)."""
        return self.status_code < 400

    def json(self, **loads_kwargs) -> Any:
        """Decode the body as JSON. Passes kwargs to ``json.loads``.

        Raises ``json.JSONDecodeError`` (a ``ValueError`` subclass) when the
        body is not valid JSON — same contract as ``requests``/``httpx``
        ``.json()``. Catch ``ValueError`` if the endpoint may return non-JSON.
        """
        return json.loads(self.body, **loads_kwargs)

    def raise_for_status(self) -> None:
        """Raise ``RuntimeError`` if the status code is 4xx or 5xx."""
        if self.status_code >= 400:
            snippet = self.text[:200] if self.body else ""
            raise RuntimeError(
                f"TestClient HTTP {self.status_code}: {snippet!r}"
            )


class TestClient:
    """Test client — serves the app in a background thread, the way ``app.run()`` does.

    The server runs in the mode ``app.run()`` picks when given none, so non-``gil=True``
    routes run in sub-interpreter workers. Workers rebuild the app by executing the
    module that created it, so define the app at module level (as in production).
    An app created inside a function can only be served with ``mode="gil"``.

    Args:
        app: Pyronova app instance.
        host: bind address (default ``127.0.0.1``).
        port: bind port. ``None`` has the server bind one the kernel picks;
              the ``port`` attribute is the one it bound — preferred for new
              tests so parallel runs don't collide on a hard-coded number.
        mode: serving mode, as for ``app.run()``. ``None`` (default) is
              ``app.run()``'s default; ``"gil"`` serves every handler on the
              main interpreter.
        timeout: default per-request timeout in seconds (default 10).
        follow_redirects: whether to follow 3xx redirects (default True,
              matching urllib's historical behavior).

    ``close()`` (or leaving the ``with`` block) stops the server: shutdown hooks
    run and the port is released.
    """

    # Tell pytest this is not a test class (silences the collection
    # warning when the client lands in a test module namespace).
    __test__ = False

    def __init__(
        self,
        app: Pyronova,
        host: str = "127.0.0.1",
        port: int | None = None,
        *,
        mode: str | None = None,
        timeout: float = 10.0,
        follow_redirects: bool = True,
    ):
        self.host = host
        self.timeout = timeout
        self.follow_redirects = follow_redirects

        # Cookie jar — cookies set by the server persist across requests
        # made through this client, matching httpx.Client / requests.Session.
        self.cookies: CookieJar = CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookies),
            _NoRedirectHandler() if not follow_redirects else urllib.request.HTTPRedirectHandler(),
        )

        self._app = app
        # Port 0: the server binds a port the kernel picks and reports it (no pick-then-
        # rebind race with other processes).
        self._settings = _ServeSettings.resolve(
            host=host, port=0 if port is None else port, mode=mode
        )
        # Under a test runner `__main__` is the runner, not the app's script.
        if app._app_file_path is None and app._defined_in is not None:
            app._set_app_file(app._defined_in)

        # This client's own server (an engine `Server`), once bound; `close()` stops that
        # one server, never another server of the same app (e.g. an outer TestClient).
        # `_closing` records a close() that came before the server existed, so the
        # server is stopped as soon as it is bound.
        self._lock = threading.Lock()
        self._server = None
        self._closing = False
        self._server_error: Exception | None = None
        self._thread = threading.Thread(
            target=self._serve, name="pyronova-testclient", daemon=True
        )
        self._thread.start()
        self._wait_until_ready()

    @property
    def port(self) -> int:
        """The port the server listens on (the one the kernel picked for ``port=None``)."""
        server = self._server
        return self._settings.port if server is None else server.port

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def _serve(self) -> None:
        try:
            self._app._serve(
                self._settings,
                lambda settings: self._app._start(settings, on_bound=self._bound),
            )
        except Exception as e:  # noqa: BLE001 — stored for the readiness probe / close()
            self._server_error = e
            _logger.error(
                "TestClient server on port %d stopped with an error; clients will see "
                "connection refused", self.port, exc_info=e,
            )

    def _bound(self, server) -> None:
        """The engine bound this client's server; it serves next unless close() came first."""
        with self._lock:
            self._server = server
            if self._closing:
                server.shutdown()

    def _wait_until_ready(self) -> None:
        # The engine binds every listener before it builds a worker, and the server is
        # this client's from then on; after that, any HTTP response (2xx-5xx) proves it is
        # serving, and only ConnectionError / timeout means it is still starting.
        for _ in range(_READY_POLLS):
            time.sleep(_READY_POLL_S)
            if not self._thread.is_alive():
                self._raise_exited_early()
            if self._server is None:
                continue
            try:
                # Context manager guarantees the response is closed even if
                # something raises after open() (arc finding testing-48).
                with self._opener.open(f"{self.base_url}/", timeout=1):
                    pass
                return
            except urllib.error.HTTPError as e:
                # An HTTP error still proves the server is up. HTTPError is a
                # file-like object holding an open socket — close it so the
                # probe doesn't leak a connection (arc finding testing-49).
                e.close()
                return
            except (urllib.error.URLError, OSError):
                pass

        # The thread may have died during the final probe iteration.
        if not self._thread.is_alive():
            self._raise_exited_early()
        raise RuntimeError(
            f"TestClient: server failed to start within {_READY_POLLS * _READY_POLL_S:.0f}s"
        )

    def _raise_exited_early(self) -> NoReturn:
        """The server thread ended before serving: raise what stopped it, as it is."""
        err = self._server_error
        if err is None:
            raise RuntimeError("TestClient: server thread exited before accepting connections")
        if self._app._defined_in is None and self._settings.mode.uses_workers:
            err.add_note(
                "This app was created inside a function, so sub-interpreter workers "
                "cannot rebuild it by executing its module. Define it at module level, "
                "or pass mode='gil' to serve every handler on the main interpreter."
            )
        raise err

    def close(self) -> None:
        """Stop this client's server and wait until it has: its shutdown hooks have run and
        the port is free. Other servers of the same app keep serving. Safe from any
        thread, also while the server is still starting. Idempotent.

        Raises ``WorkersAbandoned`` when worker threads outlived the shutdown grace
        period: ``app.run()`` would exit the process there; a test process is left
        running, and will abort when it finalizes."""
        with self._lock:
            self._closing = True
            server = self._server
        if server is not None:
            server.shutdown()
        self._thread.join(_STOP_TIMEOUT_S)
        if self._thread.is_alive():
            raise RuntimeError(
                f"TestClient: server on port {self.port} did not stop within "
                f"{_STOP_TIMEOUT_S}s"
            )
        err, self._server_error = self._server_error, None
        if isinstance(err, WorkersAbandoned):
            raise err

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | str | dict | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> TestResponse:
        """Issue a raw request. Prefer the method-specific helpers."""
        url = f"{self.base_url}{path}"
        if params:
            # Append (don't replace) — path may already carry a query.
            if path.endswith(("?", "&")):
                sep = ""
            else:
                sep = "&" if "?" in path else "?"
            url = f"{url}{sep}{urllib.parse.urlencode(params, doseq=True)}"

        req_headers = dict(headers or {})
        if isinstance(body, dict):
            body = json.dumps(body).encode("utf-8")
            req_headers.setdefault("Content-Type", "application/json")
        elif isinstance(body, str):
            body = body.encode("utf-8")

        req = urllib.request.Request(
            url, data=body, headers=req_headers, method=method.upper()
        )
        eff_timeout = timeout if timeout is not None else self.timeout

        try:
            with self._opener.open(req, timeout=eff_timeout) as resp:
                return TestResponse(
                    status_code=resp.status,
                    body=resp.read(),
                    headers=_collapse_headers(resp.headers),
                )
        except urllib.error.HTTPError as e:
            # e.read() can itself raise (timeout / broken pipe while reading
            # the error body). Degrade to an empty body rather than letting a
            # different, undocumented exception escape (arc finding testing-50).
            try:
                err_body = e.read()
            except Exception:
                err_body = b""
            finally:
                e.close()
            return TestResponse(
                status_code=e.code,
                body=err_body,
                headers=_collapse_headers(e.headers),
            )
        except urllib.error.URLError as e:
            raise RuntimeError(f"TestClient: request failed: {e.reason}") from e

    def get(self, path: str, **kwargs) -> TestResponse:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs) -> TestResponse:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs) -> TestResponse:
        return self.request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs) -> TestResponse:
        return self.request("DELETE", path, **kwargs)

    def patch(self, path: str, **kwargs) -> TestResponse:
        return self.request("PATCH", path, **kwargs)

    def options(self, path: str, **kwargs) -> TestResponse:
        return self.request("OPTIONS", path, **kwargs)

    def head(self, path: str, **kwargs) -> TestResponse:
        return self.request("HEAD", path, **kwargs)

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    def websocket_connect(self, path: str, **connect_kwargs) -> "WebSocketSession":
        """Open a WebSocket to ``path``. Requires the ``websockets`` package.

        Usage::

            with c.websocket_connect("/chat") as ws:
                ws.send("hello")
                reply = ws.recv()

        Extra kwargs are forwarded to ``websockets.sync.client.connect``
        (e.g. ``additional_headers={"Authorization": "Bearer ..."}``).
        """
        try:
            from websockets.sync.client import connect as _ws_connect
        except ImportError as e:
            raise ImportError(
                "TestClient.websocket_connect requires the websockets package. "
                "Install with: pip install websockets"
            ) from e
        uri = f"ws://{self.host}:{self.port}{path}"
        return WebSocketSession(_ws_connect(uri, **connect_kwargs))


class WebSocketSession:
    """Thin adapter around ``websockets.sync.client.ClientConnection``
    that supports use as a context manager and exposes ``send``/``recv``
    directly so tests don't have to learn the underlying library."""

    def __init__(self, conn: Any):
        self._conn = conn

    def __enter__(self) -> "WebSocketSession":
        return self

    def __exit__(self, *args):
        self.close()

    def send(self, msg: str | bytes) -> None:
        self._conn.send(msg)

    def recv(self, timeout: float | None = None) -> str | bytes:
        if timeout is not None:
            return self._conn.recv(timeout=timeout)
        return self._conn.recv()

    def __iter__(self) -> Iterator[str | bytes]:
        yield from self._conn

    def close(self) -> None:
        self._conn.close()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Suppresses urllib's automatic redirect following."""

    def redirect_request(self, *args, **kwargs):  # noqa: D401
        return None
