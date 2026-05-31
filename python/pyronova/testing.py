"""TestClient — exercise a Pyronova app without manually managing a server.

Starts the app in a background thread and talks to it over a real TCP
socket, so every piece of the stack (Rust accept loop, sub-interp
dispatch, CORS, compression, hooks) is exercised exactly as it would be
in production. Pure stdlib for HTTP; WebSocket support requires the
``websockets`` package.

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
import socket as _socket
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from collections import defaultdict
from dataclasses import dataclass, field
from http.cookiejar import CookieJar
from typing import Any, Iterator


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
    """Test client — starts Pyronova in a background thread.

    Args:
        app: Pyronova app instance.
        host: bind address (default ``127.0.0.1``).
        port: bind port. ``None`` picks an unused port — preferred for
              new tests so parallel runs don't collide on a hard-coded
              number.
        timeout: default per-request timeout in seconds (default 10).
        follow_redirects: whether to follow 3xx redirects (default True,
              matching urllib's historical behavior).
    """

    # Tell pytest this is not a test class (silences the collection
    # warning when the client lands in a test module namespace).
    __test__ = False

    def __init__(
        self,
        app: Any,
        host: str = "127.0.0.1",
        port: int | None = None,
        *,
        timeout: float = 10.0,
        follow_redirects: bool = True,
    ):
        if port is None:
            # try/finally so a failing bind() (permission denied, address in
            # use) can't leak the socket fd (arc finding testing-47).
            s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            try:
                s.bind((host, 0))
                port = s.getsockname()[1]
            finally:
                s.close()
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self.timeout = timeout
        self.follow_redirects = follow_redirects

        # Cookie jar — cookies set by the server persist across requests
        # made through this client, matching httpx.Client / requests.Session.
        self.cookies: CookieJar = CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookies),
            _NoRedirectHandler() if not follow_redirects else urllib.request.HTTPRedirectHandler(),
        )

        self._server_error: Exception | None = None

        def _run_server():
            try:
                app.run(host=host, port=port, mode="default")
            except Exception as e:
                self._server_error = e
                # Pre-fix the exception was stored but never logged.
                # Tests then saw only generic 'connection refused' with
                # no clue why (arc finding testing-3).
                import logging as _log
                _log.getLogger("pyronova.testing").exception(
                    "TestClient inner server crashed; clients will see "
                    "connection refused"
                )

        self._thread = threading.Thread(target=_run_server, daemon=True)
        self._thread.start()

        # Readiness probe. Any HTTP response (2xx-5xx) proves the server
        # is accepting connections; only ConnectionError / timeout means
        # it's still starting.
        for _ in range(50):
            time.sleep(0.1)
            if not self._thread.is_alive():
                err = self._server_error
                raise RuntimeError(
                    f"TestClient: server thread exited before accepting connections"
                    + (f": {type(err).__name__}: {err}" if err else "")
                )
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
            except urllib.error.URLError:
                pass
            except OSError:
                pass

        # The thread may have died during the final probe iteration (after the
        # top-of-loop is_alive check, while open() raised URLError/OSError).
        # Prefer the informative crash diagnostic over the generic timeout.
        if not self._thread.is_alive():
            err = self._server_error
            raise RuntimeError(
                f"TestClient: server thread exited before accepting connections"
                + (f": {type(err).__name__}: {err}" if err else "")
            )

        raise RuntimeError("TestClient: server failed to start within 5s")

    def close(self):
        """Server runs as daemon thread — dies when main thread exits."""
        pass

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
