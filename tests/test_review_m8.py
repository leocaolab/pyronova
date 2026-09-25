"""Review cced8c2, milestone M8: TestClient runs the production path.

- TestClient served on the main interpreter (`mode="default"`) while production
  (`Pyronova.run()` with no mode) serves through sub-interpreter workers.
- Its port-retry loop re-ran all of `run()`: `/mcp` was registered again (the router
  rejects the duplicate, masking the real EADDRINUSE) and startup hooks ran again.
- `close()` did nothing, so every client leaked a running server.

Every test here failed before the change (see docs/design/code-review-cced8c2-roadmap.md,
M8).

The app is module level on purpose: sub-interpreter workers rebuild it by executing this
file, as they execute a production app's script.
"""

from __future__ import annotations

import logging
import os
import socket
import subprocess
import sys
import threading

import pytest

import pyronova.engine
import pyronova.testing as testing
from pyronova import Pyronova
from pyronova.testing import TestClient

HOST = "127.0.0.1"

app = Pyronova()

# The main interpreter's record of hook runs, in order.
LIFECYCLE: list[str] = []


@app.on_startup
def _started():
    LIFECYCLE.append("startup")


@app.on_shutdown
def _stopped():
    LIFECYCLE.append("shutdown")


@app.mcp.tool()
def echo(text: str) -> str:
    return text


@app.get("/where")
def where(req):
    return {"in_worker": pyronova.engine._in_worker()}


@pytest.fixture(autouse=True)
def _two_workers(monkeypatch):
    # Every start builds its workers by executing this file; two keep the tests quick.
    monkeypatch.setenv("PYRONOVA_WORKERS", "2")


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def _os_thread_count() -> int:
    if sys.platform.startswith("linux"):
        return len(os.listdir("/proc/self/task"))
    # macOS: one line per thread after the header.
    out = subprocess.run(
        ["ps", "-M", "-p", str(os.getpid())], capture_output=True, text=True, check=True
    ).stdout
    return len(out.strip().splitlines()) - 1


def _serve_once() -> int:
    with TestClient(app) as c:
        assert c.get("/where").status_code == 200
        return c.port


# ---------------------------------------------------------------------------
# 1. The default is the production mode
# ---------------------------------------------------------------------------


def test_default_serves_through_a_sub_interpreter():
    with TestClient(app) as c:
        assert c.get("/where").json() == {"in_worker": True}


def test_mode_gil_serves_on_the_main_interpreter():
    with TestClient(app, mode="gil") as c:
        assert c.get("/where").json() == {"in_worker": False}


def test_an_app_built_in_a_function_names_why_workers_cannot_serve_it():
    def make():
        inner = Pyronova()

        @inner.get("/")
        def index(req):
            return "hi"

        return inner

    with pytest.raises(RuntimeError) as err:
        TestClient(make())
    text = "\n".join([str(err.value), *getattr(err.value, "__notes__", ())])
    # The engine's own error, plus what to do about it (as a note on it).
    assert "registered no routes in the worker" in text, text
    assert "created inside a function" in text, text
    assert "mode='gil'" in text, text


# ---------------------------------------------------------------------------
# 2. A retried start repeats only bind/serve
# ---------------------------------------------------------------------------


def test_a_failed_start_then_a_start_prepares_once():
    # A bind that fails is an OSError from the start (M5 binds before serving); the
    # next start on the same app does not prepare it again, and each start runs the
    # startup and shutdown hooks once.
    taken = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    taken.bind((HOST, 0))
    taken.listen()
    busy = taken.getsockname()[1]
    start = len(LIFECYCLE)
    try:
        with pytest.raises(OSError):
            with TestClient(app, port=busy):
                pass
    finally:
        taken.close()
    assert LIFECYCLE[start:] == ["startup", "shutdown"]

    with TestClient(app) as c:
        assert c.port != busy
        tools = c.post(
            "/mcp", body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        ).json()
        assert [t["name"] for t in tools["result"]["tools"]] == ["echo"]
    assert LIFECYCLE[start:] == ["startup", "shutdown", "startup", "shutdown"]


def test_each_start_runs_startup_and_shutdown_once():
    start = len(LIFECYCLE)
    _serve_once()
    _serve_once()
    assert LIFECYCLE[start:] == ["startup", "shutdown", "startup", "shutdown"]


# ---------------------------------------------------------------------------
# 3. close() stops the server
# ---------------------------------------------------------------------------


def test_close_frees_the_port():
    port = _serve_once()

    with pytest.raises(ConnectionRefusedError):
        socket.create_connection((HOST, port), timeout=2).close()
    # A listener without SO_REUSEPORT binds only if no server socket is left on the port.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, port))
        s.listen()


def test_close_is_idempotent():
    c = TestClient(app)
    c.close()
    c.close()


def test_many_clients_leak_no_threads_or_servers():
    _serve_once()  # first-start one-offs (logger, signal driver) settle here
    py_threads = threading.active_count()
    os_threads = _os_thread_count()

    ports = [_serve_once() for _ in range(5)]

    assert threading.active_count() == py_threads
    # A thread from before the baseline may finish meanwhile; none may be added.
    assert _os_thread_count() <= os_threads
    for port in ports:
        with pytest.raises(ConnectionRefusedError):
            socket.create_connection((HOST, port), timeout=2).close()


def test_mcp_route_is_registered_once():
    _serve_once()
    _serve_once()
    mcp = [r for r in app.routes if r["path"] == "/mcp"]
    assert len(mcp) == 1, mcp
