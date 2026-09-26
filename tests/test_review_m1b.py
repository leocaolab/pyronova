"""Code review cced8c2, milestone M1b: small Rust runtime bugs, end to end.

- Compression on a non-ASCII content type must not panic the connection.
- Idle-mode GC counts dispatched requests, so it runs (and its OOM failsafe trips) on a
  keep-alive connection; a failed `gc.collect()` leaves no pending exception behind.
- `PYRONOVA_GC_MODE` is parsed once: an unknown or unsupported mode is a startup error.
- The sub-interpreter pool's sync/async worker split is one value, read by the pool and
  the banner; a needed pool with no worker is a startup error.
"""

from __future__ import annotations

import http.client
import os
import signal
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.request

import pytest

from tests._helpers import listening_ports, settle

HOST = "127.0.0.1"
STARTUP_TIMEOUT_S = 30


class Server:
    def __init__(self, tmp_path, script: str, env: dict[str, str], ready_path: str = "/"):
        self.port: int | None = None
        self.script = tmp_path / "app.py"
        self.script.write_text(textwrap.dedent(script).replace("__PORT__", "0"))
        self.log = tmp_path / "server.log"
        full_env = dict(os.environ)
        full_env.update(env)
        with open(self.log, "w") as log:
            self.proc = subprocess.Popen(
                [sys.executable, str(self.script)],
                stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True, env=full_env,
            )
        self.ready_path = ready_path

    def output(self) -> str:
        return self.log.read_text(errors="replace")

    def wait_ready(self) -> None:
        deadline = time.time() + STARTUP_TIMEOUT_S
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited early:\n{self.output()}")
            if self.port is None:
                ports = listening_ports(self.output())
                if not ports:
                    time.sleep(0.1)
                    continue
                self.port = ports[0]
            try:
                urllib.request.urlopen(f"http://{HOST}:{self.port}{self.ready_path}", timeout=1)
                return
            except Exception:  # noqa: BLE001 — only waiting for it to come up
                time.sleep(0.1)
        raise RuntimeError(f"server did not start:\n{self.output()}")

    def wait_exit(self) -> int:
        """The exit code of a server expected to refuse to start."""
        try:
            return self.proc.wait(timeout=STARTUP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            self.stop()
            pytest.fail(f"server started instead of refusing to:\n{self.output()}")

    def stop(self) -> None:
        if self.proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
            self.proc.wait(timeout=20)
        except Exception:  # noqa: BLE001 — best-effort teardown
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            self.proc.wait(timeout=5)


@pytest.fixture
def server(tmp_path):
    started: list[Server] = []

    def start(script: str, env: dict[str, str] | None = None, ready_path: str = "/") -> Server:
        s = Server(tmp_path, script, env or {}, ready_path)
        started.append(s)
        return s

    yield start
    for s in started:
        s.stop()


# ---------------------------------------------------------------------------
# Item 1: compression on a non-ASCII content type
# ---------------------------------------------------------------------------

NON_ASCII_CT_APP = """
    import os
    from pyronova import Pyronova, Response

    app = Pyronova()
    app.enable_compression(min_size=16)

    @app.get("/")
    def root(req):
        return "ok"

    @app.get("/odd-ct")
    def odd_ct(req):
        # byte 5 falls inside the 3-byte '€': a str slice [..5] would panic
        return Response("x" * 2000, content_type="tex€/plain")

    app.run(host="127.0.0.1", port=__PORT__, mode=os.environ["M1B_MODE"])
"""


@pytest.mark.parametrize("mode", ["subinterp", "gil"])
def test_non_ascii_content_type_is_served_uncompressed(server, mode):
    s = server(NON_ASCII_CT_APP, {"M1B_MODE": mode})
    s.wait_ready()

    req = urllib.request.Request(
        f"http://{HOST}:{s.port}/odd-ct", headers={"Accept-Encoding": "gzip, br"}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        assert resp.headers.get("Content-Encoding") is None
        assert resp.read() == b"x" * 2000


# ---------------------------------------------------------------------------
# Item 2 + 3: idle-mode GC on keep-alive; failed gc.collect()
# ---------------------------------------------------------------------------

GC_COUNTING_APP = """
    import gc
    from pyronova import Pyronova

    # gc.disable() runs at worker init, so every collection here is the framework's.
    _collections = [0]

    def _on_gc(phase, info):
        if phase == "start":
            _collections[0] += 1

    gc.callbacks.append(_on_gc)

    app = Pyronova()

    @app.get("/")
    def root(req):
        return "ok"

    @app.get("/work")
    def work(req):
        a = []
        a.append(a)  # a cycle only the collector can free
        return "done"

    @app.get("/gc-count")
    def gc_count(req):
        return str(_collections[0])

    app.run(host="127.0.0.1", port=__PORT__, workers=1)
"""


def _get(conn: http.client.HTTPConnection, path: str) -> tuple[int, str]:
    conn.request("GET", path)
    resp = conn.getresponse()
    return resp.status, resp.read().decode()


def _gc_count(conn: http.client.HTTPConnection) -> int:
    status, body = _get(conn, "/gc-count")
    assert status == 200, body
    return int(body)


def test_idle_gc_runs_on_keep_alive_connection(server):
    s = server(GC_COUNTING_APP, {"PYRONOVA_GC_MODE": "idle", "PYRONOVA_GC_IDLE_MS": "50"})
    s.wait_ready()
    conn = http.client.HTTPConnection(HOST, s.port, timeout=5)

    for _ in range(3):
        assert _get(conn, "/work")[0] == 200
    # Each count probe is a request too; a quiet tick after it collects. Probing every
    # tick (50 ms) would keep the thread from ever going a full tick without a request,
    # so the probes are four ticks apart.
    first = settle(lambda: _gc_count(conn), lambda n: n >= 1, interval=0.2)

    assert _get(conn, "/work")[0] == 200
    second = settle(lambda: _gc_count(conn), lambda n: n > first, interval=0.2)
    conn.close()

    assert first >= 1
    assert second > first, "idle GC stopped collecting once the connection was kept alive"


def test_idle_gc_failsafe_trips_on_keep_alive_connection(server):
    s = server(GC_COUNTING_APP, {
        "PYRONOVA_GC_MODE": "idle",
        "PYRONOVA_GC_IDLE_MS": "600000",  # the idle tick never fires during the test
        "PYRONOVA_GC_OOM_FAILSAFE": "5",
    })
    s.wait_ready()
    conn = http.client.HTTPConnection(HOST, s.port, timeout=5)

    for _ in range(12):
        assert _get(conn, "/work")[0] == 200
    collections = _gc_count(conn)
    conn.close()

    assert collections >= 2, "OOM failsafe never tripped under sustained keep-alive traffic"


FAILING_GC_APP = """
    import gc
    from pyronova import Pyronova

    class BadGcError(Exception):
        def __str__(self):
            raise ValueError("str() of the gc error failed too")

    def _collect(*args):
        raise BadGcError()

    gc.collect = _collect  # the worker's scheduled collect now fails

    app = Pyronova()

    @app.get("/")
    def root(req):
        return "ok"

    @app.get("/work")
    def work(req):
        return "done"

    app.run(host="127.0.0.1", port=__PORT__, workers=1)
"""


def test_failed_gc_collect_leaves_no_pending_exception(server):
    s = server(FAILING_GC_APP, {"PYRONOVA_GC_MODE": "idle", "PYRONOVA_GC_IDLE_MS": "50"})
    s.wait_ready()
    conn = http.client.HTTPConnection(HOST, s.port, timeout=5)

    assert _get(conn, "/work")[0] == 200
    time.sleep(0.6)  # the idle collect runs and raises
    status, body = _get(conn, "/work")
    conn.close()

    assert status == 200, f"request after a failed gc.collect() got {status}: {body}"
    assert "BadGcError" in s.output()


# ---------------------------------------------------------------------------
# Item 2: PYRONOVA_GC_MODE is parsed once; unknown / unsupported = startup error
# ---------------------------------------------------------------------------

PLAIN_APP = """
    from pyronova import Pyronova

    app = Pyronova()

    @app.get("/")
    def root(req):
        return "ok"

    app.run(host="127.0.0.1", port=__PORT__, workers=2)
"""


def test_unknown_gc_mode_is_startup_error(server):
    s = server(PLAIN_APP, {"PYRONOVA_GC_MODE": "idel"})
    assert s.wait_exit() != 0
    out = s.output()
    assert "PYRONOVA_GC_MODE" in out and "idel" in out, out


@pytest.mark.skipif(sys.platform != "darwin", reason="the fanout topology is Darwin-only")
def test_idle_gc_on_darwin_fanout_is_startup_error(server):
    s = server(PLAIN_APP, {"PYRONOVA_GC_MODE": "idle", "PYRONOVA_TPC_DARWIN": "fanout"})
    assert s.wait_exit() != 0
    out = s.output()
    assert "idle" in out and "fanout" in out, out


def test_idle_gc_in_subinterpreter_pool_is_startup_error(server):
    s = server(PLAIN_APP, {"PYRONOVA_GC_MODE": "idle", "PYRONOVA_TPC": "0"})
    assert s.wait_exit() != 0
    out = s.output()
    assert "idle" in out and "PYRONOVA_TPC=0" in out, out


# ---------------------------------------------------------------------------
# Item 4: the pool's sync/async worker split
# ---------------------------------------------------------------------------

MIXED_APP = """
    import os
    from pyronova import Pyronova

    app = Pyronova()

    @app.get("/")
    def root(req):
        return "sync"

    @app.get("/async")
    async def aroot(req):
        return "async"

    app.run(host="127.0.0.1", port=__PORT__, workers=int(os.environ["M1B_WORKERS"]))
"""

POOL_ENV = {"PYRONOVA_TPC": "0"}


def test_one_worker_mixed_sync_async_pool_is_startup_error(server):
    s = server(MIXED_APP, {**POOL_ENV, "M1B_WORKERS": "1"})
    assert s.wait_exit() != 0
    out = s.output()
    assert "workers=1" in out and "sync" in out and "async" in out, out


def test_pool_split_banner_matches_served_pools(server):
    s = server(MIXED_APP, {**POOL_ENV, "M1B_WORKERS": "3"})
    s.wait_ready()

    with urllib.request.urlopen(f"http://{HOST}:{s.port}/async", timeout=60) as resp:
        assert resp.read() == b"async"
    with urllib.request.urlopen(f"http://{HOST}:{s.port}/", timeout=5) as resp:
        assert resp.read() == b"sync"
    assert "Workers: 2 sync + 1 async" in s.output()
