"""Final review, fix wave A, W1 (server / topology).

One test (or more) per item of the W1 row in docs/design/code-review-final-rubric.md;
each fails on the code before the fix. No fixed ports: every server binds port 0.

- Q1: on TPC, `async def` routes run on the async worker pool, off the TPC thread.
- Topology: one `Topology` resolved at start; GC modes a topology can't run are refused.
- `workers` / `io_workers` of 0, and a host that isn't an IP, are `ValueError`s naming it.
- G4: a port another server listens on with SO_REUSEPORT is `OSError(EADDRINUSE)`.
- R5: each server has its own stop handle (nested TestClients, a stop before serving).
- TestClient never `os._exit`s the test process; `_prepare` is per server where it must be.
- Registration: `stream=True` without `gil=True`, and hooks after the seal, are errors.
- RSS sampler: runs while a server with metrics serves, `None` after, back on the next run.
"""

from __future__ import annotations

import errno
import os
import socket
import subprocess
import sys
import textwrap
import threading
import time
import urllib.request

import pytest

import pyronova.engine
from pyronova import Pyronova, get_gil_metrics
from pyronova.app import WorkersAbandoned, _ServeSettings
from pyronova.testing import TestClient

from tests.apps import final_w1_mcp_late, final_w1_plain, final_w1_sealed, final_w1_tpc_async

HOST = "127.0.0.1"
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _get(url: str, timeout: float = 10) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


def _run_script(tmp_path, body: str, timeout: float = 60, env_extra=None):
    script = tmp_path / "script.py"
    script.write_text(textwrap.dedent(body))
    blas = {"OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"}
    env = {
        k: v for k, v in os.environ.items() if not k.startswith("PYRONOVA_") and k not in blas
    }
    env.update(env_extra or {})
    return subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=REPO,
        env=env,
    )


# ---------------------------------------------------------------------------
# Q1: TPC `async def` routes go to the async pool
# ---------------------------------------------------------------------------


@pytest.fixture
def one_tpc_thread(monkeypatch):
    # One TPC thread: before Q1 it ran every route, async included, one at a time.
    monkeypatch.setenv("PYRONOVA_WORKERS", "1")
    monkeypatch.delenv("PYRONOVA_TPC", raising=False)
    monkeypatch.delenv("PYRONOVA_TPC_DARWIN", raising=False)


def test_tpc_async_handlers_overlap_their_awaits(one_tpc_thread):
    with TestClient(final_w1_tpc_async.app) as c:
        assert c.get("/sleep?s=0").text == "slept"
        results: list[str] = []

        def call():
            results.append(_get(f"{c.base_url}/sleep?s=0.6"))

        threads = [threading.Thread(target=call) for _ in range(4)]
        started = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        took = time.monotonic() - started
    assert results == ["slept"] * 4
    # Inline on the one TPC thread they took 4 × 0.6 s; on the async pool they overlap.
    assert took < 1.8, took


def test_a_sync_route_answers_while_an_async_one_awaits(one_tpc_thread):
    with TestClient(final_w1_tpc_async.app) as c:
        slow = threading.Thread(target=lambda: _get(f"{c.base_url}/sleep?s=3"))
        slow.start()
        time.sleep(0.3)  # the async request is in its await
        started = time.monotonic()
        assert c.get("/fast").text == "fast"
        took = time.monotonic() - started
        slow.join(30)
    # Before Q1 the TPC thread was blocked by the async handler for its whole 3 s.
    assert took < 1.5, took


# ---------------------------------------------------------------------------
# Topology, and the arguments parsed at the boundary
# ---------------------------------------------------------------------------


def test_a_gc_mode_the_topology_cannot_run_is_refused_at_start(monkeypatch):
    monkeypatch.setenv("PYRONOVA_TPC", "0")
    monkeypatch.setenv("PYRONOVA_GC_MODE", "idle")
    engine = pyronova.engine.PyronovaApp()
    with pytest.raises(ValueError, match="PYRONOVA_GC_MODE=idle.*sub-interpreter pool"):
        engine.start(host=HOST, port=0, mode="subinterp")


@pytest.mark.parametrize("arg", ["workers", "io_workers"])
def test_zero_workers_is_a_value_error_naming_the_argument(arg):
    engine = pyronova.engine.PyronovaApp()
    with pytest.raises(ValueError, match=f"^{arg} must be at least 1"):
        engine.start(host=HOST, port=0, mode="gil", **{arg: 0})


@pytest.mark.parametrize("host", ["localhost", "not-a-host", "127.0.0"])
def test_a_host_that_is_not_an_ip_is_a_value_error_naming_it(host):
    engine = pyronova.engine.PyronovaApp()
    with pytest.raises(ValueError) as err:
        engine.start(host=host, port=0, mode="gil")
    assert f'host "{host}"' in str(err.value), err.value


def test_a_bare_ipv6_host_binds():
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.bind(("::1", 0))
    except OSError:
        pytest.skip("no IPv6 loopback")
    engine = pyronova.engine.PyronovaApp()
    server = engine.start(host="::1", port=0, mode="gil")
    assert server.port != 0
    server.shutdown()
    server.serve()  # stopped before it served: returns at once, releasing the port


# ---------------------------------------------------------------------------
# G4: a port another server listens on is in use, SO_REUSEPORT or not
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not hasattr(socket, "SO_REUSEPORT"), reason="no SO_REUSEPORT")
def test_a_port_held_with_reuseport_is_eaddrinuse():
    other = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    other.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    other.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    other.bind((HOST, 0))
    other.listen()
    port = other.getsockname()[1]
    try:
        engine = pyronova.engine.PyronovaApp()
        with pytest.raises(OSError) as err:
            engine.start(host=HOST, port=port, mode="gil")
        assert err.value.errno == errno.EADDRINUSE, err.value
    finally:
        other.close()


@pytest.mark.skipif(not hasattr(socket, "SO_REUSEPORT"), reason="no SO_REUSEPORT")
def test_a_second_server_on_a_served_port_is_eaddrinuse():
    with TestClient(final_w1_plain.app, mode="gil") as c:
        engine = pyronova.engine.PyronovaApp()
        with pytest.raises(OSError) as err:
            engine.start(host=HOST, port=c.port, mode="gil")
        assert err.value.errno == errno.EADDRINUSE, err.value
        assert c.get("/").text == "ok"


# ---------------------------------------------------------------------------
# R5: a stop handle per server
# ---------------------------------------------------------------------------


def test_nested_test_clients_on_one_app_stop_independently():
    app = final_w1_plain.app
    with TestClient(app, mode="gil") as outer:
        inner = TestClient(app, mode="gil")
        assert inner.port != outer.port
        assert inner.get("/").text == "ok"
        inner.close()
        # The inner close stopped the inner server only.
        assert outer.get("/").text == "ok"
        with pytest.raises(OSError):
            socket.create_connection((HOST, inner.port), timeout=2).close()
        started = time.monotonic()
    # Before, the outer close found no server to stop and waited 60 s.
    assert time.monotonic() - started < 10
    with pytest.raises(OSError):
        socket.create_connection((HOST, outer.port), timeout=2).close()


def test_a_server_stopped_before_it_serves_returns_at_once():
    engine = pyronova.engine.PyronovaApp()
    server = engine.start(host=HOST, port=0, mode="gil")
    port = server.port
    server.shutdown()
    started = time.monotonic()
    server.serve()
    assert time.monotonic() - started < 5
    with pytest.raises(RuntimeError, match="already served"):
        server.serve()
    del server
    # The port is released: a plain bind succeeds.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, port))


def test_a_stop_while_starting_stops_the_server_it_was_meant_for():
    # What TestClient.close() during startup does: the server is stopped as soon as it
    # is bound, so `_start` returns instead of serving on.
    app = final_w1_plain.app
    settings = _ServeSettings.resolve(host=HOST, port=0, mode="gil")
    done = threading.Event()

    def start():
        app._start(settings, on_bound=lambda server: server.shutdown())
        done.set()

    threading.Thread(target=start, daemon=True).start()
    assert done.wait(20), "the server kept serving after its stop"


def test_app_stop_stops_every_server_of_the_app():
    app = final_w1_plain.app
    a = TestClient(app, mode="gil")
    b = TestClient(app, mode="gil")
    app._stop()
    for client in (a, b):
        client._thread.join(20)
        assert not client._thread.is_alive()
        client.close()


# ---------------------------------------------------------------------------
# TestClient never exits the test process; `_prepare` per server
# ---------------------------------------------------------------------------


def test_abandoned_workers_raise_from_testclient_close(tmp_path):
    r = _run_script(
        tmp_path,
        """
        from pyronova import Pyronova
        from pyronova.app import WorkersAbandoned
        from pyronova.testing import TestClient

        class AbandoningEngine:
            # The app's engine, reporting the workers a pool shutdown gave up on.

            def __init__(self, engine, abandoned):
                self._engine, self._abandoned = engine, abandoned

            def __getattr__(self, name):
                return getattr(self._engine, name)

            def _take_abandoned_workers(self):
                taken, self._abandoned = self._abandoned, []
                return taken

        app = Pyronova()

        @app.get("/")
        def index(req):
            return "ok"

        # As a pool shutdown that gave up on a worker reports it.
        app._engine = AbandoningEngine(app._engine, ["pyronova-worker-0 (running GET /)"])

        if __name__ == "__main__":
            c = TestClient(app, mode="gil")
            try:
                c.close()
            except WorkersAbandoned as e:
                print("RAISED", e.workers, flush=True)
            print("STILL-RUNNING", flush=True)
        """,
    )
    assert "RAISED ['pyronova-worker-0 (running GET /)']" in r.stdout, r.stdout + r.stderr
    assert "STILL-RUNNING" in r.stdout, r.stdout + r.stderr


def test_app_run_still_exits_when_workers_are_abandoned(tmp_path):
    r = _run_script(
        tmp_path,
        """
        import threading, time, urllib.request
        from pyronova import Pyronova

        class AbandoningEngine:
            # The app's engine, reporting the workers a pool shutdown gave up on.

            def __init__(self, engine, abandoned):
                self._engine, self._abandoned = engine, abandoned

            def __getattr__(self, name):
                return getattr(self._engine, name)

            def _take_abandoned_workers(self):
                taken, self._abandoned = self._abandoned, []
                return taken

        app = Pyronova()

        @app.get("/")
        def index(req):
            return "ok"

        app._engine = AbandoningEngine(app._engine, ["pyronova-worker-0"])

        def stop_when_up():
            for _ in range(200):
                if app._servers:
                    break
                time.sleep(0.05)
            app._stop()

        if __name__ == "__main__":
            threading.Thread(target=stop_when_up, daemon=True).start()
            app.run(host="127.0.0.1", port=0, mode="gil")
            print("RETURNED", flush=True)
        """,
    )
    assert r.returncode == 1, r.stdout + r.stderr
    assert "RETURNED" not in r.stdout
    assert "pyronova-worker-0" in r.stderr and "without finalization" in r.stderr, r.stderr


def test_the_mcp_route_comes_with_the_first_server_that_has_tools():
    app = final_w1_mcp_late.app
    with TestClient(app, mode="gil") as c:
        assert c.post("/mcp", body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).status_code == 404

    @app.mcp.tool()
    def late_ping() -> str:
        return "pong"

    with TestClient(app, mode="gil") as c:
        r = c.post("/mcp", body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        assert r.status_code == 200, r.text
        assert [t["name"] for t in r.json()["result"]["tools"]] == ["late_ping"]


def test_blas_is_limited_by_the_first_server_that_runs_workers(tmp_path):
    r = _run_script(
        tmp_path,
        """
        import os
        from pyronova import Pyronova
        from pyronova.testing import TestClient

        app = Pyronova()

        @app.get("/")
        def index(req):
            return "ok"

        if __name__ == "__main__":
            with TestClient(app, mode="gil"):
                pass
            print("AFTER-GIL", os.environ.get("OPENBLAS_NUM_THREADS"), flush=True)
            with TestClient(app, mode="subinterp"):
                pass
            print("AFTER-WORKERS", os.environ.get("OPENBLAS_NUM_THREADS"), flush=True)
        """,
        timeout=120,
        env_extra={"PYRONOVA_WORKERS": "2"},
    )
    assert "AFTER-GIL None" in r.stdout, r.stdout + r.stderr
    assert "AFTER-WORKERS 1" in r.stdout, r.stdout + r.stderr


# ---------------------------------------------------------------------------
# Registration errors
# ---------------------------------------------------------------------------


def test_stream_without_gil_is_a_registration_error():
    app = Pyronova()

    def upload(req):
        return "ok"

    with pytest.raises(ValueError, match="stream=True needs gil=True"):
        app.post("/up", stream=True)(upload)
    assert app.routes == []


def test_hooks_after_the_seal_are_refused_like_late_routes():
    app = final_w1_sealed.app
    with TestClient(app, mode="gil") as c:
        assert c.get("/").text == "ok"

    def hook(req):
        return None

    sealed = pyronova.engine.RegistrationSealed
    assert issubclass(sealed, ValueError)
    with pytest.raises(sealed, match="before_request hook is registered after"):
        app.before_request(hook)
    with pytest.raises(sealed, match="after_request hook is registered after"):
        app.after_request(lambda req, resp: resp)
    with pytest.raises(sealed, match="fallback handler is registered after"):
        app.fallback(lambda req: "fallback")
    with pytest.raises(sealed):
        app.enable_cors()
    # A late worker route gets the same type.
    with pytest.raises(sealed, match="registered after app.run"):
        app.get("/late")(lambda req: "late")

    # Nothing was added: the next server serves what it did before.
    with TestClient(app, mode="gil") as c:
        r = c.get("/")
        assert r.text == "ok"
        assert "access-control-allow-origin" not in r.headers
        assert c.get("/nope").status_code == 404


# ---------------------------------------------------------------------------
# RSS sampler lifetime
# ---------------------------------------------------------------------------


def _rss_while(predicate, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rss = get_gil_metrics().rss_bytes
        if predicate(rss):
            return rss
        time.sleep(0.05)
    return get_gil_metrics().rss_bytes


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="RSS sampled on Linux/macOS")
def test_the_rss_sampler_runs_per_server_and_resets_after(monkeypatch):
    monkeypatch.setenv("PYRONOVA_METRICS", "1")
    app = final_w1_plain.app
    for _ in range(2):
        with TestClient(app, mode="gil") as c:
            assert c.get("/").text == "ok"
            rss = _rss_while(lambda v: isinstance(v, int))
            assert isinstance(rss, int) and rss > 0, rss
        # Stopped with the server: no stale value, and the next run samples again.
        assert get_gil_metrics().rss_bytes is None
