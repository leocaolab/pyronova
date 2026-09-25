"""E2E-8 (Layer 2, M0): main-side routes keep working once workers execute the engine.

Runs `_l2_main_side_app.py`, whose workers load the real `pyronova.engine`, then drives
worker routes, sync and async `gil=True` routes, a WebSocket echo and `/metrics`
concurrently, stops the server with SIGINT, and scans the whole log for the PyO3 fork's
panic texts.

Before M0 this failed: the GIL-bridge and WebSocket threads attached with a bare
`Python::attach` and the fork refused it (`spike/layer2-r3`, `spike/R3-RESULTS.md`).
"""
from __future__ import annotations

import collections
import concurrent.futures
import os
import signal
import subprocess
import sys
import time

import httpx
import pytest
from websockets.sync.client import connect

from conftest import _free_port, fork_panic_lines

pytestmark = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"), reason="own-GIL sub-interpreters"
)

HERE = os.path.dirname(os.path.abspath(__file__))
LOAD_SECONDS = 8
PATHS = ("/w", "/g", "/ga", "/metrics")


def _wait_up(base: str, proc: subprocess.Popen, log_path: str) -> None:
    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            raise AssertionError(
                f"server exited early (code {proc.returncode}):\n{open(log_path).read()[-4000:]}"
            )
        try:
            if httpx.get(base + "/w", timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise AssertionError(f"server not up in 60s:\n{open(log_path).read()[-4000:]}")


def _hammer(base: str) -> collections.Counter:
    statuses: collections.Counter = collections.Counter()
    stop = time.time() + LOAD_SECONDS

    def worker(i: int) -> collections.Counter:
        c: collections.Counter = collections.Counter()
        with httpx.Client(base_url=base, timeout=10) as client:
            n = i
            while time.time() < stop:
                path = PATHS[n % len(PATHS)]
                n += 1
                try:
                    r = client.get(path)
                    c[(path, r.status_code)] += 1
                except httpx.HTTPError as e:
                    c[(path, f"EXC {type(e).__name__}")] += 1
        return c

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        for c in ex.map(worker, range(16)):
            statuses.update(c)
    return statuses


def _ws_echo(port: int, i: int) -> list[str]:
    out = []
    with connect(f"ws://127.0.0.1:{port}/ws", open_timeout=5) as ws:
        for j in range(5):
            ws.send(f"{i}-{j}")
            out.append(ws.recv(timeout=5))
    return out


@pytest.mark.parametrize("tpc", ["1", "0"], ids=["tpc", "pool"])
def test_main_side_routes_survive_workers_executing_engine(tmp_path, tpc):
    """TPC: GIL bridge threads + WebSocket upgrade on a worker-bound TPC thread.
    Pool: `spawn_blocking` GIL path + WebSocket upgrade on a tstate-less Tokio worker +
    LoopGuard thread-local destructors on Tokio blocking threads."""
    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    log_path = str(tmp_path / "server.log")
    env = dict(os.environ, L2_PORT=str(port), RUST_BACKTRACE="0", PYRONOVA_TPC=tpc)
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "_l2_main_side_app.py")],
            env=env, stdout=log, stderr=subprocess.STDOUT,
        )
    try:
        _wait_up(base, proc, log_path)
        # The premise: workers really executed the engine.
        assert httpx.get(base + "/w", timeout=5).json() == {"real_engine": True}

        with concurrent.futures.ThreadPoolExecutor(4) as ex:
            ws_future = ex.map(lambda i: _ws_echo(port, i), range(4))
            statuses = _hammer(base)
            echoes = list(ws_future)
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
        try:
            exit_code = proc.wait(30)
        except subprocess.TimeoutExpired:
            proc.kill()
            exit_code = "killed after SIGINT timeout"
    log_text = open(log_path).read()
    print("statuses:", dict(sorted(statuses.items(), key=str)), "exit:", exit_code)

    rust_panics = [line for line in log_text.splitlines() if "panicked at" in line]
    assert rust_panics == [], "\n".join(rust_panics)
    bad = {k: v for k, v in statuses.items() if k[1] != 200}
    assert not bad, f"non-2xx under load: {bad}\n{log_text[-4000:]}"
    for path in PATHS:
        assert statuses[(path, 200)] > 0, f"{path} never answered: {dict(statuses)}"
    assert echoes == [[f"echo:{i}-{j}" for j in range(5)] for i in range(4)]
    assert fork_panic_lines(log_text) == [], "\n".join(fork_panic_lines(log_text))
    assert exit_code == 0, f"exit {exit_code}\n{log_text[-4000:]}"


def test_concurrent_in_process_servers(tmp_path):
    """Several TestClient servers (mode="default", daemon threads) in one process at once,
    each serving sync and async handlers from Tokio blocking threads, then stopped. The
    main-interpreter attach must not depend on which server run is current."""
    script = tmp_path / "concurrent_testclients.py"
    script.write_text(
        """
import concurrent.futures
from pyronova import Pyronova
from pyronova.testing import TestClient

def make(i):
    app = Pyronova()

    @app.get("/s")
    def s(req):
        return {"i": i, "kind": "sync"}

    @app.get("/a")
    async def a(req):
        return {"i": i, "kind": "async"}

    return app

# Main interpreter: make(i) builds each app in a function, which workers cannot rebuild.
clients = [TestClient(make(i), mode="gil") for i in range(3)]
try:
    def hit(pair):
        i, c = pair
        return [c.get(p).json() for p in ("/s", "/a") for _ in range(50)]
    with concurrent.futures.ThreadPoolExecutor(3) as ex:
        results = list(ex.map(hit, enumerate(clients)))
    for i, rs in enumerate(results):
        assert all(r["i"] == i for r in rs), rs[:3]
        assert sum(r["kind"] == "async" for r in rs) == 50
    print("CONCURRENT_OK", flush=True)
finally:
    for c in clients:
        c.close()
"""
    )
    out = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, timeout=120,
        env=dict(os.environ, RUST_BACKTRACE="0"),
    )
    text = out.stdout + out.stderr
    assert out.returncode == 0, text[-4000:]
    assert "CONCURRENT_OK" in text, text[-4000:]
    assert [line for line in text.splitlines() if "panicked at" in line] == []
    assert fork_panic_lines(text) == []
