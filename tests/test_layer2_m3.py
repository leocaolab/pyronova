"""Layer 2, M3 (issue #5): registration seal, worker-app registry and inert prerequisites.

These tests cover the main-interpreter behaviour. The worker side of M3 (FR-4, FR-5,
FR-15, FR-17, FR-18) was tested through a probe app that loaded the real engine next to
the mock; since M4 workers run the real package, so that coverage is in
`test_layer2_m4.py` (probe app and test removed at M4, approved by the user on
2026-09-23).
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import signal
import subprocess
import sys
import textwrap
import time
import uuid

import httpx
import pytest

import pyronova.engine as engine
from conftest import _free_port, fork_panic_lines
from pyronova import Pyronova

HERE = os.path.dirname(os.path.abspath(__file__))

subinterp_only = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"), reason="own-GIL sub-interpreters"
)


# ---------------------------------------------------------------------------
# helpers: a server subprocess with a log file, stopped with SIGINT
# ---------------------------------------------------------------------------


def _start(script_path: str, port: int, log_path: str, extra_env: dict | None = None):
    env = dict(os.environ)
    env["L2_PORT"] = str(port)
    env.update(extra_env or {})
    log = open(log_path, "w")
    proc = subprocess.Popen(
        [sys.executable, script_path], stdout=log, stderr=subprocess.STDOUT,
        env=env, cwd=HERE, preexec_fn=os.setsid,
    )
    log.close()
    return proc


def _wait_up(base: str, path: str, proc: subprocess.Popen, log_path: str) -> None:
    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            raise AssertionError(
                f"server exited early (code {proc.returncode}):\n{open(log_path).read()[-4000:]}"
            )
        try:
            if httpx.get(base + path, timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise AssertionError(f"server not up in 60s:\n{open(log_path).read()[-4000:]}")


def _stop(proc: subprocess.Popen) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=20)
    except Exception:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=5)


def _write(tmp_path, name: str, source: str) -> str:
    path = tmp_path / name
    path.write_text(textwrap.dedent(source))
    return str(path)


# ---------------------------------------------------------------------------
# main interpreter: seal (FR-2, N12), _in_worker (FR-15), module= (FR-18)
# ---------------------------------------------------------------------------


def test_in_worker_is_false_on_main():
    assert engine._in_worker() is False


def test_pyclasses_live_in_pyronova_engine():
    for cls in (engine.Request, engine.Response, engine.SharedState, engine.Stream,
                engine.WebSocket, engine.PyronovaApp):
        assert cls.__module__ == "pyronova.engine", cls


def test_non_gil_route_after_seal_raises_with_method_and_path():
    app = Pyronova()

    @app.get("/early")
    def early(req):
        return "ok"

    app._engine._seal_registrations()

    with pytest.raises(ValueError, match=r"GET /late .*gil=True"):
        @app.get("/late")
        def late(req):
            return "no"

    @app.get("/late-gil", gil=True)
    def late_gil(req):
        return "ok"


def test_seal_is_idempotent():
    app = Pyronova()
    app._engine._seal_registrations()

    @app.get("/after", gil=True)
    def after(req):
        return "ok"

    # A second run() (TestClient retries on port races) seals again: no error, and the
    # rule still holds.
    app._engine._seal_registrations()
    with pytest.raises(ValueError, match=r"POST /x "):
        app._engine.route("POST", "/x", lambda req: "no", False)


def test_on_startup_non_gil_route_fails_startup(tmp_path):
    port = _free_port()
    script = _write(tmp_path, "startup_route.py", f"""
        from pyronova import Pyronova
        app = Pyronova()

        @app.get("/")
        def index(req):
            return "ok"

        @app.on_startup
        def add_route_late():
            @app.get("/late")
            def late(req):
                return "no"

        app.run(host="127.0.0.1", port={port}, mode="gil")
    """)
    out = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=60)
    assert out.returncode != 0, out.stdout + out.stderr
    assert "GET /late is registered after app.run() started" in out.stderr, out.stderr


# ---------------------------------------------------------------------------
# FR-13: pydantic only for routes with model=
# ---------------------------------------------------------------------------


def test_import_pyronova_does_not_import_pydantic():
    pytest.importorskip("pydantic")
    out = subprocess.run(
        [sys.executable, "-c",
         "import sys, pyronova; from pyronova import Pyronova; Pyronova();"
         "print('pydantic' in sys.modules, 'pydantic_core' in sys.modules)"],
        capture_output=True, text=True, timeout=60,
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.split() == ["False", "False"]


def test_model_route_still_validates():
    pytest.importorskip("pydantic")
    from pyronova.testing import TestClient

    # Its own module: a worker serves one app per module.
    from tests.apps.l2_model_route import app

    with TestClient(app) as client:
        ok = client.post("/items", body=json.dumps({"name": "a", "qty": 2}))
        bad = client.post("/items", body=json.dumps({"name": "a", "qty": "x"}))
    assert ok.status_code == 200 and ok.json() == {"name": "a", "qty": 2}
    assert bad.status_code == 422


# ---------------------------------------------------------------------------
# FR-14 / E2E-7b (GIL mode half): request ids never cross between concurrent requests
# ---------------------------------------------------------------------------


def test_request_id_is_per_request_under_concurrency(tmp_path):
    port = _free_port()
    log_path = str(tmp_path / "server.log")
    script = _write(tmp_path, "request_id.py", f"""
        import asyncio, random
        from pyronova import Pyronova
        app = Pyronova()
        app.enable_request_id()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/slow")
        async def slow(req):
            await asyncio.sleep(random.random() / 50)
            return "ok"

        app.run(host="127.0.0.1", port={port}, mode="gil")
    """)
    base = f"http://127.0.0.1:{port}"
    proc = _start(script, port, log_path)
    try:
        _wait_up(base, "/ping", proc, log_path)

        def one(_):
            rid = uuid.uuid4().hex
            r = httpx.get(base + "/slow", headers={"X-Request-ID": rid}, timeout=10)
            return rid, r.status_code, r.headers.get("x-request-id")

        with concurrent.futures.ThreadPoolExecutor(32) as pool:
            results = list(pool.map(one, range(200)))
    finally:
        _stop(proc)
    assert all(status == 200 for _, status, _ in results)
    mismatched = [(sent, got) for sent, _, got in results if sent != got]
    assert mismatched == []


def test_request_id_hooks_keep_interleaved_requests_apart():
    """FR-14 at M3: two requests whose hooks interleave on one event-loop thread (as async
    requests do in a worker's loop, M4) each get their own id back. The GIL-mode E2E above
    can't catch a thread-local: there, one request's hooks run back to back on one thread."""
    import asyncio

    from pyronova.engine import Response
    from pyronova.observability import install_request_id

    class _App:
        def before_request(self, fn):
            self.before = fn

        def after_request(self, fn):
            self.after = fn

    class _Req:
        def __init__(self, rid):
            self.headers = {"x-request-id": rid}

    app = _App()
    install_request_id(app, "X-Request-ID")

    async def one(rid, first_done, second_done, first):
        req = _Req(rid)
        app.before(req)
        if first:
            first_done.set()
            await second_done.wait()  # the other request's before hook runs in between
        else:
            await first_done.wait()
            second_done.set()
        return app.after(req, Response(body="ok")).headers["x-request-id"]

    async def main():
        a, b = asyncio.Event(), asyncio.Event()
        return await asyncio.gather(one("id-1", a, b, True), one("id-2", a, b, False))

    assert asyncio.run(main()) == ["id-1", "id-2"]


# ---------------------------------------------------------------------------
# E2E-3b + E2E-1 subset: app.run() only under a __main__ guard, PYRONOVA_LOG=1, /mcp
# ---------------------------------------------------------------------------


@subinterp_only
def test_main_guard_logging_and_mcp_in_subinterp_mode(tmp_path):
    port = _free_port()
    log_path = str(tmp_path / "server.log")
    script = _write(tmp_path, "guarded.py", f"""
        from pyronova import Pyronova
        app = Pyronova()

        @app.get("/w")
        def w(req):
            return {{"ok": True}}

        @app.mcp.tool(description="add two numbers")
        def add(a: int, b: int) -> int:
            return a + b

        if __name__ == "__main__":
            app.run(host="127.0.0.1", port={port}, workers=2)
    """)
    base = f"http://127.0.0.1:{port}"
    proc = _start(script, port, log_path, {"PYRONOVA_LOG": "1"})
    try:
        _wait_up(base, "/w", proc, log_path)
        assert httpx.get(base + "/w", timeout=5).json() == {"ok": True}
        r = httpx.post(base + "/mcp", timeout=5, content=json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
        assert r.status_code == 200
        assert [t["name"] for t in r.json()["result"]["tools"]] == ["add"]
    finally:
        _stop(proc)
    log = open(log_path).read()
    assert fork_panic_lines(log) == []
    assert "Traceback" not in log, log[-3000:]
