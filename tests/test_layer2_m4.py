"""Layer 2, M4 (issue #6): workers run the real `pyronova` package and engine.

End-to-end tests from docs/design/real-engine-in-workers.md §9 that are new at M4. Each
starts a real server in a subprocess (a sub-interpreter permanently disables
`PyGILState_Check` in its process), stops it with SIGINT, and scans its whole log for the
PyO3 fork's panic texts.
"""
from __future__ import annotations

import collections
import concurrent.futures
import json
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
import uuid

import httpx
import pytest

from conftest import fork_panic_lines
from tests._helpers import bound_port, listening_ports

HERE = os.path.dirname(os.path.abspath(__file__))
PG_DSN = os.environ.get("PYRONOVA_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"), reason="own-GIL sub-interpreters"
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class Server:
    def __init__(self, tmp_path, source: str, extra_env: dict | None = None, argv=None):
        self._port: int | None = None
        self.script = tmp_path / "app.py"
        self.script.write_text(textwrap.dedent(source))
        self.log_path = tmp_path / "server.log"
        env = dict(os.environ)
        env["L2_PORT"] = "0"
        env.update(extra_env or {})
        cmd = argv or [sys.executable, str(self.script)]
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                cmd, stdout=log, stderr=subprocess.STDOUT, env=env,
                cwd=str(tmp_path), preexec_fn=os.setsid,
            )

    def log(self) -> str:
        return self.log_path.read_text(errors="replace")

    @property
    def port(self) -> int:
        """The port the server bound (it binds port 0), from its startup line."""
        if self._port is None:
            self._port = bound_port(self.log, self.proc)
        return self._port

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def wait_up(self, path: str = "/ping", timeout: float = 90) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(
                    f"server exited early (code {self.proc.returncode}):\n{self.log()[-4000:]}"
                )
            if self._port is None:
                ports = listening_ports(self.log())
                self._port = ports[0] if ports else None
            if self._port is not None:
                try:
                    if httpx.get(self.base + path, timeout=1).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
            time.sleep(0.2)
        raise AssertionError(f"server not up in {timeout}s:\n{self.log()[-4000:]}")

    def wait_exit(self, timeout: float = 90) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            raise AssertionError(f"server did not exit in {timeout}s:\n{self.log()[-4000:]}")

    def stop(self, timeout: float = 30) -> int:
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
        return self.wait_exit(timeout)

    def kill(self) -> None:
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            self.proc.wait(timeout=5)


def _no_panics(server: Server) -> None:
    text = server.log()
    assert fork_panic_lines(text) == [], text[-4000:]
    assert "panicked at" not in text, text[-4000:]


RUN = """
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ["L2_PORT"]), workers={workers})
"""


def _app(body: str, workers: int = 2, header: str = "") -> str:
    return (
        textwrap.dedent(header) + "import os\nfrom pyronova import Pyronova\n"
        + textwrap.dedent(body) + RUN.format(workers=workers)
    )


# ---------------------------------------------------------------------------
# E2E-3a: a route table that differs in a worker fails startup with both lists
# ---------------------------------------------------------------------------


def test_route_only_in_worker_fails_startup_with_both_lists(tmp_path):
    s = Server(tmp_path, _app("""
        import pyronova.engine
        app = Pyronova()

        @app.get("/ping")
        def ping(req):
            return "ok"

        if pyronova.engine._in_worker():
            @app.get("/only-in-worker")
            def extra(req):
                return "extra"
    """))
    rc = s.wait_exit()
    log = s.log()
    assert rc != 0, log
    assert "the main interpreter registered:" in log, log[-3000:]
    assert "this worker's script registered:" in log
    assert "/only-in-worker" in log and "first difference" in log
    assert fork_panic_lines(log) == []


# ---------------------------------------------------------------------------
# E2E-1 (new cases): closure hooks now run in workers; CORS is not applied twice
# ---------------------------------------------------------------------------


def test_worker_cors_header_applied_once(tmp_path):
    # `_cors_before` (a closure hook) now runs in workers too; with Rust's apply_cors
    # also setting the header, it must still appear exactly once (design R-1).
    s = Server(tmp_path, _app("""
        app = Pyronova()
        app.enable_cors(allow_origins="https://a.example")

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/data")
        def data(req):
            return {"ok": True}
    """))
    try:
        s.wait_up()
        r = httpx.get(s.base + "/data", headers={"Origin": "https://a.example"}, timeout=10)
    finally:
        rc = s.stop()
    assert r.status_code == 200 and r.json() == {"ok": True}
    assert r.headers.get_list("access-control-allow-origin") == ["https://a.example"]
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-4: app.state and a bare SharedState() in workers are main's map (FR-5)
# ---------------------------------------------------------------------------


def test_worker_shared_state_is_mains(tmp_path):
    s = Server(tmp_path, _app("""
        from pyronova import SharedState
        app = Pyronova()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/n")
        def n(req):
            app.state.incr("n", 1)
            return "ok"

        @app.get("/m")
        def m(req):
            SharedState().incr("m", 1)
            return "ok"

        @app.get("/read", gil=True)
        def read(req):
            return {"n": app.state.get("n"), "m": app.state.get("m")}
    """, workers=4))
    try:
        s.wait_up()
        with concurrent.futures.ThreadPoolExecutor(16) as pool:
            statuses = list(pool.map(
                lambda i: httpx.get(s.base + ("/n" if i % 2 else "/m"), timeout=10).status_code,
                range(800),
            ))
        read = httpx.get(s.base + "/read", timeout=10).json()
    finally:
        rc = s.stop()
    assert statuses == [200] * 800
    assert read == {"n": "400", "m": "400"}
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-5 (fetch_iter from a worker) and E2E-6 (*_async refused in a worker)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(PG_DSN is None, reason="PYRONOVA_TEST_PG_DSN not set")
def test_worker_pgpool_fetch_iter_and_async_refused(tmp_path):
    s = Server(tmp_path, _app("""
        from pyronova.db import PgPool
        app = Pyronova()
        pool = PgPool.connect(os.environ["PYRONOVA_TEST_PG_DSN"])

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/iter")
        def it(req):
            rows = [r["i"] for r in pool.fetch_iter("SELECT generate_series(1, $1::int) AS i", 250)]
            return {"n": len(rows), "sum": sum(rows)}

        @app.get("/async")
        async def a(req):
            return {"rows": await pool.fetch_all_async("SELECT 1 AS one")}
    """), {"PYRONOVA_TEST_PG_DSN": PG_DSN})
    try:
        s.wait_up()
        results = [httpx.get(s.base + "/iter", timeout=10).json() for _ in range(20)]
        refused = httpx.get(s.base + "/async", timeout=10)
        alive = httpx.get(s.base + "/ping", timeout=10).status_code
    finally:
        rc = s.stop()
    assert results == [{"n": 250, "sum": 250 * 251 // 2}] * 20
    assert refused.status_code == 500
    assert "fetch_all_async is not available in sub-interpreter workers" in s.log()
    assert alive == 200
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-7 / E2E-7b: async pool path returns headers, runs hooks per request (FR-14)
# ---------------------------------------------------------------------------


def test_async_pool_returns_headers_and_runs_hooks(tmp_path):
    s = Server(tmp_path, _app("""
        from pyronova import Response
        app = Pyronova()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.before_request
        def mark(req):
            return None

        @app.after_request
        def add(req, resp):
            return Response(resp.body, status_code=resp.status_code,
                            content_type=resp.content_type,
                            headers={**resp.headers, "x-after": "1"})

        @app.get("/a")
        async def a(req):
            return Response("async", headers={"x-a": "1"})

        @app.get("/dict")
        async def d(req):
            return {"k": 1}
    """), {"PYRONOVA_TPC": "0"})
    try:
        s.wait_up()
        r = httpx.get(s.base + "/a", timeout=10)
        d = httpx.get(s.base + "/dict", timeout=10)
    finally:
        rc = s.stop()
    assert r.status_code == 200 and r.text == "async"
    assert r.headers["x-a"] == "1" and r.headers["x-after"] == "1"
    assert d.json() == {"k": 1} and d.headers["x-after"] == "1"
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


def test_async_pool_request_ids_do_not_cross(tmp_path):
    s = Server(tmp_path, _app("""
        import asyncio, random
        app = Pyronova()
        app.enable_request_id()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/slow")
        async def slow(req):
            await asyncio.sleep(random.random() / 50)
            return "ok"
    """), {"PYRONOVA_TPC": "0"})
    try:
        s.wait_up()

        def one(_):
            rid = uuid.uuid4().hex
            r = httpx.get(s.base + "/slow", headers={"X-Request-ID": rid}, timeout=15)
            return rid, r.status_code, r.headers.get("x-request-id")

        with concurrent.futures.ThreadPoolExecutor(32) as pool:
            results = list(pool.map(one, range(200)))
    finally:
        rc = s.stop()
    assert all(status == 200 for _, status, _ in results)
    assert [(sent, got) for sent, _, got in results if sent != got] == []
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-10 surface parity, E2E-18 pyclass module, E2E-20 script as a real module
# ---------------------------------------------------------------------------

_ENGINE_NAMES = [
    "PyronovaApp", "Request", "Response", "WebSocket", "SharedState", "Stream",
    "PgPool", "PgCursor", "get_gil_metrics", "init_logger", "emit_python_log", "_in_worker",
    "_forgotten_workers", "_worker_recv", "_worker_send", "_worker_to_response",
    "_worker_app_handlers", "_worker_app_hooks",
]


def test_worker_surface_matches_main_and_script_is_a_module(tmp_path):
    s = Server(tmp_path, _app(f"""
        import dataclasses, typing
        import pyronova, pyronova.engine
        app = Pyronova()
        NAMES = {_ENGINE_NAMES!r}

        @dataclasses.dataclass
        class Point:
            x: int
            y: "list[int]"

        def surface():
            return {{
                "all": sorted(pyronova.__all__),
                "methods": sorted(m for m in vars(pyronova.Pyronova) if not m.startswith("_")),
                "engine": [n for n in NAMES if hasattr(pyronova.engine, n)],
            }}

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/worker")
        def worker(req):
            return {{
                "surface": surface(),
                "in_worker": pyronova.engine._in_worker(),
                "type": repr(type(req)),
                "hints": sorted(typing.get_type_hints(Point)),
                "module": __name__,
            }}

        @app.get("/main", gil=True)
        def main(req):
            return {{"surface": surface(), "in_worker": pyronova.engine._in_worker()}}

        @app.get("/boom")
        def boom(req):
            raise ValueError("boom-marker")
    """, header="from __future__ import annotations\n"),
        {"PYRONOVA_LOG": "1"})
    try:
        s.wait_up()
        worker = httpx.get(s.base + "/worker", timeout=10).json()
        main = httpx.get(s.base + "/main", timeout=10).json()
        boom = httpx.get(s.base + "/boom", timeout=10)
        time.sleep(0.5)  # let the log line flush
    finally:
        rc = s.stop()
    # E2E-10: the same public surface in a worker as on main (FR-12).
    assert worker["in_worker"] is True and main["in_worker"] is False
    assert worker["surface"] == main["surface"]
    assert worker["surface"]["engine"] == _ENGINE_NAMES
    # E2E-18: pyclasses are pyronova.engine's (FR-18).
    assert worker["type"] == "<class 'pyronova.engine.Request'>"
    # E2E-20: `from __future__ import annotations` works, get_type_hints resolves worker
    # classes, and a handler's traceback names the script and line (FR-20).
    assert worker["hints"] == ["x", "y"] and worker["module"] == "__pyronova_worker__"
    assert boom.status_code == 500
    log = s.log()
    assert "boom-marker" in log
    assert f'File "{s.script}", line' in log or f"File \\\"{s.script}\\\", line" in log, log[-3000:]
    assert rc == 0, log[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-11: worker log records carry their worker id; main's carry none (FR-8)
# ---------------------------------------------------------------------------


def test_worker_logs_carry_worker_id(tmp_path):
    s = Server(tmp_path, _app("""
        import logging
        app = Pyronova(log_config={"level": "INFO", "format": "json", "access_log": False})
        logging.getLogger("e2e11").warning("init-line")

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/main_log", gil=True)
        def main_log(req):
            logging.getLogger("e2e11").warning("main-line")
            return "ok"
    """, workers=4))
    try:
        s.wait_up()
        httpx.get(s.base + "/main_log", timeout=10)
        time.sleep(0.5)
    finally:
        rc = s.stop()
    records = []
    for line in s.log().splitlines():
        try:
            records.append(json.loads(line))
        except ValueError:
            pass
    fields = [r.get("fields", {}) for r in records]
    init = collections.Counter(f.get("worker") for f in fields if f.get("message") == "init-line")
    main = [f for f in fields if f.get("message") == "main-line"]
    assert init == {0: 1, 1: 1, 2: 1, 3: 1}, s.log()[-3000:]
    assert len(main) == 1 and "worker" not in main[0], main
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-13 two apps, E2E-14 isolate("pyronova"), E2E-14b pyronova never re-executed
# ---------------------------------------------------------------------------


def test_second_app_in_worker_fails_startup(tmp_path):
    s = Server(tmp_path, _app("""
        app = Pyronova()
        other = Pyronova()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @other.get("/other")
        def o(req):
            return "other"
    """))
    rc = s.wait_exit()
    log = s.log()
    assert rc != 0, log
    assert "second app" in log, log[-3000:]
    assert fork_panic_lines(log) == []


def test_isolate_pyronova_fails_on_main(tmp_path):
    s = Server(tmp_path, _app("""
        app = Pyronova()
        app.isolate("pyronova")

        @app.get("/ping")
        def ping(req):
            return "ok"
    """))
    rc = s.wait_exit()
    log = s.log()
    assert rc != 0, log
    assert "pyronova cannot be isolated" in log, log[-3000:]
    # Refused on main, before any worker exists.
    assert "Listening on" not in log


def test_reactive_isolation_never_reexecutes_pyronova(tmp_path):
    pytest.importorskip("pydantic_settings")
    s = Server(tmp_path, _app("""
        import sys
        from pyronova.context import ctx
        app = Pyronova()
        app.enable_request_id()
        ENGINE = sys.modules["pyronova.engine"]

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/settings")
        def settings(req):
            from pyronova.config import Settings  # pydantic_settings -> pydantic_core
            return {
                "same_engine": sys.modules["pyronova.engine"] is ENGINE,
                "rid": ctx.request_id(),
                "settings": Settings.__name__,
            }
    """))
    try:
        s.wait_up()
        rids = [uuid.uuid4().hex for _ in range(4)]
        out = [httpx.get(s.base + "/settings", headers={"X-Request-ID": r}, timeout=120)
               for r in rids]
    finally:
        rc = s.stop()
    for rid, r in zip(rids, out):
        assert r.status_code == 200, s.log()[-3000:]
        assert r.headers["x-request-id"] == rid
        assert r.json() == {"same_engine": True, "rid": rid, "settings": "Settings"}
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


def test_isolation_error_under_pyronova_import_keeps_the_package(tmp_path):
    """E2E-14b, the exact path of M4 review B5: an isolation-class ImportError raised
    inside an `import pyronova.<sub>` statement makes the reactive hook clone the failing
    package and retry the statement. Before FR-11 it also evicted the statement's
    package, `pyronova`, and the retry re-executed it (a second `pyronova.engine` module
    object, duplicate ContextVars). A meta-path finder that fails once stands in for a
    pyronova submodule that imports a non-shareable extension."""
    pkg = tmp_path / "e2efakepkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    s = Server(tmp_path, _app("""
        import sys
        import pyronova.engine
        app = Pyronova()
        ENGINE = sys.modules["pyronova.engine"]
        PACKAGE = sys.modules["pyronova"]

        class _FailOnce:
            fired = False

            def find_spec(self, name, path=None, target=None):
                if name == "pyronova._e2e_trigger" and not _FailOnce.fired:
                    _FailOnce.fired = True
                    raise ImportError(
                        "e2efakepkg._ext does not support loading in subinterpreters",
                        name="e2efakepkg",
                    )
                return None

        if pyronova.engine._in_worker():
            sys.meta_path.insert(0, _FailOnce())

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/trigger")
        def trigger(req):
            try:
                import pyronova._e2e_trigger  # noqa: F401
            except ImportError as e:
                outcome = type(e).__name__
            else:
                outcome = "imported"
            return {
                "fired": _FailOnce.fired,
                "outcome": outcome,
                "same_engine": sys.modules["pyronova.engine"] is ENGINE,
                "same_package": sys.modules["pyronova"] is PACKAGE,
            }
    """), {"PYTHONPATH": str(tmp_path)})
    try:
        s.wait_up()
        r = httpx.get(s.base + "/trigger", timeout=60).json()
    finally:
        rc = s.stop()
    assert r["fired"] is True, r
    assert r["outcome"] == "ModuleNotFoundError", r
    assert r["same_engine"] is True and r["same_package"] is True, r
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-15: pydantic only for model= apps, and model= validates in workers
# ---------------------------------------------------------------------------


def test_pydantic_not_imported_without_model(tmp_path):
    pytest.importorskip("pydantic")
    s = Server(tmp_path, _app("""
        import sys
        app = Pyronova()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/mods")
        def mods(req):
            return {"pydantic_core": "pydantic_core" in sys.modules,
                    "pydantic": "pydantic" in sys.modules}
    """))
    try:
        s.wait_up()
        mods = httpx.get(s.base + "/mods", timeout=10).json()
    finally:
        rc = s.stop()
    assert mods == {"pydantic_core": False, "pydantic": False}
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


@pytest.mark.parametrize("tpc", ["1", "0"])
def test_model_validates_in_workers_through_reactive_isolation(tmp_path, tpc):
    pytest.importorskip("pydantic")
    s = Server(tmp_path, _app("""
        from pydantic import BaseModel
        import pyronova.engine
        app = Pyronova()

        class Item(BaseModel):
            name: str
            qty: int

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.post("/items", model=Item)
        def create(req, item):
            return {"name": item.name, "qty": item.qty,
                    "worker": pyronova.engine._in_worker()}
    """), {"PYRONOVA_TPC": tpc})
    try:
        s.wait_up(timeout=180)
        ok = httpx.post(s.base + "/items", content=json.dumps({"name": "a", "qty": 2}),
                        timeout=30)
        bad = httpx.post(s.base + "/items", content=json.dumps({"name": "a", "qty": "x"}),
                         timeout=30)
    finally:
        rc = s.stop(timeout=60)
    assert ok.status_code == 200, s.log()[-3000:]
    assert ok.json() == {"name": "a", "qty": 2, "worker": True}
    assert bad.status_code == 422, (bad.status_code, bad.text)
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-16 Stream from a worker, E2E-17 main-only process-wide setters
# ---------------------------------------------------------------------------


def test_stream_from_worker_is_a_loud_500(tmp_path):
    s = Server(tmp_path, _app("""
        from pyronova import Stream
        app = Pyronova()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/stream")
        def stream(req):
            return Stream()
    """), {"PYRONOVA_LOG": "1"})
    try:
        s.wait_up()
        r = httpx.get(s.base + "/stream", timeout=10)
        time.sleep(0.5)
    finally:
        rc = s.stop()
    assert r.status_code == 500
    assert "gil=True, stream=True" in s.log(), s.log()[-3000:]
    assert rc == 0
    _no_panics(s)


def test_worker_max_body_size_is_mains(tmp_path):
    s = Server(tmp_path, _app("""
        import pyronova.engine
        app = Pyronova(log_config={"level": "WARN"})
        app.max_body_size = 2048
        if pyronova.engine._in_worker():
            app.max_body_size = 999_999

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.post("/upload")
        def upload(req):
            return {"len": len(req.body)}
    """))
    try:
        s.wait_up()
        big = httpx.post(s.base + "/upload", content=b"x" * 4000, timeout=10)
        small = httpx.post(s.base + "/upload", content=b"x" * 100, timeout=10)
    finally:
        rc = s.stop()
    assert big.status_code == 413 and small.json() == {"len": 100}
    assert "set_max_body_size(999999) in a worker is ignored" in s.log()
    assert rc == 0
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-19 teardown (FR-19): clean stop, failed start, abandoned worker
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tpc", ["1", "0"])
def test_graceful_stop_ends_workers_cleanly(tmp_path, tpc):
    s = Server(tmp_path, _app("""
        app = Pyronova()

        @app.get("/ping")
        def ping(req):
            return {"ok": True}

        @app.get("/a")
        async def a(req):
            return {"async": True}
    """, workers=4), {"PYRONOVA_TPC": tpc})
    try:
        s.wait_up()
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            codes = list(pool.map(
                lambda i: httpx.get(s.base + ("/a" if i % 2 else "/ping"), timeout=10).status_code,
                range(200),
            ))
    finally:
        rc = s.stop()
    assert codes == [200] * 200
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


@pytest.mark.parametrize("tpc", ["1", "0"])
def test_failed_worker_start_exits_nonzero_without_abort(tmp_path, tpc):
    s = Server(tmp_path, _app("""
        import sys
        import pyronova.engine
        if pyronova.engine._in_worker() and sys.modules["__pyronova_bootstrap__"].WORKER_ID == 2:
            raise RuntimeError("worker two refuses to start")
        app = Pyronova()

        @app.get("/ping")
        def ping(req):
            return "ok"
    """, workers=4), {"PYRONOVA_TPC": tpc})
    rc = s.wait_exit()
    log = s.log()
    assert rc not in (0, -signal.SIGABRT, -signal.SIGSEGV), (rc, log[-3000:])
    assert "worker two refuses to start" in log
    assert "Abort" not in log and "Fatal Python error" not in log, log[-3000:]
    assert fork_panic_lines(log) == []


def test_worker_stuck_past_shutdown_exits_nonzero_not_abort(tmp_path):
    s = Server(tmp_path, _app("""
        import time
        app = Pyronova()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/stuck")
        def stuck(req):
            time.sleep(120)
            return "late"
    """), {"PYRONOVA_TPC": "0"})
    try:
        s.wait_up()
        threading.Thread(
            target=lambda: httpx.get(s.base + "/stuck", timeout=200), daemon=True
        ).start()
        time.sleep(1)
        rc = s.stop(timeout=120)
    finally:
        s.kill()
    log = s.log()
    assert rc == 1, (rc, log[-3000:])
    assert "did not stop within the shutdown grace period" in log
    assert "GET /stuck" in log
    assert fork_panic_lines(log) == []


# ---------------------------------------------------------------------------
# E2E-21 the CLI, E2E-22 raw-engine scripts
# ---------------------------------------------------------------------------


def test_cli_run_serves_worker_routes(tmp_path):
    (tmp_path / "cliapp.py").write_text(textwrap.dedent("""
        import pyronova.engine
        from pyronova import Pyronova
        app = Pyronova()

        @app.get("/ping")
        def ping(req):
            return {"worker": pyronova.engine._in_worker()}
    """))
    s = Server(tmp_path, "", argv=[
        sys.executable, "-m", "pyronova.cli", "run", "cliapp:app",
        "--port", "0", "--workers", "2",
    ])
    try:
        s.wait_up()
        r = httpx.get(s.base + "/ping", timeout=10).json()
    finally:
        rc = s.stop()
    assert r == {"worker": True}
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


def test_raw_engine_script_serves_unchanged(tmp_path):
    s = Server(tmp_path, """
        import os
        from pyronova.engine import PyronovaApp

        app = PyronovaApp()

        def hello(req):
            return {"hello": "raw"}

        def ping(req):
            return "ok"

        app.get("/", hello)
        app.get("/ping", ping)

        if __name__ == "__main__":
            app.run(host="127.0.0.1", port=int(os.environ["L2_PORT"]), mode="subinterp",
                    workers=2)
    """)
    try:
        s.wait_up()
        r = httpx.get(s.base + "/", timeout=10).json()
    finally:
        rc = s.stop()
    assert r == {"hello": "raw"}
    assert rc in (0, -signal.SIGINT), s.log()[-3000:]
    _no_panics(s)
