"""Fix wave A, W3 (workers / M7): docs/design/code-review-final-rubric.md (W3 row, F6),
docs/design/code-review-cced8c2-roadmap.md (M7) and the reconciliation's §4 FFI rows.

Server tests run a real server in a subprocess on the path they name:

  - "pool"  mode="subinterp", PYRONOVA_TPC=0  → the sub-interpreter pool (sync workers and
                                                the async engine)
  - "tpc"   mode="subinterp"                  → TPC inline sub-interpreters
  - "gil"   mode="gil", PYRONOVA_TPC=0        → handlers on the main interpreter

Each binds port 0 and reads the port it got from its "Listening on" line: no fixed ports.

The panic test needs a `--features fault_injection` build (skipped otherwise).
"""

from __future__ import annotations

import ast
import os
import re
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from pathlib import Path

import httpx
import pytest

import pyronova.engine

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
BOOTSTRAP = ROOT / "python" / "pyronova" / "_bootstrap.py"
ASYNC_ENGINE = ROOT / "python" / "pyronova" / "_async_engine.py"

PATHS = {
    "pool": {"mode": "subinterp", "PYRONOVA_TPC": "0"},
    "tpc": {"mode": "subinterp", "PYRONOVA_TPC": None},
    "gil": {"mode": "gil", "PYRONOVA_TPC": "0"},
}

RUN = """
if __name__ == "__main__":
    import os as _os
    app.run(host="127.0.0.1", port=0, mode=_os.environ["W3_MODE"],
            workers=int(_os.environ.get("W3_WORKERS", "2")))
"""

_LISTENING = re.compile(r"Listening on http://127\.0\.0\.1:(\d+)")


class Server:
    """`script` (with `RUN` appended) served on `path` in a subprocess."""

    def __init__(self, script: str, path: str = "pool", workers: int = 2,
                 env: dict[str, str] | None = None, wait: bool = True):
        fd, self.script_path = tempfile.mkstemp(prefix="pyronova_w3_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script) + RUN)
        self.log_path = self.script_path + ".log"
        full_env = dict(os.environ)
        full_env.pop("PYRONOVA_TPC", None)
        full_env["W3_MODE"] = PATHS[path]["mode"]
        full_env["W3_WORKERS"] = str(workers)
        full_env["PYRONOVA_LOG"] = "1"
        if PATHS[path]["PYRONOVA_TPC"] is not None:
            full_env["PYRONOVA_TPC"] = PATHS[path]["PYRONOVA_TPC"]
        full_env.update(env or {})
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [sys.executable, self.script_path], stdout=log, stderr=subprocess.STDOUT,
                env=full_env, preexec_fn=os.setsid,
            )
        self.base = None
        if wait:
            self._wait_up()

    def _wait_up(self) -> None:
        deadline = time.time() + 60
        while time.time() < deadline:
            found = _LISTENING.search(self.log())
            if found:
                self.base = f"http://127.0.0.1:{found.group(1)}"
                for _ in range(100):
                    try:
                        httpx.get(self.base + "/__w3_probe__", timeout=1)
                        return
                    except httpx.TransportError:
                        time.sleep(0.1)
            if self.proc.poll() is not None:
                break
            time.sleep(0.1)
        raise RuntimeError(f"server did not start:\n{self.stop()[-4000:]}")

    def get(self, target: str, timeout: float = 15) -> httpx.Response:
        return httpx.get(self.base + target, timeout=timeout)

    def log(self) -> str:
        with open(self.log_path, errors="replace") as f:
            return f.read()

    def wait_exit(self, timeout: float = 60) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        finally:
            if self.proc.poll() is None:
                os.killpg(self.proc.pid, signal.SIGKILL)

    def stop(self, timeout: float = 60) -> str:
        if self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGINT)
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(self.proc.pid, signal.SIGKILL)
                self.proc.wait()
        return self.log()


def _code(path: Path) -> str:
    """`path` without its `//` comments."""
    return "\n".join(line.split("//", 1)[0] for line in path.read_text().splitlines())


def _bootstrap_ns(assigned: tuple[str, ...] = (), **extra):
    """_bootstrap.py's top-level functions and classes (it runs inside workers and can't
    be imported), plus the module-level assignments named in `assigned` and the imports
    they need, in a fresh namespace."""
    import logging
    ns = {"_os": os, "_logging": logging,
          "_ISO": {"worker_dir": None, "path_inserted": False, "isolated": set()}}
    for node in ast.parse(BOOTSTRAP.read_text()).body:
        keep = isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Import))
        if isinstance(node, ast.Assign):
            keep = any(isinstance(t, ast.Name) and t.id in assigned for t in node.targets)
        if keep:
            exec(compile(ast.Module([node], []), str(BOOTSTRAP), "exec"), ns)
    ns.update(extra)
    return ns


# ---------------------------------------------------------------------------
# M7: FFI ownership (FR-19/C3), locks, unsafe impls
# ---------------------------------------------------------------------------


def test_workers_hold_py_references_not_raw_pointers():
    # FR-19/C3: the worker holds `Py<T>` and runs them through `Bound`; no raw-pointer
    # vectors with a manual DECREF, no `ended` flag, no hardcoded immortal threshold.
    for path in (SRC / "python").glob("*.rs"):
        code = _code(path)
        assert "PyObjRef" not in code, path
        assert "Vec<*mut ffi::PyObject>" not in code, path
        assert "ended: bool" not in code, path
        assert "<< 30" not in code, path
        assert "Py_DECREF" not in code, path
    worker = _code(SRC / "python" / "worker.rs")
    assert "handlers: Vec<Py<PyAny>>" in worker
    assert "role: ManuallyDrop<R>" in worker


def test_lock_poisoning_is_not_swallowed_in_worker_code():
    for path in [SRC / "python" / n for n in ("pool.rs", "worker.rs", "worker_api.rs",
                                              "worker_app.rs", "hook_chain.rs")] + [
            SRC / "run_context.rs", SRC / "leak_detect.rs"]:
        code = _code(path)
        assert "into_inner()" not in code, f"{path}: a poisoned lock is recovered silently"
        assert ".read().ok()" not in code, path


def test_every_remaining_unsafe_impl_says_why_and_the_pool_has_none():
    assert "unsafe impl" not in _code(SRC / "python" / "pool.rs")
    for path in [*(SRC / "python").glob("*.rs"), SRC / "run_context.rs"]:
        lines = path.read_text().splitlines()
        for n, line in enumerate(lines):
            if line.lstrip().startswith("unsafe impl"):
                above = "\n".join(lines[max(0, n - 6):n])
                assert "SAFETY:" in above, f"{path}:{n + 1}: unsafe impl without SAFETY"


def test_rebind_takes_the_error_it_reports_instead_of_clearing_it():
    for path in (SRC / "python").glob("*.rs"):
        assert "PyErr_Clear" not in _code(path), path


def test_run_context_reports_a_missing_thread_state_as_a_value():
    code = _code(SRC / "run_context.rs")
    assert 'assert!(!tstate.is_null()' not in code
    assert "fn ensure(&self, main: Interp) -> Result<(), NoThreadState>" in code


def test_conn_driver_renamed_and_interp_facade_gone():
    assert (SRC / "conn_driver.rs").exists()
    assert not (SRC / "worker.rs").exists()
    assert not (SRC / "python" / "interp.rs").exists()
    assert not (SRC / "python" / "ffi.rs").exists()
    for path in SRC.rglob("*.rs"):
        code = _code(path)
        assert "python::interp" not in code, path
        assert "crate::worker::" not in code, path


def test_worker_app_record_lives_outside_app_rs_and_does_not_import_it():
    app = _code(SRC / "app.rs")
    assert "static WORKER_APP" not in app
    assert "struct WorkerRoutes" not in app
    worker_app = _code(SRC / "python" / "worker_app.rs")
    assert "static WORKER_APP" in worker_app
    assert "struct WorkerRoutes" in worker_app
    assert "crate::app" not in worker_app
    assert "crate::app" not in _code(SRC / "python" / "worker.rs")


def test_worker_api_passes_named_objects_not_tuples():
    code = _code(SRC / "python" / "worker_api.rs")
    assert "(u64, usize" not in code and "pool_id: u64" not in code
    assert "struct AsyncJob" in code and "struct WorkerHooks" in code


def test_split_error_uses_thiserror():
    code = _code(SRC / "python" / "pool.rs")
    assert "impl std::fmt::Display for SplitError" not in code
    assert "thiserror::Error)]\npub(crate) enum SplitError" in code


def test_unit_tests_link_without_the_extension_module_feature():
    # `cargo test` links libpython (maturin sets PYO3_BUILD_EXTENSION_MODULE for the wheel),
    # so the unit-test binary links on macOS too.
    cargo = (ROOT / "Cargo.toml").read_text()
    pyo3_dep = next(line for line in cargo.splitlines() if line.startswith("pyo3 ="))
    assert "extension-module" not in pyo3_dep
    assert "extension-module" not in (ROOT / "pyproject.toml").read_text()


# ---------------------------------------------------------------------------
# M7: the async engine
# ---------------------------------------------------------------------------

ASYNC_SCRIPT = """
import asyncio, gc, sys
from pyronova import Pyronova
app = Pyronova()

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/ok")
async def ok(req):
    return {"ok": True}

@app.get("/slow")
async def slow(req):
    await asyncio.sleep(20)
    return "late"

@app.get("/kill")
async def kill(req):
    asyncio.get_running_loop().stop()
    return "stopping"

@app.get("/exit")
async def exit_(req):
    raise SystemExit("w3-exit")

@app.get("/stream")
async def stream(req):
    from pyronova import Stream
    return Stream()

@app.get("/engine")
async def engine(req):
    mod = sys.modules["__pyronova_async_engine__"]
    loops = [o for o in gc.get_objects() if isinstance(o, asyncio.AbstractEventLoop)]
    return {"timeout": mod.TASK_TIMEOUT, "loops": len(loops),
            "has_pool_id": hasattr(mod, "POOL_ID")}
"""


def test_async_engine_death_answers_its_waiters_at_once():
    srv = Server(ASYNC_SCRIPT, "pool", workers=2)
    try:
        assert srv.get("/ok").status_code == 200
        slow = {}
        t = threading.Thread(target=lambda: slow.setdefault("r", srv.get("/slow", timeout=40)))
        t.start()
        time.sleep(0.5)
        killed_at = time.time()
        srv.get("/kill", timeout=10)
        t.join(timeout=40)
        waited = time.time() - killed_at
    finally:
        out = srv.stop()
    # The in-flight request is answered when the engine lets go of it, not after the
    # 30 s request budget.
    assert slow["r"].status_code == 500, out[-3000:]
    assert waited < 5, (waited, out[-3000:])
    assert "the async engine dropped the request without answering it" in out
    assert "async worker stopped serving" in out
    assert "the async engine stopped: RuntimeError: Event loop stopped" in out


def test_system_exit_in_an_async_handler_is_a_500_and_the_engine_keeps_serving():
    srv = Server(ASYNC_SCRIPT, "pool", workers=2)
    try:
        assert srv.get("/exit").status_code == 500
        assert srv.get("/ok").json() == {"ok": True}
    finally:
        out = srv.stop()
    assert "raised SystemExit: w3-exit" in out
    assert "async worker stopped serving" not in out, out[-3000:]


def test_async_engine_gets_its_budget_from_rust_and_builds_no_extra_loop():
    srv = Server(ASYNC_SCRIPT, "pool", workers=2)
    try:
        got = srv.get("/engine").json()
    finally:
        srv.stop()
    # The request budget (30 s) less the engine's margin (2 s).
    assert got["timeout"] == 28.0
    # Only the loop the engine runs on: the async worker builds none of its own.
    assert got["loops"] == 1, got
    assert got["has_pool_id"] is False
    assert "_HANDLER_TIMEOUT" not in ASYNC_ENGINE.read_text()


def test_a_non_response_on_the_async_path_is_logged_as_the_typed_error():
    srv = Server(ASYNC_SCRIPT, "pool", workers=2)
    try:
        assert srv.get("/stream").status_code == 500
    finally:
        out = srv.stop()
    failed = [line for line in out.splitlines() if '"path":"/stream"' in line]
    assert failed, out[-3000:]
    assert ('"error":"a sub-interpreter handler returned a Stream; streaming responses need '
            'gil=True, stream=True on the route"') in failed[0], failed[0]


def test_a_failing_fetcher_ends_the_engine_once_instead_of_retrying():
    script = ASYNC_SCRIPT + """
if __name__ == "__pyronova_worker__":
    import pyronova.engine
    def _broken(inbox):
        raise RuntimeError("w3-recv-broken")
    pyronova.engine._worker_recv = _broken
"""
    srv = Server(script, "pool", workers=2)
    try:
        time.sleep(1)
    finally:
        out = srv.stop()
    stopped = [line for line in out.splitlines() if "RuntimeError: w3-recv-broken" in line]
    assert len(stopped) == 1, out[-4000:]
    assert "async worker stopped serving" in stopped[0]
    assert "fetcher error" not in out


HAS_FAULT_INJECTION = hasattr(pyronova.engine, "_fault_panic")


@pytest.mark.skipif(not HAS_FAULT_INJECTION, reason="needs `--features fault_injection`")
def test_a_rust_panic_in_an_async_handler_is_a_500_and_the_engine_survives():
    script = ASYNC_SCRIPT + """
from pyronova.engine import _fault_panic

@app.get("/panic")
async def panic(req):
    _fault_panic("w3-async-panic")
"""
    srv = Server(script, "pool", workers=2)
    try:
        assert srv.get("/panic").status_code == 500
        assert srv.get("/ok").status_code == 200
    finally:
        out = srv.stop()
    assert "w3-async-panic" in out
    assert "async worker stopped serving" not in out


# ---------------------------------------------------------------------------
# Reconcile §4: WORKER_STATES per pool; FORGOTTEN_WORKERS per run
# ---------------------------------------------------------------------------


def test_two_pool_servers_in_one_process_both_serve_async_routes(monkeypatch):
    from pyronova.testing import TestClient
    from tests.apps.w3_pool_a import app as app_a
    from tests.apps.w3_pool_b import app as app_b

    monkeypatch.setenv("PYRONOVA_TPC", "0")
    monkeypatch.setenv("PYRONOVA_WORKERS", "2")
    with TestClient(app_a, mode="subinterp") as a:
        with TestClient(app_b, mode="subinterp") as b:
            for _ in range(5):
                assert b.get("/which").json() == {"app": "b"}
                assert a.get("/which").json() == {"app": "a"}


def test_engine_has_no_process_wide_abandoned_list():
    assert not hasattr(pyronova.engine, "_forgotten_workers")
    assert pyronova.engine.PyronovaApp()._take_abandoned_workers() == []


def test_a_stuck_worker_is_reported_by_the_run_that_abandoned_it():
    script = """
import time
from pyronova import Pyronova
app = Pyronova()

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/stuck")
def stuck(req):
    time.sleep(120)
    return "late"
"""
    srv = Server(script, "pool", workers=2)
    try:
        threading.Thread(target=lambda: srv.get("/stuck", timeout=200), daemon=True).start()
        time.sleep(1)
        out = srv.stop(timeout=120)
    finally:
        if srv.proc.poll() is None:
            os.killpg(srv.proc.pid, signal.SIGKILL)
    assert srv.proc.returncode == 1, out[-3000:]
    assert "did not stop within the shutdown grace period" in out
    assert "(running GET /stuck)" in out


# ---------------------------------------------------------------------------
# Final rubric: worker start errors reach Python as themselves
# ---------------------------------------------------------------------------

START_FAILURE = """
import sys
from pyronova import Pyronova
app = Pyronova()

@app.get("/__w3_probe__")
def probe(req):
    return "up"

if __name__ == "__pyronova_worker__":
    {failure}

if __name__ == "__main__":
    import os, pyronova.engine
    try:
        app.run(host="127.0.0.1", port=0, mode="subinterp", workers=1)
    except BaseException as e:
        cause = e.__cause__
        print("RAISED", type(e).__name__, "|", isinstance(e, ImportError), "|",
              type(cause).__name__, "|", isinstance(cause, pyronova.engine.WorkerException))
        print("MESSAGE", str(e).replace("\\n", " "))
        print("CAUSE", str(cause).replace("\\n", " "))
        sys.stdout.flush()
        os._exit(3)
"""


def _start_failure(failure: str, path: str) -> str:
    fd, script = tempfile.mkstemp(prefix="pyronova_w3_", suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(textwrap.dedent(START_FAILURE).replace("{failure}", failure))
    env = dict(os.environ)
    env.pop("PYRONOVA_TPC", None)
    if PATHS[path]["PYRONOVA_TPC"] is not None:
        env["PYRONOVA_TPC"] = PATHS[path]["PYRONOVA_TPC"]
    done = subprocess.run([sys.executable, script], env=env, capture_output=True, text=True,
                          timeout=120)
    out = done.stdout + done.stderr
    assert done.returncode == 3, out[-3000:]
    return out


@pytest.mark.parametrize("path", ["pool", "tpc"])
def test_a_module_missing_in_a_worker_is_a_module_not_found_error_on_main(path):
    out = _start_failure("import w3_no_such_module_anywhere", path)
    assert "RAISED ModuleNotFoundError | True | WorkerException | True" in out, out[-3000:]
    assert "No module named 'w3_no_such_module_anywhere'" in out
    # The worker's traceback is the cause's text.
    cause = next(line for line in out.splitlines() if line.startswith("CAUSE"))
    assert "Traceback (most recent call last)" in cause, cause


def test_an_absolute_import_error_gets_no_relative_import_hint():
    out = _start_failure("from os import w3_no_such_name", "pool")
    assert "RAISED ImportError | True" in out, out[-3000:]
    assert "cannot import name 'w3_no_such_name'" in out
    assert "outside any package, so a relative import" not in out


def test_any_other_start_failure_is_a_runtime_error_with_the_worker_exception_as_cause():
    out = _start_failure('raise ValueError("w3-worker-refuses")', "pool")
    assert "RAISED RuntimeError | False | WorkerException | True" in out, out[-3000:]
    assert "ValueError: w3-worker-refuses" in out


def test_a_non_str_sys_path_entry_does_not_stop_the_workers(tmp_path):
    pkgdir = tmp_path / "pathlike"
    pkgdir.mkdir()
    (pkgdir / "w3_from_pathlike.py").write_text("VALUE = 'from a pathlib.Path entry'\n")
    script = f"""
import pathlib, sys
sys.path.append(12345)
sys.path.append(pathlib.Path({str(pkgdir)!r}))
from pyronova import Pyronova
app = Pyronova()

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/value")
def value(req):
    import w3_from_pathlike
    return w3_from_pathlike.VALUE
"""
    srv = Server(script, "pool", workers=1)
    try:
        assert srv.get("/value").text == "from a pathlib.Path entry"
    finally:
        out = srv.stop()
    assert "a sys.path entry that is not a path is left out" in out


# ---------------------------------------------------------------------------
# M7: isolate() hands its list to the workers explicitly; _bootstrap.py
# ---------------------------------------------------------------------------


def test_isolate_list_reaches_the_workers_without_the_environment():
    script = """
import os, sys
from pyronova import Pyronova
app = Pyronova()
app.isolate("w3_not_installed_lib")

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/iso")
def iso(req):
    boot = sys.modules["__pyronova_bootstrap__"]
    return {"libs": list(boot.ISOLATE_LIBS), "env": "PYRONOVA_ISOLATE_LIBS" in os.environ}
"""
    srv = Server(script, "pool", workers=1)
    try:
        got = srv.get("/iso").json()
    finally:
        srv.stop()
    assert got == {"libs": ["w3_not_installed_lib"], "env": False}


def test_isolating_pyronova_is_refused_by_the_engine():
    with pytest.raises(ValueError, match="pyronova cannot be isolated"):
        pyronova.engine.PyronovaApp().isolate(["pyronova"])


def test_packages_distributions_failure_surfaces_with_its_cause(monkeypatch):
    import importlib.metadata

    def broken():
        raise KeyError("w3-broken-metadata")

    monkeypatch.setattr(importlib.metadata, "packages_distributions", broken)
    ns = _bootstrap_ns()
    with pytest.raises(RuntimeError, match="packages_distributions") as raised:
        ns["_iso_pkg2dist"]()
    assert isinstance(raised.value.__cause__, KeyError)


def test_the_import_hook_guard_is_per_thread():
    # Thread A is inside a hooked import; thread B's import must still self-heal (clone
    # and retry) instead of being taken for A's nested import.
    inside_a = threading.Event()
    release_a = threading.Event()
    calls = {"b": 0}

    def real_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "a_mod":
            inside_a.set()
            release_a.wait(5)
            return "a"
        calls["b"] += 1
        if calls["b"] == 1:
            raise ImportError("does not support loading in subinterpreters", name="b_mod")
        return "b"

    ns = _bootstrap_ns(("_iso_hook", "_ISO_SHARED_SINGLE_PHASE", "_ISO_ERROR_MARKERS"),
                       _iso_real_import=real_import,
                       _iso_isolate=lambda top: True, _iso_evict=lambda lib: None)
    a = threading.Thread(target=lambda: ns["_iso_import"]("a_mod"))
    a.start()
    assert inside_a.wait(5)
    try:
        assert ns["_iso_import"]("b_mod") == "b"
        assert calls["b"] == 2
    finally:
        release_a.set()
        a.join(5)


def _clone_twice(tmp_path, make_src, change_src):
    src = make_src(tmp_path / "site")
    worker_dir = tmp_path / "w0"
    worker_dir.mkdir()
    ns = _bootstrap_ns(_iso_resolve_src=lambda lib: str(src))
    first = ns["_iso_clone_lib"]("lib", str(worker_dir), {})
    change_src(src)
    second = ns["_iso_clone_lib"]("lib", str(worker_dir), {})
    return first, second


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="cp -c / --reflink")
def test_a_single_file_extension_is_recloned_after_an_upgrade(tmp_path):
    def make(site):
        site.mkdir()
        src = site / "lib.cpython-314-x.so"
        src.write_bytes(b"v1")
        return src

    def upgrade(src):
        src.write_bytes(b"version two")

    first, second = _clone_twice(tmp_path, make, upgrade)
    assert first == second
    assert Path(second).read_bytes() == b"version two"


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="cp -c / --reflink")
def test_a_stale_symlinked_clone_is_removed_not_followed(tmp_path):
    target = tmp_path / "elsewhere"
    target.mkdir()
    (target / "keep.txt").write_text("keep")

    def make(site):
        site.mkdir()
        src = site / "lib.cpython-314-x.so"
        src.write_bytes(b"v1")
        return src

    src = make(tmp_path / "site")
    worker_dir = tmp_path / "w0"
    worker_dir.mkdir()
    # A stale clone that is a symlink (no manifest): removed as a link.
    (worker_dir / src.name).symlink_to(target, target_is_directory=True)
    ns = _bootstrap_ns(_iso_resolve_src=lambda lib: str(src))
    dst = ns["_iso_clone_lib"]("lib", str(worker_dir), {})
    assert Path(dst).read_bytes() == b"v1"
    assert (target / "keep.txt").read_text() == "keep"


# ---------------------------------------------------------------------------
# Final rubric: one hook chain on every synchronous path
# ---------------------------------------------------------------------------

CHAIN_SCRIPT = """
import asyncio
from pyronova import Pyronova, Response
from pyronova.context import ctx
app = Pyronova()

@app.before_request
async def tag(req):
    ctx.set("tag", "set-by-async-before")
    return None

@app.before_request
def gate(req):
    if req.path == "/gated":
        return Response(body="gated", status_code=401)
    return None

@app.after_request
def wrap(req, resp):
    if req.path == "/wrapped":
        body = resp.body if isinstance(resp.body, str) else resp.body.decode()
        return Response(body="wrapped:" + body, status_code=resp.status_code)
    return None

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/gated")
def gated(req):
    return "not reached"

@app.get("/wrapped")
def wrapped(req):
    return ctx.get("tag", "missing")

@app.get("/awaited")
def awaited(req):
    async def later():
        await asyncio.sleep(0)
        return "awaited:" + ctx.get("tag", "missing")
    return later()
"""


@pytest.mark.parametrize("path", ["gil", "tpc", "pool"])
def test_the_hook_chain_is_the_same_on_every_sync_path(path):
    srv = Server(CHAIN_SCRIPT, path, workers=2)
    try:
        gated = srv.get("/gated")
        wrapped = srv.get("/wrapped")
        awaited = srv.get("/awaited")
    finally:
        srv.stop()
    assert (gated.status_code, gated.text) == (401, "gated")
    assert (wrapped.status_code, wrapped.text) == (200, "wrapped:set-by-async-before")
    assert (awaited.status_code, awaited.text) == (200, "awaited:set-by-async-before")
