"""Review cced8c2, reconciliation milestone R-a (docs/design/code-review-cced8c2-reconcile.md §6).

Every test here runs a real server in a subprocess on the path it names, so it exercises
the dispatch code that path runs:

  - "gil"   mode="gil", PYRONOVA_TPC=0          → handlers on main (Tokio blocking pool)
  - "tpc"   mode="subinterp"                    → TPC inline sub-interpreters; gil=True
                                                  routes go through the main-interp bridge
  - "pool"  mode="subinterp", PYRONOVA_TPC=0    → sub-interp pool; gil=True routes on main

The panic tests need a `--features fault_injection` build (they are skipped otherwise):

    maturin develop --release --features fault_injection
    pytest tests/test_review_ra.py -k panic
"""

from __future__ import annotations

import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time

import pytest

import pyronova.engine

PYTHON = sys.executable
HOST = "127.0.0.1"

PATHS = {
    "gil": {"mode": "gil", "tpc": "0"},
    "tpc": {"mode": "subinterp", "tpc": None},
    "pool": {"mode": "subinterp", "tpc": "0"},
}

GENERIC_500 = "Internal Server Error"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


class Reply:
    def __init__(self, status: int, body: bytes, headers: list[tuple[str, str]]):
        self.status = status
        self.body = body
        self.headers = {k.lower(): v for k, v in headers}

    def json(self):
        return json.loads(self.body)


class Server:
    """`script` served on `path`. The script's `app.run` reads mode, port and workers from
    `RA_MODE` / `RA_PORT` / `RA_WORKERS` (see `RUN`)."""

    def __init__(
        self,
        script: str,
        path: str,
        workers: int = 2,
        env: dict[str, str] | None = None,
        wait: bool = True,
    ):
        self.path = path
        self.port = _free_port()
        fd, self.script_path = tempfile.mkstemp(prefix="pyronova_ra_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.log_path = self.script_path + ".log"
        full_env = dict(os.environ)
        full_env["RA_MODE"] = PATHS[path]["mode"]
        full_env["RA_PORT"] = str(self.port)
        full_env["RA_WORKERS"] = str(workers)
        full_env.pop("PYRONOVA_TPC", None)
        full_env.pop("PYRONOVA_LOG", None)
        if PATHS[path]["tpc"] is not None:
            full_env["PYRONOVA_TPC"] = PATHS[path]["tpc"]
        full_env.update(env or {})
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [PYTHON, self.script_path],
                stdout=log,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid,
                env=full_env,
            )
        if not wait:
            return
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with socket.create_connection((HOST, self.port), timeout=0.5):
                    return
            except OSError:
                if self.proc.poll() is not None:
                    break
                time.sleep(0.1)
        raise RuntimeError(f"server ({path}) did not start:\n{self.stop()}")

    def request(
        self,
        method: str,
        target: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 15,
    ) -> Reply:
        conn = http.client.HTTPConnection(HOST, self.port, timeout=timeout)
        try:
            conn.request(method, target, body=body, headers=headers or {})
            r = conn.getresponse()
            return Reply(r.status, r.read(), r.getheaders())
        finally:
            conn.close()

    def get(self, target: str, **kw) -> Reply:
        return self.request("GET", target, **kw)

    def post(self, target: str, body: bytes, **kw) -> Reply:
        return self.request("POST", target, body=body, **kw)

    def log(self) -> str:
        with open(self.log_path, errors="replace") as f:
            return f.read()

    def records(self) -> list[dict]:
        """The JSON log records written so far (the non-JSON lines are stderr)."""
        out = []
        for line in self.log().splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if isinstance(rec, dict):
                out.append(rec)
        return out

    def wait_for_records(self, pred, count: int = 1, timeout: float = 5.0) -> list[dict]:
        """The log records matching `pred`, once at least `count` of them are written (the
        log writer is non-blocking)."""
        deadline = time.time() + timeout
        while True:
            found = [r for r in self.records() if pred(r)]
            if len(found) >= count or time.time() > deadline:
                return found
            time.sleep(0.1)

    def stop(self) -> str:
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                self.proc.wait(timeout=5)
        out = self.log()
        for p in (self.script_path, self.log_path):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass
        return out


def serve(script: str, path: str, **kw):
    """A started `Server` as a context manager; the log is shown if the body fails."""

    class _Ctx:
        def __enter__(self):
            self.srv = Server(script, path, **kw)
            return self.srv

        def __exit__(self, exc_type, exc, tb):
            out = self.srv.stop()
            if exc is not None:
                print(out[-6000:])
            return False

    return _Ctx()


RUN = """
import os
app.run(host="127.0.0.1", port=int(os.environ["RA_PORT"]), mode=os.environ["RA_MODE"],
        workers=int(os.environ["RA_WORKERS"]))
"""


def _fields(rec: dict) -> dict:
    return rec.get("fields", rec)


def _record_text(rec: dict) -> str:
    return json.dumps(rec)


# ---------------------------------------------------------------------------
# R1 + M4: a Rust panic on any dispatch path is logged once with the request id and
# answered 500; the thread that ran it keeps serving.
# ---------------------------------------------------------------------------

HAS_FAULT_INJECTION = hasattr(pyronova.engine, "_fault_panic")
needs_fault_injection = pytest.mark.skipif(
    not HAS_FAULT_INJECTION,
    reason="needs a `maturin develop --release --features fault_injection` build",
)

PANIC_SCRIPT = """
from pyronova import Pyronova
from pyronova.engine import _fault_panic
app = Pyronova()

@app.get("/panic")
def panic(req):
    _fault_panic("ra-panic " + req.query_params.get("n", ""))

@app.get("/ok")
def ok(req):
    return "fine"

@app.get("/gil-panic", gil=True)
def gil_panic(req):
    _fault_panic("ra-panic " + req.query_params.get("n", ""))

@app.get("/gil-ok", gil=True)
def gil_ok(req):
    return "fine"
""" + RUN

BRIDGE_WORKERS = 2

# (serving path, panicking route, healthy route after it)
PANIC_CASES = [
    ("tpc", "/gil-panic", "/gil-ok"),  # the main-interp bridge (R1)
    ("tpc", "/panic", "/ok"),  # TPC inline
    ("pool", "/panic", "/ok"),  # pool sync worker
    ("pool", "/gil-panic", "/gil-ok"),  # pool's main-interpreter dispatch
    ("gil", "/panic", "/ok"),  # GIL mode
]


def _assert_panic_logged_once(srv: Server, payload: str, request_id: str) -> None:
    found = srv.wait_for_records(lambda r: payload in _record_text(r))
    assert len(found) == 1, f"{payload!r} logged {len(found)} times: {found}"
    fields = _fields(found[0])
    assert fields.get("request_id") == request_id, found[0]
    assert found[0].get("level") == "ERROR", found[0]


@needs_fault_injection
@pytest.mark.parametrize("path,route,healthy", PANIC_CASES)
def test_panic_is_a_logged_500_and_the_thread_survives(path, route, healthy):
    # One worker / TPC thread (and BRIDGE_WORKERS bridge threads): the N+1 panics all land
    # on the threads that must survive them.
    env = {"PYRONOVA_GIL_BRIDGE_WORKERS": str(BRIDGE_WORKERS)}
    with serve(PANIC_SCRIPT, path, workers=1, env=env) as srv:
        for n in range(BRIDGE_WORKERS + 1):
            r = srv.get(f"{route}?n={n}")
            assert r.status == 500, (n, r.status, r.body)
            body = r.json()
            assert body["error"] == GENERIC_500
            _assert_panic_logged_once(srv, f"ra-panic {n}", body["request_id"])
        r = srv.get(healthy)
        assert (r.status, r.body) == (200, b"fine")
