"""Review cced8c2, milestone M2: one request pipeline.

GIL mode, the sub-interpreter pool and TPC each carried their own preprocessing and
response finishing, and had drifted apart. Every test here failed on 333fb74 (see
docs/design/code-review-cced8c2-roadmap.md, M2 and "Carried into M2 from M1"), except
the paths marked as regression coverage, which already behaved.

Serving paths, as `app.run` picks them:
  - "gil"       mode="gil", PYRONOVA_TPC=0  → multi-thread runtime, handlers on main
  - "tpc-gil"   mode="gil"                  → TPC threads, handlers on main
  - "tpc"       mode="subinterp"            → TPC inline (sub-interp) + main bridge (gil=True)
  - "pool"      mode="subinterp", PYRONOVA_TPC=0 → sub-interp pool + main (gil=True)
"""

from __future__ import annotations

import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import urllib.error
import urllib.request

import pytest

PYTHON = sys.executable
HOST = "127.0.0.1"

# The server-side request budget (src/handlers.rs `REQUEST_BUDGET`).
REQUEST_BUDGET_S = 30

PATHS = {
    "gil": {"mode": "gil", "tpc": "0"},
    "tpc-gil": {"mode": "gil", "tpc": None},
    "tpc": {"mode": "subinterp", "tpc": None},
    "pool": {"mode": "subinterp", "tpc": "0"},
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


class Server:
    """A Pyronova server running `script` on one serving path. `app.run` in the script
    reads `mode` and `port` from `M2_MODE` / `M2_PORT`."""

    def __init__(self, script: str, path: str, workers: int = 2):
        self.port = _free_port()
        fd, self.script_path = tempfile.mkstemp(prefix="pyronova_m2_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.log_path = self.script_path + ".log"
        env = dict(os.environ)
        env["M2_MODE"] = PATHS[path]["mode"]
        env["M2_PORT"] = str(self.port)
        env["M2_WORKERS"] = str(workers)
        env.pop("PYRONOVA_TPC", None)
        if PATHS[path]["tpc"] is not None:
            env["PYRONOVA_TPC"] = PATHS[path]["tpc"]
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [PYTHON, self.script_path],
                stdout=log,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid,
                env=env,
            )
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                with socket.create_connection((HOST, self.port), timeout=0.5):
                    return
            except OSError:
                if self.proc.poll() is not None:
                    break
                time.sleep(0.1)
        raise RuntimeError(f"server ({path}) did not start:\n{self.stop()}")

    @property
    def base(self) -> str:
        return f"http://{HOST}:{self.port}"

    def request(
        self,
        path: str,
        method: str = "GET",
        body: bytes | None = None,
        headers: dict | None = None,
        timeout: float = 10,
    ) -> tuple[int, bytes, dict]:
        req = urllib.request.Request(
            self.base + path, data=body, method=method, headers=headers or {}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read(), {k.lower(): v for k, v in r.headers.items()}
        except urllib.error.HTTPError as e:
            return e.code, e.read(), {k.lower(): v for k, v in e.headers.items()}

    def stop(self) -> str:
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                self.proc.wait(timeout=5)
        with open(self.log_path, errors="replace") as f:
            out = f.read()
        for p in (self.script_path, self.log_path):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass
        return out


RUN = """
import os
app.run(host="127.0.0.1", port=int(os.environ["M2_PORT"]), mode=os.environ["M2_MODE"],
        workers=int(os.environ["M2_WORKERS"]))
"""


def _access_lines(log: str) -> list[str]:
    return [line for line in log.splitlines() if "pyronova::access" in line]


# ---------------------------------------------------------------------------
# Fallback on TPC (TPC ignored app.fallback())
# ---------------------------------------------------------------------------

FALLBACK_SCRIPT = """
from pyronova import Pyronova
app = Pyronova()

@app.get("/")
def index(req):
    return "index"

@app.fallback
def fallback(req):
    return f"fallback:{req.method}:{req.path}"
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_fallback_serves_unmatched_paths(path):
    srv = Server(FALLBACK_SCRIPT, path)
    try:
        status, body, _ = srv.request("/nope/deeper")
        assert (status, body) == (200, b"fallback:GET:/nope/deeper")
        assert srv.request("/")[:2] == (200, b"index")
    finally:
        srv.stop()


# ---------------------------------------------------------------------------
# TPC body read has a timeout (TPC collected the body with no deadline)
# ---------------------------------------------------------------------------

BODY_SCRIPT = """
from pyronova import Pyronova
app = Pyronova()

@app.post("/echo")
def echo(req):
    return req.body
""" + RUN


def _stalled_post(port: int, wait: float) -> tuple[bytes, float]:
    """POST a body that declares 1000 bytes, send 10, stall. Returns what the server
    answered (b"" if it closed without a response) and how long that took."""
    start = time.time()
    with socket.create_connection((HOST, port), timeout=wait) as s:
        s.sendall(
            b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n0123456789"
        )
        try:
            data = s.recv(4096)
        except socket.timeout:
            data = None
    return data, time.time() - start


ADMISSION_SCRIPT = """
from pyronova import Pyronova
from pyronova.engine import get_gil_metrics
app = Pyronova()
app.enable_cors(allow_origins="https://app.example")

@app.post("/upload")
def upload(req):
    return str(len(req.body))

@app.get("/dropped", gil=True)
def dropped(req):
    return {"dropped": get_gil_metrics().dropped_requests}
""" + RUN


def test_pool_admission_rejects_large_bodies_past_the_permit_budget():
    """Regression coverage (behavioural) for the pool's admission gate: a large body takes
    a permit before a byte of it is read; with every permit held, the next one is a 503
    that carries CORS and is counted as dropped. One sync worker → 128 permits."""
    srv = Server(ADMISSION_SCRIPT, "pool", workers=1)
    held = []
    try:
        for _ in range(128):
            s = socket.create_connection((HOST, srv.port), timeout=10)
            s.sendall(b"POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n")
            held.append(s)
        time.sleep(0.5)
        status, body, headers = srv.request(
            "/upload", method="POST", body=b"x" * 100000
        )
        assert status == 503, body
        assert headers.get("access-control-allow-origin") == "https://app.example"
        status, body, _ = srv.request("/dropped")
        assert json.loads(body)["dropped"] >= 1
    finally:
        for s in held:
            s.close()
        srv.stop()


@pytest.mark.parametrize("path", ["tpc", "gil", "pool"])
def test_slow_body_times_out_with_408(path):
    srv = Server(BODY_SCRIPT, path)
    try:
        data, took = _stalled_post(srv.port, REQUEST_BUDGET_S + 10)
        assert data is not None, f"no answer within {REQUEST_BUDGET_S + 10}s: the body read has no deadline"
        assert data.startswith(b"HTTP/1.1 408"), data[:200]
        assert took >= REQUEST_BUDGET_S - 1
    finally:
        srv.stop()


# ---------------------------------------------------------------------------
# Handler timeout on every path (GIL and TPC inline had none) + DROPPED_REQUESTS
# ---------------------------------------------------------------------------

SLOW_SCRIPT = """
import time
from pyronova import Pyronova
from pyronova.engine import get_gil_metrics
app = Pyronova()

@app.get("/slow")
def slow(req):
    time.sleep(REQUEST_BUDGET + 5)
    return "late"

@app.get("/slow-gil", gil=True)
def slow_gil(req):
    time.sleep(REQUEST_BUDGET + 5)
    return "late"

@app.get("/dropped", gil=True)
def dropped(req):
    return {"dropped": get_gil_metrics().dropped_requests}
""".replace("REQUEST_BUDGET", str(REQUEST_BUDGET_S)) + RUN

# (serving path, route): every way a handler call is dispatched.
SLOW_CASES = [
    ("gil", "/slow"),          # multi-thread runtime, main interpreter
    ("tpc-gil", "/slow"),      # TPC threads, main interpreter
    ("tpc", "/slow"),          # TPC inline sub-interpreter
    ("tpc", "/slow-gil"),      # TPC → main-interp bridge (regression coverage)
    ("pool", "/slow"),         # sub-interp pool (regression coverage)
    ("pool", "/slow-gil"),     # pool mode, gil=True route on main (regression coverage)
]


def test_handler_timeout_is_504_on_every_path():
    # One server per case: a slow inline handler blocks its TPC thread, and on macOS one
    # listener takes every connection, so two cases must not share a TPC server.
    servers = {case: Server(SLOW_SCRIPT, case[0], workers=2) for case in SLOW_CASES}
    results: dict = {}

    def hit(case):
        path, route = case
        start = time.time()
        try:
            status, _, _ = servers[case].request(route, timeout=REQUEST_BUDGET_S + 20)
        except Exception as e:  # noqa: BLE001 — reported below
            status = repr(e)
        results[case] = (status, time.time() - start)

    try:
        threads = [threading.Thread(target=hit, args=(c,)) for c in SLOW_CASES]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wrong = {c: r for c, r in results.items() if r[0] != 504}
        assert not wrong, f"expected 504 on every path, got {wrong}"

        # A request the server gave up on is counted as dropped, on every path.
        for case, srv in servers.items():
            status, body, _ = srv.request("/dropped")
            assert status == 200
            assert json.loads(body)["dropped"] >= 1, f"{case}: DROPPED_REQUESTS not counted"
    finally:
        for srv in servers.values():
            srv.stop()


# ---------------------------------------------------------------------------
# One access-log line for every response (404 and fast-path wrote none; the pool
# wrote two for a handled request)
# ---------------------------------------------------------------------------

LOG_SCRIPT = """
from pyronova import Pyronova
app = Pyronova()
app.enable_logging()
app.add_fast_response("GET", "/fast", b"fast")

@app.get("/handled")
def handled(req):
    return "ok"
""" + RUN


@pytest.mark.parametrize("path", ["gil", "tpc-gil", "tpc", "pool"])
def test_every_response_writes_one_access_line(path):
    srv = Server(LOG_SCRIPT, path)
    try:
        assert srv.request("/nope-404")[0] == 404
        assert srv.request("/fast")[0] == 200
        assert srv.request("/handled")[0] == 200
        time.sleep(0.5)
    finally:
        log = srv.stop()
    lines = _access_lines(log)
    for route, status in (("/nope-404", "404"), ("/fast", "200"), ("/handled", "200")):
        mine = [line for line in lines if route in line]
        assert len(mine) == 1, f"{path} {route}: expected one access line, got {mine}\n{log[-3000:]}"
        assert status in mine[0], mine[0]


# ---------------------------------------------------------------------------
# Per-request contextvars.Context (ctx leaked into the next request on the thread)
# ---------------------------------------------------------------------------

CTX_SCRIPT = """
from pyronova import Pyronova, Response
from pyronova.context import ctx
app = Pyronova()

@app.before_request
def tag(req):
    ctx.set("hook", req.path)

@app.after_request
def echo_hook(req, resp):
    headers = dict(resp.headers)
    headers["x-ctx-hook"] = str(ctx.get("hook"))
    return Response(resp.body, status_code=resp.status_code,
                    content_type=resp.content_type, headers=headers)

def probe(req):
    seen = {"leaked": ctx.get("leak"), "hook": ctx.get("hook")}
    if req.query_params.get("set"):
        ctx.set("leak", "from-an-earlier-request")
    return seen

app.get("/ctx")(probe)
app.get("/ctx-gil", gil=True)(probe)
""" + RUN

CTX_CASES = [("gil", "/ctx"), ("tpc", "/ctx"), ("tpc", "/ctx-gil"), ("pool", "/ctx"), ("pool", "/ctx-gil")]


@pytest.mark.parametrize("path,route", CTX_CASES)
def test_ctx_does_not_leak_into_the_next_request(path, route):
    # One worker / TPC thread, so consecutive requests run on the same thread.
    srv = Server(CTX_SCRIPT, path, workers=1)
    try:
        assert srv.request(route + "?set=1")[0] == 200
        for _ in range(10):
            status, body, headers = srv.request(route)
            assert status == 200
            seen = json.loads(body)
            assert seen["leaked"] is None, f"{path} {route}: ctx leaked: {seen}"
            # Hooks and handler share the request's context.
            assert seen["hook"] == route
            assert headers["x-ctx-hook"] == route
    finally:
        srv.stop()


# ---------------------------------------------------------------------------
# D3: ws.request, and before_request runs before the 101
# ---------------------------------------------------------------------------

WS_SCRIPT = """
from pyronova import Pyronova, Response
app = Pyronova()
app.enable_cors(allow_origins="https://app.example")

@app.before_request
def guard(req):
    if req.path == "/ws" and req.headers.get("x-token") != "secret":
        return Response("denied", status_code=403)

@app.websocket("/ws")
def ws_handler(ws):
    r = ws.request
    ws.send(f"{r.method} {r.path} {r.query_params.get('q')} {r.headers.get('x-token')}")
    ws.close()
""" + RUN


def _ws_connect(port: int, token: str | None):
    from websockets.sync.client import connect

    headers = {"x-token": token} if token is not None else {}
    return connect(f"ws://{HOST}:{port}/ws?q=1", additional_headers=headers, open_timeout=10)


@pytest.mark.parametrize("path", ["gil", "tpc"])
def test_ws_request_carries_the_upgrade_request(path):
    srv = Server(WS_SCRIPT, path)
    try:
        with _ws_connect(srv.port, "secret") as ws:
            assert ws.recv(timeout=10) == "GET /ws 1 secret"
    finally:
        srv.stop()


@pytest.mark.parametrize("path", ["gil", "tpc"])
def test_before_request_rejects_ws_upgrade(path):
    from websockets.exceptions import InvalidStatus

    srv = Server(WS_SCRIPT, path)
    try:
        with pytest.raises(InvalidStatus) as exc:
            _ws_connect(srv.port, None)
        resp = exc.value.response
        assert resp.status_code == 403
        assert resp.body == b"denied"
        # The rejection is a normal response: CORS applies.
        assert resp.headers.get("access-control-allow-origin") == "https://app.example"
    finally:
        srv.stop()


# ---------------------------------------------------------------------------
# Invalid status: a fast response can't be registered with one; a handler's is a
# logged 500 on every path (TPC and the pool mapped it without a log line)
# ---------------------------------------------------------------------------


def test_fast_response_rejects_invalid_status_and_headers_at_registration():
    out = subprocess.run(
        [
            PYTHON,
            "-c",
            textwrap.dedent(
                """
                from pyronova import Pyronova
                app = Pyronova()
                for kwargs in ({"status_code": 1000}, {"status_code": 42},
                               {"headers": {"x-bad": "line\\nbreak"}},
                               {"headers": {"bad header": "v"}},
                               {"content_type": "text/\\x00plain"}):
                    try:
                        app.add_fast_response("GET", "/bad", b"x", **kwargs)
                    except ValueError as e:
                        print("rejected", e)
                    else:
                        print("accepted", kwargs)
                """
            ),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert out.returncode == 0, out.stderr
    lines = out.stdout.splitlines()
    assert len(lines) == 5, out.stdout
    assert all(line.startswith("rejected") for line in lines), out.stdout


STATUS_SCRIPT = """
from pyronova import Pyronova, Response
app = Pyronova()

@app.get("/bad-status")
def bad_status(req):
    return Response("x", status_code=1000)

@app.get("/bad-status-gil", gil=True)
def bad_status_gil(req):
    return Response("x", status_code=1000)
""" + RUN


@pytest.mark.parametrize(
    "path,route",
    [("gil", "/bad-status"), ("tpc", "/bad-status"), ("tpc", "/bad-status-gil"), ("pool", "/bad-status")],
)
def test_invalid_handler_status_is_a_logged_500(path, route):
    srv = Server(STATUS_SCRIPT, path)
    try:
        assert srv.request(route)[0] == 500
        time.sleep(0.3)
    finally:
        log = srv.stop()
    assert re.search(r"invalid HTTP status.*1000|1000.*invalid HTTP status", log), log[-3000:]
