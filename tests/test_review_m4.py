"""Review cced8c2, milestone M4: typed errors, the real text preserved (decision D4).

A failure is carried as a typed error holding the raw exception text and traceback,
logged once where it happens with the request's id, and rendered once at the edge:

- a 5xx body is generic plus that request id: ``{"error": "Internal Server Error",
  "request_id": "..."}``; the exception text never reaches the client;
- a 4xx body carries the reason;
- the log line that holds the exception holds the same request id.

rpc, health and mcp follow the same policy. Every test here failed on d5fece4 (see
docs/design/code-review-cced8c2-roadmap.md, M4), except the cases marked as regression
coverage.

Serving paths, as `app.run` picks them:
  - "gil"   mode="gil", PYRONOVA_TPC=0          → handlers on main
  - "tpc"   mode="subinterp"                    → TPC inline sub-interpreters (+ bridge)
  - "pool"  mode="subinterp", PYRONOVA_TPC=0    → sub-interp pool (sync + async workers)
"""

from __future__ import annotations

import http.client
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time

import pytest

from tests._helpers import listening_ports, read_file

from pyronova import SharedState
from pyronova.engine import Request

PYTHON = sys.executable
HOST = "127.0.0.1"

PATHS = {
    "gil": {"mode": "gil", "tpc": "0"},
    "tpc": {"mode": "subinterp", "tpc": None},
    "pool": {"mode": "subinterp", "tpc": "0"},
}
ALL_PATHS = list(PATHS)

GENERIC_500 = "Internal Server Error"
TRACEBACK = "Traceback (most recent call last)"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class Reply:
    def __init__(self, status: int, body: bytes, headers: list[tuple[str, str]]):
        self.status = status
        self.body = body
        self.headers = {k.lower(): v for k, v in headers}

    def json(self):
        return json.loads(self.body)


class Server:
    def __init__(self, script: str, path: str, workers: int = 2, wait: bool = True):
        self.path = path
        self.port = 0  # the bound one once the server listens
        fd, self.script_path = tempfile.mkstemp(prefix="pyronova_m4_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.log_path = self.script_path + ".log"
        env = dict(os.environ)
        env["M4_MODE"] = PATHS[path]["mode"]
        env["M4_PORT"] = "0"
        env["M4_WORKERS"] = str(workers)
        env.pop("PYRONOVA_TPC", None)
        env.pop("PYRONOVA_LOG", None)
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
        if not wait:
            return
        deadline = time.time() + 20
        while time.time() < deadline:
            ports = listening_ports(read_file(self.log_path)())
            if ports:
                try:
                    with socket.create_connection((HOST, ports[0]), timeout=0.5):
                        self.port = ports[0]
                        return
                except OSError:
                    pass
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
    ) -> Reply:
        conn = http.client.HTTPConnection(HOST, self.port, timeout=15)
        try:
            conn.request(method, target, body=body, headers=headers or {})
            r = conn.getresponse()
            return Reply(r.status, r.read(), r.getheaders())
        finally:
            conn.close()

    def log(self) -> str:
        with open(self.log_path, errors="replace") as f:
            return f.read()

    def log_lines_with(self, needle: str, timeout: float = 5.0) -> list[str]:
        """The log lines containing `needle`, once at least one has been written (the
        log writer is non-blocking)."""
        deadline = time.time() + timeout
        while True:
            lines = [line for line in self.log().splitlines() if needle in line]
            if lines or time.time() > deadline:
                return lines
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


RUN = """
import os
app.run(host="127.0.0.1", port=int(os.environ["M4_PORT"]), mode=os.environ["M4_MODE"],
        workers=int(os.environ["M4_WORKERS"]))
"""

SCRIPT = """
import logging

from pydantic import BaseModel

from pyronova import Pyronova

app = Pyronova()

@app.get("/sync-raise")
def sync_raise(req):
    raise RuntimeError("m4-secret-sync")

@app.get("/async-raise")
async def async_raise(req):
    raise RuntimeError("m4-secret-async")

@app.get("/gil-raise", gil=True)
def gil_raise(req):
    raise RuntimeError("m4-secret-gil")

@app.before_request
def before(req):
    if req.path == "/before-raise":
        raise PermissionError("m4-secret-before")
    return None

@app.after_request
def after(req, resp):
    if req.path == "/after-raise":
        raise LookupError("m4-secret-after")
    return None

@app.get("/before-raise")
def before_raise(req):
    return "handler ran"

@app.get("/after-raise")
def after_raise(req):
    return "handler ran"

@app.get("/rid")
def rid(req):
    return {"request_id": req.request_id}

@app.get("/log-exception")
def log_exception(req):
    try:
        raise KeyError("m4-secret-logged")
    except KeyError:
        logging.getLogger("m4.app").exception("m4 worker logged an exception")
    return "ok"

class Item(BaseModel):
    name: str
    qty: int

@app.post("/model", model=Item)
def model(body):
    return {"name": body.name}

# -- rpc ---------------------------------------------------------------------

@app.rpc("/rpc/echo")
def echo(data):
    return data

@app.rpc("/rpc/boom")
def rpc_boom(data):
    raise RuntimeError("m4-secret-rpc")

# -- health ------------------------------------------------------------------

app.enable_health_probes()

@app.readiness_check("db")
def db_ready():
    raise ConnectionError("m4-secret-health dsn=postgres://admin:hunter2@db")

# -- mcp ---------------------------------------------------------------------

@app.mcp.tool(description="raises")
def explode() -> str:
    raise RuntimeError("m4-secret-mcp")

@app.mcp.tool(description="adds")
def add(a: int, b: int) -> int:
    return a + b
""" + RUN


RID_SCRIPT = """
from pyronova import Pyronova

app = Pyronova()
app.enable_request_id()

@app.get("/sync-raise")
def sync_raise(req):
    raise RuntimeError("m4-secret-rid")

@app.get("/rid")
def rid(req):
    return {"request_id": req.request_id}
""" + RUN


@pytest.fixture(scope="module", params=ALL_PATHS)
def srv(request):
    s = Server(SCRIPT, request.param)
    yield s
    s.stop()


@pytest.fixture(scope="module", params=ALL_PATHS)
def rid_srv(request):
    s = Server(RID_SCRIPT, request.param)
    yield s
    s.stop()


def _assert_generic_500(r: Reply, secret: str) -> str:
    """The 5xx body is the generic shape; returns its request id."""
    assert r.status == 500, (r.status, r.body)
    assert secret.encode() not in r.body, r.body
    body = r.json()
    rid = body.get("request_id")
    assert isinstance(rid, str) and rid, body
    assert body == {"error": GENERIC_500, "request_id": rid}, body
    return rid


def _assert_logged_once(srv: Server, secret: str, rid: str) -> None:
    """One log line carries the exception: its text, its traceback and the request id.
    No other line mentions it (no second logger, no raw traceback on stderr)."""
    lines = srv.log_lines_with(secret)
    assert len(lines) == 1, "\n".join(lines) or srv.log()[-3000:]
    line = lines[0]
    assert rid in line, line
    assert TRACEBACK in line, line
    assert "--- Logging error ---" not in srv.log()


# ---------------------------------------------------------------------------
# 1 + 6. Handler and hook errors: generic 500 + request id; the real error in the log
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "route,secret",
    [
        ("/sync-raise", "m4-secret-sync"),
        ("/async-raise", "m4-secret-async"),
        ("/gil-raise", "m4-secret-gil"),
        ("/before-raise", "m4-secret-before"),
        ("/after-raise", "m4-secret-after"),
    ],
)
def test_error_is_generic_500_and_the_real_text_is_logged_once(srv, route, secret):
    r = srv.request("GET", route)
    rid = _assert_generic_500(r, secret)
    assert b"handler ran" not in r.body
    _assert_logged_once(srv, secret, rid)


def test_request_ids_are_distinct_per_request(srv):
    a = _assert_generic_500(srv.request("GET", "/sync-raise"), "m4-secret-sync")
    b = _assert_generic_500(srv.request("GET", "/sync-raise"), "m4-secret-sync")
    assert a != b


def test_request_id_is_readable_by_the_handler(srv):
    r = srv.request("GET", "/rid")
    assert r.status == 200
    rid = r.json()["request_id"]
    assert isinstance(rid, str) and rid


# ---------------------------------------------------------------------------
# D4 request id: one writer. `enable_request_id` reuses it, the client's id wins.
# ---------------------------------------------------------------------------


def test_client_request_id_is_the_id_of_the_500(rid_srv):
    r = rid_srv.request("GET", "/sync-raise", headers={"X-Request-ID": "client-abc-123"})
    rid = _assert_generic_500(r, "m4-secret-rid")
    assert rid == "client-abc-123"
    lines = rid_srv.log_lines_with("m4-secret-rid")
    assert any("client-abc-123" in line for line in lines), lines


def test_minted_request_id_is_the_same_everywhere(rid_srv):
    # The id the handler reads, the one echoed in X-Request-ID, and the one a 500
    # reports come from the same single writer.
    r = rid_srv.request("GET", "/rid")
    assert r.status == 200
    assert r.headers["x-request-id"] == r.json()["request_id"]
    r = rid_srv.request("GET", "/rid", headers={"X-Request-ID": "given-1"})
    assert r.headers["x-request-id"] == "given-1"
    assert r.json()["request_id"] == "given-1"


# ---------------------------------------------------------------------------
# 4xx carry the reason
# ---------------------------------------------------------------------------


def test_rpc_malformed_body_is_a_400_with_the_reason(srv):
    r = srv.request(
        "POST", "/rpc/echo", body=b"{not json", headers={"Content-Type": "application/json"}
    )
    assert r.status == 400, (r.status, r.body)
    body = r.json()
    assert body["ok"] is False
    # The decoder's reason, with where the body stops being JSON.
    assert "line 1 column 2" in body["error"], body


def test_validation_error_is_a_422_with_the_detail(srv):
    # Regression coverage: model= validation already answered 422 with pydantic's detail.
    r = srv.request(
        "POST", "/model", body=b'{"name": "x"}', headers={"Content-Type": "application/json"}
    )
    assert r.status == 422
    assert r.json()["detail"][0]["loc"] == ["qty"]


def test_not_found_is_a_404_with_the_reason(srv):
    # Regression coverage.
    r = srv.request("GET", "/nope")
    assert r.status == 404
    assert r.json() == {"error": "not found"}


# ---------------------------------------------------------------------------
# rpc / health / mcp: the same policy (M1a item 9)
# ---------------------------------------------------------------------------


def test_rpc_handler_error_is_generic_with_request_id(srv):
    r = srv.request("POST", "/rpc/boom", body=b"{}", headers={"Content-Type": "application/json"})
    assert r.status == 500, (r.status, r.body)
    assert b"m4-secret-rpc" not in r.body and b"RuntimeError" not in r.body
    body = r.json()
    rid = body["request_id"]
    assert body == {"ok": False, "error": GENERIC_500, "request_id": rid}
    _assert_logged_once(srv, "m4-secret-rpc", rid)


def test_readyz_does_not_leak_the_check_exception(srv):
    r = srv.request("GET", "/readyz")
    assert r.status == 503
    assert b"m4-secret-health" not in r.body and b"hunter2" not in r.body
    body = r.json()
    rid = body["request_id"]
    assert body == {"status": "not_ready", "checks": {"db": {"ok": False}}, "request_id": rid}
    _assert_logged_once(srv, "m4-secret-health", rid)


def _mcp(srv: Server, payload: dict) -> dict:
    r = srv.request(
        "POST", "/mcp", body=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    assert r.status == 200, (r.status, r.body)
    assert b"m4-secret-mcp" not in r.body
    return r.json()


def test_mcp_internal_error_is_generic_with_request_id(srv):
    resp = _mcp(srv, {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                      "params": {"name": "explode", "arguments": {}}})
    err = resp["error"]
    rid = err["data"]["request_id"]
    assert err == {"code": -32603, "message": "Internal error", "data": {"request_id": rid}}
    _assert_logged_once(srv, "m4-secret-mcp", rid)


def test_mcp_invalid_params_keep_the_reason(srv):
    # Regression coverage: -32602 carries its message.
    resp = _mcp(srv, {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                      "params": {"name": "add", "arguments": {"a": 1}}})
    assert resp["error"]["code"] == -32602
    assert "missing required argument(s): ['b']" in resp["error"]["message"]


def test_logged_exception_keeps_its_traceback(srv):
    # `logger.exception(...)` in any interpreter: the Python→Rust logging handler called
    # a `formatException` it doesn't have, so the traceback was replaced by a
    # "--- Logging error ---" dump on stderr.
    assert srv.request("GET", "/log-exception").status == 200
    lines = srv.log_lines_with("m4 worker logged an exception")
    assert len(lines) == 1, lines or srv.log()[-3000:]
    assert "m4-secret-logged" in lines[0] and TRACEBACK in lines[0], lines[0]
    assert "--- Logging error ---" not in srv.log()


@pytest.mark.skipif(
    os.environ.get("PYRONOVA_TEST_PG_DSN") is None,
    reason="PYRONOVA_TEST_PG_DSN not set — skipping the Postgres CRUD case",
)
def test_crud_database_failure_is_generic_with_request_id(caplog):
    # The CRUD helper's 500 said "database error" with no id to find the log line by.
    from pyronova import Pyronova
    from pyronova.crud import register_crud
    from pyronova.db import PgPool
    from pyronova.testing import TestClient

    table = "pyronova_m4_gone"
    pool = PgPool.connect(os.environ["PYRONOVA_TEST_PG_DSN"])
    pool.execute(f"DROP TABLE IF EXISTS {table}")
    app = Pyronova()
    register_crud(app, pool, prefix="/gone", table=table, columns=["id", "name"])
    # Main interpreter: the app is built here, the pool is this process's,
    # and caplog only sees main-interpreter records.
    with TestClient(app, port=None, mode="gil") as c:
        r = c.get("/gone")
    assert r.status_code == 500
    assert b"does not exist" not in r.body
    body = r.json()
    rid = body["request_id"]
    assert body == {"error": GENERIC_500, "request_id": rid}
    logged = [rec for rec in caplog.records if rid in rec.getMessage()]
    assert len(logged) == 1, [rec.getMessage() for rec in caplog.records]
    assert "does not exist" in str(logged[0].exc_info[1])


# ---------------------------------------------------------------------------
# 2. Worker start failures carry the real error
# ---------------------------------------------------------------------------


def _start_failure(script: str, path: str) -> str:
    """Runs a script whose `app.run` must fail; returns its output."""
    s = Server(textwrap.dedent(script) + RUN, path, wait=False)
    try:
        s.proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass
    out = s.stop()
    assert s.proc.returncode not in (None, 0), out[-3000:]
    return out


def test_worker_script_error_keeps_its_text():
    # The text of an exception message that is not valid UTF-8 (a lone surrogate) was
    # replaced by "" ("failed to extract string").
    script = """
    from pyronova import Pyronova
    app = Pyronova()

    @app.get("/")
    def index(req):
        return "ok"

    if __name__ == "__pyronova_worker__":
        raise RuntimeError("boot \\ud800 failed m4")
    """
    out = _start_failure(script, "pool")
    assert re.search(r"raised while the worker started: RuntimeError: boot .*failed m4", out), (
        out[-3000:]
    )


def test_worker_asyncio_setup_failure_is_a_startup_error():
    # The worker's event-loop setup cleared its error and served on; an async handler
    # then failed with "async handler used but asyncio event loop not available".
    script = """
    import sys
    from pyronova import Pyronova
    app = Pyronova()

    @app.get("/")
    def index(req):
        return "ok"

    if __name__ == "__pyronova_worker__":
        sys.modules["asyncio"] = None
    """
    out = _start_failure(script, "pool")
    assert "import of asyncio halted" in out, out[-3000:]


def test_missing_isojson_fails_at_startup():
    # isojson is a hard dependency: without it the server must not start and answer 500
    # to every dict.
    script = """
    import sys
    from pyronova import Pyronova
    sys.modules["isojson"] = None
    app = Pyronova()

    @app.get("/")
    def index(req):
        return {"a": 1}
    """
    out = _start_failure(script, "gil")
    assert "import of isojson halted" in out, out[-3000:]


# ---------------------------------------------------------------------------
# 3. Sentinels
# ---------------------------------------------------------------------------


def test_unparseable_client_ip_is_an_error_not_0000():
    with pytest.raises(ValueError, match="not-an-ip"):
        Request("GET", "/", {}, "", b"", {}, "not-an-ip")
    assert Request("GET", "/", {}, "", b"", {}, "10.1.2.3").client_ip == "10.1.2.3"


def test_python_built_request_has_a_request_id():
    a = Request("GET", "/", {}, "", b"", {}, "10.1.2.3")
    b = Request("GET", "/", {}, "", b"", {}, "10.1.2.3")
    assert a.request_id and a.request_id == a.request_id
    assert a.request_id != b.request_id


def test_rejected_route_names_the_route():
    # 5. The route table's error is typed (method, path, matchit's error), not a string
    #    that lost which route it was about.
    from pyronova import Pyronova

    app = Pyronova()

    @app.get("/dup")
    def first(req):
        return "a"

    with pytest.raises(ValueError, match="GET /dup: .*conflict"):
        @app.get("/dup")
        def second(req):
            return "b"


def test_state_non_utf8_value_raises_on_every_read():
    s = SharedState()
    s.set_bytes("blob", b"\xff\xfe")
    s["text"] = "fine"
    assert "blob" in s
    with pytest.raises(TypeError, match="blob"):
        s["blob"]
    with pytest.raises(TypeError, match="blob"):
        s.get("blob")
    with pytest.raises(TypeError, match="blob"):
        s.values()
    with pytest.raises(TypeError, match="blob"):
        s.items()
    assert s.get("missing", "d") == "d"
    assert s.get_bytes("blob") == b"\xff\xfe"
    del s["blob"]
    assert s.values() == ["fine"]
    assert s.items() == [("text", "fine")]
