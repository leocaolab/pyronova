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
from typing import Literal, Optional

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


# ---------------------------------------------------------------------------
# R4: a ContextVar written by an `async def` hook or handler is seen by the rest of the
# request on every path (the awaitable used to run in a copy of the request's context).
# ---------------------------------------------------------------------------

CTX_SCRIPT = """
from pyronova import Pyronova, Response
from pyronova.context import ctx
app = Pyronova()

@app.before_request
async def tag(req):
    ctx.set("user", "u" + req.path)

@app.after_request
def expose(req, resp):
    headers = dict(resp.headers)
    headers["x-handler"] = str(ctx.get("handler"))
    headers["x-user"] = str(ctx.get("user"))
    return Response(resp.body, status_code=resp.status_code,
                    content_type=resp.content_type, headers=headers)

def sync_probe(req):
    return {"user": ctx.get("user")}

async def async_probe(req):
    ctx.set("handler", "h" + req.path)
    return {"user": ctx.get("user")}

app.get("/sync")(sync_probe)
app.get("/sync-gil", gil=True)(sync_probe)
app.get("/async")(async_probe)
app.get("/async-gil", gil=True)(async_probe)
""" + RUN

# TPC: inline sub-interpreter (def and async def) and the bridge (gil=True); pool: sync
# worker, async engine and main-interpreter dispatch; GIL mode: every route on main.
CTX_ROUTES = ["/sync", "/async", "/sync-gil", "/async-gil"]


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_async_hook_and_handler_ctx_writes_reach_the_rest_of_the_request(path):
    with serve(CTX_SCRIPT, path, workers=2) as srv:
        for route in CTX_ROUTES:
            # Twice: the second request runs on a thread that already served one.
            for _ in range(2):
                r = srv.get(route)
                assert r.status == 200, (route, r.status, r.body)
                # async before_request → sync or async handler
                assert r.json() == {"user": "u" + route}, (path, route, r.body)
                # before-hook → after-hook
                assert r.headers["x-user"] == "u" + route, (path, route, r.headers)
                # async handler → after-hook
                expected = "h" + route if route.startswith("/async") else "None"
                assert r.headers["x-handler"] == expected, (path, route, r.headers)


# ---------------------------------------------------------------------------
# R3 / Q2: route templates take only `{name}` / `{*name}`
# ---------------------------------------------------------------------------


def test_colon_param_is_refused_whatever_the_handler_takes():
    from pyronova import Pyronova
    from pyronova.engine import PyronovaApp

    app = Pyronova()
    # A handler that takes only the request gets no injection, but the route would still
    # never match: refused all the same.
    with pytest.raises(ValueError, match=r"write it as `\{id\}`"):
        app.get("/users/:id", lambda req: "x")
    # The engine's own registration refuses it too.
    with pytest.raises(ValueError, match=r"GET /users/:id: .*write it as `\{id\}`"):
        PyronovaApp().get("/users/:id", lambda req: "x")
    # A colon inside a segment is literal text.
    app.get("/v1/items:batchGet", lambda req: "x")


def test_route_params_is_the_one_template_parser():
    from pyronova.engine import _route_params

    assert _route_params("/a/{x}/img{y}.png/{*rest}") == ["x", "y", "rest"]
    assert _route_params("/{{literal}}/{z}") == ["z"]


TEMPLATE_SCRIPT = """
from pyronova import Pyronova
app = Pyronova()

@app.get("/items/{item_id}")
def item(req, item_id):
    return {"item_id": item_id}

@app.get("/files/{*rest}")
def files(req, rest):
    return {"rest": rest}

@app.get("/gil-files/{*rest}", gil=True)
async def gil_files(req, rest):
    return {"rest": rest}
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_brace_params_and_catch_all_are_injected(path):
    with serve(TEMPLATE_SCRIPT, path) as srv:
        r = srv.get("/items/42")
        assert (r.status, r.json()) == (200, {"item_id": "42"})
        r = srv.get("/files/a/b%20c.txt")
        assert (r.status, r.json()) == (200, {"rest": "a/b c.txt"})
        r = srv.get("/gil-files/x/y")
        assert (r.status, r.json()) == (200, {"rest": "x/y"})


# ---------------------------------------------------------------------------
# M4 gaps: isojson is required in every worker; the async engine's death at run time is
# its own error; a relative import failing in a worker says why
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


@pytest.mark.parametrize("path", ["pool", "tpc"])
def test_isojson_refused_in_a_worker_stops_the_start(path):
    # isojson imports fine on main; the script hides it only inside the workers, so the
    # start fails at the worker's own JSON check, not main's.
    script = """
    import sys
    from pyronova import Pyronova
    app = Pyronova()

    @app.get("/")
    def index(req):
        return {"a": 1}

    if __name__ == "__pyronova_worker__":
        sys.modules["isojson"] = None
    """
    out = _start_failure(script, path)
    assert "loading the JSON serializer failed" in out, out[-3000:]
    assert "import of isojson halted" in out, out[-3000:]


ENGINE_DEATH_SCRIPT = """
from pyronova import Pyronova
app = Pyronova()

@app.get("/die")
async def die(req):
    raise SystemExit("ra-engine-died")
""" + RUN


def test_async_engine_death_at_run_time_is_reported_as_such():
    srv = Server(ENGINE_DEATH_SCRIPT, "pool", workers=1)
    try:
        # The request itself is lost with the engine (M7); only the log matters here.
        with pytest.raises(OSError):
            srv.get("/die", timeout=2)
    finally:
        out = srv.stop()
    # The engine ends when its fetcher does, at shutdown; the record is written then.
    stopped = [
        line for line in out.splitlines()
        if "the async engine stopped: SystemExit: ra-engine-died" in line
    ]
    assert len(stopped) == 1, out[-4000:]
    assert "async worker stopped serving" in stopped[0]
    assert "raised while the worker started" not in out, out[-4000:]


def test_relative_import_failing_in_a_worker_says_why(tmp_path):
    # A worker executes the app's file as a module of its own, outside its package: a
    # relative import there fails, and the start error now says so.
    pkg = tmp_path / "ra_pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "models.py").write_text("GREETING = 'hi'\n")
    (pkg / "app.py").write_text(textwrap.dedent("""
        import os
        from .models import GREETING
        from pyronova import Pyronova
        app = Pyronova()

        @app.get("/")
        def index(req):
            return GREETING

        if __name__ == "__main__":
            app.run(host="127.0.0.1", port=int(os.environ["RA_PORT"]), mode="subinterp",
                    workers=1)
    """))
    env = dict(os.environ, RA_PORT=str(_free_port()), PYRONOVA_TPC="0")
    proc = subprocess.run(
        [PYTHON, "-m", "ra_pkg.app"], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=60,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode != 0, out[-3000:]
    assert "attempted relative import" in out, out[-3000:]
    assert "outside any package, so a relative import" in out, out[-3000:]


# ---------------------------------------------------------------------------
# M4 gap: a streamed body's rejection is typed, and answers what a buffered body's does
# ---------------------------------------------------------------------------

STREAM_SCRIPT = """
from pyronova import Pyronova
from pyronova.engine import BodyRejected
app = Pyronova()
app.max_body_size = 1024

@app.post("/buffered", gil=True)
def buffered(req):
    return {"bytes": len(req.body)}

@app.post("/iterate", gil=True, stream=True)
def iterate(req):
    total = 0
    for chunk in req.stream:
        total += len(chunk)
    return {"bytes": total}

@app.post("/drain", gil=True, stream=True)
def drain(req):
    return {"bytes": req.stream.drain_count()}

@app.post("/catch", gil=True, stream=True)
def catch(req):
    try:
        req.stream.read()
    except OSError as e:
        return {"caught": type(e).__name__, "is_body_rejected": isinstance(e, BodyRejected),
                "text": str(e)}
    return "read it all"
""" + RUN

TOO_LARGE = {"error": "payload too large"}


def _no_error_records(srv: Server) -> None:
    errors = [r for r in srv.records() if r.get("level") == "ERROR"]
    assert errors == [], errors


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_oversized_streamed_body_is_413_like_a_buffered_one(path):
    with serve(STREAM_SCRIPT, path) as srv:
        body = b"x" * 4096
        buffered = srv.post("/buffered", body)
        assert (buffered.status, buffered.json()) == (413, TOO_LARGE)
        for route in ("/iterate", "/drain"):
            r = srv.post(route, body)
            assert (r.status, r.json()) == (buffered.status, buffered.json()), route
        # A handler can still catch it, as the OSError it always was.
        r = srv.post("/catch", body)
        assert r.status == 200
        assert r.json() == {
            "caught": "BodyRejected",
            "is_body_rejected": True,
            "text": "request body is larger than max_body_size",
        }
        # Within the cap, streaming is unchanged.
        assert srv.post("/iterate", b"y" * 100).json() == {"bytes": 100}
        time.sleep(0.5)  # the log writer is non-blocking
        _no_error_records(srv)


def _send_partial_body(port: int, target: str) -> tuple[int, bytes]:
    """Declares a 100-byte body, sends 10 bytes, then waits for the answer."""
    with socket.create_connection((HOST, port), timeout=60) as s:
        s.sendall(
            f"POST {target} HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n".encode()
            + b"z" * 10
        )
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        head, _, rest = data.partition(b"\r\n\r\n")
        status = int(head.split(b" ")[1])
        return status, rest


def test_slow_streamed_body_is_408_like_a_buffered_one():
    # The 30 s body budget runs once per path, all paths at the same time.
    import concurrent.futures

    servers = {path: Server(STREAM_SCRIPT, path) for path in ("tpc", "gil")}
    try:
        with concurrent.futures.ThreadPoolExecutor(len(servers) * 2) as pool:
            futures = {
                (path, route): pool.submit(_send_partial_body, srv.port, route)
                for path, srv in servers.items()
                for route in ("/buffered", "/iterate")
            }
            results = {key: f.result() for key, f in futures.items()}
        for (path, route), (status, _) in results.items():
            assert status == 408, (path, route, status)
        for srv in servers.values():
            _no_error_records(srv)
    finally:
        for srv in servers.values():
            srv.stop()


# ---------------------------------------------------------------------------
# MCP: argument values are checked against the tool schema; hints map to real schemas
# or fail registration; non-str results are JSON
# ---------------------------------------------------------------------------


def _mcp_call(server, name: str, arguments: dict) -> dict:
    body = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    })
    return json.loads(server.handle_request(body))


def test_mcp_wrong_argument_type_is_32602_with_the_reason():
    from pyronova.mcp import MCPServer

    mcp = MCPServer()

    @mcp.tool()
    def add(a: int, b: int) -> int:
        return a + b

    @mcp.tool()
    def pick(tags: list[str], mode: Literal["fast", "slow"], limit: int | None = None) -> str:
        return f"{tags} {mode} {limit}"

    resp = _mcp_call(mcp, "add", {"a": 1, "b": "2"})
    assert resp["error"]["code"] == -32602, resp
    assert resp["error"]["message"] == "invalid params: argument 'b': expected integer, got string"

    # bool is not an integer in JSON Schema terms
    resp = _mcp_call(mcp, "add", {"a": True, "b": 2})
    assert resp["error"]["code"] == -32602, resp
    assert "argument 'a': expected integer, got boolean" in resp["error"]["message"]

    resp = _mcp_call(mcp, "pick", {"tags": ["x", 3], "mode": "fast"})
    assert resp["error"]["code"] == -32602, resp
    assert "argument 'tags'[1]: expected string, got integer" in resp["error"]["message"]

    resp = _mcp_call(mcp, "pick", {"tags": [], "mode": "medium"})
    assert resp["error"]["code"] == -32602, resp
    assert "'medium' is not one of ['fast', 'slow']" in resp["error"]["message"]

    resp = _mcp_call(mcp, "pick", {"tags": ["x"], "mode": "slow", "limit": None})
    assert resp["result"]["content"][0]["text"] == "['x'] slow None"
    assert _mcp_call(mcp, "add", {"a": 2, "b": 3})["result"]["content"][0]["text"] == "5"


def test_mcp_schema_maps_common_hints():
    from pyronova.mcp import _extract_schema

    def tool(
        a: list[int],
        b: dict[str, float],
        c: int | None,
        d: Optional[str],
        e: Literal["x", "y"],
        f: bool,
        g,
        h: list,
    ):
        pass

    props = _extract_schema(tool)["properties"]
    assert props["a"] == {"type": "array", "items": {"type": "integer"}}
    assert props["b"] == {"type": "object", "additionalProperties": {"type": "number"}}
    assert props["c"] == {"anyOf": [{"type": "integer"}, {"type": "null"}]}
    assert props["d"] == {"anyOf": [{"type": "string"}, {"type": "null"}]}
    assert props["e"] == {"enum": ["x", "y"]}
    assert props["f"] == {"type": "boolean"}
    assert props["g"] == {}  # no hint: any JSON value, not "string"
    assert props["h"] == {"type": "array"}


class Point:
    """A class with no JSON form (module level: this file's annotations are strings,
    resolved against its globals)."""


def test_mcp_unmappable_hint_is_a_registration_error_naming_the_parameter():
    from pyronova.mcp import MCPServer

    mcp = MCPServer()
    with pytest.raises(TypeError, match=r"parameter 'where' is annotated .*Point.*input_schema="):
        @mcp.tool()
        def locate(name: str, where: Point) -> str:
            return name

    with pytest.raises(TypeError, match=r"parameter 'ids'"):
        @mcp.tool()
        def lookup(ids: set[int]) -> str:
            return ""

    # An explicit schema is the way out, and is what gets checked.
    @mcp.tool(input_schema={"type": "object", "properties": {"where": {"type": "object"}}})
    def locate2(where: Point) -> str:
        return "ok"

    assert _mcp_call(mcp, "locate2", {"where": [1]})["error"]["code"] == -32602


def test_mcp_non_str_results_are_json():
    from pyronova.mcp import MCPServer

    mcp = MCPServer()

    @mcp.tool()
    def items() -> list:
        return [{"id": 1, "ok": True}, None]

    @mcp.tool()
    def ratio() -> float:
        return 0.5

    @mcp.tool()
    def nothing() -> None:
        return None

    def text(name):
        return _mcp_call(mcp, name, {})["result"]["content"][0]["text"]

    assert json.loads(text("items")) == [{"id": 1, "ok": True}, None]
    assert text("items") != str([{"id": 1, "ok": True}, None])
    assert text("ratio") == "0.5"
    assert text("nothing") == "null"


# ---------------------------------------------------------------------------
# Logging: an explicit enable_logging(level=...) is the level
# ---------------------------------------------------------------------------

LEVEL_SCRIPT = """
import logging, os
from pyronova import Pyronova
app = Pyronova(debug=os.environ.get("RA_DEBUG") == "1")
app.enable_logging(level=os.environ["RA_LEVEL"])

@app.get("/log", gil=True)
def log(req):
    logging.getLogger("ra.app").info("ra-info-line")
    logging.getLogger("ra.app").warning("ra-warning-line")
    logging.getLogger("ra.app").error("ra-error-line")
    return "ok"
""" + RUN


@pytest.mark.parametrize(
    "env,shown,hidden",
    [
        # debug=True's DEBUG used to win over the explicit level
        ({"RA_DEBUG": "1", "RA_LEVEL": "warn"}, "ra-warning-line", "ra-info-line"),
        # the implicit enable_logging() at run() used to raise an explicit ERROR to INFO
        ({"PYRONOVA_LOG": "1", "RA_LEVEL": "error"}, "ra-error-line", "ra-warning-line"),
    ],
    ids=["over-debug", "over-PYRONOVA_LOG"],
)
def test_explicit_logging_level_wins(env, shown, hidden):
    with serve(LEVEL_SCRIPT, "gil", env=env) as srv:
        assert srv.get("/log").status == 200
        srv.wait_for_records(lambda r: shown in _record_text(r))
        text = srv.log()
        assert shown in text, text[-3000:]
        assert hidden not in text, text[-3000:]


def test_enable_logging_without_a_level_keeps_the_configured_one():
    from pyronova import Pyronova

    app = Pyronova(log_config={"level": "DEBUG"})
    app.enable_logging()
    assert app._log_config["level"] == "DEBUG"
    quiet = Pyronova()  # ERROR would hide the INFO access lines
    quiet.enable_logging()
    assert quiet._log_config["level"] == "INFO"
    pinned = Pyronova()
    pinned.enable_logging(level="error")
    pinned.enable_logging()  # PYRONOVA_LOG=1 / debug=True at run()
    assert pinned._log_config["level"] == "ERROR"


# ---------------------------------------------------------------------------
# Python: model= rule, metrics counters, readiness timeouts, uploads
# ---------------------------------------------------------------------------


def test_model_rule_is_checked_at_registration():
    from pydantic import BaseModel

    from pyronova import Pyronova

    class Item(BaseModel):
        name: str

    app = Pyronova()

    # The body annotated as the model, but standing where the request goes: `sku` was
    # meant as a path param the template lacks. Used to bind `item` to the request.
    def misplaced(item: Item, sku):
        return sku

    with pytest.raises(TypeError, match=r"'item' is annotated Item but stands where the request goes"):
        app.post("/items", misplaced, model=Item)

    # No parameter for the body at all: used to fail on every request instead.
    def bodiless():
        return "x"

    with pytest.raises(TypeError, match=r"no parameter for the validated Item body"):
        app.post("/things", bodiless, model=Item)

    # The two shapes the rule allows.
    app.post("/a/{item_id}", lambda req, body, item_id: None, model=Item)
    app.post("/b/{item_id}", lambda body, item_id: None, model=Item)


def test_corrupt_metrics_counter_fails_the_scrape_not_reads_as_zero():
    from pyronova.observability import CorruptMetric, _render_prometheus

    class State(dict):
        pass

    ok = _render_prometheus(State({"_m:req:total": "3"}))
    assert "pyronova_http_requests_total 3" in ok
    with pytest.raises(CorruptMetric, match=r"'_m:req:total' holds 'garbage'"):
        _render_prometheus(State({"_m:req:total": "garbage"}))


def test_sync_readiness_check_is_bounded_like_an_async_one(monkeypatch):
    import pyronova.health as health

    monkeypatch.setattr(health, "_CHECK_TIMEOUT_S", 0.5)
    started = time.monotonic()
    ok, results = health._run_checks_sync(
        [("hangs", lambda: time.sleep(5)), ("fine", lambda: True)], "rid-ra"
    )
    assert time.monotonic() - started < 3
    assert ok is False
    assert results == {"hangs": {"ok": False}, "fine": {"ok": True}}


class _MultipartRequest:
    def __init__(self, content_type: str, body: bytes):
        self.headers = {"content-type": content_type}
        self.body = body


def _form(boundary: str, disposition: str, data: bytes = b"x") -> bytes:
    return (
        f"--{boundary}\r\nContent-Disposition: {disposition}\r\n\r\n".encode()
        + data
        + f"\r\n--{boundary}--\r\n".encode()
    )


def test_upload_filename_star_is_decoded_and_wins():
    from pyronova.uploads import parse_multipart

    body = _form(
        "b1",
        "form-data; name=\"f\"; filename=\"rates.txt\"; filename*=UTF-8''%E2%82%AC%20rates.txt",
    )
    f = parse_multipart(_MultipartRequest("multipart/form-data; boundary=b1", body))["f"]
    assert f.filename == "€ rates.txt"

    body = _form("b1", "form-data; name=\"f\"; filename*=iso-8859-1'en'%A3.txt")
    f = parse_multipart(_MultipartRequest("multipart/form-data; boundary=b1", body))["f"]
    assert f.filename == "£.txt"


def test_upload_bad_filename_star_is_a_multipart_error():
    from pyronova.uploads import MultipartError, parse_multipart

    body = _form("b1", "form-data; name=\"f\"; filename*=KOI8-R''%C1")
    with pytest.raises(MultipartError, match="RFC 8187"):
        parse_multipart(_MultipartRequest("multipart/form-data; boundary=b1", body))


def test_upload_media_type_is_case_insensitive_and_boundary_may_quote_a_semicolon():
    from pyronova.uploads import MultipartError, parse_multipart

    body = _form("a;b", 'form-data; name="t"', b"hello")
    form = parse_multipart(_MultipartRequest('Multipart/Form-Data; Boundary="a;b"', body))
    assert form["t"].data == b"hello"
    with pytest.raises(MultipartError, match="Expected multipart/form-data"):
        parse_multipart(_MultipartRequest('multipart/mixed; boundary="a;b"', body))
