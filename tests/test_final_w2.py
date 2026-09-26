"""Final review, fix wave A, W2 (request path): docs/design/code-review-final-rubric.md.

Every server test runs a real server in a subprocess on the path it names, bound to port
0 (the port is read from its "Listening on" line), so it exercises the dispatch code that
path runs:

  - "gil"     mode="gil", PYRONOVA_TPC=0         → handlers on main (Tokio blocking pool)
  - "tpc-gil" mode="gil"                         → TPC threads, handlers on main
  - "tpc"     mode="subinterp"                   → TPC inline sub-interpreters; gil=True
                                                   routes go through the main-interp bridge
  - "pool"    mode="subinterp", PYRONOVA_TPC=0   → sub-interp pool; gil=True routes on main

The request budget is 30 s (`body::REQUEST_BUDGET`); the tests that must outlast it run
their paths concurrently. The bridge spawn-failure test needs a
`maturin develop --release --features fault_injection` build (skipped otherwise).
"""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import os
import pathlib
import re
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
REQUEST_BUDGET_S = 30

PATHS = {
    "gil": {"mode": "gil", "tpc": "0"},
    "tpc-gil": {"mode": "gil", "tpc": None},
    "tpc": {"mode": "subinterp", "tpc": None},
    "pool": {"mode": "subinterp", "tpc": "0"},
}

RUN = """
import os
app.run(host="127.0.0.1", port=0, mode=os.environ["W2_MODE"],
        workers=int(os.environ["W2_WORKERS"]))
"""

LISTENING = re.compile(r"Listening on http://127\.0\.0\.1:(\d+)")


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
    """`script` served on `path`, on a port the kernel picked."""

    def __init__(
        self,
        script: str,
        path: str,
        workers: int = 2,
        env: dict[str, str] | None = None,
        wait: bool = True,
    ):
        self.path = path
        self.port: int | None = None
        fd, self.script_path = tempfile.mkstemp(prefix="pyronova_w2_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.log_path = self.script_path + ".log"
        full_env = dict(os.environ)
        full_env["W2_MODE"] = PATHS[path]["mode"]
        full_env["W2_WORKERS"] = str(workers)
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
                start_new_session=True,
                env=full_env,
            )
        if wait:
            self.port = self._bound_port()

    def _bound_port(self, timeout: float = 30) -> int:
        deadline = time.time() + timeout
        while time.time() < deadline:
            found = LISTENING.search(self.log())
            if found:
                port = int(found.group(1))
                try:
                    with socket.create_connection((HOST, port), timeout=0.5):
                        return port
                except OSError:
                    pass
            if self.proc.poll() is not None:
                break
            time.sleep(0.05)
        raise RuntimeError(f"server ({self.path}) did not start:\n{self.stop()}")

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
        """The JSON log records written so far."""
        out = []
        for line in self.log().splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if isinstance(rec, dict):
                out.append(rec)
        return out

    def records_after(self, pred, settle: float = 1.0, timeout: float = 10.0) -> list[dict]:
        """The records matching `pred` once at least one is written and none has been
        added for `settle` seconds (the log writer is non-blocking)."""
        deadline = time.time() + timeout
        found: list[dict] = []
        stable_since = time.time()
        while time.time() < deadline:
            now = [r for r in self.records() if pred(r)]
            if len(now) != len(found):
                found, stable_since = now, time.time()
            elif found and time.time() - stable_since >= settle:
                break
            time.sleep(0.1)
        return found

    def wait_for_output(self, text: str, timeout: float = 15) -> None:
        deadline = time.time() + timeout
        while text not in self.log():
            if time.time() > deadline:
                raise AssertionError(f"{text!r} never appeared:\n{self.log()[-3000:]}")
            time.sleep(0.05)

    def wait_exit(self, timeout: float = 60) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.stop()
            raise AssertionError(f"server did not exit in {timeout}s:\n{self.log()[-4000:]}")

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


def _fields(rec: dict) -> dict:
    return rec.get("fields", rec)


def _text(rec: dict) -> str:
    return json.dumps(rec)


def raw_exchange(port: int, request: bytes, timeout: float = 15) -> tuple[int, dict, bytes]:
    """Sends `request` as is and reads one response: its head, and its body when it
    declares a `content-length`."""
    with socket.create_connection((HOST, port), timeout=timeout) as s:
        s.sendall(request)
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        head, _, rest = data.partition(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        status = int(lines[0].split(" ")[1])
        headers = {}
        for line in lines[1:]:
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
        length = int(headers.get("content-length", "0"))
        while len(rest) < length:
            chunk = s.recv(4096)
            if not chunk:
                break
            rest += chunk
    return status, headers, rest


def ws_request(path: str, **overrides: str | None) -> bytes:
    """A WebSocket opening handshake for `path`; an override of `None` drops the header,
    `request_line` replaces the request line."""
    fields = {
        "Host": "x",
        "Upgrade": "websocket",
        "Connection": "Upgrade",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
    }
    request_line = overrides.pop("request_line", None) or f"GET {path} HTTP/1.1"
    for name, value in overrides.items():
        name = name.replace("_", "-")
        if value is None:
            fields.pop(name, None)
        else:
            fields[name] = value
    head = request_line + "\r\n" + "".join(f"{k}: {v}\r\n" for k, v in fields.items())
    return (head + "\r\n").encode()


def ws_connect(port: int, target: str, headers: dict | None = None):
    from websockets.sync.client import connect

    return connect(
        f"ws://{HOST}:{port}{target}", additional_headers=headers or {}, open_timeout=60
    )


# ---------------------------------------------------------------------------
# 1. One Request constructor: every path builds the same Request
# ---------------------------------------------------------------------------

REQUEST_SCRIPT = """
import json
from pyronova import Pyronova
app = Pyronova()

def echo(req):
    return {
        "method": req.method, "path": req.path, "query": req.query,
        "q": req.query_params.get("q"), "params": req.params,
        "h": req.headers.get("x-h"), "ip": req.client_ip, "body": req.body.decode(),
        "has_id": bool(req.request_id), "stream": req.stream is None,
    }

app.post("/echo/{id}")(echo)
app.post("/echo-gil/{id}", gil=True)(echo)

@app.get("/plain")
def plain(req):
    return "plain body"

@app.post("/only-post")
def only_post(req):
    return "posted"

@app.websocket("/ws")
def ws(ws):
    r = ws.request
    ws.send(json.dumps({"method": r.method, "path": r.path, "query": r.query,
                        "body": r.body.decode(), "ip": r.client_ip}))
    ws.close()
""" + RUN

REQUEST_CASES = [
    ("tpc", "/echo/7"),  # TPC inline
    ("tpc", "/echo-gil/7"),  # TPC bridge
    ("pool", "/echo/7"),  # pool worker
    ("pool", "/echo-gil/7"),  # pool's main dispatch
    ("gil", "/echo/7"),  # GIL mode
    ("tpc-gil", "/echo/7"),  # TPC threads, handler on main
]


@pytest.mark.parametrize("path", sorted({p for p, _ in REQUEST_CASES}))
def test_every_path_builds_the_same_request(path):
    with serve(REQUEST_SCRIPT, path) as srv:
        for _, route in (c for c in REQUEST_CASES if c[0] == path):
            r = srv.post(route + "?q=a%20b&x=1", b"payload", headers={"x-h": "hv"})
            assert r.status == 200, (route, r.status, r.body)
            assert r.json() == {
                "method": "POST",
                "path": route,
                "query": "q=a%20b&x=1",
                "q": "a b",
                "params": {"id": "7"},
                "h": "hv",
                "ip": "127.0.0.1",
                "body": "payload",
                "has_id": True,
                "stream": True,
            }, route


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_ws_request_comes_from_the_same_constructor(path):
    with serve(REQUEST_SCRIPT, path) as srv:
        with ws_connect(srv.port, "/ws?q=1") as ws:
            got = json.loads(ws.recv(timeout=10))
        assert got == {"method": "GET", "path": "/ws", "query": "q=1", "body": "",
                       "ip": "127.0.0.1"}


def test_python_request_constructor_goes_through_the_same_constructor():
    from pyronova import Request

    req = Request("GET", "/p", {"id": "1"}, "a=1&a=2", b"x", {"x-h": "v"}, "10.0.0.1")
    assert (req.method, req.path, req.query) == ("GET", "/p", "a=1&a=2")
    assert req.query_params == {"a": "1"}
    assert req.params == {"id": "1"}
    assert req.headers["x-h"] == "v"
    assert req.body == b"x"
    assert req.stream is None
    with pytest.raises(ValueError, match="not a valid request line"):
        Request("GET", "/a b", {}, "", b"", {}, "10.0.0.1")
    with pytest.raises(ValueError, match="not a valid request line"):
        Request("BAD METHOD", "/", {}, "", b"", {}, "10.0.0.1")


# ---------------------------------------------------------------------------
# 2. Streamed body: bounded feeder (F4), total deadline (G2), StreamState
# ---------------------------------------------------------------------------

STREAM_SCRIPT = """
import time
from pyronova import Pyronova
from pyronova.engine import BodyRejected
app = Pyronova()
app.max_body_size = 1024

@app.post("/reread", gil=True, stream=True)
def reread(req):
    stream = req.stream
    outcomes = []
    for _ in range(2):
        try:
            stream.read()
            outcomes.append("read")
        except BodyRejected:
            outcomes.append("rejected")
    try:
        next(stream)
        outcomes.append("chunk")
    except BodyRejected:
        outcomes.append("rejected")
    except StopIteration:
        outcomes.append("stop")
    return outcomes

@app.post("/drain-twice", gil=True, stream=True)
def drain_twice(req):
    stream = req.stream
    first = len(stream.read())
    try:
        second = stream.drain_count()
    except RuntimeError as e:
        second = "RuntimeError: " + str(e)
    return {"first": first, "second": second}

@app.post("/take-twice", gil=True, stream=True)
def take_twice(req):
    first = req.stream
    try:
        req.stream
        second = "returned"
    except RuntimeError as e:
        second = "RuntimeError: " + str(e)
    return {"first": type(first).__name__, "second": second, "rest": len(first.read())}

@app.post("/stuck", gil=True, stream=True)
def stuck(req):
    # Takes the stream, never reads it, and outlasts two budgets' worth of the old
    # per-frame wait.
    req.stream
    time.sleep(70)
    return "late"

@app.post("/iterate", gil=True, stream=True)
def iterate(req):
    return {"bytes": sum(len(c) for c in req.stream)}
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_a_rejected_stream_keeps_raising_the_rejection(path):
    with serve(STREAM_SCRIPT, path) as srv:
        r = srv.post("/reread", b"x" * 4096)
        # The handler caught every read, so it answers normally with what each read did.
        assert r.status == 200, r.body
        assert r.json() == ["rejected", "rejected", "rejected"]


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_drain_count_on_a_finished_stream_is_an_error_not_zero(path):
    with serve(STREAM_SCRIPT, path) as srv:
        r = srv.post("/drain-twice", b"y" * 100)
        assert r.status == 200, r.body
        body = r.json()
        assert body["first"] == 100
        assert body["second"].startswith("RuntimeError: "), body


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_taking_req_stream_twice_is_an_error_not_none(path):
    with serve(STREAM_SCRIPT, path) as srv:
        r = srv.post("/take-twice", b"z" * 10)
        assert r.status == 200, r.body
        body = r.json()
        assert body["first"] == "BodyStream"
        assert body["second"].startswith("RuntimeError: "), body
        assert "already taken" in body["second"]
        assert body["rest"] == 10


def _chunked_body_then_wait(port: int, target: str, chunks: int) -> tuple[int, float]:
    """Sends a chunked body of `chunks` one-byte chunks (one body frame each) in one go,
    then waits for the answer. Returns the status and how long it took."""
    body = b"".join(b"1\r\nx\r\n" for _ in range(chunks)) + b"0\r\n\r\n"
    request = (
        f"POST {target} HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n".encode()
        + body
    )
    started = time.time()
    status, _, _ = raw_exchange(port, request, timeout=120)
    return status, time.time() - started


def _drip_body(port: int, target: str, every: float, total: int) -> tuple[int, float]:
    """Declares a `total`-byte body and sends one byte every `every` seconds (each well
    within the budget), until the server answers. Returns the status and how long it
    took."""
    started = time.time()
    with socket.create_connection((HOST, port), timeout=120) as s:
        s.sendall(
            f"POST {target} HTTP/1.1\r\nHost: x\r\nContent-Length: {total}\r\n\r\n".encode()
        )
        s.settimeout(every)
        data = b""
        for _ in range(total):
            try:
                s.sendall(b"d")
            except OSError:
                break
            try:
                chunk = s.recv(4096)
            except socket.timeout:
                continue
            if not chunk:
                break
            data += chunk
            if b"\r\n\r\n" in data:
                break
        s.settimeout(60)
        while b"\r\n\r\n" not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
    status = int(data.split(b" ")[1])
    return status, time.time() - started


def test_streamed_body_budget_bounds_every_send_and_the_whole_body():
    # One run, all paths at once, both cases: the budget is 30 s.
    #
    # F4: a body of exactly the channel's 8 frames fills it; the body's end then has no
    # room while the handler never reads. That send is bounded like every other, so the
    # feeder ends at the budget and the handler gets its own budget: 504 at ~60 s, before
    # the handler's 70 s sleep ends (cced8c2+R-a: the feeder waited forever, the request
    # got the handler's late 200).
    #
    # G2: a body dripping one byte every 2 s never stalls one frame for 30 s, but takes
    # 80 s in total: the total deadline answers it 408 at ~30 s (per-frame: 200).
    servers = {path: Server(STREAM_SCRIPT, path) for path in ("tpc", "gil")}
    try:
        with concurrent.futures.ThreadPoolExecutor(len(servers) * 2) as pool:
            stuck = {
                path: pool.submit(_chunked_body_then_wait, srv.port, "/stuck", 8)
                for path, srv in servers.items()
            }
            drip = {
                path: pool.submit(_drip_body, srv.port, "/iterate", 2.0, 40)
                for path, srv in servers.items()
            }
            for path, f in stuck.items():
                status, took = f.result()
                assert status == 504, (path, status, took)
                assert took < 69, (path, took)
            for path, f in drip.items():
                status, took = f.result()
                assert status == 408, (path, status, took)
                assert REQUEST_BUDGET_S - 1 < took < REQUEST_BUDGET_S + 10, (path, took)
    finally:
        for srv in servers.values():
            srv.stop()


# ---------------------------------------------------------------------------
# 3. R4 residual: a non-coroutine awaitable runs in the request's context
# ---------------------------------------------------------------------------

AWAITABLE_SCRIPT = """
from pyronova import Pyronova, Response
from pyronova.context import ctx
app = Pyronova()

class Setting:
    # An awaitable that is not a coroutine, like a Cython or mypyc coroutine or a custom
    # awaitable: what it sets while awaited must reach the rest of the request.
    def __init__(self, key, value, result=None):
        self.key, self.value, self.result = key, value, result

    def __await__(self):
        ctx.set(self.key, self.value)
        return self.result
        yield  # a generator: __await__ returns an iterator

@app.before_request
def tag(req):
    return Setting("user", "u" + req.path)

@app.after_request
def expose(req, resp):
    headers = dict(resp.headers)
    headers["x-handler"] = str(ctx.get("handler"))
    return Response(resp.body, status_code=resp.status_code,
                    content_type=resp.content_type, headers=headers)

def probe(req):
    return Setting("handler", "h" + req.path, {"user": ctx.get("user")})

app.get("/probe")(probe)
app.get("/probe-gil", gil=True)(probe)
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_a_non_coroutine_awaitable_writes_the_request_context(path):
    # One worker / TPC thread: the second request runs on the thread that ran the first.
    with serve(AWAITABLE_SCRIPT, path, workers=1) as srv:
        for route in ("/probe", "/probe-gil"):
            for _ in range(2):
                r = srv.get(route)
                assert r.status == 200, (route, r.status, r.body)
                assert r.json() == {"user": "u" + route}, route
                assert r.headers["x-handler"] == "h" + route, route


# ---------------------------------------------------------------------------
# 4. Errors
# ---------------------------------------------------------------------------

OVERRUN_SCRIPT = """
import time
from pyronova import Pyronova
app = Pyronova()

@app.get("/slow")
def slow(req):
    time.sleep(31)
    raise ValueError("w2-late-and-broken")
""" + RUN


def test_an_inline_overrun_logs_once_with_what_the_handler_raised():
    with serve(OVERRUN_SCRIPT, "tpc", workers=1) as srv:
        r = srv.get("/slow", timeout=60)
        assert r.status == 504, r.body
        request_id = r.json()["request_id"]
        mine = srv.records_after(lambda rec: request_id in _text(rec))
        assert len(mine) == 1, mine
        text = _text(mine[0])
        assert "ran past" in text and "w2-late-and-broken" in text, mine[0]
        assert mine[0].get("level") == "ERROR"


BRIDGE_FULL_SCRIPT = """
import time
from pyronova import Pyronova, get_gil_metrics
app = Pyronova(log_config={"level": "WARN"})

@app.get("/hold", gil=True)
def hold(req):
    print("w2 hold started", flush=True)
    time.sleep(3)
    return "held"

@app.get("/dropped", gil=True)
def dropped(req):
    return {"dropped": get_gil_metrics().dropped_requests}
""" + RUN


def test_a_full_bridge_queue_is_one_counted_logged_refusal():
    # One bridge thread, one queue slot: with one request running, of two more exactly one
    # is queued and one refused (503) — counted once, where it is refused, and logged once.
    env = {"PYRONOVA_GIL_BRIDGE_WORKERS": "1", "PYRONOVA_GIL_BRIDGE_CAPACITY": "1"}
    with serve(BRIDGE_FULL_SCRIPT, "tpc", env=env) as srv:
        with concurrent.futures.ThreadPoolExecutor(3) as pool:
            running = pool.submit(srv.get, "/hold", timeout=30)
            srv.wait_for_output("w2 hold started")
            contenders = [pool.submit(srv.get, "/hold", timeout=30) for _ in range(2)]
            statuses = sorted(f.result().status for f in contenders)
            assert running.result().status == 200
        assert statuses == [200, 503], statuses
        assert srv.get("/dropped").json() == {"dropped": 1}
        refused = srv.records_after(lambda rec: "request refused" in _text(rec))
        assert len(refused) == 1, refused
        assert "gil=True bridge queue" in _text(refused[0])


HAS_FAULT_INJECTION = hasattr(pyronova.engine, "_fault_fail_bridge_spawn")


@pytest.mark.skipif(
    not HAS_FAULT_INJECTION,
    reason="needs a `maturin develop --release --features fault_injection` build",
)
def test_a_bridge_thread_that_cannot_start_fails_the_server_start():
    script = """
    import os, sys
    from pyronova import Pyronova
    from pyronova.engine import _fault_fail_bridge_spawn
    app = Pyronova()

    @app.get("/on-main", gil=True)
    def on_main(req):
        return "main"

    if __name__ == "__main__":
        _fault_fail_bridge_spawn(1)
        try:
            app.run(host="127.0.0.1", port=0, mode=os.environ["W2_MODE"],
                    workers=int(os.environ["W2_WORKERS"]))
        except RuntimeError as e:
            print("w2 start failed:", e, flush=True)
            sys.exit(3)
    """
    env = {"PYRONOVA_GIL_BRIDGE_WORKERS": "2"}
    srv = Server(script, "tpc", env=env, wait=False)
    try:
        rc = srv.wait_exit(timeout=60)
        log = srv.log()
        assert rc == 3, log[-3000:]
        assert "could not start main-interpreter bridge thread 1 of 2" in log, log[-3000:]
        assert "Listening on" not in log
    finally:
        srv.stop()


WS_ERROR_SCRIPT = """
from pyronova import Pyronova
app = Pyronova()

@app.websocket("/ws-raise")
def ws_raise(ws):
    raise ValueError("w2-ws-boom")
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_a_websocket_handler_error_is_logged_with_its_request(path):
    with serve(WS_ERROR_SCRIPT, path) as srv:
        with ws_connect(srv.port, "/ws-raise") as ws:
            try:
                ws.recv(timeout=10)
            except Exception:
                pass
        found = srv.records_after(lambda rec: "w2-ws-boom" in _text(rec))
        assert len(found) == 1, found
        fields = _fields(found[0])
        assert fields.get("request_id"), found[0]
        assert fields.get("method") == "GET", found[0]
        assert fields.get("path") == "/ws-raise", found[0]
        assert "Traceback" in fields.get("traceback", ""), found[0]
        assert found[0].get("level") == "ERROR"


# ---------------------------------------------------------------------------
# 5. WebSocket: typed handshake, routing, connection slot (R2), one Pong
# ---------------------------------------------------------------------------

WS_SCRIPT = """
import time
from pyronova import Pyronova, Response
app = Pyronova()
app.max_websocket_connections = 1

@app.before_request
def guard(req):
    print("w2 hook ran", req.method, req.path, req.query, flush=True)
    if req.query_params.get("slow"):
        time.sleep(40)
    if req.query_params.get("deny"):
        return Response("denied", status_code=403)

@app.websocket("/ws")
def hold(ws):
    while ws.recv() is not None:
        pass

@app.get("/plain")
def plain(req):
    return "plain body"
""" + RUN

BAD_HANDSHAKES = [
    ({"request_line": "POST /ws HTTP/1.1"}, 400),
    ({"request_line": "GET /ws HTTP/1.0"}, 400),
    ({"Connection": "keep-alive"}, 400),
    ({"Sec-WebSocket-Key": None}, 400),
    ({"Sec-WebSocket-Version": "8"}, 426),
]


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_a_bad_handshake_is_refused_before_any_hook_slot_or_thread(path):
    with serve(WS_SCRIPT, path) as srv:
        for overrides, status in BAD_HANDSHAKES:
            got, headers, _ = raw_exchange(srv.port, ws_request("/ws", **dict(overrides)))
            assert got == status, (overrides, got)
            if status == 426:
                assert headers.get("sec-websocket-version") == "13"
        # With one connection slot, a valid upgrade still gets it: none was taken.
        with ws_connect(srv.port, "/ws") as ws:
            ws.send("hi")
        # Only the valid upgrade ran the hook.
        hooks = srv.log().count("w2 hook ran")
        assert hooks == 1, srv.log()[-3000:]


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_upgrade_on_an_ordinary_route_is_routed_normally(path):
    with serve(WS_SCRIPT, path) as srv:
        status, _, body = raw_exchange(
            srv.port,
            b"GET /plain HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Content-Length: 0\r\nConnection: close\r\n\r\n",
        )
        assert status == 200
        assert body.startswith(b"plain body")


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_a_rejected_upgrade_frees_its_slot(path):
    from websockets.exceptions import InvalidStatus

    with serve(WS_SCRIPT, path) as srv:
        with pytest.raises(InvalidStatus) as exc:
            ws_connect(srv.port, "/ws?deny=1")
        assert exc.value.response.status_code == 403
        # The slot is released once the hook's thread is joined.
        deadline = time.time() + 10
        while True:
            try:
                with ws_connect(srv.port, "/ws") as ws:
                    ws.send("hi")
                break
            except InvalidStatus as e:
                assert e.response.status_code == 503
                assert time.time() < deadline, "the rejected upgrade kept its slot"
                time.sleep(0.1)


def _slot_held_through_a_timed_out_hook(port: int) -> tuple[int, int, float]:
    """A hook sleeping past the budget: the handshake answers 504 at ~30 s while the hook
    keeps its thread (and so the only slot) until ~40 s. Returns the 504's status, the
    status of a connection tried right after it, and when a connection next succeeded."""
    from websockets.exceptions import InvalidStatus

    started = time.time()
    try:
        ws_connect(port, "/ws?slow=1").close()
        slow = 101
    except InvalidStatus as e:
        slow = e.response.status_code
    try:
        ws_connect(port, "/ws").close()
        right_after = 101
    except InvalidStatus as e:
        right_after = e.response.status_code
    while True:
        try:
            ws_connect(port, "/ws").close()
            return slow, right_after, time.time() - started
        except InvalidStatus:
            if time.time() - started > 60:
                return slow, right_after, float("inf")
            time.sleep(0.2)


def test_the_connection_slot_lives_as_long_as_the_hook_thread():
    # R2: the slot used to drop with the 504 while the hook thread kept running, so the
    # next connection got a second thread past the cap.
    servers = {path: Server(WS_SCRIPT, path) for path in ("tpc", "gil")}
    try:
        with concurrent.futures.ThreadPoolExecutor(len(servers)) as pool:
            futures = {
                path: pool.submit(_slot_held_through_a_timed_out_hook, srv.port)
                for path, srv in servers.items()
            }
            for path, f in futures.items():
                slow, right_after, freed_at = f.result()
                assert slow == 504, (path, slow)
                assert right_after == 503, (path, right_after)
                assert 39 < freed_at < 55, (path, freed_at)
    finally:
        for srv in servers.values():
            srv.stop()


def _ws_frame(opcode: int, payload: bytes) -> bytes:
    """A masked client frame (payload under 126 bytes)."""
    mask = b"\x01\x02\x03\x04"
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return bytes([0x80 | opcode, 0x80 | len(payload)]) + mask + masked


def _server_frames(data: bytes) -> list[tuple[int, bytes]]:
    """The unmasked server frames in `data` (payloads under 126 bytes)."""
    frames = []
    while len(data) >= 2:
        opcode, length = data[0] & 0x0F, data[1] & 0x7F
        if len(data) < 2 + length:
            break
        frames.append((opcode, data[2 : 2 + length]))
        data = data[2 + length :]
    return frames


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_a_ping_gets_exactly_one_pong(path):
    with serve(WS_SCRIPT, path) as srv:
        with socket.create_connection((HOST, srv.port), timeout=10) as s:
            s.sendall(ws_request("/ws"))
            data = b""
            while b"\r\n\r\n" not in data:
                data += s.recv(4096)
            head, _, rest = data.partition(b"\r\n\r\n")
            assert b" 101 " in head.split(b"\r\n")[0], head
            s.sendall(_ws_frame(0x9, b"w2-ping"))
            s.settimeout(0.3)
            deadline = time.time() + 2
            while time.time() < deadline:
                try:
                    chunk = s.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                rest += chunk
        pongs = [p for op, p in _server_frames(rest) if op == 0xA]
        assert pongs == [b"w2-ping"], _server_frames(rest)


# ---------------------------------------------------------------------------
# 6. Layering: shared error/body types in the bottom layer, one panic_message
# ---------------------------------------------------------------------------

SRC = pathlib.Path(__file__).resolve().parent.parent / "src"


def test_lower_layers_do_not_import_the_handlers_layer():
    lower = [
        *sorted((SRC / "python").glob("*.rs")),
        SRC / "response.rs",
        SRC / "grpc.rs",
        SRC / "db.rs",
        *sorted((SRC / "db").glob("*.rs")),
        SRC / "error.rs",
        SRC / "body.rs",
        SRC / "request_head.rs",
        *sorted((SRC / "types").glob("*.rs")),
    ]
    offenders = [p.name for p in lower if "crate::handlers" in p.read_text()]
    assert offenders == [], offenders


def test_there_is_one_panic_message():
    defs = [
        str(p.relative_to(SRC))
        for p in SRC.rglob("*.rs")
        if re.search(r"\bfn panic_message\b", p.read_text())
    ]
    assert defs == ["error.rs"], defs


# ---------------------------------------------------------------------------
# 7. Small items: HEAD, 405, sampling, Accept-Encoding
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_head_on_a_get_route_is_served_without_a_body(path):
    with serve(REQUEST_SCRIPT, path) as srv:
        get = srv.get("/plain")
        head = srv.request("HEAD", "/plain")
        assert head.status == 200, head.status
        assert head.body == b""
        assert head.headers["content-length"] == str(len(get.body))
        assert head.headers["content-type"] == get.headers["content-type"]


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_a_wrong_method_is_405_with_allow(path):
    with serve(REQUEST_SCRIPT, path) as srv:
        r = srv.get("/only-post")
        assert r.status == 405, r.body
        assert r.headers["allow"] == "POST"
        assert r.json() == {"error": "method not allowed"}
        r = srv.request("DELETE", "/plain")
        assert (r.status, r.headers["allow"]) == (405, "GET, HEAD")
        # A path no method has is still 404.
        assert srv.get("/nope").status == 404


SAMPLING_SCRIPT = """
from pyronova import Pyronova
app = Pyronova()
app.enable_logging(level="info", sample=10)

@app.get("/s")
def s(req):
    return "ok"
""" + RUN


def test_access_log_keeps_one_in_n_per_thread():
    # One TPC thread: its roll starts at its first response, so of 100 responses on it
    # exactly 10 (the 1st, 11th, ...) are logged.
    with serve(SAMPLING_SCRIPT, "tpc", workers=1) as srv:
        conn = http.client.HTTPConnection(HOST, srv.port, timeout=10)
        try:
            for _ in range(100):
                conn.request("GET", "/s")
                r = conn.getresponse()
                r.read()
                assert r.status == 200
        finally:
            conn.close()
        lines = srv.records_after(
            lambda rec: _fields(rec).get("path") == "/s"
            and rec.get("target") == "pyronova::access"
        )
        assert len(lines) == 10, len(lines)


COMPRESSION_SCRIPT = """
from pyronova import Pyronova
app = Pyronova()
app.enable_compression(min_size=64)

@app.get("/big")
def big(req):
    return "abcdefgh" * 200
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_no_or_unreadable_accept_encoding_is_not_compressed(path):
    with serve(COMPRESSION_SCRIPT, path) as srv:
        def get(extra: bytes) -> tuple[int, dict]:
            status, headers, _ = raw_exchange(
                srv.port, b"GET /big HTTP/1.1\r\nHost: x\r\nConnection: close\r\n" + extra + b"\r\n"
            )
            return status, headers

        status, headers = get(b"")
        assert status == 200 and "content-encoding" not in headers
        status, headers = get(b"Accept-Encoding: gzip\xff\r\n")
        assert status == 200 and "content-encoding" not in headers
        status, headers = get(b"Accept-Encoding: gzip\r\n")
        assert status == 200 and headers.get("content-encoding") == "gzip"
