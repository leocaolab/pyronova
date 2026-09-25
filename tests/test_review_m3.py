"""Review cced8c2, milestone M3: one response mapping + a real header multimap.

The main interpreter (GIL mode, `gil=True` routes) and the sub-interpreter workers (pool,
TPC inline, the async engine) each turned a handler's return value into a response with
their own code, and had drifted apart. Response headers were a `HashMap<String,String>`
with NUL-packed repeats, appended after the defaults; request headers were flattened
into one `", "`-joined string per name. Every test here failed on 8daaa86 (see
docs/design/code-review-cced8c2-roadmap.md, M3), except the cases marked as regression
coverage.

Serving paths, as `app.run` picks them:
  - "gil"   mode="gil", PYRONOVA_TPC=0          → handlers on main
  - "tpc"   mode="subinterp"                    → TPC inline sub-interpreters
  - "pool"  mode="subinterp", PYRONOVA_TPC=0    → sub-interp pool (sync + async workers)
"""

from __future__ import annotations

import http.client
import os
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time

import pytest

from pyronova import Response

PYTHON = sys.executable
HOST = "127.0.0.1"

PATHS = {
    "gil": {"mode": "gil", "tpc": "0"},
    "tpc": {"mode": "subinterp", "tpc": None},
    "pool": {"mode": "subinterp", "tpc": "0"},
}
ALL_PATHS = list(PATHS)
SUBINTERP_PATHS = ["tpc", "pool"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


class Reply:
    """A response with its header lines as sent: repeats kept, names lower-cased."""

    def __init__(self, status: int, body: bytes, header_lines: list[tuple[str, str]]):
        self.status = status
        self.body = body
        self.header_lines = [(k.lower(), v) for k, v in header_lines]

    def all(self, name: str) -> list[str]:
        return [v for k, v in self.header_lines if k == name]

    def one(self, name: str) -> str:
        values = self.all(name)
        assert len(values) == 1, f"{name}: expected one header line, got {values}"
        return values[0]


class Server:
    def __init__(self, script: str, path: str, workers: int = 2):
        self.path = path
        self.port = _free_port()
        fd, self.script_path = tempfile.mkstemp(prefix="pyronova_m3_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.log_path = self.script_path + ".log"
        env = dict(os.environ)
        env["M3_MODE"] = PATHS[path]["mode"]
        env["M3_PORT"] = str(self.port)
        env["M3_WORKERS"] = str(workers)
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

    def request(
        self, target: str, headers: list[tuple[str, str]] | None = None
    ) -> Reply:
        conn = http.client.HTTPConnection(HOST, self.port, timeout=10)
        try:
            conn.putrequest("GET", target)
            for k, v in headers or []:
                conn.putheader(k, v)
            conn.endheaders()
            r = conn.getresponse()
            return Reply(r.status, r.read(), r.getheaders())
        finally:
            conn.close()

    def log(self) -> str:
        with open(self.log_path, errors="replace") as f:
            return f.read()

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


SCRIPT = """
import json
import os

from pyronova import Pyronova, Response
from pyronova.cookies import get_cookies

app = Pyronova()

# -- one response mapping --------------------------------------------------

@app.get("/list")
def a_list(req):
    return [{"a": True}]

@app.get("/async-list")
async def an_async_list(req):
    return [{"a": True}]

@app.get("/set")
def a_set(req):
    return {"s": {1}}

@app.get("/none")
def nothing(req):
    return None

# -- content type from the returned value, never sniffed from the text -----

@app.get("/brace-text")
def brace_text(req):
    return "{user} logged in"

@app.get("/bracket-text")
def bracket_text(req):
    return "[1]"

@app.get("/response-brace-text")
def response_brace_text(req):
    return Response("{user} logged in")

@app.get("/async-brace-text")
async def async_brace_text(req):
    return "{user} logged in"

# -- response headers: user headers override the defaults -----------------

@app.get("/ct-override")
def ct_override(req):
    return Response("a,b", headers={"Content-Type": "text/csv"})

@app.get("/server-override")
def server_override(req):
    return Response("x", headers={"Server": "mine"})

@app.get("/set-cookies")
def set_cookies(req):
    return Response("x", headers={"set-cookie": ["a=1", "b=2"], "x-one": "1"})

# -- sub-interp silent defaults become real errors ------------------------

class Duck:
    status_code = 65736          # truncated to 200 by `as u16`
    body = "duck"

@app.get("/duck")
def duck(req):
    return Duck()

@app.get("/surrogate-body")
def surrogate_body(req):
    return Response("\\ud800")

class BadStr:
    def __str__(self):
        raise RuntimeError("no str for you")

@app.get("/bad-str-body")
def bad_str_body(req):
    return Response(BadStr())

# -- after_request hook errors: one policy ---------------------------------

@app.after_request
def boom(req, resp):
    if req.path.startswith("/after-boom"):
        raise RuntimeError("after hook failed")
    return None

@app.get("/after-boom")
def after_boom(req):
    return "handler ok"

@app.get("/after-boom-async")
async def after_boom_async(req):
    return "handler ok"

# -- request headers: multi-value view -------------------------------------

@app.get("/req-headers")
def req_headers(req):
    return {
        "joined": req.headers.get("x-multi"),
        "all": req.headers.get_all("x-multi"),
        "missing": req.headers.get_all("x-none"),
    }

@app.get("/async-req-headers")
async def async_req_headers(req):
    return {"all": req.headers.get_all("x-multi")}

@app.get("/cookies")
def cookies(req):
    return {"cookie": req.headers.get("cookie"), "cookies": get_cookies(req)}

@app.get("/async-cookies")
async def async_cookies(req):
    return {"cookie": req.headers.get("cookie"), "cookies": get_cookies(req)}

app.run(host="127.0.0.1", port=int(os.environ["M3_PORT"]), mode=os.environ["M3_MODE"],
        workers=int(os.environ["M3_WORKERS"]))
"""


@pytest.fixture(scope="module", params=ALL_PATHS)
def srv(request):
    server = Server(SCRIPT, request.param)
    yield server
    server.stop()


def _only(srv, paths):
    if srv.path not in paths:
        pytest.skip(f"not a {paths} case")


# ---------------------------------------------------------------------------
# One response mapping
# ---------------------------------------------------------------------------


def test_list_is_json_on_every_path(srv):
    r = srv.request("/list")
    assert r.status == 200
    assert r.one("content-type") == "application/json"
    assert r.body == b'[{"a":true}]'


def test_async_list_is_json_on_every_path(srv):
    r = srv.request("/async-list")
    assert r.status == 200
    assert r.one("content-type") == "application/json"
    assert r.body == b'[{"a":true}]'


def test_set_inside_a_dict_serializes_on_every_path(srv):
    r = srv.request("/set")
    assert (r.status, r.body) == (200, b'{"s":[1]}')


def test_none_is_an_empty_200_on_every_path(srv):
    r = srv.request("/none")
    assert (r.status, r.body) == (200, b"")


# ---------------------------------------------------------------------------
# Content type comes from the returned value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "route",
    ["/brace-text", "/bracket-text", "/response-brace-text", "/async-brace-text"],
)
def test_text_that_looks_like_json_stays_text(srv, route):
    r = srv.request(route)
    assert r.status == 200
    assert r.one("content-type") == "text/plain; charset=utf-8"


# ---------------------------------------------------------------------------
# Response headers
# ---------------------------------------------------------------------------


def test_user_content_type_header_replaces_the_default(srv):
    r = srv.request("/ct-override")
    assert r.status == 200
    assert r.all("content-type") == ["text/csv"]


def test_user_server_header_replaces_the_default(srv):
    r = srv.request("/server-override")
    assert r.all("server") == ["mine"]


def test_repeated_header_values_go_out_as_separate_lines(srv):
    # The GIL path split the NUL-packed value into lines; a worker took the packed string
    # as one value, which no header can hold, and answered 500.
    r = srv.request("/set-cookies")
    assert r.all("set-cookie") == ["a=1", "b=2"]
    assert r.all("x-one") == ["1"]


def test_non_str_header_value_is_a_type_error_naming_the_key():
    with pytest.raises(TypeError, match="x-count"):
        Response("x", headers={"x-count": 5})


def test_non_str_item_in_a_header_list_is_a_type_error_naming_the_key():
    with pytest.raises(TypeError, match="set-cookie"):
        Response("x", headers={"set-cookie": ["a=1", 5]})


def test_invalid_header_value_is_a_value_error_naming_the_key():
    with pytest.raises(ValueError, match="x-evil"):
        Response("x", headers={"x-evil": "a\r\nset-cookie: admin=1"})


def test_response_headers_read_back_as_given():
    resp = Response("x", headers={"set-cookie": ["a=1", "b=2"], "X-One": "1"})
    assert resp.headers == {"set-cookie": ["a=1", "b=2"], "x-one": "1"}


# ---------------------------------------------------------------------------
# Sub-interpreter silent defaults
# ---------------------------------------------------------------------------


def test_duck_typed_object_is_not_a_truncated_status(srv):
    # A worker took any object with `status_code` + `body` as a response, and read the
    # status with `as u16`: 65736 went out as 200. Only `Response` is a response, on
    # every path, as on the main interpreter.
    r = srv.request("/duck")
    assert (r.status, r.body) != (200, b"duck")
    assert r.status == 200 and r.body.startswith(b"<")
    assert r.one("content-type") == "text/plain; charset=utf-8"


@pytest.mark.parametrize("route", ["/surrogate-body", "/bad-str-body"])
def test_unconvertible_body_is_a_500_not_an_empty_200(srv, route):
    r = srv.request(route)
    assert r.status == 500


# ---------------------------------------------------------------------------
# after_request hook errors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("route", ["/after-boom", "/after-boom-async"])
def test_after_hook_error_is_a_500_on_every_path(srv, route):
    r = srv.request(route)
    assert r.status == 500
    assert b"handler ok" not in r.body


# ---------------------------------------------------------------------------
# Request headers
# ---------------------------------------------------------------------------


def test_repeated_request_header_is_visible_as_a_list(srv):
    import json

    r = srv.request("/req-headers", [("X-Multi", "a"), ("X-Multi", "b, c")])
    assert r.status == 200, r.body
    got = json.loads(r.body)
    assert got["all"] == ["a", "b, c"]
    assert got["joined"] == "a, b, c"
    assert got["missing"] == []


def test_async_handler_sees_repeated_request_headers(srv):
    import json

    r = srv.request("/async-req-headers", [("X-Multi", "a"), ("X-Multi", "b")])
    assert r.status == 200, r.body
    assert json.loads(r.body)["all"] == ["a", "b"]


def test_http1_repeated_cookie_headers_join_with_semicolon(srv):
    import json

    r = srv.request("/cookies", [("Cookie", "a=1"), ("Cookie", "b=2")])
    got = json.loads(r.body)
    assert got["cookie"] == "a=1; b=2"
    assert got["cookies"] == {"a": "1", "b": "2"}


@pytest.mark.parametrize("route", ["/cookies", "/async-cookies"])
def test_http2_split_cookie_headers_join_with_semicolon(srv, route):
    # RFC 9113 §8.2.3: an HTTP/2 client may split `cookie` into one field per crumb; the
    # server joins them with "; " (not ", ") before handing them to the application.
    httpx = pytest.importorskip("httpx")
    pytest.importorskip("h2")
    with httpx.Client(http1=False, http2=True, timeout=10) as c:
        r = c.get(
            f"http://{HOST}:{srv.port}{route}",
            headers=[("cookie", "a=1"), ("cookie", "b=2")],
        )
    assert r.http_version == "HTTP/2"
    assert r.status_code == 200, r.text
    assert r.json()["cookies"] == {"a": "1", "b": "2"}
