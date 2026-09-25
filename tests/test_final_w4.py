"""Final review, fix wave A, W4 (package & data): docs/design/code-review-final-rubric.md.

Each test names the item it covers. Server tests run a real server, either in a
subprocess on the serving path they name (`serve`, from test_review_ra: "gil", "tpc",
"pool") or in-process with `TestClient(mode="gil")`; both bind ports the kernel picks.
The Postgres tests need `PYRONOVA_TEST_PG_DSN` (CI and the supervisor use
postgres://hucao@localhost:5432/pyronova_test); their tables are prefixed `w4_`.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import os
import subprocess
import sys
import textwrap
import threading
import time

import pytest

from pyronova import Pyronova, Response, Stream
from pyronova.testing import TestClient
from tests.test_review_ra import RUN, serve

PYTHON = sys.executable
PG_DSN = os.environ.get("PYRONOVA_TEST_PG_DSN")
needs_pg = pytest.mark.skipif(PG_DSN is None, reason="PYRONOVA_TEST_PG_DSN not set")


def _run_script(body: str, env: dict[str, str] | None = None, timeout: float = 30):
    """A snippet in a fresh process (the tracing subscriber, the metrics switch and the
    Postgres pool are process-wide)."""
    full_env = dict(os.environ)
    full_env.update(env or {})
    return subprocess.run(
        [PYTHON, "-c", textwrap.dedent(body)],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=full_env,
    )


# ---------------------------------------------------------------------------
# F1 / G3: /mcp and @app.rpc refuse cross-site requests
# ---------------------------------------------------------------------------

CSRF_SCRIPT = """
import os
from pyronova import Pyronova
app = Pyronova()
app.trusted_origins = ["https://app.example.com"]

@app.rpc("/rpc/echo")
def echo(data):
    return data

@app.mcp.tool()
def ping() -> str:
    return "pong"
""" + RUN

MCP_LIST = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode()


@pytest.fixture(scope="module", params=["gil", "tpc"])
def csrf_srv(request):
    with serve(CSRF_SCRIPT, request.param) as srv:
        yield srv


@pytest.mark.parametrize("target,body", [("/rpc/echo", b'{"a": 1}'), ("/mcp", MCP_LIST)])
def test_f1_simple_request_body_types_are_415(csrf_srv, target, body):
    for headers in ({"Content-Type": "text/plain"},
                    {"Content-Type": "application/x-www-form-urlencoded"},
                    {}):
        r = csrf_srv.post(target, body, headers=headers)
        assert r.status == 415, (headers, r.status, r.body)
        assert b"Content-Type must be" in r.body, r.body


def test_f1_rpc_cross_site_origin_is_403_with_the_reason(csrf_srv):
    r = csrf_srv.post("/rpc/echo", b'{"a": 1}', headers={
        "Content-Type": "application/json", "Origin": "https://evil.example"})
    assert r.status == 403, r.body
    assert r.json()["ok"] is False
    assert "https://evil.example" in r.json()["error"]


def test_f1_mcp_cross_site_origin_is_403_as_a_json_rpc_error(csrf_srv):
    r = csrf_srv.post("/mcp", MCP_LIST, headers={
        "Content-Type": "application/json", "Origin": "null"})
    assert r.status == 403, r.body
    err = r.json()["error"]
    assert err["code"] == -32600 and "Origin" in err["message"], err


@pytest.mark.parametrize("origin", [None, "same-host", "https://app.example.com"])
def test_f1_no_origin_same_host_and_trusted_origins_are_admitted(csrf_srv, origin):
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if origin == "same-host":
        headers["Origin"] = f"http://127.0.0.1:{csrf_srv.port}"
    elif origin is not None:
        headers["Origin"] = origin
    r = csrf_srv.post("/rpc/echo", b'{"a": 1}', headers=headers)
    assert r.status == 200, r.body
    assert r.json() == {"ok": True, "result": {"a": 1}}
    r = csrf_srv.post("/mcp", MCP_LIST, headers=headers)
    assert r.status == 200, r.body
    assert [t["name"] for t in r.json()["result"]["tools"]] == ["ping"]


def test_f1_rpc_still_takes_msgpack(csrf_srv):
    msgpack = pytest.importorskip("msgpack")
    r = csrf_srv.post("/rpc/echo", msgpack.packb({"a": 2}), headers={
        "Content-Type": "application/msgpack", "Accept": "application/msgpack"})
    assert r.status == 200, r.body
    assert msgpack.unpackb(r.body) == {"ok": True, "result": {"a": 2}}


def test_f1_origin_policy_is_parsed_once_and_bad_entries_raise():
    from pyronova._csrf import Origin, OriginPolicy

    policy = OriginPolicy.of(["https://app.example.com", "http://localhost:3000"])
    assert policy.admits(None, "127.0.0.1:8000")
    assert policy.admits("https://app.example.com:443", "127.0.0.1:8000")
    assert policy.admits("http://localhost:3000", "127.0.0.1:8000")
    assert not policy.admits("http://localhost:3001", "127.0.0.1:8000")
    # Same host: Origin host:port == Host, a missing port meaning the scheme's default.
    assert policy.admits("http://api.example.com", "api.example.com")
    assert policy.admits("http://[::1]:8000", "[::1]:8000")
    assert not policy.admits("https://api.example.com", "api.example.com:80")
    assert not policy.admits("null", "api.example.com")
    assert Origin.parse("https://a.example/path") is None
    for bad in (["not an origin"], ["ftp://x.example"], ["https://x.example/p"], [3]):
        with pytest.raises(ValueError):
            OriginPolicy.of(bad)
    app = Pyronova()
    with pytest.raises(ValueError):
        app.trusted_origins = ["javascript:alert(1)"]
    app.trusted_origins = ["https://app.example.com"]
    assert app.trusted_origins == ["https://app.example.com:443"]


# ---------------------------------------------------------------------------
# F2: req.json() decodes with isojson; wide integers stay exact, floats round-trip
# ---------------------------------------------------------------------------

JSON_SCRIPT = """
from pyronova import Pyronova
app = Pyronova()

@app.post("/decode")
def decode(req):
    try:
        value = req.json()
    except ValueError as e:
        return {"error": type(e).__name__}
    return {"repr": repr(value)}
""" + RUN

WIDE = [2**64, 2**64 + 1, -(2**63) - 1, 10**40, -(10**30), 2**63 - 1, -(2**63)]
FLOATS = [0.1, 1e-300, 5e-324, 1.7976931348623157e308, 123456789.12345678, -0.0, 1.0]


@pytest.mark.parametrize("path", ["gil", "pool", "tpc"])
def test_f2_json_keeps_wide_integers_exact_and_floats_round_trip(path):
    doc = {"ints": WIDE, "floats": FLOATS, "text": "12345678901234567890123"}
    with serve(JSON_SCRIPT, path) as srv:
        r = srv.post("/decode", json.dumps(doc).encode(),
                     headers={"Content-Type": "application/json"})
        assert r.status == 200, r.body
        assert r.json()["repr"] == repr(doc)

        # No wide digit run: isojson's own path.
        narrow = {"floats": FLOATS, "a": [1, -3, 2**63 - 1], "b": None, "c": True}
        r = srv.post("/decode", json.dumps(narrow).encode())
        assert r.json()["repr"] == repr(narrow)

        for bad in (b"{bad", b"NaN", b"[1e400]", b"[12345678901234567890, NaN]"):
            r = srv.post("/decode", bad)
            assert r.json().get("error") in ("JSONDecodeError", "ValueError"), (bad, r.body)


@needs_pg
def test_f2_jsonb_column_keeps_wide_integers_exact():
    r = _run_script(f"""
        from pyronova.db import PgPool
        pool = PgPool.connect({PG_DSN!r})
        pool.execute("DROP TABLE IF EXISTS w4_json")
        pool.execute("CREATE TABLE w4_json (id int PRIMARY KEY, doc jsonb)")
        try:
            pool.execute(
                "INSERT INTO w4_json VALUES (1, $1::text::jsonb)",
                '{{"big": 18446744073709551616, "neg": -9223372036854775809, "f": 0.1}}',
            )
            doc = pool.fetch_one("SELECT doc FROM w4_json WHERE id = 1")["doc"]
            assert doc == {{"big": 2**64, "neg": -(2**63) - 1, "f": 0.1}}, doc
            assert type(doc["big"]) is int and type(doc["neg"]) is int, doc
            print("OK")
        finally:
            pool.execute("DROP TABLE w4_json")
    """)
    assert "OK" in r.stdout, (r.stdout, r.stderr)


# ---------------------------------------------------------------------------
# F5: @cached_json on a handler with path params
# ---------------------------------------------------------------------------

CACHE_SCRIPT = """
from pyronova import Pyronova
from pyronova.cache import cached_json
app = Pyronova()
CALLS = []

@app.get("/item/{item_id}")
@cached_json(ttl=60)
def item(req, item_id):
    CALLS.append(item_id)
    return {"id": item_id, "calls": len(CALLS)}

@app.get("/aitem/{item_id}")
@cached_json(ttl=60)
async def aitem(req, item_id):
    return {"id": item_id}
""" + RUN


@pytest.mark.parametrize("path", ["gil", "tpc"])
def test_f5_cached_json_with_path_params_serves_and_caches(path):
    with serve(CACHE_SCRIPT, path, workers=1) as srv:
        first = srv.get("/item/7")
        assert first.status == 200, first.body
        assert first.json() == {"id": "7", "calls": 1}
        assert srv.get("/item/7").json() == first.json()  # a hit: the handler didn't run
        assert srv.get("/item/8").json()["id"] == "8"
        if path == "gil":  # async routes under TPC are W1's item (Q1)
            r = srv.get("/aitem/9")
            assert r.status == 200, r.body
            assert r.json() == {"id": "9"}


def test_f5_a_wrapper_that_drops_the_path_params_is_a_registration_error():
    import functools

    def swallowing(fn):
        @functools.wraps(fn)
        def wrapper(req):  # can't take item_id, though fn declares it
            return fn(req)
        return wrapper

    app = Pyronova()
    with pytest.raises(TypeError, match="item_id"):
        @app.get("/x/{item_id}")
        @swallowing
        def handler(req, item_id):
            return item_id

    def passing(fn):
        @functools.wraps(fn)
        def wrapper(req, **params):
            return fn(req, **params)
        return wrapper

    @app.get("/y/{item_id}")
    @passing
    def ok(req, item_id):
        return item_id


# ---------------------------------------------------------------------------
# F7: one bounded "drive a sync or async callable" helper
# ---------------------------------------------------------------------------


def test_f7_hung_check_runs_on_one_daemon_thread_however_often_it_is_probed(monkeypatch):
    import pyronova.health as health

    monkeypatch.setattr(health, "_CHECK_TIMEOUT_S", 0.2)
    release = threading.Event()
    check = health.ReadinessCheck.of("hangs", lambda: release.wait(30))
    before = {t.ident for t in threading.enumerate()}
    try:
        for _ in range(5):
            ok, results = health._run_checks_sync([check], "rid-w4")
            assert ok is False and results == {"hangs": {"ok": False}}
        started = [
            t for t in threading.enumerate()
            if t.ident not in before and t.name.startswith("pyronova-bounded")
        ]
        assert len(started) == 1, started
        assert started[0].daemon
    finally:
        release.set()


def test_f7_a_hung_call_does_not_block_interpreter_exit():
    r = _run_script("""
        import time
        from pyronova._bounded import call_with_timeout
        try:
            call_with_timeout(lambda: time.sleep(120), 0.2)
        except TimeoutError:
            print("timed out")
    """, timeout=20)
    assert r.returncode == 0 and "timed out" in r.stdout, (r.stdout, r.stderr)


def test_f7_helper_works_from_inside_a_running_loop():
    from pyronova._bounded import call_with_timeout
    from pyronova.mcp import MCPServer

    async def slow_double(x: int) -> int:
        await asyncio.sleep(0.01)
        return 2 * x

    mcp = MCPServer()
    mcp.tool(slow_double)

    async def main():
        direct = call_with_timeout(lambda: slow_double(4), 5)
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                           "params": {"name": "slow_double", "arguments": {"x": 5}}})
        return direct, json.loads(mcp.handle_request(body))

    direct, resp = asyncio.run(main())
    assert direct == 8
    assert resp["result"]["content"][0]["text"] == "10", resp


def test_f7_readiness_fails_only_on_raise_timeout_or_false(monkeypatch):
    import pyronova.health as health

    monkeypatch.setattr(health, "_CHECK_TIMEOUT_S", 0.3)

    async def async_false():
        return False

    def boom():
        raise RuntimeError("down")

    checks = [health.ReadinessCheck.of(name, fn) for name, fn in [
        ("none", lambda: None), ("zero", lambda: 0), ("empty", lambda: ""),
        ("true", lambda: True), ("false", lambda: False), ("async-false", async_false),
        ("raises", boom), ("slow", lambda: time.sleep(2)),
    ]]
    ok, results = health._run_checks_sync(checks, "rid-w4")
    assert ok is False
    assert {n for n, r in results.items() if r["ok"]} == {"none", "zero", "empty", "true"}


# ---------------------------------------------------------------------------
# MCP: client field types checked before use; prompts sent as JSON
# ---------------------------------------------------------------------------


@pytest.fixture
def mcp():
    from pyronova.mcp import MCPServer

    server = MCPServer()

    @server.tool()
    def add(a: int, b: int) -> int:
        return a + b

    @server.resource("config://app")
    def config():
        return {"v": 1}

    @server.prompt("plan")
    def plan(goal: str):
        return {"goal": goal, "steps": [1, 2]}

    return server


def _call(server, payload):
    return json.loads(server.handle_request(json.dumps(payload)))


@pytest.mark.parametrize("payload,code,words", [
    ({"jsonrpc": "2.0", "id": 1, "method": ["tools/list"]}, -32600, "method must be a string"),
    ({"jsonrpc": "2.0", "id": 1, "method": {"a": 1}}, -32600, "method must be a string"),
    ({"jsonrpc": "2.0", "id": 1}, -32600, "method must be a string, got nothing"),
    ({"jsonrpc": "2.0", "id": [1], "method": "tools/list"}, -32600, "id must be"),
    ({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": ["add"]}},
     -32602, "'name' must be a string"),
    ({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {}},
     -32602, "'name' must be a string, got nothing"),
    ({"jsonrpc": "2.0", "id": 1, "method": "prompts/get", "params": {"name": {"x": 1}}},
     -32602, "'name' must be a string"),
    ({"jsonrpc": "2.0", "id": 1, "method": "resources/read", "params": {"uri": ["a"]}},
     -32602, "'uri' must be a string"),
])
def test_mcp_field_types_are_checked_before_use(mcp, payload, code, words):
    resp = _call(mcp, payload)
    assert resp["error"]["code"] == code, resp
    assert words in resp["error"]["message"], resp


def test_mcp_prompt_result_is_sent_as_json(mcp):
    resp = _call(mcp, {"jsonrpc": "2.0", "id": 1, "method": "prompts/get",
                       "params": {"name": "plan", "arguments": {"goal": "ship"}}})
    text = resp["result"]["messages"][0]["content"]["text"]
    assert json.loads(text) == {"goal": "ship", "steps": [1, 2]}, text


# ---------------------------------------------------------------------------
# Small items: crud id_type, RPCClient result, DbError::Connect sqlstate
# ---------------------------------------------------------------------------

CRUD_SCRIPT = """
import os
from pyronova import Pyronova
from pyronova.crud import register_crud
from pyronova.db import PgPool

pool = PgPool.connect(os.environ["PYRONOVA_TEST_PG_DSN"])
pool.execute("CREATE TABLE IF NOT EXISTS w4_crud_items (id int PRIMARY KEY, name text)")

def item_id(raw):
    if raw == "broken":
        raise RuntimeError("w4-converter-bug")
    return int(raw)

app = Pyronova()
register_crud(app, pool, prefix="/items", table="w4_crud_items",
              columns=["id", "name"], id_type=item_id)
""" + RUN


@needs_pg
def test_crud_only_type_and_value_errors_from_id_type_are_400():
    try:
        with serve(CRUD_SCRIPT, "gil") as srv:
            r = srv.get("/items/abc")
            assert r.status == 400, r.body
            assert r.json()["error"].startswith("invalid id: "), r.body
            r = srv.get("/items/broken")
            assert r.status == 500, r.body
            assert b"w4-converter-bug" not in r.body
            assert r.json()["request_id"]
            assert srv.get("/items/1").status == 404
    finally:
        _run_script(f"""
            from pyronova.db import PgPool
            PgPool.connect({PG_DSN!r}).execute("DROP TABLE IF EXISTS w4_crud_items")
        """)


def test_rpc_client_raises_when_an_ok_reply_has_no_result():
    pytest.importorskip("httpx")
    from pyronova.rpc import RPCClient

    app = Pyronova()

    @app.post("/rpc/partial")
    def partial(req):
        return {"ok": True}

    @app.post("/rpc/full")
    def full(req):
        return {"ok": True, "result": None}

    with TestClient(app, mode="gil") as c, RPCClient(c.base_url, use_msgpack=False) as rpc:
        with pytest.raises(RuntimeError, match="no 'result'"):
            rpc.partial()
        assert rpc.full() is None


@needs_pg
def test_db_connect_error_carries_sqlstate():
    base = PG_DSN.rsplit("/", 1)[0]
    r = _run_script(f"""
        from pyronova.db import PgPool
        try:
            PgPool.connect({base + "/w4_no_such_database"!r})
        except ConnectionError as e:
            print("sqlstate", e.sqlstate)
    """)
    assert "sqlstate 3D000" in r.stdout, (r.stdout, r.stderr)

    r = _run_script("""
        from pyronova.db import PgPool
        try:
            PgPool.connect("postgres://nobody@127.0.0.1:1/none", acquire_timeout_secs=2)
        except ConnectionError as e:
            print("sqlstate", e.sqlstate)
    """)
    assert "sqlstate None" in r.stdout, (r.stdout, r.stderr)


# ---------------------------------------------------------------------------
# G1: compression per app, validated, large bodies compressed
# ---------------------------------------------------------------------------


def test_g1_out_of_range_levels_and_no_algorithm_raise():
    from pyronova import Compression

    app = Pyronova()
    for kwargs in ({"gzip_level": 0}, {"gzip_level": 10}, {"brotli_quality": 12},
                   {"brotli_quality": -1}, {"min_size": -1}, {"gzip": False, "brotli": False}):
        with pytest.raises(ValueError):
            app.enable_compression(**kwargs)
        with pytest.raises(ValueError):
            Compression(**kwargs)
    settings = Compression(gzip_level=9, brotli_quality=11, gzip=False)
    assert (settings.gzip, settings.brotli, settings.gzip_level, settings.brotli_quality) == (
        False, True, 9, 11)


def _text_app(size: int) -> Pyronova:
    app = Pyronova()

    @app.get("/text", gil=True)
    def text(req):
        return Response(body="w4 " * size, content_type="text/plain")

    return app


def test_g1_compression_is_per_app():
    compressed, plain = _text_app(1000), _text_app(1000)
    compressed.enable_compression(min_size=16, brotli=False)
    with TestClient(compressed, mode="gil") as a:
        on = a.get("/text", headers={"Accept-Encoding": "gzip"})
    with TestClient(plain, mode="gil") as b:
        off = b.get("/text", headers={"Accept-Encoding": "gzip"})
    assert on.headers.get("content-encoding") == "gzip", on.headers
    assert gzip.decompress(on.body) == b"w4 " * 1000
    assert "content-encoding" not in off.headers, off.headers
    assert off.body == b"w4 " * 1000


def test_g1_large_body_compresses_correctly():
    app = _text_app(200_000)  # 600 KB: compressed off the I/O thread
    app.enable_compression(brotli=False)
    with TestClient(app, mode="gil") as c:
        r = c.get("/text", headers={"Accept-Encoding": "gzip"})
    assert r.headers.get("content-encoding") == "gzip", r.headers
    assert gzip.decompress(r.body) == b"w4 " * 200_000


# ---------------------------------------------------------------------------
# G5: the static-file cache revalidates on (mtime, len)
# ---------------------------------------------------------------------------


def test_g5_edited_static_file_is_served_fresh(tmp_path):
    page = tmp_path / "page.txt"
    page.write_text("first version")
    app = Pyronova()
    app.static("/static", str(tmp_path))
    with TestClient(app, mode="gil") as c:
        assert c.get("/static/page.txt").text == "first version"
        assert c.get("/static/page.txt").text == "first version"  # now cached
        page.write_text("second, longer version")
        assert c.get("/static/page.txt").text == "second, longer version"
        page.unlink()
        assert c.get("/static/page.txt").status_code == 404


# ---------------------------------------------------------------------------
# Logging: one level parser, one handler class
# ---------------------------------------------------------------------------


def test_logging_level_is_typed_and_parsed_when_given():
    from pyronova import LogLevel

    app = Pyronova()
    with pytest.raises(ValueError, match="verbose"):
        app.enable_logging(level="verbose")
    with pytest.raises(ValueError):
        Pyronova(log_config={"level": "loud"})
    app.enable_logging(level=LogLevel.Warn)
    assert app._log_config["level"] == LogLevel.Warn
    app.enable_logging(level="error")
    assert app._log_config["level"] == LogLevel.Error
    assert LogLevel.parse("warning") == LogLevel.Warn


def test_logging_main_root_level_comes_from_the_engine():
    r = _run_script("""
        import logging
        from pyronova.engine import LogLevel, init_logger
        from pyronova.app import _setup_python_logging_bridge
        init_logger(LogLevel.Warn, False, "text")
        _setup_python_logging_bridge()
        _setup_python_logging_bridge()
        root = logging.getLogger()
        names = [type(h).__qualname__ for h in root.handlers]
        print("level", root.level, "handlers", names.count("RustLogHandler"))
    """)
    assert "level 30 handlers 1" in r.stdout, (r.stdout, r.stderr)


LOG_WORKER_SCRIPT = """
import logging
from pyronova import Pyronova
app = Pyronova()
app.enable_logging(level="warn")

@app.get("/log-setup")
def log_setup(req):
    root = logging.getLogger()
    return {"level": root.level, "handlers": [type(h).__qualname__ for h in root.handlers]}
""" + RUN


def test_logging_worker_uses_the_same_handler_and_level():
    with serve(LOG_WORKER_SCRIPT, "pool") as srv:
        r = srv.get("/log-setup")
        assert r.status == 200, r.body
        assert r.json() == {"level": 30, "handlers": ["RustLogHandler"]}


# ---------------------------------------------------------------------------
# Structure and hot path
# ---------------------------------------------------------------------------


def test_request_maps_are_fresh_dicts_built_from_the_cached_parse():
    app = Pyronova()

    @app.get("/q/{name}", gil=True)
    def q(req, name):
        first = req.query_params
        first["a"] = "mutated"
        params = req.params
        params["name"] = "mutated"
        all_values = req.query_params_all()
        return {
            "a": req.query_params["a"],
            "one": req.query_param("a"),
            "missing": req.query_param("zzz"),
            "all": all_values["a"],
            "name": req.params["name"],
            "fresh": first is not req.query_params,
        }

    with TestClient(app, mode="gil") as c:
        r = c.get("/q/bob?a=1&a=2&b=x")
    assert r.json() == {"a": "1", "one": "1", "missing": None, "all": ["1", "2"],
                        "name": "bob", "fresh": True}


def test_shared_state_reads_return_str_and_bytes():
    from pyronova import SharedState

    state = SharedState()
    state["k"] = "välue"
    state.set_bytes("raw", b"\xff\x00")
    assert state["k"] == "välue" and state.get("k") == "välue"
    assert state.get("nope", "dflt") == "dflt" and state.get("nope") is None
    assert state.get_bytes("raw") == b"\xff\x00" and state.get_bytes("nope") is None
    assert dict(state.items())["k"] == "välue"
    with pytest.raises(TypeError, match="raw"):
        state["raw"]


def test_sse_event_lines_split_on_every_line_ending():
    app = Pyronova()

    @app.get("/sse", gil=True)
    def sse(req):
        stream = Stream()
        stream.send_event("a\r\nb\rc\nd\n", event="e", id="1")
        stream.send_event("")
        stream.close()
        return stream

    with TestClient(app, mode="gil") as c:
        r = c.get("/sse")
    assert r.text == (
        "id: 1\nevent: e\ndata: a\ndata: b\ndata: c\ndata: d\n\n"
        "data: \n\n"
    ), repr(r.text)


def test_total_requests_is_none_when_metrics_are_off():
    env = {k: v for k, v in os.environ.items() if k != "PYRONOVA_METRICS"}
    r = subprocess.run(
        [PYTHON, "-c", "from pyronova import get_gil_metrics; print(get_gil_metrics().total_requests)"],
        capture_output=True, text=True, timeout=30, env=env,
    )
    assert r.stdout.strip() == "None", (r.stdout, r.stderr)


def test_routes_are_typed_records():
    from pyronova import FastRouteInfo, RouteInfo

    app = Pyronova()

    @app.get("/a/{x}", gil=True)
    async def a(req, x):
        return x

    app.add_fast_response("GET", "/health", b"ok")
    [route] = app.routes
    assert route == RouteInfo(method="GET", path="/a/{x}", handler=a.__qualname__,
                              gil=True, stream=False, model=None, is_async=True)
    assert app.fast_routes == [FastRouteInfo(method="GET", path="/health", status_code=200,
                                             content_type="text/plain", body_bytes=2)]
