"""Regression tests for the cced8c2 review, milestone M1a (Python package).

Each test fails on cced8c2 and passes after the fix it names.
"""

from __future__ import annotations

import ast
import json
import logging
import os
import stat
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pyronova import Pyronova
from pyronova.cookies import set_cookie
from pyronova.engine import Response
from pyronova.mcp import MCPServer
from pyronova.testing import TestClient
from pyronova.uploads import parse_multipart


# ---------------------------------------------------------------------------
# M1a-1 / M1a-10: multipart parser
# ---------------------------------------------------------------------------


class _MultipartRequest:
    def __init__(self, body: bytes, boundary: str = "XyZ"):
        self.headers = {"content-type": f"multipart/form-data; boundary={boundary}"}
        self.body = body


def _multipart(*parts: tuple[str, bytes], nl: bytes = b"\r\n") -> bytes:
    out = b""
    for name, data in parts:
        out += b"--XyZ" + nl
        out += f'Content-Disposition: form-data; name="{name}"; filename="{name}.txt"'.encode() + nl
        out += nl + data + nl
    return out + b"--XyZ--" + nl


@pytest.mark.parametrize("data", [b"line1\r\n", b"a\r\n\r\n", b"\r\n", b"ends-with-lf\n"])
def test_multipart_keeps_trailing_newlines_of_file_data(data):
    form = parse_multipart(_MultipartRequest(_multipart(("f", data), ("g", b"x"))))
    assert form["f"].data == data
    assert form["g"].data == b"x"


def test_multipart_lf_framing_keeps_trailing_newline():
    form = parse_multipart(_MultipartRequest(_multipart(("f", b"text\n"), nl=b"\n")))
    assert form["f"].data == b"text\n"


def test_multipart_without_delimiter_raises():
    from pyronova.uploads import MultipartError

    with pytest.raises(MultipartError, match="XyZ"):
        parse_multipart(_MultipartRequest(b"no multipart framing here"))


def test_multipart_truncated_body_raises():
    from pyronova.uploads import MultipartError

    body = b'--XyZ\r\nContent-Disposition: form-data; name="f"\r\n\r\npartial data'
    with pytest.raises(MultipartError, match="closing"):
        parse_multipart(_MultipartRequest(body))


# ---------------------------------------------------------------------------
# M1a-2: cookies
# ---------------------------------------------------------------------------


def test_set_cookie_expires_takes_datetime_and_formats_http_date():
    when = datetime(2026, 10, 21, 7, 28, 0, tzinfo=timezone.utc)
    header = set_cookie(Response(body="ok"), "sid", "v", expires=when).headers["set-cookie"]
    assert "Expires=Wed, 21 Oct 2026 07:28:00 GMT" in header


def test_set_cookie_expires_converts_to_gmt():
    when = datetime(2026, 10, 21, 9, 28, 0, tzinfo=timezone(timedelta(hours=2)))
    header = set_cookie(Response(body="ok"), "sid", "v", expires=when).headers["set-cookie"]
    assert "Expires=Wed, 21 Oct 2026 07:28:00 GMT" in header


def test_set_cookie_expires_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone"):
        set_cookie(Response(body="ok"), "sid", "v", expires=datetime(2026, 10, 21))


def test_set_cookie_samesite_is_an_enum():
    from pyronova.cookies import SameSite

    header = set_cookie(
        Response(body="ok"), "sid", "v", samesite=SameSite.STRICT
    ).headers["set-cookie"]
    assert "SameSite=Strict" in header
    with pytest.raises(ValueError, match="Sideways"):
        set_cookie(Response(body="ok"), "sid", "v", samesite="Sideways")


# ---------------------------------------------------------------------------
# M1a-4: reloader
# ---------------------------------------------------------------------------


class _FakeProc:
    def __init__(self, returncode=None):
        self.returncode = returncode

    def poll(self):
        return self.returncode

    def terminate(self):
        pass

    def kill(self):
        pass

    def wait(self, timeout=None):
        return self.returncode


def _write_module(directory: Path, name: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{name}.py").write_text(
        "from pyronova import Pyronova\napp = Pyronova()\n"
        "@app.get('/')\ndef index(req):\n    return 'ok'\n"
    )


@pytest.fixture
def _restore_modules():
    path, mods = list(sys.path), set(sys.modules)
    yield
    sys.path[:] = path
    for m in set(sys.modules) - mods:
        del sys.modules[m]


def test_dev_reloader_reexecs_the_real_command_and_watches_the_app_dir(
    tmp_path, monkeypatch, _restore_modules
):
    import watchfiles
    from pyronova.cli import main

    app_dir = tmp_path / "proj"
    _write_module(app_dir, "m1a_dev_app")
    monkeypatch.syspath_prepend(str(app_dir))
    command = [sys.executable, "/venv/bin/pyronova", "dev", "m1a_dev_app"]
    monkeypatch.setattr(sys, "orig_argv", command)
    monkeypatch.setattr(sys, "argv", command[1:])
    monkeypatch.setenv("PYRONOVA_LOG", "0")
    monkeypatch.delenv("_PYRONOVA_RELOAD_CHILD", raising=False)

    spawned, watched = [], []
    monkeypatch.setattr(subprocess, "Popen", lambda argv, **kw: spawned.append(argv) or _FakeProc())

    def _watch(path, **kw):
        watched.append(path)
        raise KeyboardInterrupt

    monkeypatch.setattr(watchfiles, "watch", _watch)

    main(["dev", "m1a_dev_app"])

    assert spawned == [command]
    assert [os.path.realpath(p) for p in watched] == [os.path.realpath(app_dir)]


class _Restarted(Exception):
    pass


def test_polling_reloader_restarts_after_the_child_crashes(tmp_path, monkeypatch):
    script = tmp_path / "server.py"
    script.write_text("print('v1')\n")
    monkeypatch.setitem(sys.modules, "watchfiles", None)  # force the polling reloader
    monkeypatch.setattr(sys, "orig_argv", [sys.executable, str(script)])
    monkeypatch.setattr(sys, "argv", [str(script)])
    monkeypatch.setattr(sys.modules["__main__"], "__file__", str(script), raising=False)
    monkeypatch.delenv("_PYRONOVA_RELOAD_CHILD", raising=False)

    spawned = []

    def _popen(argv, **kw):
        spawned.append(argv)
        if len(spawned) == 1:
            script.write_text("print('v2')\n")  # the fix the developer saves
            return _FakeProc(returncode=1)  # the child crashed on start
        raise _Restarted

    monkeypatch.setattr(subprocess, "Popen", _popen)

    with pytest.raises(_Restarted):
        Pyronova().run(reload=True)
    assert spawned[0] == [sys.executable, str(script)]


# ---------------------------------------------------------------------------
# M1a-5 / M1a-6: isolation clone dir
# ---------------------------------------------------------------------------

_BOOTSTRAP = Path(__file__).resolve().parent.parent / "python" / "pyronova" / "_bootstrap.py"


def _bootstrap_ns(**extra):
    """The top-level functions and classes of _bootstrap.py (it runs inside
    workers and can't be imported as a module), in a fresh namespace."""
    ns = {"_os": os, "_logging": logging, "_ISO": {"worker_dir": None, "path_inserted": False, "isolated": set()}}
    for node in ast.parse(_BOOTSTRAP.read_text()).body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            exec(compile(ast.Module([node], []), str(_BOOTSTRAP), "exec"), ns)
    ns.update(extra)
    return ns


_unix_only = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin") or os.getuid() == 0,
    reason="cp -c / --reflink and permission checks (root bypasses them)",
)


@_unix_only
def test_isolate_clone_failure_raises_with_cp_stderr(tmp_path):
    src = tmp_path / "fakelib"
    src.mkdir()
    (src / "__init__.py").write_text("")
    worker_dir = tmp_path / "w0"
    worker_dir.mkdir()
    worker_dir.chmod(0o500)  # cp cannot create the clone here
    ns = _bootstrap_ns(_iso_resolve_src=lambda lib: str(src))
    try:
        with pytest.raises(Exception, match="Permission denied"):
            ns["_iso_clone_lib"]("fakelib", str(worker_dir), {})
    finally:
        worker_dir.chmod(0o700)


@_unix_only
def test_isolate_default_root_is_private_to_this_user(tmp_path, monkeypatch):
    monkeypatch.delenv("PYRONOVA_ISOLATE_DIR", raising=False)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    ns = _bootstrap_ns()

    worker_dir = ns["_iso_worker_dir"](seed_libs=())

    assert worker_dir.startswith(str(tmp_path) + os.sep)
    root = tmp_path / os.path.relpath(worker_dir, tmp_path).split(os.sep)[0]
    st = root.stat()
    assert st.st_uid == os.getuid()
    assert stat.S_IMODE(st.st_mode) == 0o700


@_unix_only
def test_isolate_root_writable_by_others_is_refused(tmp_path, monkeypatch):
    shared = tmp_path / "shared"
    shared.mkdir()
    shared.chmod(0o777)
    monkeypatch.setenv("PYRONOVA_ISOLATE_DIR", str(shared))
    ns = _bootstrap_ns()
    with pytest.raises(Exception, match="writable"):
        ns["_iso_worker_dir"](seed_libs=())


# ---------------------------------------------------------------------------
# M1a-7: model= composes with path-param injection
# ---------------------------------------------------------------------------


def test_model_route_also_injects_path_params():
    # Its own module: a worker serves one app per module.
    from tests.apps.m1a_model_path_params import app

    with TestClient(app) as c:
        r = c.put("/items/7", body={"name": "bolt"})
        assert r.status_code == 200, r.text
        assert r.json() == {"id": "7", "name": "bolt", "path": "/items/7"}

        r = c.post("/tags/red", body={"name": "nut"})
        assert r.status_code == 200, r.text
        assert r.json() == {"tag": "red", "name": "nut"}

        r = c.put("/items/7", body={"nom": "bolt"})
        assert r.status_code == 422
        assert r.json()["detail"][0]["loc"] == ["name"]


def test_model_route_signature_checked_at_registration():
    from pydantic import BaseModel

    class Item(BaseModel):
        name: str

    app = Pyronova()

    def bad(req, item, oops):
        return oops

    with pytest.raises(ValueError, match="oops"):
        app.post("/items/{item_id}", bad, model=Item)


def test_model_must_be_a_pydantic_model():
    app = Pyronova()
    with pytest.raises(TypeError, match="dict"):
        app.post("/items", lambda req, body: body, model=dict)


# ---------------------------------------------------------------------------
# M1a-8: MCP JSON-RPC error codes
# ---------------------------------------------------------------------------


@pytest.fixture
def mcp():
    server = MCPServer()

    @server.tool(description="Add two numbers")
    def add(a: int, b: int) -> int:
        return a + b

    @server.tool()
    def explode() -> str:
        raise RuntimeError("secret dsn postgres://u:p@db")

    @server.prompt("greet")
    def greet(name: str) -> str:
        return f"hi {name}"

    return server


def _rpc(server, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 7, "method": method, "params": params})
    return json.loads(server.handle_request(body))


def test_mcp_invalid_params_is_32602_with_the_validation_message(mcp):
    resp = _rpc(mcp, "tools/call", {"name": "add", "arguments": {"a": 1}})
    assert resp["error"]["code"] == -32602
    assert "missing required argument(s): ['b']" in resp["error"]["message"]

    resp = _rpc(mcp, "prompts/get", {"name": "greet", "arguments": []})
    assert resp["error"]["code"] == -32602
    assert "must be an object" in resp["error"]["message"]


def test_mcp_unknown_tool_is_32602(mcp):
    resp = _rpc(mcp, "tools/call", {"name": "nope"})
    assert resp["error"] == {"code": -32602, "message": "Unknown tool: nope"}


def test_mcp_unknown_resource_is_32002(mcp):
    resp = _rpc(mcp, "resources/read", {"uri": "config://missing"})
    assert resp["error"] == {"code": -32002, "message": "Resource not found: config://missing"}


def test_mcp_unexpected_exception_is_32603_internal_error(mcp):
    resp = _rpc(mcp, "tools/call", {"name": "explode", "arguments": {}})
    assert resp["error"] == {"code": -32603, "message": "Internal error"}


def test_mcp_undecodable_body_is_a_parse_error(mcp):
    resp = json.loads(mcp.handle_request(b"\xff\xfe{"))
    assert resp["error"]["code"] == -32700


# ---------------------------------------------------------------------------
# M1a-10: smaller items
# ---------------------------------------------------------------------------


def test_mcp_schema_with_unresolvable_hint_fails_at_registration():
    server = MCPServer()

    def tool(x: "NoSuchType") -> str:  # noqa: F821
        return x

    with pytest.raises(TypeError, match="NoSuchType"):
        server.tool(tool)


def test_rpc_and_mcp_routes_are_listed_in_app_routes():
    # Its own module: a worker serves one app per module.
    from tests.apps.m1a_rpc_and_mcp import app

    with TestClient(app):
        paths = {(r["method"], r["path"]) for r in app.routes}
    assert ("POST", "/rpc/echo") in paths
    assert ("POST", "/mcp") in paths


class _FakePool:
    def fetch_one(self, sql, *args):
        return {"id": 1, "name": "x"}

    def fetch_all(self, sql, *args):
        return []

    def execute(self, sql, *args):
        return 1


def test_crud_client_bad_json_is_400_with_the_parse_error_not_logged_as_error(caplog):
    from pyronova.crud import register_crud

    app = Pyronova()
    register_crud(app, _FakePool(), prefix="/items", table="items", columns=["id", "name"])

    @app.get("/")
    def index(req):
        return "ok"

    caplog.set_level(logging.DEBUG, logger="pyronova.crud")
    # caplog sees only the main interpreter's log records.
    with TestClient(app, mode="gil") as c:
        r = c.post("/items", body=b"{not json", headers={"Content-Type": "application/json"})
    assert r.status_code == 400
    assert r.json()["error"].startswith("invalid JSON: ")
    assert r.json()["error"] != "invalid JSON: "
    crud_records = [rec for rec in caplog.records if rec.name == "pyronova.crud"]
    # Capture works (the rejection is logged, at INFO), so "no ERROR record" means something.
    assert [rec.levelno for rec in crud_records if "rejected request body" in rec.getMessage()] == [
        logging.INFO
    ], crud_records
    assert not [rec for rec in crud_records if rec.levelno >= logging.ERROR]
