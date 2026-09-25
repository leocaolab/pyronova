"""Tests for RPC system — server-side @app.rpc() + RPCClient."""

import json
import pytest
from pyronova import Pyronova
from pyronova.testing import TestClient


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()


@app.get("/")
def health(req):
    return {"ok": True}


@app.rpc("/rpc/add")
def add(data):
    return {"sum": data["a"] + data["b"]}


@app.rpc("/rpc/echo")
def echo(req, data):
    """Handler that takes (req, data) — 2-arg form."""
    return {"echoed": data, "method": req.method}


@app.rpc("/rpc/error")
def fail(data):
    raise ValueError("intentional error")


@pytest.fixture(scope="module")
def client():
    c = TestClient(app, port=19878)
    yield c
    c.close()


def test_rpc_json_basic(client):
    """JSON RPC: send dict, get envelope back."""
    resp = client.post(
        "/rpc/add",
        body=json.dumps({"a": 3, "b": 5}).encode(),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["result"]["sum"] == 8


def test_rpc_two_arg_handler(client):
    """Handler that takes (req, data)."""
    resp = client.post(
        "/rpc/echo",
        body=json.dumps({"hello": "world"}).encode(),
        headers={"Content-Type": "application/json"},
    )
    data = resp.json()
    assert data["ok"] is True
    assert data["result"]["echoed"]["hello"] == "world"
    assert data["result"]["method"] == "POST"


def test_rpc_error_envelope(client):
    """Handler that raises → {"ok": false, "error": ...}."""
    resp = client.post(
        "/rpc/error",
        body=b"{}",
        headers={"Content-Type": "application/json"},
    )
    data = resp.json()
    assert data["ok"] is False
    assert "intentional error" in data["error"]


def test_rpc_empty_body(client):
    """Empty body → handler receives empty dict."""
    resp = client.post(
        "/rpc/add",
        body=b"",
        headers={"Content-Type": "application/json"},
    )
    # Will error because {} has no "a" key — but should return error envelope, not crash
    data = resp.json()
    assert data["ok"] is False


def test_rpc_msgpack(client):
    """MsgPack encoding if available."""
    try:
        import msgpack
    except ImportError:
        pytest.skip("msgpack not installed")

    payload = msgpack.packb({"a": 10, "b": 20}, use_bin_type=True)
    resp = client.post(
        "/rpc/add",
        body=payload,
        headers={
            "Content-Type": "application/msgpack",
            "Accept": "application/msgpack",
        },
    )
    assert resp.status_code == 200
    data = msgpack.unpackb(resp.body, raw=False)
    assert data["ok"] is True
    assert data["result"]["sum"] == 30
