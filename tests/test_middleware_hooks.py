"""Tests for before_request / after_request middleware hooks.

Covers the P2 fix: after_request hooks reuse the original request object
instead of rebuilding it, so req attributes must be fully intact.
"""

import pytest
from pyronova import Pyronova, Response
from pyronova.testing import TestClient


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()


# before_request: inject custom header
@app.before_request
def inject_timing(req):
    # Return None to continue — just verify req fields are accessible
    _ = req.method
    _ = req.path
    _ = req.client_ip
    return None


# after_request: add response header using req info
@app.after_request
def add_request_info(req, resp):
    headers = dict(getattr(resp, "headers", {}) or {})
    headers["x-handled-path"] = req.path
    headers["x-handled-method"] = req.method
    headers["x-client-ip"] = req.client_ip
    return Response(
        body=resp.body,
        status_code=resp.status_code,
        content_type=resp.content_type,
        headers=headers,
    )


@app.get("/")
def index(req):
    return {"ok": True}


@app.get("/user/{name}")
def user(req):
    return {"name": req.params["name"]}


@app.post("/echo")
def echo(req):
    return req.json()


@app.get("/query")
def query(req):
    return {"q": req.query_params.get("q", "")}


@app.get("/headers-echo")
def headers_echo(req):
    return {"ua": req.headers.get("user-agent", "none")}


@pytest.fixture(scope="module")
def client():
    c = TestClient(app, port=19883)
    yield c
    c.close()


def test_after_hook_sees_correct_path(client):
    """after_request hook should see the original request path."""
    resp = client.get("/")
    path = None
    for k, v in resp.headers.items():
        if k.lower() == "x-handled-path":
            path = v.strip()
    assert path == "/"


def test_after_hook_sees_correct_method(client):
    """after_request hook should see the original request method."""
    resp = client.post("/echo", body={"x": 1})
    method = None
    for k, v in resp.headers.items():
        if k.lower() == "x-handled-method":
            method = v.strip()
    assert method == "POST"


def test_after_hook_sees_client_ip(client):
    """after_request hook should have access to client_ip."""
    resp = client.get("/")
    ip = None
    for k, v in resp.headers.items():
        if k.lower() == "x-client-ip":
            ip = v.strip()
    assert ip in ("127.0.0.1", "::1")


def test_after_hook_with_path_params(client):
    """after_request hook should work correctly with parameterized routes."""
    resp = client.get("/user/alice")
    assert resp.status_code == 200
    assert resp.json()["name"] == "alice"
    path = None
    for k, v in resp.headers.items():
        if k.lower() == "x-handled-path":
            path = v.strip()
    assert path == "/user/alice"


def test_after_hook_with_query(client):
    """after_request hook should not interfere with query params."""
    resp = client.get("/query?q=test")
    assert resp.json()["q"] == "test"


def test_after_hook_preserves_status_code(client):
    """after_request hook should preserve the original status code."""
    resp = client.get("/")
    assert resp.status_code == 200


def test_before_hook_passes_through(client):
    """before_request returning None should allow handler to execute."""
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_before_hook_short_circuit():
    """before_request returning a response should short-circuit."""
    # Its own module: a worker serves one app per module.
    from tests.apps.middleware_short_circuit import app

    c = TestClient(app, port=19884)
    try:
        # Without token — should get 401
        resp = c.get("/protected")
        assert resp.status_code == 401
        assert resp.text == "unauthorized"

        # With token — should pass through
        resp = c.get("/protected", headers={"x-token": "valid"})
        assert resp.status_code == 200
        assert resp.json()["secret"] == "data"
    finally:
        c.close()


def test_multiple_after_hooks():
    """Multiple after_request hooks should chain correctly."""
    # Its own module: a worker serves one app per module.
    from tests.apps.middleware_two_after_hooks import app

    c = TestClient(app, port=19885)
    try:
        resp = c.get("/")
        assert resp.status_code == 200
        h1 = h2 = None
        for k, v in resp.headers.items():
            if k.lower() == "x-hook-1":
                h1 = v.strip()
            if k.lower() == "x-hook-2":
                h2 = v.strip()
        assert h1 == "yes"
        assert h2 == "yes"
    finally:
        c.close()
