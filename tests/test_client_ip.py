"""Tests for req.client_ip — verifies client IP is populated in all modes."""

import pytest
from pyronova import Pyronova, Response
from pyronova.testing import TestClient


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()


@app.get("/")
def index(req):
    return "ok"


@app.get("/ip")
def get_ip(req):
    return {"client_ip": req.client_ip}


@app.get("/ip-type")
def ip_type(req):
    return {"type": type(req.client_ip).__name__, "len": len(req.client_ip)}


@app.post("/ip-post")
def post_ip(req):
    return {"client_ip": req.client_ip, "method": req.method}


@app.get("/ip-with-params/{id}")
def ip_with_params(req):
    return {"client_ip": req.client_ip, "id": req.params["id"]}


@app.get("/ip-with-query")
def ip_with_query(req):
    return {"client_ip": req.client_ip, "q": req.query_params.get("q", "")}


@app.get("/ip-in-header-response")
def ip_echo(req):
    return Response(
        body="ok",
        headers={"x-client-ip": req.client_ip},
    )


@pytest.fixture(scope="module")
def client():
    c = TestClient(app)
    yield c
    c.close()


def test_client_ip_present(client):
    """client_ip should be a non-empty string for local connections."""
    resp = client.get("/ip")
    assert resp.status_code == 200
    data = resp.json()
    assert data["client_ip"] != ""
    assert data["client_ip"] in ("127.0.0.1", "::1")


def test_client_ip_is_string(client):
    """client_ip must be a string type."""
    resp = client.get("/ip-type")
    data = resp.json()
    assert data["type"] == "str"
    assert data["len"] > 0


def test_client_ip_on_post(client):
    """client_ip should work on POST requests too."""
    resp = client.post("/ip-post", body={"test": 1})
    data = resp.json()
    assert data["client_ip"] in ("127.0.0.1", "::1")
    assert data["method"] == "POST"


def test_client_ip_with_path_params(client):
    """client_ip should coexist with path params."""
    resp = client.get("/ip-with-params/42")
    data = resp.json()
    assert data["client_ip"] in ("127.0.0.1", "::1")
    assert data["id"] == "42"


def test_client_ip_with_query_params(client):
    """client_ip should coexist with query params."""
    resp = client.get("/ip-with-query?q=hello")
    data = resp.json()
    assert data["client_ip"] in ("127.0.0.1", "::1")
    assert data["q"] == "hello"


def test_client_ip_in_response_header(client):
    """Handler can use client_ip in response headers."""
    resp = client.get("/ip-in-header-response")
    assert resp.status_code == 200
    ip = None
    for k, v in resp.headers.items():
        if k.lower() == "x-client-ip":
            ip = v.strip()
    assert ip in ("127.0.0.1", "::1")


def test_client_ip_consistent_across_requests(client):
    """Multiple requests from same client should have same IP."""
    ips = []
    for _ in range(5):
        resp = client.get("/ip")
        ips.append(resp.json()["client_ip"])
    assert len(set(ips)) == 1  # All same IP
