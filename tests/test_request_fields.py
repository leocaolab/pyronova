"""Tests for Request field completeness and edge cases.

Verifies all request fields (method, path, params, query, headers,
client_ip, body) are correctly populated across different scenarios.
"""

import pytest
from pyronova import Pyronova, Response
from pyronova.testing import TestClient


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()


@app.get("/")
def index(req):
    return "ok"


@app.get("/fields")
def all_fields(req):
    return {
        "method": req.method,
        "path": req.path,
        "query": req.query,
        "client_ip": req.client_ip,
        "has_headers": len(req.headers) > 0,
        "params": req.params,
    }


@app.post("/fields")
def post_fields(req):
    return {
        "method": req.method,
        "path": req.path,
        "body_len": len(req.body),
        "text": req.text(),
        "client_ip": req.client_ip,
    }


@app.get("/user/{name}")
def user(req):
    return {"name": req.params["name"]}


@app.get("/a/{x}/b/{y}/c/{z}")
def triple_param(req):
    return {
        "x": req.params["x"],
        "y": req.params["y"],
        "z": req.params["z"],
    }


@app.get("/query-multi")
def query_multi(req):
    return {"raw": req.query, "parsed": req.query_params}


@app.put("/put-body")
def put_body(req):
    return {"method": req.method, "data": req.json()}


@app.patch("/patch-body")
def patch_body(req):
    return {"method": req.method, "data": req.json()}


@app.delete("/del/{id}")
def del_item(req):
    return {"method": req.method, "id": req.params["id"]}


@app.get("/empty-query")
def empty_query(req):
    return {"query": req.query, "params_count": len(req.query_params)}


@app.get("/headers-check")
def headers_check(req):
    return {
        "content_type": req.headers.get("content-type", "none"),
        "custom": req.headers.get("x-custom", "none"),
        "accept": req.headers.get("accept", "none"),
    }


@app.post("/headers-check")
def headers_check_post(req):
    return {
        "content_type": req.headers.get("content-type", "none"),
        "custom": req.headers.get("x-custom", "none"),
        "accept": req.headers.get("accept", "none"),
    }


@app.post("/binary-body")
def binary_body(req):
    return {"body_len": len(req.body), "is_bytes": isinstance(req.body, (bytes, memoryview))}


@app.get("/unicode-param/{name}")
def unicode_param(req):
    return {"name": req.params["name"]}


@pytest.fixture(scope="module")
def client():
    c = TestClient(app, port=19887)
    yield c
    c.close()


# --- Method tests ---

def test_get_method(client):
    resp = client.get("/fields")
    assert resp.json()["method"] == "GET"


def test_post_method(client):
    resp = client.post("/fields", body="hello")
    assert resp.json()["method"] == "POST"


def test_put_method(client):
    resp = client.put("/put-body", body={"x": 1})
    assert resp.json()["method"] == "PUT"


def test_patch_method(client):
    resp = client.patch("/patch-body", body={"x": 1})
    assert resp.json()["method"] == "PATCH"


def test_delete_method(client):
    resp = client.delete("/del/5")
    assert resp.json()["method"] == "DELETE"


# --- Path param tests ---

def test_single_path_param(client):
    resp = client.get("/user/alice")
    assert resp.json()["name"] == "alice"


def test_numeric_path_param(client):
    resp = client.get("/user/12345")
    assert resp.json()["name"] == "12345"


def test_triple_path_params(client):
    resp = client.get("/a/1/b/2/c/3")
    data = resp.json()
    assert data["x"] == "1"
    assert data["y"] == "2"
    assert data["z"] == "3"


def test_path_param_with_dash(client):
    resp = client.get("/user/foo-bar")
    assert resp.json()["name"] == "foo-bar"


def test_path_param_with_dot(client):
    resp = client.get("/user/file.txt")
    assert resp.json()["name"] == "file.txt"


def test_path_param_empty_on_non_param_route(client):
    resp = client.get("/fields")
    assert resp.json()["params"] == {}


# --- Query param tests ---

def test_query_string_raw(client):
    resp = client.get("/fields?foo=bar&baz=qux")
    assert "foo=bar" in resp.json()["query"]


def test_query_params_parsed(client):
    resp = client.get("/query-multi?a=1&b=2&c=3")
    data = resp.json()
    assert data["parsed"]["a"] == "1"
    assert data["parsed"]["b"] == "2"
    assert data["parsed"]["c"] == "3"


def test_empty_query(client):
    resp = client.get("/empty-query")
    data = resp.json()
    assert data["query"] == ""
    assert data["params_count"] == 0


def test_query_with_special_chars(client):
    resp = client.get("/query-multi?msg=hello+world&emoji=%F0%9F%94%A5")
    data = resp.json()
    assert data["parsed"]["msg"] == "hello world"


# --- Body tests ---

def test_post_body_text(client):
    resp = client.post("/fields", body="hello world")
    data = resp.json()
    assert data["body_len"] == 11
    assert data["text"] == "hello world"


def test_post_body_json(client):
    resp = client.put("/put-body", body={"key": "value", "num": 42})
    data = resp.json()
    assert data["data"]["key"] == "value"
    assert data["data"]["num"] == 42


def test_post_empty_body(client):
    resp = client.post("/fields", body="")
    data = resp.json()
    assert data["body_len"] == 0


def test_post_large_body(client):
    large = "x" * 10000
    resp = client.post("/fields", body=large)
    data = resp.json()
    assert data["body_len"] == 10000


# --- Headers tests ---

def test_custom_header(client):
    resp = client.get("/headers-check", headers={"X-Custom": "myvalue"})
    data = resp.json()
    assert data["custom"] == "myvalue"


def test_content_type_header(client):
    resp = client.post("/headers-check",
                       body="test",
                       headers={"Content-Type": "text/plain"})
    data = resp.json()
    assert data["content_type"] == "text/plain"


# --- Client IP tests ---

def test_client_ip_on_get(client):
    resp = client.get("/fields")
    assert resp.json()["client_ip"] in ("127.0.0.1", "::1")


def test_client_ip_on_post(client):
    resp = client.post("/fields", body="test")
    assert resp.json()["client_ip"] in ("127.0.0.1", "::1")


# --- Response type tests ---

def test_dict_response_json(client):
    resp = client.get("/user/test")
    assert resp.headers.get("Content-Type", resp.headers.get("content-type", "")).startswith("application/json")


def test_delete_with_path_param(client):
    resp = client.delete("/del/abc")
    data = resp.json()
    assert data["id"] == "abc"
    assert data["method"] == "DELETE"
