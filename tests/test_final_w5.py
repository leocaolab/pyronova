"""Final review, fix wave B, W5: docs/design/code-review-final-rubric.md.

The CSRF check of `/mcp` and `@app.rpc` compares `Origin` with the request's authority:
an HTTP/2 request may carry only `:authority`, no `Host`.
"""

from __future__ import annotations

import json

import pytest

from pyronova import Pyronova
from pyronova.engine import Request
from pyronova.testing import TestClient

httpx = pytest.importorskip("httpx")
pytest.importorskip("h2")

MCP_LIST = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode()


def _request(target: str, headers: dict[str, str]) -> Request:
    return Request("POST", target, {}, "", b"", headers, "127.0.0.1")


def test_authority_is_the_host_header_for_an_origin_form_target():
    assert _request("/rpc", {"host": "api.example.com:8443"}).authority == "api.example.com:8443"


def test_authority_of_an_absolute_form_target_wins_over_host():
    req = _request("http://api.example.com/rpc", {"host": "other.example"})
    assert req.authority == "api.example.com"


def test_authority_is_none_without_host_or_target_authority():
    assert _request("/rpc", {}).authority is None


@pytest.fixture(scope="module")
def h2_client():
    app = Pyronova()

    @app.rpc("/rpc/echo")
    def echo(data):
        return data

    @app.mcp.tool()
    def ping() -> str:
        return "pong"

    with TestClient(app, mode="gil") as c:
        with httpx.Client(http1=False, http2=True, timeout=10) as h2:
            yield c, h2


@pytest.mark.parametrize("target,body", [("/rpc/echo", b'{"a": 1}'), ("/mcp", MCP_LIST)])
def test_same_host_origin_over_http2_is_admitted(h2_client, target, body):
    c, h2 = h2_client
    r = h2.post(
        f"http://127.0.0.1:{c.port}{target}",
        content=body,
        headers={"Content-Type": "application/json", "Origin": f"http://127.0.0.1:{c.port}"},
    )
    assert r.http_version == "HTTP/2"
    assert r.status_code == 200, r.text


@pytest.mark.parametrize("target,body", [("/rpc/echo", b'{"a": 1}'), ("/mcp", MCP_LIST)])
def test_cross_site_origin_over_http2_is_403(h2_client, target, body):
    c, h2 = h2_client
    r = h2.post(
        f"http://127.0.0.1:{c.port}{target}",
        content=body,
        headers={"Content-Type": "application/json", "Origin": "https://evil.example"},
    )
    assert r.http_version == "HTTP/2"
    assert r.status_code == 403, r.text
