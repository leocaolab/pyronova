"""End-to-end tests for response compression (gzip / brotli).

Compression is opt-in. When disabled, responses must look identical to the
pre-compression behavior — no Content-Encoding / Vary headers added, bodies
byte-for-byte unchanged.
"""

import gzip
import json
import urllib.request

import pytest

from pyronova import Pyronova
from pyronova.testing import TestClient

from tests.apps.compression_routes import LARGE_JSON, add_routes


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()
add_routes(app)


def _raw_request(base_url: str, path: str, accept_encoding: str | None):
    """Raw HTTP request that does NOT auto-decompress (urllib decompresses
    if the caller sets Accept-Encoding itself, and we want to inspect the
    Content-Encoding header + compressed bytes directly)."""
    headers = {}
    if accept_encoding is not None:
        headers["Accept-Encoding"] = accept_encoding
    req = urllib.request.Request(f"{base_url}{path}", headers=headers)
    resp = urllib.request.urlopen(req, timeout=5)
    return resp.status, dict(resp.headers), resp.read()


# ---------------------------------------------------------------------------
# Disabled by default
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def default_client():
    # No enable_compression() call — framework must behave identically to
    # pre-compression build.
    with TestClient(app, port=None) as client:
        yield client


def test_disabled_by_default_no_content_encoding(default_client):
    status, headers, body = _raw_request(
        default_client.base_url, "/big-json", "gzip, br"
    )
    assert status == 200
    assert "content-encoding" not in {k.lower() for k in headers}
    assert "vary" not in {k.lower() for k in headers}
    # Body parses as plain JSON — not compressed
    assert json.loads(body) == LARGE_JSON


# ---------------------------------------------------------------------------
# Enabled — per-scenario assertions
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def compressed_client():
    # Its own module (a worker serves one app per module), imported here because
    # compression settings are process-wide.
    from tests.apps.compression_enabled import app as compressed_app

    with TestClient(compressed_app, port=None) as client:
        yield client


def test_brotli_preferred_when_both_offered(compressed_client):
    status, headers, body = _raw_request(
        compressed_client.base_url, "/big-json", "gzip, br"
    )
    assert status == 200
    # Header keys preserve casing from server; lowercase for comparison
    lower = {k.lower(): v for k, v in headers.items()}
    assert lower["content-encoding"] == "br"
    assert "accept-encoding" in lower["vary"].lower()
    # Compressed body is smaller than raw JSON
    raw = json.dumps(LARGE_JSON).encode()
    assert len(body) < len(raw)


def test_gzip_when_only_gzip_accepted(compressed_client):
    status, headers, body = _raw_request(
        compressed_client.base_url, "/big-json", "gzip"
    )
    assert status == 200
    lower = {k.lower(): v for k, v in headers.items()}
    assert lower["content-encoding"] == "gzip"
    # Decompress and check
    decoded = gzip.decompress(body)
    assert json.loads(decoded) == LARGE_JSON


def test_no_accept_encoding_header_not_compressed(compressed_client):
    status, headers, body = _raw_request(
        compressed_client.base_url, "/big-json", None
    )
    assert status == 200
    lower = {k.lower() for k in headers}
    assert "content-encoding" not in lower


def test_unsupported_encoding_not_compressed(compressed_client):
    status, headers, _body = _raw_request(
        compressed_client.base_url, "/big-json", "deflate, compress"
    )
    assert status == 200
    lower = {k.lower() for k in headers}
    assert "content-encoding" not in lower


def test_small_body_not_compressed(compressed_client):
    status, headers, body = _raw_request(
        compressed_client.base_url, "/small", "gzip, br"
    )
    assert status == 200
    lower = {k.lower() for k in headers}
    assert "content-encoding" not in lower
    assert json.loads(body) == {"ok": True}


def test_binary_content_type_not_compressed(compressed_client):
    status, headers, _body = _raw_request(
        compressed_client.base_url, "/big-binary", "gzip, br"
    )
    assert status == 200
    lower = {k.lower() for k in headers}
    assert "content-encoding" not in lower


def test_handler_content_encoding_respected(compressed_client):
    status, headers, _body = _raw_request(
        compressed_client.base_url, "/preset-encoding", "gzip, br"
    )
    assert status == 200
    lower = {k.lower(): v for k, v in headers.items()}
    # Framework must not overwrite the handler's choice
    assert lower["content-encoding"] == "identity"


def test_q_zero_excludes_algorithm(compressed_client):
    # br;q=0 → must fall back to gzip
    status, headers, body = _raw_request(
        compressed_client.base_url, "/big-json", "br;q=0, gzip"
    )
    assert status == 200
    lower = {k.lower(): v for k, v in headers.items()}
    assert lower["content-encoding"] == "gzip"
    assert json.loads(gzip.decompress(body)) == LARGE_JSON
