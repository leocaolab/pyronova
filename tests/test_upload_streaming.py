"""Tests for `@app.post(..., stream=True)` upload streaming.

Scope (matches v1 impl in src/python/body_stream.rs):
  * gil=True + sync handler only (registration should reject async / non-gil)
  * Iterator protocol: `for chunk in req.stream: ...` + `.read(n)`
  * max_body_size is still enforced for streaming routes
  * Non-stream routes' `req.stream` is None
"""

import urllib.request
import urllib.error

import pytest

from pyronova import Pyronova
from pyronova.testing import TestClient


# ---------------------------------------------------------------------------
# Registration validation
# ---------------------------------------------------------------------------

def test_stream_requires_gil():
    app = Pyronova()
    with pytest.raises(ValueError, match="gil=True"):
        @app.post("/x", stream=True)  # gil defaults to False
        def handler(req):
            pass


def test_stream_rejects_async():
    app = Pyronova()
    with pytest.raises(ValueError, match="async def"):
        @app.post("/x", gil=True, stream=True)
        async def handler(req):
            pass


# ---------------------------------------------------------------------------
# Functional tests
# ---------------------------------------------------------------------------

# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()


@app.get("/")
def root(req):
    return {"ready": True}


@app.post("/upload", gil=True, stream=True)
def upload(req):
    total = 0
    count = 0
    for chunk in req.stream:
        assert isinstance(chunk, bytes)
        total += len(chunk)
        count += 1
    return {"bytes": total, "chunks": count}


@app.post("/read_all", gil=True, stream=True)
def read_all(req):
    data = req.stream.read()
    return {"len": len(data), "echo": data.decode("utf-8", errors="replace")}


@app.post("/buffered", gil=True)
def buffered(req):
    # Non-stream route — req.stream must be None, req.body must work.
    return {
        "stream_is_none": req.stream is None,
        "body_len": len(req.body),
    }


@pytest.fixture(scope="module")
def client():
    with TestClient(app, port=None) as c:
        yield c


def test_upload_small_body(client):
    resp = client.post("/upload", body=b"hello world")
    data = resp.json()
    assert data["bytes"] == 11


def test_upload_large_body_does_not_oom(client):
    # 5 MB — well under default 10 MB cap. We're mainly checking the
    # iterator delivers chunks and the total matches.
    payload = b"x" * (5 * 1024 * 1024)
    resp = client.post("/upload", body=payload)
    data = resp.json()
    assert data["bytes"] == 5 * 1024 * 1024
    assert data["chunks"] >= 1


def test_read_all_equivalent_to_body(client):
    resp = client.post("/read_all", body=b"streamed text")
    data = resp.json()
    assert data["len"] == 13
    assert data["echo"] == "streamed text"


def test_non_stream_route_has_no_stream(client):
    resp = client.post("/buffered", body=b"abc")
    data = resp.json()
    assert data["stream_is_none"] is True
    assert data["body_len"] == 3


def test_max_body_size_enforced_on_stream(client):
    # Temporarily shrink max_body_size; with IOError expected on overflow.
    # TestClient's app is the module-scoped one; we can reach into it via
    # the base_url to set the cap then restore.
    # Actually we want a separate app for this to avoid bleeding into other
    # tests. Build one here.
    # Its own module: a worker serves one app per module.
    from tests.apps.upload_small_cap import app

    with TestClient(app, port=None) as c:
        # 2 KB body > 1 KB cap → feeder sends ChunkMsg::Err
        try:
            resp = c.post("/up", body=b"y" * 2048)
            data = resp.json()
            assert "error" in data
            assert "max_body_size" in data["error"] or "exceed" in data["error"].lower()
        except urllib.error.HTTPError as e:
            # Acceptable: server may also return 413 via the initial-bounds
            # check path. Either signals the cap worked.
            assert e.code in (400, 413, 500)
