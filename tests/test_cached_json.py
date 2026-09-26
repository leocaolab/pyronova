"""Tests for @cached_json decorator."""

from __future__ import annotations

import json
import time

import pytest

from pyronova import Pyronova, Response, cached_json
from pyronova.testing import TestClient

# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module. Tests whose handler bumps a test-local counter build
# their own app and serve it with mode="gil".
app = Pyronova()


@app.get("/s")
@cached_json(ttl=10.0)
def string_handler(req):
    return "hello world"


@app.get("/b")
@cached_json(ttl=10.0)
def bytes_handler(req):
    return b'{"raw":true}'


def test_first_call_runs_handler_and_caches():
    app = Pyronova()
    counter = {"n": 0}

    @app.get("/h")
    @cached_json(ttl=10.0)
    def handler(req):
        counter["n"] += 1
        return {"n": counter["n"]}

    # The handler's counter lives in the test: only the main interpreter shares it.
    with TestClient(app, port=None, mode="gil") as c:
        r1 = c.get("/h")
        r2 = c.get("/h")
        r3 = c.get("/h")

    assert r1.status_code == 200
    assert r1.json() == {"n": 1}
    # Cached hits must NOT re-run the handler.
    assert r2.json() == {"n": 1}
    assert r3.json() == {"n": 1}
    assert counter["n"] == 1


def test_cache_expires():
    app = Pyronova()
    counter = {"n": 0}

    @app.get("/h")
    @cached_json(ttl=0.05)  # 50ms
    def handler(req):
        counter["n"] += 1
        return {"n": counter["n"]}

    # The handler's counter lives in the test: only the main interpreter shares it.
    with TestClient(app, port=None, mode="gil") as c:
        c.get("/h")
        time.sleep(0.1)
        r = c.get("/h")

    assert r.json() == {"n": 2}
    assert counter["n"] == 2


def test_response_object_passthrough_not_cached():
    """Explicit Response returns signal custom status/headers — don't cache."""
    app = Pyronova()
    counter = {"n": 0}

    @app.get("/h")
    @cached_json(ttl=10.0)
    def handler(req):
        counter["n"] += 1
        return Response(body=b'{"seq":' + str(counter["n"]).encode() + b'}',
                        status_code=202, content_type="application/json")

    # The handler's counter lives in the test: only the main interpreter shares it.
    with TestClient(app, port=None, mode="gil") as c:
        r1 = c.get("/h")
        r2 = c.get("/h")

    assert r1.status_code == 202
    assert r2.status_code == 202
    # Each call re-ran the handler because Response objects bypass the cache.
    assert counter["n"] == 2


def test_string_return_cached_as_bytes():
    with TestClient(app, port=None) as c:
        r = c.get("/s")
    assert r.status_code == 200
    assert r.text == "hello world"


def test_bytes_return_cached_verbatim():
    with TestClient(app, port=None) as c:
        r = c.get("/b")
    assert r.json() == {"raw": True}


def test_custom_key_function():
    app = Pyronova()
    counter = {"n": 0}

    @app.get("/k")
    @cached_json(ttl=10.0, key=lambda req: req.path + "?" + (req.query or ""))
    def handler(req):
        counter["n"] += 1
        return {"n": counter["n"], "q": req.query}

    # The handler's counter lives in the test: only the main interpreter shares it.
    with TestClient(app, port=None, mode="gil") as c:
        r1 = c.get("/k?x=1")
        r2 = c.get("/k?x=1")  # same key → hit
        r3 = c.get("/k?x=2")  # different key → miss

    assert r1.json()["n"] == 1
    assert r2.json()["n"] == 1  # cached
    assert r3.json()["n"] == 2  # new miss
    assert counter["n"] == 2


def test_invalid_ttl_rejected():
    with pytest.raises(ValueError):
        cached_json(ttl=0)
    with pytest.raises(ValueError):
        cached_json(ttl=-1)
