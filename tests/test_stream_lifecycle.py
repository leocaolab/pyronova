"""Tests for Stream deterministic lifecycle.

Covers:
- close() performs immediate channel teardown (not deferred to GC)
- send() after close() raises ConnectionError
- send_event() produces correct SSE format
- Double close is safe (idempotent)
- Streams without explicit close still work (backward compat)
- Custom content-type, status_code, headers
"""

import pytest
from pyronova import Pyronova, Response, Stream
from pyronova.testing import TestClient


@pytest.fixture(scope="module")
def client():
    app = Pyronova()

    @app.get("/stream/basic")
    def stream_basic(req):
        stream = Stream()
        stream.send("data: hello\n\n")
        stream.send("data: world\n\n")
        stream.close()
        return stream

    @app.get("/stream/event")
    def stream_event(req):
        stream = Stream()
        stream.send_event("payload1", event="update", id="1")
        stream.send_event("payload2")
        stream.close()
        return stream

    @app.get("/stream/close-then-send")
    def stream_close_then_send(req):
        """After close(), send() should raise ConnectionError."""
        stream = Stream()
        stream.send("data: before\n\n")
        stream.close()
        try:
            stream.send("data: after\n\n")
            # Should not reach here
            return Response(body="send after close did not raise", status_code=500)
        except ConnectionError:
            return stream

    @app.get("/stream/double-close")
    def double_close(req):
        stream = Stream()
        stream.send("data: ok\n\n")
        stream.close()
        stream.close()  # must not panic
        return stream

    @app.get("/stream/no-close")
    def no_close(req):
        """Stream that relies on Drop/GC — backward compat."""
        stream = Stream()
        stream.send("data: gc\n\n")
        return stream

    @app.get("/stream/custom")
    def stream_custom(req):
        stream = Stream(
            content_type="text/plain",
            status_code=202,
            headers={"x-stream": "yes"},
        )
        stream.send("line1\n")
        stream.close()
        return stream

    @app.get("/")
    def index(req):
        return {"ok": True}

    # Main interpreter: a Stream is served only from a main-interpreter route.
    c = TestClient(app, mode="gil")
    yield c
    c.close()


def test_basic_send(client):
    resp = client.get("/stream/basic")
    assert resp.status_code == 200
    assert b"hello" in resp.body
    assert b"world" in resp.body


def test_send_event_format(client):
    resp = client.get("/stream/event")
    assert resp.status_code == 200
    text = resp.text
    assert "id: 1\n" in text
    assert "event: update\n" in text
    assert "data: payload1\n" in text
    assert "data: payload2\n" in text


def test_close_then_send_raises(client):
    """After close(), subsequent send() must raise ConnectionError."""
    resp = client.get("/stream/close-then-send")
    assert resp.status_code == 200
    assert "before" in resp.text
    assert "after" not in resp.text


def test_double_close_safe(client):
    resp = client.get("/stream/double-close")
    assert resp.status_code == 200
    assert "ok" in resp.text


def test_no_explicit_close(client):
    """Streams without close() should still work via GC/Drop."""
    resp = client.get("/stream/no-close")
    assert resp.status_code == 200
    assert "gc" in resp.text


def test_custom_content_type_and_status(client):
    resp = client.get("/stream/custom")
    assert resp.status_code == 202
    assert "line1" in resp.text
    ct = resp.headers.get("Content-Type", resp.headers.get("content-type", ""))
    assert "text/plain" in ct
