"""Tests for static file serving."""

import os
import tempfile
import pytest
from pyronova.testing import TestClient


@pytest.fixture(scope="module")
def static_dir():
    """Create a temp directory with test static files."""
    d = tempfile.mkdtemp(prefix="pyronova_static_")
    with open(os.path.join(d, "index.html"), "w") as f:
        f.write("<h1>Hello</h1>")
    with open(os.path.join(d, "style.css"), "w") as f:
        f.write("body { color: red; }")
    with open(os.path.join(d, "data.json"), "w") as f:
        f.write('{"key": "value"}')
    with open(os.path.join(d, "readme.txt"), "w") as f:
        f.write("plain text file")
    # Nested directory
    sub = os.path.join(d, "sub")
    os.makedirs(sub)
    with open(os.path.join(sub, "nested.js"), "w") as f:
        f.write("console.log('hi')")
    # The apps (tests/apps/static_*.py) read their root from here, in main and in
    # every worker.
    os.environ["PYRONOVA_TEST_STATIC_DIR"] = d
    yield d
    del os.environ["PYRONOVA_TEST_STATIC_DIR"]


@pytest.fixture(scope="module")
def client(static_dir):
    from tests.apps.static_site import app

    c = TestClient(app)
    yield c
    c.close()


def test_static_html(client):
    resp = client.get("/static/index.html")
    assert resp.status_code == 200
    assert "<h1>Hello</h1>" in resp.text
    assert any("text/html" in v for v in resp.headers.values())


def test_static_css(client):
    resp = client.get("/static/style.css")
    assert resp.status_code == 200
    assert "color: red" in resp.text
    assert any("text/css" in v for v in resp.headers.values())


def test_static_json(client):
    resp = client.get("/static/data.json")
    assert resp.status_code == 200
    assert resp.json()["key"] == "value"


def test_static_txt(client):
    resp = client.get("/static/readme.txt")
    assert resp.status_code == 200
    assert "plain text file" in resp.text


def test_static_nested(client):
    resp = client.get("/static/sub/nested.js")
    assert resp.status_code == 200
    assert "console.log" in resp.text


def test_static_not_found(client):
    resp = client.get("/static/nonexistent.html")
    assert resp.status_code == 404


def test_static_path_traversal(client):
    resp = client.get("/static/../../../etc/passwd")
    assert resp.status_code == 403


def test_api_still_works(client):
    """API routes coexist with static files."""
    resp = client.get("/")
    assert resp.json()["api"] is True


def test_post_static_file_rejected(client):
    """POST to static file should return 404 (only GET/HEAD allowed)."""
    resp = client.post("/static/index.html")
    assert resp.status_code == 404


# -----------------------------------------------------------------------------
# TOCTOU regression (advisor audit #14)
# -----------------------------------------------------------------------------
#
# Background: static file serving canonicalizes the candidate path, verifies
# it sits inside the configured root, then opens the file. If an attacker can
# swap the final path component for a symlink between those two steps, they
# can read arbitrary files on disk through the otherwise-valid mount.
#
# Defense: `tokio::fs::OpenOptions::custom_flags(O_NOFOLLOW)` — refuses to
# follow symlinks at the final component. Legitimate symlinks inside the
# static root are resolved earlier by `canonicalize` so real static mounts
# still work; only symlinks that arrive *after* the containment decision
# are rejected.
#
# This test simulates the post-check swap: we replace one of the static
# files with a symlink to /etc/passwd, then request it. Before the fix the
# server would happily read /etc/passwd. After the fix it refuses (500 or
# 404 or 403 — any non-200 with no /etc/passwd content is acceptable).

def test_static_symlink_out_of_root_refused(static_dir):
    """Symlink pointing outside the static root must not be followed."""
    import subprocess
    # Replace readme.txt with a symlink to /etc/passwd
    readme = os.path.join(static_dir, "trap.txt")
    try:
        os.symlink("/etc/passwd", readme)
    except (OSError, NotImplementedError):
        pytest.skip("symlink not supported on this platform")

    # Its own module: a worker serves one app per module.
    from tests.apps.static_symlink import app

    c = TestClient(app)
    try:
        resp = c.get("/s/trap.txt")
        # Accept any refusal path:
        #   - 403 (canonicalize resolved to outside root → starts_with fails)
        #   - 404 (O_NOFOLLOW ELOOP at open → `continue`s to next mount)
        #   - any non-200
        # What we MUST NOT see: 200 + /etc/passwd contents.
        assert resp.status_code != 200 or b"root:" not in resp.content, (
            f"symlink to /etc/passwd was served! status={resp.status_code} "
            f"body[:80]={resp.content[:80]!r}"
        )
    finally:
        try:
            os.unlink(readme)
        except OSError:
            pass
