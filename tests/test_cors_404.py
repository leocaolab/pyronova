"""Regression for the CORS-404 short-circuit bug.

Before the fix, early-return paths in `handle_request` / `handle_request_subinterp`
(unmatched route, static-file hit) bypassed `apply_cors`. Browsers doing a
CORS preflight (OPTIONS) against an unknown path got a bare 404 with no
Access-Control-* headers, blocking the follow-up real request with an
opaque CORS policy error.
"""

import urllib.request

from pyronova.testing import TestClient


def _request_with_cors(base: str, path: str, method: str = "GET"):
    req = urllib.request.Request(
        f"{base}{path}",
        headers={"Origin": "https://app.example.com"},
        method=method,
    )
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers)


def test_404_carries_cors_headers():
    # Its own module: a worker serves one app per module.
    from tests.apps.cors_404 import app

    with TestClient(app, port=None) as client:
        status, headers = _request_with_cors(client.base_url, "/nonexistent")
        assert status == 404
        lower = {k.lower(): v for k, v in headers.items()}
        # Must have allow-origin or the browser blocks the real request
        assert "access-control-allow-origin" in lower, (
            f"404 response missing CORS headers: {headers}"
        )


def test_options_preflight_on_unknown_path_not_blocked():
    """Simulates a real CORS preflight for a path the dev forgot to register
    an OPTIONS handler for. The browser will see allow-origin and proceed."""
    # Its own module: a worker serves one app per module.
    from tests.apps.cors_preflight_unknown_path import app

    with TestClient(app, port=None) as client:
        # OPTIONS /api/login — route not registered for OPTIONS, falls to 404
        status, headers = _request_with_cors(
            client.base_url, "/api/login", method="OPTIONS"
        )
        lower = {k.lower(): v for k, v in headers.items()}
        assert "access-control-allow-origin" in lower


def test_static_file_hit_carries_cors():
    """Static-file success path was also short-circuiting. A CSS file served
    over CORS needs the headers too."""
    import os
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        css = os.path.join(tmpdir, "reset.css")
        with open(css, "w") as f:
            f.write("body { margin: 0 }")

        # The app's module reads its static root from here, in main and in every worker.
        os.environ["PYRONOVA_TEST_CORS_STATIC_DIR"] = tmpdir
        try:
            from tests.apps.cors_static import app

            with TestClient(app, port=None) as client:
                status, headers = _request_with_cors(client.base_url, "/static/reset.css")
                assert status == 200
                lower = {k.lower(): v for k, v in headers.items()}
                assert "access-control-allow-origin" in lower
        finally:
            del os.environ["PYRONOVA_TEST_CORS_STATIC_DIR"]
