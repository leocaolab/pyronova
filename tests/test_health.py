"""Tests for /livez and /readyz health probes."""

from __future__ import annotations

import json

import pytest

from pyronova.testing import TestClient


def test_livez_returns_200_always():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_livez import app

    with TestClient(app, port=None) as c:
        r = c.get("/livez")
        assert r.status_code == 200
        assert r.json() == {"status": "alive"}


def test_readyz_ok_when_no_checks():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_no_checks import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 200
        data = r.json()
        assert data == {"status": "ready", "checks": {}}


def test_readyz_ok_with_passing_checks():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_passing_checks import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ready"
        assert data["checks"]["always_ok"] == {"ok": True}
        assert data["checks"]["none_also_ok"] == {"ok": True}


def test_readyz_503_on_exception():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_raising_check import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        data = r.json()
        assert data["status"] == "not_ready"
        assert data["checks"]["db"]["ok"] is False
        assert "connection refused" in data["checks"]["db"]["error"]
        assert "RuntimeError" in data["checks"]["db"]["error"]


def test_readyz_503_on_false_return():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_false_check import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        assert r.json()["checks"]["feature_flag"]["ok"] is False


def test_readyz_aggregates_multiple_checks():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_mixed_checks import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        # One failure → overall 503, but every check is reported.
        assert r.status_code == 503
        checks = r.json()["checks"]
        assert checks["ok1"]["ok"] is True
        assert checks["ok2"]["ok"] is True
        assert checks["bad"]["ok"] is False


def test_async_readiness_check_supported():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_async_checks import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        data = r.json()
        assert data["checks"]["async_ok"]["ok"] is True
        assert data["checks"]["async_fail"]["ok"] is False
        assert "timeout" in data["checks"]["async_fail"]["error"]


def test_enable_health_probes_idempotent():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_enabled_twice import app

    with TestClient(app, port=None) as c:
        assert c.get("/livez").status_code == 200


def test_custom_paths():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_custom_paths import app

    with TestClient(app, port=None) as c:
        assert c.get("/_alive").status_code == 200
        assert c.get("/_ready").status_code == 200
        # Default paths are NOT registered.
        assert c.get("/livez").status_code == 404


def test_check_registered_after_enable_still_runs():
    """You can enable probes early (e.g., in Pyronova() setup) and register
    checks later as modules load. The readyz handler closes over the
    shared list, so late appends take effect immediately."""
    # Its own module: a worker serves one app per module.
    from tests.apps.health_late_check import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        assert "late check ran" in r.json()["checks"]["late"]["error"]
