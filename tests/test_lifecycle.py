"""Tests for on_startup / on_shutdown lifecycle hooks."""

import pytest
from pyronova import Pyronova, Response
from pyronova.testing import TestClient


@pytest.fixture(scope="module")
def app_with_hooks():
    """Create an app with startup hooks that set state."""
    # Module level (workers rebuild the app by executing its module).
    from tests.apps.lifecycle_hooks import app, hook_log

    return app, hook_log


@pytest.fixture(scope="module")
def client(app_with_hooks):
    app, _ = app_with_hooks
    c = TestClient(app)
    yield c
    c.close()


def test_startup_hooks_executed(client, app_with_hooks):
    """Startup hooks should run before server accepts requests."""
    _, hook_log = app_with_hooks
    assert hook_log["startup_called"] is True


def test_startup_hook_order(client, app_with_hooks):
    """Startup hooks should run in registration order."""
    _, hook_log = app_with_hooks
    assert hook_log["startup_order"] == ["init_cache", "init_counter"]


def test_startup_initialized_state(client):
    """State set by startup hooks should be accessible via routes."""
    resp = client.get("/cache-status")
    assert resp.json()["ready"] == "true"


def test_startup_counter_initialized(client):
    """Multiple startup hooks should all execute."""
    resp = client.get("/counter")
    assert resp.json()["counter"] == "0"


def test_on_startup_as_decorator():
    """on_startup should work as a decorator and return the original function."""
    app = Pyronova()

    @app.on_startup
    def my_hook():
        pass

    assert my_hook is not None
    assert callable(my_hook)
    assert my_hook.__name__ == "my_hook"


def test_on_startup_as_direct_call():
    """on_startup should work as a direct call (non-decorator)."""
    app = Pyronova()
    called = []

    def my_hook():
        called.append(True)

    app.on_startup(my_hook)
    assert len(app._startup_hooks) == 1


def test_on_shutdown_as_decorator():
    """on_shutdown should work as a decorator and return the original function."""
    app = Pyronova()

    @app.on_shutdown
    def cleanup():
        pass

    assert cleanup is not None
    assert callable(cleanup)
    assert cleanup.__name__ == "cleanup"


def test_on_shutdown_as_direct_call():
    """on_shutdown should work as a direct call (non-decorator)."""
    app = Pyronova()

    def cleanup():
        pass

    app.on_shutdown(cleanup)
    assert len(app._shutdown_hooks) == 1


def test_multiple_startup_hooks_registered():
    """Multiple startup hooks should all be registered."""
    app = Pyronova()

    @app.on_startup
    def hook1():
        pass

    @app.on_startup
    def hook2():
        pass

    @app.on_startup
    def hook3():
        pass

    assert len(app._startup_hooks) == 3


def test_multiple_shutdown_hooks_registered():
    """Multiple shutdown hooks should all be registered."""
    app = Pyronova()

    @app.on_shutdown
    def hook1():
        pass

    @app.on_shutdown
    def hook2():
        pass

    assert len(app._shutdown_hooks) == 2
