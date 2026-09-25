"""Path-parameter injection: FastAPI-style ``def h(req, order_id)``.

Pyronova registers the user's handler with the Rust engine after a Python-
side decorator pass. Routes whose handler signature is exactly ``(req)``
stay byte-identical to today — the decorator returns the original function
and the hot path is not touched. Only routes that declare extra positional
or keyword params get wrapped with a shim that copies matching values out
of ``req.params``.
"""

import pytest

from pyronova import Pyronova
from pyronova.testing import TestClient


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()


@app.get("/orders/{order_id}")
def get_order(req, order_id):
    return {"order_id": order_id}


@app.get("/users/{user_id}/posts/{post_id}")
def get_post(req, user_id, post_id):
    return {"user": user_id, "post": post_id}


@app.get("/legacy/{order_id}")
def legacy(req):
    return {"order_id": req.params["order_id"]}


@app.get("/async/{name}")
async def get_async(req, name):
    return {"name": name}


@app.get("/decoded/{slug}")
def decoded(req, slug):
    return {"slug": slug}


@pytest.fixture(scope="module")
def client():
    c = TestClient(app)
    yield c
    c.close()


def test_single_param_injected(client):
    r = client.get("/orders/abc123")
    assert r.status_code == 200
    assert r.json() == {"order_id": "abc123"}


def test_multiple_params_injected(client):
    r = client.get("/users/u42/posts/p7")
    assert r.status_code == 200
    assert r.json() == {"user": "u42", "post": "p7"}


def test_legacy_req_only_unchanged(client):
    """Hot-path handler signature `(req)` still works via req.params."""
    r = client.get("/legacy/xyz")
    assert r.status_code == 200
    assert r.json() == {"order_id": "xyz"}


def test_async_handler_injection(client):
    r = client.get("/async/leo")
    assert r.status_code == 200
    assert r.json() == {"name": "leo"}


def test_percent_decoded_injection(client):
    """Router already URL-decodes params; injection sees the decoded value."""
    r = client.get("/decoded/hello%20world")
    assert r.status_code == 200
    assert r.json() == {"slug": "hello world"}


# ---- Unit-level guarantees on the decorator return value ------------------
#
# These don't need a running server. They prove the *hot path* (req-only
# handler) ends up with the original function in module globals, which is
# what sub-interp workers grab by __name__.


def test_req_only_handler_returned_unchanged():
    app = Pyronova()

    def my_handler(req):
        return "ok"

    bound = app.get("/x", my_handler)
    # Decorator must hand back the SAME function object — no shim, no
    # wrapping, no extra frame on the hot path.
    assert bound is my_handler


def test_injected_handler_returns_shim_for_subinterp_lookup():
    app = Pyronova()

    def my_handler(req, item_id):
        return item_id

    bound = app.get("/items/{item_id}", my_handler)
    # When injection is in play, the decorator returns the SHIM (not the
    # original), so sub-interp workers find the shim in module globals
    # when looking handlers up by __name__.
    assert bound is not my_handler
    assert bound.__name__ == "my_handler"
    assert bound.__wrapped__ is my_handler


def test_mismatched_param_raises_at_registration():
    app = Pyronova()

    def bad(req, oops):
        return oops

    with pytest.raises(ValueError, match="oops"):
        app.get("/items/{item_id}", bad)


def test_colon_path_template_is_a_registration_error():
    """Only `{name}` / `{*name}` are path parameters. matchit 0.8 reads `:name` as
    literal text, so `/items/:item_id` would register, never match `/items/42`, and
    injection would pass None; registration refuses it and names the `{item_id}` to
    write instead (decision Q2)."""
    app = Pyronova()

    def h(req, item_id):
        return item_id

    with pytest.raises(ValueError, match=r"`:item_id` is not a path parameter; write it as `\{item_id\}`"):
        app.get("/items/:item_id", h)
