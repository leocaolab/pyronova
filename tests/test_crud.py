"""End-to-end tests for `pyronova.crud.register_crud`.

Same gating as `test_db_pg.py`: requires `PYRONOVA_TEST_PG_DSN` to point at a
live Postgres. Locally::

    docker run --rm -d --name pyronova-pg -p 5433:5432 \\
        -e POSTGRES_PASSWORD=pyronova -e POSTGRES_DB=pyronovatest postgres:17-alpine
    export PYRONOVA_TEST_PG_DSN="postgres://postgres:pyronova@127.0.0.1:5433/pyronovatest"
    pytest tests/test_crud.py
"""

import os

import pytest

from pyronova import Pyronova
from pyronova.crud import register_crud
from pyronova.db import PgPool
from pyronova.testing import TestClient


PG_DSN = os.environ.get("PYRONOVA_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    PG_DSN is None,
    reason="PYRONOVA_TEST_PG_DSN not set — skipping Postgres integration tests",
)


@pytest.fixture(scope="module")
def client():
    # Rust-side PgPool is a process global, connect() is idempotent — safe
    # to call from every module that uses the pool.
    pool = PgPool.connect(PG_DSN)

    # Fresh schema per module.
    pool.execute("DROP TABLE IF EXISTS pyronova_crud_items")
    pool.execute("""
        CREATE TABLE pyronova_crud_items (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            quantity INTEGER
        )
    """)

    app = Pyronova()
    register_crud(
        app, pool,
        prefix="/items",
        table="pyronova_crud_items",
        columns=["id", "name", "quantity"],
        id_column="id",
        id_type=int,
    )

    @app.get("/")
    def root(req):
        return "ok"

    with TestClient(app, port=None) as c:
        yield c

    pool.execute("DROP TABLE IF EXISTS pyronova_crud_items")


# ---------------------------------------------------------------------------
# Registration validation
# ---------------------------------------------------------------------------

def test_register_rejects_bad_identifiers():
    app = Pyronova()
    # No max_connections here (and below): the process-wide pool binds
    # the value from the first connect() call; pinning it to 1 starves
    # later tests that need multiple concurrent connections.
    pool = PgPool.connect(PG_DSN)
    with pytest.raises(ValueError, match="invalid SQL identifier"):
        register_crud(
            app, pool,
            prefix="/bad",
            table="drop_table; --",
            columns=["id"],
        )


def test_register_requires_id_in_columns():
    app = Pyronova()
    pool = PgPool.connect(PG_DSN)
    with pytest.raises(ValueError, match="must be included in columns"):
        register_crud(
            app, pool,
            prefix="/x",
            table="pyronova_crud_items",
            columns=["name"],
            id_column="id",
        )


def test_register_rejects_bad_prefix():
    app = Pyronova()
    pool = PgPool.connect(PG_DSN)
    with pytest.raises(ValueError, match="must start with"):
        register_crud(app, pool, prefix="no-slash", table="pyronova_crud_items", columns=["id"])
    with pytest.raises(ValueError, match="must not end with"):
        register_crud(app, pool, prefix="/trailing/", table="pyronova_crud_items", columns=["id"])


# ---------------------------------------------------------------------------
# CRUD round-trip
# ---------------------------------------------------------------------------

def test_create_returns_201_with_row(client):
    resp = client.post("/items", body={"name": "widget", "quantity": 5})
    assert resp.status_code == 201
    row = resp.json()
    assert row["name"] == "widget"
    assert row["quantity"] == 5
    assert isinstance(row["id"], int)


def test_get_one(client):
    created = client.post("/items", body={"name": "gear", "quantity": 12}).json()
    got = client.get(f"/items/{created['id']}").json()
    assert got == created


def test_get_one_missing(client):
    resp = client.get("/items/999999")
    assert resp.status_code == 404
    assert resp.json() == {"error": "not found"}


def test_list_paginated(client):
    # List the default page.
    resp = client.get("/items")
    assert resp.status_code == 200
    rows = resp.json()
    assert isinstance(rows, list)
    # ?limit= too large is capped but doesn't error
    resp = client.get("/items?limit=10")
    assert resp.status_code == 200
    assert len(resp.json()) <= 10


def test_update_returns_updated_row(client):
    created = client.post("/items", body={"name": "orig", "quantity": 1}).json()
    resp = client.put(f"/items/{created['id']}", body={"quantity": 99})
    assert resp.status_code == 200
    row = resp.json()
    assert row["id"] == created["id"]
    assert row["quantity"] == 99
    assert row["name"] == "orig"  # untouched


def test_update_missing_returns_404(client):
    resp = client.put("/items/999999", body={"name": "ghost"})
    assert resp.status_code == 404


def test_delete_roundtrip(client):
    created = client.post("/items", body={"name": "goner", "quantity": 0}).json()
    resp = client.delete(f"/items/{created['id']}")
    assert resp.status_code == 204
    # Subsequent GET is 404.
    resp = client.get(f"/items/{created['id']}")
    assert resp.status_code == 404


def test_delete_missing(client):
    resp = client.delete("/items/999999")
    assert resp.status_code == 404


def test_body_unknown_keys_ignored(client):
    # Extra keys that aren't in `columns` should be dropped silently —
    # the insert should succeed using just {name, quantity}.
    resp = client.post(
        "/items",
        body={"name": "extras", "quantity": 3, "evil_column": "drop users"},
    )
    assert resp.status_code == 201
    row = resp.json()
    assert row["name"] == "extras"


def test_empty_body_422(client):
    resp = client.post("/items", body={})
    assert resp.status_code == 422


def test_invalid_id_400(client):
    resp = client.get("/items/not-a-number")
    assert resp.status_code == 400
