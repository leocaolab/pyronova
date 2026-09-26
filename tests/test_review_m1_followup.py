"""Code review cced8c2, M1 follow-up: the CRUD helper answers DB errors by type.

A constraint violation is the client's conflict (409), a value the column cannot
take is the client's bad input (422); both carry the database's reason (D4: 4xx
carry the reason). Anything else stays a generic 500.
"""

import os

import pytest

from pyronova.db import PgPool
from pyronova.testing import TestClient

PG_DSN = os.environ.get("PYRONOVA_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    PG_DSN is None,
    reason="PYRONOVA_TEST_PG_DSN not set — skipping Postgres integration tests",
)

TABLE = "pyronova_m1f_items"


@pytest.fixture(scope="module")
def client():
    pool = PgPool.connect(PG_DSN)
    pool.execute(f"DROP TABLE IF EXISTS {TABLE}")
    pool.execute(f"""
        CREATE TABLE {TABLE} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            quantity INTEGER CHECK (quantity >= 0)
        )
    """)

    # After the schema exists: workers execute the app's module, which only connects
    # and registers routes.
    from tests.apps.m1_followup_items import app

    with TestClient(app, port=None) as c:
        yield c

    pool.execute(f"DROP TABLE IF EXISTS {TABLE}")


def test_duplicate_insert_is_409_with_the_reason(client):
    first = client.post("/items", body={"name": "dup", "quantity": 1})
    assert first.status_code == 201, first.text

    again = client.post("/items", body={"name": "dup", "quantity": 2})
    assert again.status_code == 409, again.text
    error = again.json()["error"]
    assert "duplicate key" in error, error
    assert f"{TABLE}_name_key" in error, error


def test_duplicate_update_is_409_with_the_reason(client):
    a = client.post("/items", body={"name": "upd-a"}).json()
    client.post("/items", body={"name": "upd-b"})

    resp = client.put(f"/items/{a['id']}", body={"name": "upd-b"})
    assert resp.status_code == 409, resp.text
    assert "duplicate key" in resp.json()["error"]


def test_other_integrity_violation_is_409_with_the_reason(client):
    # CHECK constraint (SQLSTATE 23514): an IntegrityError, not a unique violation.
    resp = client.post("/items", body={"name": "neg", "quantity": -1})
    assert resp.status_code == 409, resp.text
    assert "check constraint" in resp.json()["error"], resp.text


def test_value_the_column_cannot_take_is_422_with_the_reason(client):
    # quantity is INTEGER: a JSON string cannot be sent as int4 (TypeError).
    resp = client.post("/items", body={"name": "typed", "quantity": "many"})
    assert resp.status_code == 422, resp.text
    assert "parameter $2" in resp.json()["error"], resp.text

    # Out of range for int4 (ValueError).
    resp = client.post("/items", body={"name": "big", "quantity": 2**40})
    assert resp.status_code == 422, resp.text
    assert "out of range" in resp.json()["error"], resp.text
