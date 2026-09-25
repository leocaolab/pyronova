"""End-to-end Postgres tests.

Skipped entirely unless `PYRONOVA_TEST_PG_DSN` is set. To run locally::

    docker run --rm -d --name pyronova-pg -p 5433:5432 \\
        -e POSTGRES_PASSWORD=pyronova -e POSTGRES_DB=pyronovatest postgres:17-alpine

    export PYRONOVA_TEST_PG_DSN="postgres://postgres:pyronova@127.0.0.1:5433/pyronovatest"
    pytest tests/test_db_pg.py

The pool is a global in the Rust side, which means these tests must share
a single `PgPool.connect()` per process. pytest's default process-per-run
model is fine — all tests here use a module-scoped fixture that sets up
a schema once.
"""

import datetime
import os
import json
import threading
import time
import urllib.request
import uuid

import pytest

from pyronova import Pyronova
from pyronova.db import PgPool
from pyronova.testing import TestClient


PG_DSN = os.environ.get("PYRONOVA_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    PG_DSN is None,
    reason="PYRONOVA_TEST_PG_DSN not set — skipping Postgres integration tests",
)


@pytest.fixture(scope="module")
def pool():
    # Rust-side PgPool is a process global; connect() is idempotent.
    p = PgPool.connect(PG_DSN, max_connections=4)
    # Fresh schema per test module.
    p.execute("DROP TABLE IF EXISTS pyronova_test_rows")
    p.execute("""
        CREATE TABLE pyronova_test_rows (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER,
            flag BOOLEAN DEFAULT false,
            meta JSONB,
            bin BYTEA
        )
    """)
    yield p
    p.execute("DROP TABLE IF EXISTS pyronova_test_rows")


# ---------------------------------------------------------------------------
# Raw pool API
# ---------------------------------------------------------------------------

def test_execute_returns_rows_affected(pool):
    n = pool.execute(
        "INSERT INTO pyronova_test_rows (name, value) VALUES ($1, $2)",
        "alice", 42,
    )
    assert n == 1


def test_fetch_one_returns_dict(pool):
    pool.execute("INSERT INTO pyronova_test_rows (name, value) VALUES ($1, $2)", "bob", 7)
    row = pool.fetch_one("SELECT name, value FROM pyronova_test_rows WHERE name = $1", "bob")
    assert row == {"name": "bob", "value": 7}


def test_fetch_one_none_on_no_match(pool):
    row = pool.fetch_one("SELECT name FROM pyronova_test_rows WHERE name = $1", "nobody")
    assert row is None


def test_fetch_all_returns_list(pool):
    pool.execute("DELETE FROM pyronova_test_rows")
    for i in range(3):
        pool.execute("INSERT INTO pyronova_test_rows (name, value) VALUES ($1, $2)", f"n{i}", i)
    rows = pool.fetch_all("SELECT name, value FROM pyronova_test_rows ORDER BY value")
    assert [(r["name"], r["value"]) for r in rows] == [("n0", 0), ("n1", 1), ("n2", 2)]


def test_fetch_scalar(pool):
    count = pool.fetch_scalar("SELECT COUNT(*) FROM pyronova_test_rows")
    assert isinstance(count, int)
    assert count >= 0


def test_null_values(pool):
    pool.execute("INSERT INTO pyronova_test_rows (name, value) VALUES ($1, $2)", "nullish", None)
    row = pool.fetch_one("SELECT name, value FROM pyronova_test_rows WHERE name = $1", "nullish")
    assert row == {"name": "nullish", "value": None}


def test_bool_and_json(pool):
    pool.execute(
        "INSERT INTO pyronova_test_rows (name, flag, meta) VALUES ($1, $2, $3)",
        "mix", True, {"a": 1, "b": [1, 2, 3]},
    )
    row = pool.fetch_one(
        "SELECT flag, meta FROM pyronova_test_rows WHERE name = $1", "mix"
    )
    assert row["flag"] is True
    assert row["meta"] == {"a": 1, "b": [1, 2, 3]}


def test_bytes_roundtrip(pool):
    blob = bytes(range(256))
    pool.execute(
        "INSERT INTO pyronova_test_rows (name, bin) VALUES ($1, $2)", "blob", blob
    )
    row = pool.fetch_one("SELECT bin FROM pyronova_test_rows WHERE name = $1", "blob")
    assert row["bin"] == blob


# ---------------------------------------------------------------------------
# Handler integration
# ---------------------------------------------------------------------------

def test_handler_can_query(pool):
    # Pre-seed data
    pool.execute("DELETE FROM pyronova_test_rows")
    pool.execute("INSERT INTO pyronova_test_rows (name, value) VALUES ($1, $2)", "carol", 100)

    app = Pyronova()

    @app.get("/")
    def root(req):
        return "ok"

    @app.get("/users/{name}", gil=True)
    def get_user(req):
        return pool.fetch_one(
            "SELECT name, value FROM pyronova_test_rows WHERE name = $1",
            req.params["name"],
        ) or {"error": "not found"}

    with TestClient(app, port=None) as c:
        resp = c.get("/users/carol")
        assert resp.status_code == 200
        assert resp.json() == {"name": "carol", "value": 100}

        resp = c.get("/users/nobody")
        assert resp.json() == {"error": "not found"}


def test_unsupported_param_type_raises(pool):
    with pytest.raises(ValueError, match="unsupported parameter type"):
        pool.fetch_one("SELECT $1::text", object())


def test_connect_is_idempotent(pool):
    """Calling connect() again with same DSN returns a usable handle."""
    again = PgPool.connect(PG_DSN)
    # Uses the same underlying pool.
    assert again.fetch_scalar("SELECT 1") == 1


# ---------------------------------------------------------------------------
# Async API (v2 — await-ready methods for async def handlers)
# ---------------------------------------------------------------------------

def test_async_api(pool):
    """The *_async methods return awaitables that cooperate with asyncio."""
    import asyncio

    async def run():
        val = await pool.fetch_scalar_async("SELECT 1")
        assert val == 1

        one = await pool.fetch_one_async("SELECT $1::int AS x", 42)
        assert one == {"x": 42}

        # None on no match.
        none_row = await pool.fetch_one_async(
            "SELECT id FROM pyronova_test_rows WHERE id = $1", 999999
        )
        assert none_row is None

        rows = await pool.fetch_all_async(
            "SELECT 1 AS a UNION SELECT 2 ORDER BY a"
        )
        assert [r["a"] for r in rows] == [1, 2]

    asyncio.run(run())


def test_async_execute(pool):
    """execute_async returns the rows-affected count."""
    import asyncio

    async def run():
        pool.execute("DELETE FROM pyronova_test_rows WHERE name = $1", "async_tmp")
        n = await pool.execute_async(
            "INSERT INTO pyronova_test_rows (name, value) VALUES ($1, $2)",
            "async_tmp", 123,
        )
        assert n == 1
        # Clean up.
        pool.execute("DELETE FROM pyronova_test_rows WHERE name = $1", "async_tmp")

    asyncio.run(run())


def test_unknown_column_types_do_not_explode(pool):
    """Regression for the DB-decode-fallback bug: the old `_ =>` fallback
    in `column_to_py` forced `<String as Decode>::decode` on the raw binary
    value, which failed for every non-text Postgres type (UUID, TIMESTAMP,
    INET, interval, …). Queries touching those types blew up with
    PyRuntimeError.

    Columns are now decoded by their Postgres type: UUID and timestamps
    become `uuid.UUID` / `datetime.datetime`, and a type with no decoder
    (INET) is always returned as its raw wire bytes, never guessed text.
    """
    # UUID — decoded to uuid.UUID.
    row = pool.fetch_one("SELECT gen_random_uuid() AS u")
    assert row is not None
    assert isinstance(row["u"], uuid.UUID)

    # TIMESTAMPTZ — decoded to datetime.datetime.
    row = pool.fetch_one("SELECT now() AS t")
    assert row is not None
    assert isinstance(row["t"], datetime.datetime)

    # INET — no decoder: raw wire bytes.
    row = pool.fetch_one("SELECT '10.0.0.1'::inet AS ip")
    assert row is not None
    assert isinstance(row["ip"], bytes)


def test_uuid_text_cast_roundtrip(pool):
    """Workaround documented in the fallback comment: cast to text
    server-side for a clean string column in the Python dict."""
    row = pool.fetch_one("SELECT gen_random_uuid()::text AS u")
    assert isinstance(row["u"], str)
    # Rough UUID shape check: 8-4-4-4-12 hex + dashes.
    assert len(row["u"]) == 36
    assert row["u"].count("-") == 4


def test_fetch_iter_basic(pool):
    """Cursor yields rows one at a time, preserving value + type."""
    rows = list(pool.fetch_iter(
        "SELECT $1::int AS x, $2::text AS y UNION ALL SELECT $3, $4",
        1, "a", 2, "b",
    ))
    assert rows == [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]


def test_fetch_iter_large_result_set_bounded_memory(pool):
    """Streaming 50k rows must not spike resident memory. We can't
    assert exact RSS in a pytest, but we can (a) verify the iterator
    completes and (b) verify the process didn't crash — which fetch_all
    on 50k-row queries has historically been close to doing on
    memory-constrained boxes because of the O(2N) peak."""
    count = 0
    checksum = 0
    for row in pool.fetch_iter(
        "SELECT i, i * 2 AS doubled FROM generate_series(1, 50000) i"
    ):
        count += 1
        checksum += row["doubled"]
    assert count == 50000
    assert checksum == sum(i * 2 for i in range(1, 50001))


def test_fetch_iter_early_break_cleans_up(pool):
    """Breaking out of the iteration mid-stream must not leak the
    Postgres connection. Run many cursors with early break in sequence;
    if connection cleanup is busted the pool (size 4) runs out after
    a handful of iterations."""
    for _ in range(50):
        seen = 0
        for _row in pool.fetch_iter(
            "SELECT i FROM generate_series(1, 1000000) i"
        ):
            seen += 1
            if seen == 3:
                break
        assert seen == 3
    # If we got here the pool never starved.


def test_fetch_iter_propagates_sql_error(pool):
    """SQL errors surface as RuntimeError on the first __next__ that
    sees the error (sqlx discovers the error during streaming)."""
    import pytest
    with pytest.raises(RuntimeError):
        # Division by zero raises during execution (not planning).
        list(pool.fetch_iter("SELECT 1/0"))


def test_fetch_iter_empty_result(pool):
    assert list(pool.fetch_iter("SELECT 1 WHERE FALSE")) == []


def test_fetch_iter_via_to_list(pool):
    """to_list is the eager-drain shortcut; equivalent to list(cursor)."""
    rows = pool.fetch_iter(
        "SELECT i FROM generate_series(1, 5) i"
    ).to_list()
    assert [r["i"] for r in rows] == [1, 2, 3, 4, 5]


def test_async_concurrent_queries(pool):
    """Concurrent async queries interleave on the pool's tokio runtime."""
    import asyncio
    import time

    async def run():
        # pg_sleep(0.1) × 4 concurrent: ~0.1s parallel, ~0.4s serial.
        # Threshold 0.3s detects a re-serialized pool while leaving slack
        # for noisy full-suite runs.
        start = time.perf_counter()
        results = await asyncio.gather(*[
            pool.fetch_scalar_async("SELECT pg_sleep(0.1), 1")
            for _ in range(4)
        ])
        elapsed = time.perf_counter() - start
        assert len(results) == 4
        assert elapsed < 0.3, f"async queries serialized: {elapsed:.2f}s"

    asyncio.run(run())
