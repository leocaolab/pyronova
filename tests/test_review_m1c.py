"""Code review cced8c2, milestone M1c: the Postgres layer (`src/db.rs`).

Each test names the review item it proves. Skipped unless `PYRONOVA_TEST_PG_DSN` is set.
Tables are prefixed `m1c_` because other suites share the database.
"""

import asyncio
import datetime
import decimal
import os
import subprocess
import sys
import textwrap
import threading
import time
import uuid

import pytest

PG_DSN = os.environ.get("PYRONOVA_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    PG_DSN is None,
    reason="PYRONOVA_TEST_PG_DSN not set — skipping Postgres integration tests",
)


@pytest.fixture(scope="module")
def pool():
    from pyronova.db import PgPool

    return PgPool.connect(PG_DSN)


def _fresh_table(pool, name, columns):
    pool.execute(f"DROP TABLE IF EXISTS {name}")
    pool.execute(f"CREATE TABLE {name} ({columns})")


# ---------------------------------------------------------------------------
# Item 1 — parameters are encoded as the statement declares them
# ---------------------------------------------------------------------------


def test_none_binds_into_text_int_uuid_columns(pool):
    """`None` used to be bound as a bigint NULL, which a uuid column rejects."""
    _fresh_table(pool, "m1c_nulls", "t text, i int4, u uuid, n numeric, ts timestamptz")
    pool.execute(
        "INSERT INTO m1c_nulls (t, i, u, n, ts) VALUES ($1, $2, $3, $4, $5)",
        None, None, None, None, None,
    )
    row = pool.fetch_one("SELECT t, i, u, n, ts FROM m1c_nulls")
    assert row == {"t": None, "i": None, "u": None, "n": None, "ts": None}
    pool.execute("DROP TABLE m1c_nulls")


def test_same_sql_with_different_python_types_stores_the_right_values(pool):
    """sqlx caches a prepared statement per SQL text with the parameter types of its first
    execution. Encoding each value by its own Python type then sent float8 bytes into an
    int8 parameter: 5.5 was stored as 4.6e18 without any error."""
    _fresh_table(pool, "m1c_cache", "f float8, n numeric, i int4")
    insert_f = "INSERT INTO m1c_cache (f) VALUES ($1) RETURNING f"
    insert_n = "INSERT INTO m1c_cache (n) VALUES ($1) RETURNING n"
    insert_i = "INSERT INTO m1c_cache (i) VALUES ($1) RETURNING i"
    # Sequential calls reuse idle connections, so every connection the pool holds
    # caches these statements with the first call's value types.
    for _ in range(20):
        pool.fetch_one(insert_f, 5)
        pool.fetch_one(insert_n, 5.5)
        pool.fetch_one(insert_i, None)
    assert pool.fetch_one(insert_f, 5.5) == {"f": 5.5}
    assert pool.fetch_one(insert_n, 7) == {"n": decimal.Decimal("7")}
    assert pool.fetch_one(insert_i, 9) == {"i": 9}
    pool.execute("DROP TABLE m1c_cache")


def test_value_the_declared_type_cannot_hold_is_a_type_error(pool):
    _fresh_table(pool, "m1c_mismatch", "i int4")
    with pytest.raises(TypeError, match=r"parameter \$1 is int4.*str"):
        pool.execute("INSERT INTO m1c_mismatch (i) VALUES ($1)", "seven")
    with pytest.raises(ValueError, match=r"parameter \$1.*out of range for int4"):
        pool.execute("INSERT INTO m1c_mismatch (i) VALUES ($1)", 2**40)
    with pytest.raises(TypeError, match="takes 1 parameter"):
        pool.execute("INSERT INTO m1c_mismatch (i) VALUES ($1)", 1, 2)
    pool.execute("DROP TABLE m1c_mismatch")


def test_uuid_date_timestamp_decimal_params_roundtrip(pool):
    _fresh_table(
        pool, "m1c_rt",
        "u uuid, d date, ts timestamp, tz timestamptz, n numeric",
    )
    u = uuid.UUID("8f3a1c2e-0000-4000-8000-00000000abcd")
    d = datetime.date(1999, 12, 31)
    ts = datetime.datetime(2024, 2, 29, 13, 45, 1, 123456)
    tz = datetime.datetime(2024, 2, 29, 13, 45, 1, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=-5)))
    n = decimal.Decimal("-12345678901234567890.000123400")
    pool.execute("INSERT INTO m1c_rt VALUES ($1, $2, $3, $4, $5)", u, d, ts, tz, n)
    row = pool.fetch_one("SELECT * FROM m1c_rt WHERE u = $1", u)
    assert row["u"] == u
    assert row["d"] == d
    assert row["ts"] == ts
    assert row["tz"] == tz  # aware datetimes compare by instant
    assert row["tz"].tzinfo == datetime.timezone.utc
    assert row["n"] == n and str(row["n"]) == str(n)
    pool.execute("DROP TABLE m1c_rt")


# ---------------------------------------------------------------------------
# Item 2 — columns decode by a closed type-OID kind
# ---------------------------------------------------------------------------


def test_date_uuid_timestamp_numeric_decode_to_python_types(pool):
    row = pool.fetch_one(
        """SELECT '2024-01-02'::date AS d,
                  '00000000-0000-0000-0000-000000000041'::uuid AS u,
                  '2000-01-01 00:00:00.000065'::timestamp AS ts,
                  '2000-01-01 00:00:00+00'::timestamptz AS tz,
                  12.50::numeric(6,2) AS n,
                  'NaN'::numeric AS nan,
                  0.000001::numeric AS small,
                  -98765432109876543210.5::numeric AS big"""
    )
    assert row["d"] == datetime.date(2024, 1, 2)
    assert row["u"] == uuid.UUID("00000000-0000-0000-0000-000000000041")
    assert row["ts"] == datetime.datetime(2000, 1, 1, 0, 0, 0, 65)
    assert row["tz"] == datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    assert str(row["n"]) == "12.50" and isinstance(row["n"], decimal.Decimal)
    assert row["nan"].is_nan()
    assert row["small"] == decimal.Decimal("0.000001")
    assert row["big"] == decimal.Decimal("-98765432109876543210.5")


def test_unknown_kind_is_bytes_for_every_value(pool):
    """A type without a decoder used to be guessed as UTF-8 text: the same column came
    back as `str` on one row and `bytes` on the next."""
    rows = pool.fetch_all(
        "SELECT m FROM (VALUES ('41:42:43:44:45:46'::macaddr), ('8f:00:00:00:00:01'::macaddr)) v(m)"
    )
    assert [type(r["m"]) for r in rows] == [bytes, bytes]
    assert rows[0]["m"] == b"ABCDEF"


def test_enum_and_domain_columns_decode_as_their_base(pool):
    pool.execute("DROP TABLE IF EXISTS m1c_kinds")
    pool.execute("DROP TYPE IF EXISTS m1c_mood")
    pool.execute("DROP DOMAIN IF EXISTS m1c_pos")
    pool.execute("CREATE TYPE m1c_mood AS ENUM ('ok', 'sad')")
    pool.execute("CREATE DOMAIN m1c_pos AS int4 CHECK (VALUE > 0)")
    pool.execute("CREATE TABLE m1c_kinds (mood m1c_mood, pos m1c_pos)")
    pool.execute("INSERT INTO m1c_kinds VALUES ($1, $2)", "sad", 3)
    assert pool.fetch_one("SELECT mood, pos FROM m1c_kinds") == {"mood": "sad", "pos": 3}
    pool.execute("DROP TABLE m1c_kinds")
    pool.execute("DROP TYPE m1c_mood")
    pool.execute("DROP DOMAIN m1c_pos")


# ---------------------------------------------------------------------------
# Item 3 — database errors are typed and carry SQLSTATE
# ---------------------------------------------------------------------------


def test_unique_violation_is_typed_with_sqlstate(pool):
    from pyronova.db import DatabaseError, IntegrityError, UniqueViolation

    _fresh_table(pool, "m1c_uniq", "k text PRIMARY KEY")
    pool.execute("INSERT INTO m1c_uniq VALUES ($1)", "a")
    with pytest.raises(UniqueViolation) as info:
        pool.execute("INSERT INTO m1c_uniq VALUES ($1)", "a")
    err = info.value
    assert isinstance(err, IntegrityError) and isinstance(err, DatabaseError)
    assert err.sqlstate == "23505"
    assert "duplicate key value violates unique constraint" in str(err)

    async def insert_again():
        await pool.execute_async("INSERT INTO m1c_uniq VALUES ($1)", "a")

    with pytest.raises(UniqueViolation) as info:
        asyncio.run(insert_again())
    assert info.value.sqlstate == "23505"
    pool.execute("DROP TABLE m1c_uniq")


def test_other_database_errors_carry_their_sqlstate(pool):
    from pyronova.db import DatabaseError, IntegrityError

    with pytest.raises(DatabaseError) as info:
        pool.fetch_all("SELECT * FROM m1c_no_such_table")
    assert info.value.sqlstate == "42P01"
    assert not isinstance(info.value, IntegrityError)
    assert "m1c_no_such_table" in str(info.value)

    with pytest.raises(DatabaseError) as info:
        list(pool.fetch_iter("SELECT 1/0"))
    assert info.value.sqlstate == "22012"


# ---------------------------------------------------------------------------
# Item 4 — a second connect() asking for another DSN or other settings is an
# error, not silently dropped
# ---------------------------------------------------------------------------


def _run(body: str) -> str:
    script = "import os\nfrom pyronova.db import PgPool\nDSN = os.environ['PYRONOVA_TEST_PG_DSN']\n"
    proc = subprocess.run(
        [sys.executable, "-c", script + textwrap.dedent(body)],
        capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return proc.stdout + proc.stderr  # the Rust log goes to stderr


def test_second_connect_with_another_dsn_raises():
    out = _run("""
        PgPool.connect(DSN, max_connections=2)
        PgPool.connect(DSN)                     # unspecified options: the pool as it is
        PgPool.connect(DSN, max_connections=2)  # same settings
        try:
            PgPool.connect(DSN + "?application_name=other")
        except ValueError as e:
            print("REFUSED", e)
        print("STILL_WORKS", PgPool.connect(DSN).fetch_scalar("SELECT 1"))
    """)
    assert "REFUSED PgPool is already connected to a different DSN" in out, out
    assert "STILL_WORKS 1" in out
    # The DSN may hold a password; the message must not echo it.
    assert "application_name=other" not in out


def test_second_connect_with_other_settings_raises():
    out = _run("""
        PgPool.connect(DSN, max_connections=2)
        for kwargs in ({"max_connections": 3}, {"acquire_timeout_secs": 5}):
            try:
                PgPool.connect(DSN, **kwargs)
            except ValueError as e:
                print("REFUSED", e)
        print("STILL_WORKS", PgPool.connect(DSN).fetch_scalar("SELECT 1"))
    """)
    assert "REFUSED PgPool is already connected with max_connections=2, not 3" in out, out
    assert "REFUSED PgPool is already connected with acquire_timeout_secs=30, not 5" in out, out
    assert "STILL_WORKS 1" in out


# ---------------------------------------------------------------------------
# Item 5 — a cursor shared by two threads
# ---------------------------------------------------------------------------


def test_second_thread_waits_instead_of_stopping_early(pool):
    """While one thread waited for the next batch it held the channel, and a second
    thread calling `next` got StopIteration although rows were still coming."""
    cursor = pool.fetch_iter("SELECT i, pg_sleep(0.3) FROM generate_series(1, 3) i")
    first: list = []
    waiter = threading.Thread(target=lambda: first.append(next(cursor)))
    waiter.start()
    time.sleep(0.1)  # the waiter is now blocked on the first batch
    second = next(cursor)
    waiter.join(timeout=10)
    rest = list(cursor)
    got = sorted(r["i"] for r in first + [second] + rest)
    assert got == [1, 2, 3]


# ---------------------------------------------------------------------------
# Items 2 + 3 inside a sub-interpreter worker: the stdlib types and the
# exception classes are per interpreter
# ---------------------------------------------------------------------------

_WORKER_APP = '''
import os
import uuid
import decimal
from pyronova import Pyronova
from pyronova.db import PgPool, UniqueViolation

pool = PgPool.connect(os.environ["PYRONOVA_TEST_PG_DSN"])
pool.execute("DROP TABLE IF EXISTS m1c_worker")
pool.execute("CREATE TABLE m1c_worker (k uuid PRIMARY KEY, n numeric)")
app = Pyronova()

@app.get("/__ping")
def ping(req):
    return "pong"

@app.get("/kinds")
def kinds(req):
    k = uuid.uuid4()
    pool.execute("INSERT INTO m1c_worker VALUES ($1, $2)", k, decimal.Decimal("1.50"))
    try:
        pool.execute("INSERT INTO m1c_worker VALUES ($1, $2)", k, None)
        unique = "not raised"
    except UniqueViolation as e:
        unique = e.sqlstate
    row = pool.fetch_one("SELECT k, n, now() AS t, current_date AS d FROM m1c_worker WHERE k = $1", k)
    return {
        "types": [type(row[c]).__name__ for c in ("k", "n", "t", "d")],
        "same_key": row["k"] == k,
        "n": str(row["n"]),
        "unique": unique,
    }

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ["PYRONOVA_PORT"]), mode="subinterp", workers=2)
'''


def test_stdlib_types_and_typed_errors_in_a_worker(tmp_path):
    import json
    import signal
    import socket
    import urllib.request

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    script = tmp_path / "m1c_worker_app.py"
    script.write_text(_WORKER_APP)
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        env={**os.environ, "PYRONOVA_PORT": str(port)},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True,
    )
    try:
        deadline = time.time() + 15
        while True:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/__ping", timeout=0.5)
                break
            except OSError:
                if time.time() > deadline or proc.poll() is not None:
                    proc.kill()
                    raise AssertionError(proc.communicate()[0].decode(errors="replace")[-4000:])
                time.sleep(0.1)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/kinds", timeout=10) as r:
            got = json.loads(r.read())
    finally:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=10)
    assert got == {
        "types": ["UUID", "Decimal", "datetime", "date"],
        "same_key": True,
        "n": "1.50",
        "unique": "23505",
    }
