"""`PgPool.*_async` result delivery (Layer 2 M1, C9): results, errors, concurrency,
cancellation and a closed loop.

Each case runs in a fresh interpreter so a stuck delivery shows up as a timeout, not as a
hung pytest process. Skipped unless `PYRONOVA_TEST_PG_DSN` is set.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

PG_DSN = os.environ.get("PYRONOVA_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    PG_DSN is None,
    reason="PYRONOVA_TEST_PG_DSN not set — skipping Postgres integration tests",
)

PRELUDE = """
import asyncio, sys, time
from pyronova.db import PgPool
pool = PgPool.connect(sys.argv[1])
"""


def _run(body: str, timeout: float = 60) -> str:
    code = PRELUDE + textwrap.dedent(body)
    out = subprocess.run(
        [sys.executable, "-c", code, PG_DSN],
        capture_output=True, text=True, timeout=timeout,
        env=dict(os.environ, RUST_BACKTRACE="0", PYRONOVA_LOG="1"),
    )
    text = out.stdout + out.stderr
    assert out.returncode == 0, text[-4000:]
    assert [line for line in text.splitlines() if "panicked at" in line] == [], text[-4000:]
    return text


def test_results_of_every_method():
    out = _run("""
        async def main():
            assert await pool.fetch_one_async("SELECT 1 AS a, $1::text AS b", "x") == {"a": 1, "b": "x"}
            assert await pool.fetch_one_async("SELECT 1 WHERE false") is None
            assert await pool.fetch_all_async("SELECT generate_series(1, 3) AS i") == [
                {"i": 1}, {"i": 2}, {"i": 3}]
            assert await pool.fetch_scalar_async("SELECT 41 + $1::int", 1) == 42
            # A pooled connection per statement: a TEMP table would be invisible to the next one.
            await pool.execute_async("DROP TABLE IF EXISTS m1_async_t")
            await pool.execute_async("CREATE TABLE m1_async_t (x int)")
            assert await pool.execute_async("INSERT INTO m1_async_t VALUES (1), (2)") == 2
            await pool.execute_async("DROP TABLE m1_async_t")
            print("RESULTS_OK")
        asyncio.run(main())
    """)
    assert "RESULTS_OK" in out


def test_error_becomes_runtime_error_with_the_database_message():
    out = _run("""
        async def main():
            try:
                await pool.fetch_all_async("SELECT * FROM no_such_table_m1")
            except RuntimeError as e:
                assert "no_such_table_m1" in str(e), str(e)
                print("ERROR_OK")
        asyncio.run(main())
    """)
    assert "ERROR_OK" in out


def test_many_concurrent_awaits():
    out = _run("""
        async def main():
            rows = await asyncio.gather(*(
                pool.fetch_scalar_async("SELECT $1::int * 2", i) for i in range(200)))
            assert rows == [i * 2 for i in range(200)], rows[:5]
            print("CONCURRENT_OK")
        asyncio.run(main())
    """)
    assert "CONCURRENT_OK" in out


def test_no_running_loop_is_an_error_not_a_hang():
    out = _run("""
        try:
            pool.fetch_all_async("SELECT 1")
        except RuntimeError as e:
            assert "running event loop" in str(e), str(e)
            print("NO_LOOP_OK")
    """)
    assert "NO_LOOP_OK" in out


def test_cancel_while_query_in_flight():
    """Cancelling the awaiting task cancels promptly and aborts the query's task; the late
    result must not be set on the cancelled future, and the pool keeps working."""
    out = _run("""
        async def main():
            task = asyncio.ensure_future(pool.fetch_one_async("SELECT pg_sleep(3)"))
            await asyncio.sleep(0.3)
            t0 = time.monotonic()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            assert time.monotonic() - t0 < 1.0, "cancellation waited for the query"
            assert task.cancelled()
            # The aborted query released its connection: the pool still serves.
            assert await asyncio.wait_for(pool.fetch_scalar_async("SELECT 7"), 5) == 7
            await asyncio.sleep(3.5)   # past the original query's end: nothing may fire late
            print("CANCEL_OK")

        errors = []
        loop = asyncio.new_event_loop()
        loop.set_exception_handler(lambda l, ctx: errors.append(ctx.get("message")))
        loop.run_until_complete(main())
        loop.close()
        assert errors == [], errors
    """)
    assert "CANCEL_OK" in out
    assert "InvalidStateError" not in out, out[-2000:]


def test_closed_loop_drops_the_result_and_says_so():
    """The loop is closed before the result arrives: delivery can't reach it, the process
    neither crashes nor hangs, the failure is logged, and later queries on a new loop work."""
    out = _run("""
        from pyronova.engine import init_logger
        init_logger("WARN", False, "text")
        loop = asyncio.new_event_loop()
        async def start():
            return pool.fetch_one_async("SELECT pg_sleep(0.5)")
        fut = loop.run_until_complete(start())
        loop.close()
        time.sleep(1.5)   # the result arrives after the loop is gone
        async def after():
            return await pool.fetch_scalar_async("SELECT 9")
        assert asyncio.run(after()) == 9
        print("CLOSED_LOOP_OK")
    """)
    assert "CLOSED_LOOP_OK" in out
    assert "the asyncio loop can't be reached" in out, out[-3000:]
