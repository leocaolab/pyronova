"""Two threads iterating one PgCursor must neither deadlock nor lose or repeat rows.

`PgCursor.__next__` waits for the next batch with the GIL released. If it held the
cursor's lock while waiting, a second thread holding the GIL and wanting the lock would
deadlock against it. Skipped unless `PYRONOVA_TEST_PG_DSN` is set.
"""
import os
import threading

import pytest

from pyronova.db import PgPool

PG_DSN = os.environ.get("PYRONOVA_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    PG_DSN is None,
    reason="PYRONOVA_TEST_PG_DSN not set — skipping Postgres integration tests",
)


def test_two_threads_share_one_cursor():
    pool = PgPool.connect(PG_DSN)
    n = 5000
    cursor = pool.fetch_iter("SELECT generate_series(1, $1::int) AS i", n)
    seen: list[list[int]] = [[], []]
    errors: list[BaseException] = []

    def drain(k: int) -> None:
        try:
            for row in cursor:
                seen[k].append(row["i"])
        except BaseException as e:  # noqa: BLE001 — reported by the assertion below
            errors.append(e)

    threads = [threading.Thread(target=drain, args=(k,)) for k in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in threads), "cursor iteration deadlocked"
    assert errors == []
    got = seen[0] + seen[1]
    assert len(got) == len(set(got)), "a row was delivered twice"
    # A thread that finds the receiver checked out by the other one stops early,
    # so the union is what must be complete.
    assert sorted(got) == list(range(1, n + 1))
