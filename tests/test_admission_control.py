"""Regression for the eager-eval DOS (audit round 5 bug #1).

Before the fix, `handle_request_subinterp` collected the entire
request body (up to `max_body_size`, default 10 MB) *before* checking
if the worker channel could accept. A flood of concurrent uploads
could pile N × max_body_size into RAM while each request waited for
a full queue — 5000 × 10 MB = 50 GB, OOM'd.

Now a `tokio::sync::Semaphore` on `InterpreterPool` is acquired
*before* the body collect. No permit → 503 Overloaded, body stays
in the kernel TCP buffer.

The ordering (permit before body) and the rejection (503 + CORS, counted
as dropped) are covered behaviourally by
tests/test_review_m2.py::test_pool_admission_rejects_large_bodies_past_the_permit_budget.
"""

import pathlib

_REPO = pathlib.Path(__file__).parent.parent


def test_pool_exposes_submit_semaphore():
    """The InterpreterPool struct carries the Arc<Semaphore> so it can be
    reached from handle_request_subinterp."""
    src = "\n".join(p.read_text() for p in (_REPO / "src/python").glob("*.rs"))
    assert "submit_semaphore: Arc<tokio::sync::Semaphore>" in src, (
        "InterpreterPool must hold Arc<tokio::sync::Semaphore> as submit_semaphore"
    )
    # It's populated in InterpreterPool::new with a non-zero permit budget.
    # Rough check: the permit count uses `n * 128` (matches channel capacity).
    assert "total_permits = n * 128" in src, (
        "permit count should match channel capacity (n * 128) so permit-holders "
        "are guaranteed to find a slot when they reach submit()"
    )
