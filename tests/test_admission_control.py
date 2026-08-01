"""Regression for the eager-eval DOS (audit round 5 bug #1).

Before the fix, `handle_request_subinterp` collected the entire
request body (up to `max_body_size`, default 10 MB) *before* checking
if the worker channel could accept. A flood of concurrent uploads
could pile N × max_body_size into RAM while each request waited for
a full queue — 5000 × 10 MB = 50 GB, OOM'd.

Now a `tokio::sync::Semaphore` on `InterpreterPool` is acquired
*before* the body collect. No permit → 503 Overloaded, body stays
in the kernel TCP buffer.

We verify the submit_semaphore is structurally wired in (the permit
is acquired in the right place) and that the permit lifecycle
defaults are correct. A true DOS simulation needs real traffic.
"""

import pathlib

_REPO = pathlib.Path(__file__).parent.parent


def test_submit_semaphore_wired_before_body_collect():
    src = (_REPO / "src/handlers/subinterp.rs").read_text()
    # The permit must be taken BEFORE the body-collect phase. The
    # body-collect is guarded by `if is_stream_route` / `else` on
    # the `body_obj = req.into_body()` — find the submit_semaphore
    # call and assert it comes before that branch.
    acquire_idx = src.find("submit_semaphore.clone().try_acquire_owned()")
    assert acquire_idx != -1, (
        "handle_request_subinterp must try_acquire_owned() a permit "
        "from pool.submit_semaphore before collecting the request body"
    )
    # The body-decide `let (body_bytes, body_stream_rx_early) = if is_stream_route`
    # must come AFTER the acquire.
    body_decide_idx = src.find(
        "let (body_bytes, body_stream_rx_early) = if is_stream_route"
    )
    assert body_decide_idx != -1
    assert acquire_idx < body_decide_idx, (
        "permit acquire must precede the body-collect — otherwise a full "
        "queue lets N × max_body_size of in-flight uploads pile into RAM"
    )


def test_semaphore_reject_returns_503():
    """Confirm the rejection branch is 503 + CORS-applied, not a panic
    or silent drop (the CORS 404 bug from round 3 — don't regress it)."""
    src = (_REPO / "src/handlers/subinterp.rs").read_text()
    # The Err branch of try_acquire_owned should construct an
    # overloaded_response and apply_cors before returning.
    # Search for the critical pieces close to the acquire site.
    idx = src.find("submit_semaphore.clone().try_acquire_owned()")
    assert idx != -1, "submit_semaphore.clone().try_acquire_owned() not found in subinterp.rs"
    window = src[idx:idx + 1500]
    assert "overloaded_response" in window, (
        "rejection must return 503 (overloaded_response), not 500 or silent drop"
    )
    assert "apply_cors" in window, (
        "rejection path must still apply CORS headers — the CORS-404 "
        "bug we fixed in round 3 must not regress here"
    )
    assert "DROPPED_REQUESTS" in window, (
        "rejection should bump the dropped-requests metric for observability"
    )


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
