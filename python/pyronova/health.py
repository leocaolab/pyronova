"""Kubernetes-style health probes — ``/livez`` + ``/readyz``.

Wire-up::

    from pyronova import Pyronova
    from pyronova.db import PgPool

    app = Pyronova()
    app.enable_health_probes()   # /livez + /readyz auto-registered

    pool = PgPool.connect(...)

    @app.readiness_check("db")
    def _db_ready():
        pool.fetch_scalar("SELECT 1")         # raises on failure

    @app.readiness_check("cache")
    async def _cache_ready():
        await redis.ping()

Behaviour:

- ``GET /livez`` always returns ``200 {"status":"alive"}``. The process
  is running; that's all this probe answers. k8s uses it to decide
  whether to restart the pod.
- ``GET /readyz`` runs every registered check. Success → ``200
  {"status":"ready","checks":{...}}``. Any failure (exception or
  falsy-non-None return) → ``503 {"status":"not_ready","checks":{...}}``.
  k8s uses this to gate traffic.

Checks run sequentially in the handler. Keep them fast — a readyz
handler is a hot loop during rolling deploys. Sync + async both work;
async checks are awaited from the async pool.
"""

from __future__ import annotations

import asyncio
import json
import inspect
import logging
from typing import Any, Awaitable, Callable, Union

from pyronova.engine import Response

_log = logging.getLogger(__name__)


CheckFn = Union[Callable[[], Any], Callable[[], Awaitable[Any]]]

# A readiness check must fail fast. A hung check (DB deadlock, network
# partition without a connection timeout, infinite loop) would otherwise
# block the readyz handler thread forever — and k8s probes timing out
# keep spawning fresh hung threads until the worker pool is exhausted.
# Bound every check so the endpoint returns an explicit 503 instead
# (arc finding health-33).
_CHECK_TIMEOUT_S = 10.0


def _drive(coro: Awaitable[Any]) -> Any:
    """Run a coroutine to completion from sync code, even if this thread
    already has a running event loop.

    ``asyncio.run()`` raises ``RuntimeError`` when called from a thread
    with a running loop (e.g. an async request worker). In that case we
    offload to a dedicated thread that owns its own fresh loop, so the
    readyz handler works in both sync and async deployments
    (arc finding health-32).

    Every check is bounded by ``_CHECK_TIMEOUT_S`` so a hung coroutine
    surfaces as ``TimeoutError`` (recorded as a failed check) rather than
    blocking the handler thread indefinitely.
    """
    bounded = asyncio.wait_for(coro, _CHECK_TIMEOUT_S)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop running on this thread — create+close one properly.
        return asyncio.run(bounded)
    # A loop is already running here; asyncio.run() would blow up. Drive
    # the coroutine on a worker thread that has no running loop. The inner
    # wait_for cancels the coroutine on timeout; the slightly-longer
    # .result() timeout is a backstop in case the worker itself wedges.
    import concurrent.futures

    # NOTE: do NOT use ThreadPoolExecutor as a context manager here. Its
    # __exit__ calls shutdown(wait=True), which blocks until the worker future
    # finishes. If the check wedges (e.g. a coroutine that swallows the
    # wait_for cancellation), .result() raises TimeoutError as intended — but
    # shutdown(wait=True) would then block forever joining the still-running
    # thread, defeating the timeout and re-exposing the exhausted-worker-pool
    # scenario this bound exists to prevent. shutdown(wait=False) abandons a
    # hung worker (it can't be killed in Python) so _drive returns promptly
    # and the TimeoutError propagates to be recorded as a failed check.
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        return ex.submit(asyncio.run, bounded).result(timeout=_CHECK_TIMEOUT_S + 1.0)
    finally:
        ex.shutdown(wait=False)


def _run_checks_sync(checks: list[tuple[str, CheckFn]]) -> tuple[bool, dict[str, Any]]:
    """Run every check, catching exceptions. Returns (all_ok, results)."""
    results: dict[str, Any] = {}
    all_ok = True
    for name, fn in checks:
        try:
            if inspect.iscoroutinefunction(fn):
                res = _drive(fn())
            else:
                res = fn()
                # A plain function that *returns* a coroutine/awaitable
                # (e.g. `def c(): return redis.ping()`) would otherwise be
                # recorded as passing with the un-awaited awaitable as its
                # truthy result — the check never actually runs. Drive it
                # (arc finding health-33).
                if inspect.isawaitable(res):
                    res = _drive(res)
            # Treat False OR any other falsy non-None value as failure,
            # matching the docstring contract.
            if res is not None and not res:
                results[name] = {"ok": False, "error": "check returned falsy value"}
                all_ok = False
            else:
                results[name] = {"ok": True}
        except Exception as e:  # noqa: BLE001 — probe must never crash
            _log.exception("readiness check %r raised", name)
            results[name] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
            all_ok = False
    return all_ok, results


def _build_livez_handler():
    body = json.dumps({"status": "alive"}).encode("utf-8")

    def livez(req):
        return Response(body=body, content_type="application/json")

    return livez


def _build_readyz_handler(checks: list[tuple[str, CheckFn]]):
    def readyz(req):
        ok, results = _run_checks_sync(checks)
        payload = json.dumps({
            "status": "ready" if ok else "not_ready",
            "checks": results,
        }).encode("utf-8")
        return Response(
            body=payload,
            status_code=200 if ok else 503,
            content_type="application/json",
        )

    return readyz


__all__ = ["CheckFn"]
