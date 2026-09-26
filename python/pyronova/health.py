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
  {"status":"ready","checks":{"db":{"ok":true},...}}``. Any failure
  (an exception, a timeout, or a ``False`` return) → ``503 {"status":"not_ready",
  "checks":{"db":{"ok":false},...},"request_id":"..."}``. The probe is
  unauthenticated, so why a check failed is never in the body: it goes
  to the log with the same request id. k8s uses this to gate traffic.

Checks run sequentially in the handler. Keep them fast — a readyz
handler is a hot loop during rolling deploys. Sync + async both work, and
each is bounded by the same timeout: one that takes longer fails. A check
that hangs is left running (a thread can't be killed), and later probes wait
on that same run rather than starting another: a stuck check costs one
thread, not one per probe.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Sequence, Union

from pyronova._bounded import BoundedCall
from pyronova._errors import log_server_error
from pyronova.engine import Response

_log = logging.getLogger(__name__)


CheckFn = Union[Callable[[], Any], Callable[[], Awaitable[Any]]]

# A readiness check must fail fast: a hung one (DB deadlock, a partition without a
# connection timeout) would otherwise hold the readyz handler forever while k8s keeps
# probing.
_CHECK_TIMEOUT_S = 10.0


@dataclass(frozen=True)
class ReadinessCheck:
    """A registered check: its name and the bounded call that runs it."""

    name: str
    call: BoundedCall

    @classmethod
    def of(cls, name: str, fn: CheckFn) -> ReadinessCheck:
        return cls(name, BoundedCall(fn, _CHECK_TIMEOUT_S))


def _passed(result: Any) -> bool:
    """A check fails by raising (a timeout included) or by returning ``False``; any other
    value, ``None`` included, passes."""
    return result is not False


def _run_checks_sync(
    checks: Sequence[ReadinessCheck], request_id: str
) -> tuple[bool, dict[str, Any]]:
    """Run every check. Returns (all_ok, results). A failure is logged with `request_id`;
    the results only say which checks passed."""
    outcomes = [(check.name, _run_check(check, request_id)) for check in checks]
    return all(ok for _, ok in outcomes), {name: {"ok": ok} for name, ok in outcomes}


def _run_check(check: ReadinessCheck, request_id: str) -> bool:
    """Whether ``check`` passed; why it failed goes to the log."""
    try:
        result = check.call()
    except Exception:  # noqa: BLE001 — a failing check is a 503, never a crash
        log_server_error(_log, request_id, "readiness check %r raised", check.name)
        return False
    if not _passed(result):
        _log.error("readiness check %r returned %r (request_id=%s)", check.name, result, request_id)
    return _passed(result)


def _build_livez_handler():
    body = json.dumps({"status": "alive"}).encode("utf-8")

    def livez(req):
        return Response(body=body, content_type="application/json")

    return livez


def _build_readyz_handler(checks: Sequence[ReadinessCheck]):
    def readyz(req):
        ok, results = _run_checks_sync(checks, req.request_id)
        body: dict[str, Any] = {"status": "ready" if ok else "not_ready", "checks": results}
        if not ok:
            body["request_id"] = req.request_id
        payload = json.dumps(body).encode("utf-8")
        return Response(
            body=payload,
            status_code=200 if ok else 503,
            content_type="application/json",
        )

    return readyz


__all__ = ["CheckFn", "ReadinessCheck"]

