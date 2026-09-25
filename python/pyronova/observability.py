"""Observability helpers — X-Request-ID + Prometheus ``/metrics``.

Two opt-in toggles on ``Pyronova``:

    app.enable_request_id()   # guarantees X-Request-ID on every response
    app.enable_metrics()      # GET /metrics → Prometheus text format

Both ride on ``before_request`` / ``after_request`` hooks and keep state
in ``app.state`` (the shared DashMap), so counters aggregate correctly
across sub-interpreter workers.

Metrics exposed (v1, RED-style without histograms):

- ``pyronova_http_requests_total`` — global request counter
- ``pyronova_http_requests_by_class_total{class="2xx|3xx|4xx|5xx"}``
- ``pyronova_http_requests_by_method_total{method="GET|POST|..."}``
- ``pyronova_http_request_duration_seconds_sum``
- ``pyronova_http_request_duration_seconds_count``

(Latency is tracked as a running sum + count; compute avg via
``sum / count`` in the dashboard. Per-bucket histograms are a v1.1
upgrade.)

Why ``app.state`` and not a Python dict: in sub-interpreter mode, each
worker has its own Python globals, so a module-level ``defaultdict``
would silently fragment counts per worker. ``app.state.incr`` is one
atomic DashMap op shared by every interpreter.
"""

from __future__ import annotations

import logging
import time
from contextvars import ContextVar
from typing import TYPE_CHECKING

from pyronova.engine import Response

if TYPE_CHECKING:
    from pyronova.app import Pyronova


_STATUS_CLASSES = ("1xx", "2xx", "3xx", "4xx", "5xx")
_TRACKED_METHODS = ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS")

# Per-request values the before hook hands to the after hook. ContextVars, not a
# thread-local: async requests interleave on one event-loop thread, and each one runs in
# its own context, so a thread-local would hand one request's value to another.
_request_id: ContextVar[str | None] = ContextVar("pyronova_obs_request_id", default=None)
_metrics_start_ns: ContextVar[int | None] = ContextVar("pyronova_obs_metrics_start_ns", default=None)


def install_request_id(app: "Pyronova", header: str) -> None:
    from pyronova.context import ctx

    header_lower = header.lower()

    def _before(req):
        # The engine wrote the request's id once, as `req.request_id` (the client's `header`
        # value when it sent a usable one); a 5xx reports the same id. Stash it for the
        # after-hook, which echoes it without mutating the frozen req.
        rid = req.request_id
        _request_id.set(rid)
        ctx.set_request_id(rid)
        return None

    def _after(req, resp):
        rid = _request_id.get()
        if rid is None:
            return resp
        # HTTP header names are case-insensitive, but a Python dict is not.
        # If resp.headers already carries any case-variant of this header
        # (e.g. "X-Request-ID" vs our "x-request-id"), a naive {**, lower:
        # rid} merge would emit BOTH as separate response headers. Drop any
        # existing case-variant first, then set the canonical lower-case key
        # (arc finding observability-45).
        merged = {k: v for k, v in resp.headers.items() if k.lower() != header_lower}
        merged[header_lower] = rid
        return Response(
            body=resp.body,
            status_code=resp.status_code,
            content_type=resp.content_type,
            headers=merged,
        )

    app.before_request(_before)
    app.after_request(_after)


def install_metrics(app: "Pyronova", path: str) -> None:
    state = app.state

    def _before(req):
        _metrics_start_ns.set(time.monotonic_ns())
        return None

    def _after(req, resp):
        # Don't count the /metrics scrape itself — it would turn the
        # counter into a self-fulfilling load generator.
        if req.path == path:
            return resp

        # Observability MUST NOT break the request. Wrap every counter
        # touch so a transient state.incr failure (DashMap contention,
        # value-type drift, etc.) logs and continues instead of raising
        # into the response path (arc finding observability-2).
        try:
            status = resp.status_code
            state.incr("_m:req:total", 1)
            state.incr(f"_m:req:class:{status // 100}xx", 1)
            method = req.method.upper()
            if method in _TRACKED_METHODS:
                state.incr(f"_m:req:method:{method}", 1)

            start = _metrics_start_ns.get()
            if start is not None:
                elapsed_us = max(0, (time.monotonic_ns() - start) // 1000)
                state.incr("_m:lat:sum_us", int(elapsed_us))
                state.incr("_m:lat:count", 1)
        except Exception:
            # Log via stdlib (routed to Rust tracing in sub-interps);
            # do NOT propagate.
            logging.getLogger("pyronova.observability").exception(
                "metrics _after hook failed; request unaffected"
            )
        return resp

    app.before_request(_before)
    app.after_request(_after)

    def _metrics_handler(req):
        return Response(
            body=_render_prometheus(state),
            content_type="text/plain; version=0.0.4; charset=utf-8",
        )

    # gil=True: /metrics reads cross-interpreter counters via app.state;
    # that works from any interp, but keeping the handler itself on the
    # main interp avoids dispatching a trivial scrape to a worker.
    app.get(path, gil=True)(_metrics_handler)


class CorruptMetric(ValueError):
    """A metrics counter in ``app.state`` holds something that is not an integer."""


def _read_int(state, key: str) -> int:
    """The counter ``key``; one never incremented is 0. A value that is not an integer is
    ``CorruptMetric`` (the scrape fails, with the key and value in the log): reporting it
    as 0 would hand the monitoring a made-up number."""
    v = state.get(key)
    if v is None:
        return 0
    try:
        return int(v)
    except (ValueError, TypeError):
        raise CorruptMetric(f"metrics counter {key!r} holds {v!r}, not an integer") from None


def _render_prometheus(state) -> str:
    total = _read_int(state, "_m:req:total")

    parts: list[str] = []
    parts.append("# HELP pyronova_http_requests_total Total HTTP requests handled.")
    parts.append("# TYPE pyronova_http_requests_total counter")
    parts.append(f"pyronova_http_requests_total {total}")

    parts.append("# HELP pyronova_http_requests_by_class_total Requests by status class.")
    parts.append("# TYPE pyronova_http_requests_by_class_total counter")
    for cls in _STATUS_CLASSES:
        v = _read_int(state, f"_m:req:class:{cls}")
        parts.append(f'pyronova_http_requests_by_class_total{{class="{cls}"}} {v}')

    parts.append("# HELP pyronova_http_requests_by_method_total Requests by HTTP method.")
    parts.append("# TYPE pyronova_http_requests_by_method_total counter")
    for m in _TRACKED_METHODS:
        v = _read_int(state, f"_m:req:method:{m}")
        parts.append(f'pyronova_http_requests_by_method_total{{method="{m}"}} {v}')

    sum_us = _read_int(state, "_m:lat:sum_us")
    count = _read_int(state, "_m:lat:count")
    parts.append("# HELP pyronova_http_request_duration_seconds_sum Cumulative latency in seconds.")
    parts.append("# TYPE pyronova_http_request_duration_seconds_sum counter")
    parts.append(f"pyronova_http_request_duration_seconds_sum {sum_us / 1_000_000:.6f}")
    parts.append("# HELP pyronova_http_request_duration_seconds_count Samples in the latency sum.")
    parts.append("# TYPE pyronova_http_request_duration_seconds_count counter")
    parts.append(f"pyronova_http_request_duration_seconds_count {count}")

    parts.append("")  # trailing newline — Prometheus is tolerant but nicer
    return "\n".join(parts)
