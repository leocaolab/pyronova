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
import threading
import time
import uuid
from typing import TYPE_CHECKING

from pyronova.engine import Response

if TYPE_CHECKING:
    from pyronova.app import Pyronova


_STATUS_CLASSES = ("1xx", "2xx", "3xx", "4xx", "5xx")
_TRACKED_METHODS = ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS")

# Thread-local start timestamp. before_request and after_request for the
# same request run on the same worker thread, so a thread-local is
# sufficient in both GIL and sub-interpreter modes.
_tls = threading.local()


def install_request_id(app: "Pyronova", header: str) -> None:
    from pyronova.context import ctx, _reset_for_new_request

    header_lower = header.lower()

    def _before(req):
        # Fresh context per request — prevents leftover keys from a
        # recycled worker thread from leaking into the next caller.
        _reset_for_new_request()
        # Stash the incoming id (or a freshly-minted one) onto the thread so
        # the after-hook can echo it back without mutating the frozen req.
        headers = req.headers
        rid = headers.get(header_lower) or headers.get(header) or uuid.uuid4().hex
        _tls.request_id = rid
        ctx.set_request_id(rid)
        return None

    def _after(req, resp):
        rid = getattr(_tls, "request_id", None)
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
        _tls.metrics_start_ns = time.monotonic_ns()
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

            start = getattr(_tls, "metrics_start_ns", None)
            _tls.metrics_start_ns = None
            if start is not None:
                elapsed_us = max(0, (time.monotonic_ns() - start) // 1000)
                state.incr("_m:lat:sum_us", int(elapsed_us))
                state.incr("_m:lat:count", 1)
        except Exception:
            # Log via stdlib (routed to Rust tracing in sub-interps);
            # do NOT propagate.
            import logging
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


_log = logging.getLogger("pyronova.observability")


def _read_int(state, key: str) -> int:
    v = state.get(key)
    if v is None:
        return 0
    try:
        return int(v)
    except (ValueError, TypeError):
        _log.warning("metrics key %r has non-integer value %r", key, v)
        return 0


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
