"""Response caching decorators.

Trade freshness for throughput: stash a handler's JSON-serialized bytes
for a fixed TTL, serve hits straight from cache without re-running the
handler or re-running json.dumps. Designed for public read endpoints
that tolerate brief staleness (``/health``, ``/status``, homepage
leaderboards, public product listings, rate-limited feeds).

Usage::

    from pyronova import Pyronova
    from pyronova.cache import cached_json

    app = Pyronova()

    @app.get("/health")
    @cached_json(ttl=1.0)
    def health(req):
        return {"status": "ok"}

Order matters: ``@cached_json`` must sit **inside** ``@app.get`` so the
framework sees the wrapped function, not the raw handler.

Cache is per sub-interpreter (each TPC worker owns its own dict). With
N workers a hot endpoint does up to N handler evaluations per TTL
window; within the window every subsequent hit is a pure dict lookup.
For cross-worker shared caching back this with ``app.state`` manually —
a few extra Bytes copies, but one miss per TTL across the whole fleet.

Cache key is the request path only. Query strings are ignored. If you
need query-aware caching, pre-compose the key yourself:

    @app.get("/search")
    @cached_json(ttl=5.0, key=lambda req: req.path + "?" + req.query)
    def search(req): ...
"""

from __future__ import annotations

import functools
import inspect
import json
import logging
import threading
import time
from typing import Callable

from .app import Response

__all__ = ["cached_json"]

_log = logging.getLogger("pyronova.cache")

# Hard cap on per-handler cache entries. The cache is keyed by request
# path (or a user key_fn), so a high-cardinality endpoint (/item/1,
# /item/2, ...) or a hostile client cycling unique paths would otherwise
# grow the dict without bound until the worker OOMs (arc finding cache-17).
# On overflow we drop the whole dict — simple, allocation-free, and the
# TTL is short so the rebuild cost is bounded.
_MAX_ENTRIES = 10_000


def cached_json(ttl: float, key: Callable | None = None):
    """Cache a handler's JSON response for ``ttl`` seconds (per worker).

    :param ttl: lifetime in seconds. Must be > 0. Hits older than this
        re-run the handler and replace the cached entry.
    :param key: optional ``f(req) -> str`` to derive the cache key. Default
        keys on ``req.path`` alone.
    """
    if ttl <= 0:
        raise ValueError("cached_json ttl must be > 0")
    key_fn = key if key is not None else (lambda req: req.path)

    def decorator(handler):
        _cache: dict[str, tuple[bytes, float]] = {}
        _lock = threading.Lock()
        _is_async = inspect.iscoroutinefunction(handler)

        def _serialize(result) -> bytes:
            if isinstance(result, (bytes, bytearray)):
                return bytes(result)
            if isinstance(result, str):
                return result.encode("utf-8")
            return json.dumps(result, separators=(",", ":")).encode("utf-8")

        def _hit(k: str, now: float):
            # Read under the lock: the async path is explicitly multi-thread
            # (threading.Lock guards writes), so an unlocked .get() can tear
            # against a concurrent resize (arc finding cache-16).
            with _lock:
                entry = _cache.get(k)
            if entry is not None and entry[1] > now:
                return Response(body=entry[0], content_type="application/json")
            return None

        def _store(k: str, body: bytes) -> None:
            # Expiry is measured from store time, not from request start —
            # a handler slower than the TTL must not insert an
            # already-stale entry that every later request re-computes
            # (arc finding cache-15).
            expires = time.monotonic() + ttl
            with _lock:
                if len(_cache) >= _MAX_ENTRIES and k not in _cache:
                    _cache.clear()
                _cache[k] = (body, expires)

        def _safe_key(req):
            # A raising key_fn must degrade to an uncached call, not 500
            # the request (arc finding cache-13).
            try:
                return key_fn(req), True
            except Exception:
                _log.exception("cached_json key function raised; bypassing cache")
                return None, False

        if _is_async:
            @functools.wraps(handler)
            async def async_wrapper(req):
                now = time.monotonic()
                k, ok = _safe_key(req)
                if ok:
                    cached = _hit(k, now)
                    if cached is not None:
                        return cached
                result = await handler(req)
                if isinstance(result, Response):
                    return result
                # A non-serializable handler result must fall back to the
                # framework's normal serialization path uncached, not raise
                # out of the wrapper (arc finding cache-14).
                try:
                    body = _serialize(result)
                except (TypeError, ValueError):
                    _log.exception("cached_json could not serialize result; returning uncached")
                    return result
                if ok:
                    _store(k, body)
                return Response(body=body, content_type="application/json")
            return async_wrapper

        @functools.wraps(handler)
        def wrapper(req):
            now = time.monotonic()
            k, ok = _safe_key(req)
            if ok:
                cached = _hit(k, now)
                if cached is not None:
                    return cached
            result = handler(req)
            # Handler returned an explicit Response — user is signalling
            # a custom status / headers; don't cache, don't rewrap.
            if isinstance(result, Response):
                return result
            try:
                body = _serialize(result)
            except (TypeError, ValueError):
                _log.exception("cached_json could not serialize result; returning uncached")
                return result
            if ok:
                _store(k, body)
            return Response(body=body, content_type="application/json")

        return wrapper
    return decorator
