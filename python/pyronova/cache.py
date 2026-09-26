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

The cache is per interpreter (each sub-interpreter worker has its own dict):
with N workers a hot endpoint runs its handler up to N times per TTL window,
and every other hit in the window is a dict lookup. For one miss per TTL
across all workers, cache in ``app.state`` yourself.

A handler with path params works as usual (``def item(req, item_id)``): the
wrapper passes them on, and the default key, the path, already tells
``/item/1`` from ``/item/2``.

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

# Entries per handler. Keys come from the request, so a client cycling unique paths
# would otherwise grow the dict until the worker runs out of memory. On overflow the
# whole dict is dropped: the TTL is short, so refilling it is cheap.
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
            # Under the lock: one route's handler runs on several threads at once.
            with _lock:
                entry = _cache.get(k)
            if entry is not None and entry[1] > now:
                return Response(body=entry[0], content_type="application/json")
            return None

        def _store(k: str, body: bytes) -> None:
            # From store time: a handler slower than the TTL must not store an entry
            # that is already stale.
            expires = time.monotonic() + ttl
            with _lock:
                if len(_cache) >= _MAX_ENTRIES and k not in _cache:
                    _cache.clear()
                _cache[k] = (body, expires)

        def _safe_key(req):
            # A raising key function costs the cache, not the request.
            try:
                return key_fn(req), True
            except Exception:
                _log.exception("cached_json key function raised; bypassing cache")
                return None, False

        if _is_async:
            @functools.wraps(handler)
            async def async_wrapper(req, **path_params):
                now = time.monotonic()
                k, ok = _safe_key(req)
                if ok:
                    cached = _hit(k, now)
                    if cached is not None:
                        return cached
                result = await handler(req, **path_params)
                if isinstance(result, Response):
                    return result
                # Not JSON-serializable here: the engine's own serialization answers it.
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
        def wrapper(req, **path_params):
            now = time.monotonic()
            k, ok = _safe_key(req)
            if ok:
                cached = _hit(k, now)
                if cached is not None:
                    return cached
            result = handler(req, **path_params)
            # An explicit Response (custom status / headers) is neither cached nor rewrapped.
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
