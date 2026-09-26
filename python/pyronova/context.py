"""Request-scoped context — carry values through handlers and hooks.

Usage::

    from pyronova.context import ctx

    @app.before_request
    def tag(req):
        ctx.set("user_id", req.headers.get("x-user"))

    @app.get("/me")
    def me(req):
        return {"user": ctx.get("user_id"), "trace": ctx.request_id()}

The context is a per-request dictionary. Values set during the request
are visible to the request's hooks, its handler, and everything they call
or await; the next request starts empty.

Under the hood it is a ``ContextVar[dict]``, and the server runs each
request's before-hooks, handler and after-hooks inside a fresh
``contextvars.Context``, so nothing set by one request is visible to another,
on any thread. An ``async def`` hook or handler runs in that same context,
so what it sets is seen by the rest of the request, sync or async.

``request_id()`` is a dedicated accessor because it's the canonical
correlation ID everyone needs and we don't want every caller to know
the magic key. Other values live under user-chosen keys.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any


_REQUEST_ID_KEY = "__pyronova_request_id__"

# "No per-request dict yet": a private object, not a shared `{}` default, so no caller
# can hold the default and two requests never share one mutable dict.
_UNSET: Any = object()
_current: ContextVar[Any] = ContextVar("pyronova_ctx", default=_UNSET)


class _Ctx:
    """Facade over the ``ContextVar``. Module-level ``ctx`` is the only
    instance users need."""

    def get(self, key: str, default: Any = None) -> Any:
        d = _current.get()
        if d is _UNSET:
            return default
        return d.get(key, default)

    def set(self, key: str, value: Any) -> None:
        # Copy-on-write: never mutate a dict stored in an outer scope.
        d = _current.get()
        if d is _UNSET:
            d = {}
        else:
            d = dict(d)
        d[key] = value
        _current.set(d)

    def clear(self) -> None:
        _current.set(_UNSET)

    def request_id(self) -> str | None:
        return self.get(_REQUEST_ID_KEY)

    def set_request_id(self, rid: str) -> None:
        self.set(_REQUEST_ID_KEY, rid)

    def snapshot(self) -> dict[str, Any]:
        """Return a **shallow** copy of the current context dict.

        Top-level keys are copied, but nested mutable values (lists, dicts)
        are shared by reference with the live context — mutating them after
        snapshotting leaks across the boundary. If you need an isolated copy
        to hand to a background task, deep-copy the result yourself
        (``copy.deepcopy(ctx.snapshot())``).
        """
        d = _current.get()
        if d is _UNSET:
            return {}
        return dict(d)


ctx = _Ctx()


__all__ = ["ctx"]
