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
are visible to every hook and helper called from the same thread or
awaited coroutine, and cleared before the next request begins.

Under the hood:

- Backed by a ``ContextVar[dict]`` so async handlers inherit the scope
  across ``await`` boundaries without extra plumbing.
- Each before-request hook (installed by ``reset_context_on_request``,
  which Pyronova wires automatically when you enable request-id or metrics)
  replaces the stored dict, so leftover keys from a recycled worker
  thread never leak.

``request_id()`` is a dedicated accessor because it's the canonical
correlation ID everyone needs and we don't want every caller to know
the magic key. Other values live under user-chosen keys.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any


_REQUEST_ID_KEY = "__pyronova_request_id__"

# Sentinel marking "no per-request dict installed yet". Using a unique
# object() rather than a shared empty `{}` as the ContextVar default means
# the copy-on-write guard in set() keys on identity that nothing else can
# forge: a caller cannot accidentally (or maliciously) store the sentinel,
# so two requests can never end up sharing one mutable dict (arc finding
# context-22). It also makes clear()/reset restore the true "unset" state
# so the next set() allocates fresh instead of copying a stale `{}`
# (arc finding context-23).
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


def _reset_for_new_request() -> None:
    """Called by Pyronova's internal before-request hook to start each
    request with a fresh (unset) scope."""
    _current.set(_UNSET)


__all__ = ["ctx"]
