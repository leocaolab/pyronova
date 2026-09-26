"""Running a sync or async callable with a timeout: readiness checks and MCP handlers.

The call runs on a thread of its own, so the caller can stop waiting even when the call
never returns (a Python thread can't be killed; a hung one is abandoned). A coroutine or
other awaitable it returns is driven on that thread's own event loop, so the caller may
itself be inside a running loop. The thread is a daemon wherever the interpreter allows
one (main; a sub-interpreter worker does not), so a hung call never holds up exit there.
"""

from __future__ import annotations

import _thread
import asyncio
import contextvars
import inspect
import threading
import time
from typing import Any, Callable


class _Run:
    """One call of ``fn`` on a new thread, in a copy of the caller's context."""

    def __init__(self, fn: Callable[[], Any], timeout: float) -> None:
        self._done = threading.Event()
        self._value: Any = None
        self._error: BaseException | None = None
        deadline = time.monotonic() + timeout
        context = contextvars.copy_context()
        thread = threading.Thread(
            target=context.run,
            args=(self._run, fn, deadline),
            name=f"pyronova-bounded:{getattr(fn, '__qualname__', repr(fn))}",
            daemon=_thread.daemon_threads_allowed(),
        )
        thread.start()

    def _run(self, fn: Callable[[], Any], deadline: float) -> None:
        try:
            result = fn()
            if inspect.isawaitable(result):
                result = asyncio.run(_await_by(result, deadline))
            self._value = result
        except BaseException as e:  # noqa: BLE001 — handed to the waiting caller
            self._error = e
        finally:
            self._done.set()

    def done(self) -> bool:
        return self._done.is_set()

    def result(self, timeout: float) -> Any:
        """The call's value, or its exception re-raised; ``TimeoutError`` when it has not
        finished within ``timeout``."""
        if not self._done.wait(timeout):
            raise TimeoutError(f"did not finish within {timeout:g}s")
        if self._error is not None:
            raise self._error
        return self._value


async def _await_by(awaitable: Any, deadline: float) -> Any:
    """``awaitable``'s value, cancelled at ``deadline`` (so its thread ends too)."""
    return await asyncio.wait_for(awaitable, max(0.0, deadline - time.monotonic()))


def call_with_timeout(fn: Callable[[], Any], timeout: float) -> Any:
    """``fn()`` (driven to a value if it returns an awaitable), waited for at most
    ``timeout`` seconds: ``TimeoutError`` past it. Each call runs on its own thread."""
    return _Run(fn, timeout).result(timeout)


class BoundedCall:
    """``fn`` called with a timeout, at most one run at a time: while a run is still going
    (a hung check), a new call waits on that run instead of starting another thread."""

    def __init__(self, fn: Callable[[], Any], timeout: float) -> None:
        self.fn = fn
        self.timeout = timeout
        self._lock = threading.Lock()
        self._run: _Run | None = None

    def __call__(self) -> Any:
        with self._lock:
            if self._run is None or self._run.done():
                self._run = _Run(self.fn, self.timeout)
            run = self._run
        return run.result(self.timeout)
