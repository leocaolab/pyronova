"""Async engine — run in each async sub-interpreter worker (pool mode).

It drives a Python asyncio event loop that processes requests received from
Rust through `pyronova.engine._worker_recv` / `_worker_send` (Layer 2, C5).
It runs as the module `__pyronova_async_engine__`, in its own namespace; Rust
sets `WORKER_ID` (this worker's slot) and `POOL_ID` (its pool, the zombie
guard) in it before it runs. The handlers and hooks are the ones the user's
script registered on its app, indexed like the main interpreter's routes.
"""

import asyncio
import logging
import threading
import time

import pyronova.engine as _engine

_log = logging.getLogger("pyronova.async")

# Timeout for async handlers — 2s before Rust's 30s gateway timeout,
# so Python can abort cleanly instead of computing a result nobody wants.
_HANDLER_TIMEOUT = 28

# Fetched once (M4 review N1e): the script has run, so the app's table is final.
_HANDLERS = _engine._worker_app_handlers()
_BEFORE_HOOKS, _AFTER_HOOKS = _engine._worker_app_hooks()


def _error_response(status, text):
    return _engine.Response(text, status_code=status, content_type="text/plain")


async def _call(fn, *args):
    res = fn(*args)
    if asyncio.iscoroutine(res) or asyncio.isfuture(res) or hasattr(res, "__await__"):
        res = await res
    return res


async def _handle(handler, req):
    # Hooks and handler run in this request's own Task, so per-request state kept
    # in ContextVars (observability's request id, pyronova.context) stays apart
    # from concurrent requests on this loop (FR-14). Same order and semantics as
    # the sync worker path: a before hook that returns something short-circuits;
    # one that raises fails the request; after hooks get a Response and may
    # replace it; one that raises is logged and skipped.
    for hook in _BEFORE_HOOKS:
        res = await _call(hook, req)
        if res is not None:
            return _engine._worker_to_response(res)
    res = await _call(handler, req)
    res = _engine._worker_to_response(res)
    for hook in _AFTER_HOOKS:
        try:
            replaced = await _call(hook, req, res)
        except Exception:
            _log.exception("worker=%s after_request hook raised", WORKER_ID)
            continue
        if replaced is not None:
            res = _engine._worker_to_response(replaced)
    return res


async def _process_request(req_id, handler_idx, req):
    try:
        try:
            handler = _HANDLERS[handler_idx]
        except IndexError:
            _log.error("worker=%s invalid handler_idx=%r", WORKER_ID, handler_idx)
            _engine._worker_send(WORKER_ID, POOL_ID, req_id, _error_response(500, "routing error"))
            return
        # Bracket pattern: bound the request's lifetime so cancelled/timed-out
        # requests don't accumulate as phantom load in the event loop.
        res = await asyncio.wait_for(_handle(handler, req), timeout=_HANDLER_TIMEOUT)
        _engine._worker_send(WORKER_ID, POOL_ID, req_id, res)
    except asyncio.TimeoutError:
        try:
            _engine._worker_send(WORKER_ID, POOL_ID, req_id, _error_response(504, "handler timeout"))
        except Exception:
            _log.exception("async handler req_id=%s: send timeout response failed", req_id)
    except asyncio.CancelledError:
        # Propagated cancellation — client disconnected or Rust future dropped.
        # Re-raise to let asyncio mark the task as CANCELLED (required by asyncio contract).
        raise
    except Exception:
        _log.exception("async handler req_id=%s path=%s raised", req_id, req.path)
        try:
            _engine._worker_send(
                WORKER_ID, POOL_ID, req_id, _error_response(500, "internal server error")
            )
        except Exception:
            _log.exception("async handler req_id=%s: send error response failed", req_id)


def _fetcher_thread(loop):
    consecutive_errors = 0
    while True:
        try:
            # POOL_ID is the zombie-worker guard: a stale worker whose pool has
            # been replaced sees None here and exits the loop.
            req_data = _engine._worker_recv(WORKER_ID, POOL_ID)
            if req_data is None:
                break
            req_id, handler_idx, req = req_data
            asyncio.run_coroutine_threadsafe(_process_request(req_id, handler_idx, req), loop)
            consecutive_errors = 0
        except RuntimeError:
            # run_coroutine_threadsafe raises RuntimeError once the event loop
            # is closed (interpreter teardown / shutdown race). This is fatal
            # and non-recoverable — retrying only spins the CPU and spams logs.
            if loop.is_closed():
                _log.info(
                    "worker=%s fetcher: event loop closed — exiting", WORKER_ID
                )
                break
            # A RuntimeError while the loop is still alive is unexpected but
            # potentially transient (a Rust panic in the engine call surfaces as
            # one too); fall through to the retriable path.
            consecutive_errors += 1
            _log.exception("worker=%s fetcher error — continuing", WORKER_ID)
            time.sleep(min(0.05 * consecutive_errors, 1.0))
        except Exception:
            # Retriable error (e.g. transient scheduling failure). Back off
            # proportionally so a persistent error does not pin a core or
            # flood the log.
            #
            # A closed loop is fatal here too: if the loop is torn down while
            # this path keeps firing (e.g. OOM during shutdown), the back-off
            # would never exit and the worker would burn CPU until killed.
            # Mirror the RuntimeError branch and exit on a closed loop.
            if loop.is_closed():
                _log.info(
                    "worker=%s fetcher: event loop closed — exiting", WORKER_ID
                )
                break
            consecutive_errors += 1
            _log.exception("worker=%s fetcher error — continuing", WORKER_ID)
            time.sleep(min(0.05 * consecutive_errors, 1.0))


async def _pyronova_engine():
    loop = asyncio.get_running_loop()
    t = threading.Thread(target=_fetcher_thread, args=(loop,), daemon=False)
    try:
        t.start()
    except RuntimeError:
        _log.exception("worker=%s fetcher thread failed to start", WORKER_ID)
        return
    try:
        # The fetcher returns only when the request channel closes, i.e. at
        # shutdown, so this waits for the worker's whole life. No timeout: one
        # used to start counting at worker start, ended the engine after 30 s of
        # normal serving, and Py_EndInterpreter then blocked forever joining the
        # still-running (non-daemon; sub-interpreters forbid daemon threads)
        # fetcher. A timeout can't bound shutdown anyway for the same reason.
        await asyncio.to_thread(t.join)
    finally:
        # Graceful asyncio shutdown. Without this, Py_EndInterpreter
        # would tear the VM down while pending tasks (background
        # asyncio.create_task'd work) still hold FDs — orphan sockets,
        # possible SIGSEGV during CPython's emergency task GC.
        #
        # 1. Cancel every task still pending on this loop.
        # 2. Drain cancellations with gather(return_exceptions=True).
        # 3. Close async generators (asyncpg-style connection pools
        #    use async generators for iterate-on-demand results).
        try:
            pending = [t for t in asyncio.all_tasks(loop)
                       if t is not asyncio.current_task()]
            for task in pending:
                task.cancel()
            if pending:
                # Bound the drain so a task that ignores cancellation cannot
                # block Py_EndInterpreter forever; matches the 30s zombie guard.
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*pending, return_exceptions=True),
                        timeout=30.0,
                    )
                except asyncio.TimeoutError:
                    pass
            await loop.shutdown_asyncgens()
        except Exception:
            # Shutdown is best-effort; any exception here is better
            # logged than allowed to propagate and abort the Py_EndInterpreter.
            _log.exception("asyncio shutdown error")


# Fail-fast contract check: `WORKER_ID` and `POOL_ID` are set by the Rust host
# before this runs, and the engine functions come from `pyronova.engine`. If
# either is missing, say so here, before the fetcher thread starts — otherwise
# the first missing reference raises NameError inside the fetcher, whose
# back-off loop catches and retries it forever, pinning a core.
_missing = [_name for _name in ("WORKER_ID", "POOL_ID") if _name not in globals()]
_missing += [
    _name for _name in ("_worker_recv", "_worker_send", "_worker_to_response")
    if not hasattr(_engine, _name)
]
if _missing:
    raise RuntimeError(
        "pyronova async engine: missing " + ", ".join(_missing)
        + " — the worker namespace or the engine is not what this engine expects; "
        "refusing to start the fetcher thread"
    )

asyncio.run(_pyronova_engine())
