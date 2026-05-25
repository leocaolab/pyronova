"""Async engine script — injected into each sub-interpreter worker.

This script runs inside a sub-interpreter with its own GIL.
It drives a Python asyncio event loop that processes requests
received from Rust via the _pyronova_recv/_pyronova_send C-FFI bridge.

Template variables (replaced by Rust before exec):
  {worker_idx}     — this worker's index (0..N)
  {handlers_array}  — comma-separated quoted handler names
"""

import asyncio
import logging
import threading

_log = logging.getLogger("pyronova.async")

try:
    import orjson as _orjson

    def _orjson_default(obj):
        if isinstance(obj, (set, frozenset)):
            return list(obj)
        raise TypeError(f'Object of type {type(obj).__name__} is not JSON serializable')

    def _json_dumps_bytes(obj):
        return _orjson.dumps(obj, default=_orjson_default)

except ImportError:
    import json as _json_mod

    def _json_dumps_bytes(obj):
        return _json_mod.dumps(obj).encode("utf-8")

# Injected by Rust: WORKER_ID = {worker_idx}
# Injected by Rust: HANDLER_NAMES = [{handlers_array}]
# Injected by Rust: _pyronova_pool_id (sub-interp pool identifier; passed to
#   every _pyronova_send/_pyronova_recv call — see src/python/interp.rs:2480
#   for the injection site).


# Timeout for async handlers — 2s before Rust's 30s gateway timeout,
# so Python can abort cleanly instead of computing a result nobody wants.
_HANDLER_TIMEOUT = 28


async def _process_request(req_id, handler_idx, method, path, params, query, body_bytes, headers, client_ip):
    try:
        try:
            handler_name = HANDLER_NAMES[int(handler_idx)]
        except (ValueError, IndexError) as e:
            _log.error("worker=%s invalid handler_idx=%r: %s", WORKER_ID, handler_idx, e)
            _pyronova_send(WORKER_ID, _pyronova_pool_id, req_id, 500, "text/plain", b"routing error")
            return
        handler = globals().get(handler_name)
        if handler is None:
            _pyronova_send(WORKER_ID, _pyronova_pool_id, req_id, 500, "text/plain", b"handler not found")
            return

        req = _Request(method, path, params, query, body_bytes, headers, client_ip)
        res = handler(req)

        if asyncio.iscoroutine(res) or asyncio.isfuture(res) or hasattr(res, '__await__'):
            # Bracket pattern: bound the coroutine's lifetime so cancelled/timed-out
            # requests don't accumulate as phantom load in the event loop.
            # ensure_future wraps any awaitable (coroutine, Future, __await__) into
            # a Task so wait_for can cancel it uniformly on timeout.
            res = await asyncio.wait_for(asyncio.ensure_future(res), timeout=_HANDLER_TIMEOUT)

        if isinstance(res, _Response):
            body = (
                str(res.body).encode("utf-8")
                if not isinstance(res.body, bytes)
                else res.body
            )
            _pyronova_send(
                WORKER_ID,
                _pyronova_pool_id,
                req_id,
                res.status_code,
                res.content_type or "text/plain",
                body,
            )
        elif isinstance(res, dict):
            _pyronova_send(
                WORKER_ID,
                _pyronova_pool_id,
                req_id,
                200,
                "application/json",
                _json_dumps_bytes(res),
            )
        elif isinstance(res, bytes):
            _pyronova_send(WORKER_ID, _pyronova_pool_id, req_id, 200, "application/octet-stream", res)
        elif res is None:
            _pyronova_send(WORKER_ID, _pyronova_pool_id, req_id, 200, "text/plain", b"")
        else:
            body = str(res).encode("utf-8")
            body_stripped = body.lstrip()
            ct = (
                "application/json"
                if body_stripped.startswith(b"{") or body_stripped.startswith(b"[")
                else "text/plain"
            )
            _pyronova_send(WORKER_ID, _pyronova_pool_id, req_id, 200, ct, body)
    except asyncio.TimeoutError:
        try:
            _pyronova_send(WORKER_ID, _pyronova_pool_id, req_id, 504, "text/plain", b"handler timeout")
        except Exception:
            _log.exception("async handler req_id=%s: send timeout response failed", req_id)
    except asyncio.CancelledError:
        # Propagated cancellation — client disconnected or Rust future dropped.
        # Re-raise to let asyncio mark the task as CANCELLED (required by asyncio contract).
        raise
    except Exception:
        _log.exception("async handler req_id=%s path=%s raised", req_id, path)
        try:
            _pyronova_send(WORKER_ID, _pyronova_pool_id, req_id, 500, "text/plain", b"internal server error")
        except Exception:
            _log.exception("async handler req_id=%s: send error response failed", req_id)


def _fetcher_thread(loop):
    while True:
        try:
            # The pool_id argument is the zombie-worker guard (see
            # src/interp.rs :: pyronova_recv_cfunc). A stale worker whose pool
            # has been replaced will see None here and exit the loop.
            req_data = _pyronova_recv(WORKER_ID, _pyronova_pool_id)
            if req_data is None:
                break
            req_id, handler_idx, method, path, params, query, body_bytes, headers, client_ip = req_data
            asyncio.run_coroutine_threadsafe(
                _process_request(req_id, handler_idx, method, path, params, query, body_bytes, headers, client_ip),
                loop,
            )
        except Exception:
            _log.exception("worker=%s fetcher error — continuing", WORKER_ID)


async def _pyronova_engine():
    loop = asyncio.get_running_loop()
    t = threading.Thread(target=_fetcher_thread, args=(loop,), daemon=False)
    try:
        t.start()
    except RuntimeError:
        _log.exception("worker=%s fetcher thread failed to start", WORKER_ID)
        return
    try:
        # Bound shutdown duration. Without a timeout, a stuck fetcher
        # thread (e.g. _pyronova_recv FFI hanging, zombie-worker guard
        # failing, channel deadlock) would block shutdown indefinitely
        # (arc finding async-engine-4). 30s aligns with Rust's gateway
        # timeout — if the fetcher hasn't exited by then, it never will.
        try:
            await asyncio.wait_for(asyncio.to_thread(t.join), timeout=30.0)
        except asyncio.TimeoutError:
            _log.error(
                "worker=%s fetcher thread did not exit within 30s — "
                "proceeding with shutdown; thread will be killed by "
                "Py_EndInterpreter",
                WORKER_ID,
            )
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
                await asyncio.gather(*pending, return_exceptions=True)
            await loop.shutdown_asyncgens()
        except Exception:
            # Shutdown is best-effort; any exception here is better
            # logged than allowed to propagate and abort the Py_EndInterpreter.
            _log.exception("asyncio shutdown error")


asyncio.run(_pyronova_engine())
