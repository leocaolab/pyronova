"""Async engine — run in each async sub-interpreter worker (pool mode).

It drives a Python asyncio event loop that runs the requests a fetcher thread pulls from
Rust through `pyronova.engine._worker_recv`. It runs as the module
`__pyronova_async_engine__`, in its own namespace; Rust sets in it before it runs:

- `WORKER_ID`: this worker's index (log records);
- `CHANNEL`: this worker's request inbox;
- `TASK_TIMEOUT`: seconds a request's task may run, a little under the request budget
  Rust answers 504 at, so the task is cancelled rather than left computing a result
  nobody takes.

Each request comes as a job that carries where its answer goes; the engine answers it
with `_worker_send` / `_worker_fail` / `_worker_timed_out`. A job the engine lets go of
unanswered (its task cancelled when the engine stops) answers itself with an error, so
no caller waits for a request that will never run.

The handlers and hooks are the ones the user's script registered on its app, indexed
like the main interpreter's routes.
"""

import asyncio
import logging
import threading

import pyronova.engine as _engine

_log = logging.getLogger("pyronova.async")

# Fetched once: the script has run, so the app's table is final.
_HANDLERS = _engine._worker_app_handlers()
_HOOKS = _engine._worker_app_hooks()
_BEFORE_HOOKS, _AFTER_HOOKS = _HOOKS.before, _HOOKS.after


async def _call(fn, *args):
    res = fn(*args)
    if asyncio.iscoroutine(res) or asyncio.isfuture(res) or hasattr(res, "__await__"):
        res = await res
    return res


async def _handle(handler, req):
    # Hooks and handler run in this request's own Task, so per-request state kept
    # in ContextVars (observability's request id, pyronova.context) stays apart
    # from concurrent requests on this loop. Same order and semantics as
    # every other path (`src/python/hook_chain.rs`): a before hook that returns
    # something short-circuits; after hooks get a Response and may replace it; a
    # hook that raises fails the request.
    for hook in _BEFORE_HOOKS:
        res = await _call(hook, req)
        if res is not None:
            return _engine._worker_to_response(res)
    res = _engine._worker_to_response(await _call(handler, req))
    for hook in _AFTER_HOOKS:
        replaced = await _call(hook, req, res)
        if replaced is not None:
            res = _engine._worker_to_response(replaced)
    return res


async def _process_request(job):
    # Whatever the task raises goes to Rust as what it is, as on every other path: the
    # exception itself (logged there once, with its traceback and the request id; the
    # client gets a generic 500), or the timeout (504). SystemExit and KeyboardInterrupt
    # from a handler are the handler's failure too, not the engine's. Only the task's own
    # cancellation propagates, as asyncio requires; the job then answers itself.
    budget = asyncio.timeout(TASK_TIMEOUT)
    try:
        async with budget:
            res = await _handle(_HANDLERS[job.route], job.request)
    except (asyncio.CancelledError, GeneratorExit):
        raise
    except BaseException as exc:
        if isinstance(exc, TimeoutError) and budget.expired():
            _engine._worker_timed_out(job)
        else:
            _engine._worker_fail(job, exc)
    else:
        _engine._worker_send(job, res)


class _Fetcher(threading.Thread):
    """Pulls requests from `CHANNEL` (with the GIL released while it waits) and schedules
    each on the loop, until the inbox closes. Whatever it raises (a Rust panic in
    `_worker_recv` arrives as RuntimeError) ends it, and the engine with it: it is never
    retried."""

    def __init__(self, loop):
        super().__init__(name=f"pyronova-async-fetcher-{WORKER_ID}")
        self._loop = loop
        self.error = None

    def run(self):
        try:
            while (job := _engine._worker_recv(CHANNEL)) is not None:
                asyncio.run_coroutine_threadsafe(_process_request(job), self._loop)
        except BaseException as exc:  # handed to the engine, which raises it
            self.error = exc


async def _pyronova_engine():
    t = _Fetcher(asyncio.get_running_loop())
    t.start()
    try:
        # The fetcher returns when the inbox closes: at shutdown, or when this engine
        # stops (below). No timeout: this join lasts the worker's whole serving life.
        await asyncio.to_thread(t.join)
    finally:
        # However the engine stops (shutdown, the fetcher failing, the loop dying), close
        # the inbox so the fetcher returns: the interpreter waits for it at its end.
        _engine._worker_close(CHANNEL)
        # Pending tasks (background `create_task` work) still hold sockets: cancel them
        # (their jobs answer themselves) and close async generators before
        # Py_EndInterpreter tears the loop down. The wait is bounded, so a task that
        # ignores cancellation can't hold the interpreter's end forever.
        loop = asyncio.get_running_loop()
        pending = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        if pending:
            done, stuck = await asyncio.wait(pending, timeout=TASK_TIMEOUT)
            await asyncio.gather(*done, return_exceptions=True)
            if stuck:
                _log.warning(
                    "worker=%s: %d task(s) did not end within %ss of being cancelled at "
                    "shutdown; the interpreter ends with them pending",
                    WORKER_ID, len(stuck), TASK_TIMEOUT,
                )
        await loop.shutdown_asyncgens()
    if t.error is not None:
        raise t.error


asyncio.run(_pyronova_engine())
