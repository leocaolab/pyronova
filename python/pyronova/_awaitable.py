"""Any awaitable as a native coroutine.

The engine runs an awaitable a hook or handler returns as a task in the request's own
``contextvars.Context`` (``loop.create_task(coro, context=...)``), so the ``ctx`` writes
it makes are seen by the rest of the request. A task takes only a coroutine; a Cython or
mypyc coroutine, or an object with ``__await__``, is run through ``drive`` instead.
"""


async def drive(awaitable):
    return await awaitable
