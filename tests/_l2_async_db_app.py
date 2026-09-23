"""E2E-8, async-DB part (Layer 2, M1): `PgPool.*_async` on main while workers execute the
real engine.

Like `_l2_main_side_app.py`, every worker also loads the real `pyronova.engine`, so more
than one interpreter has executed it and the PyO3 fork refuses a bare foreign-thread attach.
`pyo3-async-runtimes` resolved `*_async` futures with exactly such an attach on its own
threads (spike R-3). Two callers are driven here:

- `/adb`: an async `gil=True` route awaiting `fetch_all_async`;
- a thread on the main interpreter running its own asyncio loop that awaits
  `fetch_all_async` in a loop; `/bg` reports its successes and failures.
"""
import os

import pyronova.engine as _engine

IN_WORKER = "_pyronova_emit_log" in globals()

if IN_WORKER:
    import importlib.machinery
    import importlib.util

    _loader = importlib.machinery.ExtensionFileLoader(
        "pyronova.engine", os.environ["L2_ENGINE_PATH"]
    )
    _real_engine = importlib.util.module_from_spec(
        importlib.util.spec_from_loader("pyronova.engine", _loader)
    )
    _loader.exec_module(_real_engine)
else:
    os.environ["L2_ENGINE_PATH"] = _engine.__file__

from pyronova import Pyronova  # noqa: E402

app = Pyronova()

if not IN_WORKER:
    import asyncio
    import threading

    from pyronova.db import PgPool

    pool = PgPool.connect(os.environ["PYRONOVA_TEST_PG_DSN"])
    background = {"ok": 0, "errors": []}

    def _background() -> None:
        async def run() -> None:
            n = 0
            while True:
                n += 1
                try:
                    rows = await pool.fetch_all_async("SELECT $1::int AS n", n)
                    assert rows == [{"n": n}], rows
                    background["ok"] += 1
                except Exception as e:  # noqa: BLE001 — reported through /bg
                    background["errors"].append(repr(e))
                await asyncio.sleep(0.001)

        asyncio.run(run())

    threading.Thread(target=_background, daemon=True).start()


@app.get("/w")
def worker_route(req):
    return {"real_engine": "_real_engine" in globals()}


@app.get("/adb", gil=True)
async def async_db(req):
    rows = await pool.fetch_all_async("SELECT 2 AS two, $1::text AS s", "x")
    return {"rows": rows}


@app.get("/bg", gil=True)
def background_state(req):
    return {"ok": background["ok"], "errors": background["errors"][:5]}


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ["L2_PORT"]), workers=2)
