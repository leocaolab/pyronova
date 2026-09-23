"""E2E-8 app (Layer 2, M0): main-interpreter routes while workers execute the real engine.

Workers still get the mock `pyronova` package from `_bootstrap.py`. This script, which
every worker executes, additionally loads the real `pyronova.engine` extension module
into each worker, so that more than one interpreter has executed it. From that moment
the PyO3 fork refuses a bare foreign-thread `Python::attach` (and `Py<T>` drop), which is
the situation M0 must survive: GIL-bridge threads, WebSocket threads and `/metrics` keep
serving the main interpreter.

That load is user code in this script, not a framework switch (roadmap §M0).
"""
import os

import pyronova.engine as _engine

if "_pyronova_emit_log" in globals():
    # Inside a worker: `_pyronova_emit_log` is injected only into worker globals.
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
    # Main interpreter: the real module; tell the workers where it is.
    os.environ["L2_ENGINE_PATH"] = _engine.__file__

from pyronova import Pyronova  # noqa: E402

app = Pyronova()
app.enable_metrics()


@app.get("/w")
def worker_route(req):
    return {"real_engine": "_real_engine" in globals()}


@app.get("/g", gil=True)
def main_sync(req):
    return {"main": "sync"}


@app.get("/ga", gil=True)
async def main_async(req):
    import asyncio

    await asyncio.sleep(0)
    return {"main": "async"}


@app.websocket("/ws")
def echo(sock):
    while True:
        m = sock.recv()
        if m is None:
            break
        sock.send("echo:" + m)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ["L2_PORT"]), workers=2)
