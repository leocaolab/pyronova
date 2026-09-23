"""E2E-8 app (Layer 2): main-interpreter routes while workers execute the real engine.

Every worker imports the real `pyronova` package and engine (M4), so more than one
interpreter has executed the engine. From that moment the PyO3 fork refuses a bare
foreign-thread `Python::attach` (and `Py<T>` drop), which is the situation M0 made safe:
GIL-bridge threads, WebSocket threads and `/metrics` keep serving the main interpreter.
(Rewritten at M4 activation, approved by the user on 2026-09-23: before M4 this script
loaded the engine into each worker by hand, next to the mock package.)
"""
import os

import pyronova.engine as _engine
from pyronova import Pyronova

app = Pyronova()
app.enable_metrics()


@app.get("/w")
def worker_route(req):
    # Served by a worker, which runs the real engine.
    return {"real_engine": _engine._in_worker() and hasattr(_engine, "_worker_recv")}


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
