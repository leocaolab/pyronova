"""App for tests/test_final_w1.py (Q1): a sync route and an `async def` route served by
the TPC sub-interpreter server. Its own module: a worker serves one app per module."""

import asyncio

from pyronova import Pyronova

app = Pyronova()


@app.get("/fast")
def fast(req):
    return "fast"


@app.get("/sleep")
async def sleep(req):
    await asyncio.sleep(float(req.query_params.get("s", "0.5")))
    return "slept"
