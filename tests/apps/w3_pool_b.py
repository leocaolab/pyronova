"""The second of the two apps tests/test_final_w3.py serves at once in pool mode (see
w3_pool_a.py)."""

import asyncio

from pyronova import Pyronova

app = Pyronova()


# TestClient's readiness probe hits "/".
@app.get("/")
def root(req):
    return {"ready": True}


@app.get("/which")
async def which(req):
    await asyncio.sleep(0)
    return {"app": "b"}
