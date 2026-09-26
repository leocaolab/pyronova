"""Test: async def handlers in both GIL and sub-interpreter modes."""
from pyronova import Pyronova

app = Pyronova()


@app.get("/sync")
def sync_handler(req):
    return "sync ok"


@app.get("/async")
async def async_handler(req):
    import asyncio
    await asyncio.sleep(0.001)
    return "async ok"


@app.get("/async-json")
async def async_json(req):
    import asyncio
    await asyncio.sleep(0.001)
    return {"status": "async", "value": 42}


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "default"
    app.run(host="127.0.0.1", port=0, mode=mode)
