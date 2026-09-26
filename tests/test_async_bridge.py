"""Test: Phase 7.2 async bridge — sleep(1ms) should break the 8k ceiling."""
from pyronova import PyronovaApp

app = PyronovaApp()


def hello(req):
    return "Hello"


async def async_sleep(req):
    import asyncio
    await asyncio.sleep(0.001)
    return "ok"


def sync_sleep(req):
    import time
    time.sleep(0.001)
    return "ok"


app.get("/hello", hello)
app.get("/async-sleep", async_sleep)
app.get("/sync-sleep", sync_sleep)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, mode="async")
