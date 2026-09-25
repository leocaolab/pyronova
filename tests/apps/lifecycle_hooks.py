"""App with startup hooks for tests/test_lifecycle.py. The hooks run on the main
interpreter, so `hook_log` here is the one the tests read; `app.state` is shared with
the workers that serve the routes."""

from pyronova import Pyronova

app = Pyronova()
# Track hook execution via a mutable container
hook_log = {"startup_called": False, "startup_order": []}


@app.on_startup
def init_cache():
    hook_log["startup_called"] = True
    hook_log["startup_order"].append("init_cache")
    app.state["cache_ready"] = "true"


@app.on_startup
def init_counter():
    hook_log["startup_order"].append("init_counter")
    app.state["counter"] = "0"


@app.get("/")
def index(req):
    return {"ok": True}


@app.get("/cache-status")
def cache_status(req):
    try:
        return {"ready": app.state["cache_ready"]}
    except KeyError:
        return {"ready": "false"}


@app.get("/counter")
def get_counter(req):
    try:
        return {"counter": app.state["counter"]}
    except KeyError:
        return {"counter": "-1"}


@app.get("/hook-log")
def get_hook_log(req):
    return hook_log
