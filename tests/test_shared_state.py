"""Test: SharedState across GIL routes in hybrid mode."""
import json
from pyronova import Pyronova

app = Pyronova()


@app.get("/set/{key}/{value}", gil=True)
def set_state(req):
    """Set a key via SharedState (GIL route — direct Rust DashMap access)."""
    key = req.params["key"]
    value = req.params["value"]
    app.state[key] = value
    return {"set": key, "value": value}


@app.get("/get/{key}", gil=True)
def get_state(req):
    """Get a key via SharedState."""
    key = req.params["key"]
    try:
        return {"key": key, "value": app.state[key]}
    except KeyError:
        return {"key": key, "value": None}


@app.get("/numpy-compute", gil=True)
def numpy_compute(req):
    """GIL route: compute with numpy and store result in shared state."""
    import numpy as np
    result = float(np.mean(np.random.randn(1000)))
    app.state["numpy_result"] = json.dumps({"mean": round(result, 6)})
    return {"stored": True, "mean": round(result, 6)}


@app.get("/read-numpy", gil=True)
def read_numpy(req):
    """Read numpy result stored by another request (possibly different thread)."""
    try:
        return json.loads(app.state["numpy_result"])
    except KeyError:
        return {"error": "not computed yet"}


@app.get("/stats", gil=True)
def stats(req):
    """Show all keys in shared state."""
    return {"keys": app.state.keys(), "count": len(app.state)}


@app.get("/fast")
def fast_route(req):
    """Sub-interp route: proves fast routes still work alongside state routes."""
    return "fast"


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, mode="subinterp")


# ---------------------------------------------------------------------------
# Unit tests for SharedState atomic operations (incr/decr)
# ---------------------------------------------------------------------------

from pyronova.engine import SharedState


def test_incr_creates_key():
    s = SharedState()
    assert s.incr("x", 1) == 1


def test_incr_accumulates():
    s = SharedState()
    s.incr("a", 1)
    s.incr("a", 1)
    assert s.incr("a", 1) == 3


def test_decr():
    s = SharedState()
    s.incr("b", 5)
    assert s.decr("b", 2) == 3


def test_incr_by_amount():
    s = SharedState()
    assert s.incr("c", 10) == 10
    assert s.incr("c", 5) == 15


def test_incr_result_is_string_in_get():
    s = SharedState()
    s.incr("d", 42)
    assert s.get("d") == "42"
