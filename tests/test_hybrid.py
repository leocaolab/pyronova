"""Test: Hybrid mode — fast routes on sub-interp, numpy routes on GIL."""

from pyronova import Pyronova, Response

app = Pyronova()


@app.get("/")
def index(req):
    """Fast route → sub-interpreter (215k req/s)."""
    return "Hello from sub-interpreter!"


@app.get("/hello/{name}")
def greet(req):
    """Fast route → sub-interpreter."""
    name = req.params.get("name", "world")
    return '{"message": "Hello, ' + name + '!"}'


@app.get("/numpy", gil=True)
def numpy_route(req):
    """Heavy route → main GIL (full numpy support)."""
    import numpy as np
    arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    return {
        "mean": float(np.mean(arr)),
        "std": float(np.std(arr)),
        "sum": float(np.sum(arr)),
    }


@app.get("/orjson", gil=True)
def orjson_route(req):
    """Heavy route → main GIL."""
    import orjson
    data = {"key": "value", "numbers": [1, 2, 3]}
    return orjson.loads(orjson.dumps(data))


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, mode="subinterp")
