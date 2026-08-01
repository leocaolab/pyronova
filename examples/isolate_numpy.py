"""app.isolate() — per-worker numpy copies the built-in way (no manual cp).

Run:  PYRONOVA_WORKERS=8 python examples/isolate_numpy.py
Test: curl http://127.0.0.1:8000/np
"""
from pyronova import Pyronova

app = Pyronova()
app.isolate("numpy")

@app.get("/np")
def np_op(req):
    import numpy as np
    import _interpreters
    a = np.arange(100000, dtype="f8")
    return {"sum": float(np.sin(a).sum()), "numpy": np.__version__,
            "interp": _interpreters.get_current()[0], "path": np.__file__}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, mode="subinterp")
