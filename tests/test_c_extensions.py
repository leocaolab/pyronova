"""Test: C/C++ extensions (numpy, orjson) in sub-interpreter mode."""

from pyronova import PyronovaApp

app = PyronovaApp()


def numpy_test(req):
    import numpy as np
    arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    return {
        "mean": float(np.mean(arr)),
        "std": float(np.std(arr)),
        "sum": float(np.sum(arr)),
        "shape": list(arr.shape),
    }


def orjson_test(req):
    import orjson
    data = {"key": "value", "numbers": [1, 2, 3], "nested": {"a": True}}
    encoded = orjson.dumps(data)
    decoded = orjson.loads(encoded)
    return decoded


def combined_test(req):
    import numpy as np
    import orjson
    arr = np.random.randn(100).tolist()
    result = {"data": arr[:5], "mean": float(np.mean(arr)), "len": len(arr)}
    # Round-trip through orjson
    encoded = orjson.dumps(result)
    return orjson.loads(encoded)


app.get("/numpy", numpy_test)
app.get("/orjson", orjson_test)
app.get("/combined", combined_test)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, mode="subinterp")
