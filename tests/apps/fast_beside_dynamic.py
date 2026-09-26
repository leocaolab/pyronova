"""App for tests/test_fast_response.py::test_fast_does_not_interfere_with_dynamic. Its
own module: a worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()


@app.get("/")
def root(req):
    return "probe"


@app.get("/users/{id}")
def user(req):
    return {"id": req.params["id"]}

app.add_fast_response("GET", "/health", b"ok")
