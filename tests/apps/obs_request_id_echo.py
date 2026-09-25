"""App for tests/test_observability.py::test_request_id_echoed_when_client_supplies. Its
own module: a worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_request_id()


@app.get("/")
def root(req):
    return "ok"
