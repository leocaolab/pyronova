"""App for tests/test_observability.py::test_custom_header_name. Its own module: a
worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_request_id(header="X-Trace-Id")


@app.get("/")
def root(req):
    return "ok"
