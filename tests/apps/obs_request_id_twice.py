"""App for tests/test_observability.py::test_enable_request_id_idempotent. Its own
module: a worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_request_id()
app.enable_request_id()  # no error, no double-hook


@app.get("/")
def root(req):
    return "ok"
