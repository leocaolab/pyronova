"""App for tests/test_log_sampling.py::test_set_request_log_sampling_directly. Its own
module: a worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_logging()
# Direct Rust setter — exercise the binding shape
app._engine.set_request_log_sampling(50, None)
app._engine.set_request_log_sampling(1, 500)


@app.get("/h")
def handler(req):
    return "ok"
