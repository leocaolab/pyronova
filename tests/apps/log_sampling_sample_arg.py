"""App for tests/test_log_sampling.py::test_enable_logging_with_sample_arg. Its own
module: a worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_logging(level="info", sample=100, always_log_status=400)


@app.get("/h")
def handler(req):
    return "ok"
