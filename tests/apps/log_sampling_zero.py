"""App for tests/test_log_sampling.py::test_sample_zero_clamped_to_one. Its own module:
a worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_logging()
app._engine.set_request_log_sampling(0, 0)  # would be UB without clamp


@app.get("/h")
def handler(req):
    return "ok"
