"""App for tests/test_observability.py::test_custom_metrics_path. Its own module: a
worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_metrics(path="/_/prom")


@app.get("/")
def root(req):
    return "ok"
