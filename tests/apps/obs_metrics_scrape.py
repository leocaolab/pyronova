"""App for tests/test_observability.py::test_metrics_scrape_is_not_counted. Its own
module: a worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_metrics()


@app.get("/")
def root(req):
    return "ok"
