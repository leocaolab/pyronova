"""App for tests/test_observability.py::test_metrics_counts_requests. Its own module: a
worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_metrics()


@app.get("/")
def root(req):
    return "ok"


@app.get("/boom")
def boom(req):
    from pyronova import Response
    return Response(body="broken", status_code=500)
