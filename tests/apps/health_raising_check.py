"""App for tests/test_health.py::test_readyz_503_on_exception. Its own module: a worker
serves one app per module."""

from pyronova import Pyronova

app = Pyronova()


@app.readiness_check("db")
def _():
    raise RuntimeError("connection refused")

app.enable_health_probes()
