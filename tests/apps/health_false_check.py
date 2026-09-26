"""App for tests/test_health.py::test_readyz_503_on_false_return. Its own module: a
worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()


@app.readiness_check("feature_flag")
def _():
    return False

app.enable_health_probes()
