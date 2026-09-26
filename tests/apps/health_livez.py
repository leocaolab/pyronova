"""App for tests/test_health.py::test_livez_returns_200_always. Its own module: a worker
serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_health_probes()
