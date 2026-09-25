"""App for tests/test_health.py::test_custom_paths. Its own module: a worker serves one
app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_health_probes(livez_path="/_alive", readyz_path="/_ready")
