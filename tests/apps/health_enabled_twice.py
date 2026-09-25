"""App for tests/test_health.py::test_enable_health_probes_idempotent. Its own module: a
worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_health_probes()
# Second call is a no-op — no duplicate route registration error.
app.enable_health_probes()
