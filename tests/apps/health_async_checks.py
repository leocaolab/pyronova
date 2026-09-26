"""App for tests/test_health.py::test_async_readiness_check_supported. Its own module: a
worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()


@app.readiness_check("async_ok")
async def _():
    return True


@app.readiness_check("async_fail")
async def _():
    raise ConnectionError("timeout")

app.enable_health_probes()
