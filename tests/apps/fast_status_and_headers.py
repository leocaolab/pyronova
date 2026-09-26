"""App for tests/test_fast_response.py::test_fast_status_and_headers. Its own module: a
worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()


@app.get("/")
def root(req):
    return "probe"

app.add_fast_response(
    "GET", "/maintenance",
    b"we are down",
    content_type="text/plain",
    status_code=503,
    headers={"Retry-After": "30"},
)
