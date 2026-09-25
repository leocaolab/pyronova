"""App for tests/test_cors_404.py::test_404_carries_cors_headers. Its own module: a
worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_cors(
    allow_origins="https://app.example.com",
    allow_methods="GET, POST, OPTIONS",
)


@app.get("/")
def root(req):
    return "hello"
