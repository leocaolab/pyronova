"""App for tests/test_cors_404.py::test_options_preflight_on_unknown_path_not_blocked.
Its own module: a worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()
app.enable_cors(
    allow_origins="https://app.example.com",
    allow_methods="GET, POST, OPTIONS",
    allow_headers="content-type",
)


# TestClient readiness probe hits "/"
@app.get("/")
def root(req):
    return "ready"


@app.post("/api/login")
def login(req):
    return {"ok": True}
