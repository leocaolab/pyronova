"""Two chained after_request hooks — for tests/test_middleware_hooks.py."""

from pyronova import Pyronova, Response

app = Pyronova()


@app.after_request
def add_header_1(req, resp):
    headers = dict(getattr(resp, "headers", {}) or {})
    headers["x-hook-1"] = "yes"
    return Response(
        body=resp.body, status_code=resp.status_code,
        content_type=resp.content_type, headers=headers,
    )


@app.after_request
def add_header_2(req, resp):
    headers = dict(getattr(resp, "headers", {}) or {})
    headers["x-hook-2"] = "yes"
    return Response(
        body=resp.body, status_code=resp.status_code,
        content_type=resp.content_type, headers=headers,
    )


@app.get("/")
def index(req):
    return "ok"
