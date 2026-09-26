"""before_request that short-circuits without a token — for
tests/test_middleware_hooks.py."""

from pyronova import Pyronova, Response

app = Pyronova()


@app.before_request
def auth_check(req):
    # Skip auth for health check route
    if req.path == "/":
        return None
    if "x-token" not in req.headers:
        return Response(body="unauthorized", status_code=401)
    return None


@app.get("/")
def index(req):
    return "ok"


@app.get("/protected")
def protected(req):
    return {"secret": "data"}
