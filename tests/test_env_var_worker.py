"""Test: env var worker mode — full user scripts run in sub-interpreters
without AST filtering. Covers decorator syntax, multi-file imports,
custom variable names, and middleware."""

from pyronova import Pyronova, Response

# Custom variable name (not "app" — old AST filter would miss this)
my_server = Pyronova()

# Decorator syntax
@my_server.get("/")
def index(req):
    return "hello from decorator"

@my_server.get("/json")
def json_route(req):
    return {"status": "ok", "source": "subinterp"}

@my_server.get("/params/{name}")
def with_params(req):
    return {"name": req.params.get("name", "unknown")}

@my_server.get("/query")
def with_query(req):
    return {"q": req.query_params.get("q", "")}

# Middleware (should work in sub-interp after_request fix)
@my_server.after_request
def add_header(req, resp):
    return Response(
        body=resp.body,
        status_code=resp.status_code,
        content_type=resp.content_type,
        headers={**resp.headers, "x-test": "passed"},
    )

# Helper function (not a route — should survive in sub-interp)
def compute_value(x):
    return x * 42

@my_server.get("/compute")
def compute_route(req):
    return {"result": compute_value(10)}

if __name__ == "__main__":
    my_server.run(host="127.0.0.1", port=0, mode="subinterp")
