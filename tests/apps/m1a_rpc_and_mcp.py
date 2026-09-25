"""App for tests/test_review_m1a.py::test_rpc_and_mcp_routes_are_listed_in_app_routes:
an RPC route and an MCP tool. Its own module: a worker serves one app per module."""

from pyronova import Pyronova

app = Pyronova()


@app.rpc("/rpc/echo")
def echo(data):
    return data


@app.mcp.tool()
def ping() -> str:
    return "pong"


@app.get("/")
def index(req):
    return "ok"
