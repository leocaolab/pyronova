"""App for tests/test_final_w1.py: its first server has no MCP tools; a tool is added
before the second, which must serve `/mcp`. Served with mode="gil"."""

from pyronova import Pyronova

app = Pyronova()


@app.get("/")
def index(req):
    return "ok"
