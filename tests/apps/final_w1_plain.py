"""App for tests/test_final_w1.py: one route, served by several servers at once (nested
TestClients) and with metrics. Served with mode="gil"."""

from pyronova import Pyronova

app = Pyronova()


@app.get("/")
def index(req):
    return "ok"
