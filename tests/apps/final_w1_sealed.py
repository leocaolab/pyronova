"""App for tests/test_final_w1.py (F3): hooks registered after the first server started
are refused. Served with mode="gil"."""

from pyronova import Pyronova

app = Pyronova()


@app.get("/")
def index(req):
    return "ok"
