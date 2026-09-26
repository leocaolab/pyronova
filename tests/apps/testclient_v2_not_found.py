"""`/` answers 404 — for tests/test_testclient_v2.py (`.ok` on a 4xx). Its own module:
the suite's main app answers `/` with 200, and a worker serves one app per module."""

from pyronova import Pyronova, Response

app = Pyronova()


@app.get("/")
def root(req):
    return Response(body="nope", status_code=404)
