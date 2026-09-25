"""CORS-enabled app for tests/test_testclient.py. Its own module: a worker serves one
app per module, and the suite's main app has no CORS."""

from pyronova import Pyronova, Response

app = Pyronova()
app.enable_cors()


@app.get("/")
def index(req):
    return {"ok": True}


@app.get("/binary")
def binary(req):
    return Response(
        body=b"\xff\xd8\xff\xe0\x00\x10JFIF",
        content_type="image/jpeg",
    )
