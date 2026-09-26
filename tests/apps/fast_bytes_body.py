"""App for tests/test_fast_response.py::test_bytes_body_fast_path_in_pyronova_response.
Its own module: a worker serves one app per module."""

from pyronova import Pyronova, Response

app = Pyronova()


@app.get("/")
def root(req):
    return "probe"


@app.get("/raw")
def raw(req):
    return Response(b"\x00\x01\x02\xff", content_type="application/octet-stream")
