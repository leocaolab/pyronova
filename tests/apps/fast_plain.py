"""App for tests/test_fast_response.py::test_fast_plain. Its own module: a worker serves
one app per module."""

from pyronova import Pyronova

app = Pyronova()


@app.get("/")
def root(req):
    return "probe"

app.add_fast_response("GET", "/health", b'{"ok":true}',
                     content_type="application/json")
app.add_fast_response("GET", "/robots.txt",
                     b"User-agent: *\nDisallow: /\n")
# str body also accepted (encoded as utf-8)
app.add_fast_response("GET", "/ping", "pong", content_type="text/plain")
