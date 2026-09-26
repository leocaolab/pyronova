"""Test: after_request hooks work in sub-interpreter mode."""
from pyronova import PyronovaApp, Response

app = PyronovaApp()


def add_cors(req, resp):
    """after_request hook: add CORS header."""
    # Return a Response with extra headers
    return Response(
        body=resp.body,
        status_code=resp.status_code,
        content_type=resp.content_type,
        headers={**resp.headers, "x-cors": "true"},
    )


def fast_route(req):
    return "fast"


def json_route(req):
    return '{"key": "value"}'


app.after_request(add_cors)
app.get("/fast", fast_route)
app.get("/json", json_route)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, mode="subinterp")
