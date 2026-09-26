"""A streaming upload route with a 1 KB max_body_size — for
tests/test_upload_streaming.py. max_body_size is process-wide, so the suite imports
this module only inside its test."""

from pyronova import Pyronova

app = Pyronova()
app.max_body_size = 1024


@app.get("/")
def root(req):
    return "ok"


@app.post("/up", gil=True, stream=True)
def up(req):
    try:
        total = 0
        for chunk in req.stream:
            total += len(chunk)
        return {"bytes": total}
    except IOError as e:
        return {"error": str(e)}
