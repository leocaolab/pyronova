"""Regression for the streaming-body OOM (benchmark-17 audit bug #1).

Before the fix the feeder used `std::sync::mpsc` — unbounded. A fast
client could dump 10 GB of upload body into the channel faster than a
slow Python handler drained it, trashing RAM.

Now the feeder uses `tokio::sync::mpsc::channel(CHANNEL_CAPACITY)` and
`.send().await`, which propagates backpressure to `poll_frame` → TCP
window → client.

The full DoS test needs a multi-GB-per-second peer that's not easy to
simulate in pytest; this checks functional correctness (the handler still
receives every chunk). The streamed-body budget is covered by test_final_w2.
"""

from pyronova import Pyronova
from pyronova.testing import TestClient

# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()


@app.get("/")
def root(req):
    return "ready"


@app.post("/upload", gil=True, stream=True)
def upload(req):
    total = 0
    chunks = 0
    for chunk in req.stream:
        total += len(chunk)
        chunks += 1
    return {"bytes": total, "chunks": chunks}


def test_streaming_handler_receives_all_chunks():
    """Functional check: streamed uploads still work correctly with the
    new bounded channel. Sends 1 MB split into ~16KB hyper frames and
    the handler should see the same total byte count."""
    payload = b"x" * (1 * 1024 * 1024)  # 1 MB
    with TestClient(app, port=None) as c:
        resp = c.post("/upload", body=payload)
        data = resp.json()
        assert data["bytes"] == len(payload)
        # With CHANNEL_CAPACITY=8 and hyper frames of typically a few KB
        # to tens of KB, we expect at least 1 chunk. Upper bound depends
        # on hyper's chunk size; just sanity-check it's not absurd.
        assert data["chunks"] >= 1
