"""Static mount for tests/test_review_m1d.py's static tests. The `static_server` fixture
creates the tree and exports its root as PYRONOVA_TEST_M1D_STATIC_ROOT before importing
this module; workers inherit the environment, so they mount the same root."""

import os

from pyronova import Pyronova

app = Pyronova()


@app.get("/health")
def health(req):
    return {"ok": True}


app.static("/static", os.environ["PYRONOVA_TEST_M1D_STATIC_ROOT"])
