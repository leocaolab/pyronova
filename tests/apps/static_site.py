"""Static-file app for tests/test_static_files.py. The suite's `static_dir` fixture
creates the root and exports it as PYRONOVA_TEST_STATIC_DIR before importing this
module; workers inherit the environment, so they mount the same root."""

import os

from pyronova import Pyronova

app = Pyronova()


@app.get("/")
def index(req):
    return {"api": True}


app.static("/static/", os.environ["PYRONOVA_TEST_STATIC_DIR"])
