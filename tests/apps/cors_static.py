"""App for tests/test_cors_404.py::test_static_file_hit_carries_cors: CORS plus a static
mount whose root the test creates and exports as PYRONOVA_TEST_CORS_STATIC_DIR before
importing this module (workers inherit the environment)."""

import os

from pyronova import Pyronova

app = Pyronova()
app.enable_cors(allow_origins="https://app.example.com")
app.static("/static", os.environ["PYRONOVA_TEST_CORS_STATIC_DIR"])


@app.get("/")
def root(req):
    return "hello"
