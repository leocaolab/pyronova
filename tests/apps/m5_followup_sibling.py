"""A sibling module tests/test_review_m5_followup.py imports as `tests.apps.…`; each
sub-interpreter worker imports it again when it re-executes that test file."""

import sys


def add_routes(app) -> None:
    # TestClient's readiness probe hits "/".
    @app.get("/")
    def root(req):
        return {"ready": True}

    @app.get("/import-env")
    def import_env(req):
        return {"sys_path": sys.path, "sibling": __name__}
