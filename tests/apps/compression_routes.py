"""Routes shared by tests/test_compression.py's two apps (compression off / on)."""

from pyronova import Response

LARGE_JSON = {"items": ["hello world" for _ in range(200)]}  # ~2.5 KB
LARGE_TEXT = "abcdefghijklmnopqrstuvwxyz" * 200  # ~5 KB


def add_routes(app) -> None:
    # TestClient readiness probe hits "/"; 404 would loop forever.
    @app.get("/")
    def root(req):
        return {"ready": True}

    @app.get("/small")
    def small(req):
        return {"ok": True}

    @app.get("/big-json")
    def big_json(req):
        return LARGE_JSON

    @app.get("/big-text")
    def big_text(req):
        return Response(LARGE_TEXT, content_type="text/plain; charset=utf-8")

    @app.get("/big-binary")
    def big_binary(req):
        return Response(bytes([0] * 4096), content_type="image/png")

    @app.get("/preset-encoding")
    def preset_encoding(req):
        # Handler already set Content-Encoding — framework must not re-compress
        return Response(
            LARGE_TEXT,
            content_type="text/plain; charset=utf-8",
            headers={"Content-Encoding": "identity"},
        )
