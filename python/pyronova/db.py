"""Async Postgres support for Pyronova handlers.

Thin Python-side re-export of the Rust `PgPool` class. Initialize once at
startup, then call `pool.fetch_one(...)`, `.fetch_all(...)`, `.fetch_scalar(...)`,
`.execute(...)` from any handler, sub-interpreter workers included: every
interpreter shares one connection pool. The query runs on a dedicated DB
runtime while the calling thread waits with its GIL released, so other
workers make progress. The `*_async` variants run on the main interpreter
(`gil=True` routes); in a worker they raise `NotImplementedError`.

Example::

    from pyronova import Pyronova
    from pyronova.db import PgPool

    app = Pyronova()
    pool = PgPool.connect("postgres://localhost/mydb", max_connections=20)

    @app.get("/users/{id}", gil=True)
    def get_user(req):
        row = pool.fetch_one(
            "SELECT id, name, email FROM users WHERE id = $1",
            int(req.params["id"]),
        )
        if row is None:
            return Response({"error": "not found"}, 404)
        return row

Supported parameter types: int, float, str, bool, bytes, None, dict
(JSON), list (JSON). Rows decode the same set plus the Postgres type
families int2/int4/int8, float4/float8, text/varchar/char, bytea, bool,
json/jsonb.

For large result sets, use `pool.fetch_iter(sql, ...)` to get a
streaming cursor — O(1) memory, rows yielded one at a time; it works in any
interpreter. Returning a streaming *response* (`Stream`) needs `gil=True`,
so an export-style handler looks like this:

    @app.get("/export", gil=True)
    def export(req):
        def stream():
            for row in pool.fetch_iter("SELECT * FROM transactions"):
                yield json.dumps(row) + "\\n"
        return Stream(stream())

Deferred to v2: datetime / uuid / decimal types; transactions;
automatic Pydantic model mapping.
"""

from .engine import PgCursor, PgPool

__all__ = ["PgCursor", "PgPool"]
