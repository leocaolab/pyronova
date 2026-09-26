"""Postgres support for Pyronova handlers.

Thin Python-side re-export of the Rust `PgPool` class. Initialize once at
startup, then call `pool.fetch_one(...)`, `.fetch_all(...)`, `.fetch_scalar(...)`,
`.execute(...)` from any handler, sub-interpreter workers included: every
interpreter shares one connection pool. The query runs on a dedicated DB
runtime while the calling thread waits with its GIL released, so other
workers make progress. The `*_async` variants run on the main interpreter
(`gil=True` routes); in a worker they raise `NotImplementedError`.

A process has one pool: a later `PgPool.connect()` returns the same pool, and
raises `ValueError` if it asks for a different DSN or different pool settings
(`max_connections`, `acquire_timeout_secs`). Settings left out match the open pool.

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

Parameters are encoded as the type the statement declares for them (the
server infers it from the SQL), so the same SQL behaves the same whatever
values came first, and ``None`` is a NULL of the right type. Python ↔ Postgres:

    bool                        bool
    int                         int2 / int4 / int8 (range-checked); float4/8, numeric
    float                       float4 / float8; numeric
    str                         text / varchar / char / name, enum labels
    bytes                       bytea
    dict / list                 json / jsonb
    decimal.Decimal             numeric (NaN and ±Infinity included)
    uuid.UUID                   uuid
    datetime.date               date
    datetime.datetime (naive)   timestamp
    datetime.datetime (aware)   timestamptz (read back in UTC)

A value the declared type cannot take (wrong type, out of range, not encodable)
raises ``ParamError``, a subclass of both ``TypeError`` and ``ValueError``,
before the query is sent. An int past 64 bits can be a ``numeric``; for an
integer column it is out of range. A column of any other type — arrays, inet, interval,
citext, … — reads back as its raw binary wire ``bytes``; cast it in SQL
(``col::text``) to get text. An ambiguous parameter such as ``SELECT $1`` is
text; cast it (``$1::int``) to send another type.

A failed query raises ``DatabaseError`` (a ``RuntimeError``) carrying the
server's SQLSTATE in ``.sqlstate`` (``None`` if the server never reported one,
e.g. a pool timeout). Integrity violations raise ``IntegrityError``, duplicate
keys its subclass ``UniqueViolation`` (SQLSTATE 23505).

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

Not supported: transactions; mapping rows to Pydantic models.
"""

from .engine import DatabaseError, IntegrityError, ParamError, PgCursor, PgPool, UniqueViolation

__all__ = ["DatabaseError", "IntegrityError", "ParamError", "PgCursor", "PgPool", "UniqueViolation"]
