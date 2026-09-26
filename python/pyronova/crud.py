"""Lightweight CRUD REST helper built on :class:`pyronova.db.PgPool`.

Registers five standard REST routes in one call::

    from pyronova import Pyronova
    from pyronova.db import PgPool
    from pyronova.crud import register_crud

    app = Pyronova()
    pool = PgPool.connect("postgres://...")

    register_crud(
        app, pool,
        prefix="/users",
        table="users",
        columns=["id", "name", "email"],
        id_column="id",
        id_type=int,
    )

After ``register_crud`` returns the following routes are live:

=======  ==========  ============================================
Method   Path        Behavior
=======  ==========  ============================================
GET      /users      list rows (pagination via ?limit=&offset=)
GET      /users/{id} fetch one or 404
POST     /users      insert from JSON body, 201 + created row
PUT      /users/{id} update from JSON body, 200 + updated row or 404
DELETE   /users/{id} delete, 204 or 404
=======  ==========  ============================================

All SQL uses parameterized queries — column and table identifiers are
validated at registration time (alphanumeric + underscore only) and
never interpolated from request data. The ``columns`` list is the
allowlist: unknown keys in the JSON body are silently ignored on
POST/PUT, so a caller can't sneak in columns the developer didn't
intend to expose.
"""

import logging
import re
from dataclasses import dataclass
from typing import Callable, TYPE_CHECKING

from .app import Response
from ._errors import log_server_error, server_error_body
from .db import IntegrityError, ParamError

_log = logging.getLogger("pyronova.crud")

if TYPE_CHECKING:
    from .app import Pyronova
    from .db import PgPool

__all__ = ["register_crud"]


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Longest ?limit= / ?offset= value parsed: a legitimate one has a handful of digits, and
# int() on a million-digit string is a cheap way to burn server CPU.
_MAX_QUERY_INT_DIGITS = 18


def _validate_ident(name: str, role: str) -> str:
    """Refuse, at registration, a name that is not a plain identifier: it is
    interpolated into the SQL unquoted."""
    if not _IDENT_RE.match(name):
        raise ValueError(
            f"invalid SQL identifier for {role}: {name!r} "
            "(must match [A-Za-z_][A-Za-z0-9_]*)"
        )
    return name


@dataclass(frozen=True)
class _Rejected:
    """A request refused before it reaches the database, with the reply."""

    response: Response


def _json_object(req) -> "dict | _Rejected":
    try:
        body = req.json()
    except ValueError as e:
        # The client's own bad input: tell it what is wrong; not a server error.
        _log.info("rejected request body: %s", e)
        return _Rejected(Response(body={"error": f"invalid JSON: {e}"}, status_code=400))
    if not isinstance(body, dict):
        return _Rejected(Response(body={"error": "body must be a JSON object"}, status_code=400))
    return body


def _query(req, what: str, call: Callable, *args) -> object:
    """Run one pool call for `req`. A failure the client caused is refused with
    the database's reason; any other failure is logged with the request id and
    answered with a generic 500 carrying that id.

    - ``IntegrityError`` (SQLSTATE class 23: duplicate key, NOT NULL, CHECK,
      foreign key; ``UniqueViolation`` is a subclass) → 409.
    - ``ParamError``: the pool refused a body value the column's type cannot
      take, before sending the query → 422.
    - anything else (``DatabaseError``, pool timeout, a bug — including a
      ``TypeError`` / ``ValueError`` of our own) → 500. It is caught here rather
      than left to the framework so the client never sees exception text.
    """
    try:
        return call(*args)
    except IntegrityError as e:
        _log.info("%s: refused by a constraint: %s", what, e)
        return _Rejected(Response(body={"error": str(e)}, status_code=409))
    except ParamError as e:
        _log.info("%s: refused a value: %s", what, e)
        return _Rejected(Response(body={"error": str(e)}, status_code=422))
    except Exception:
        log_server_error(_log, req.request_id, "%s failed", what)
        return _Rejected(Response(body=server_error_body(req.request_id), status_code=500))


def register_crud(
    app: "Pyronova",
    pool: "PgPool",
    *,
    prefix: str,
    table: str,
    columns: list[str],
    id_column: str = "id",
    id_type: Callable[[str], object] = int,
    default_limit: int = 100,
    max_limit: int = 1000,
) -> None:
    """Register five REST routes backed by a Postgres table.

    Args:
        app: the ``Pyronova`` instance.
        pool: a connected ``PgPool``.
        prefix: URL prefix, e.g. ``"/users"`` (no trailing slash).
        table: SQL table name. Validated against ``[A-Za-z_][A-Za-z0-9_]*``.
        columns: list of columns to expose. The primary key should be
            included. Body keys outside this list are dropped.
        id_column: primary-key column name. Default ``"id"``.
        id_type: callable that coerces the raw path string to the right
            Python type for binding (``int`` by default; use ``str`` for
            UUID-ish keys).
        default_limit: rows returned by ``GET /{prefix}`` when ``limit`` is
            absent.
        max_limit: upper cap on the ``?limit=`` query param.
    """
    if not prefix.startswith("/"):
        raise ValueError("prefix must start with '/'")
    if prefix.endswith("/"):
        raise ValueError("prefix must not end with '/'")

    if not columns:
        raise ValueError("columns must not be empty")
    if len(set(columns)) != len(columns):
        raise ValueError(f"columns must be unique, got duplicates in {columns!r}")
    if not callable(id_type):
        raise TypeError(f"id_type must be callable, got {type(id_type).__name__}")
    # A non-int limit would fail every request, reported as the client's bad ?limit=.
    if not isinstance(default_limit, int) or isinstance(default_limit, bool):
        raise TypeError(f"default_limit must be int, got {type(default_limit).__name__}")
    if not isinstance(max_limit, int) or isinstance(max_limit, bool):
        raise TypeError(f"max_limit must be int, got {type(max_limit).__name__}")
    if default_limit < 1:
        raise ValueError(f"default_limit must be >= 1, got {default_limit}")
    if max_limit < 1:
        raise ValueError(f"max_limit must be >= 1, got {max_limit}")
    if default_limit > max_limit:
        raise ValueError(
            f"default_limit ({default_limit}) must not exceed max_limit ({max_limit})"
        )

    _validate_ident(table, "table")
    for c in columns:
        _validate_ident(c, "column")
    _validate_ident(id_column, "id_column")
    if id_column not in columns:
        raise ValueError(
            f"id_column {id_column!r} must be included in columns={columns!r}"
        )

    col_list = ", ".join(columns)
    non_id_cols = [c for c in columns if c != id_column]
    # With only the PK no PUT could ever succeed.
    if not non_id_cols:
        raise ValueError(
            f"columns={columns!r} must include at least one non-PK column "
            f"besides id_column {id_column!r} (PUT can only update non-PK columns)"
        )

    def parse_id(req) -> "object | _Rejected":
        raw_id = req.params.get("id")
        if raw_id is None:
            return _Rejected(Response(body={"error": "missing id"}, status_code=400))
        try:
            return id_type(raw_id)
        except (TypeError, ValueError) as e:
            # A converter's way of saying "not an id" (`int("abc")`, `UUID("x")`): the
            # client's error. Anything else it raises is a bug in the converter and takes
            # the 500 path, logged with the request id.
            return _Rejected(Response(body={"error": f"invalid id: {e}"}, status_code=400))

    # --- GET /prefix --------------------------------------------------------
    # Every route runs on main (gil=True); running them in workers is out of scope of
    # docs/design/real-engine-in-workers.md (non-goals).
    list_sql = f"SELECT {col_list} FROM {table} ORDER BY {id_column} LIMIT $1 OFFSET $2"

    @app.get(prefix, gil=True)
    def list_rows(req):
        q = req.query_params
        try:
            raw_limit = q.get("limit", default_limit)
            raw_offset = q.get("offset", 0)
            if max(len(str(raw_limit)), len(str(raw_offset))) > _MAX_QUERY_INT_DIGITS:
                raise ValueError("limit/offset too long")
            limit = max(1, min(int(raw_limit), max_limit))
            offset = max(int(raw_offset), 0)
        except (TypeError, ValueError):
            return Response(
                body={"error": "invalid limit/offset"},
                status_code=400,
            )
        rows = _query(req, "list_rows", pool.fetch_all, list_sql, limit, offset)
        if isinstance(rows, _Rejected):
            return rows.response
        return rows

    # --- GET /prefix/{id} ---------------------------------------------------
    get_sql = f"SELECT {col_list} FROM {table} WHERE {id_column} = $1"

    @app.get(f"{prefix}/{{id}}", gil=True)
    def get_row(req):
        id_val = parse_id(req)
        if isinstance(id_val, _Rejected):
            return id_val.response
        row = _query(req, "get_row", pool.fetch_one, get_sql, id_val)
        if isinstance(row, _Rejected):
            return row.response
        if row is None:
            return Response(body={"error": "not found"}, status_code=404)
        return row

    # --- POST /prefix -------------------------------------------------------
    # The columns inserted are the body's keys that are in `columns`: the client can't
    # name any other.
    @app.post(prefix, gil=True)
    def create_row(req):
        body = _json_object(req)
        if isinstance(body, _Rejected):
            return body.response

        present = [c for c in columns if c in body]
        if not present:
            return Response(
                body={"error": f"body must include at least one of {columns}"},
                status_code=422,
            )
        placeholders = ", ".join(f"${i + 1}" for i in range(len(present)))
        col_clause = ", ".join(present)
        insert_sql = (
            f"INSERT INTO {table} ({col_clause}) VALUES ({placeholders}) "
            f"RETURNING {col_list}"
        )
        args = [body[c] for c in present]
        row = _query(req, f"create_row: INSERT into {table}", pool.fetch_one, insert_sql, *args)
        if isinstance(row, _Rejected):
            return row.response
        return Response(body=row, status_code=201)

    # --- PUT /prefix/{id} ---------------------------------------------------
    @app.put(f"{prefix}/{{id}}", gil=True)
    def update_row(req):
        id_val = parse_id(req)
        if isinstance(id_val, _Rejected):
            return id_val.response
        body = _json_object(req)
        if isinstance(body, _Rejected):
            return body.response

        # Only update non-PK columns.
        present = [c for c in non_id_cols if c in body]
        if not present:
            return Response(
                body={"error": f"body must include at least one of {non_id_cols}"},
                status_code=422,
            )
        set_clause = ", ".join(f"{c} = ${i + 1}" for i, c in enumerate(present))
        id_placeholder = f"${len(present) + 1}"
        update_sql = (
            f"UPDATE {table} SET {set_clause} WHERE {id_column} = {id_placeholder} "
            f"RETURNING {col_list}"
        )
        args = [body[c] for c in present] + [id_val]
        row = _query(req, f"update_row: UPDATE in {table}", pool.fetch_one, update_sql, *args)
        if isinstance(row, _Rejected):
            return row.response
        if row is None:
            return Response(body={"error": "not found"}, status_code=404)
        return row

    # --- DELETE /prefix/{id} ------------------------------------------------
    delete_sql = f"DELETE FROM {table} WHERE {id_column} = $1"

    @app.delete(f"{prefix}/{{id}}", gil=True)
    def delete_row(req):
        id_val = parse_id(req)
        if isinstance(id_val, _Rejected):
            return id_val.response
        affected = _query(req, f"delete_row: DELETE from {table}", pool.execute, delete_sql, id_val)
        if isinstance(affected, _Rejected):
            return affected.response
        if affected == 0:
            return Response(body={"error": "not found"}, status_code=404)
        return Response(body=b"", status_code=204)
