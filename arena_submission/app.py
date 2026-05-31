"""Pyronova framework — HTTP Arena submission.

Exposes the eight endpoints required by the Arena test harness, mirroring
the Actix / FastAPI reference implementations for semantic parity.
Route-by-route behavior is identical so head-to-head numbers are
apples-to-apples; what varies is the server engine underneath.

Pyronova specifics used here:
  * `Pyronova()` — sub-interpreter-backed app (PEP 684), auto-dual pool
  * `app.enable_compression()` — gzip/brotli Accept-Encoding negotiation
  * `@app.post(stream=True)` — chunked body ingest without buffering
  * `pyronova.db.PgPool` — async sqlx-backed PG pool shared across
    workers via Rust-side OnceLock
  * `app.static(prefix, dir)` — async-fs-served static files
  * TLS via `PYRONOVA_TLS_CERT` / `PYRONOVA_TLS_KEY` env (launcher picks up)

The app is ~120 lines because the Rust engine does the heavy lifting —
no boilerplate for workers, TLS, or compression.
"""

import json
import logging
import os

from pyronova import Pyronova, Request, Response
from pyronova.db import PgPool

# Log benchmark-path errors at WARNING so they surface in the runner log
# but don't flood the tracing subscriber under load. Every broad-except
# site below calls log.warning(..., exc_info=True) so the stack trace is
# preserved instead of silently swallowed — swallowing a traceback to
# hand a 404 / 400 / {} back has been a regular source of "why is
# throughput suddenly tanking?" debugging evenings elsewhere.
log = logging.getLogger("pyronova.arena")


# ---------------------------------------------------------------------------
# Dataset (loaded once at process start)
# ---------------------------------------------------------------------------

DATASET_PATH = os.environ.get("DATASET_PATH", "/data/dataset.json")
try:
    with open(DATASET_PATH) as f:
        DATASET_ITEMS = json.load(f)
except Exception:
    # The file's documented contract (~line 40) is: every broad-except
    # logs with exc_info=True so init failures aren't silent. Apply
    # consistently here (arc finding arena-app-1).
    import logging as _arena_log
    _arena_log.getLogger(__name__).warning(
        "arena_submission: failed to load DATASET_PATH=%r; "
        "/json endpoints will return empty results",
        DATASET_PATH, exc_info=True,
    )
    DATASET_ITEMS = []


# ---------------------------------------------------------------------------
# Postgres pool (Rust-side OnceLock shared by all workers)
# ---------------------------------------------------------------------------

PG_POOL = None
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    try:
        # Arena harness sets DATABASE_MAX_CONN to the total budget; we follow
        # the Actix convention of using the whole pool size on the Rust side.
        max_conn = int(os.environ.get("DATABASE_MAX_CONN", "256"))
        PG_POOL = PgPool.connect(DATABASE_URL, max_connections=max_conn)
    except Exception:
        # Same contract as arena-app-1: surface init failures so
        # benchmark profiles that need PG don't silently regress to
        # empty responses (arc finding arena-app-2).
        import logging as _arena_log
        _arena_log.getLogger(__name__).warning(
            "arena_submission: PgPool.connect failed; /async-db and "
            "/crud/* will return empty/500 responses",
            exc_info=True,
        )
        PG_POOL = None


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = Pyronova()
# Arena's upload profile sends bodies up to 20 MiB; the engine default
# is 10 MiB (set to protect normal apps from run-away uploads). Bump to
# 25 MiB for the benchmark target so the 20 MiB template isn't 413'd.
app.max_body_size = 25 * 1024 * 1024
# min_size=256 skips tiny bodies (/pipeline "ok", short query params).
# Arena's json-comp rotates through /json/1..50 with pipeline depth 25
# and hundreds of connections — throughput is bounded by compression
# CPU, so pick the cheapest settings that still compress meaningfully:
# gzip level=1 (fastest) wins ~2x speed over level=6 at ~15% worse ratio,
# and brotli quality=0 is the brotli equivalent.
app.enable_compression(min_size=256, brotli_quality=0, gzip_level=1)

# Static serving — Arena harness populates /data/static/.
app.static("/static", "/data/static")


# Fast-path /pipeline: served directly from the Rust accept loop with
# zero Python dispatch (GIL, sub-interp, handler call — all skipped).
# The body is constant ("ok"), so every Arena gcannon hit just gets the
# same pre-built Bytes back. This is what `add_fast_response` is for —
# health-checks, robots.txt, static probe endpoints.
#
# Doesn't affect any other route. Dynamic handlers keep their normal
# Python dispatch path. Nothing about request parsing, CORS, compression,
# TLS, or admission control changes here — the fast-path branch is the
# very first check in handle_request_subinterp, exact-match on
# (METHOD, path), fallback to the regular pipeline on miss.
app.add_fast_response("GET", "/pipeline", b"ok", content_type="text/plain")


def _sum_query_params(req) -> int:
    total = 0
    for v in req.query_params.values():
        try:
            total += int(v)
        except ValueError:
            pass
    return total


@app.get("/baseline11")
def baseline11_get(req: "Request"):
    return Response(str(_sum_query_params(req)), content_type="text/plain")


@app.post("/baseline11")
def baseline11_post(req: "Request"):
    total = _sum_query_params(req)
    body = req.body
    if body:
        try:
            total += int(body.decode("ascii", errors="strict").strip())
        except (ValueError, UnicodeDecodeError):
            pass
    return Response(str(total), content_type="text/plain")


@app.get("/baseline2")
def baseline2(req: "Request"):
    return Response(str(_sum_query_params(req)), content_type="text/plain")


@app.post("/upload", gil=True, stream=True)
def upload(req: "Request"):
    # drain_count() runs the whole consume loop in Rust with the GIL
    # released once — vs a Python `for chunk in req.stream:` that pays
    # GIL release+reacquire + PyBytes alloc per 16 KB hyper frame
    # (~1600 iterations for a 25 MB upload). Worth ~50% throughput on
    # the /upload profile; zero impact on streaming use cases that
    # actually want the per-chunk bytes.
    try:
        size = req.stream.drain_count()
    except Exception:
        # drain_count() is a Rust FFI call over the live request stream;
        # a mid-upload client disconnect or a corrupt hyper frame raises
        # here. Match the file-wide contract (line ~30): log with the
        # trace instead of 500'ing silently, and hand back 400 — the
        # body was never fully received, so the request is malformed from
        # the server's view (arc finding ISSUE-65).
        log.warning("/upload: stream.drain_count failed", exc_info=True)
        return _BAD_REQUEST
    return Response(str(size), content_type="text/plain")


# Same payload shape + multiplier semantics as Actix/FastAPI reference:
# take the first `count` dataset items, set `total = price * quantity * m`,
# return {"items": [...], "count": N}.
#
# We return a plain Python dict rather than a pre-serialized bytes body.
# Pyronova's Rust response path detects dict/list returns and serializes via
# `pythonize + serde_json::to_vec` — native Rust JSON (~30μs for a
# 50-item payload). Using Python's stdlib `json.dumps` instead costs
# ~150μs per call on the same data. Returning the dict shaves ~100μs
# per request on the /json profile.
@app.get("/json/{count}")
def json_endpoint(req: "Request"):
    # Returning a dict directly triggers Pyronova's Rust-side JSON
    # serialization path (pythonize + serde_json::to_vec). Empirically
    # this matches or beats orjson.dumps() + Response(bytes) for
    # small nested payloads — the explicit orjson path pays the C-API
    # wrap twice (orjson → bytes, bytes → Response) while the
    # dict-return path is a single Rust traversal.
    try:
        count = int(req.params["count"])
    except (KeyError, ValueError):
        return {"items": [], "count": 0}
    try:
        m = int(req.query_params.get("m", "1"))
    except ValueError:
        m = 1
    count = max(0, min(count, len(DATASET_ITEMS)))
    # A schema-wrong dataset (missing price/quantity on an item) must not
    # KeyError at request time — the load at line ~42 only validates JSON
    # syntax, not shape. Default the multiplicands to 0 (and coerce a null
    # price/quantity to 0 via `or 0`) so a malformed item yields total=0
    # instead of crashing the /json profile (arc finding arena-5/arena-app).
    items = [
        {**dsitem, "total": (dsitem.get("price", 0) or 0) * (dsitem.get("quantity", 0) or 0) * m}
        for dsitem in DATASET_ITEMS[:count]
    ]
    return {"items": items, "count": count}


@app.get("/json-comp/{count}")
def json_comp_endpoint(req: "Request"):
    # Identical payload; Arena's json-comp profile hits /json/{count} in
    # practice (see benchmark-15), but we keep this alias registered for
    # legacy URL shape compatibility.
    return json_endpoint(req)


# Async DB — mirrors Actix's query against the items table. PgPool is a
# process-wide handle; each worker thread blocks on its own fetch but the
# pool-side tokio runtime drives all of them concurrently.
PG_SQL = (
    "SELECT id, name, category, price, quantity, active, tags, "
    "rating_score, rating_count "
    "FROM items WHERE price BETWEEN $1 AND $2 LIMIT $3"
)


@app.get("/async-db")
def async_db_endpoint(req: "Request"):
    # Runs on a sub-interpreter worker. `PG_POOL` here is the mock
    # `_PgPool` proxy installed by `_bootstrap.py` — calls forward to
    # four C-FFI entry points (`_pyronova_db_fetch_{all,one,scalar}`,
    # `_pyronova_db_execute`) that Rust injected into each worker's
    # globals at startup. Those entry points queue the sqlx future
    # onto the dedicated DB tokio runtime via `rt.spawn + channel`
    # (see `src/bridge/db_bridge.rs::run_on_db_rt`) and block the
    # sub-interp thread until the result lands — no nested runtime,
    # no main-interp GIL bottleneck, parallelism ceiling is
    # `min(sub_interp_workers, DATABASE_MAX_CONN)`.
    if PG_POOL is None:
        return _EMPTY_DB_RESPONSE
    q = req.query_params
    try:
        min_val = int(q.get("min", "10"))
        max_val = int(q.get("max", "50"))
        limit = int(q.get("limit", "50"))
        limit = max(1, min(limit, 50))
    except ValueError:
        log.warning("/async-db: bad query params %r", dict(q), exc_info=True)
        return _EMPTY_DB_RESPONSE
    try:
        rows = PG_POOL.fetch_all(PG_SQL, min_val, max_val, limit)
    except RuntimeError:
        # pyronova.db raises RuntimeError for sqlx failures; keep the
        # empty-response contract Arena expects, but don't lose the trace.
        log.warning("/async-db: fetch_all failed", exc_info=True)
        return _EMPTY_DB_RESPONSE
    return _rows_to_payload(rows)


def _parse_tags(raw):
    # The `tags` column is jsonb; the driver usually hands back a
    # dict/list already, but a string-typed column (or a row written
    # outside the jsonb contract) can carry malformed JSON. A bare
    # json.loads() there raises JSONDecodeError and 500s the request —
    # match the file-wide contract: log with the trace, degrade to an
    # empty tag list rather than crash the handler (arc finding arena-1/2).
    if raw.__class__ is str:
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            log.warning("malformed tags JSON %r; using []", raw, exc_info=True)
            return []
    return raw


def _rows_to_payload(rows):
    # Hot loop — shaves ~30% per-row Python overhead by reading each
    # column exactly once and skipping the `isinstance(tags, str)` check
    # when PG already returned jsonb as dict/list (the common path).
    items = []
    append = items.append
    for row in rows:
        tags = _parse_tags(row["tags"])
        append({
            "id": row["id"],
            "name": row["name"],
            "category": row["category"],
            "price": row["price"],
            "quantity": row["quantity"],
            "active": row["active"],
            "tags": tags,
            "rating": {
                "score": row["rating_score"],
                "count": row["rating_count"],
            },
        })
    return {"items": items, "count": len(items)}


_EMPTY_DB_RESPONSE = {"items": [], "count": 0}
_NOT_FOUND = Response("not found", status_code=404, content_type="text/plain")
_BAD_REQUEST = Response("bad request", status_code=400, content_type="text/plain")
_INTERNAL_ERROR = Response("internal server error", status_code=500, content_type="text/plain")
_SERVICE_UNAVAILABLE = Response(
    "service unavailable",
    status_code=503,
    content_type="text/plain",
    # Retry-After hints clients to back off ~30s instead of hammering a
    # server that can't recover quickly (e.g. DB creds misconfigured at
    # startup). 503 already signals "ours and retryable"; this adds the
    # backpressure so a tight client retry loop doesn't pile on.
    headers={"retry-after": "30"},
)


# ---------------------------------------------------------------------------
# CRUD — paths mirror Arena's aspnet-minimal reference:
#   GET  /crud/items?category=X&page=N&limit=M   paginated list
#   GET  /crud/items/{id}                        single item (200ms cache)
#   POST /crud/items                             upsert, returns 201
#   PUT  /crud/items/{id}                        update, invalidates cache
#
# Cache is an in-process dict per sub-interpreter. Arena's aspnet impl
# uses IMemoryCache (same semantics). `gil=True` on every handler for
# the same reason /async-db needs it — our PgPool lives behind a
# Rust-side OnceLock populated by the main interpreter's module-import.
# ---------------------------------------------------------------------------

import time as _time

_CRUD_TTL_S = 0.2
# _CRUD_CACHE is a bare dict because every handler below runs with
# `gil=True` on Pyronova's main interpreter — only one handler thread
# executes at a time, so dict get/set/pop are atomic under the GIL and
# no lock is needed. If a handler is ever demoted off the main interp
# this dict becomes a race; wrap it in threading.Lock or flip to
# threading.local at that point.
_CRUD_CACHE: dict = {}  # item_id -> (payload_dict, expires_at_monotonic)

_CRUD_COLS = (
    "id, name, category, price, quantity, active, tags, "
    "rating_score, rating_count"
)
_CRUD_GET_SQL = f"SELECT {_CRUD_COLS} FROM items WHERE id = $1 LIMIT 1"
_CRUD_LIST_SQL = (
    f"SELECT {_CRUD_COLS} FROM items WHERE category = $1 "
    "ORDER BY id LIMIT $2 OFFSET $3"
)
# `name = $1, price = $2, quantity = $3 WHERE id = $4`. Arena's aspnet
# UPDATE doesn't touch tags/active/category — mirror exactly.
_CRUD_UPDATE_SQL = "UPDATE items SET name = $1, price = $2, quantity = $3 WHERE id = $4"
# Fixed tags/rating in the INSERT path — Arena's aspnet does the same
# (`'[\"bench\"]'` literal, rating 0/0) so the row always passes its
# CHECK constraints regardless of input shape.
_CRUD_UPSERT_SQL = (
    "INSERT INTO items "
    "(id, name, category, price, quantity, active, tags, rating_score, rating_count) "
    "VALUES ($1, $2, $3, $4, $5, true, '[\"bench\"]', 0, 0) "
    "ON CONFLICT (id) DO UPDATE SET name = $2, price = $4, quantity = $5 "
    "RETURNING id"
)


def _row_to_full_item(row):
    tags = _parse_tags(row["tags"])
    return {
        "id": row["id"],
        "name": row["name"],
        "category": row["category"],
        "price": row["price"],
        "quantity": row["quantity"],
        "active": row["active"],
        "tags": tags,
        "rating": {"score": row["rating_score"], "count": row["rating_count"]},
    }


@app.get("/crud/items/{id}", gil=True)
def crud_get_one(req: "Request"):
    # Arena validator requires an `X-Cache: MISS|HIT` response header on
    # this endpoint so the cache-aside test can observe that a second
    # GET for the same id hits the in-process cache without re-querying
    # Postgres. Header is computed once per code path and wrapped in a
    # Response() because a bare dict return would skip custom headers.
    if PG_POOL is None:
        # No DB connection is a server-side outage, not a missing item —
        # 503 tells clients the failure is ours and retryable, not 404
        # ("item not found") which misleads (matches PUT/POST handlers).
        return _SERVICE_UNAVAILABLE
    try:
        item_id = int(req.params["id"])
    except (KeyError, ValueError):
        return _BAD_REQUEST
    now = _time.monotonic()
    entry = _CRUD_CACHE.get(item_id)
    if entry is not None and entry[1] > now:
        return Response(
            body=json.dumps(entry[0]),
            content_type="application/json",
            headers={"x-cache": "HIT"},
        )
    try:
        row = PG_POOL.fetch_one(_CRUD_GET_SQL, item_id)
    except RuntimeError:
        log.warning("/crud/items/%s: fetch_one failed", item_id, exc_info=True)
        return _INTERNAL_ERROR
    if row is None:
        return _NOT_FOUND
    item = _row_to_full_item(row)
    _CRUD_CACHE[item_id] = (item, now + _CRUD_TTL_S)
    return Response(
        body=json.dumps(item),
        content_type="application/json",
        headers={"x-cache": "MISS"},
    )


@app.get("/crud/items", gil=True)
def crud_list(req: "Request"):
    if PG_POOL is None:
        return _EMPTY_CRUD_LIST
    q = req.query_params
    category = q.get("category") or "electronics"
    try:
        page = int(q.get("page", "1"))
        if page < 1:
            page = 1
    except ValueError:
        page = 1
    try:
        limit = int(q.get("limit", "10"))
    except ValueError:
        limit = 10
    if limit < 1 or limit > 50:
        limit = 10
    # Cap page so a pathological `?page=99999999999999999` can't build a
    # gigantic OFFSET that PG rejects (or that overflows a bind param).
    # 1M pages × 50/page is far past any real dataset.
    if page > 1_000_000:
        page = 1_000_000
    offset = (page - 1) * limit
    try:
        rows = PG_POOL.fetch_all(_CRUD_LIST_SQL, category, limit, offset)
    except RuntimeError:
        log.warning("/crud/items list: fetch_all failed", exc_info=True)
        return _INTERNAL_ERROR
    items = [_row_to_full_item(r) for r in rows]
    return {"items": items, "total": len(items), "page": page, "limit": limit}


@app.put("/crud/items/{id}", gil=True)
def crud_update(req: "Request"):
    if PG_POOL is None:
        # No DB connection is a server-side outage, not a missing item —
        # 503 tells clients the failure is ours and retryable, not 404
        # ("item not found") which misleads (arc finding arena-3).
        return _SERVICE_UNAVAILABLE
    try:
        item_id = int(req.params["id"])
        body = json.loads(req.body) if req.body else {}
    except (KeyError, ValueError, TypeError):
        return _BAD_REQUEST
    name = body.get("name") or "Updated"
    try:
        price = int(body.get("price", 0))
        quantity = int(body.get("quantity", 0))
    except (TypeError, ValueError):
        return _BAD_REQUEST
    try:
        affected = PG_POOL.execute(_CRUD_UPDATE_SQL, name, price, quantity, item_id)
    except RuntimeError:
        log.warning("/crud/items/%s update: execute failed", item_id, exc_info=True)
        return _INTERNAL_ERROR
    if affected == 0:
        return _NOT_FOUND
    _CRUD_CACHE.pop(item_id, None)
    return {"id": item_id, "name": name, "price": price, "quantity": quantity}


@app.post("/crud/items", gil=True)
def crud_upsert(req: "Request"):
    if PG_POOL is None:
        # Server-side DB outage — 503, not 400 ("your request is
        # malformed"). The client did nothing wrong (arc finding arena-4).
        return _SERVICE_UNAVAILABLE
    try:
        body = json.loads(req.body) if req.body else {}
        item_id = int(body["id"])
    except (KeyError, ValueError, TypeError):
        return _BAD_REQUEST
    name = body.get("name") or "New Product"
    category = body.get("category") or "test"
    try:
        price = int(body.get("price", 0))
        quantity = int(body.get("quantity", 0))
    except (TypeError, ValueError):
        return _BAD_REQUEST
    try:
        new_id = PG_POOL.fetch_scalar(
            _CRUD_UPSERT_SQL,
            item_id, name, category, price, quantity,
        )
    except RuntimeError:
        log.warning("/crud/items upsert id=%s: fetch_scalar failed", item_id, exc_info=True)
        return _INTERNAL_ERROR
    _CRUD_CACHE.pop(item_id, None)
    return Response(
        body=json.dumps({
            "id": new_id,
            "name": name,
            "category": category,
            "price": price,
            "quantity": quantity,
        }),
        status_code=201,
        content_type="application/json",
    )


_EMPTY_CRUD_LIST = {"items": [], "total": 0, "page": 1, "limit": 10}


if __name__ == "__main__":
    # launcher.py decides which port + TLS config to pass via env.
    host = os.environ.get("PYRONOVA_HOST", "0.0.0.0")
    port = int(os.environ.get("PYRONOVA_PORT", "8080"))
    # Detect worker count from cgroup cpu.max (same pattern as actix's helper).
    # Pyronova's engine will fall back to num_cpus if PYRONOVA_WORKERS isn't set.
    _tls_ports_env = os.environ.get("PYRONOVA_TLS_PORTS")
    extra_tls_ports = None
    if _tls_ports_env:
        extra_tls_ports = []
        for _p in _tls_ports_env.split(","):
            _p = _p.strip()
            if not _p:
                continue
            try:
                extra_tls_ports.append(int(_p))
            except ValueError:
                # Skip junk tokens rather than crashing startup on a
                # single bad entry in PYRONOVA_TLS_PORTS.
                log.warning("PYRONOVA_TLS_PORTS: ignoring non-numeric port %r", _p)
        extra_tls_ports = extra_tls_ports or None
    app.run(host=host, port=port, extra_tls_ports=extra_tls_ports)
