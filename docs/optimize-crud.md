# Optimize CRUD

> **Superseded prerequisite (2026-09, Layer 2, `docs/design/real-engine-in-workers.md`):** the "SharedState C-FFI bridge" below (`src/bridge/state_bridge.rs`, a bridge-backed replacement for the `_bootstrap.py` mock) was never built. Workers now import the real `pyronova.engine`, and `SharedState()` / `app.state` in a worker is handed the running app's map (C2). The CRUD routes themselves are still `gil=True` (the stated `block_on` reason is stale since `run_on_db_rt`); moving them into workers is a separate follow-up.


Plan to lift the `/crud/*` path past its current ceiling. Score:
**CRUD 5.6 (12,432 req/s)** — the lowest in the suite.

## Current state

All four CRUD routes in `arena_submission/app.py` still carry `gil=True`:

| Route | Line | Behavior |
|---|---|---|
| `GET /crud/items/{id}` | 325 | cache-aside via `_CRUD_CACHE` (Python dict), DB on miss |
| `GET /crud/items` | 362 | DB list query, no cache |
| `PUT /crud/items/{id}` | 390 | DB update, invalidates cache |
| `POST /crud/items` | 416 | DB upsert |

`/async-db` already left `gil=True` behind via Design A in
`arena-async-db-and-static.md` and lives at 27k req/s. CRUD did not migrate
and is at 12k — sitting on the **old, pre-bridge bottleneck**.

### Why this is half of `/async-db`

The math: 12,432 req/s ÷ ~4 GIL-bridge workers (`launcher.py:55` default) ≈
**3,108 req/s/worker**. Per-worker throughput is OK; the cap is the worker
count. `gil=True` routes funnel onto the main interpreter's bounded pool.
Sub-interp pool (≈64 workers) sits idle for these routes.

### Why CRUD didn't migrate

`crud_get_one:339` uses `_CRUD_CACHE` — a process-local Python dict — for
cache-aside. The Arena validator checks the `X-Cache: HIT|MISS` contract:
a second GET for the same id must hit. Under `gil=True` the cache is
visible to every request because they all run in one interpreter.

Drop `gil=True` and dispatch fans across N sub-interp workers. Each
sub-interp has its own `_CRUD_CACHE` (per-interpreter dict, by design).
Second GET lands on a different worker → still MISS → validator fails.

This is a structural block on the migration, not a one-line edit.

## The unlock: move the cache to SharedState

`pyronova.SharedState` (Rust `Arc<DashMap<String, Bytes>>` in `src/state.rs`)
is shared across all sub-interpreters by design — every worker sees one
global map, lock-free for distinct keys, nanosecond access. After
migration:

```python
# Before
_CRUD_CACHE: dict[int, tuple[dict, float]] = {}

# After
_CRUD_CACHE = app.state  # SharedState — global across all sub-interps
```

### Prerequisite: SharedState C-FFI bridge ⚠️

**SharedState is not yet usable from sub-interpreters.** Same situation
`PgPool` was in before Design A landed:

- `src/state.rs:18` declares `#[pyclass] SharedState` — accessible only
  from main interp's `gil=True` routes.
- `python/pyronova/_bootstrap.py:161` injects an empty mock
  (`type("SharedState", (), {})`) into each sub-interp's `pyronova.engine`
  namespace, so imports don't crash — but methods don't exist.
- `ROADMAP.md:351` confirms: `app.state` currently requires `gil=True`;
  native sub-interp access is planned but not done.

So step #1 below has a **strict prerequisite**: build a state bridge
mirroring the DB bridge.

#### State bridge design (mirrors `db_bridge.rs`)

New file `src/bridge/state_bridge.rs`. Inject 4 (or 5) C-FFI capsules
into each sub-interp's globals at startup — same injection point as
`_pyronova_db_fetch_all` etc. in `src/interp.rs`:

```rust
// src/bridge/state_bridge.rs (new, ~150 LOC)
#[no_mangle]
pub extern "C" fn _pyronova_state_get(
    key_ptr: *const c_char, key_len: usize,
) -> *mut ffi::PyObject;       // returns PyBytes or Py_None

#[no_mangle]
pub extern "C" fn _pyronova_state_set(
    key_ptr: *const c_char, key_len: usize,
    val_ptr: *const u8,        val_len: usize,
);

#[no_mangle]
pub extern "C" fn _pyronova_state_delete(
    key_ptr: *const c_char, key_len: usize,
) -> c_int;                     // 1 if deleted, 0 if missing

#[no_mangle]
pub extern "C" fn _pyronova_state_contains(
    key_ptr: *const c_char, key_len: usize,
) -> c_int;

// Optional but recommended for counters: atomic incr
#[no_mangle]
pub extern "C" fn _pyronova_state_incr(
    key_ptr: *const c_char, key_len: usize, amount: i64,
) -> i64;
```

All 5 bottom out at the **same** `Arc<DashMap>` the main-interp pyclass
uses — process-global, no per-interp state. No tokio runtime needed
(DashMap calls are sync), so this is much simpler than the DB bridge:
no `run_on_db_rt`, no `sync_channel.recv()`. Just GIL-detach → DashMap
op → GIL-attach.

```python
# python/pyronova/_bootstrap.py (replace the empty mock)
class _SharedState:
    def __getitem__(self, key):
        v = _pyronova_state_get(key.encode())
        if v is None:
            raise KeyError(key)
        return v.decode()
    def __setitem__(self, key, value):
        _pyronova_state_set(key.encode(), str(value).encode())
    def __delitem__(self, key):
        if not _pyronova_state_delete(key.encode()):
            raise KeyError(key)
    def __contains__(self, key):
        return bool(_pyronova_state_contains(key.encode()))
    def get(self, key, default=None):
        v = _pyronova_state_get(key.encode())
        return v.decode() if v is not None else default
    def incr(self, key, amount=1):
        return _pyronova_state_incr(key.encode(), amount)
    # ... set_bytes / get_bytes for binary if needed

_mock_engine.SharedState = _SharedState   # replaces type("SharedState", (), {})
```

Scope: ~150 LOC Rust + ~30 LOC Python. Strictly contained — if anything
breaks, revert injection and `gil=True` routes are untouched.

### Caveats once the bridge lands

- **TTL eviction**: today's code stamps an expiry tuple
  (`(item, now + _CRUD_TTL_S)`). SharedState stores `Bytes`, not Python
  tuples — encode TTL into the value (e.g. `f"{expiry:.3f}\n{json}"` or
  msgpack `[expiry, blob]`). Expired entries linger until next GET; add
  a periodic sweep if memory grows. Alternative: a Rust-side
  `moka::Cache` with native TTL (separate doc, ROADMAP.md mentions this).
- **Cache invalidation on PUT**: `_CRUD_CACHE.pop(item_id, None)` →
  `del state[key]` (or wrap to swallow `KeyError`).
- **Bytes-only values**: SharedState stores `Bytes`, not arbitrary Python
  objects. The cached item dict has to be serialized (json bytes is the
  natural choice — and aligns with optimization #2 below, "cache the
  pre-serialized JSON bytes"). Two birds, one stone: the encoding for
  SharedState storage IS the response body bytes.
- **Per-worker startup state**: nothing else to migrate; `_CRUD_CACHE`
  is the only mutable global the routes depend on.

## Optimization roadmap (ranked by ROI)

### 1. Build state bridge + migrate cache + drop `gil=True` (the biggest win)

**Two PRs, must ship in order:**

#### PR 1a — State bridge (no behavior change)

Land `src/bridge/state_bridge.rs` + `_bootstrap.py` swap (the design in
the prerequisite section above). After this PR, sub-interps can call
`app.state[...]`, but no caller exists yet. Validates:

- Smoke test: a new `def state_route(req)` (no `gil=True`) that does
  `app.state["k"] = "v"; return app.state["k"]` — must work end-to-end
- All existing tests still pass (mock replacement is additive)
- main-interp `gil=True` routes still work (same `Arc<DashMap>` backs
  both paths)

#### PR 1b — CRUD migration

```python
# arena_submission/app.py
- _CRUD_CACHE: dict[int, tuple[dict, float]] = {}
+ # _CRUD_CACHE replaced by app.state — see prerequisite caveats for
+ # the value encoding (TTL prefix + json bytes).

- @app.get("/crud/items/{id}", gil=True)
+ @app.get("/crud/items/{id}")
  def crud_get_one(req): ...

# same removal for /crud/items, PUT, POST
```

The four route bodies need small adapters around `app.state` (encode TTL
into the value, decode on read, handle `KeyError` instead of `None`).
Worth keeping a thin `_crud_cache_get(id)` / `_crud_cache_set(id, item)`
helper module-local for clarity.

Parallelism ceiling moves from `gil_bridge_workers (4)` to
`sub_interp_workers (~64)`. Per-route throughput rises by **5–10x**
before any other optimization touches the code path.

Validation must keep passing:

- HIT/MISS observable across requests (SharedState is global → trivially
  yes)
- PUT invalidation visible to subsequent GET (same)
- 4xx contracts (`_BAD_REQUEST`, `_NOT_FOUND`) unchanged

Expected: **12k → 60–100k req/s** on the GET-heavy mix the validator runs.

### 2. JSON serialization (overlaps with `optimize-json.md`)

Once #1 lands, the cache HIT path is the hottest code in the suite:

```python
return Response(
    body=json.dumps(entry[0]),                       # ← whole budget
    content_type="application/json",
    headers={"x-cache": "HIT"},
)
```

No DB call, no row decode — pure Python dict → JSON bytes. The
`optimize-json.md` #1 rewrite (Python → bytes directly, skip
`serde_json::Value`) gives the maximum relative win on exactly this
shape.

Per-request save: **100–200 µs**, roughly **+15–25%** on the HIT path
specifically.

Even better: short-circuit by caching the **already-serialized JSON
bytes** in SharedState instead of the Python dict. The HIT path becomes
zero serialization, just a `Response.from_bytes`. Validation: re-check
that the value still matches expected JSON byte-for-byte (PUT must
overwrite the cached bytes too).

### 3. Bulk row decode on MISS path

Same as `optimize-async-db.md` #2a:

`fetch_one` and `fetch_all` rebuild row dicts column-by-column on the
sub-interp side. For `crud_list` returning 10 rows × 9 columns = 90
PyObject allocations per request. Move the dict construction into the
bridge (`src/bridge/db_bridge.rs`) so each call returns a ready-built
`PyList[PyDict]`.

Free if `optimize-async-db.md` #2a lands first — same code path, same
fix. **+5–8%** on MISS-heavy traffic.

### 4. Async handler + concurrent queries

Same as `optimize-async-db.md` #3. Once #1 above is done, CRUD MISS paths
are sync-blocked on PG round-trip per worker. Async bridge lets one
worker hold multiple in-flight queries.

Bigger payoff for `/crud/items` (list) than for the GETs (which are
mostly cache HITs and don't touch DB).

**Defer until phase-7.2 async bridge covers DB.**

### 5. PG-side: prepared statement cache

`optimize-async-db.md` #4 — same fix, same code, applies here too.
**+5–10%** on MISS path.

## Out of scope

- **Smarter cache eviction (LRU, sized cap)**: today's TTL-on-read works
  for Arena's bounded test set. Real workloads need a Rust-side bounded
  cache, but that's a feature, not a perf fix.
- **Write-through cache for PUT/POST**: would let the next GET hit
  immediately. Marginal for the validator (which probes specific keys);
  worth a microbench but not blocking.

## Suggested execution order

1. **PR 1a — State C-FFI bridge.** `src/bridge/state_bridge.rs` + 4–5
   capsules + `_bootstrap.py` mock replacement. No behavior change to
   existing routes. Smoke test with a new non-`gil=True` route that
   touches `app.state`. **Hard prerequisite for everything else.**
2. **PR 1b — CRUD migration.** Replace `_CRUD_CACHE` with `app.state`,
   remove `gil=True` from all 4 CRUD routes. Re-run Arena validator
   end-to-end — this is the change most likely to break the HIT/MISS
   contract if value encoding doesn't round-trip cleanly. Expected:
   12k → 60–100k req/s.
3. **Cache pre-serialized JSON bytes** in SharedState (extension of
   PR 1b). Trivial diff once SharedState is the cache backend; pairs
   with `optimize-json.md` landing.
4. Adopt `optimize-json.md` and `optimize-async-db.md` PRs as they land —
   no CRUD-specific work, the wins flow through automatically.
5. **#4 async bridge** is the last lever; track with phase-7.2.

## Validation

- Arena validator full pass (HIT/MISS, PUT invalidation, 4xx contracts)
- TechEmpower-style score: target 5.6 → 25+ after step 1, → 30+ after
  step 2
- Microbench: GET hit p50/p99 with 64 concurrent clients before/after
- `arena_submission` validation job — must keep passing on every PR

## Related docs

- `docs/arena-async-db-and-static.md` — Design A pattern (already shipped
  for `/async-db`)
- `docs/optimize-async-db.md` — sister doc; #2a/#3/#4 here are shared
  changes
- `docs/optimize-json.md` — JSON writer rewrite that #2 builds on
