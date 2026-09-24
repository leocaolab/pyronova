# Arena bottleneck design: async-db and static

> **Update (2026-09):** "Design B — make `pyronova.engine` sub-interp loadable" below was implemented as Layer 2 (`docs/design/real-engine-in-workers.md`): workers import the real engine, the mock `_mock_engine` and the C-FFI DB bridge (Design A, `db_bridge.rs`) are gone, and `PgPool` works in workers directly. The PyO3 0.28 blockers listed for Design B no longer apply (per-interpreter types come from the leocaolab/pyo3 fork).


Targeted design doc for two underperforming profiles in HttpArena v1 results on pyronova v2.1.5. Scope is narrow on purpose — each section ends with a concrete patch outline, not a redesign.

Benchmark target (Arena, TR 3995WX 64-core):

| profile | current | target | peer reference |
|---|---|---|---|
| async-db | 3.7k rps | 30–50k rps | actix ~60k, fastapi ~30k |
| static | 91k rps | 300k+ rps | actix ~400k, bun ~350k |

## Part 1 — async-db: break the `gil=True` forced serialization

### Current state

`arena_submission/app.py` marks the `/async-db` handler `gil=True`. Every request queues onto the **main interpreter's** worker, not the sub-interpreter pool. On a 64-core box this means:

- 64 sub-interp workers sit idle for this route.
- Main interp GIL serializes the handler's Python bytecode (param parsing, dict construction, JSON return).
- The sqlx pool is async and thread-safe, but dispatch is single-threaded.

Observed: 3.7k rps, matching a single-GIL bound on the Python bytecode + Rust-side transition cost per request (~270 µs/req).

### Why the "just unmock and go" plan doesn't work

The Rust side is already correct for multi-interp access:

- `static PG_POOL: OnceLock<sqlx::PgPool>` at `src/db.rs:43` — process-global, not per-interp.
- `sqlx::PgPool` is `Clone + Send + Sync` (internally `Arc<SharedPool>`).
- `PgPool` pyclass at `src/db.rs:321` is **zero-sized** (`pub(crate) struct PgPool;`). Methods dispatch through `pool_ref()` → the same global on every caller.
- `connect()` is already idempotent (`src/db.rs:340`): if the OnceLock is set, return `Ok(PgPool)`.

So the natural first instinct — "just delete the `_MockPgPool` and let sub-interps import the real PgPool" — was the direction of Part 1 of this doc's first draft. **It fails at the Python layer.**

**The real blocker**: `src/lib.rs:57` declares `#[pymodule] fn engine(...)` *without* a `Py_mod_multiple_interpreters` slot. Under CPython 3.12+, a module without that slot refuses to load in a sub-interpreter — the import raises `ImportError: module does not support loading in subinterpreters`. This is why `python/pyronova/_bootstrap.py:104-140` hand-rolls `_mock_engine`: it's the *only* way sub-interps can satisfy `from pyronova.engine import ...` statements in user code. Everything else — `Request`, `Response`, `SharedState` — is reconstituted inside the sub-interp via C-FFI shims, not via the Rust pyclass.

Removing the `_MockPgPool` today would:

1. Let `from pyronova.db import PgPool, PgCursor` execute in sub-interp.
2. That import triggers `from .engine import PgPool, PgCursor`.
3. `sys.modules["pyronova.engine"]` is already the mock (set at `_bootstrap.py:215`) with no `PgPool` attribute.
4. `ImportError` at sub-interp replay time. Every async-db request dies before reaching the handler.

So Part 1 needs a real C-FFI bridge for DB — the same pattern that carries Request/Response across the interp boundary. Not a one-liner.

### Two candidate designs

#### Design A — C-FFI DB bridge (recommended)

Build a `pyronova_db_query` C function, the same shape as `pyronova_recv` / `pyronova_send` in `src/interp.rs`. Sub-interp's mock `PgPool` is a thin Python class whose `fetch_all(sql, *params)` calls `_pyronova_db_query(worker_id, sql, params_tuple)`, which:

1. Serializes the params to a Rust-side `Vec<BoundParam>` while holding the sub-interp's GIL.
2. Releases GIL with `py.detach()`.
3. Looks up the global `PG_POOL` (already thread-safe).
4. Runs the sqlx query on the shared `PG_RUNTIME`. Other sub-interp workers can simultaneously drive their own queries on the same pool — sqlx handles the fan-in.
5. Serializes rows to a Rust-owned buffer (JSON bytes, or a msgpack-style encoding for speed).
6. Reacquires the calling sub-interp's GIL, materializes into `PyList<PyDict>`, returns.

Injection point: `src/interp.rs` already injects `_pyronova_emit_log`, `_pyronova_recv`, `_pyronova_send` into each sub-interp's globals as builtin PyCapsules. Add `_pyronova_db_query` / `_pyronova_db_execute` / `_pyronova_db_fetch_one` / `_pyronova_db_fetch_scalar` next to them. `_bootstrap.py:280` swaps the current `_MockPgPool` for a bridge-backed `_PgPool` whose methods forward to those capsules.

**Concurrency**: because step (3) returns a `&'static sqlx::PgPool` and step (4) releases the calling GIL, N sub-interp workers can have N queries in flight on the same pool. Parallelism ceiling moves from 1 (GIL) to `min(cores, max_connections)`. On Arena's 64-core box with `max_connections=256`, that's 64.

**Serialization cost**: step (5) is the new overhead vs. a native in-interp pyclass. `items` rows are small (~200 B each × 1000 rows = 200 KB). Encoding to a contiguous Rust buffer then decoding once on the sub-interp side is ~5–10 µs for this size — cheap vs. the ~1 ms typical PG round-trip.

**Rollback risk**: the new C-FFI entry points are self-contained. If an edge case in the bridge breaks, revert the injection and re-enable the mock; the `gil=True` path is untouched.

#### Design B — make `pyronova.engine` sub-interp loadable

PyO3 0.28 supports the `Py_mod_multiple_interpreters` slot via `#[pymodule(gil_used = false)]` or per-module attribute (check PyO3 release notes — spelling has shifted across 0.25–0.28). If we can truthfully declare it, the real `engine` module loads in sub-interps, `_mock_engine` disappears, every pyclass (PgPool, SharedState, Stream, WebSocket) works natively.

**What blocks this today**: not a one-flag change. Every `#[pyclass]` in `src/` has to be audited for sub-interp safety. Any use of `Py<T>` that crosses interpreters (e.g. a Response constructed in the main interp and moved to a sub-interp) is an immediate soundness violation and a segfault risk. `state::SharedState` uses `Arc<DashMap<String, Py<PyAny>>>` — those `Py<PyAny>` handles are interp-tied. The current `_mock_engine` exists partly because dropping it would require fixing all of this first.

Design B is the *correct long-term* answer. Design A is the *shippable this week* answer that unblocks async-db without restructuring the whole extension.

### Recommendation: ship Design A now, file Design B as a v2.3+ track

Design A is additive and contained: new C-FFI functions, new bridge-backed mock PgPool, drop `gil=True` in arena app. No change to existing pyclasses, no audit of Py<T> interp affinity. Expected async-db: 3.7k → 30k+ rps on 64 cores.

Design B unlocks more — real PgCursor streaming across sub-interps, native SharedState, etc. — but needs a dedicated pyclass audit PR before it's safe.

### Code outline for Design A

```rust
// src/db_bridge.rs (new)
#[no_mangle]
pub extern "C" fn _pyronova_db_fetch_all(
    py_state: *mut ffi::PyThreadState,
    sql_ptr: *const c_char, sql_len: usize,
    params_capsule: *mut ffi::PyObject,  // Py<PyList> of already-validated BoundParam payloads
) -> *mut ffi::PyObject {
    // 1. Parse params (sub-interp GIL held)
    // 2. py.detach() → Rust runtime
    // 3. pool_ref().fetch_all(bound_query)
    // 4. Encode rows into a side buffer
    // 5. Reattach, decode into PyList<PyDict> in the sub-interp
    // 6. Return
}
```

```python
# python/pyronova/_bootstrap.py (new mock — bridge-backed)
class _PgPool:
    @classmethod
    def connect(cls, *a, **kw): return cls()  # noop on sub-interp side
    def fetch_all(self, sql, *params):
        return _pyronova_db_fetch_all(_worker_id, sql, params)
    # ... fetch_one / fetch_scalar / execute follow the same shape
```

```python
# arena_submission/app.py — the payoff
@app.get("/async-db")                      # gil=True removed
def async_db(req: "Request"):
    rows = PG_POOL.fetch_all(_DB_SQL, int(req.query_params.get("limit", 10)))
    return {"items": [dict(r) for r in rows], "count": len(rows)}
```

### Scoping

- ~250 lines Rust for the bridge (new `src/db_bridge.rs`).
- ~40 lines Python for the sub-interp mock replacement.
- ~1 line change to `arena_submission/app.py` per DB route.
- Tests: port `test_db_pg.py` to run a subset through sub-interp workers (requires a Postgres fixture in CI, which is already present for the `arena_submission` validation job).

### Why not keep `gil=True` and parallelize inside the main interp?

Exhausted:
- Off-thread via `spawn_blocking` → one GIL holder still serializes the Python return path.
- `async def` handler → main interp is still one event loop, one GIL.
- No path breaks the single-GIL ceiling without sub-interp fan-out.

---

## Part 2 — static: hoist into the fast-response lookup

### Current state

`src/handlers.rs:340-350` checks `try_static_file()` **after** the full request entry sequence:

1. Fast-response hashmap lookup (miss) — 2 String allocs + hash.
2. `Arc::from(path)` + `Arc::from(method)`.
3. `raw_headers.clone()` (HeaderMap clone — ~100 ns on 5-header request).
4. `req.into_body()` ownership takedown.
5. `routes.lookup(method, path)` — full radix-router walk over all registered routes.
6. Branch `None && GET/HEAD` → `try_static_file()`.
7. `try_static_file` itself: `DashMap` read (hot path) or `tokio::fs::read` (cold).
8. `full_body(resp)` + `apply_cors`.

That's roughly **7–9 µs** per request even on a cache hit. `add_fast_response` paths (used for `/pipeline`) skip all of 2–6 and cost ~0.4 µs.

Observed 91k rps × 11 µs/req matches the path-length estimate.

### Fix

Move the static lookup into the fast-response branch, before the body-ownership dance.

At `app.static("/static", "/data/static")` registration time, **walk the directory tree once and populate the fast-response map** with `("GET", full_path) → (Bytes, content_type)`. Same for `("HEAD", full_path)`.

```rust
// src/app.rs, in register_static_dir:
for entry in walkdir::WalkDir::new(&root) {
    let path_on_disk = entry.path();
    let url_path = format!("{prefix}/{rel}");
    let bytes = std::fs::read(path_on_disk)?;
    let mime = crate::static_fs::mime_from_ext(&url_path);
    fast_responses.insert(
        ("GET".into(), url_path.clone()),
        FastResponse { body: Bytes::from(bytes), content_type: mime, status: 200 },
    );
}
```

Result: static hits take the **same** code path as `/pipeline`, landing in the `routes.fast_responses.get(...)` branch at `handlers.rs:311`. Per-request cost drops to the hashmap lookup + one `Bytes::clone` (Arc bump).

### Cache bound

Pre-loading the whole tree into memory is bounded by dir size. Arena's static fixture is ~20 files, ~200 KB — trivial. For real apps with 100s of MB of assets, we gate on a size limit (e.g. `STATIC_PRELOAD_MAX = 128 MiB`, reuse the `STATIC_CACHE_MAX_BYTES` constant) and fall back to the current `try_static_file` async path for files above the cap.

### Compatibility

- 404 for missing files still works — cache miss in fast-response map → falls through to `try_static_file` at line 341 → async fs check → `None` → 404.
- Directory changes at runtime: current impl is preload-at-startup; runtime-modified files won't reflect. Arena doesn't mutate static; for development, gate the preload behind `release` feature or a config flag.
- `Content-Length` and `Content-Type` come from `FastResponse.body.len()` and `content_type` in `build_fast_response()` — already wired.
- Compression: `app.enable_compression()` path runs post-fast-response in the current branch. Needs preserving — either skip compression for fast-response static (acceptable; static files are cached at network edge anyway in real deployments) or integrate a `build_fast_response_compressed` variant.

### Risk + rollback

- If `walkdir` pulls in non-trivial dep weight: use `std::fs::read_dir` recursion instead.
- If some static route needs dynamic headers (ETag, Cache-Control with mtime): defer those routes to the slow path. Mark them at registration.
- Arena doesn't care about ETag/Cache-Control — the benchmark is throughput of raw content delivery.
- Rollback: revert the registration change; `try_static_file` is untouched and remains the slow-path fallback.

### Expected gain

From ~11 µs/req to ~0.5–1 µs/req on cache hit. Given network + kernel + syscall floor of ~300 ns/req at peak, 300–500k rps is realistic. Still below `/pipeline`'s 2.6M rps (which has a constant body; no Bytes clone amortization needed), but the right order of magnitude.

---

## Sequencing

Ship in two separate PRs against `main`:

1. **PR A** (this doc's Part 1): `db.rs` idempotent connect + `_bootstrap.py` stop mocking. Add a sub-interp PgPool smoke test. Drop `gil=True` in `arena_submission/app.py` for `/async-db` and `/crud/*`.
2. **PR B** (Part 2): static preload in `app.rs::static_dir` registration. Extend `handlers.rs:311` fast-response branch to skip the body/header dance (already does).

Bump to v2.2.0 (first minor release that changes sub-interp DB semantics — callers with custom mock-dependent tests break).
