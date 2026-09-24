# Optimize async-db

> **Update (2026-09, Layer 2, `docs/design/real-engine-in-workers.md`):** the C-FFI DB bridge referenced below (`src/bridge/db_bridge.rs`, `_PgPool` in `_bootstrap.py`) is gone; workers use the real `PgPool`, whose sync methods run on the DB runtime via `run_on_db_rt` (`src/db.rs`). Async `*_async` methods are main-interpreter only and use `await_on_loop` (no `pyo3-async-runtimes`).


Plan to lift the `/async-db` path past the post-bridge ceiling.

## Current state

Score: **Async DB 13.3 (27,358 req/s)**.

`/async-db` in `arena_submission/app.py:196` is no longer `gil=True` —
Design A from `arena-async-db-and-static.md` already shipped:

- `_bootstrap.py:407–471` installs a bridge-backed `_PgPool` whose methods
  call C-FFI capsules (`_pyronova_db_fetch_all`, `_pyronova_db_fetch_one`,
  `_pyronova_db_fetch_scalar`, `_pyronova_db_execute`).
- Bridge implementation in `src/bridge/db_bridge.rs::run_on_db_rt` does
  `rt.spawn + channel.recv` against a dedicated DB tokio runtime.
- Parallelism ceiling is `min(sub_interp_workers, DATABASE_MAX_CONN)`.

So this score is **not the GIL bottleneck** that Design A was meant to fix —
it's the new ceiling Design A landed at.

## Where the time goes

27,358 req/s ÷ ~64 sub-interp workers ≈ **427 req/s/worker ≈ 2.3 ms/req**.

Rough budget per request:

| Stage | Estimate |
|---|---|
| PG round-trip (incl. pool acquire, plan) | 0.8–1.5 ms |
| Row decode Rust→PyDict (×50 rows) | 100–200 µs |
| `_rows_to_payload` Python loop + `json.loads(tags)` | 100–300 µs |
| Response JSON serialize (`py_to_json_value` + `sonic_rs`) | 100–200 µs |
| Bridge crossing (param serialize, channel send/recv) | 30–80 µs |
| Sub-interp ↔ DB runtime hop | ~30 µs |

## The structural ceiling

The DB bridge is **sync-blocking**: `run_on_db_rt = rt.spawn + channel.recv`.
The sub-interp worker is parked for the full PG round-trip (GIL is detached,
but the worker thread is occupied — it can't service another request).

That means **one query in flight per worker**. Per-worker throughput is
pinned to `1 / RTT`. Doubling throughput requires either more workers or a
shorter RTT — code optimization alone can't break it.

## Optimization roadmap (ranked by ROI)

### 1. Response JSON serialization (overlaps with `optimize-json.md`)

Same change as `optimize-json.md` #1: replace
`py_to_json_value → serde_json::Value → sonic_rs::to_vec` with a direct
Python → bytes writer.

`/async-db` returns ~50-row payload — mid-size JSON, exactly the case where
the intermediate `serde_json::Value` tree wastes the most. Per-request save:
**100–200 µs ≈ +5–10% throughput**.

Free if `optimize-json.md` lands first.

### 2. Bulk row decode (Rust → Python)

Today: each row is reconstructed in the sub-interp as a `PyDict` with 9
columns, one Python object per column. 50 rows × ~10 PyObject allocations =
500 allocations and 50 dict creations per request, all under the sub-interp
GIL.

Two paths:

#### 2a. One-shot list construction in Rust

Build the `PyList[PyDict]` entirely in Rust, returning a single
`*mut PyObject` across the bridge boundary instead of decoding row-by-row
on the Python side.

```rust
// src/bridge/db_bridge.rs
fn rows_to_pylist(py: Python, rows: &[PgRow]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for row in rows {
        let dict = PyDict::new(py);
        for col in row.columns() {
            dict.set_item(col.name(), decode_value(py, row, col)?)?;
        }
        list.append(dict)?;
    }
    Ok(list.unbind())
}
```

Save: ~100 µs per request. **+5–8%.**

#### 2b. Skip Python intermediate altogether

If the handler's job is `rows → JSON response`, push the JSON encoding into
the bridge:

```python
# python/pyronova/db.py — new method
rows_json: bytes = PG_POOL.fetch_all_json(sql, *params)  # Rust serializes directly
return Response.from_bytes(rows_json, content_type="application/json")
```

Eliminates the row-by-row PyDict materialization AND the response JSON
serialization in one shot. Per-request save: 200–400 µs.

**+15–25%.** Bigger refactor — a new bridge entry point and a Python-side
escape hatch for handlers that just forward DB rows. Pairs naturally with
the JSON writer from `optimize-json.md` #1.

### 3. Async handler + concurrent query (the real win)

Today's `def async_db_endpoint(req)` is sync. The bridge blocks the worker
for the full RTT.

If the handler were `async def` and the bridge exposed an awaitable
(`await PG_POOL.fetch_all_async(...)`), one sub-interp worker could:

- Issue query A, await
- Service request B, issue its query, await
- Resume A when its row arrives, return response
- ...

Per-worker throughput becomes `concurrent_queries / RTT` instead of
`1 / RTT`.

This needs phase-7.2's async bridge (`docs/phase-7.2-async-bridge.md`)
extended to cover DB operations — the existing `_pyronova_recv` / `_send`
async pattern, but for sqlx futures. Sub-interp's event loop registers a
waker, the DB tokio runtime resolves the future, the waker re-injects into
the sub-interp's loop.

Scope: medium. The async bridge already exists; routing DB futures through
it is mostly plumbing. The hard part is testing — concurrent in-flight
queries on one worker exercise paths today's sync bridge never hits.

**+50–100%** potential. This is the path to push the score to 25+.

### 4. Prepared statement reuse

`sqlx::PgPool::fetch` re-prepares per call by default. Cache prepared
statements per pool, keyed by SQL hash:

```rust
// pseudo
static STMT_CACHE: Lazy<DashMap<u64, Arc<PgStatement>>> = ...;
let stmt = STMT_CACHE.entry(hash(sql)).or_insert_with(|| pool.prepare(sql));
stmt.bind(params).fetch_all(pool).await
```

Saves PG-side parse + plan (~30–80 µs/query depending on query complexity).

**+5–10%.** Cheap, contained, do it before #3 — easier to measure in
isolation while the bridge is still synchronous.

### 5. Out of scope: PG-side tuning

`pg_prewarm` for the `items` table, BTREE on `price`, `work_mem` tuning,
checking `pg_stat_statements` for plan churn. These belong in the
benchmark harness setup, not the framework. Worth noting if reproducing
the score on a fresh box gives wildly different numbers.

## Suggested execution order

1. **Land `optimize-json.md` #1 first.** #1 here becomes free, and the
   bytes-out variant in #2b is much easier to wire once the writer exists.
2. **Ship #2a + #4 in one PR.** Both are bridge-internal, both safe, both
   measurable. Expected: 13.3 → ~16 (+15–20%).
3. **Evaluate #2b.** If the row-shape is stable across `/async-db` and
   `/crud/*`, the `fetch_all_json` shortcut is worth it. If handlers do
   non-trivial post-processing, it's not.
4. **#3 async bridge** is the real ceiling-breaker. Tracks with phase-7.2;
   needs its own doc and PR sequence. Defer until #1–#4 are measured.

## Validation

- `benchmarks/run_comparison.sh` — Pyronova vs FastAPI on async-db
- TechEmpower-style score: target 13.3 → 17 after PR-1, → 20 after PR-2
- Microbench: 50-row fetch latency p50/p99 with 1, 8, 64 concurrent clients
- `arena_submission` validation job — must keep passing

## Related docs

- `docs/arena-async-db-and-static.md` — Design A (already shipped)
- `docs/optimize-json.md` — JSON serializer rewrite (prerequisite for #1)
- `docs/phase-7.2-async-bridge.md` — async bridge that #3 builds on
