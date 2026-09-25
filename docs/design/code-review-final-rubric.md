# Final rubric review at b114cba

Read-only review of `src/` and `python/pyronova/` (plus `tests/` for test quality)
against every `status: stable` rule in the arc rubrics that apply to Rust/Python:
`rust_best_practices`, `security_common`, `l4_contracts`, `l5_architecture`,
`structural`, `fp_lens`, `llm_generated_code_smells`, `python_best_practices`,
`comment_hygiene`, `test_quality` (~120 rules; every rule reported a verdict).
Items already on the roadmap (M7, M9) or the reconciliation (§1–§4, R-b, §7) are not
repeated here. Nothing was built or run; b114cba includes R-a, whose tests have not
been run yet.

## Blocking

| # | Rule | Finding | Evidence | Fix |
|---|---|---|---|---|
| F1 ✅ | validate-external-input | `/mcp` and `@app.rpc` accept cross-site POSTs (CSRF): body parsed as JSON whatever the content type, `Origin` never checked; a `text/plain` simple request from any web page runs a tool. The MCP HTTP transport spec requires Origin validation. | `mcp.py:450`, `app.py:1063-1069`, `rpc.py:150-157` | Require `application/json` (415 otherwise) and validate `Origin` (403). **Decision G3.** |
| F2 ✅ | correctness | `req.json()` silently turns integers ≥ 2^64 into floats (serde_json without arbitrary precision, then pythonize); floats not guaranteed to round-trip. Responses use isojson — two JSON codecs. | `types.rs:214-219`, `Cargo.toml:53` | Decode with isojson; bigint + float round-trip tests. |
| F3 | doc-vs-code | Hooks (`before_request`/`after_request`/`fallback`) registered after the seal (e.g. inside `on_startup`, or `enable_cors()` there) are accepted but only run on `gil=True` routes — worker routes silently skip auth. | `app.rs:384-400` vs `:1029-1035` | Same typed "sealed" error routes get. |
| F4 | partial-function | Streamed-body feeder's final `Eof`/`Err` sends are unbounded: a `stream=True` handler that stops reading hangs its thread forever, no 504. Regression from R-a. | `handlers.rs:229,255`; `pipeline.rs:431-441` | One bounded send helper under `REQUEST_BUDGET`; test. |
| F5 | doc-vs-code | `@cached_json` on a handler with path params registers fine, then every request is a 500 (signature inspection follows `__wrapped__`). | `app.py:1222-1227`, `cache.py:142-143` | Inspect the callable itself; pass params through or reject at registration. |
| F6 | doc-vs-code | Isolation re-clone after a lib upgrade crashes for single-file extensions (`rmtree` on a file) — every worker start fails until the isolate dir is deleted by hand. | `_bootstrap.py:341` | `os.remove` for files/symlinks. |
| F7 | doc-vs-code | Bounded sync readiness checks (R-a) use a `concurrent.futures` pool: a hung check blocks interpreter exit; a new thread per probe. | `health.py:63-76` | Daemon thread, one in-flight run per check; one shared "drive with timeout" helper (also `mcp.py:51-80`). |
| F8 | comment-contradicts-code | 13 false comments that would mislead the next edit: admission permit flow (`pool.rs:233-239,426-431`), bridge "500" (it is 503; also the operator log at `main_bridge.rs:179`), pool-id counter (`ffi.rs:40-47`), `python/mod.rs`/`interp.rs` module docs, streaming slot reason (`types.rs:40,64,90`), `websocket.rs:602`, `db.rs:753`, worker handler lookup (`app.py:486`, `rpc.py:236`), `TestResponse` headers (`testing.py:73`), `readiness_check` contract (`app.py:769`, `health.py:126`), `state.rs:13-17`. | as cited | Correct or delete each. |

## Advisory (grouped)

**Correctness / robustness**
- `workers=0` / `io_workers=0` accepted by the raw engine → TPC binds nothing yet prints "Listening", fanout panics, tokio panics. `NonZeroUsize` at the boundary. (`app.rs:436,487`; `tpc.rs:295-328`)
- R4 residual: only native coroutines get the request context; Cython/mypyc `async def` or custom awaitables still lose `ctx` writes. (`request_context.rs:65-71`)
- `stream=True` without `gil=True` silently ignored → `req.stream is None` → 500. Registration error. (`app.rs:1036-1041`)
- `body_stream` state collapses EOF / rejected / consumed into `None`: a second read after `BodyRejected` is a clean StopIteration; `drain_count()` 0 is ambiguous. `enum StreamState`. (`body_stream.rs:184-276`)
- WebSocket handshake checks only `Upgrade: websocket` (not method/Connection/version/HTTP1.1): any client makes it run hooks, spawn a thread, log ERROR. Typed `WsHandshake` → 400/426 first. (`websocket.rs:381-386`)
- MCP: unhashable/non-str `method`/`name`/`uri` escape the try → 500 with traceback; prompts still send `str()` results. (`mcp.py:472-605`)
- `SO_REUSEPORT` on every listener: a second server on the same port binds silently and splits traffic, breaking M5's "port in use is an OSError". **Decision G4.** (`server/listener.rs:80-96`)
- Static-file cache never invalidated → edited files served stale until restart. **Decision G5.** (`static_fs.rs:285-292`)
- Compression settings are process-global (the class M5 fixed for `max_body_size`); out-of-range levels clamped silently; `enable_compression(gzip=False, brotli=False)` "enables" nothing; brotli q11 on a big body runs on a tokio worker. **Decision G1.** (`compression.rs:99-121,308`)
- Streamed-body budget is per frame, not per body: 4 drip-feeding clients hold every default bridge worker. **Decision G2.** (`handlers.rs:201-203`)
- `FORGOTTEN_WORKERS` is process-global: with two servers, one's abandoned worker makes the other `os._exit(1)`. Return it to the owning `run()`. (`pool.rs:219-224,307`)
- RSS sampler: `Once` latched across runs, not stopped by `shutdown()`, stale `rss_bytes` after stop. (`app.rs:464-469,711-715`; `monitor.rs:170-184`)
- `WorkerStartError` flattened to `RuntimeError(text)` at the edge: a worker-only `ImportError` can't be caught as one; the `ScriptImport` hint blames relative imports for any ImportError. (`app.rs:1108,1240`; `worker.rs:82-97`)
- `DbError::Connect` has no `.sqlstate` (bad password vs refused connection indistinguishable). (`db/error.rs:80`)
- Non-`str` `sys.path` entry fails worker start with an unrelated TypeError. (`WorkerProgram::read`)
- `crud.py:216` maps any exception from the user's `id_type` to 400; `rpc.py:93` returns the envelope as the result when `result` is missing.
- `run_context.rs:185` `assert!` on null tstate outside `catch_panic`; `handlers.rs:136-140` `debug_assert_eq!` guarding the loop/interpreter invariant; `types.rs:176` lock `.unwrap()` across a pymethod.
- `websocket.rs:684-701` handler errors logged without the request tag.
- HEAD on a GET route → 404; wrong method → 404 not 405; `run(host="localhost")` / bare IPv6 rejected without the host in the error.

**Architecture** (l5 / structural)
- Shared error/body types live in the top `handlers` layer → cycles (`python/*` → `handlers::error`, `response` ↔ `handlers::error`, `grpc` ↔ `pipeline`). Move `PyException`, `HandlerError`, `Stage`, `catch_panic`, `panic_message`, `BodyReject`, `REQUEST_BUDGET` to bottom-layer `crate::error` / `crate::body`; `stream_body_feeder` next to `read_body`.
- Worker layer imports `app.rs` (`WORKER_APP`, `WorkerRoutes`) → move to `router.rs`/`python/worker_app.rs`.
- `config` ↔ `tpc` cycle via `GcMode` → move GC policy into `config.rs`; `tpc.rs` (1030 lines) also carries CPU pinning/core counting → `server/cpu.rs`.
- Server topology re-derived at 4 match sites; `GcServer` a partial second enum; mismatch caught at runtime → one `enum Topology` resolved once.
- Hook → handler → hook chain implemented 3× (main, sub-interp FFI, async engine) — fold the two sync ones after M7; `panic_message` 3 copies with different text; Python→Rust log handler written twice; log level parsed twice (Python map is dead).
- `types.rs` (758 lines) holds request + response + headers → split. Hand-written `Display` for `GcModeError`/`SplitError` next to `thiserror`. Unused deps `serde`, `itoa`, `ryu`.
- Boolean-trap / positional params: `configure_compression(True, …, gzip, brotli)`, `register_route(…, gil, false)`, `worker_api` `(usize, u64, u64, …)`, logging `&str ×3`; anonymous tuples from `_worker_recv`, bench results. `enable_logging(level: str)` closed set as string.
- Hot path: query map cloned on every access (`types.rs:251-273`); `SharedState` values copied twice; SSE events allocate twice.

**Comments** — ~25 more comment-contradicts-code (list in the review transcript: `app.py:146,267,419,864`, `engine.pyi:261-264` stubs lack `stream`, `handlers.rs:204`, `monitor.rs:46,73,138,235`, `leak_detect.rs:22,106`, `router.rs:364`, `compression.rs:7,167`, `types.rs:59,207,263`, …); ~100 internal-vocabulary sites (arc finding tags, "D4", "FR-n", milestone names — **"(v1 limitation)" appears in user-facing error text at `app.rs:1017,1022`**, "v1.5" in a log at `ffi.rs:287`); 7 dead-identifier history notes; the pool-id zombie guard explained 7×; what-narration; missing `engine.pyi` summaries. All go to M9, the false ones first.

**Tests** — no blocking: every traced fix has a test that fails on revert, on the path it names; CI runs the fault-injection and Postgres tests. Advisory:
- R1 panic tests don't prove a Rust panic (a Python exception gives the same 500) — assert the panic text; add an async-engine panic case.
- "Logged once" checks stop at the first match while the writer is non-blocking — count after a marker.
- R4 test doesn't force thread reuse (`workers=1`, one bridge worker).
- Shared-cursor test never checks the waiter; M5 follow-up never checks it ran in a worker; per-app WS cap test checks only the permissive side; one test asserts private state; `grpc.rs` deadline test is tautological; M4 tests depend on run order.
- Fixed sleeps to wait for work / before absence checks (11 sites) — poll with a deadline (`Server.log_lines_with` as the shared helper).
- Happy-path-only: multipart parser, NUMERIC codec, route-template parser, RFC 8187 `filename*`, MCP validator, static traversal → property tests.
- Darwin-only fanout guard never runs in CI (Ubuntu only) → macOS CI job.
- Listener bind test depends on host network (`192.0.2.1`).

## Decisions for the human

- **G1** Compression settings: per app in `SiteConfig` (like M5's limits) vs keep process-wide (FR-17 made it process-wide on purpose).
- **G2** Streamed-body budget: a total deadline per body vs per-frame (document the exposure).
- **G3** `/mcp` + RPC CSRF: require `application/json` + an `Origin` allow-list (default: requests without `Origin`, and same-host origins) vs content-type only.
- **G4** Port-in-use with `SO_REUSEPORT`: probe-bind without reuseport first (then the reuseport set) vs drop the promise.
- **G5** Static-file cache: revalidate on `(mtime, len)` per request vs make caching opt-in.

## Plan

One fix wave, then a single full-suite run (human decision): these findings + R-b (Q1, R2, R5, reconcile §2/§4 leftovers) + M7 + M9 + §7 (no fixed test ports) + the macOS `cargo test` link failure after R-a. Grouped by file ownership so parallel implementers do not collide; implementers run only targeted tests, serialized; the supervisor runs the full suite once at the end.
