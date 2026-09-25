# Reconciliation at 34ecc33 (M1–M4, M6, M8 merged; M5 in flight)

Five read-only reviewers re-checked every ledger item in
`code-review-cced8c2-roadmap.md` against the code and tests at 34ecc33, using the
same rubric (arc-reasoning / shift-left-rust / shift-left-python / coding-principles).
An item is FIXED only with code evidence **and** a test that fails if the fix is
reverted. Items inside M5 / M7 / M9 scope were confirmed still open and are not
repeated here. Supervisor re-verification is marked ✅.

## 1. New blocking findings

| # | Finding | Evidence | Root fix |
|---|---|---|---|
| R1 ✅ | A panic in a gil=True bridge thread kills that thread for good; after `workers` (4) such panics every gil=True route answers 503 "server shutting down" while the server is up. | `bridge/main_bridge.rs:138-147,293` — no `catch_panic` around `dispatch_one` | Run `dispatch_one` through the M4 `catch_panic` + log with the tag, like pool/inline; integration test on the bridge. |
| R2 ✅ | WS connection cap stops counting a live thread after a `before_request` timeout: the slot drops when the handler returns 504 while the hook thread keeps running → unbounded threads (the hole M1d#1 was meant to close). | `websocket.rs:503,531-534` | Move the slot into the task that joins the thread (timeout and Reject paths); test with a hook sleeping past the budget. |
| R3 ✅ | `:id` route syntax: matchit 0.8 treats it as a literal segment. `/items/:item_id` registers, `/items/42` is 404, and path-param injection passes `None` (rule 3). `{*rest}` injection raises at registration. Observed live. | `app.py:1278-1299`, `Cargo.toml:41`; `tests/test_path_param_injection.py:130` only checks registration | Parse only `{name}` / `{*name}`; `:name` is a registration error; index params `p[n]`. **Decision Q2.** |
| R4 | ContextVar writes from an `async def` hook/handler are lost on every sync path: `run_until_complete(coro)` runs a Task in a *copy* of the request context. `async before_request` doing `ctx.set("user")` → sync handler reads `None` on TPC (default), pool sync, GIL. Async engine is correct (one Task for the chain). | `python/worker.rs:577`, `handlers.rs:184`, `request_context.rs:24`; `test_review_m2.py:370-406` uses sync hooks only | Run the awaitable with `create_task(coro, context=request_ctx)` (Enter only around sync calls), or drive the whole hook chain as one coroutine; test async-hook-writes / sync-handler-reads on every path. |
| R5 | Nested TestClients on one app: the engine has one `serving` slot; the inner `close()` clears it, the outer `close()` is a no-op → 60 s hang, RuntimeError, leaked server. Same if `close()` lands during the retry backoff. | `src/app.rs:622,652`, `testing.py:246` | Per-server stop handle returned by `_start` (fits M5's bound-port work), or a typed error on a second concurrent run. |
| R6 ✅ | **The premise of the TPC-inline-timeout decision was wrong.** On default TPC, `async def` routes also run inline and block the core (`app.rs:578`: "inline, blocking"), so "move slow handlers to `async def`" — written into `tpc-rearch.md` Line 3 and the 504 log hint — does not give an on-time timeout, and async handlers get no concurrency on TPC. | `handlers/tpc.rs:43` dispatches `Call::Worker(route, _)` inline regardless of `HandlerKind` | **Decision Q1.** |

## 2. Fake-done / partial (claimed fixed, not proven or not complete)

| Item | Status | Evidence |
|---|---|---|
| M2 "TPC stays zero-alloc by borrowing from `Parts`" | PARTIAL | `handlers/tpc.rs:113-121` allocates 2× `Arc<str>` + `Arc<Mutex<None>>` per inline request; `pipeline.rs:105-106` comment is now false. Root: `PyronovaRequest` is hand-built in 4 places (`gil.rs:106`, `handlers/tpc.rs:112`, `main_bridge.rs:279`, `websocket.rs:464`). |
| M4 panic payload logged on every path | claim-without-test | Only `catch_panic` is unit-tested (`error.rs:348,359`); removing it from `serve()` stays green. Bridge has no `catch_panic` at all (R1). Needs a gated fault-injection route. |
| M4 "logged exactly once" | PARTIAL | Inline timeout logs 2 ERROR lines (3 if the handler also raised): `handlers/tpc.rs:143,147,154`. |
| M4 no `Result<_, String>` at boundaries | silent-scope-drop | `ChunkMsg::Err(String)` (`handlers.rs:260-281`, `body_stream.rs:47,205`): an oversized/timed-out **streamed** body is a generic 500 + ERROR traceback, a buffered one is 413/408. |
| M4 isojson required in workers | claim-without-test | Test hides isojson in main under `mode="gil"` (`test_review_m4.py:546-560`) → never reaches `worker.rs:371`. |
| M4 async-engine errors keep their traceback | PARTIAL | No test; engine death at runtime is typed `WorkerStartError::Script` → logs "raised while the worker started" (`worker.rs:72`). |
| M2 CORS parsed once at config time | claim-without-test | No test feeds a bad CORS value. |
| M1d#1 WS cap | PARTIAL | Tested only on TPC-GIL; pool path has no WS test at all. Plus R2. |
| M6 bench Python tests | never run in CI | `ci.yml` maturin builds lack `--features bench`. |
| M1a-8 MCP invalid params → -32602 | PARTIAL | Only names are checked; a wrong value type → -32603 (`mcp.py:184-190,438-441`). |
| M1a-10 MCP schema silently "string" | PARTIAL | Only `NameError` fixed; any unmapped hint (`list[int]`, `int \| None`, `Literal`) still becomes "string" (`mcp.py:130`). |
| Logging single writer (3cba81e) | PARTIAL | `enable_logging(level=…)` silently ignored when the level is already non-ERROR (`app.py:860`); stale "idempotent + lock-guarded" comment (`app.py:1023`). The GIL logging test lost its path/status checks — **restored in 17ad21c**. |
| Test vacuity | — | `test_review_m1a.py:429-432` asserts only "no ERROR record" with nothing proving capture works. |
| Pool worker spawn failure | scope-drop | `pool.rs:436-444` leaks a live interpreter (abort at finalize); M5 lists only the `tpc.rs` copy. |

## 3. Confirmed FIXED (code + test)

M1a 1–7, 9 and most of 10; M1b 1–5; M1c 1–6 (DB tests run against Postgres in CI); M1d 3–7; M2 fallback on TPC, body timeout, handler timeout + dropped counter, one `finish()` used by every path, typed route table, config split, infallible `build_response`, invalid status → 500, fast response parsed at registration, per-request context (sync hooks), D3; M3 all items; M4 typed errors in pool/ffi/router/state/tls/listener, PyStatus text, client_ip sentinel, `BridgeResponse`, D4 policy (rpc/health/mcp/crud, real subprocess on gil/tpc/pool); M6 gRPC opt-in on every path, bench feature, `build_workers` + `end_all`; M8 default production path, prepare/serve split, `close()` (single client). The M8 test migration was verified faithful (assertions identical). Static-file traversal, NUMERIC codec, logging reload and `PgCursor` hand-off were reviewed and found clean.

## 4. Advisory (to fold into the milestones that own the files)

Core: route-count banner double-counts async (`app.rs:1182`); `RouteShape` parallel `Vec<bool>` reintroduced (`router.rs:429`, `pool.rs:177`); two sources for the effective content type (`response.rs:241`, `compression.rs:380`); `Response.status_code` validated late unlike `FastResponse`; CORS overrides a handler's own `Access-Control-Allow-Origin` (`site.rs:93-98`); `expect` invariant for `Target::Fallback` (`router.rs:370`); method-case normalization differs between route and fast-response lookup (`router.rs:397-408`); `request_id.rs:39,74` overflow / empty-string sentinel; `configure_compression` builds the value twice; stop requested before `run()` stores its token is lost (`app.rs:622`).
Pipeline: bridge thread-spawn failure is a 503 "shutting down", not an error; access-log sampling counter is one shared atomic; `into_response` has a hidden counter effect; `AcceptEncoding::as_str` `""` sentinel; `try_dispatch` returns an item nobody uses.
FFI/workers: `worker_thread_loop_async` lacks `SubInterpGilGuard`/`catch_panic` (`pool.rs:569-579`); async path stringifies `ResponseError` (`worker_api.rs:187,191`); orphan sweep scans the map under the mutex past 64 in flight; global `WORKER_STATES` breaks a second pool-mode server in one process; async workers build an unused loop + handler vector.
DB/WS/gRPC: `db.rs:6` doc contradicts the connect decision; NUMERIC digit ≥ 10000 and dscale > 0x3FFF not rejected; big ints can't reach NUMERIC; float4 narrowing to `inf`; gRPC body read has no budget and skips `count_request`; `unframe` accepts trailing bytes; any request with `Upgrade: websocket` on a normal route is 404; duplicate Pong; limit setters are read-modify-write across two locks.
Python: `os._exit(1)` reachable from a TestClient thread; `_prepare` unlocked and once-per-app (BLAS/`/mcp` fixed by the first server's settings); MCP list results sent as Python repr; crud maps any TypeError/ValueError to 422; `model=` guesses whether the handler takes `req`; `testing.py:361` / `observability.py:138` swallow into sentinels; sync readiness checks unbounded; uploads `filename*=` ignored, `filename` unsanitized and undocumented; `_defined_in` misses package-relative imports.

## 5. Plan

- **Q1, Q2** go to the human (below).
- **R-milestone** (after M5 merges, before M7): R1–R5, every §2 row, and the §4 rows outside M7/M9 scope. Same rules: repro test first, no existing-test edits without approval.
- §4 FFI rows join M7; hygiene rows join M9.
