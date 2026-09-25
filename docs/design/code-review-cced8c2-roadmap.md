# Code review cced8c2 — fix roadmap

Source: 5-way review of `main@cced8c2` against arc-reasoning / shift-left-rust /
shift-left-python / coding-principles (2026-09-25). Every item below cites the
file:line where the reviewer found it; ✅ = supervisor re-read the code and confirmed.

Integration branch: `review/cced8c2-fixes`. One branch per milestone, merged into
the integration branch after the supervisor re-runs the verify commands.

## Ground rules (all milestones)

- **Root fix, not patch.** Each milestone names the structural root; fix the root so
  the bug class is unrepresentable. No compat shim, no "keep old + tolerate".
- **Repro test first.** Every bug item gets a NEW test that fails on cced8c2 and passes
  after the fix. New tests go in new files or new functions.
- **Never edit an existing test.** If an existing test goes red, stop and report: is it
  freezing correct behaviour (our bug) or the old wrong behaviour (test must change)?
  The human decides.
- **Hot path.** 400k req/s server — any change on the per-request path states its cost
  (allocations, extra branches). Pure code moves are free; say so.
- **Verify** (each milestone): `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test`, `maturin develop --release`, then the targeted pytest files named in the
  milestone plus every new test. The supervisor runs the full suite serially after merge
  (tests use fixed ports, so full suites cannot run in parallel).

## Sequencing

```
M1a ─┐
M1b ─┤ (parallel, disjoint files)
M1c ─┤
M1d ─┘
      └─> M2 ─> M3 ─> M4 ─> M5 ─> M6 ─> M7 ─> M8 ─> M9
```

M2–M7 overlap heavily in `handlers*`, `app.rs`, `tpc.rs`, `python/worker.rs`, so they run
serially. Decisions D1–D4 (bottom) gate parts of M1d, M4, M6, M8.

---

## M1a — Python package correctness (files: `python/pyronova/*` except `testing.py`)

| # | Item | Evidence |
|---|---|---|
| 1 | Multipart parser strips one extra trailing newline from every part → data corruption | ✅ `uploads.py:183-187` |
| 2 | `expires=` unusable: `,` is forbidden but every HTTP-date contains one. Take a `datetime`, format it; forbid only CR/LF/NUL there; `samesite` → Literal/Enum | ✅ `cookies.py:31,121` |
| 3 | `ctx` leaks between requests on the same thread; reset only when request-id is enabled; docstring references a non-existent `reset_context_on_request`. Root: each request's hooks+handler run in a fresh `contextvars.Context`; delete the opt-in reset | `context.py:15-26`, `observability.py:62` |
| 4 | `pyronova dev` broken: reloader re-execs bare `sys.argv[0]` and watches `venv/bin`. Reloader takes the real argv + app dir as parameters. Polling reloader quits forever on child crash (`while/else`) | `cli.py:99-108`, `app.py:1262-1281,1339-1356` |
| 5 | Isolation clone: `cp` errors discarded (check=False, stderr→DEVNULL) then misread as "lost the race". Raise with stderr; lost race only if dst exists | `_bootstrap.py:319-324` |
| 6 | Isolation clone dir `/tmp/pyronova-isolate` shared by all users → another user can plant a `.so`. Per-user dir, mode 0700, owner check | `_bootstrap.py:195,236` |
| 7 | `model=` disables path-param injection → TypeError/500 every request. Compose validate→inject; check signature at registration | `app.py:542-555` |
| 8 | MCP turns every validation error into `-32000 Internal error`; disagrees with app.py's -32603. Typed `JsonRpcError(code,msg)`: invalid params → -32602 with message; unexpected → -32603 | `mcp.py:315-324,356-420`, `app.py:1163` |
| 9 | `rpc.py`, `health.py`, `mcp.py` have three different policies for exposing exception text (rpc comment contradicts its code). One shared policy — **gated by D4** | `rpc.py:171-194`, `health.py:128` |
| 10 | Smaller: `except Exception: pass` in error extraction + double request logging (`app.py:989-1006`); dead `port is None` guard (`app.py:1090`); pydantic-v1 dead fallback (`app.py:488-493`); rpc/mcp routes bypass `_route` so missing from `app.routes` (`rpc.py:199`, `app.py:1170`); mcp schema silently "string" on hint failure (`mcp.py:96-104`); MCP registries → dataclasses (`mcp.py:141`); crud id-parse ×3, client bad JSON logged at ERROR, constraint violation → 500 (`crud.py:208,237,255-282`); uploads malformed → silent `{}` (`uploads.py:163`); `_async_engine.py` duplicate except arms (107-138); `isolate()` list via `os.environ` (`app.py:343-373`); cli comment wrong about bind addr (`cli.py:102`) | as cited |

Verify: `tests/test_uploads_e2e.py tests/test_cookies_unit.py tests/test_cookies_e2e.py tests/test_config_context.py tests/test_observability.py tests/test_cli.py tests/test_isolate.py tests/test_path_param_injection.py tests/test_mcp.py tests/test_rpc.py tests/test_health.py tests/test_crud.py` + new tests.

## M1b — Small Rust runtime bugs (files: `compression.rs`, `tpc.rs` GC section, `python/pool.rs` split, `app.rs` banner line only)

| # | Item | Evidence |
|---|---|---|
| 1 | `ct[..5]` byte-slices a str → panic on non-ASCII content-type, outside catch_unwind | ✅ `compression.rs:211` |
| 2 | Idle-mode GC counts accepts, not requests → never runs on keep-alive; OOM failsafe can't trip. Count at dispatch. Fanout silently downgrades Idle→Count: make `GcMode` a parsed value; unsupported mode = startup error | ✅ `tpc.rs:1023-1128`, `tpc.rs:811-818` |
| 3 | Failed `gc.collect()` leaves a pending exception on the sub-interp (next request starts with SystemError). Use `PyErr::take` | `tpc.rs:923-950` |
| 4 | n=1 + any async handler → 0 sync workers → every `def` route 503; banner uses a different formula. One pure `split_workers(n, has_sync, has_async) -> Result<WorkerSplit, _>` read by pool and banner; zero-worker pool that is needed = startup error | ✅ `python/pool.rs:256-260`, ✅ `app.rs:1111` |
| 5 | Compression settings as 5 separate atomics (torn reads) + hand-rolled bitmask; `gzip_compress`/`brotli_compress` return `bool` hiding the error | `compression.rs:55-60` |

Verify: `tests/test_compression.py tests/test_async*.py tests/test_hybrid.py` + new tests.

## M1c — DB (file: `db.rs`, `python/pyronova/db.py`)

| # | Item | Evidence |
|---|---|---|
| 1 | `None` bound as `None::<i64>` → "expression is of type bigint" on text columns. Bind untyped null / typed per declared param | ✅ `db.rs:192` |
| 2 | Unknown column types: binary wire bytes guessed as UTF-8 → same column returns garbage `str` on one row, `bytes` on another. Dispatch on closed `enum PgKind` from type OID; the `"INT"/"BIGINT"/"BOOLEAN"` name arms never match. Unknown kind → `bytes` always, never guessed text | ✅ `db.rs:225-289` |
| 3 | Stringly errors: `Result<_, &'static str>` (92,116, loses panic payload — use `JoinHandle::into_panic`), `CursorMsg::Err(String)` (331), `F: Result<T,String>` (844). `thiserror` `DbError` carrying sqlx error + SQLSTATE so Python can map unique-violation etc. | `db.rs` as cited |
| 4 | Second `connect` with a different DSN silently ignored (`let _ = PG_POOL.set`) → raise | `db.rs:442-460` |
| 5 | Cursor shared by two threads: second sees `rx` taken → premature StopIteration | `db.rs:380-387` |
| 6 | 8 methods repeat extract→detach→run_on_db_rt→map_err: one `run_query` helper | `db.rs:466-786` |

Verify (needs `PYRONOVA_TEST_PG_DSN=postgres://hucao@localhost:5432/pyronova_test`): `tests/test_db_*.py tests/test_crud.py tests/test_layer2_async_db.py` + new tests.

## M1d — WebSocket / static / logging / monitor (files: `websocket.rs`, `static_fs.rs`, `logging.rs`, `monitor.rs`, `grpc.rs` error text only)

| # | Item | Evidence |
|---|---|---|
| 1 | Unbounded OS thread per WS connection + tungstenite 64 MiB default message size + message-count (not byte) backpressure. Explicit `WebSocketConfig` limits, byte-based cap, bounded concurrency → 503 when full | ✅ `websocket.rs:232-287` |
| 2 | WS handler gets no request (headers/Origin/cookies) and skips `before_request` → cannot defend against cross-site WS hijack. **Gated by D3** | `websocket.rs:302`, `worker.rs:158` |
| 3 | `recv()`/`recv_bytes()` silently drop the other message type; `recv_message` returns `"text"/"binary"` string → typed; `let _ =` on handler-thread panic/close error | `websocket.rs:61,78,98,435,442` |
| 4 | static_fs: every IO error → silent 404 (incl. EACCES/ELOOP); mounts untyped `(String,String)` re-canonicalized per request → `StaticMount{prefix, root: CanonicalPath}` parsed once, bad root = registration error; path not percent-decoded (`my%20file` 404) | `static_fs.rs:70-183` |
| 5 | logging: dispatch on `levelname` string drops custom levels → map `levelno` by threshold; invalid level / failed subscriber install silently OK → raise; `format` string → enum | `logging.rs:47-194` |
| 6 | monitor: anonymous 9-tuple + getter that resets peaks (hidden write) → named `Metrics` pyclass + separate `reset_peaks()`; RSS `0` sentinel → `Option`; `50_000` magic | `monitor.rs:105,228,236,276` |
| 7 | grpc: `"body read failed"` replaces real error; bad status header falls back to `"0"` (OK); status `&str` consts → enum; unknown non-varint fields rejected (proto3 says skip) | `grpc.rs:43-47,78-87,142,181` |

Verify: `tests/test_websocket.py tests/test_static_files.py tests/test_logging.py tests/test_log_sampling.py tests/test_passive_gil_metrics.py tests/test_observability.py` + new tests.

---

## M2 — One request pipeline (root A + C)

Root: GIL / sub-interp / TPC each carry their own copy of preprocessing and response
finishing, and have drifted. Route table is six parallel `Vec`s plus `usize::MAX` as a
fallback marker.

- One `preprocess_request(&RouteTable, …)` used by all three paths (TPC keeps zero-alloc by borrowing from `Parts`). Fixes: ✅ TPC ignores `app.fallback()` (`handlers/tpc.rs:101-107`).
- One `collect_body(body, Limits) -> Result<Bytes, BodyReject>`; `const REQUEST_BUDGET` replaces the 7× `30s` literal. Fixes: TPC body read has no timeout (`handlers.rs:47-70`); GIL maps any read error to 413 vs sub-interp 500 (`gil.rs:83-97`, `subinterp.rs:169`).
- Handler timeout on every path per `docs/tpc-rearch.md:177-190` (`gil.rs:125`, `tpc.rs:313-347`, bridge).
- One `finish(resp, cors, log_ctx)` applying CORS + access log (~20 copies; `gil.rs:155`, `subinterp.rs:262,319`, `tpc.rs:270,349`).
- `RouteTable` → `Vec<Route { handler, name, key, dispatch: Dispatch }>` + `enum Target { Route(RouteId), Fallback }`; delete the out-of-range / stream-on-subinterp guards (`handlers.rs:576-595`, `tpc.rs:287-307`, `subinterp.rs:113-128`) and the unchecked `is_stream[idx]` (`handlers/tpc.rs:126`). Split config (`cors_config`, `request_log_*`) from the table.
- Remove unnecessary `unsafe impl Send/Sync for RouteTable` (`router.rs:281`).
- CORS parsed to `HeaderValue` once at config time (`handlers.rs:341-360,443-461`).
- Invalid status → 500 everywhere (fast path maps to 200: `handlers.rs:434`; `handlers.rs:401` without log).
- `DROPPED_REQUESTS` counted on every path (`tpc.rs:236` only).
- `build_response` infallible (`response.rs:166-196`); dead `Err` arms (`gil.rs:138-149`, `subinterp.rs:258`); dead `try_send` branch (`subinterp.rs:186-206`).
- `collect_body_with_admission` returns the permit in the enum, no `&mut Option` out-param (`handlers.rs:101-108`).

Verify: `tests/test_routing_e2e.py tests/test_cors_*.py tests/test_middleware_hooks.py tests/test_subinterp_*.py tests/test_fast_response.py tests/test_admission_control.py tests/test_tls_slowloris.py tests/test_hybrid.py` + new tests: fallback on TPC, TPC slow-body timeout, TPC handler timeout.

### Carried into M2 from M1

- **Per-request `contextvars.Context`** (M1a item 3): the dispatcher creates and enters a fresh context around before-hooks + handler + after-hooks on every sync path (GIL, sub-interp, TPC inline, bridge) — `PyContext_New` + `PyContext_Enter`/`Exit`; then delete the opt-in reset in `context.py` / `observability.py`. Async path already gets one per Task.
- **404 (and every other non-handler response) must write the access log line**: observed on `3cba81e`, `GET /nope` in GIL/TPC mode produces no `pyronova::access` line. The single `finish()` writes it for every response.
- **D3 WebSocket** (M1d item 2): `ws.request` + `before_request` before the 101, on top of the unified pipeline — do it in M2 once `preprocess_request` exists.

## M3 — One response mapping + real header multimap (root B + F)

- One Python-result → response mapping for GIL and sub-interp; `SubInterpResponse` merges into `ResponseData`. Fixes ✅ list returned from a sub-interp handler serialized as Python repr (`python/worker.rs:1019,1064` vs `response.rs:62`).
- Response headers as `Vec<(String,String)>` / `HeaderMap`, no `\0` packing (`types.rs:389`, `response.rs:186`); user headers override defaults instead of appending (⚠️ verify with a test first: `response.rs:179-188`); collapse `set_compression_headers{,_vec}` (`compression.rs:305-360`).
- Non-`str` header values raise `TypeError` naming the key instead of vanishing (`types.rs:380-392`).
- Request headers stop being flattened with `", "`; `cookie` joined with `"; "` per RFC 9113 §8.2.3 (⚠️ verify with an HTTP/2 test first: `types.rs:423-438`).
- Content-type sniffing single source; `{user} logged in` must not become JSON (`response.rs:75,95`).
- Sub-interp silent defaults: status fallback 200 + `as u16` truncation (`python/worker.rs:1138-1149`), empty body on error (1280,1287), dropped `Set-Cookie` on conversion failure (1191-1245).

Verify: `tests/test_json_serializ*.py tests/test_cookies_e2e.py tests/test_request_fields.py tests/test_subinterp_features.py tests/test_compression.py` + new tests (list on sub-interp, header override, non-str header value, h2 cookies).

## M4 — Typed errors, real text preserved (root D)

`thiserror` enums replace `Result<_, String>` at every boundary; the raw error is carried
to the log/response, never replaced by a fixed string.

- `HandlerError` (Python exception with context+text, Panic(payload), Overloaded, PoolClosed, Timeout…) logged once where it happens, rendered once at the edge (`handlers.rs:186,611-627,716-722`, bridge `main_bridge.rs`, `pool.rs:48,238,412`, `ffi.rs:21`).
- Placeholders replaced by the real error: `"invalid response headers"` (`response.rs:190`), `"Py_NewInterpreterFromConfig failed"` drops `PyStatus.err_msg` (`python/worker.rs:122`), `"failed to extract string"` (`convert.rs:156`), worker panic payload dropped (`pool.rs:495`), async engine errors reported as `run_until_complete error:` without traceback.
- `client_ip` unparseable → `0.0.0.0` sentinel (`types.rs:139-141`) → error or `Option<IpAddr>`.
- `BridgeResponse` duplicates `HandlerResult` (`main_bridge.rs:67-70,303-307`).
- tls/listener/router/state `Result<_, String>` (`tls.rs`, `listener.rs`, `router.rs:201`, `state.rs:26`); `state.rs` get/values/items vs `__getitem__` policy on non-UTF-8 values.
- Sub-interp setup failures cleared then surfaced as placeholder (`python/worker.rs:223-273,244,499`); `isojson` ImportError silently falls back to `json` (`python/worker.rs:895`).
- Client-facing text policy per **D4** (4xx carry the reason; 5xx generic + request id). Includes M1a item 9: rpc / health / mcp use the same policy.

Verify: `tests/test_pyerr_no_stderr.py tests/test_ffi_panic_safety.py tests/test_fixes.py tests/test_subinterp_*.py tests/test_shared_state.py` + new tests asserting the real error text reaches the log.

## M5 — Config parsed once at the edge + TPC runtime cleanup (root E)

- One typed `Config` parsed at startup; invalid values are startup errors: `mode` enum (typo silently runs GIL: `app.rs:595`, `app.py:1177`), TLS ports single parser (`app.rs:488-494` vs `app.py:1108-1124`), GC threshold (`python/worker.rs:289-292`, doc says 5000), bridge env (`app.rs:1388-1399`, `CAPACITY=0`), log level, `always_status=0` sentinel / `sample_n.max(1)` (`app.rs:129-131`).
- Listener set `Vec<Listener { addr, tls: Option<Acceptor> }>` built once and consumed by all four run paths. Fixes ✅ `extra_tls` dropped on 3 of 4 paths + fanout `_extra_tls` (`app.rs:596-621`, `tpc.rs:569`).
- Bind every listener before spawning threads; bind failure is a typed error, not `Ok(())` (`tpc.rs:241-253,981-1014`).
- Dead TPC selection plumbing: `tpc_incompatible`, `set_tpc`, `tpc=`/`PYRONOVA_TPC=1` opt-in (`app.rs:559-579,63`), `set_cors_origin` shim (`app.rs:67`).
- `tpc.rs` duplication: accept arm ×6 (1047-1185) → merged `AcceptSource` stream; sigint thread ×3; panic-message extraction ×5; `TpcContext` param object replacing `too_many_arguments`; error classified by message text (`tpc.rs:325`, `app.rs:698-705`) → typed predicates; accept loop + shutdown duplicated between run_gil / run_subinterp (`app.rs:921-1046` vs `1151-1261`).
- `websocket()` silently overwrites duplicate path (`app.rs:370`); `unwrap_or(false)` swallowing inspect errors (`app.rs:843-852`); `let _ = set_nodelay` (`app.rs:964,1186`).
- **Per-app limits, not process globals**: `max_body_size` is a process-wide `static AtomicUsize` written through a per-app setter (`handlers.rs` `MAX_BODY_SIZE`, `app.rs` `set_max_body_size`), so one app's setting leaks into every other app in the process (observed: `test_upload_streaming` → `test_request_fields::test_post_large_body` order dependence). Move it (and the other limits) into `SiteConfig`. The human approved restoring it in the test instead; the supervisor chose the root fix — the test then needs no edit.
- `tpc.rs` production driver (deferred from M6): `Arc::into_raw` `&'static Site` handling → owned/`Rc` site (then `bench.rs` `Threads::lend` loses its last `unsafe`); the worker moved into a failed `thread::spawn` is lost; accept loop logs-and-returns on bind failure.
- `MaybeTlsStream` forwards `poll_write_vectored`/`is_write_vectored` (hot-path copy, `tls.rs:35-71`); `tls.rs:133` hardcoded "10s".

Verify: `tests/test_tls*.py tests/test_env_var_worker.py tests/test_lifecycle.py tests/test_async_shutdown.py tests/test_cli.py` + new tests (extra TLS port served in each mode, bad mode string rejected, port-in-use → error).

## M6 — Benchmark code out of the production library (root G) — **gated by D1, D2**

- Built-in `benchmark.BenchmarkService/GetSum` gRPC handler intercepts every `application/grpc*` POST before routing (✅ `grpc.rs:116`, `handlers.rs:821`, `handlers/tpc.rs:42`).
- `bench.rs` + `PyronovaApp.bench_*` (`app.rs:632,647,1430,1520`) incl. `unsafe` `Arc::into_raw` leaks on failure paths (`bench.rs:151-164,334,384-396`), duplicated response parser, `Ok` after worker panic.
- Benchmark worker builds skip `end_all` on partial failure → abort at finalize (`app.rs:1488-1505,1555-1572`); fold the 3 build loops + 4 script-path blocks into one `build_workers` (the correct one is `app.rs:1364-1374`).

## M7 — FFI ownership + worker hardening

- Per design doc FR-19/C3 (`docs/design/real-engine-in-workers.md:131,267`): worker holds `Py<T>`, code uses `Bound<'py, _>`, not `Vec<*mut PyObject>` + manual DECREF + `ended: bool` (`python/ffi.rs:114-185`, `python/worker.rs:41-48,331-360`); hardcoded immortal threshold `1<<30`.
- Lock poisoning swallowed (`pool.rs:306`, `ffi.rs:59`); unnecessary `unsafe impl Send/Sync for InterpreterPool` (`pool.rs:218`); `worker.rs:73` unsafe impl without SAFETY.
- Async engine exit leaves `response_map` waiters hanging 30s; never calls `inc_completed` (`pool.rs:565-571`).
- `&WorkRequest` instead of 8–11 exploded params (`pool.rs:225`, `python/worker.rs:435,523,553`).
- `_async_engine.py`: RuntimeError from a Rust panic retried forever; `_HANDLER_TIMEOUT = 28` duplicates the Rust budget (take it from M2's `REQUEST_BUDGET`).
- `_bootstrap.py`: `packages_distributions` failure swallowed (381-386); module-global `_iso_in_hook` bool not thread-safe (501).
- `isolate()` list handed to workers through `os.environ` (M1a item 10 deferred): pass it into worker init explicitly.
- Rename `src/worker.rs` (TPC connection driver) → `conn_driver.rs`; delete `python/interp.rs` compat facade.

Verify: `tests/test_worker_no_leaked_refs.py tests/test_capi_hygiene.py tests/test_attach_allowlist.py tests/test_assume_attached_allowlist.py tests/test_subinterp_memory_regression.py tests/test_async_*.py tests/test_isolate*.py tests/test_c_extensions.py` + new test (async engine death → prompt 500).

## M8 — TestClient runs the production path (may turn existing tests red → report, do not edit them)

- `mode="default"` = main-interp GIL, while docstring promises sub-interp "exactly as in production" (✅ `testing.py:201`). This is why the M3 list bug was never caught.
- Split `run()` into one-time prepare + bind/serve: retry loop currently re-registers `/mcp` (duplicate rejected by `router.rs:210`, masking the real EADDRINUSE) and re-runs startup/shutdown hooks (`app.py:1170,1186,1229`).
- `close()` actually stops the server (`testing.py:290-292`).

Verify: full suite. Any existing test that goes red is reported to the human, not edited.

## M9 — Hygiene sweep

History / WHAT comments, stale "Phase 1 / old pool" docs, misplaced doc comments, `#[allow(clippy::…)]` masking design, magic literals, `let _ =` leftovers — only in files not already cleaned by M1–M8. Full list in the review transcript; the implementer re-greps rather than trusting line numbers.

---

## Decisions (human, 2026-09-25)

- **D1** built-in gRPC benchmark handler → **explicit opt-in.** Not registered by default; enabled by an explicit call (e.g. `app.enable_grpc_benchmark()`) so HttpArena can still turn it on. User `application/grpc*` routes are never intercepted when it's off. (M6)
- **D2** `bench.rs` + `bench_*` → **behind a `bench` cargo feature**, off by default; `benchmarks/` builds with `--features bench`. Also fix the leaks / Ok-after-panic. (M6)
- **D3** WebSocket → **`ws.request` attribute** (a `Request`; handler signature unchanged) **and run `before_request` before replying 101**; a hook returning a response rejects the upgrade with that response. (M1d item 2, dispatched after M2 so it reuses the unified pipeline)
- **D4** client-facing error text → **4xx carry the reason; 5xx carry a generic message + request id**, full exception + traceback go to the log. Applies to rpc / health / mcp / hook errors / handler 500 bodies. (M1a item 9, M4)

## Status

- M1a / M1b / M1c / M1d merged (`ab8c510`, `9622f05`, `b68c638`, M1d merge). Full suite after M1: 546 passed, 6 failed (all pre-announced frozen-wrong tests, approved for update), 2 skipped.
- Logging: the Rust access log is the only request log (human decision; `3cba81e`).
- Approved test updates: `test_mcp.py:91`, `test_db_pg.py::test_unknown_column_types_do_not_explode`, `test_passive_gil_metrics.py` ×4, `static_fs.rs` unit-test setup, `tests/e2e/ws_binary_server.py`.
- DB `connect()` with the same DSN but different settings → raises (human decision); fixtures stop passing differing settings.
- M2 merged (`129d1fb`). Full suite after M2: 580 passed, 2 failed (the two source-grep admission tests, approved for deletion), 2 skipped. TPC inline handler timeout: late 504, no preemption (human decision; documented in `docs/tpc-rearch.md` Line 3). M2 measured about -1.3% on TPC inline `def` (per-request context + one `Instant::now()`), no change on the fast and GIL paths.
- M6 merged (`4539104`), M3 merged (`82b9758`). Full suite after M3+M6: 658 passed, 1 failed (`test_pyerr_no_stderr.py::test_log_helper_present_and_wired`, a source-grep test — deletion approved), 8 skipped (bench-feature-only tests in the default build). M3 wrk A/B: dict +3%, str +2.5% on TPC `def`.
