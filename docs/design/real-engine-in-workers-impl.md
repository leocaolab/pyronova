# Impl map — Real `pyronova.engine` in every worker

> Companion to `real-engine-in-workers.md` (the design). Produced by the `impl-design`
> three-step process on 2026-09-23. Base: `17734bd` (v2.7.2 + design). The spike evidence
> is on branch `spike/layer2-r3` (`07ed94e`, `spike/R3-RESULTS.md`).
> **Gate status: 0 blocking after round 2** (see §3). Round 1 had 7 blocking findings,
> all resolved by amending the design.
>
> **Superseded in part (2026-09-23, fresh review, implemented in M0 on `layer2/m0`):** the
> process-wide `RunContext` / `run_context::publish` / `RunGuard` below were replaced by a
> `MAIN` handle captured in `engine()` plus a per-worker shared-state cell (M3), because
> TestClient runs servers concurrently in one process (B4). Main-side threads keep one
> thread-local main tstate (B5); WebSocket handshakes do no Python on TPC/Tokio threads
> (B2). The design doc C2/C4/§8.3 are authoritative; the prose below is the original map.

## 1. CUJ implementations (prose)

**CUJ-1: existing app, unchanged.**
1. `python app.py` imports `pyronova` on main. The script registers routes and hooks on
   the main `PyronovaApp` and calls `app.run()`.
2. `run()` first calls `self._engine._seal_registrations()`. That records how many
   routes, before hooks and after hooks the *script* registered.
3. It then does its run-time registrations: `/mcp` (gil=True), the `enable_logging`
   hooks when `PYRONOVA_LOG=1` or `debug`, and the logger init. Then the engine's `run`.
4. The engine freezes routes, publishes the `RunContext` (shared-state `Arc` + the main
   `InterpreterHandle`), and creates N workers.
5. Each worker executes the slim bootstrap plus the script. The script's
   `from pyronova import Pyronova` loads the real package. The engine module executes
   per interpreter, with no global side effects.
6. `Pyronova()` registers itself as the worker's app via `_register_worker_app`. The
   script's decorators populate the worker's own route table. `app.run()` in the worker
   calls `_seal_registrations()` and returns immediately.
7. Rust reads the worker app and checks that its sealed prefix equals main's sealed
   prefix: `(method, path, gil)` per index plus the hook counts. It then binds handlers
   and hooks by index.
8. Requests arrive with main's route index. The worker calls its own handler at that
   index, runs its own hooks, and replies.
9. On Ctrl-C, workers end their interpreters on their own threads. Main-side threads have
   already dropped every `Py<T>` while attached to main. The `RunGuard` drops last.

**CUJ-2: shared state.** In a worker, `app.state` goes through the real
`PyronovaApp.state` getter. Because this `PyronovaApp` was constructed in a non-main
interpreter while a `RunContext` was published, its `shared_state` is the context's
`Arc`. `incr`/`get` therefore hit main's `DashMap`. A main `gil=True` route reads the same
map.

**CUJ-3: Postgres from a worker.**
1. `PgPool.connect(dsn)` at module level runs on main first and sets `PG_POOL`. In each
   worker it returns early (`PG_POOL` is already set).
2. `pool.fetch_all(sql)` in a TPC worker converts parameters to owned Rust values, then
   `py.detach(|| run_on_db_rt(fut))`. The future runs on `pyronova-db` and the worker
   thread waits on a std channel.
3. Rows come back as Rust values and become Python dicts after reattach.
4. `fetch_iter` returns a `PgCursor` whose `__next__` waits on a std/crossbeam channel
   (not `blocking_recv`) inside `detach`.

**CUJ-4: async handler in pool mode.**
1. The async worker thread runs `_async_engine.py`. Its fetcher thread calls
   `pyronova.engine._worker_recv(WORKER_ID, POOL_ID)` (GIL released while waiting).
2. It gets `handler_idx` and takes the handler from `pyronova.engine._worker_app_handler(idx)`.
3. It runs the before hooks, awaits the handler, runs the after hooks, and calls
   `_worker_send(..., response)`. The Rust side maps the `Response` with
   `parse_sky_response`, headers included.

**CUJ-5: main-interpreter routes keep working.**
- Every main-side thread gets its interpreter from `RunContext.main`:
  - GIL bridge threads: they create one main thread state at spawn, then per item
    acquire it and use the handle's fast path.
  - WebSocket threads: the same, per connection.
  - `spawn_blocking`: `main_attach` per call.
  - `LoopGuard::drop`: `main_attach`.
- Main-interpreter `*_async` DB calls use the new resolver (C9). It captures the calling
  interpreter's handle and event loop, runs the query on the DB runtime, and delivers the
  result with `handle.attach(|py| loop.call_soon_threadsafe(fut.set_result, value))`. Every
  `Py<T>` it holds is dropped inside that attach.

**CUJ-6: maintainer adds an API.** The new method exists in the real package, which
workers import. E2E-10 compares the surfaces on main and in a worker.

**CUJ-7: long run and teardown.**
- Grill soak and the memory-regression suite run.
- On graceful stop, bridge and WebSocket threads drop their `FrozenRoutes` clones and
  handler references inside a main attach before they exit, and `run()` joins them before
  returning. So the last `Arc<RouteTable>` drop happens on main while attached.
- Every E2E log is scanned for the fork's panic texts (`Python::attach was called on a
  thread that has no Python thread state`, `a Py<T> was dropped on a thread that has no
  Python thread state`). Any hit fails the test.

## 2. Mapping (CUJ step → file / function)

No module in this repo has a README placement contract (`ls src/README* python/pyronova/README*` → none).
Every row's contract column is therefore `无契约`, citing the nearest architecture line in
`CLAUDE.md` instead (finding G-8).

| CUJ step | E/N | File / function (evidence) | Contract |
|---|---|---|---|
| 1.2 seal | NEW | `PyronovaApp::_seal_registrations(&mut self)` in `src/app.rs` (pymethod); stores `sealed: Option<(usize, usize, usize)>` on `RouteTable` (`src/router.rs:34`). Search: `rg -n "seal" src python` → no hits | 无契约 (CLAUDE.md: "app.rs — PyronovaApp … route registration") |
| 1.2 call site | EXISTING | `Pyronova.run` `python/pyronova/app.py` (the `_is_worker()` check is at `:1174`; run-time registrations at `:1135-1166`) | 无契约 |
| 1.4 freeze | EXISTING | `run` freeze `src/app.rs:389-418` | 无契约 |
| 1.4 publish | NEW | `run_context::publish(RunContext) -> RunGuard`, `src/run_context.rs`. Search: `rg -n "RunContext\|run_context" src` → none | 无契约 (CLAUDE.md lists no module for process-wide run state; new module) |
| 1.4 workers | EXISTING | `SubInterpreterWorker::new` `src/python/worker.rs:70-104`; callers `src/app.rs:1199`, `src/python/pool.rs:252` | 无契约 |
| 1.5 bootstrap + script exec | EXISTING | `init_in_sub_interp` `src/python/worker.rs:114-233` (`include_str!` `:121`, `PyRun_String` `:216-233`) | 无契约 |
| 1.5 engine exec per interpreter | EXISTING | `#[pymodule] fn engine` `src/lib.rs:43-61`; fork slot `module.rs:578`. **Verified by spike**: worker `exec_module` of the real engine succeeds (`R3-RESULTS.md`) | 无契约 |
| 1.6 register worker app | NEW | `PyronovaApp::_register_worker_app(slf: Py<Self>, py)` sets `static WORKER_APP: PyOnceLock<Py<PyronovaApp>>` in `src/app.rs`, a no-op on main and an error if already set; called from `Pyronova.__init__` (`app.py:188`). Search: `rg -n "WORKER_APP\|register_worker" src python` → none | 无契约 |
| 1.7 compare + bind | NEW | `SubInterpreterWorker::bind_routes(&mut self, py, main: &RouteTable) -> Result<(), String>` in `src/python/worker.rs`, replacing the name lookup at `:236-246`. Search: `rg -n "bind_routes\|route_signature" src` → none | 无契约 |
| 1.8 call by index | EXISTING→changed | `call_handler` `src/python/worker.rs:847` (name lookup `:860-863`); callers `src/handlers/tpc.rs:313-331`, `src/python/pool.rs:435-452` | 无契约 |
| 1.9 teardown | EXISTING | `end_worker_interpreter` `src/python/ffi.rs:747-753`; `Drop for InterpreterPool` `src/python/pool.rs:128-184` | 无契约 |
| 2 state | EXISTING→changed | `PyronovaApp::new` `src/app.rs:43-58`, `shared_state` `:25`, getter `:220`; `SharedState::with_inner` `src/state.rs:25-27`, `#[new]` `:32-37` | 无契约 (CLAUDE.md: "state.rs — SharedState backed by Arc<DashMap>") |
| 3.1 connect | EXISTING | `PgPool::connect` `src/db.rs:364-392` (early return `:373`) | 无契约 |
| 3.2 sync query | EXISTING→changed | `fetch_one` `src/db.rs:397`, `fetch_all` `:489`, `fetch_scalar` `:516`, `execute` `:679`: all `rt.block_on` → `run_on_db_rt` (moved from `src/bridge/db_bridge.rs:42-78`), with `unpack_args` (`db_bridge.rs:89`) | 无契约 |
| 3.4 cursor | EXISTING→changed | `PgCursor::__next__` `src/db.rs:307-321` (`rx.blocking_recv()` `:321`, which panics inside a Tokio context) → std/crossbeam receiver; producer in `fetch_iter` `:442` | 无契约 |
| 4.1 recv | NEW (moved) | `#[pyfunction] _worker_recv` `src/python/worker_api.rs`; body from `pyronova_recv_cfunc` `src/python/ffi.rs:126-304`. Search: `rg -n "_worker_recv\|worker_api" src` → none | 无契约 |
| 4.2 handler by index | NEW | `#[pyfunction] _worker_app_handler(py, idx) -> Py<PyAny>` in `worker_api.rs`, reading `WORKER_APP` | 无契约 |
| 4.3 send | NEW (moved) | `#[pyfunction] _worker_send`; body from `pyronova_send_cfunc` `ffi.rs:310-412` + `parse_sky_response` `src/python/worker.rs:649-840` | 无契约 |
| 4.x loop | EXISTING→changed | `python/pyronova/_async_engine.py:49-122` (`_process_request`), `:125-171` (fetcher) | 无契约 |
| 5 bridge threads | EXISTING→changed | `MainInterpBridge::spawn` `src/bridge/main_bridge.rs:111-195`, `dispatch_one` `:223-269` → `call_handler_with_hooks` `src/handlers.rs:521` (bare attach `:536`); spike: panics | 无契约 |
| 5 persistent tstate | EXISTING (pattern) | `rebind_tstate_to_current_thread` `src/python/ffi.rs:687-732`, `SubInterpGilGuard` `:624-648` | 无契约 |
| 5 main attach helper | NEW | `run_context::main_attach<R>(f: impl for<'py> FnOnce(Python<'py>) -> R) -> R`. Search: `rg -n "main_attach" src` → none; fork `InterpreterHandle::attach` (`interpreter_handle.rs:88`) is what it calls | 无契约 |
| 5 WebSocket | EXISTING→changed | `src/websocket.rs:192` (handler lookup), `:282` (per-connection thread); spike: panics | 无契约 |
| 5 LoopGuard | EXISTING→changed | `src/handlers.rs:208-240` (`Python::attach` `:225`), thread-local `:248-249` | 无契约 |
| 5 async DB resolver | NEW | `db::await_on_loop<T: IntoPyObject + Send>(py, fut) -> PyResult<Bound<PyAny>>` in `src/db.rs`, replacing `pyo3_async_runtimes::tokio::future_into_py` (`db.rs:578,607,637,666`) and the in-future `Python::attach` (`db.rs:585,614,644`). Search: `rg -n "call_soon_threadsafe\|create_future" src` → only `src/handlers.rs` loop helpers (none reusable as a resolver). Spike: `future_into_py` panics on `tokio-rt-worker` | 无契约 |
| 6 parity | NEW | `tests/test_worker_surface_parity.py` | 无契约 |
| 7 panic-text scan | NEW | `tests/conftest.py` `_boot`/teardown (`:69-112`) gains a log scan helper | 无契约 |

**Design-corpus sweep** (commands and hits, pasted):
```
$ ls docs/design/
real-engine-in-workers.md            # (+ untracked polars-in-subinterpreters.md in the main checkout, read from there)
$ grep -rlnE "state_bridge|Design B|sub-interp loadable|mock_engine|real engine|Py_mod_multiple_interpreters" docs ROADMAP.md
docs/arena-async-db-and-static.md  docs/subinterp-c-extension-compat.md  docs/optimize-crud.md
docs/subinterp-c-extension-status.en.md  docs/subinterp-c-extension-status.md  ROADMAP.md
$ grep -rnE "RunContext|run_context|WORKER_APP|bind_routes|main_attach|worker_api|_worker_recv|_worker_send|route_signature|InterpreterHandle" src python docs
(no hits outside this design)
```
Dispositions (from reading their bodies):
- `docs/arena-async-db-and-static.md:67-79` "Design B": **adopt**. This design is Design B.
  Its blockers are superseded: the pyclass audit is done (design §6 C1), and `SharedState`
  now stores `Bytes`, not the `Py<PyAny>` claimed at `:71` (`src/state.rs:20`).
- `docs/optimize-crud.md:56-135` + `ROADMAP.md:430` "SharedState C-FFI bridge
  (`state_bridge.rs`)": **replace**. The real `SharedState` via `RunContext` (C2) makes the
  bridge unnecessary. Mark both docs superseded when C2 lands.
- `docs/tpc-rearch.md:75,94` "the FFI contract stays": **replace**. The contract becomes
  `_worker_recv/_worker_send` pyfunctions. That doc is historical, so add a pointer.
- `docs/design/polars-in-subinterpreters.md` (untracked): **compatible, defer**.
  - Its D2 option (b) relies on the same fork home-interpreter rule, per extension copy.
    The engine is one shared copy, so the rule makes bare attaches panic, which is exactly
    what C4 handles. polars copies are separate `.so`s and unaffected.
  - Its line references into `_bootstrap.py` (`:686,:806,:945,:1030,:1067`) shift when C7
    deletes lines 158-657. Update that doc in the same change.
- `docs/subinterp-c-extension-status*.md`, `subinterp-c-extension-compat.md`: they mention
  the slot. No conflict; update the "engine can't load in workers" wording when C1's
  comment cleanup lands.

## 3. Rubric audit and amendments

**Auditor note (method-gap, reported to the maintainer).** The skill requires a *fresh*
adversarial auditor. This gate ran inside a fork that may not spawn subagents, so the audit
below was done by the writer, adversarially. The rubric families applied are
`arc/docs/{onion,data-truth,economy}-portable-rules.md`, since the repo has no rubric.
Every `file:line` was re-read (script output kept in the session). A fresh-auditor re-run
is recommended before `impl-build` starts.

### Round 1 findings

Class key: method-gap / executor-violation / upstream-inherited / grounding-gap / judgment-call.

| # | Finding | Class | Blocking | Disposition |
|---|---|---|---|---|
| G-1 | **Main-side attach panics are certain, not a risk.** Spike: all 4 GIL bridge threads, the WebSocket thread and 2 `tokio-rt-worker` threads panic at fork `state.rs:102` once workers exec the engine. | grounding-gap | yes | Design: CUJ-5 marked *confirmed*; new §8.8 ordering invariant: C4 + C9 land and pass E2E-8 **before** any change that makes workers exec the engine |
| G-2 | **R-3 is a component, not a risk.** `future_into_py` completion and `db.rs:585/614/644` attach on foreign threads. | grounding-gap | yes | New C9 (async resolver); `pyo3-async-runtimes` dependency removed; R-3 closed |
| G-3 | **`Py<T>` drops on foreign threads are a second failure mode** (spike: "a Py<T> was dropped on a thread that has no Python thread state"). FR-12's grep for `Python::attach(` can't see drops. Example: the last `Arc<RouteTable>` drop on a detached bridge thread (`main_bridge.rs:96-99`, threads not joined). | method-gap | yes | FR-6 extended to drops; C4 adds joined bridge/WS threads and attach-scoped drops; FR-12 adds the E2E panic-text log scan (every E2E) |
| G-4 | **The FR-3 signature check would always fail.** `run()` registers `/mcp` (`app.py:1165`) and the `enable_logging` hooks (`app.py:1004-1005`, auto-enabled in `run`) on main only, before the `_is_worker()` check (`:1174`). The worker would lack them. | grounding-gap | yes | New seal boundary: `_seal_registrations()` at the top of `run()`. The compare covers the sealed prefix only; post-seal routes must be `gil=True` (asserted) and post-seal hooks run only on main paths. FR-2/FR-3/C3/C8 amended |
| G-5 | **`WORKER_APP` can't be set from `#[new]`**: no `Py<Self>` exists yet. | grounding-gap | yes | `_register_worker_app(slf: Py<Self>)` pymethod called from `Pyronova.__init__`; a no-op on main |
| G-6 | **`PgCursor.__next__` uses `blocking_recv` (`db.rs:321`)**, which panics inside a Tokio context such as a TPC worker. With the mock gone, `fetch_iter` in a worker would crash instead of returning the mock's empty cursor (itself a silent lie, `_bootstrap.py:462-468`). | grounding-gap | yes | C6 amended: std/crossbeam receiver; `fetch_iter` is supported in workers; E2E-5 extended |
| G-7 | **The async engine can't find handlers by index**: `HANDLER_NAMES`/`globals()` lookup (`_async_engine.py:52-57`) is gone with C3. | grounding-gap | yes | C5 adds `_worker_app_handler(idx)` and hook accessors |
| G-8 | No module README placement contracts exist; every row is `无契约`. | method-gap | no | Reported; CLAUDE.md architecture list used as the de-facto contract |
| G-9 | CORS double application (R-1): `_cors_before` will now run in workers. `apply_cors` uses `insert` (`handlers.rs:320-341`), so there are no duplicate headers. | judgment-call | no | R-1 narrowed to "verify in E2E-1"; no design change |
| G-10 | `enable_request_id` hooks (`observability.py:59-90`) are **silently skipped in workers today** (closure names not in globals). The request-id feature has been broken for sub-interpreter routes. | upstream-inherited | no | Listed in §8.7 as a fix; E2E-1 case added |
| G-11 | Wrong refs: `router.rs:194` (the alias is at `:196/:199`), `db.rs:47-67` (`runtime()` is at `:46`). | executor-violation | no | Fixed in the design |
| G-12 | Duplicate-invention check: `run_on_db_rt` (exists) vs a new blocking helper; the `RunContext.shared_state` Arc vs `state_bridge.rs` (planned in `optimize-crud.md`). | — (counted: 0 new duplicates; 1 planned duplicate avoided) | no | Reuse `run_on_db_rt`; the planned `state_bridge.rs` is superseded |
| G-13 | Sibling designs unreconciled in the original doc: arena Design B, optimize-crud state bridge, tpc-rearch FFI contract, polars design. | method-gap | no | Dispositions in §2 above; design §14 points here |

Duplicate-definition / re-invention count (item 4): **0** in the amended design. The one
planned duplicate (`state_bridge.rs`) is superseded.

### Round 2 (after amendments)
- Re-checked every amended surface: FR table, C3/C4/C5/C6/C8/C9, §8.1, §8.3, §8.8, §9,
  risks.
- G-1…G-7 are propagated to prose, tables and tests. No new blocking findings.
- Open judgment call: whether `*_async` should be allowed in workers now that C9 is
  interpreter-generic. **Kept fail-closed** (NotImplementedError in workers) until an E2E
  proves it. This is a follow-up, not a blocker.

**Result: 0 blocking.**
