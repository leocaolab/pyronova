# Design — Real `pyronova.engine` in every sub-interpreter worker ("Layer 2")

> Status: design, 2026-09-23. Implements: the Layer 2 item deferred from v2.7.1
> (replace `_bootstrap.py`'s mock engine; see "Design B" in
> `docs/arena-async-db-and-static.md:67-79`, whose PyO3 0.28 blockers no longer hold).
> Baseline: v2.7.2 (`c050396`); M0–M3 merged at `8d89297`. PyO3 from `leocaolab/pyo3` tag
> `subinterp-2026-09-23.2` (`Cargo.toml:83`, `Cargo.lock:1291` → `97b2098`). Minimum Python 3.13.
> **Rev3 (2026-09-23):** resolves the M4 readiness review (B1–B5 blocking, N1–N15). Line
> numbers in this revision are at `8d89297`. Both user decisions from that review were made on
> 2026-09-23 (B2 → Q-2 option (a); B4 → Q-3 approved; see "Open questions").
> Gate: rounds 1–6 in the impl map (rev3: 5 blocking from the M4 readiness review resolved; Q-2/Q-3 decided 2026-09-23). The impl map and audit record are
> in `real-engine-in-workers-impl.md`; spike evidence is in `spike/R3-RESULTS.md` on branch
> `spike/layer2-r3`.

## 1. Overview & goal

Every own-GIL worker today runs the user's script against a **hand-written imitation**
of Pyronova. `_bootstrap.py` installs fake `pyronova`, `pyronova.engine`, `pyronova.app`,
`.cache`, `.cookies`, `.db`, `.uploads`, `.crud`, `.rpc`, `.mcp`, `.testing` modules and
a pydantic stub (`_bootstrap.py:158-657`). Rust injects raw C functions into the worker's
globals: `_pyronova_emit_log` (`worker.rs:129-148`), four `_pyronova_db_*`
(`bridge/db_bridge.rs:284-364`) and, for async workers, `_pyronova_recv`/`_pyronova_send`
(`pool.rs:542-631`). Handlers are then found by `__name__` in that globals dict
(`worker.rs:236-246`).

The reason was that the engine could not be imported in a sub-interpreter
(`_bootstrap.py:3-5,110-115,451-453`, `bridge/db_bridge.rs:3-8`,
`docs/logging-design.en.md:180`). That reason is gone. The fork's `#[pymodule]` macro
always emits `Py_mod_multiple_interpreters = Py_MOD_PER_INTERPRETER_GIL_SUPPORTED`
(fork `pyo3-macros-backend/src/module.rs:578`), and `#[pyclass]` type objects,
`PyOnceLock`, `intern!` and the deferred-decref pools are per interpreter. v2.7.1 already
relies on this for `Request`/`Response` (`worker.rs:163-214`).

**Goal.** Each worker imports the real `pyronova` package and the real extension, with
one code path for main and workers. Concretely:

- Delete the mock modules and the injected C functions.
- Workers resolve handlers from their own real `Pyronova` route table.
- `SharedState`, `PgPool` and the logging bridge work in workers through the normal
  module API.

**Non-goals.**

- **Streaming (`Stream`) and WebSocket handlers in workers.** They stay main-interpreter
  (`gil=True`) for now (`app.rs:696-703`, `websocket.rs:276-281`). This design only
  makes their types importable, and it keeps their main-side threads correct (§8.3).
- **Async DB (`*_async`) in workers.** C9 makes the resolver interpreter-generic, but
  workers stay fail-closed with a clear error (`_bootstrap.py:517-536`) until an E2E proves
  them. That is a follow-up.
- **Making `pydantic_core` itself sub-interpreter-safe.** It is built on upstream PyO3.
  Only the stub question is in scope (§8.6, Q-1).
- **Changing the per-worker copy mechanism for third-party extensions.**
  `_IsolatingExtensionLoader` and in-worker `PyInit` stay as they are.
- **Moving existing `gil=True` framework routes into workers.** That covers CRUD,
  `/metrics`, `/mcp`, RPC and health. This design removes the blocker for CRUD (§8.5);
  flipping the routes is a follow-up.

## 2. Critical User Journeys (CUJs)

- **CUJ-1: Existing app, unchanged.** Actor: a user with a working v2.7.2 app (decorator
  routes, hooks, `model=`, cookies, redirects, `app.isolate(...)`). Trigger: they upgrade
  and run `python app.py` (default `mode="subinterp"`). Steps: start → requests → Ctrl-C.
  Success: every route answers exactly as in v2.7.2 or better, startup succeeds, and
  shutdown is clean. The only behaviour differences are the ones listed in §8.7.
- **CUJ-2: Shared state from a worker route.** Actor: a user who wants a counter or cache
  shared across workers. Trigger: a sub-interpreter route calls `app.state.incr("hits")`
  / `app.state.get(...)`. Success: every worker and the main interpreter see the same
  values. Today this silently returns a fresh `{}` in workers (`_bootstrap.py:261-263`).
- **CUJ-3: Postgres from a worker route.** Actor: a user with `PgPool.connect(dsn)` at
  module level. Trigger: a route without `gil=True` calls `pool.fetch_all(sql, params)`
  under TPC (the default). Success: the rows come back, with no nested-runtime panic and
  the GIL released during I/O. This works today only through the C bridge
  (`db_bridge.rs`).
- **CUJ-4: Async handler in pool mode.** Actor: a user with `async def` routes, run with
  `PYRONOVA_TPC=0`. Trigger: a request to an async route that returns
  `Response(headers={...})`. Success: status, body **and headers** arrive. Today the send
  path has no headers argument (`ffi.rs:310-412`, `_async_engine.py:72-107`).
- **CUJ-5: Main-interpreter routes keep working.** Actor: a user mixing worker routes
  with `gil=True` routes, a WebSocket route and `/metrics`. Trigger: concurrent traffic to
  both kinds. Success: `gil=True` and WebSocket handlers still run on the main interpreter
  and nothing panics. **Confirmed broken without C4/C9 by the spike.** Once workers exec the
  engine, every GIL bridge thread, the WebSocket thread and the `pyo3-async-runtimes`
  threads panic at fork `state.rs:102`, because the fork refuses a bare foreign-thread
  `Python::attach` when several interpreters loaded the copy (`CHANGELOG-FORK.md:80-85`;
  `spike/R3-RESULTS.md`).
- **CUJ-6: Maintainer adds an API.** Actor: a Pyronova developer. Trigger: they add a
  method to `Pyronova` or a new `#[pyclass]`. Success: the method is usable in worker
  routes without touching `_bootstrap.py`, and CI proves main and worker see the same
  surface. Worker logs carry the real worker id (today all sync/TPC workers log
  `worker=0`, `_bootstrap.py:30,77`).
- **CUJ-7: Long run and teardown.** Actor: an operator. Trigger: 16 workers under
  sustained load, then graceful stop. Success: RSS stays flat, throughput is within
  budget, and no crash occurs at worker end (`end_worker_interpreter`, `ffi.rs:747-753`).

## 3. Feature list

| Feature | Serves | Notes |
|---|---|---|
| F1: Real package import in workers | CUJ-1, CUJ-6 | `import pyronova` in a worker loads `python/pyronova/__init__.py` + the real extension |
| F2: Handler resolution from the worker's own route table | CUJ-1, CUJ-6 | replaces lookup by `__name__`; `model=`, path-param shims and closure hooks behave as on main |
| F3: Main interpreter handle + per-worker shared state | CUJ-2, CUJ-5 | the main `InterpreterHandle` reachable from any thread (M0); the running app's `SharedState` map handed to each worker (M3) — see C2 |
| F4: Foreign-thread attach discipline | CUJ-5, CUJ-7 | every Rust thread that attaches names its interpreter explicitly |
| F5: Worker API as `#[pyfunction]`s | CUJ-4, CUJ-6 | recv/send/log become normal module functions; headers on the async path |
| F6: Real `PgPool` in workers | CUJ-3 | sync methods run through the DB runtime without nested `block_on` |
| F7: Slim bootstrap | CUJ-1, CUJ-6 | `_bootstrap.py` keeps only logging, GC policy and isolation |
| F8: Surface-parity and attach gates in CI | CUJ-6, CUJ-5 | fails the build if main/worker surfaces diverge or a bare foreign attach is added |

## 4. Requirements

**Functional**

| # | Requirement | Feature |
|---|---|---|
| FR-1 | A worker's `sys.modules["pyronova"]` is the real package, and `pyronova.engine` is the real extension (per-interpreter module object). No `types.ModuleType` stand-ins for `pyronova*` remain. | F1, F7 |
| FR-2 | `Pyronova.run()` first returns if it runs in a worker (`_is_worker()`, FR-15), before any side effect. On main it then calls `self._engine._seal_registrations()`, which marks the end of the script's registrations; `init_logger`, `/mcp` auto-registration, the `enable_logging` auto-enable, startup hooks and reload handling all run after the seal (today they run before the `_is_worker()` check, `app.py:1086-1174`). The seal is **main-only and idempotent**: `Pyronova` seals once, in the one-time `_prepare()` of its first server (TestClient retries only the bind on port races, review cced8c2 M8), and the engine's own `run()` seals an unsealed table too; only the first call sets the boundary. Every route registered after the seal must be `gil=True` (asserted in `add_route`, with a message naming the route). A worker never needs the seal (review B1): a script may call `app.run()` only under `if __name__ == "__main__"`, which a worker never executes. **The engine also seals** (M4 review B1): `PyronovaApp.run()` (`app.rs:419`) and `bench_inmem` / `bench_loopback` (through `bench_site`; `--features bench` builds only) call the same idempotent seal at the freeze if nothing sealed yet, so raw-engine scripts and benches (`examples/hello_subinterp.py:33`, `benchmarks/bench_subinterp.py:29`, `benchmarks/suite/servers/pyronova_subinterp.py:72`, `benchmarks/bench_inmem.py:24`, `bench_loopback.py:22`) have a sealed table too. An unsealed table never reaches `route_signature`. **Raw `PyronovaApp.run()` off main raises** (M4 review N10): the mock made it a no-op in workers; the real one would start a nested server from inside a worker's init. | F1, F2 |
| FR-3 | After the script runs, the worker takes its handlers, before hooks and after hooks from the worker's own `PyronovaApp` route table. **The worker's whole table** (everything registered while the script executed; a worker has no run-time registrations) must equal **main's sealed prefix**, checked per index on `(method, path, gil)` (`RouteTable.route_keys`, C3) plus the before/after hook counts. A mismatch fails worker startup with both lists in the error. Post-seal routes and hooks (e.g. `/mcp`, the `enable_logging` hooks) exist only on main and are `gil=True`, as today. | F2 |
| FR-4 | Exactly one app per script: a worker with zero apps, or more than one app that registered anything, fails startup with a message naming the count. **Decided 2026-09-23 (Q-2, option (a); M4 review B2):** in a worker, a `PyronovaApp` registers itself in `WORKER_APP` on its **first** route / hook / fallback registration (`add_route`, `before_request`, `after_request`, `fallback` take `slf: Bound<Self>`), so raw-engine `PyronovaApp()` scripts (examples, benchmarks, tests) work unchanged; `Pyronova.__init__`'s explicit `_register_worker_app()` call (`app.py:190`) goes. An app that registers nothing does not count. | F2 |
| FR-5 | `app.state` and `SharedState` obtained in any interpreter during a server run refer to the running app's single `Arc<DashMap<String, Bytes>>`. | F3 |
| FR-6 | A bare `Python::attach` **or a `Py<T>` drop** on a thread with no thread state never runs in production code, and no main-interpreter object is touched on a thread bound to a sub-interpreter. Main-side threads (GIL bridge, WebSocket, `spawn_blocking` GIL path in every mode, `LoopGuard` drop, DB async resolver) attach through `main_attach` / `attach_to` (C4). Every thread that serves many requests keeps one main thread state for its life. Threads drop every `Py<T>` they own (including `FrozenRoutes` clones) while attached before exiting. `run()` joins the bridge threads before it returns and keeps the last `Arc<RouteTable>`, dropped on main while attached. `attach_to` asserts that the **current** tstate (`PyThreadState_GetUnchecked()`) is null or the thread's bound one before it calls `Python::attach` (M4 review N4): with another tstate current (the main thread during a worker's init, which runs the whole real package in M4) `PyGILState_Ensure` → `PyEval_RestoreThread` is a fatal error. | F4 |
| FR-7 | The async worker loop uses `pyronova.engine._worker_recv(worker_id, pool_id)` and `pyronova.engine._worker_send(worker_id, pool_id, req_id, response)`. `response` may be a `Response`, and its headers reach the client. | F5 |
| FR-8 | Worker logging uses `pyronova.engine.emit_python_log` (`logging.rs:203-233`) and carries the real worker index. | F5 |
| FR-9 | `PgPool.fetch_all / fetch_one / fetch_scalar / execute` and `fetch_iter` iteration work from a TPC worker thread. They release the GIL during I/O and never call `Runtime::block_on` or Tokio `blocking_recv` from inside a Tokio context. `*_async` called in a worker raises `NotImplementedError` with the existing message. `*_async` on main resolves through C9. | F6 |
| FR-10 | `_bootstrap.py` contains only the logging-handler install, the GC policy and the isolation machinery (`_bootstrap.py:660-1160`). The injected globals `_Request`, `_Response`, `_pyronova_emit_log`, `_pyronova_db_*`, `_pyronova_recv`, `_pyronova_send` and `_pyronova_pool_id` are gone. | F7 |
| FR-11 | `pyronova` and `pyronova.*` are never cloned, evicted or re-executed by the isolation machinery (M4 review B5). Concretely: `_IsolatingExtensionFinder.find_spec` returns `None` for them (the engine then loads through CPython's own check, which it passes by declaring per-interpreter support, independent of the `_iso_is_single_phase_file` heuristic); `_iso_isolate` and `_pyronova_isolate_libs` (`_bootstrap.py:963-997`) refuse `pyronova` with an error naming FR-11; `Pyronova.isolate("pyronova")` raises on main too; `_iso_import` never calls `_iso_evict` on `pyronova*` (today `_bootstrap.py:1353` evicts the user's outer package, and `from pyronova.config import Settings` → pydantic_settings → pydantic_core reaches it: measured, the retry re-executes the package and yields a second `pyronova.engine` module object). One shared copy is required, because the engine's process-global statics (`MAIN`, `WORKER_STATES`, `LOGGER`, `PG_POOL`) must be one instance, and a re-executed package duplicates `pyronova.context.ctx` and its ContextVars. | F1 |
| FR-12 | A CI test imports `pyronova` in a real worker and compares its public surface with main's (names in `pyronova.__all__`, `Pyronova` public methods, `pyronova.engine` classes and functions). A CI check fails on any `Python::attach(` / `Python::with_gil(` in `src/` outside an allowlist. Every E2E server log is scanned for the fork's two panic texts (`Python::attach was called on a thread that has no Python thread state`, `a Py<T> was dropped on a thread that has no Python thread state`); any hit fails the test (`tests/conftest.py` `fork_panic_lines`; the shared `feature_server` fixture now stops servers with SIGINT and scans their full log, shutdown included). | F8 |
| FR-13 | The `pyronova` package imports pydantic **only** when a route declares `model=` (review B7): no module-level `from pydantic import …` (today `app.py:17-20`). `_route` imports `pydantic.ValidationError` when `model is not None`; an import failure there raises, it never falls back to `Exception` (today `app.py:19-20,507`). A worker of an app that never uses `model=` does not import `pydantic_core`, so auto-isolate does not clone it. | F1 |
| FR-14 | Per-request state kept by framework hooks uses a `ContextVar`, not `threading.local` (review B6): `observability.py:50` `_tls` (request id, metrics start time) moves to `ContextVar`s, as `context.py:50` already does. The async worker loop calls a request's before hooks, handler and after hooks inside that request's own asyncio Task, so they share one context; concurrent requests never see each other's values. | F5 |
| FR-15 | `_is_worker()` is an interpreter check, `pyronova.engine._in_worker()` (Rust: `PyInterpreterState_Get() != PyInterpreterState_Main()`), not the process environment (review N17). Nothing sets `PYRONOVA_WORKER` any more: the four `std::env::set_var` sites (`app.rs:1187,1358,1453`, `pool.rs:211`) and `_bootstrap.py:120` are removed. The env var was process-wide, so it leaked into child processes started from `gil=True` routes; three test scripts reset it for that reason (`test_pyerr_no_stderr.py:69`, `test_subinterp_timeout.py:63`, `test_async_timeout.py:78`; harmless afterwards). | F1 |
| FR-16 | A worker handler that returns a `Stream` fails loudly: a 500, and an error log saying streaming responses need `gil=True, stream=True` (review N18). Today `parse_result` (`worker.rs:432-504`) would stringify it, and with the real package the type becomes importable in workers. | F1 |
| FR-17 | Setters that write **process-global** config apply only on main (review N13): `set_max_body_size` (→ `handlers.rs:144` `MAX_BODY_SIZE`) and `configure_compression` (→ `compression.rs:55-60`). A worker's script calls them again while it executes; there the call is a no-op, and if the value differs from the current global it logs a warning naming the setter and both values, instead of racing main's value. (`METRICS_ENABLED`, `monitor.rs:62`, is written only by `run()`, which returns early in a worker.) | F1 |
| FR-19 | Worker teardown owns its Python references (M4 review B3). `SubInterpreterWorker` holds `Py<T>` (handlers, hooks, cached callables) once C3 lands, so dropping it after `Py_EndInterpreter` (today's order: `tpc.rs:514-518,657-661`, `pool.rs:511`, `pool.rs:706-712`) would decref into a dead interpreter. New `SubInterpreterWorker::end(self)`: restore the worker's tstate, drop/clear every Python field, then `Py_EndInterpreter`, all on the worker's own thread. Failed-start paths (`?` in `pool.rs:251-259`; `sub_workers` when a spawn fails in `tpc.rs`) end each already-built worker on the main thread by swapping to that worker's tstate, never by dropping it with main's tstate current. | F4 |
| FR-20 | The worker executes the script as a **real module** (M4 review N9): the bootstrap runs first in its own namespace; the user script is compiled with `compile(src, script_path, "exec")` into `types.ModuleType("__pyronova_worker__")` registered in `sys.modules` before exec. Today bootstrap + script are one `PyRun_String` (`worker.rs:123-124,227`): `from __future__ import annotations` in a user script is a SyntaxError in every worker (measured), tracebacks show `<string>` with offset lines, and `typing.get_type_hints` on a worker dataclass raises (no `sys.modules[__name__]`). `_async_engine.py` also runs in its own namespace, not the user's globals (N1d). | F1, F7 |
| FR-18 | Every `#[pyclass]` declares `module = "pyronova.engine"`, so worker (and main) types print as `pyronova.engine.Request`, not `builtins.Request` (found by the polars probe). | F1 |

**Non-functional** (measured on bluewhale unless noted)

| # | Requirement | Threshold | Feature |
|---|---|---|---|
| NFR-1 | Hot-path throughput | `just bench-compare` within 3% of v2.7.2, run on a quiet box (load avg < 3) | F2, F5 |
| NFR-2 | Worker startup | per-worker init time ≤ v2.7.2 + 50 ms (median of 16 workers, warm isolate dir), measured **with pydantic installed** and an app that does not use `model=` (FR-13) | F1 |
| NFR-2b | Worker startup, `model=` apps | measured and recorded (no hard gate): the M4 reviewer measured pydantic import + first model at ~70 ms and +14 MB per interpreter on main; worker init is serial on the main thread, so 16 workers ≈ +1.1 s startup and +225 MB before the pydantic_core clone's own pages. Release notes state the number | F1 |
| NFR-3 | Memory | RSS per worker ≤ v2.7.2 + 5 MB after startup, measured with pydantic installed and no `model=` (FR-13); `test_subinterp_memory_regression.py` 9/9 on Linux | F1, F3 |
| NFR-3b | Memory, `model=` apps | RSS per worker measured with a `model=` route (see NFR-2b) and recorded | F1 |
| NFR-4 | Stability | grill soak W=16, `wrk -c128`, 180 s: 0 non-2xx, 0 crashes, RSS flat ±5% | all |
| NFR-5 | Teardown | 20 graceful SIGINT runs with 16 workers, **normal finalization** (`os._exit` was removed in `5531809`): 0 aborts, 0 fork panic texts, exit 0; plus the failed-start case of FR-19 (a worker whose script raises) exits non-zero without an abort | F4 |
| NFR-6 | Parity | the surface-parity test from FR-12 passes on Python 3.13 and 3.14 in CI | F8 |

## 5. Infra

| Need | Exists? | Where / new |
|---|---|---|
| Per-interpreter module object / type objects / `PyOnceLock` | ✅ | fork `src/impl_/pymodule.rs:67` (`PerInterpreterCell<Py<PyModule>>`), fork CHANGELOG "Per-interpreter caches" |
| `Py_MOD_PER_INTERPRETER_GIL_SUPPORTED` on the engine | ✅ | fork `module.rs:578` (always emitted by `#[pymodule]`) |
| Explicit-interpreter attach from a foreign thread | ✅ | `pyo3::sync::InterpreterHandle` (fork `src/sync/interpreter_handle.rs:53-110`; `Copy + Send + Sync`, `current(py)`, `attach(f)`) |
| Persistent per-thread tstate helpers | ✅ | `rebind_tstate_to_current_thread` (`ffi.rs:687-732`), `SubInterpGilGuard` (`ffi.rs:624-648`) |
| DB runtime with non-nested blocking | ✅ | `run_on_db_rt` (`bridge/db_bridge.rs:42-78`), `db::runtime()` (`db.rs:46`) |
| Async result delivery | ➖ | `pyo3-async-runtimes` (`Cargo.toml:27`) is **removed**; replaced by C9 (spike: it panics once workers load the engine) |
| Main handle + attach helpers | ✅ (M0) | `src/run_context.rs` (§6 C2, C4) |
| Per-worker shared-state cell | ➕ | M3 (§6 C2) |
| Worker API functions | ➕ | new `src/python/worker_api.rs` (§6 C5) |
| CI surface-parity + attach-allowlist gates | ➕ | new tests (§9) |
| Postgres in CI for worker DB tests | ✅ (M2) | `services: postgres` in `ci.yml` (`06ff8d1`); `test_db_pg.py` / `test_db_subinterp.py` run in CI |

## 6. Components

### C1: Engine module as a multi-interpreter module
- **Responsibility:** the `engine` module initializes safely in any interpreter and has no
  per-interpreter side effects on process-global state.
- **Reuses:** `#[pymodule] fn engine` (`lib.rs:43-61`). Module init only calls
  `add_class`/`add_function`, with no runtime, logger or env side effects (verified).
- **Audit result (Phase 0):**
  - The only process-global static holding a Python object is `JSON_HELPER`, a
    `PyOnceLock<Py<PyAny>>` (`response.rs:23`) and per interpreter under the fork.
  - `thread_local! LOOP` (`handlers.rs:248-249`) holds a `Py<PyAny>` bound to whichever
    interpreter first used that thread. It is only used on main-side threads and stays
    main-only (C4).
  - Every other static is plain Rust (`monitor.rs:30-127`, `logging.rs:33`,
    `compression.rs:28-60`, `static_fs.rs:31-36`, `db.rs:43-44`, `ffi.rs:38,55`).
  - Plain-Rust statics that a **worker's script can now write** through the real
    `PyronovaApp` setters (review N13): `MAX_BODY_SIZE` (`handlers.rs:144`, via
    `set_max_body_size` `app.rs:131`) and the compression atomics (`compression.rs:55-60`,
    via `configure_compression` `app.rs:206`). With the mock these calls were no-ops. Today
    a replayed script writes the same value, but worker-conditional config would race
    main's. FR-17 makes these setters main-only. `METRICS_ENABLED` (`monitor.rs:62`) is
    written only by `run()` (`app.rs:354,1357,1452`), which returns early in a worker.
- **New:** nothing in `engine()`. Delete the stale comments that claim the opposite:
  `_bootstrap.py:3-5,110-115,451-453,547-550`, `bridge/db_bridge.rs:3-8`,
  `docs/logging-design*.md:180`.
- **Interface:** unchanged.

### C2: Main interpreter handle + per-worker shared state
> Revised 2026-09-23 (fresh review B4, implemented in M0). The original single process-wide
> `RUN` slot (`RunContext` published by `run()`) was dropped: TestClient runs several
> servers concurrently in one process (`testing.py:200,247`), `run_gil`/`run_tpc_gil`/bench
> need a main attach too, and threads such as WebSocket connections and Tokio blocking
> threads can outlive a run. None of that needs per-run state; the main interpreter is a
> process constant.
- **Main handle (M0, done):** `src/run_context.rs`.
  ```rust
  static MAIN: OnceLock<Interp> = OnceLock::new();   // Interp = InterpreterHandle + raw ptr
  pub(crate) fn capture_main(py);   // called from engine() exec; no-op off main
  pub(crate) fn main_interp() -> Interp;
  pub(crate) fn main_attach<F, R>(f: F) -> R;       // see C4
  pub(crate) fn attach_to<F, R>(interp: Interp, f: F) -> R;
  ```
  `engine()` (`lib.rs`) calls `capture_main` first; the main interpreter always executes
  the module before any server can start, because the server is started through it.
- **Shared state (M3):** instead of a published run context, `init_in_sub_interp` hands the
  running app's `Arc<DashMap<String, Bytes>>` (`app.rs:25,48`) to each worker through a
  per-interpreter cell set before the script executes. `PyronovaApp::new` and
  `SharedState::new()` (`state.rs:32-37`) in a worker read that cell
  (`SharedState::with_inner`, `state.rs:25-27`), so `app.state` and a bare `SharedState()`
  see main's map (FR-5). Main-interpreter behaviour does not change: TestClient apps in one
  process keep separate maps.

### C3: Worker app registry and handler resolution
- **Responsibility:** find the worker's own `PyronovaApp` after the script runs, and build
  the worker's handler table from it, aligned with main's frozen table.
- **Reuses:**
  - `RouteTable` fields `handlers`, `handler_names`, `before_hooks`, `after_hooks`,
    `fallback_handler` (`router.rs:34-48`).
  - `add_route` (`app.rs:670-715`).
  - The freeze step in `run()` (`app.rs:389-418`), whose route order is the reference.
- **New:**
  - `static WORKER_APP: pyo3::sync::PyOnceLock<Py<PyronovaApp>>`, per interpreter under
    the fork. It is set by the pymethod `_register_worker_app(slf: Py<Self>, py)`, which
    `Pyronova.__init__` (`app.py:188`) calls unconditionally. It is a no-op on the main
    interpreter and errors if already set (FR-4). It can't be set from `#[new]`: no
    `Py<Self>` exists there yet.
  - `route_keys: Vec<(String, String)>` on `RouteTable` (review N1): per index `(METHOD,
    path)`, pushed in `RouteTable::insert` (`router.rs:105-128`) next to `handlers`.
    `routers` is a `matchit` map and cannot be iterated back into keys. Copied in the
    freeze (`app.rs:389-418`) and in the two bench table builders (`app.rs:1290` `build_one`
    and `app.rs:1402`, line numbers at `1b30a03`).
  - `_seal_registrations(&mut self)` pymethod, **main only** (FR-2): stores `sealed:
    Option<(usize, usize, usize)>` (route, before-hook, after-hook counts) on `RouteTable`
    (`router.rs:34`). Idempotent: a second call keeps the first boundary (TestClient
    retries `run()`). `add_route` rejects a non-`gil` route after the seal.
  - `fn route_signature(t: &RouteTable, upto: usize) -> Vec<(String, String, bool)>` from
    `route_keys` + `requires_gil`, plus hook counts. Main uses its sealed counts; a worker
    uses its whole table (B1: a worker is never sealed).
  - `SubInterpreterWorker::bind_routes(&mut self, py, expected: &RouteSignature) ->
    PyResult<()>` replaces the globals lookup (`worker.rs:236-246`). `RouteSignature` is a
    plain value (`Vec<(String, String, bool)>` + before/after hook counts) computed on main
    **before** the workers are created (M4 review N3): `bind_routes` runs on the main OS
    thread with the worker's tstate current, so it must not touch main's `RouteTable`
    (`Py<T>` handlers) there. It stores `Vec<Py<PyAny>>` handlers, before hooks and after
    hooks, **indexed like main**.
  - **No fallback binding** (M4 review N2): the fallback is never dispatched to a worker.
    Pool mode sends `usize::MAX` to main (`handlers/subinterp.rs:65`); TPC answers 404
    before the fallback is consulted (`handlers/tpc.rs:100-106`), a pre-existing bug
    recorded in Open questions, out of scope here. The fallback stays a main-interpreter
    handler.
- **Constructor changes (review N2, refs updated at `8d89297`):** `SubInterpreterWorker::new`
  (`worker.rs:64-70`) today takes `(script, script_path, func_names, pool_id, shared_state)`
  and no worker index. Callers: `app.rs:1301` (TPC), `app.rs:1432`,
  `app.rs:1499` (bench builders), `pool.rs:251` (pool, reached from `app.rs:1063`). It becomes `new(worker_id, script,
  script_path, expected: &RouteSignature, pool_id, shared_state)`; `func_names` is dropped.
  `InterpreterPool::new` (`pool.rs:194`) gets the same `&RouteSignature` instead of the
  name lists. `pool_id` is exposed to the async engine as its `POOL_ID` (C5).
- **Interface change:** `call_handler(&mut self, handler_name: &str, before: &[String],
  after: &[String], …)` (`worker.rs:847`) becomes `call_handler(&mut self, idx: usize, …)`.
  Hooks come from the worker's own vectors. Callers:
  - `handlers/tpc.rs:313-331`
  - `pool.rs:435-452`
  - `tpc.rs::fire_gc` (unchanged)
- **Teardown (FR-19):** `SubInterpreterWorker::end(self)` replaces `end_worker_interpreter`
  (`ffi.rs:747-753`) at every exit: restore the worker tstate, clear the `Py<T>` fields,
  `Py_EndInterpreter`. `Drop` for `SubInterpreterWorker` checks it was ended; if not, it
  leaks the fields with an error log and never decrefs into another interpreter.

### C4: Foreign-thread attach discipline
- **Responsibility:** every Rust→Python entry on a thread that has no current thread state
  targets an explicit interpreter.
- **Reuses:**
  - `InterpreterHandle::attach` (fork), only on threads with no thread state (see B3 below).
  - `PyGILState_Ensure` via plain `Python::attach` on threads already bound to the target
    interpreter.
  - `MAIN` (C2).
- **Invariant (review B2):** no Python-level operation on a main-interpreter object
  (attach, `clone_ref`, `Py<T>` drop) happens on a thread bound to a sub-interpreter (TPC
  threads, pool workers). `Arc<RouteTable>` clones may pass through those threads as plain
  Rust values: `run()` keeps the last clone and drops it on main, attached (B9).
- **Sites that change** (M0, implemented; all were bare `Python::attach`):

  | Site | Thread | New form |
  |---|---|---|
  | `handlers.rs` `call_handler_with_hooks` | GIL bridge threads, `spawn_blocking` in `run_gil` (`handlers/gil.rs:125`, the whole `mode="gil"`/TestClient path) and pool mode (`handlers/subinterp.rs:230`) | `main_attach`. A thread with no thread state gets **one main tstate for its life** in a thread-local, released by its TLS destructor (review B5): no per-request tstate create/destroy on any path |
  | `handlers.rs` `LoopGuard::drop` | same threads (TLS destructor) | the loop records its interpreter at creation; drop uses `attach_to(that)`. Long-lived bridge threads close it explicitly first (`close_thread_event_loop`) |
  | `websocket.rs` handshake | TPC thread (**bound to the worker**, `worker.rs:157-166`) or tstate-less Tokio worker (pool mode) | **no Python at all**: existence check `ws_handlers.contains_key` only (review B2). Before M0 the bare attach here re-attached the *worker's* thread state and `clone_ref`'d main's handler under the worker GIL |
  | `websocket.rs` per-connection `std::thread` | fresh thread | `attach_to(main)` for the connection's life; the handler is looked up **there**, and the thread's `routes` clone is dropped inside the attach. Not `main_attach`: the thread can outlive `run()` |
  | `db.rs` `*_async` | DB runtime threads | **done in M1** (`4a6a611`): C9 `await_on_loop`; `pyo3-async-runtimes` removed |
  | `main_bridge.rs` bridge threads | bridge threads at shutdown | `JoinHandle`s kept; order (N15): TPC threads joined → their bridge `Arc`s dropped → `MainInterpBridge::shutdown_join` drops the `Sender` and joins; each thread closes its loop and drops its `routes` clone inside `main_attach`, then its TLS destructor releases the main tstate before `join` returns |
  | `worker.rs:427` (`build_request`), `db_bridge.rs:95` | worker thread with its tstate current | kept through M3 as `Python::attach` (allowlisted; justified: the tstate is gilstate-bound by `rebind_tstate_to_current_thread`, `ffi.rs:687-732`). **M4:** `db_bridge.rs` is deleted, so its allowlist entry is removed (the gate fails on a stale entry, `test_attach_allowlist.py:31-35,70-71`); `call_handler` already runs under `SubInterpGilGuard`, so `build_request` takes the `py` it has and the `worker.rs` entry goes too (review N5) |

- **Allowlist also covers `assume_attached`** (M4 review N4): the gate's regex adds
  `Python::assume_attached(` with its own per-file allowlist (`worker.rs:194` today; M4 adds
  the worker-init sites), because an unregistered attach defers `Py<T>` decrefs.
- **New:** `main_attach` / `attach_to` in `run_context.rs`. `attach_to(interp)`:
  bound tstate null → `InterpreterHandle::attach` (fresh tstate); bound tstate of `interp` →
  `Python::attach` (re-attaches through `PyGILState_Ensure`, correct whether or not that
  tstate is current); bound to another interpreter → panic. `main_attach` additionally
  creates the thread-local main tstate on first use. This is the only allowed spelling
  outside the allowlist in FR-12.
- **Fork bug avoided (review B3):** `InterpreterHandle::attach`'s fast path compares only
  the thread's *bound* (gilstate) tstate, so on a bound-but-detached thread (the main
  thread inside `py.detach`, a bridge thread between items) it runs `f` without the GIL,
  and its non-fast path asserts on a thread bound to another interpreter even when that
  tstate is detached. `attach_to` never calls it on a bound thread. Fork fix tracked
  separately.
- **Ordering (§8.8):** C4 and C9 land, and E2E-8 passes, before any change that makes a
  worker execute the engine (C1 activation, C7).

### C5: Worker API (`#[pyfunction]`s)
- **Responsibility:** replace the raw C functions with module functions.
- **Reuses:**
  - The bodies of `pyronova_recv_cfunc` (`ffi.rs:126-304`: `WORKER_STATES` lookup,
    `pool_id` check, `next_req_id`, `response_map` insert, `extract_headers`) and
    `pyronova_send_cfunc` (`ffi.rs:310-412`).
  - `parse_sky_response` / `parse_result` (`worker.rs:649-840`, `432-504`) so the async
    path returns headers. Today they are `&self` methods on `SubInterpreterWorker` using
    per-worker cached pointers (`sky_response_cls`, `json_dumps_func`); `_worker_send` has
    no worker object, so they become **free functions** taking `py` and reading
    per-interpreter `PyOnceLock` caches (`Response` type via `py.get_type::<PyronovaResponse>()`,
    `isojson.dumps`), used by both the sync path and `_worker_send` (review N4).
  - `emit_python_log` (`logging.rs:205-233`), which **already** takes `worker_id:
    Option<usize>` (review N5).
- **New:** `src/python/worker_api.rs`, registered in `engine()`.
  ```rust
  #[pyfunction]
  fn _worker_recv(py: Python<'_>, worker_id: usize, pool_id: u64)
      -> PyResult<Option<(u64, usize, String, String, Py<PyDict>, String, Py<PyBytes>, Py<PyDict>, String)>>;
      // blocks in py.detach(|| state.rx.recv())
  #[pyfunction]
  fn _worker_send(py: Python<'_>, worker_id: usize, pool_id: u64, req_id: u64,
                  response: &Bound<'_, PyAny>) -> PyResult<()>;
      // Response / dict / str / bytes / None, same mapping as parse_result (worker.rs:432-504)
  #[pyfunction]
  fn _worker_app_handler(py: Python<'_>, idx: usize) -> PyResult<Py<PyAny>>;   // from WORKER_APP (C3)
  #[pyfunction]
  fn _worker_app_hooks(py: Python<'_>) -> PyResult<(Vec<Py<PyAny>>, Vec<Py<PyAny>>)>; // the worker's before/after hooks
  ```
  Decisions for the async path (M4 review N1):
  - (a) After hooks get a normalized `Response`, as on the sync path (`build_sky_response`,
    `worker.rs:1000-1026`): a new `_worker_to_response(res) -> Response` pyfunction, built
    on the same free-function mapping as `parse_result`, is called in `_process_request`
    before the after hooks; `_worker_send` then always receives a `Response`.
  - (b) One mapping for both paths. Three return types map differently today and are
    unified; each is listed in §8.7: `bytes` → `application/octet-stream` body (sync today:
    `str(b'..')`); `None` → empty 200 (sync today: `"None"`); a `str` starting with `{`/`[`
    stays `text/plain` on both (async today sniffs JSON, `_async_engine.py:103-108`; the
    sync path doesn't, and the sniffing is dropped as a guess).
  - (c) `worker_api` maps a Rust panic to `RuntimeError` (catch_unwind inside the
    pyfunction). PyO3's `PanicException` is a `BaseException`, and the fetcher's
    `except Exception` (`_async_engine.py:147,163`) would let it kill the thread.
  - (d) The engine script runs in its own module namespace, not the user's globals
    (`pool.rs:697-702` today overwrites user names such as `time`, `asyncio`, `_log`);
    `WORKER_ID`/`POOL_ID` live there.
  - (e) Handlers and hooks are fetched once at engine start, not per request.
  - (f) `_worker_recv` returns a Rust-built `Request` (params/headers stay Rust-side)
    instead of dicts that `Request.__new__` converts back (`types.rs:124-148`).
  - (g) "sealed" does not apply to a worker; `_worker_app_hooks` returns the worker's hooks.
  These replace `HANDLER_NAMES` + `globals().get(name)` (`_async_engine.py:52-57`,
  `pool.rs:519-528`). That lookup disappears with C3 (gate finding G-7).
  `_async_engine.py` also stops using injected names (review N3): `_Request(...)` /
  `isinstance(res, _Response)` (`_async_engine.py:62,72`) become
  `pyronova.engine.Request` / `Response`; `_pyronova_pool_id` becomes a `POOL_ID` global
  set by Rust before exec, next to `WORKER_ID`; its fail-fast check of injected names
  (`_async_engine.py:231-249`) checks `WORKER_ID`, `POOL_ID` and the engine functions
  instead. `ffi_catch_unwind` (`ffi.rs:88-116`) is replaced by the panic → `RuntimeError`
  mapping in (c). The Rust unit tests of `WORKER_STATES` (`ffi.rs:757-911`) are kept.
- **Deleted:**
  - `pyronova_recv_cfunc`, `pyronova_send_cfunc`, `pyronova_emit_log_cfunc` and their
    `PyMethodDef` registration (`worker.rs:129-148`, `pool.rs:537-694`).
  - `bridge/db_bridge.rs` (C6).

### C6: `PgPool` usable from workers
> Sync methods and `PgCursor` implemented in M2 (#4, `06ff8d1`, `2e89112`).
- **Reuses:** `run_on_db_rt` (`db.rs:89`, moved in M2); `PG_POOL` / `PG_RUNTIME`
  (`db.rs:43-44`); `extract_param` (`db.rs:149`) for parameters. `unpack_args`
  (`db_bridge.rs:53`) is **not** reusable: it parses a raw cfunc argument tuple, and is
  deleted with `db_bridge.rs` in M4 (review N5).
- **Change:** `PgPool::{fetch_one, fetch_all, fetch_scalar, execute, connect}` (`db.rs:366-
  700`) call `py.detach(|| run_on_db_rt(fut))` instead of `rt.block_on`. `PgCursor`
  (`db.rs:300-321`): its receiver becomes a std/crossbeam channel, because Tokio's
  `blocking_recv` (`:321`) panics inside a Tokio context such as a TPC worker thread. Main-thread
  callers are unaffected, since `spawn` + channel works everywhere. `*_async` checks
  `interpreter != main` and raises `NotImplementedError` (message from
  `_bootstrap.py:517-536`).
- **Deleted:** `src/bridge/db_bridge.rs`, `_bootstrap.py:449-540` (`_PgPool`,
  `_MockPgCursor`, `_db_ffi`).

### C9: Interpreter-generic async resolver for `PgPool.*_async`
- **Responsibility:** return a Python awaitable for a DB future without any bare
  foreign-thread attach.
- **Reuses:** `db::runtime()` (`db.rs:46`); `row_to_dict`; `InterpreterHandle` (fork).
- **New:** in `src/db.rs`:
  ```rust
  fn await_on_loop<T, F>(py: Python<'_>, fut: F) -> PyResult<Bound<'_, PyAny>>
  where F: Future<Output = PyResult<T>> + Send + 'static, T: Send + 'static,
        T: for<'py> IntoPyObject<'py>;
  // 1. loop = asyncio.get_running_loop(); pyfut = loop.create_future()   (caller's interpreter)
  // 2. handle = InterpreterHandle::current(py); keep Py<loop>, Py<pyfut>
  // 3. runtime().spawn(async move { let r = fut.await;
  //      handle.attach(|py| { loop.call_soon_threadsafe(set_result_or_exception, pyfut, r) }) })
  //    (all Py<T> are dropped inside that attach)
  // 4. return pyfut
  ```
  Rows are converted to Rust-owned values inside the future. The Python dicts are built
  inside `handle.attach`. `pyo3-async-runtimes` is dropped from `Cargo.toml`.
- **Interface:** `PgPool.{fetch_one,fetch_all,fetch_scalar,execute}_async`, unchanged.

### C7: Slim worker bootstrap
Every section of `_bootstrap.py` is accounted for (review N6). **At `8d89297` the file is
1365 lines** (M4 review N7): item 3 (`195828a`) grew the isolation section, and M3 removed
the `PYRONOVA_WORKER` line. The line ranges below are at `1b30a03`; the isolation section is
now `659-1365` (~707 lines), so the result is ≈ 830 lines. Exact ranges are re-derived when
M4 starts; the design fixes the disposition of each section.

| Lines | Content | Disposition |
|---|---|---|
| 1-16 | module docstring (mock rationale, pydantic warning) | **rewrite**: logging + GC + isolation only |
| 17-94 | logging bridge (`_PyronovaRustHandler`, level map) | **keep**; `emit` calls `pyronova.engine.emit_python_log(..., worker_id=WORKER_ID)`; the "not injected" fallback (68-75) goes (the function is a real import now) |
| 96-116 | comment block about injected `_Request`/`_Response` | **delete** |
| 117-119 | mock header + `import sys, types, os` | **keep** `import sys, os` (isolation uses them); drop `types` |
| 120 | `os.environ["PYRONOVA_WORKER"] = "1"` | **already deleted in M3** (FR-15) |
| 122-156 | GC policy | **keep** |
| 158-659 | mock `pyronova*` modules, `cached_json`, cookies, `pyronova.db`, pydantic stub, uploads | **delete** (the real package provides all of it; stub per Q-1) |
| 660-1173 | isolation (`_ISO`, `_iso_*`, `_iso_init_here`, loader/finder, reactive hook, install) | **keep**; add the `pyronova` refusal (FR-11) |

The success criterion is content, not a line count (§10).
`tests/test_isolate_shared_ext.py:35-43` extracts bootstrap functions **by name**, so the
kept isolation functions keep their names (M4 review N6).
- **Adds:**
  - `WORKER_ID` and `POOL_ID` are set by Rust as globals before exec, as `_async_engine.py`
    already does for `WORKER_ID` (`pool.rs:519-528`).
  - Rust side: `sky_response_cls` is taken from `py.get_type::<PyronovaResponse>()`
    instead of the `_Response` global (`worker.rs:253-256`, review N3), and the
    `_Request`/`_Response` injection (`worker.rs:192-214`) is removed.
  - A guard: `_iso_*` refuses `pyronova` (FR-11).
- **Execution model (FR-20, M4 review N9):** no longer one `PyRun_String` of bootstrap +
  script (`worker.rs:123-124,227`). Steps: (1) exec the bootstrap in its own module
  namespace `__pyronova_bootstrap__` with `WORKER_ID` set; (2) `mod =
  types.ModuleType("__pyronova_worker__")`, `mod.__file__ = script_path`,
  `sys.modules["__pyronova_worker__"] = mod`; (3) `exec(compile(src, script_path, "exec"),
  mod.__dict__)`. Handlers come from C3, not from these globals. A user script starting
  with `from __future__ import annotations` then works, tracebacks show the real path and
  line, and `typing.get_type_hints` resolves worker classes.

### C8: `Pyronova` Python class changes (`python/pyronova/app.py`)
- `run()`: the first line becomes `if _is_worker(): return` (moved from `app.py:1174`),
  then `self._engine._seal_registrations()` (main only, idempotent). Run-time
  registrations (`/mcp`, `enable_logging` auto-enable, startup hooks) stay after it (FR-2).
- `_is_worker()` (`app.py:35-37`): `return _engine_mod._in_worker()` (FR-15).
- Module top (`app.py:17-20`): the pydantic `try/import` is removed. `_wrap_with_model`
  (`app.py:476-529`, called from `_route`) imports `from pydantic import ValidationError` inside the
  `model is not None` branch and builds `_BODY_ERRORS` from it (FR-13).
- `__init__` (`app.py:188`): call `self._engine._register_worker_app()` (C3).
- CLI (`pyronova run module:app`, M4 review N15): `__main__.__file__` is `cli.py`, so
  workers exec the CLI (`app.rs:1271-1276`, `cli.py:83`). Before M4 they silently found no
  handlers; after M4, FR-4 aborts startup. Fix in M4: a `PyronovaApp.set_script_path(path)`
  setter (the field exists, `app.rs:28`, but nothing sets it), called by `cli._load_app`
  with the loaded module's `__file__`.
- `isolate()` (`app.py:348-372`): in a worker it is now the real method. It keeps only
  recording the env var, which is harmless because the bootstrap has already acted on it.
- Remove the `__name__`-rebinding workaround comments (`app.py:412-415,558-563`), which
  are no longer needed. Keep `shim.__name__ = fn.__name__` for tracebacks.

## 7. Interfaces with other modules

| Direction | Module | Symbol / signature | Purpose |
|---|---|---|---|
| lib.rs `engine()` → run_context | C2 | `run_context::capture_main(py)` (no-op off main) | record the main interpreter (M0) |
| worker.rs `init_in_sub_interp` → worker cell | C2 | per-interpreter cell with the app's `Arc<DashMap<String, Bytes>>`, set before the script executes | shared state for workers (M3) |
| PyronovaApp::new / SharedState::new → worker cell | C2 | read the per-interpreter shared-state cell when not main | FR-5 |
| worker.rs → engine (Python) | C3 | `WORKER_APP.get(py) -> Option<&Py<PyronovaApp>>` | handler table source |
| worker.rs ← handlers/tpc.rs, pool.rs | C3 | `call_handler(&mut self, idx: usize, method, path, params, query, body, headers, client_ip) -> Result<SubInterpResponse, String>` | index-based call |
| main_bridge.rs, handlers.rs, websocket.rs → run_context | C4 | `main_attach(f)`, `attach_to(interp, f)`, `main_interp()` | explicit main attach |
| _async_engine.py → engine | C5 | `_worker_recv(worker_id, pool_id)`, `_worker_send(worker_id, pool_id, req_id, response)`, `_worker_app_handler(idx)`, `_worker_app_hooks()` | async bridge |
| app.py → engine | C3/C8 | `PyronovaApp._register_worker_app()`, `PyronovaApp._seal_registrations()` (main only, idempotent), `_in_worker() -> bool` | worker app + seal boundary + worker check (FR-15) |
| app.rs / pool.rs → worker.rs | C3 | `InterpreterPool::new(n, py, script, expected: &RouteSignature, …)`, `SubInterpreterWorker::new(worker_id, script, script_path, expected: &RouteSignature, pool_id, shared_state)`, `SubInterpreterWorker::end(self)` | name lists replaced by a plain signature (M4 N3) + worker index; explicit teardown (FR-19) |
| router.rs | C3 | `RouteTable.route_keys: Vec<(String, String)>`, `sealed: Option<(usize, usize, usize)>` | per-index keys for the signature (N1) |
| db.rs → asyncio (any interpreter) | C9 | `await_on_loop(py, fut, convert)` (as implemented in M1, `4a6a611`): `loop.create_future()`, delivery via `main_attach`/`attach_to`, `call_soon_threadsafe` | async DB without foreign attach |
| cli.py → engine | C8 | `PyronovaApp.set_script_path(path)` | workers exec the app module, not `cli.py` (M4 N15) |
| _bootstrap.py → engine | C5 | `emit_python_log(level, name, message, pathname, lineno, worker_id=None)` | logging |
| db.rs → db runtime | C6 | `run_on_db_rt<F: Future + Send + 'static>(fut) -> Result<T, &'static str>` | non-nested blocking |
| tests → engine | F8 | `pyronova.engine.__all__`-style surface dump | parity test |

## 8. Main algorithms

### 8.1 Worker startup
```
main (PyronovaApp.run, main interpreter, GIL held):
 0. seal if not sealed yet (engine-level, idempotent; M4 B1) — also in bench_site (bench_inmem / bench_loopback)
 1. freeze routes (app.rs:471) → FrozenRoutes F; sig_m = route_signature(F, F.sealed) as a plain value
 2. (M3) each worker's per-interpreter shared-state cell is set in init_in_sub_interp (C2); MAIN was captured at engine() exec
 3. for i in 0..n: SubInterpreterWorker::new(i, script, &sig_m, …)   (TPC app.rs:1301 / pool via app.rs:1063 → pool.rs:251)
    on any failure: end every already-built worker by swapping to its tstate (FR-19)
 worker i (new interpreter, own GIL, created on main thread as today worker.rs:70-104):
 4. exec the slim bootstrap in its own namespace (__pyronova_bootstrap__, WORKER_ID=i)
 5. register ModuleType("__pyronova_worker__") in sys.modules; exec(compile(script, script_path), mod.__dict__)   (FR-20)
      - `from pyronova import Pyronova` → real package → engine module exec (per-interp, no global side effects)
      - the first route/hook/fallback registration puts that app in WORKER_APP (FR-4, Q-2 (a)); a second app that registers anything → error
      - decorators register into app's own RouteTable (real add_route, real model= wrap)
      - app.run(), if the script calls it unguarded → returns at its first line (_in_worker);
        under `if __name__ == "__main__"` it is never called. Either way nothing is
        registered after this point, and a worker is never sealed.
 6. app = WORKER_APP.get(py) or fail "script created no Pyronova app" (FR-4)
 7. sig_w = route_signature(app.routes, all); compare with the plain sig_m value (never main's RouteTable, M4 N3)
    if sig_w != sig_m: fail with both lists (first differing index highlighted)
 8. bind handlers/hooks by index (no fallback, M4 N2); cache json dumps, loop, gc (worker.rs:263-356 unchanged)
 9. PyEval_SaveThread (worker.rs:360)
```
Invariants:
- A worker never serves a request unless its sealed table equals main's by index.
- Post-seal routes are `gil=True`, so a worker is never asked for an index beyond main's
  sealed prefix, which is its whole table.
- `MAIN` is set before any worker exists (the engine executed on main first).

Edge cases:
- A script that registers routes conditionally on `PYRONOVA_WORKER`: this fails at step 7
  with a readable diff. That is intended, because it would have silently misrouted before.
- A raw-engine script (`PyronovaApp()` directly): registers on its first route (FR-4, Q-2 (a)) and works unchanged. Its unguarded
  `app.run()` raises in a worker (FR-2, M4 N10).
- `pyronova run module:app`: workers exec the module file set by `set_script_path` (C8,
  M4 N15).
- A script with `if __name__ == "__main__": app.run()` (47 files in tests/examples/
  benchmarks at `1b30a03` call `.run(` under that guard, e.g. `tests/test_capi_hygiene.py:258`): `run` isn't called in the worker.
  Fine, since a worker needs no seal (review B1).
- A route registered from an `on_startup` hook (`app.py:1186`, after the seal): must be
  `gil=True`, otherwise `add_route` raises at startup (§8.7).
- Two `Pyronova()` instances: FR-4 error.

### 8.2 Request path (TPC inline, unchanged cost)
`handle_request_tpc_inline` (`handlers/tpc.rs:34-368`) already knows the route index.
Today it passes `handler_name` and `call_handler` does a `HashMap<String,_>` lookup
(`worker.rs:860-863`). After the change it passes `idx`, which removes one hash lookup per
request and per hook. Request construction (`worker.rs:389-426`), the vectorcall
(`worker.rs:967-973`), `parse_result` and GC scheduling are unchanged.

### 8.3 Attach discipline (C4)
```
main_attach(f):                       # any thread not bound to a sub-interpreter
  bound = PyGILState_GetThisThreadState()
  if bound == NULL:
      THREAD_MAIN_TSTATE.try_with(ensure)     # PyThreadState_New(main): binds gilstate
  attach_to(MAIN, f)

attach_to(interp, f):
  bound = PyGILState_GetThisThreadState()
  NULL          → interp.handle.attach(f)      # fresh tstate, destroyed after
  interp(bound) == interp → Python::attach(f)  # PyGILState_Ensure re-attaches it,
                                               # current or not; counter 1→2→1
  otherwise     → panic("attach_to(interp A) on a thread bound to interpreter B")

THREAD_MAIN_TSTATE drop (thread exit, before join returns):
  if Py_IsInitialized: PyEval_RestoreThread(t); PyThreadState_Clear(t); DeleteCurrent()
```
(Revised in M0 per fresh review B3/B5: the earlier sketch used the handle's fast path on
a persistent tstate, which runs `f` without the GIL when the tstate is bound but not
current, and created a fresh tstate per `spawn_blocking` request.)
Why it is required: the fork resolves a bare foreign-thread `Python::attach` to "the
interpreter that executed this extension copy's module", and panics when several
interpreters executed it (fork `CHANGELOG-FORK.md:80-85`). Once workers import the engine,
every bare attach on a main-side thread becomes a panic. Today it silently lands in main.

CI gate (FR-12, `tests/test_attach_allowlist.py`): non-comment `Python::(attach|with_gil|
try_attach)(` in `src/` must match an allowlist with **exact** per-file counts and a stated
reason: `run_context.rs` (1), `python/worker.rs` (1), `bridge/db_bridge.rs` (1, gone in
M4), `db.rs` (3, gone in M1). E2E-9 seeds a bare attach into a copy of `src/` and asserts
the gate reports it.

**Dependency audit:** `pyo3-async-runtimes` (`Cargo.toml:27`) calls `Python::attach` on
its runtime threads inside `future_into_py`. The spike shows it panics. It is replaced by
C9 and removed.

**Drops count too.** The fork applies the same rule to `Py<T>` drops (spike: "a Py<T>
was dropped on a thread that has no Python thread state"). A thread that owns `Py<T>` or
a `FrozenRoutes` clone must drop them while attached. A grep can't see drops, so the E2E
log scan (FR-12) is the enforcement.

### 8.8 Ordering invariant (from the spike)
The moment a second interpreter executes the engine module, every remaining bare
foreign-thread attach or drop becomes a panic. So C4 + C9 (and the FR-12 gates) must land
and pass E2E-8 **while workers still use the mock**. Their correctness does not depend on
the mock: explicit handles are correct either way. Only then may C1 activation / C7 make
workers execute the engine.

### 8.4 Async worker loop
`_async_engine.py` keeps its structure (fetcher thread + `run_coroutine_threadsafe`,
`_async_engine.py:125-226`). It changes in four ways:
- `_pyronova_recv` → `engine._worker_recv`.
- The result goes through `engine._worker_send(…, response_obj)`, which carries headers.
- The handler is taken from the worker app by index, not from `globals()` by name
  (`_async_engine.py:52-57`).
- Before/after hooks run, matching the sync path. Today this path skips them
  (`pool.rs:514-518`). They run **inside the request's own Task**
  (`_process_request`), together with the handler, so each request has its own
  `contextvars` context. Framework hooks keep per-request state in `ContextVar`s (FR-14):
  `observability.py:50`'s `threading.local` would be shared by every coroutine on the loop
  thread, so one client could receive another request's `x-request-id` and the metrics
  latencies would mix (review B6). On the sync paths Rust calls the hooks and the handler
  one after another on one thread in that thread's context, so a `ContextVar` behaves like
  the thread-local did.

### 8.5 DB from workers
`py.detach(|| run_on_db_rt(async move { pool.fetch_all(...) }))`. The future runs on the
2-thread `pyronova-db` runtime (`db.rs:47-67`). The worker thread waits on a
`sync_channel` and never enters a Tokio context. Parameters are converted to owned Rust
values before `detach` with `extract_param` (`db.rs:149`; `unpack_args` is cfunc-only and
deleted with `db_bridge.rs`). Rows are converted
back to Python after reattaching, so no Python object crosses interpreters (the invariant
stated at `db_bridge.rs:24-28` is kept). Implemented in M2 (`06ff8d1`, `2e89112`).

### 8.6 pydantic in workers (decision needed, Q-1)
Removing the stub has a consequence:
- `import pydantic` in a worker reaches `pydantic_core` (upstream PyO3, declares
  not-supported).
- The load-time override plus the reactive auto-isolate clone it per worker. That path
  is measured working: 432k requests (`docs/subinterp-ecosystem-isolation.md:211-216`).
- `model=` then validates for real in workers. Today it silently passes an empty instance
  (`_bootstrap.py:552-571`: `model_validate_json` returns `cls()`).
- Cost: one pydantic copy per worker **for apps that use `model=`**. Without FR-13 it would
  be every worker of every app with pydantic merely installed, because `app.py:17-20`
  imports it at module load (review B7). With FR-13 the import happens in
  `_wrap_with_model`, so an app with no `model=` never imports `pydantic_core` in a
  worker; NFR-2/3 are measured that way.

**Decision (2026-09-23):** remove the stub. The stub violates "don't cover": it returns an empty
model and skips validation. **Alternative:** keep a stub that *raises* on validation in
workers, so the lie becomes a loud error.

### 8.7 Deliberate behaviour changes (release notes)
- Worker `model=` validates (or errors, per Q-1) instead of returning an empty model.
- `redirect()` in workers enforces the 3xx check (`__init__.py:27-31`); the mock skipped
  it (`_bootstrap.py:193-203`).
- `@app.options`, `@app.head`, `@app.readiness_check` and other methods the mock's
  `__getattr__` turned into `@None` now work in workers (`_bootstrap.py:247-254`).
- Closure hooks registered on main (`_cors_before` `app.py:649-654`, `_log_before/_after`
  `app.py:965-972`, observability `observability.py:59-92`) now run in workers. Today
  they are skipped because their names aren't in worker globals. CORS already runs in
  Rust, so the risk is double handling. Verify in E2E-1.
- `app.state` in workers is shared instead of a fresh `{}`.
- The async pool path runs hooks and returns headers.
- A route added from an `on_startup` hook, or any other route registered after `run()`
  begins, must be `gil=True`; a non-`gil` one raises at startup with its method and path.
  It runs in `mode="gil"` too, where there are no workers, because the rule is about the
  table, not the mode (review N12).
- `PYRONOVA_WORKER` is no longer set in the process environment (FR-15). Code that read it
  should use `pyronova.engine._in_worker()`; child processes no longer inherit it.
- A worker handler returning a `Stream` gets a 500 with an error log, instead of a
  stringified body (FR-16).
- `app.max_body_size = …` / `app.enable_compression(…)` executed in a worker's replay are
  no-ops (main's call already set the process-wide value); a differing value logs a
  warning (FR-17).
- `pydantic` is imported only by apps that use `model=` (FR-13); an app with `model=` and
  a broken pydantic install fails at route registration instead of catching every
  `Exception` as a validation error. A `model=` app pays a pydantic copy per worker
  (NFR-2b/3b numbers in the release notes).
- Handler return mapping is the same on sync and async paths (C5 N1b): `bytes` →
  `application/octet-stream` (sync used to send `str(b'..')`), `None` → empty 200 (sync used
  to send `"None"`), and a `str` that looks like JSON is no longer relabelled
  `application/json` on the async path.
- `/metrics` counts worker-route traffic, because `install_metrics` hooks
  (`observability.py:94-133`) now run in workers and write the shared map; each worker
  request pays that hook (M4 N13). NFR-1's bench variant includes it.
- RPC in workers: `pyronova/rpc.py:19` imports msgpack, whose `_cmsgpack` refuses to load in
  sub-interpreters, so a worker gets `msgpack.fallback` (pure Python). RPC routes are
  `gil=True`, so no hot path is affected (M4 N12).
- `pyronova run module:app` with sub-interpreters: workers now exec the app module; before,
  they exec'd `cli.py` and silently had no handlers (M4 N15).
- A raw `PyronovaApp.run()` executed inside a worker raises instead of being ignored (M4
  N10).
- User scripts may start with `from __future__ import annotations`; worker tracebacks show
  the script's real path and line numbers (FR-20).

## 9. Integration / E2E tests

Every test runs the server in a subprocess (`tests/conftest.py:69-112`), because a
sub-interpreter permanently disables `PyGILState_Check` in its process.

| Test | CUJ | Setup → Action → Assertion |
|---|---|---|
| E2E-1 | CUJ-1 | existing `feature_server_factory` suites (`test_cookies_e2e`, `test_cors_e2e`, `test_routing_e2e`, `test_uploads_e2e`) in `subinterp` mode + `test_capi_hygiene.py` → unchanged assertions pass; new cases assert: one CORS header value (no double application); `enable_request_id` echoes `x-request-id` from a worker route; `PYRONOVA_LOG=1` still starts (post-seal hooks main-only) |
| E2E-2 | CUJ-1 | `test_isolate.py` (all 5) + grill W=4 smoke → pass |
| E2E-3 | CUJ-1 | (a) script registering a route only `if pyronova.engine._in_worker()` → server exits non-zero; stderr contains both route lists. (b) script with `app.run()` only under `if __name__ == "__main__"` + `enable_logging` + an MCP tool → starts, routes answer (B1: no seal needed in workers; post-seal `/mcp` main-only) |
| E2E-4 | CUJ-2 | 4 workers; one route does `app.state.incr("n")`, another a bare `SharedState().incr("m")`; 400 requests each → a main-side `gil=True` route reads `n == 400` and `m == 400` (N10) |
| E2E-5 | CUJ-3 | PG service in CI; `test_db_subinterp.py` with TPC default → 5 tests pass (today always skipped) + a new `fetch_iter` iteration case from a worker route → all rows, no panic |
| E2E-6 | CUJ-3 | worker route calls `pool.fetch_all_async` → 500; the **server log** contains the `NotImplementedError` message (the 500 body is the generic "handler raised an exception", `worker.rs` ~987, review N8); server stays up |
| E2E-7 | CUJ-4 | `PYRONOVA_TPC=0`, async route returns `Response(headers={"x-a":"1"})` and a before hook sets a header → both headers present |
| E2E-7b | CUJ-4 | `PYRONOVA_TPC=0`, `app.enable_request_id()`, async route that `await asyncio.sleep(random)`; 200 concurrent requests, each with a distinct `x-request-id` → every response echoes its own id (review B6, FR-14) |
| E2E-8 | CUJ-5 | `tests/test_layer2_main_side.py`, **parametrized over `PYRONOVA_TPC=1` and `=0`** (review B8: pool mode has its own sites). App (`tests/_l2_main_side_app.py`) whose own script execs the real engine in each worker (at activation the probe apps are rewritten, approved 2026-09-23, Q-3); worker route + `gil=True` sync/async routes + WebSocket echo + `/metrics` under concurrent load, then SIGINT → 0 non-2xx, WS echoes, exit 0, and the log has no `panicked at` line and neither fork panic text (including at shutdown). M1 adds the `gil=True` async DB route + a main Python thread awaiting `fetch_all_async`. Plus `test_concurrent_in_process_servers`: 3 TestClient servers at once in one process (B4) |
| E2E-9 | CUJ-5 | `tests/test_attach_allowlist.py`: the allowlist gate (FR-12) reports a bare `Python::attach` seeded into a copy of `src/` |
| E2E-10 | CUJ-6 | surface parity: a worker route returns `pyronova.__all__`, the public methods of `pyronova.Pyronova` (`sorted(m for m in vars(pyronova.Pyronova) if not m.startswith("_"))`), and an explicit list of `pyronova.engine` names checked with `hasattr` → equal to main's. Not `dir(pyronova)`, which varies with lazily imported submodules (review N9) |
| E2E-11 | CUJ-6 | script logs `logging.getLogger().info("init")` **at top level** (runs once per worker at init, and once on main) with 4 workers → **exactly one** "init" line per worker id 0–3, plus main's line, which carries no worker id (M4 N14: `emit_python_log` maps `worker_id=None` to 0, `logging.rs:213`; main must log without an id so it can't stand in for worker 0). Not per request: on macOS SO_REUSEPORT sends almost all traffic to one TPC thread (`tpc.rs:372`) |
| E2E-12 | CUJ-7 | `test_subinterp_memory_regression.py` (Linux, 9 tests) + grill W=16 180 s + 20× graceful SIGINT (normal finalization) → NFR-3/4/5 |
| E2E-13 | CUJ-1 | script creating two `Pyronova()` apps → startup fails; message names the count (FR-4, N10) |
| E2E-14 | CUJ-1 | `app.isolate("pyronova")` → startup fails with the FR-11 message, on main before workers exist (N10, M4 B5) |
| E2E-14b | CUJ-1 | worker route: `from pyronova.context import ctx`, then `from pyronova.config import Settings` (pulls pydantic_core through the reactive path) → the request-id round-trips, and `sys.modules["pyronova.engine"]` is the same object before and after (M4 B5) |
| E2E-15 | CUJ-1 | app with pydantic installed and no `model=` → in a worker `"pydantic_core" not in sys.modules`; app with `model=` and **no** `app.isolate` → invalid body gets 422 from a worker route, i.e. the reactive auto-isolate path for pydantic_core (never exercised in a server before: the stub masked it; only the proactive path is tested, `test_isolate.py:373`). Runs on macOS **and** Linux (FR-13, B7, M4 N11) |
| E2E-16 | CUJ-1 | worker handler returns `Stream()` → 500, log names `gil=True, stream=True` (FR-16) |
| E2E-17 | CUJ-1 | script sets `app.max_body_size` to a different value only in workers (`if pyronova.engine._in_worker()`) → main's value is enforced; log has the FR-17 warning |
| E2E-18 | CUJ-6 | `repr(type(req))` in a worker route → `pyronova.engine.Request` (FR-18) |
| E2E-19 | CUJ-7 | teardown (FR-19): (a) 4 workers serve, SIGINT → exit 0, no fork panic text, no abort; (b) failed start: worker 2's script raises at import (conditional on `_in_worker()` and a worker id) → the server exits non-zero with the script's error, no abort, no panic text |
| E2E-20 | CUJ-1 | worker script that starts with `from __future__ import annotations` and has a dataclass → starts; `typing.get_type_hints` on it works in a worker route; a raising handler's logged traceback names the script path and line (FR-20) |
| E2E-21 | CUJ-1 | `pyronova run module:app` with 2 workers → worker routes answer (C8 CLI, M4 N15) |
| E2E-22 | CUJ-1 | raw-engine scripts: `examples/hello_subinterp.py`-style `PyronovaApp()` server answers in subinterp mode, with no edits to the script (Q-2 (a)) |

**Existing tests that must change** (the `test_ffi_panic_safety.py` rewrite below was approved by the user on 2026-09-23; any other test change still needs a report first):
- `tests/test_ffi_panic_safety.py:22-60` greps for `pyronova_recv_cfunc`,
  `pyronova_send_cfunc`, `pyronova_emit_log_cfunc` and `ffi_catch_unwind`. All are deleted
  by C5. Proposed replacement: assert that the `_worker_recv`/`_worker_send` pyfunctions
  exist and that no `extern "C" fn` remains in `src/python/`.
- `tests/test_capi_hygiene.py:96` imports `Response` inside a worker from
  `pyronova.engine`. It keeps working, now against the real module, so no change is
  expected.
- `test_capi_hygiene.py:258` constructs `Request` with 7 wrong-typed args. It must keep
  passing against `#[new] py_new` (`types.rs:124-148`), so no change is expected.
- `tests/test_async_shutdown.py:24` greps `_async_engine.py` for shutdown steps. These are
  kept, so no change is expected.
- **Break, report before changing (review N7):**
  - `tests/test_subinterp_hooks.py:11` and `tests/test_env_var_worker.py:30` build the
    injected `_Response` global in an after hook. Both are manual scripts (pytest collects
    no `test_` function), but they stop working: `_Response` no longer exists. Proposed:
    `from pyronova import Response`.
  - `tests/test_subinterp_memory_regression.py:94` counts objects whose type is named
    `"_Request"`, but the class has been `"Request"` since v2.6 (`types.rs:23`), so the
    assertion is already vacuous today. Proposed: count `pyronova.engine.Request`
    instances. This is a pre-existing test bug; report it with M4.
- **Become redundant, no change needed:** the three scripts that reset
  `os.environ["PYRONOVA_WORKER"] = ""` (FR-15).
- **Must change at activation — approved by the user 2026-09-23 (Q-3, M4 review B4):** the three Layer-2
  probe apps detect a worker by `"_pyronova_emit_log" in globals()`, which M4 removes, so in
  workers they take the main branch: `tests/_l2_main_side_app.py:16`,
  `tests/_l2_async_db_app.py:17`, `tests/_l2_m3_worker_app.py:19`. Broken as a result:
  `test_layer2_main_side.py:102` and `test_layer2_async_db.py:95` (assert `{"real_engine":
  True}`) and `test_layer2_m3.py:343,347` (the M3 probe calls `_register_worker_app()` before
  `Pyronova()`, so the real app would be refused as a second one). Proposed: detect with
  `pyronova.engine._in_worker()`, drop the manual `ExtensionFileLoader` load (the real
  package is imported normally), delete the M3 probe app and its two tests (M4's real
  activation covers them).
- **No change (Q-2 (a)):** the raw-`PyronovaApp()` files `tests/test_c_extensions.py:5`,
  `tests/test_subinterp_hooks.py:4`, `examples/hello_subinterp.py:12`,
  `benchmarks/bench_subinterp.py:6`, `benchmarks/suite/servers/pyronova_subinterp.py:5` keep
  working as they are (their `_Response`-global use in `test_subinterp_hooks.py:11` is the
  separate N7 item above).
- **Source-grep tests that constrain M4's code (keep the strings, or report first; M4
  review N6):** `test_pyerr_no_stderr.py:43` greps `src/python/*.rs` for "failed to create
  _Response"; `test_admission_control.py:71` greps `src/python` for `total_permits = n *
  128`; `test_isolate_shared_ext.py:35-43` extracts bootstrap functions by name;
  `test_attach_allowlist.py` needs its `db_bridge.rs` (and, with N5, `worker.rs`) entries
  removed in the same change.

## 10. Success criteria
- [ ] FR-1 … FR-12 met; `rg '_pyronova_(recv|send|emit_log|db_)|_mock_engine|types.ModuleType\("pyronova' src python` returns nothing.
- [ ] `pyo3-async-runtimes` removed from `Cargo.toml`; C9 serves every `*_async`.
- [ ] No E2E server log contains either fork panic text (FR-12 scan), including across shutdown.
- [ ] NFR-1 … NFR-6 thresholds hit (§4), with bench numbers from a quiet bluewhale.
- [ ] E2E-1 … E2E-22 pass on macOS and Linux (E2E-22: raw-engine scripts unchanged, Q-2 (a)); the parity/attach gates are in CI.
- [ ] `_bootstrap.py` contains only the three kept sections of the C7 table (≈ 830 of 1365
  lines at `8d89297`); `rg -n "_Fake|_Mock|PYRONOVA_WORKER|ModuleType\(\"pyronova" python/pyronova/_bootstrap.py` returns nothing.
- [ ] `rg -n "_iso_evict\(|find_spec" python/pyronova/_bootstrap.py` shows the `pyronova` guards (FR-11).
- [ ] `rg -n "^(from|import) pydantic|^\s+from pydantic" python/pyronova` hits only `_wrap_with_model` (FR-13).
- [ ] `rg -n "threading.local" python/pyronova` returns nothing (FR-14).
- [ ] `rg -n "PYRONOVA_WORKER\b|\"PYRONOVA_WORKER\"" src python` returns nothing (FR-15).

## 11. Performance considerations
- **Hot path (TPC inline).** The per-request work is unchanged apart from dropping a
  `HashMap<String>` lookup per handler/hook (§8.2). Expect equal or slightly better. Gate:
  NFR-1.
- **Startup.** A worker now imports the real package: `__init__.py`, `app.py`, `mcp.py`,
  `rpc.py`, `cookies.py`, `uploads.py`, `cache.py` and the engine module exec. Today the
  bootstrap defines the mocks inline. Measure per-worker init time (log it, as
  `worker.rs` already times init) before and after. Budget NFR-2.
- **Memory.** The real package adds module objects per worker. Its type objects were
  already per interpreter for `Request`/`Response`; now there are 9 pyclasses per worker.
  Budget NFR-3.
- **GIL bridge and `spawn_blocking`.** Every thread that attaches to main keeps one main
  tstate for its life (M0, §8.3), including the `mode="gil"`/TestClient `spawn_blocking`
  path (review B5). No path creates and destroys a tstate per request.
- **Measure:** `just bench-compare` on a quiet bluewhale (the 2.7.2 run was invalidated
  by load avg 43), grill throughput W=4/8/16, worker init time, RSS per worker.

## 12. Reliability considerations
- **Failure modes and handling:**
  - Route-table mismatch between main and a worker: fail startup loudly (8.1 step 7). It
    never serves requests from a misaligned table.
  - No app, or two apps, in the script: fail startup (FR-4).
  - Engine cloned by isolation: refuse (FR-11). A cloned engine would have its own
    `WORKER_STATES`, `LOGGER` and `MAIN`, and workers would silently talk to nothing.
  - Bare foreign attach: the CI gate prevents it. At run time the fork panics with a
    message, which PyO3 surfaces as `PanicException`; the request gets a 500.
  - `main_attach` before the engine executed on main, or on a thread bound to a
    sub-interpreter: panics with an explanation. That is a programming error, not user
    input.
- **Teardown (FR-19, M4 review B3/N8):** each worker ends through
  `SubInterpreterWorker::end(self)` on its own thread: its `Py<T>` fields are dropped while
  its tstate is current, then `Py_EndInterpreter`. The engine module object is per
  interpreter (`PerInterpreterCell<Py<PyModule>>`), so ending a worker frees only that
  worker's module. The last `Arc<RouteTable>` is `run()`'s own clone, dropped on main while
  attached after all workers and bridge threads have ended (after `InterpreterPool` drop,
  `pool.rs:128-184`, the TPC thread join and `MainInterpBridge::shutdown_join`).
- **No `os._exit` any more** (removed in `5531809`): a graceful stop finalizes normally.
  That exposes one case the hard exit used to hide: `InterpreterPool::drop` waits 5 s per
  worker thread and then `mem::forget`s a thread that hasn't finished (`pool.rs:128-184`);
  that worker's sub-interpreter is still alive when `Py_Finalize` runs, which
  `ffi.rs:737-741` documents as an abort. Defined behaviour: `run()` checks after the pool
  drop whether any worker was forgotten; if so it logs an error naming the worker and the
  handler it was running, flushes, and exits with a non-zero status via `os._exit` **only
  in that case**, instead of letting `Py_Finalize` abort. NFR-5/E2E-19 cover the normal path;
  a stuck-handler case (a handler that ignores shutdown) is added to E2E-19.
- **Failed start:** workers built before the failure are ended by swapping to each one's
  tstate on the main thread (FR-19); they are never dropped with main's tstate current.
- **Fail-closed defaults:**
  - Strict `check_multi_interp_extensions: 1` stays (`worker.rs:79`). The engine passes it
    on its own; third-party extensions still go through the transient override.
  - No fallback to globals-by-name lookup. It is removed, not kept as a shim.

## 13. Security considerations
- **Trust boundary unchanged.** The user script is trusted and runs in every worker, as
  today. HTTP input reaches the same parsers.
- **Code that goes away:** raw `PyArg_ParseTuple` format strings (`ffi.rs:126-412`,
  `db_bridge.rs:150-248`), which parsed Python-controlled arguments in `unsafe` code. The
  DB cfuncs had no `catch_unwind` (`db_bridge.rs:150-248`). PyO3's typed extraction
  replaces both, which removes a class of memory-safety risk.
- **Behaviour now enforced in workers:** the mock `redirect` skipped the 3xx status check,
  so a worker could emit, e.g., `redirect(url, 200)` with a `Location` header. The real
  one rejects it. The CR/LF guard was already present in both.
- **Shared state:** `SharedState` stores `Bytes` only (`state.rs:20`), so exposing main's
  map to workers shares data but no Python objects or capabilities.

## 14. Abstraction & reuse

**Approach:** delete the parallel implementation instead of abstracting over it.
- The worker becomes "the same program in another interpreter". The only worker-specific
  pieces left are handler binding by index (C3), attach discipline for main-side threads
  (C4) and the isolation/logging/GC bootstrap.
- New code is small: `run_context.rs`, `worker_api.rs`, `bind_routes`, `main_attach`.
- Everything else is moved or deleted code.

**Reuse map** (existing code to call):

| Symbol | Location | How we use it |
|---|---|---|
| `#[pymodule] fn engine` | `src/lib.rs:43-61` | loaded as-is in workers; register `worker_api` fns |
| `RouteTable` / `FrozenRoutes` | `src/router.rs:34-48,196-199` | worker binding source + reference signature |
| route freeze in `run` | `src/app.rs:389-418` | reference order for 8.1 step 7 |
| `add_route` | `src/app.rs:670-715` | unchanged; now also runs in workers |
| `PyronovaApp.shared_state`, `state` getter | `src/app.rs:25,48,220` | source for each worker's shared-state cell (M3) |
| `SharedState::with_inner` | `src/state.rs:25-27` | worker `SharedState()` / `app.state` |
| `InterpreterHandle` | fork `src/sync/interpreter_handle.rs:53-110` | main handle + explicit attach |
| `rebind_tstate_to_current_thread` | `src/python/ffi.rs:687-732` | pattern for persistent main tstate on bridge threads |
| `SubInterpGilGuard` | `src/python/ffi.rs:624-648` | per-item acquire on persistent tstate |
| `end_worker_interpreter` | `src/python/ffi.rs:747-753` | folded into `SubInterpreterWorker::end` (FR-19) |
| `WORKER_STATES`, `WorkerState` | `src/python/ffi.rs:20-63` | backing for `_worker_recv/_send` |
| recv/send bodies | `src/python/ffi.rs:126-412` | moved into `worker_api.rs` pyfunctions |
| `parse_sky_response`, `parse_result` | `src/python/worker.rs:432-504,649-840` | response mapping for `_worker_send` |
| `build_request` | `src/python/worker.rs:389-426` | unchanged |
| `call_handler` | `src/python/worker.rs:847-1059` | signature → index-based |
| `emit_python_log` | `src/logging.rs:205-233` | worker logging; `worker_id` already exists |
| `ContextVar` pattern | `python/pyronova/context.py:50` | model for FR-14 (`observability.py:50`) |
| `RouteTable::insert` | `src/router.rs:105-128` | also pushes `route_keys` (N1) |
| `run_on_db_rt` | `src/db.rs:89` | moved in M2, used by `PgPool` sync methods |
| `extract_param` | `src/db.rs:149` | param conversion before `detach` (`unpack_args` is cfunc-only, deleted in M4) |
| `db::runtime`, `PG_POOL` | `src/db.rs:43-46` | unchanged; also C9's runtime |
| `MainInterpBridge::spawn` | `src/bridge/main_bridge.rs:111-195` | add persistent main tstate per thread |
| `call_handler_with_hooks` | `src/handlers.rs:521-714` | attach via `main_attach` |
| isolation machinery | `python/pyronova/_bootstrap.py:659-1365` (at `8d89297`) | kept + `pyronova` guards in finder / evict / isolate (FR-11) |
| logging handler / GC policy | `python/pyronova/_bootstrap.py:22-94,143-156` | kept; call real `emit_python_log` |
| `_async_engine.py` loop | `python/pyronova/_async_engine.py:125-226` | kept; recv/send/handler lookup swapped |
| `_is_worker` | `python/pyronova/app.py:35-37` | first line of `run`; body → `_in_worker()` (FR-15) |
| subprocess test harness | `tests/conftest.py:69-138` | all E2E tests |

**New abstractions and why each is needed:**
- `MAIN` + the per-worker shared-state cell (C2): foreign threads need the main handle
  (a process constant), and a worker's `SharedState` needs main's map, which today nothing
  hands over (`state.rs:32-37`). A process-wide "current run" slot was rejected: servers
  can run concurrently in one process (TestClient).
- `WORKER_APP` + `bind_routes` (C3): replaces name-based lookup, which is the root of the
  mock/real divergence (wrapped vs unwrapped handlers, skipped closure hooks).
- `main_attach` (C4): a single audited spelling for "run on main from any thread". It makes
  the FR-12 gate enforceable.
- `worker_api.rs` (C5): the async bridge needs *some* interface. Module functions replace
  `extern "C"` + `PyArg_ParseTuple`.
- Seal boundary (C3/C8): distinguishes script registrations (shared with workers) from
  run-time main-only ones. Without it, FR-3's equality check fails on every app that uses
  `/mcp` or `PYRONOVA_LOG` (gate finding G-4).
- `await_on_loop` (C9): the only way to deliver an async result to an asyncio loop without
  a bare foreign attach. It replaces a whole dependency (`pyo3-async-runtimes`) rather than
  adding one.

**Sibling designs** (dispositions in `real-engine-in-workers-impl.md` §2):
- `docs/arena-async-db-and-static.md` Design B: adopted (this is it).
- `docs/optimize-crud.md` / `ROADMAP.md:430` `state_bridge.rs`: replaced by C2.
- `docs/tpc-rearch.md:75,94` FFI contract: replaced by C5.
- `docs/design/polars-in-subinterpreters.md`: compatible. Update its `_bootstrap.py` line
  references with C7.

## Open questions and risks

- **Q-1: pydantic stub — DECIDED 2026-09-23: remove it.** Real pydantic reaches workers through
  auto-isolate, one copy per worker (§8.6).
- **Q-2 — DECIDED 2026-09-23: option (a)** (M4 review B2): raw `PyronovaApp()` scripts in workers. FR-4's
  registry is filled only by `Pyronova.__init__` (`app.py:190`), so every script that builds
  the raw engine fails worker startup with "no app": `examples/hello_subinterp.py:12`,
  `benchmarks/bench_subinterp.py:6`, `benchmarks/suite/servers/pyronova_subinterp.py:5` (used
  by `benchmarks/suite/runner.py:75`), `tests/test_c_extensions.py:5`,
  `tests/test_subinterp_hooks.py:4`.
  - **(a) Chosen:** in a worker, a `PyronovaApp` registers itself in `WORKER_APP` on its
    first route / hook / fallback registration (those pymethods take `slf: Bound<Self>`).
    FR-4 then counts "apps that registered anything". Raw-engine examples, benchmarks and
    tests keep working with no edits, and `Pyronova` needs no special call.
  - (b), not chosen: raw `PyronovaApp` unsupported in workers and the five files above
    migrated to `Pyronova`.
  - Why (a): it keeps the engine usable on its own and costs one check per registration,
    off the hot path.
- **Q-3 — APPROVED 2026-09-23** (M4 review B4): the three Layer-2 probe apps and their tests
  are rewritten at M4 activation as described in §9 ("Must change at activation"): detect a
  worker with `pyronova.engine._in_worker()`, drop the manual `ExtensionFileLoader` load,
  delete the M3 probe app and its two tests.
- **R-1: closure hooks now run in workers (§8.7).**
  - CORS: `_cors_before` returns the preflight response, and Rust's `apply_cors` uses
    `insert` (`handlers.rs:320-341`), so there are no duplicate headers. Verified in E2E-1.
  - The `enable_logging` hooks are registered after the seal, so they stay main-only, as
    today.
  - `enable_request_id` hooks (`observability.py:59-90`) were silently skipped in workers
    until now. They start working (E2E-1 case).
- **R-2: user scripts with import-time side effects** now execute real `Pyronova()`
  construction in N workers. That was already true of their own code, but `Pyronova()`
  itself now does real work (`app.py:188-204`). Its constructor must stay free of
  process-global side effects; add an assertion test.
- **R-3: CLOSED → C9.** Measured by the spike: `future_into_py` panics on
  `tokio-rt-worker` once workers exec the engine (`spike/R3-RESULTS.md`).
- **Fresh review (2026-09-23), remaining items resolved in this revision:** B1 → FR-2/FR-3,
  §8.1; B6 → FR-14, §8.4, E2E-7b; B7 → FR-13, §8.6, NFR-2/3, E2E-15; N1-N3 → C3/C5/C7,
  §7; N4-N5 → C5/C6, reuse map; N6 → C7 table, §10; N7 → §9 list; N8-N10 → E2E-4/6/10/11/13/14;
  N12 → FR-2, §8.7; N13 → C1, FR-17; N17 → FR-15; N18 → FR-16. No disagreements.
- **R-5 (new): FR-17's "differing value" warning** only catches worker-conditional config
  for the two setters that exist today. A future setter that writes process-global state
  must follow the same main-only rule; the attach allowlist doesn't cover this, so it is a
  review item, not a gate.
- **M4 readiness review (fresh auditor, 2026-09-23), resolved in rev3:** B1 → FR-2, §8.1;
  B2 → FR-4 + Q-2 (decided: (a)); B3 → FR-19, C3, §12, E2E-19; B4 → §9 + Q-3 (approved); B5 →
  FR-11, E2E-14/14b; N1 → C5 (a–g), §8.7; N2 → C3 (no fallback binding); N3 → C3, §7, §8.1;
  N4 → FR-6, C4; N5 → C4 table; N6 → C5, C7, §9; N7 → header, C3, C5, C6, C7, §8.5, reuse
  map; N8 → NFR-5, §12; N9 → FR-20, C7, E2E-20; N10 → FR-2, §8.7; N11 → NFR-2b/3b, E2E-15;
  N12, N13 → §8.7; N14 → E2E-11; N15 → C8, §7, §8.7, E2E-21. Every finding was checked
  against `8d89297` before resolving; none was disputed.
- **Pre-existing bug found by the M4 review, out of scope:** TPC never serves the fallback
  handler (`handlers/tpc.rs:100-106` returns 404 on a lookup miss). Track separately.
- **R-4: `thread_local! LOOP` (`handlers.rs:248-249`)** binds an asyncio loop to whichever
  interpreter first touched a thread. It must only ever be touched on main-side threads.
  Add a debug assertion that compares the interpreter id recorded at creation.
