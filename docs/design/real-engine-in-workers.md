# Design — Real `pyronova.engine` in every sub-interpreter worker ("Layer 2")

> Status: design, 2026-09-23. Implements: the Layer 2 item deferred from v2.7.1
> (replace `_bootstrap.py`'s mock engine; see "Design B" in
> `docs/arena-async-db-and-static.md:67-79`, whose PyO3 0.28 blockers no longer hold).
> Baseline: v2.7.2 (`c050396`), PyO3 from `leocaolab/pyo3` tag `subinterp-2026-09-23`
> (`Cargo.toml:81-88`, `Cargo.lock` → `0e008b3e`). Minimum Python 3.13.
> Gate: `impl-design` passed at 0 blocking on 2026-09-23. The impl map and audit record are
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
| FR-2 | `Pyronova.run()` first calls `self._engine._seal_registrations()`, which marks the end of the script's registrations. In a worker it then returns before any side effect: `init_logger`, `/mcp` auto-registration, the `enable_logging` auto-enable and reload handling all run after the seal and only on main (today they run before the `_is_worker()` check, `app.py:1086-1174`). Every route registered after the seal must be `gil=True` (asserted in `add_route`). | F1, F2 |
| FR-3 | After the script runs, the worker takes its handlers, before hooks and after hooks from the worker's own `PyronovaApp` route table. The worker's sealed prefix must equal main's sealed prefix, checked per index on `(method, path, gil)` plus the before/after hook counts. A mismatch fails worker startup with both lists in the error. Post-seal hooks (e.g. the `enable_logging` hooks) run only on main paths, as today. | F2 |
| FR-4 | Exactly one `Pyronova` app per script. Zero or more than one registered app in a worker fails startup with a message naming the count. | F2 |
| FR-5 | `app.state` and `SharedState` obtained in any interpreter during a server run refer to the running app's single `Arc<DashMap<String, Bytes>>`. | F3 |
| FR-6 | A bare `Python::attach` **or a `Py<T>` drop** on a thread with no thread state never runs in production code, and no main-interpreter object is touched on a thread bound to a sub-interpreter. Main-side threads (GIL bridge, WebSocket, `spawn_blocking` GIL path in every mode, `LoopGuard` drop, DB async resolver) attach through `main_attach` / `attach_to` (C4). Every thread that serves many requests keeps one main thread state for its life. Threads drop every `Py<T>` they own (including `FrozenRoutes` clones) while attached before exiting. `run()` joins the bridge threads before it returns and keeps the last `Arc<RouteTable>`, dropped on main while attached. | F4 |
| FR-7 | The async worker loop uses `pyronova.engine._worker_recv(worker_id, pool_id)` and `pyronova.engine._worker_send(worker_id, pool_id, req_id, response)`. `response` may be a `Response`, and its headers reach the client. | F5 |
| FR-8 | Worker logging uses `pyronova.engine.emit_python_log` (`logging.rs:203-233`) and carries the real worker index. | F5 |
| FR-9 | `PgPool.fetch_all / fetch_one / fetch_scalar / execute` and `fetch_iter` iteration work from a TPC worker thread. They release the GIL during I/O and never call `Runtime::block_on` or Tokio `blocking_recv` from inside a Tokio context. `*_async` called in a worker raises `NotImplementedError` with the existing message. `*_async` on main resolves through C9. | F6 |
| FR-10 | `_bootstrap.py` contains only the logging-handler install, the GC policy and the isolation machinery (`_bootstrap.py:660-1160`). The injected globals `_Request`, `_Response`, `_pyronova_emit_log`, `_pyronova_db_*`, `_pyronova_recv`, `_pyronova_send` and `_pyronova_pool_id` are gone. | F7 |
| FR-11 | `pyronova` / `pyronova.engine` are never cloned by `app.isolate` or reactive auto-isolate. Requesting it is an error: one shared copy is required, because the engine's process-global statics must be one instance. | F1 |
| FR-12 | A CI test imports `pyronova` in a real worker and compares its public surface with main's (names in `pyronova.__all__`, `Pyronova` public methods, `pyronova.engine` classes and functions). A CI check fails on any `Python::attach(` / `Python::with_gil(` in `src/` outside an allowlist. Every E2E server log is scanned for the fork's two panic texts (`Python::attach was called on a thread that has no Python thread state`, `a Py<T> was dropped on a thread that has no Python thread state`); any hit fails the test (`tests/conftest.py` `fork_panic_lines`; the shared `feature_server` fixture now stops servers with SIGINT and scans their full log, shutdown included). | F8 |

**Non-functional** (measured on bluewhale unless noted)

| # | Requirement | Threshold | Feature |
|---|---|---|---|
| NFR-1 | Hot-path throughput | `just bench-compare` within 3% of v2.7.2, run on a quiet box (load avg < 3) | F2, F5 |
| NFR-2 | Worker startup | per-worker init time ≤ v2.7.2 + 50 ms (median of 16 workers, warm isolate dir) | F1 |
| NFR-3 | Memory | RSS per worker ≤ v2.7.2 + 5 MB after startup; `test_subinterp_memory_regression.py` 9/9 on Linux | F1, F3 |
| NFR-4 | Stability | grill soak W=16, `wrk -c128`, 180 s: 0 non-2xx, 0 crashes, RSS flat ±5% | all |
| NFR-5 | Teardown | 20 graceful SIGINT runs with 16 workers: 0 aborts and 0 fork panic texts in the logs (with `os._exit` present; see §12) | F4 |
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
| Process-wide run context | ➕ | new `src/run_context.rs` (§6 C2) |
| Worker API functions | ➕ | new `src/python/worker_api.rs` (§6 C5) |
| CI surface-parity + attach-allowlist gates | ➕ | new tests (§9) |
| Postgres in CI for worker DB tests | ➕ | `ci.yml` has no PG service; `test_db_subinterp.py` is always skipped (`tests/test_db_subinterp.py:23-25`). Add a `services: postgres` job. |

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
  - `_seal_registrations(&mut self)` pymethod: stores `sealed: Option<(routes, before,
    after)>` on `RouteTable` (`router.rs:34`). `add_route` rejects a non-`gil` route after
    the seal.
  - `fn route_signature(t: &RouteTable) -> Vec<(Method, String, bool)>` over the sealed
    prefix, plus hook counts.
  - `SubInterpreterWorker::bind_routes(&mut self, py, main: &RouteTable) -> PyResult<()>`
    replaces the globals lookup (`worker.rs:236-246`). It stores
    `Vec<Py<PyAny>>` handlers, `Vec<Py<PyAny>>` before hooks, `Vec<Py<PyAny>>` after
    hooks and `Option<Py<PyAny>>` fallback, **indexed like main**.
- **Interface change:** `call_handler(&mut self, handler_name: &str, before: &[String],
  after: &[String], …)` (`worker.rs:847`) becomes `call_handler(&mut self, idx: usize, …)`.
  Hooks come from the worker's own vectors. Callers:
  - `handlers/tpc.rs:313-331`
  - `pool.rs:435-452`
  - `tpc.rs::fire_gc` (unchanged)

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
  | `db.rs:585,614,644` + `future_into_py` | pyo3-async-runtimes Tokio threads | replaced by C9 in M1; allowlisted until then |
  | `main_bridge.rs` bridge threads | bridge threads at shutdown | `JoinHandle`s kept; order (N15): TPC threads joined → their bridge `Arc`s dropped → `MainInterpBridge::shutdown_join` drops the `Sender` and joins; each thread closes its loop and drops its `routes` clone inside `main_attach`, then its TLS destructor releases the main tstate before `join` returns |
  | `worker.rs:421`, `db_bridge.rs:131` | worker thread with its tstate current | **kept** as `Python::attach` (allowlisted): a re-entrant attach on the thread's current tstate. `assume_attached()` would not register the attach with PyO3, so a `Py<T>` dropped inside would be deferred instead of decref'd |

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
  - `parse_sky_response` (`worker.rs:649-840`) so the async path returns headers.
  - `emit_python_log` (`logging.rs:203-233`).
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
  fn _worker_app_hooks(py: Python<'_>) -> PyResult<(Vec<Py<PyAny>>, Vec<Py<PyAny>>)>; // sealed before/after
  ```
  These replace `HANDLER_NAMES` + `globals().get(name)` (`_async_engine.py:52-57`,
  `pool.rs:519-528`). That lookup disappears with C3 (gate finding G-7).
  `emit_python_log` gains `worker_id: Option<usize>`. PyO3 turns a Rust panic into
  `PanicException`, which replaces `ffi_catch_unwind` (`ffi.rs:88-116`).
- **Deleted:**
  - `pyronova_recv_cfunc`, `pyronova_send_cfunc`, `pyronova_emit_log_cfunc` and their
    `PyMethodDef` registration (`worker.rs:129-148`, `pool.rs:542-666`).
  - `bridge/db_bridge.rs` (C6).

### C6: `PgPool` usable from workers
- **Reuses:** `run_on_db_rt` (`db_bridge.rs:42-78`), moved into `db.rs`; `PG_POOL` /
  `PG_RUNTIME` (`db.rs:43-44`); `unpack_args` from `db_bridge.rs`.
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
- **Keeps** (moved as-is):
  - The logging handler (`_bootstrap.py:22-94`), which now calls
    `pyronova.engine.emit_python_log(..., worker_id=WORKER_ID)`.
  - The GC policy (`_bootstrap.py:143-156`).
  - Isolation (`_bootstrap.py:660-1160`), including `_iso_init_here` and the reactive
    import hook.
- **Deletes:** `_bootstrap.py:158-657`, i.e. all mocks, the inline `cached_json`, cookies,
  uploads and the pydantic stub (see Q-1).
- **Adds:**
  - `WORKER_ID` is set by Rust as a global before exec, as `_async_engine.py` already
    does (`pool.rs:519-528`).
  - A guard: `_iso_*` refuses `pyronova` (FR-11).
- **Execution model:** unchanged. One `PyRun_String` of bootstrap + script in a fresh
  globals dict (`worker.rs:121-233`). Two differences: `__name__` is set explicitly to
  `"__pyronova_worker__"`, and the handler lookup is replaced by C3.

### C8: `Pyronova` Python class changes (`python/pyronova/app.py`)
- `run()`: the first line becomes `self._engine._seal_registrations()`, followed by
  `if _is_worker(): return` (moved from `app.py:1174`). Run-time registrations (`/mcp`,
  `enable_logging` auto-enable) stay after it (FR-2).
- `__init__` (`app.py:188`): call `self._engine._register_worker_app()` (C3).
- `isolate()` (`app.py:348-372`): in a worker it is now the real method. It keeps only
  recording the env var, which is harmless because the bootstrap has already acted on it.
- Remove the `__name__`-rebinding workaround comments (`app.py:412-415,558-563`), which
  are no longer needed. Keep `shim.__name__ = fn.__name__` for tracebacks.

## 7. Interfaces with other modules

| Direction | Module | Symbol / signature | Purpose |
|---|---|---|---|
| lib.rs `engine()` → run_context | C2 | `run_context::capture_main(py)` (no-op off main) | record the main interpreter (M0) |
| worker.rs `init_in_sub_interp` → worker cell | C2 | per-interpreter cell with the app's `Arc<DashMap<String, Bytes>>`, set before the script executes | shared state for workers (M3) |
| PyronovaApp::new → run_context | C2 | reads `shared_state` when not main | FR-5 |
| worker.rs → engine (Python) | C3 | `WORKER_APP.get(py) -> Option<&Py<PyronovaApp>>` | handler table source |
| worker.rs ← handlers/tpc.rs, pool.rs | C3 | `call_handler(&mut self, idx: usize, method, path, params, query, body, headers, client_ip) -> Result<SubInterpResponse, String>` | index-based call |
| main_bridge.rs, handlers.rs, websocket.rs → run_context | C4 | `main_attach(f)`, `attach_to(interp, f)`, `main_interp()` | explicit main attach |
| _async_engine.py → engine | C5 | `_worker_recv(worker_id, pool_id)`, `_worker_send(worker_id, pool_id, req_id, response)`, `_worker_app_handler(idx)`, `_worker_app_hooks()` | async bridge |
| app.py → engine | C3/C8 | `PyronovaApp._register_worker_app()`, `PyronovaApp._seal_registrations()` | worker app + seal boundary |
| db.rs → asyncio (any interpreter) | C9 | `await_on_loop(py, fut)`: `loop.create_future()`, `InterpreterHandle::current`, `call_soon_threadsafe` | async DB without foreign attach |
| _bootstrap.py → engine | C5 | `emit_python_log(level, name, message, pathname, lineno, worker_id=None)` | logging |
| db.rs → db runtime | C6 | `run_on_db_rt<F: Future + Send + 'static>(fut) -> Result<T, &'static str>` | non-nested blocking |
| tests → engine | F8 | `pyronova.engine.__all__`-style surface dump | parity test |

## 8. Main algorithms

### 8.1 Worker startup
```
main (PyronovaApp.run, main interpreter, GIL held):
 1. freeze routes (app.rs:389-418) → FrozenRoutes F
 2. (M3) each worker's per-interpreter shared-state cell is set in init_in_sub_interp (C2); MAIN was captured at engine() exec
 3. for i in 0..n: SubInterpreterWorker::new(i, script, &F)      (app.rs:1199 / pool.rs:252)
 worker i (new interpreter, own GIL, created on main thread as today worker.rs:70-104):
 4. globals = {__builtins__, __name__="__pyronova_worker__", __file__=script, WORKER_ID=i}
 5. exec(bootstrap_slim + script, globals)
      - `from pyronova import Pyronova` → real package → engine module exec (per-interp, no global side effects)
      - `app = Pyronova()` → `_register_worker_app` sets WORKER_APP (error if already set)
      - decorators register into app's own RouteTable (real add_route, real model= wrap)
      - app.run() → `_seal_registrations()`, then returns (_is_worker)
 6. app = WORKER_APP.get(py) or fail "script created no Pyronova app"; not sealed → fail
    "script never called app.run()" (a worker needs the seal)
 7. sig_w = route_signature(sealed prefix of app.routes); sig_m = route_signature(sealed prefix of F)
    if sig_w != sig_m: fail with both lists (first differing index highlighted)
 8. bind handlers/hooks/fallback by index; cache json dumps, loop, gc (worker.rs:263-356 unchanged)
 9. PyEval_SaveThread (worker.rs:360)
```
Invariants:
- A worker never serves a request unless its sealed table equals main's by index.
- Post-seal routes are `gil=True`, so a worker is never asked for an index beyond its
  sealed prefix.
- `MAIN` is set before any worker exists (the engine executed on main first).

Edge cases:
- A script that registers routes conditionally on `PYRONOVA_WORKER`: this fails at step 7
  with a readable diff. That is intended, because it would have silently misrouted before.
- A script with `if __name__ == "__main__": app.run()`: `run` isn't called in the worker,
  which is fine.
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
  (`pool.rs:514-518`).

### 8.5 DB from workers
`py.detach(|| run_on_db_rt(async move { pool.fetch_all(...) }))`. The future runs on the
2-thread `pyronova-db` runtime (`db.rs:47-67`). The worker thread waits on a
`sync_channel` and never enters a Tokio context. Parameters are converted to owned Rust
values before `detach`, reusing `unpack_args` from `db_bridge.rs`. Rows are converted
back to Python after reattaching, so no Python object crosses interpreters (the invariant
at `db_bridge.rs:24-28` is kept).

### 8.6 pydantic in workers (decision needed, Q-1)
Removing the stub has a consequence:
- `import pydantic` in a worker reaches `pydantic_core` (upstream PyO3, declares
  not-supported).
- The load-time override plus the reactive auto-isolate clone it per worker. That path
  is measured working: 432k requests (`docs/subinterp-ecosystem-isolation.md:211-216`).
- `model=` then validates for real in workers. Today it silently passes an empty instance
  (`_bootstrap.py:552-571`: `model_validate_json` returns `cls()`).
- Cost: one pydantic copy per worker.

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

## 9. Integration / E2E tests

Every test runs the server in a subprocess (`tests/conftest.py:69-112`), because a
sub-interpreter permanently disables `PyGILState_Check` in its process.

| Test | CUJ | Setup → Action → Assertion |
|---|---|---|
| E2E-1 | CUJ-1 | existing `feature_server_factory` suites (`test_cookies_e2e`, `test_cors_e2e`, `test_routing_e2e`, `test_uploads_e2e`) in `subinterp` mode + `test_capi_hygiene.py` → unchanged assertions pass; new cases assert: one CORS header value (no double application); `enable_request_id` echoes `x-request-id` from a worker route; `PYRONOVA_LOG=1` still starts (post-seal hooks main-only) |
| E2E-2 | CUJ-1 | `test_isolate.py` (all 5) + grill W=4 smoke → pass |
| E2E-3 | CUJ-1 | script registering route only `if os.environ.get("PYRONOVA_WORKER")` → server exits non-zero; stderr contains both route lists |
| E2E-4 | CUJ-2 | 4 workers, route `app.state.incr("n")`; 400 requests → main-side `gil=True` route reads `n == 400` |
| E2E-5 | CUJ-3 | PG service in CI; `test_db_subinterp.py` with TPC default → 5 tests pass (today always skipped) + a new `fetch_iter` iteration case from a worker route → all rows, no panic |
| E2E-6 | CUJ-3 | worker route calls `pool.fetch_all_async` → 500 with body containing `NotImplementedError` message; server stays up |
| E2E-7 | CUJ-4 | `PYRONOVA_TPC=0`, async route returns `Response(headers={"x-a":"1"})` and a before hook sets a header → both headers present |
| E2E-8 | CUJ-5 | `tests/test_layer2_main_side.py`, **parametrized over `PYRONOVA_TPC=1` and `=0`** (review B8: pool mode has its own sites). App (`tests/_l2_main_side_app.py`) whose own script execs the real engine in each worker; worker route + `gil=True` sync/async routes + WebSocket echo + `/metrics` under concurrent load, then SIGINT → 0 non-2xx, WS echoes, exit 0, and the log has no `panicked at` line and neither fork panic text (including at shutdown). M1 adds the `gil=True` async DB route + a main Python thread awaiting `fetch_all_async`. Plus `test_concurrent_in_process_servers`: 3 TestClient servers at once in one process (B4) |
| E2E-9 | CUJ-5 | `tests/test_attach_allowlist.py`: the allowlist gate (FR-12) reports a bare `Python::attach` seeded into a copy of `src/` |
| E2E-10 | CUJ-6 | surface parity: worker route returns `sorted(dir(pyronova))`, `pyronova.__all__`, `sorted(m for m in dir(pyronova.Pyronova) if not m.startswith("_"))`, `dir(pyronova.engine)` → equals main's |
| E2E-11 | CUJ-6 | worker `logging.getLogger().info("x")` with 4 workers → log lines show worker ids {0,1,2,3} |
| E2E-12 | CUJ-7 | `test_subinterp_memory_regression.py` (Linux, 9 tests) + grill W=16 180 s + 20× graceful SIGINT → NFR-3/4/5 |

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

## 10. Success criteria
- [ ] FR-1 … FR-12 met; `rg '_pyronova_(recv|send|emit_log|db_)|_mock_engine|types.ModuleType\("pyronova' src python` returns nothing.
- [ ] `pyo3-async-runtimes` removed from `Cargo.toml`; C9 serves every `*_async`.
- [ ] No E2E server log contains either fork panic text (FR-12 scan), including across shutdown.
- [ ] NFR-1 … NFR-6 thresholds hit (§4), with bench numbers from a quiet bluewhale.
- [ ] E2E-1 … E2E-12 pass on macOS and Linux; CI gains the PG job and the parity/attach gates.
- [ ] `_bootstrap.py` ≤ ~560 lines (isolation + logging + GC), down from 1160.

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
- **GIL bridge.** Persistent tstate per bridge thread (§8.3). A per-call
  `InterpreterHandle::attach` on a cold thread would create and destroy a tstate per
  request (fork `interpreter_handle.rs:88-110`). That is acceptable only for pool-mode
  `spawn_blocking`.
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
  - Main bridge thread without a run context: `main_attach` panics with an explanation.
    That is a programming error, not user input.
- **Teardown:** worker modules are torn down by `Py_EndInterpreter` as today
  (`ffi.rs:747-753`). The engine module object is per interpreter
  (`PerInterpreterCell<Py<PyModule>>`), so ending a worker frees only that worker's
  module. The last `Arc<RouteTable>` is `run()`'s own clone, dropped on main while
  attached after all workers and bridge threads have ended (after `InterpreterPool` drop,
  `pool.rs:128-184`, the TPC thread join and `MainInterpBridge::shutdown_join`). The `os._exit` on graceful stop
  (`app.py:1245-1253`) stays until the item-3 investigation (branch
  `fix/singlephase-finalize`) shows finalization is clean. This design must not depend on
  either outcome.
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
| `end_worker_interpreter` | `src/python/ffi.rs:747-753` | unchanged teardown |
| `WORKER_STATES`, `WorkerState` | `src/python/ffi.rs:20-63` | backing for `_worker_recv/_send` |
| recv/send bodies | `src/python/ffi.rs:126-412` | moved into `worker_api.rs` pyfunctions |
| `parse_sky_response`, `parse_result` | `src/python/worker.rs:432-504,649-840` | response mapping for `_worker_send` |
| `build_request` | `src/python/worker.rs:389-426` | unchanged |
| `call_handler` | `src/python/worker.rs:847-1059` | signature → index-based |
| `emit_python_log` | `src/logging.rs:203-233` | worker logging (+ `worker_id`) |
| `run_on_db_rt` | `src/bridge/db_bridge.rs:42-78` | moved to `db.rs`, used by `PgPool` sync methods |
| `unpack_args` | `src/bridge/db_bridge.rs` | param conversion before `detach` |
| `db::runtime`, `PG_POOL` | `src/db.rs:43-46` | unchanged; also C9's runtime |
| `MainInterpBridge::spawn` | `src/bridge/main_bridge.rs:111-195` | add persistent main tstate per thread |
| `call_handler_with_hooks` | `src/handlers.rs:521-714` | attach via `main_attach` |
| isolation machinery | `python/pyronova/_bootstrap.py:660-1160` | kept verbatim + `pyronova` refusal |
| logging handler / GC policy | `python/pyronova/_bootstrap.py:22-94,143-156` | kept; call real `emit_python_log` |
| `_async_engine.py` loop | `python/pyronova/_async_engine.py:125-226` | kept; recv/send/handler lookup swapped |
| `_is_worker` | `python/pyronova/app.py:35-37` | moved to top of `run` |
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
- **R-4: `thread_local! LOOP` (`handlers.rs:248-249`)** binds an asyncio loop to whichever
  interpreter first touched a thread. It must only ever be touched on main-side threads.
  Add a debug assertion that compares the interpreter id recorded at creation.
