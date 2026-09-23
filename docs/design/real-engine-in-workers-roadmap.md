# Roadmap — Real `pyronova.engine` in every worker

> Built from the gated design `real-engine-in-workers.md` and impl map
> `real-engine-in-workers-impl.md`, gate at 0 blocking (commit `9028756`, branch
> `design/layer2-gate`). 2026-09-23.
> **Caveat:** the gate audit was done by the writer (the fork could not spawn a fresh
> auditor; impl map §3 "Auditor note"). Re-run a fresh-auditor pass before M3 starts. If it
> changes the design, re-run this roadmap.

## Sequencing logic

- **Risk first, and the spike decides the order.** The moment a second interpreter
  executes the engine, every bare foreign-thread `Python::attach` and every `Py<T>` drop on
  a thread with no thread state panics. The spike showed this for the GIL bridge,
  WebSocket and `pyo3-async-runtimes` threads (`spike/R3-RESULTS.md`).
- So the fixes that are correct regardless of mock vs real land first (M0, M1). They are
  proven with a test app whose *own script* executes the real engine inside each worker,
  the way the spike did. That is user code, not a framework switch, so no shim ships.
- Activation (M4) is one atomic step, because a worker either runs the mock or the real
  package.

## Milestones

### M0: Main-side attach and drop discipline (highest risk)
- **Scope** (revised by the fresh review, B2–B5, B8, B9, N15, N16): C2's main handle
  (`MAIN`, captured in `engine()`; no run context), C4 (every site in the C4 table), FR-6,
  the FR-12 allowlist gate + E2E log panic-text scan (`tests/conftest.py`).
  - Every main-side thread that serves many requests (bridge, `spawn_blocking` in every
    mode) keeps one main tstate for its life (thread-local, TLS destructor).
  - WebSocket: no Python on the TPC/Tokio thread; lookup + handler on the connection
    thread, attached to main for the connection's life.
  - `LoopGuard` attaches to the interpreter recorded at loop creation; R-4 debug assertion.
  - Bridge threads joined after the TPC threads; `run()` keeps the last `Arc<RouteTable>`.
  - Shared `feature_server` fixture: log to a file, stop with SIGINT, scan the log.
- **Depends:** —
- **Verification:**
  - E2E-8 over `PYRONOVA_TPC=1` and `=0`: sync/async `gil=True` + WebSocket + `/metrics`
    under concurrent load while each worker's script execs the real engine; 0 non-2xx,
    WebSocket echoes, exit 0, no `panicked at` / fork panic text in the log, SIGINT
    included. Plus 3 concurrent TestClient servers in one process.
  - E2E-9: the allowlist gate catches a seeded bare attach.
  - The existing suite stays green on macOS + Linux.
- **Shippable:** yes. There is no user-visible change, and it removes the latent
  wrong-interpreter hazard for any future multi-interpreter load of the engine.

### M1: Async DB resolver (C9)
- **Scope:** C9 `await_on_loop`; the `*_async` methods move to it; `pyo3-async-runtimes`
  is removed from `Cargo.toml` (success criterion); FR-9 (`*_async` on main).
- **Depends:** M0 (uses `InterpreterHandle` + the panic-text scan).
- **Verification:**
  - E2E-8, the async-DB parts: a `gil=True` async DB route plus the main Python thread
    awaiting `fetch_all_async`, with workers exec'ing the engine. The result arrives with
    no panic text.
  - `tests/test_db_pg.py` async cases (`:169,:194,:314`) green with the PG job.
- **Shippable:** yes. Main-interpreter async DB is unchanged for users and has one less
  dependency.

### M2: `PgPool` safe from any thread (C6) + Postgres in CI
- **Scope:** C6. Sync methods use `run_on_db_rt` (moved into `db.rs`); the `PgCursor`
  receiver becomes std/crossbeam; FR-9 (sync + `fetch_iter`). Infra: a `services:
  postgres` job in `ci.yml`, so `test_db_pg.py` and `test_db_subinterp.py` stop being
  skipped.
- **Depends:** — (can run in parallel with M0/M1).
- **Verification:**
  - `test_db_pg.py` and `test_db_subinterp.py` (still the C bridge) green in CI.
  - A new unit-level case: `fetch_iter` iterated from inside a Tokio `current_thread`
    context does not panic.
- **Shippable:** yes. It fixes a latent nested-runtime hazard; CI gains DB coverage.

### M3: Registration seal and worker-app registry (inert)
- **Scope:**
  - C3's `_seal_registrations` + the `add_route` post-seal `gil` assertion.
  - C3's `_register_worker_app` / `WORKER_APP` (a no-op on main).
  - C8's `run()` reordering (seal → `_is_worker` return → run-time registrations) and the
    `__init__` call.
  - FR-2.
  - C2's shared state: `init_in_sub_interp` sets a per-interpreter cell with the running
    app's map before the script executes; `PyronovaApp::new` / `SharedState::new` read it
    when not on main.
- **Depends:** M0 (C2 main handle).
- **Verification:**
  - Main-interpreter unit tests: a post-seal non-gil route raises.
  - `/mcp` and `PYRONOVA_LOG=1` still work (E2E-1 subset).
  - The existing suite stays green.
- **Shippable:** yes. There is no behaviour change while workers still run the mock.

### M4: Activation, workers run the real package
- **Scope:** everything that flips the worker from mock to real, as one change:
  - C3 `bind_routes` + index-based `call_handler` (FR-3, FR-4).
  - C5 `worker_api.rs` (`_worker_recv/_worker_send/_worker_app_handler/_worker_app_hooks`,
    `emit_python_log(worker_id)`) and the `_async_engine.py` rewrite (FR-7, FR-8).
  - C7 slim `_bootstrap.py`, with the pydantic stub removed (Q-1 decided) and the
    `pyronova` isolate refusal (FR-10, FR-11).
  - C1 stale-comment cleanup; FR-1, FR-5 end-to-end.
  - Delete the raw cfuncs and `bridge/db_bridge.rs`.
  - Rewrite `tests/test_ffi_panic_safety.py` as approved by the user on 2026-09-23. Any
    other red test is reported before it is changed.
  - FR-12 surface-parity test.
- **Depends:** M0, M1, M2, M3.
- **Verification:** E2E-1, E2E-2, E2E-3, E2E-4, E2E-5 (incl. worker `fetch_iter`), E2E-6,
  E2E-7, E2E-10 and E2E-11 on macOS + Linux, plus E2E-8/9 re-run against the real
  activation.
- **Shippable:** yes. This is the user-visible release: real `SharedState`/DB/`model=`
  validation in workers, and headers on the async path. Release notes are §8.7.

### M5: Hardening, performance and docs
- **Scope:**
  - NFR-1…NFR-6 measured on a quiet bluewhale: `bench-compare` (load avg < 3), worker
    init time, RSS per worker, grill W=16 180 s, 20× graceful SIGINT.
  - E2E-12.
  - Doc reconciliation (impl map §2):
    - Mark `docs/optimize-crud.md` state bridge, `ROADMAP.md:430` and
      `docs/tpc-rearch.md:75,94` superseded.
    - Update `docs/design/polars-in-subinterpreters.md` `_bootstrap.py` line refs,
      `docs/logging-design*.md:180`, `docs/subinterp-c-extension-*`, and `CLAUDE.md`
      architecture lines (mock injection / C-FFI bridge).
  - CHANGELOG, then release per `.claude/commands/release.md`.
- **Depends:** M4.
- **Verification:** E2E-12 + the NFR thresholds (§4) + the release runbook's two-platform
  gate.
- **Shippable:** yes (the release).

**Removed or deferred by the gate (no milestone):**
- R-3 as a "measure later" item was closed into C9 (M1).
- `*_async` in workers stays fail-closed (a follow-up, not a milestone).
- The planned `src/bridge/state_bridge.rs` (`optimize-crud.md`, `ROADMAP.md:430`) is
  superseded by C2 and gets no milestone.

## Tracker reconciliation (proposed, NOT executed; needs user approval)

State read on 2026-09-23 (`gh issue list --repo leocaolab/pyronova --state all`;
`gh api repos/leocaolab/pyronova/milestones`): the only issue is #1 (closed, grill crash).
There are no milestones and nothing to align or close.

| Milestone | Proposed tracker action |
|---|---|
| (all) | `gh api repos/leocaolab/pyronova/milestones -f title="Layer 2: real engine in workers"` |
| M0 | new issue "Layer 2 M0: explicit-interpreter attach/drop on main-side threads", body = M0 scope + E2E-8/9 + spike link |
| M1 | new issue "Layer 2 M1: replace future_into_py with interpreter-generic resolver; drop pyo3-async-runtimes" |
| M2 | new issue "Layer 2 M2: PgPool safe from Tokio contexts (run_on_db_rt, PgCursor channel) + Postgres CI job" |
| M3 | new issue "Layer 2 M3: registration seal + worker-app registry (inert)" |
| M4 | new issue "Layer 2 M4: activate real pyronova package in workers (remove mocks, C-FFI, pydantic stub)" |
| M5 | new issue "Layer 2 M5: NFR measurement, doc reconciliation, release" |
| ROADMAP.md:430 "SharedState C-FFI bridge" | edit to "superseded by Layer 2 C2 (docs/design/real-engine-in-workers.md)" |
