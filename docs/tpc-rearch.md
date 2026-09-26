# TPC Re-architecture (v2.3)

> **Superseded in part (2026-09, Layer 2, `docs/design/real-engine-in-workers.md`):** the C-FFI pieces this plan keeps — the DB bridge (`src/db_bridge.rs`, §"what stays") and the `_pyronova_recv` / `_pyronova_send` / `_pyronova_pool_id` registrations (the bootstrap step and "Delete" list below) — are gone. Workers import the real `pyronova.engine`; `PgPool` is used directly, and the async engine calls `pyronova.engine._worker_recv` / `_worker_send`. The TPC design itself is unchanged.


Target: replace tokio multi-threaded + work-stealing + sharded channels with a pure Thread-Per-Core design. Goal is absolute linear scaling to 128+ core NUMA hosts with zero cross-core cache-line traffic on the hot path.

## The insight that forced this

v2.1.5's sharding trio (P2C + per-worker queues + busy flag) was engineered to fix MPMC contention in tokio's work-stealing model. On 7840HS (UMA, 8 cores) it worked. On Arena's TR 3995WX (NUMA, 8 CCDs) it collapsed — every P2C probe was a cross-CCD atomic load (~80-120 ns on Infinity Fabric). 300k rps × 4 atomic probes × 500 ns mean latency = 60% of dispatch budget spent on fabric round-trips.

Sharding treated the symptom (one queue bottlenecks at 64 cores). The real illness is **work-stealing itself** — tokio moves tasks between OS threads for IO, but the Python sub-interpreters (the actual compute work) are pinned to specific threads. The scheduler's flexibility isn't something we *use*; it's something that *hurts* us, because every task reassignment takes the request away from the core whose L1/L2 still has its cache-warm state.

TPC deletes the problem: one OS thread owns one CPU core, one tokio runtime, one sub-interpreter. No dispatch queue. No scheduler. The kernel load-balances new TCP connections via SO_REUSEPORT and that is the *entire* load-balancer.

## Target architecture

```
                    :8080
             ┌────────┴────────┐
             │   Linux kernel  │   SO_REUSEPORT hash on (src_ip, src_port)
             │   accept-hash   │
             └─┬───┬───┬─────┬─┘
               │   │   │     │
   ╔═══════════╪═══╪═══╪═════╪════════════╗
   ║  Core 0   ║ 1 ║ 2 ║ ... ║ Core N-1   ║
   ║ ─────────┤    │    │    │  ────────  ║
   ║ listener │    │    │    │ listener   ║  ← std::net::TcpListener bound with SO_REUSEPORT
   ║ current- │    │    │    │ current-   ║  ← tokio::runtime::Builder::new_current_thread()
   ║ thread   │    │    │    │ thread     ║
   ║ runtime  │    │    │    │ runtime    ║
   ║ sub-     │    │    │    │ sub-       ║  ← Py_NewInterpreter() once per thread, lives forever
   ║ interp   │    │    │    │ interp     ║
   ╚══════════╩════╩════╩═════╩════════════╝
                                  │
                               gil=True routes (cold path)
                                  │
                                  ▼ tokio::sync::mpsc
                      ┌─────────────────────┐
                      │   Main-interp       │  ← one dedicated thread
                      │   thread            │  ← handles numpy / pandas / legacy C-ext
                      │   (MainThreadState) │
                      └─────────────────────┘
```

One OS thread per CPU core, pinned via `core_affinity`. Each thread:
1. Owns its own `std::net::TcpListener` bound with `SO_REUSEPORT` — kernel distributes new connections via a 4-tuple hash, so each thread's accept queue is independent and contention-free.
2. Runs `tokio::runtime::Builder::new_current_thread()` — single-threaded executor, no work-stealing.
3. Runs Python in a sub-interpreter created by `Py_NewInterpreter()` once at thread startup. The `PyThreadState` is created, attached, and never crosses threads.
4. Handles accept → parse → call handler → send response **sequentially**. A slow handler on thread 3 only blocks thread 3; kernel routes new connections to threads 0-2, 4-N.

## What gets deleted

Every line of code listed below stops being used.

| Subsystem | What goes |
|---|---|
| Work dispatch | `InterpreterPool::submit`, `InterpreterPool::new`, all sharding state (`sync_work_txs: Vec<Sender>`, `sync_worker_busy: Vec<Arc<CachePadded<AtomicBool>>>`), P2C xorshift, shard capacity constants |
| Channels | `crossbeam_channel::bounded` per-worker queues for sync + async pools (kept only for gil=True fallback as a single `tokio::sync::mpsc`) |
| Admission | `submit_semaphore: Arc<tokio::sync::Semaphore>` — replaced by per-thread local `Cell<u32>` counter |
| Zombie / cross-pool theft | Pool-id stamping (`_pyronova_pool_id`), cross-pool guards in `_pyronova_recv` / `_pyronova_send`, zombie timeout watchdog — physically impossible in TPC because work cannot leave its thread |
| tstate rebinding | `rebind_tstate_to_current_thread` — stays put forever, no rebind needed |
| Dual pool | Separate sync + async worker pools (`async_work_txs`, async engine script injection) — an `async def` handler becomes a normal `LocalSet::spawn_local` call inside the sub-interp's current-thread runtime. *Not done: TPC keeps the async pool for `async def` routes (decision Q1, see Line 3).* |

Estimated line count to delete: ~2,000 lines (primarily `src/interp.rs`).

## What must stay

- `PgPool` + `PG_POOL: OnceLock<sqlx::PgPool>` + `PG_RUNTIME`: one process-wide DB pool accessed by all threads via `Arc`. The C-FFI bridge (`src/db_bridge.rs`) is unchanged — every worker thread's sub-interp has the 4 capsules injected.
- `SharedState` (`Arc<DashMap<String, ...>>`): cross-worker state is the explicit feature; its lock-freedom under 64 cores depends on DashMap's internal sharding, which is independent of our dispatcher.
- `add_fast_response`: per-thread fast lookup — already zero-dispatch.
- `static_fs`: already bypasses Python entirely; per-thread preload cache (Part 2 of arena-async-db-and-static.md) still applies.
- Main-interp gil=True bridge: single MPSC `(tx, rx)` pair, producers = N TPC threads, consumer = 1 main-interp thread. Volume is cold-path only (numpy/pandas/legacy compat routes).

## What must be reworked

- `PyronovaApp::run_subinterp` / `run_gil`: completely rewritten around per-core thread spawn instead of one multi_thread tokio runtime with shared worker pool.
- `handle_request_subinterp` in `handlers.rs`: instead of `pool.submit(work)` + cross-thread channel + worker dequeues, it becomes a direct call into the current thread's sub-interpreter via the existing `_pyronova_recv` / `_pyronova_send` FFI (now in-thread instead of cross-thread). The FFI contract stays; the caller and callee just run on the same thread.
- Async handlers (`async def`): today they run on a separate "async pool" with a tokio-on-sub-interp bridge. In TPC they run as `spawn_local` tasks on the same per-thread tokio runtime that's running the accept loop, sharing the same sub-interp. Much simpler — LocalSet handles the single-thread scheduling naturally. *Superseded (decision Q1): the TPC server sends `async def` routes to the async worker pool; see Line 3.*
- Graceful shutdown: one `CancellationToken`, but fanned to N threads instead of one runtime. Each thread drains its own accept loop + in-flight handlers, then joins.

## Phase plan

### Phase 1 — TPC scaffolding behind a flag (1 PR, ~400 lines)

Goal: a `PYRONOVA_TPC=1` env flag (or `app.run(tpc=True)`) that spawns N pinned OS threads, each with its own current_thread runtime + SO_REUSEPORT listener. Handlers still go through the *old* `InterpreterPool` submit path — we're only validating that the new accept layer serves traffic correctly.

- New file: `src/tpc.rs` with `run_tpc(app: &PyronovaApp, ...)`.
- `src/app.rs` branches on the flag at `run()` entry.
- Each thread: `core_affinity::set_for_current()` → build current_thread rt → bind reuseport listener → accept loop → existing `handle_request` / `handle_request_subinterp`.
- Acceptance: bench baseline rps ≥ current (any gain is a bonus; equality means accept layer works).

### Phase 2 — Per-thread sub-interpreter (1 PR, ~600 lines, includes deletions)

Goal: each TPC thread hosts its own sub-interpreter directly, skipping the shared pool.

- `src/tpc.rs`: at thread start, call `Py_NewInterpreter()`; run the bootstrap script (sharded from `_bootstrap.py` with the existing C-FFI registrations for `_pyronova_recv` / `_pyronova_send` / `_pyronova_db_*` / `_pyronova_emit_log`).
- `handle_request_subinterp`: instead of `pool.submit(work)` + wait on oneshot, it calls `_pyronova_recv` *on this thread's sub-interp tstate* directly (via `PyEval_RestoreThread` → run handler → `PyEval_SaveThread`). The response comes back via `_pyronova_send` synchronously in the same thread.
- In-flight counter: per-thread `Cell<u32>` capped at a TPC-local admission limit (default 128 per thread). 503 when exhausted.
- Delete: the zombie-worker pool-id guard in `_pyronova_recv` / `_pyronova_send` (still tolerated for compatibility but becomes dead code on TPC paths).
- Acceptance: 327 existing tests pass in TPC mode. Sub-interp DB bridge tests pass unchanged.

### Phase 3 — gil=True main-interp bridge (1 PR, ~250 lines)

Goal: gil=True routes reach the main interpreter through a single MPSC rather than the old pool's GIL-handler fallback.

- One std::thread owns the main-interp tstate, runs a tiny tokio current_thread runtime for `PgPool`-style global awaits.
- `tokio::sync::mpsc::channel::<GilWorkRequest>(capacity)` shared to all TPC threads.
- TPC thread's `handle_request` for a `gil=True` route: build the work item, `tx.send().await`, await oneshot response.
- Main-interp thread: rx loop, dispatch to `handle_request` (GIL variant), send response back via oneshot.
- Fall-through path for legacy C-extensions (numpy / pandas users) — known cold path, acceptable MPSC cost.
- Acceptance: existing gil=True tests unchanged pass rate.

### Phase 4 — Delete the old pool (1 PR, -2000 lines net)

Goal: remove `InterpreterPool`, sharding, P2C, admission semaphore, dual sync/async pool, zombie guards, bootstrap pool-id, etc. Old `handle_request_subinterp` path becomes the only path.

- Remove `src/interp.rs`'s `InterpreterPool` struct and all sharding code.
- Remove `submit_semaphore` + permit logic.
- Simplify `WorkerState` to a per-thread singleton (no id-cookie match).
- `_bootstrap.py`: drop the pool-id zombie-guard constant.
- `Py<T>` vs `Rc<T>` audit: every handle that no longer crosses threads can switch from `Arc` to `Rc` where it matters for cache locality.
- Acceptance: same 327 tests. Line count decreases by ~2000.

### Phase 5 — NUMA-aware thread pinning + real-hardware validation (outside this roadmap)

Goal: on multi-socket / multi-CCD boxes, map TPC thread N to the CCD its SO_REUSEPORT hash targets. On TR 3995WX this means building a NUMA topology table from `/sys/devices/system/node/` and pinning threads to their local CCD.

Requires actual NUMA hardware (GCP c3d or AWS c7a, see design discussion). Deferred until Phase 1-4 prove out.

## Risks + rollback

- `Py_NewInterpreter` quirks on CPython 3.14: we already use it in `InterpreterPool` (just via a different spawn path). Same C-API; no new 3.14-specific concerns.
- Handler blocks the thread = accept stalls on that thread: mitigated by SO_REUSEPORT → kernel routes to other threads. Worst case: one slow handler blocks one thread's keep-alive pipeline, which mirrors current behavior (one slow handler blocks its pool worker).
- `async def` handlers on `LocalSet::spawn_local`: cleaner than today (no cross-thread bridge), but needs careful testing that the handler's tokio primitives work inside a sub-interpreter's tstate. Likely works; PyO3 async bridge (`pyo3-async-runtimes`) is already in-tree.
- Rollback: each phase is a separate PR against `main`. If Phase N regresses, revert. Phases 1-3 keep the old pool as a fallback behind a flag; Phase 4 is the point-of-no-return.

## Slow-handler defense (three-line rampart)

TPC's hard constraint: accept + handler run on the same thread, so a 50 ms sync `def` freezes that core's accept loop for 50 ms. Handled by defense in depth, not by trying to preempt Python (which is physically impossible without UB).

### Line 1 — TPC watchdog with per-core last-tick

Extend `src/monitor.rs` with `static TPC_LAST_TICK: [AtomicU64; MAX_CORES]`. Each TPC thread stamps a monotonic ns timestamp before entering Python and zeros it on exit. A dedicated monitor thread sweeps every 5 ms and emits a loud `tracing::warn!` when any core's delta crosses the configured threshold (default 20 ms). Log includes core id, time blocked, and last-seen route so the offender is named and shamed.

```rust
// on TPC thread, around the handler call:
TPC_LAST_TICK[core_id].store(now_ns(), Ordering::Release);
let resp = run_python_handler(req);
TPC_LAST_TICK[core_id].store(0, Ordering::Release);

// monitor thread:
if now_ns() - TPC_LAST_TICK[core_id].load(Ordering::Acquire) > threshold_ns {
    tracing::warn!(target: "pyronova::server", core = core_id,
        "TPC core blocked — slow sync handler; use async def or blocking=True");
}
```

Cost: 2 Relaxed stores per request, 1 Relaxed load per 5 ms per core. Negligible.

### Line 2 — `blocking=True` escape hatch

Explicit API:

```python
@app.get("/heavy", blocking=True)
def heavy(req):
    # This runs on tokio's blocking pool, NOT on a TPC core.
    slow_sync_numpy_stuff()
    return {...}
```

Dispatch: when the router matches a `blocking=True` route, the TPC thread does `tokio::task::spawn_blocking(move || run_on_main_interp(work))` and `.await`s the oneshot — same cross-thread hop as gil=True but decorated for user visibility. The TPC event loop immediately returns to accepting / polling.

Internally this is the same pipe as gil=True (single main-interp thread + MPSC), just with an explicit opt-in. Users who haven't read the docs and write slow sync handlers will be shamed by Line 1's watchdog until they flip the flag.

### Line 3 — per-request timeout with 504

Wrap every handler call (sub-interp and main-interp paths) in `tokio::time::timeout(default_10s, handler_future)`. On timeout: drop the pending task, respond 504, log the freeze. The Python interpreter keeps running the zombie code (can't preempt), but:

- Client doesn't hang forever.
- SO_REUSEPORT re-hashes new connections to other cores whose accept queues aren't full → one stuck core ≠ cluster down.
- Watchdog (Line 1) has already been screaming.

```rust
let resp = tokio::time::timeout(handler_timeout, spawn_local(run_python_handler(req))).await;
match resp {
    Ok(Ok(r)) => send(r),
    Ok(Err(panic)) => send_500(),
    Err(_) => { record_timeout(core_id); send_504(); }
}
```

**Inline sync handlers are the exception (decided 2026-09-25).** Only a sync `def` route runs inline on the TPC thread: the Python call runs on the current_thread runtime's only thread, so no timer can fire until the call returns — `tokio::time::timeout` cannot preempt it. The implemented behaviour: when the call returns past `REQUEST_BUDGET`, the response is a 504, counted in `DROPPED_REQUESTS`, with an error log naming the handler; the client still waits for the handler. Preemption was rejected: `PyThreadState_SetAsyncExc` can land inside C extensions or `finally` blocks, and moving the call off-thread costs a cross-thread wake on every request.

**`async def` routes run on the async worker pool, not inline (decision Q1, 2026-09-25).** Earlier the TPC thread drove an `async def` handler on its worker's own loop and blocked for the coroutine's whole run, so its awaits overlapped nothing and its timeout was as late as a sync handler's. Now, when the table has `async def` routes, the TPC server also builds the dual-engine async pool (`split` of zero sync workers and N async-engine workers: one per TPC thread when there are no sync worker routes, half of them otherwise) and a TPC thread sends each `async def` request there, awaiting the reply under `REQUEST_BUDGET` on its own loop. Cost per `async def` request: one crossbeam send, one oneshot wake back to the TPC thread, and two `Arc<str>` plus the query `String`; the sync inline path is unchanged (the dispatch already matched on the route's call, now also on its handler kind). So the on-time 504 applies to `async def` and `gil=True` routes; only a sync `def` on TPC cannot be preempted. Slow work belongs on `async def` or `gil=True`.

## gil=True / blocking=True bridge — concrete design

- Channel: `tokio::sync::mpsc::channel::<GilWorkRequest>(16)` (not flume, not SPSC-per-core). Rationale:
  - Bottleneck is CPython GIL ≈ 200 QPS on a real numpy workload. A channel with any sensible synchronization primitive services that 10,000× over.
  - MPSC on tokio with 64 producers has measurable atomic contention, but the destination thread is throughput-capped by the GIL, so the queue spends most of its life empty or with 1-2 items — exactly the case where atomic CAS is cheap.
  - Star-of-SPSC (option B from our review) adds complexity (N receivers) without measurable benefit on this cold path.
- Admission: `tx.try_send(work)` only. Full channel → immediate 503 on the TPC thread. **Never `.send().await`** — that would couple TPC throughput to GIL throughput, defeating the whole point.
- Shutdown: main-interp thread drains its own rx loop on cancellation, same two-stage pattern as TPC threads.

## Process-global state sandbox

Today: zero defense. `_bootstrap.py` even sets `os.environ["PYRONOVA_WORKER"] = "1"` itself, normalizing the exact pattern that blows up in user code on 64 cores.

TPC introduces more cores per process, so this risk scales linearly. Mitigation in Phase 2:

```python
# in _bootstrap.py, after sub-interp starts:
import os as _os

class _SandboxedEnviron(dict):
    """Read-only view of os.environ in sub-interpreter workers.
    Writes raise RuntimeError with a pointer to SharedState for
    cross-worker config distribution.
    """
    def __setitem__(self, k, v):
        raise RuntimeError(
            "os.environ mutation forbidden in sub-interp workers — "
            "all workers share the process env. Use app.state or "
            "SharedState for cross-worker config."
        )
    __delitem__ = lambda self, k: _SandboxedEnviron.__setitem__(self, k, None)

# Freeze os.environ AFTER our own PYRONOVA_WORKER write.
_snapshot = dict(_os.environ)
_os.environ = _SandboxedEnviron(_snapshot)

# Block os.chdir — same reasoning.
def _chdir_blocked(path):
    raise RuntimeError("os.chdir forbidden in sub-interp workers — "
                       "CWD is process-global. Use absolute paths.")
_os.chdir = _chdir_blocked
```

Caveats:
- C extensions that call `setenv` / `chdir` directly bypass this. No Python-level fix for those.
- Some libs (pytest, pydantic settings) read `os.environ` via `.get` — read side untouched.
- Opt-out for power users who know what they're doing: `PYRONOVA_SANDBOX=off`.

## TLS handshake rate limit

Today: none. On a cold-start flood (LB restart, campaign burst), a single TPC thread could serialize through hundreds of TLS handshakes before serving any HTTP.

Mitigation in Phase 2: per-thread semaphore capped at `max(4, physical_cores / 8)` in-flight handshakes. Accepted connections beyond the cap queue in the listener backlog — new connections land on other TPC threads via kernel re-hash before the capped thread finishes.

```rust
// Per-thread state:
let tls_permits = Arc::new(tokio::sync::Semaphore::new(4));

// In accept task, before TLS handshake:
let permit = tls_permits.clone().acquire_owned().await?;
tokio::spawn_local(async move {
    let _keep = permit; // dropped on task exit
    let tls_stream = acceptor.accept(stream).await;
    // ... normal request handling
});
```

Trade-off: legitimate TLS burst gets slightly queued; the alternative (unbounded parallel handshakes) starves Python handler execution on the same thread.

## Non-goals for v2.3

- Monoio / io_uring — stays on epoll/kqueue (Tokio). The TPC shape does not require io_uring; Actix has been doing this on epoll for years.
- H/2 single-connection multiplexing hot-spotting — inherent to SO_REUSEPORT + kernel L4 hashing, industry-wide unsolved (Envoy, NGINX have the same limit). Documented as a known pathology; fix in deployment (multiple client connections or per-stream scheduling, neither in scope).
- Reverse-proxy long-connection skew — deployment concern. `docs/deployment.md` will carry a "required LB settings" section: minimum upstream connection count, keep-alive idle timeout, etc.
- New features (HTTP/3, WebTransport, multi-tenant gRPC, etc.) — feature parity with v2.2 only.
- Benchmarking tuning (NUMA topology, cgroups, etc.) — Phase 5.

## Resolved design decisions (post-review)

1. **Thread count default**: `num_cpus::get_physical()`. SMT threads on the same core thrash L1I/L1D. Leave SMT siblings to the kernel network stack + NIC interrupts. Override via `workers=` param or `PYRONOVA_WORKERS` env.
2. **gil=True throughput ceiling**: one main-interp thread = single GIL ≈ 200 QPS on numpy-class workloads. Accepted limitation. Documentation points heavy-gil users to Polars (GIL-releasing) or out-of-process workers (Celery etc.).
3. **LocalSet drop semantics**: explicit two-stage shutdown. (a) Cancel accept → no new work admitted. (b) Drain LocalSet with a cancellation token + sleep fallback (hard-stop after `shutdown_grace_seconds`). (c) Only after all `spawn_local` tasks complete: `PyThreadState_Clear` → `PyThreadState_Delete` → `Py_EndInterpreter`. Existing per-thread teardown in `interp.rs` covers most of this; Phase 2 makes it mandatory on TPC exit.

---

Approved? If yes, I'll start Phase 1 behind the flag — old path untouched, new path opt-in.
