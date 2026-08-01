# Changelog

## v2.6.0 (2026-08-01) — PyO3 0.29 + C extensions under sub-interpreters

### Upgraded PyO3 0.28 → 0.29

Clean upgrade: 522 `pyo3::ffi` call sites and 11 `#[pyclass]` types compile
unchanged; all 52 Rust unit tests pass. Why it matters: PyO3 0.28 hard-panics
(`pyo3#576`) when a `#[pyclass]` is registered inside a sub-interpreter — the
reason Pyronova had to `bypass pyo3` and hand-build its `_Request` type via raw
C-API. PyO3 0.29 loads `#[pyclass]` types under the multi-interp override, with a
*distinct* module instance per sub-interpreter (8 sub-interps → 8 module
addresses, true isolation). Numeric C extensions no longer need the raw-C-API
treatment. `pyo3-async-runtimes` and `pythonize` bumped to 0.29 to match.

### C extensions under sub-interpreters — documented and demonstrated

- **New doc** `docs/subinterp-c-extension-status.md` (+ `.en.md`) — the *measured*
  status (Python 3.14.6 / PyO3 0.29): the two walls (slot check vs. global-state
  `cannot load module more than once`), the
  `_imp._override_multi_interp_extensions_check` switch, a fresh compatibility
  matrix (pydantic / msgpack / cryptography work under override; numpy / orjson /
  lxml need a per-worker physical copy), PyO3 0.28-vs-0.29, and the kernel
  throughput spectrum. Supersedes the stale `subinterp-c-extension-compat.md`.
- **New example** `examples/c_extension_subinterp.py` — a Rust/PyO3 0.29 native
  kernel running inside own-GIL sub-interpreter workers, zero-copy over an f64
  buffer. Load test (`wrk -t8 -c256 -d10s /compute`): **6,647 req/s, 66,740
  requests, zero errors, 38 ms latency**.

## v2.3.1 (2026-04-23) — Sub-interpreter DB bridge unlocked under TPC

### Sub-interpreter DB bridge now works under TPC

`src/bridge/db_bridge.rs` previously panicked the moment a DB-backed
route without `gil=True` ran under TPC:

    thread 'pyronova-tpc-N' panicked at tokio ... multi_thread/mod.rs:88:
    Cannot start a runtime from within a runtime.

Cause: the bridge's C-FFI entry points called `rt.block_on(fut)` on
the dedicated DB runtime, from inside the TPC worker thread's own
tokio `current_thread` runtime. tokio forbids nested `block_on`.

Fix: `rt.spawn(fut)` + `std::sync::mpsc::sync_channel` + `rx.recv()`.
`spawn` imposes no runtime-context check — it only queues the task
onto the DB runtime's worker pool. The sub-interp worker blocks on
the channel with the GIL released (`py.detach`), so peer
sub-interpreters keep making progress while the query is in flight.
Parallelism ceiling becomes `min(sub_interp_workers,
DATABASE_MAX_CONN)` instead of the main-interp GIL.

`BoundParam` gained `#[derive(Clone)]` so params can be moved into
the spawned future (which must be `'static`). One clone per query is
rounding error next to the PG round-trip.

Arena `/async-db` route drops `gil=True` as a result:

    bench (7840HS, wrk -t8, PG sidecar on 5433):
      main_bridge 16 workers (v2.3.0-rc):   15k req/s @ c=4096
      sub-interp DB bridge (v2.3.0):        **35k req/s @ c=4096**, 0 drops

crud routes keep `gil=True` because their cache-aside semantics
require a single interpreter's `_CRUD_CACHE` dict (sub-interp workers
have independent heaps — the HttpArena validator's "MISS then HIT"
check would fail if two consecutive requests hashed to different
sub-interps via SO_REUSEPORT).

### Arena submission tuning

`arena_submission/launcher.py` now exports
`PYRONOVA_GIL_BRIDGE_WORKERS=16` +
`PYRONOVA_GIL_BRIDGE_CAPACITY=8192` so the remaining gil=True routes
(crud) don't 503 under HttpArena's 1024-4096 concurrency profiles.
`arena_submission/app.py::crud_get_one` emits `X-Cache: MISS|HIT`
header for the HttpArena cache-aside validator.

## v2.3.0 (2026-04-23) — Multi-worker GIL bridge + observability knobs

Mechanical improvements on top of the v2.2 sub-interpreter foundation:
the main-interpreter bridge goes multi-threaded, access logging gains
a sampling knob for production deployments, and a new `@cached_json`
decorator lands for the "public read endpoint" pattern.

### TPC main-interpreter bridge: single thread → N workers

`gil=True` routes under TPC (numpy, pandas, pydantic-core, anything
that can't run in a sub-interpreter) used to funnel through a single
`std::thread` driven by `Receiver::blocking_recv`. That's correct for
pure-CPU handlers — CPython's GIL caps parallelism at 1 — but
catastrophic for I/O-bound handlers. `time.sleep`, `pool.fetch_one`,
`open(...).read()`, and anything else that *releases* the GIL still
blocks the **thread**, so the released GIL goes unused while new work
piles up until 503.

v2.3 replaces the single thread with a pool backed by a crossbeam MPMC
queue. Each released GIL is immediately picked up by a peer worker.
CPU-bound handlers serialize as before; I/O-bound handlers see real
concurrency bounded only by worker count.

- Default: 4 workers, capacity 16 × workers
- Knobs: `PYRONOVA_GIL_BRIDGE_WORKERS`, `PYRONOVA_GIL_BRIDGE_CAPACITY`

Measured on a 5 ms `time.sleep` handler at c=64: single-thread ~200
req/s with 99.96% 503s, 4-thread ~777 req/s with 0 drops.

### Access-log sampling

`enable_logging()` takes two new optional args:

    app.enable_logging(sample=100, always_log_status=400)

- `sample=N` — log 1-in-N requests (default `1`, log every).
- `always_log_status` — always log when status >= this (e.g. `400`
  keeps 4xx/5xx full visibility, samples 2xx).

Avoids the 25-30% throughput tax of full-traffic logging at 400k+ req/s.

### `@cached_json(ttl=...)` decorator

    from pyronova import Pyronova, cached_json

    @app.get("/health")
    @cached_json(ttl=1.0)
    def health(req):
        return {"status": "ok"}

Per-worker response cache: first call within the TTL runs the handler
and stashes JSON bytes; hits short-circuit handler + `json.dumps`.
100-row JSON (~40 KB) on 7840HS: 68k → **336k req/s** (5.0× throughput,
9× lower p50).

### Startup banner

TPC mode's startup line prints the route-count breakdown again
(`Routes: 5 sub-interp + 6 GIL + 0 async`), plus `(N stream)` suffix
when stream routes are registered.

### Internal

- `serde_json` → `sonic-rs` on response hot path (within noise now,
  kept for future SIMD payload work).
- `body_stream_rx` only materialized when dispatching to main_bridge —
  TPC inline hot path no longer pays an Arc::clone per request.
- Arena-submission `/crud/items/{id}` emits `X-Cache: MISS|HIT` header
  (HttpArena validator requirement).

### Bugfixes / CI

- `test_passive_gil_metrics::test_total_requests_counter` now green —
  the metrics flag re-reads env on every `app.run()` instead of
  latching once per process.
- `clippy::collapsible_match` (Rust 1.95) silenced on WebSocket read
  loop — guard-based rewrite duplicates arms, hurts readability.
- `cargo fmt` + `clippy --all-targets -- -D warnings` green on 1.94
  and 1.95.

## v2.0.0 (2026-04-21) — Rename to Pyronova

Project rename — `pyreframework` → `pyronova`, brand `Pyre` → `Pyronova`.
Clean break, no alias. All downstream users must update imports. This is
a rename-only release — no behavioral changes.

### Why

"Pyre" collided with Meta's widely-used Pyre type checker. After a round
of name-collision research (PyPI availability + GitHub hit count), we
settled on **Pyronova** — retains the fire theme, encodes "Python +
Rust + nova (explosive newness)", single GitHub collision, free on
PyPI. See the rename PR description for the full candidate shortlist.

### Breaking — import surface

- PyPI package: `pip install pyronova` (was `pyreframework`).
- Main class: `from pyronova import Pyronova` (was `from pyreframework
  import Pyre`).
- Type names switch to unprefixed ("FastAPI style"): `Request`,
  `Response`, `WebSocket`, `Stream`, `BodyStream`, `RPCClient`,
  `Settings`. The `PyreRequest` / `PyreResponse` / … prefixed forms
  are gone.
- CLI: `pyronova run/dev/routes` (was `pyre ...`).
- Env vars: `PYRONOVA_HOST`, `PYRONOVA_PORT`, `PYRONOVA_LOG`,
  `PYRONOVA_WORKERS`, `PYRONOVA_IO_WORKERS`, `PYRONOVA_TLS_CERT`,
  `PYRONOVA_TLS_KEY`, `PYRONOVA_RELOAD`, `PYRONOVA_LOG_LEVEL`,
  `PYRONOVA_TEST_PG_DSN`.
- Rust crate: `pyronova-engine`, lib name `pyronova_engine` (was
  `pyreframework-engine` / `pyreframework_engine`).
- Rust FFI symbols injected into sub-interpreter globals: `_pyronova_recv`,
  `_pyronova_send`, `_pyronova_emit_log`, `_pyronova_pool_id`.
- Log target names: `pyronova::server`, `pyronova::access`,
  `pyronova::app`.

### Migration

Mechanical — two find-and-replace passes handle the common shape:

```bash
git grep -l pyreframework | xargs sed -i 's/pyreframework/pyronova/g'
git grep -l '\bPyre\b' | xargs sed -i 's/\bPyre\b/Pyronova/g'
# Type names lost their prefix — handle per-class:
git grep -l PyreRequest | xargs sed -i 's/PyreRequest/Request/g'
git grep -l PyreResponse | xargs sed -i 's/PyreResponse/Response/g'
# …and the same for WebSocket, Stream, BodyStream, RPCClient, Settings.
# Env vars:
git grep -l PYRE_ | xargs sed -i 's/PYRE_/PYRONOVA_/g'
```

Historical release notes, benchmark reports, and incident post-mortems
under `benchmarks/benchmark-*.md`, `docs/memory-leak-investigation-*.md`,
`docs/advisor-triage-*.md` were deliberately left unrenamed — the
framework was called Pyre when those events happened, and rewriting the
history is dishonest.

## v1.5.0 (2026-04-19)

Memory-leak root cause fix + hardening pass. Minor-bump because Python
3.13+ is now required (dropped 3.10-3.12 support, see "Breaking").

### Headline — sub-interpreter memory leak closed

Pyre sub-interpreters had a long-standing unbounded RSS growth under
sustained load (~128 B/req at 400k rps → OOM in ~10 min). Root cause
isolated via a pure-C reproducer to cross-thread `PyThreadState` reuse:
`SubInterpreterWorker::new` ran on the main thread, `Py_NewInterpreterFromConfig`
bound the tstate to that OS thread, then the worker pthread attach/detached
that tstate on every request. CPython's per-OS-thread tstate bookkeeping
accumulates when the attaching thread differs from the creator — measured
at ~1 KB per iteration in isolation.

Fix (`fc45a7f`): new `rebind_tstate_to_current_thread` helper that each
worker calls on entry. Creates a fresh tstate via `PyThreadState_New(interp)`
bound to the worker's OS thread, swaps it in, and disposes of the creator
tstate. All subsequent request dispatch runs against the thread-local
tstate. **Measured result**: 73.8M requests @ 410k rps over 180s = total
RSS growth of **4 MB** (0.057 B/req, below /proc sampling noise).

Side effect: Python `__del__` / `tp_finalize` now fires correctly in
sub-interp handlers (previously silently broken, was xfail'd).

### Architecture — raw C-API `_PyreRequest` type

Replaced the Python-defined `_PyreRequest` class with a custom heap
type built via `PyType_FromSpec` + `PyMemberDef` in
`src/pyre_request_type.rs`. Custom `tp_dealloc` synchronously DECREFs
all seven slot fields. pyo3's `#[pyclass]` can't be used — pyo3 0.28
hard-rejects sub-interpreters.

Two invariants learned the hard way:

- **No `Py_TPFLAGS_BASETYPE`**: Python subclassing triggers CPython's
  `subtype_dealloc` fallback which silently bypasses our `tp_dealloc`.
  Helper methods (`.text()`, `.json()`, `.body`, `.query_params`) are
  monkey-patched onto the heap type at sub-interp init instead.
- **No `Py_TPFLAGS_HAVE_GC`**: empirically made per-request leak 5×
  worse. Our workload has no cycles; GC tracking costs without benefit.

### Correctness & hygiene — 22 fixes triaged from adversarial review

Full triage record (31 claims reviewed, 21 real / 10 rejected) lives in
`docs/advisor-triage-2026-04-19.md`.

C-API hygiene: `PyDict_Next` + `PyObject_Str` re-entrancy in
`parse_sky_response`; `PyObject_IsInstance == 1` now handles `-1`
(error); `py_str_dict` clears pending exceptions on OOM; `PyDict_SetItem`
return value checked; `_PyreRequest.__init__` path type-checks dict
slots via `PyDict_Check` before `PyDict_Clear`.

Lifecycle: `LoopGuard` drop-after-`Py_Finalize` segfault fixed via
`std::mem::forget`; Hyper graceful shutdown via `TaskTracker` waits
up to 30s for in-flight connections before runtime drop (was
RST-on-shutdown); `InterpreterPool::drop` bounds worker-thread join
at 5s; `spawn_rss_sampler` JoinHandle now joined on shutdown;
`WORKER_STATES` `OnceLock` → `RwLock<Vec>` so repeated `app.run()`
gets fresh channels.

WebSocket: async WS handlers now driven via `asyncio.run` (was
silently dropped); explicit `drop(handler)` under GIL.

Routing: path params URL-decoded (`/user/john%20doe` → `"john doe"`);
async middleware coroutines driven through `resolve_coroutine` in
both sub-interp and GIL-mode paths; CORS `origin="*"` + credentials
emits W3C-violation warning.

Static files: `canonicalize → File::open` TOCTOU closed via
`O_NOFOLLOW` on Unix (refuses post-check symlink swap).

### Performance

Awaitable detection in `resolve_coroutine` moved from
`PyObject_HasAttrString(obj, "__await__")` (μs per call — interns the
string, walks the MRO, runs descriptor protocol) to a direct
`Py_TYPE(obj)->tp_as_async->am_await` pointer probe (ns, L1-resident).

### Benchmark

Bench targets split cleanly. `benchmarks/bench_plaintext.py` (new,
feature-light) is the target for `just bench-record` / `bench-compare`.
`examples/hello.py` (restored to v1.4.0 content) is the feature demo,
run via `just bench-features`. `just bench-tfb-plaintext` runs
TechEmpower-style `wrk -t8 -c256 -d15s --pipeline 16`.

Numbers (AMD Ryzen 7 7840HS, 8C/16T, Python 3.14.4, performance
governor, wrk 4.1.0):

| Workload | Config | Req/s |
|---|---|---|
| Plaintext baseline | `wrk -t4 -c100 -d10s` | **422,976** |
| Feature demo | `wrk -t4 -c100 -d10s` on `hello.py` | 381,123 |
| **TFB Plaintext** | `wrk -t8 -c256 -d15s --pipeline 16` | **902,213** |
| TFB JSON (`/hello/{name}`) | `wrk -t8 -c512 -d15s` | ~536,000 |

Plaintext baseline vs v1.4.0's published 419,730 on the same machine:
**+0.8% — zero regression**. The ~10% gap on `hello.py` reflects the
added cost of async-correct middleware + access log (intentional
hygiene tax, opt-in).

### Tests

- `test_sustained_concurrent_load_no_leak` — 12s soak, fails if RSS
  grows >15 MB. Regression guard for the tstate fix.
- `test_subinterp_python_finalizers_fire` — xfail removed.
- `test_capi_hygiene.py` — 5 tests (reentrancy, malformed init,
  URL-decode, async hook, instancecheck raise).
- `test_static_symlink_out_of_root_refused` — O_NOFOLLOW regression.
- `worker_states_can_be_reinstalled` — Rust unit test for hot-reload
  of sub-interp pool.
- `TestClient(port=None)` auto-allocates port (new).
- Port collision fixes (19878, 19883).

Total: 235 passed / 2 full-suite runs / 0 flakes / 0 regressions.

### Breaking

- **`requires-python = ">=3.13"`** (was `>=3.10`). Users on 3.10-3.12
  should stay on v1.4.x.

### Deferred to future releases

- SSE dedicated asyncio background thread.
- `response_map` active-sweep GC for deadlocked-worker orphans.
- PyBuffer-based zero-copy body write on the send path.
- `query_params_all()` additive API for HPP-correct multivalue access.

---

## v1.4.5 (2026-04-19)

Security + correctness hardening from an adversarial review pass. 23 fixes
(6 critical + 17 error).

**Same-day same-machine benchmark** (AMD 7840HS, `wrk -t4 -c100 -d10s`
on `examples/hello.py` hybrid mode):

| Build | Requests/sec (avg of 3) |
|---|---|
| d0ce481 (v1.4.4 pre-hotfix) | ~260k |
| v1.4.5 (this release) | ~320k |

No regression; observed a small net gain within noise. Both runs are
below the 419k historical baseline from v1.4.0's benchmark day — the
machine is in a different thermal / load state today, not a code
change. The relative comparison is what matters and it is clean.

### Critical (already shipped in hotfix)
- `accept()` loop classifies errno (EMFILE / ENFILE / ENOBUFS / ENOMEM)
  and backs off — was 100% CPU spin on FD exhaustion
- Sub-interpreter RAII: `SubInterpreterWorker::new` no longer leaks the
  sub-interp on any of 5+ fallible init steps
- WebSocket: `py_handle.join()` moved off the Tokio worker pool;
  `recv/recv_bytes/recv_message` release the GIL via `py.detach()`
- MCP: reject non-object JSON-RPC payloads with -32600 instead of
  crashing through `AttributeError`

### Security
- Cookies: reject CRLF / NUL in name / value / domain / path / expires
  (HTTP Response Splitting)
- Error responses: `serde_json` for `{"error": msg}` — hand-rolled
  escape handled only `"`, leaving backslash / control-char injection
  open
- Static files: open-once design removes a TOCTOU where a rename
  between `metadata()` and `read()` bypassed the `MAX_STATIC_FILE_BYTES`
  cap; `.take(cap)` adds belt-and-braces
- `before_request` hook that raises now fails the request with 500
  — previously fell through to the unprotected main handler, a
  critical auth-bypass for deny-via-raise auth hooks
- Sub-interp path: CORS now applied on the `Err` branch so browsers
  show the real 5xx instead of an opaque CORS error
- Interp FFI: `PyTuple_SetItem` never embeds a NULL — every leaf
  allocation NULL-checked up front, partial failures cleaned up
  atomically (was a latent segfault on OOM)

### Correctness
- `logging::init_logger`: `(writer, guard)` stored atomically in
  `OnceLock<LoggerState>` — previous design could drop the guard of
  whichever caller won `try_init()`, silently killing the log thread
- `router::lookup` uppercases the method to match `insert` — HTTP is
  case-insensitive per RFC 9110 §9.1, lowercase / `Get` was silently
  missing routes
- `SharedState::incr` raises `TypeError` / `OverflowError` instead of
  silently resetting non-numeric values to 0
- Bounded channels: `PyreStream` (1024) + WebSocket outgoing (1024)
  with `try_send` — unbounded was an OOM DoS under slow-client
  backpressure
- `handlers::handle_request` error path: full PyErr logged server-side
  (`e.display(py)` + `tracing::error!`); client gets a generic "handler
  error" instead of a leaked one-line repr

### Python
- `rpc.py` / `_async_engine.py`: `log.exception` before the error
  envelope so server-side stack traces survive RPC / async failures
- `_bootstrap.py` `PyreRustHandler.emit`: `self.handleError(record)`
  instead of `pass` — stdlib logging's standard "I failed to log" hook
- `mcp._extract_schema`: `typing.get_type_hints(fn)` so tools defined
  in modules with `from __future__ import annotations` don't silently
  regress to "string" for every argument
- `UploadFile`: `@dataclass(frozen=True, slots=True)` — shares memory
  with the raw multipart buffer, mutation would corrupt replay

## v1.4.0 (2026-04-01)

### Performance — Linux 42万 QPS
- **SO_REUSEPORT multi-accept** — N=io_workers 个独立 accept loop，Linux 内核级四元组哈希负载均衡，macOS 自动降级为 1
- **M:N scheduling** — `io_workers` (Tokio I/O threads) 和 `workers` (Python sub-interpreters) 独立配置，解耦网络层与计算层
- **LTO fat + codegen-units=1** — 编译期全局优化，+4% JSON/params 路由
- **TCP_QUICKACK** — Linux 禁用延迟 ACK，降低首字节延迟
- **Headers OnceLock lazy view** — 不访问 headers 时零开销，延迟转换
- **serde_json + pythonize** — Rust 侧 JSON 序列化，替代 Python json.loads
- **SharedState Bytes** — 零拷贝 clone
- **Arc\<str\> method/path** — 请求路径零分配
- **IpAddr lazy eval** — 不访问时不解析
- **Bytes zero-copy body** — 请求体零拷贝
- **mimalloc global allocator** — 高并发分配性能

### Features
- `io_workers` parameter — `app.run(workers=24, io_workers=16)` 或 `PYRE_IO_WORKERS=16`
- `client_ip` — 请求客户端 IP 地址
- Lifecycle hooks — `on_startup` / `on_shutdown`
- Zero-cost logging — Rust tracing engine + Python→Rust FFI bridge, OFF 级别原子跳过

### Benchmarks (Linux, AMD Ryzen 7 7840HS 8C/16T)
- **GET /: 420k req/s** (P99 571μs) — vs macOS v1.2.0 214k (+96%)
- **300s sustained: 401k req/s**, 1.2 亿请求, 0 错误, 内存仅 +27 MB
- **vs Robyn: 14-16x faster** across all routes

## v1.3.0 (2026-03-31)

### Features
- **Zero-cost logging system** — Rust `tracing` + `EnvFilter`, three targets, Python logging bridge via C-FFI
- **client_ip** property on PyreRequest
- **on_startup / on_shutdown** lifecycle hooks

### Performance
- IpAddr lazy evaluation
- Bytes zero-copy request body
- Arc\<str\> method/path to eliminate allocations
- Vec params (from HashMap)
- Zero-allocation hook iteration
- Sync Python log level with Rust EnvFilter

### Docs
- Sub-interpreter C extension compatibility guide (30/30 libs confirmed)
- English translations for all benchmark reports

## v1.2.0 (2026-03-25)

### Features
- **Dual async/sync worker pool** — `async def` handlers auto-route to asyncio event loops, `def` handlers to sync sub-interpreters. Zero config, zero performance loss.
- **Native async bridge (C-FFI)** — `pyre_recv`/`pyre_send` release GIL during channel wait, enabling true async in sub-interpreters.
- **MCP Server** — JSON-RPC 2.0 with `@app.mcp.tool()`, `@app.mcp.resource()`, `@app.mcp.prompt()` decorators.
- **MsgPack RPC** — `@app.rpc()` with content negotiation (MsgPack/JSON) + `PyreRPCClient` magic client.
- **SSE Streaming** — `PyreStream` with mpsc channel, returned directly from handlers.
- **SharedState** — Cross-worker `app.state` backed by `Arc<DashMap>`, nanosecond latency.
- **GIL Watchdog** — Monitor GIL contention, hold time, queue depth, memory RSS.
- **Backpressure** — Bounded channels with `try_send()`, returns 503 on overload.
- **Request timeout** — 30s zombie reaper in sub-interpreter mode (504 Gateway Timeout).
- **mimalloc** — Global allocator for high-concurrency allocation performance.
- **Hybrid dispatch** — `gil=True` routes auto-dispatch to main interpreter for C extension compatibility.

### Code Quality
- Extracted bootstrap script from Rust string to `python/pyreframework/_bootstrap.py` (`include_str!`).
- Removed dead `filter_script_ast` code.
- Moved CORS/logging from global statics to per-instance `PyreApp` fields.
- Added `debug_assert!(PyGILState_Check())` in `PyObjRef::Drop`.
- Full `cargo fmt` + zero clippy warnings.
- Migrated deprecated PyO3 `downcast` → `cast` calls.

### Bug Fixes
- **Fixed segfault on Ctrl+C** — `InterpreterPool::Drop` now joins worker threads before `Py_Finalize`.
- **Fixed KeyboardInterrupt noise** — Guard `signal.signal()` for main thread only.
- Hot reload fallback skips `.venv`/`node_modules`/`__pycache__`.

### Testing
- 21 Rust unit tests (response builders, MIME detection, header extraction, query params).
- 54 Python pytest tests (MCP, cookies, TestClient, RPC, static files, WebSocket, async isolation, logging).
- 22 integration tests (GIL + sub-interp modes, all features end-to-end).
- 5-minute stability benchmark: 64M requests, zero memory leaks, zero crashes.

### CI/CD
- GitHub Actions: cargo test → pytest → integration tests on Python 3.13/3.14.
- Blocking `cargo fmt --check` + `cargo clippy -- -D warnings`.

## v1.1.0 (2026-03-24)

### Features
- WebSocket support (text + binary) via tokio-tungstenite.
- Cookie utilities (`get_cookie`, `set_cookie`, `delete_cookie`).
- Multipart file upload parser.
- Redirect helper.
- TestClient for testing without a running server.
- Env var configuration (`PYRE_HOST`, `PYRE_PORT`, `PYRE_WORKERS`, `PYRE_LOG`).
- Hot reload (`reload=True` or `PYRE_RELOAD=1`).

## v1.0.0 (2026-03-23)

### Initial Release
- Rust core with Tokio + Hyper HTTP server.
- Per-Interpreter GIL (PEP 684) sub-interpreter pool.
- Decorator routing (`@app.get`, `@app.post`, etc.).
- Path params, query params, JSON parsing.
- CORS middleware.
- Static file serving with MIME detection + path traversal protection.
- Pydantic validation via `model=` parameter.
- Before/after request hooks.
- Graceful shutdown via Ctrl+C.
