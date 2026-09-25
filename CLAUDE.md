# Pyronova 🔥

High-performance Python web framework powered by Rust. Per-Interpreter GIL (PEP 684) for true multi-core parallelism in a single process.

**Benchmarks**: 420k req/s (Linux), 222 MB memory. 15x faster than Robyn, 300s sustained 400k QPS zero errors.

## Architecture

- **Rust core** (`src/`): 12 modules
  - `lib.rs` — module declarations, `#[pymodule]`, mimalloc global allocator
  - `types.rs` — `Request`, `Headers` (the request-header view), `Response`, `ResponseHeaders`, `ResponseData`
  - `app.rs` — `PyronovaApp` with `run_gil()` / `run_subinterp()`, graceful shutdown
  - `handlers.rs` — GIL handler, sub-interp handler (30s zombie timeout), streaming
  - `router.rs` — `RouteTable` (`Vec<Route>`, `Target`, `Call`), `MutableRoutes`; `site.rs` — `Site` (frozen table + CORS/access-log config) served by a run
  - `response.rs` — the one handler-result → `ResponseData` mapping every interpreter uses (type from the value, never sniffed from the text), response builders (200/404/413/500/503/504)
  - `json.rs` — Rust-side `py_to_json_value` serializer
  - `static_fs.rs` — async static file serving + MIME detection + path traversal protection
  - `python/` — sub-interpreter workers: `worker.rs` (`SubInterpreterWorker`, runs the real `pyronova` package + engine), `pool.rs` (dual worker pool, sync+async), `worker_api.rs` (`_worker_recv`/`_worker_send` pyfunctions for the async engine), `ffi.rs` (`PyObjRef` RAII, tstate helpers)
  - `run_context.rs` — explicit-interpreter attach (`main_attach`/`attach_to`) for every Rust thread that enters Python
  - `websocket.rs` — WebSocket upgrade, `WebSocket` pyclass, async↔sync bridge
  - `stream.rs` — `Stream` SSE with mpsc channel
  - `logging.rs` — `init_logger` (tracing-subscriber), `emit_python_log` (Python→Rust bridge)
  - `monitor.rs` — GIL watchdog, memory RSS, atomic counters
  - `state.rs` — `SharedState` backed by `Arc<DashMap>`
- **Python interface** (`python/pyronova/`):
  - `engine` (Rust): `PyronovaApp`, `Request`, `Response`, `WebSocket`, `SharedState`, `Stream`
  - `app.py`: `Pyronova` class — decorators, CORS, logging, Pydantic model=, env var config, hot reload, dual pool auto-detection
  - `mcp.py`: MCP server (JSON-RPC 2.0) with tool/resource/prompt decorators
  - `rpc.py`: MsgPack RPC + `RPCClient` magic client
  - `cookies.py`: Cookie get/set/delete utilities
  - `uploads.py`: Multipart form-data parser
  - `testing.py`: `TestClient` for tests without a running server
  - `_async_engine.py`: Async engine script injected into sub-interpreter workers
  - `engine.pyi`: Type stubs for IDE autocomplete
- **Build**: Maturin (mixed python/rust project), module name `pyronova.engine`

## Development

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install maturin

# Build (release mode)
maturin develop --release

# Run example
python examples/hello.py

# Run tests
uv pip install -e ".[test]"
pytest tests/ --ignore=tests/test_ws_binary_client.py -q

# Benchmark vs FastAPI (requires wrk: brew install wrk)
bash benchmarks/run_comparison.sh

# Benchmark vs Robyn
bash benchmarks/run_bench.sh
```

## Key Design Decisions

- Route table is `Vec<Route { handler, name, key, dispatch }>` + `Router<RouteId>`; every serving path runs one pipeline (`handlers/pipeline.rs`: `preprocess` → dispatch → `finish`, which applies CORS and writes the access log for every response)
- GIL released via `py.detach()` during Tokio event loop, reacquired via `Python::attach()` per-request
- `#[pyclass(frozen)]` on Request/Response for thread safety
- `Pyronova` Python wrapper provides decorator syntax; `PyronovaApp` is the raw Rust engine
- Sub-interpreter mode uses `crossbeam-channel` multi-consumer pool with `tokio::sync::oneshot` async responses
- `PyObjRef` RAII wrapper for all raw FFI pointer operations — Drop auto-DECREFs
- Workers import the real `pyronova` package and engine (the fork makes the module per-interpreter); the async engine talks to Rust through `pyronova.engine._worker_recv`/`_worker_send`, which release the GIL during the channel wait
- Every Rust thread that enters Python names its interpreter (`run_context::main_attach` / `attach_to`); a bare foreign-thread `Python::attach` is rejected by `tests/test_attach_allowlist.py`
- Hybrid dispatch: `gil=True` routes go to main interpreter (for C extensions), others to sub-interpreters
- Auto dual-pool: framework detects `async def` vs `def` handlers, routes to appropriate worker pool
- C extensions in workers: extensions that CPython allows load shared; others are copied per worker and initialized inside that worker (`app.isolate(...)` or reactive auto-isolate in `_bootstrap.py`); `pyronova` itself is never isolated
- Static files served via Tokio async fs — no GIL needed
- Middleware: before_request/after_request hooks stored in RouteTable
- WebSocket: tokio-tungstenite async ↔ Python sync via dual channels, one OS thread per connection
- SSE: `Stream` with mpsc unbounded channel, returned from handler
- Logging: Rust `tracing` with `EnvFilter` (zero-cost OFF), three targets (`pyronova::server`, `pyronova::access`, `pyronova::app`), Python logging routed to Rust via `pyronova.engine.emit_python_log` in every interpreter
- mimalloc global allocator for high-concurrency allocation performance
- 30s zombie request timeout in sub-interpreter mode (504 Gateway Timeout)
- Graceful shutdown via `signal::ctrl_c()` or `PyronovaApp.shutdown()` (a per-run `CancellationToken`, `app::until_stopped`); `TestClient.close()` uses the latter

## Project Structure

```
src/
  lib.rs              # Module declarations + #[pymodule] + mimalloc
  logging.rs          # Rust tracing engine + Python logging bridge
  types.rs            # Request, Headers, Response, ResponseHeaders, ResponseData
  app.rs              # PyronovaApp — route registration + server startup
  handlers.rs         # handle_request (GIL), handle_request_subinterp (channel)
  router.rs           # RouteTable, MutableRoutes
  site.rs             # Site: frozen RouteTable + CORS / access-log config
  response.rs         # the one handler-result → response mapping + builders
  json.rs             # py_to_json_value
  static_fs.rs        # try_static_file, mime_from_ext
  run_context.rs      # main_attach / attach_to: explicit-interpreter attach
  python/             # worker.rs, pool.rs, worker_api.rs, ffi.rs: sub-interpreter workers
  websocket.rs        # WebSocket, upgrade handler, async↔sync bridge
  stream.rs           # Stream SSE
  monitor.rs          # GIL watchdog, memory RSS, atomic counters
  state.rs            # SharedState (DashMap)
python/pyronova/
  __init__.py         # Re-exports all public APIs
  app.py              # Pyronova class (decorators, CORS, logging, config)
  mcp.py              # MCP server (JSON-RPC 2.0)
  rpc.py              # MsgPack RPC + RPCClient
  cookies.py          # Cookie utilities
  uploads.py          # Multipart form-data parser
  testing.py          # TestClient
  _async_engine.py    # Async engine script for sub-interpreters
  engine.pyi          # Type stubs
examples/
  hello.py            # Basic demo
  ai_agent_server.py  # MCP + SSE + SharedState + Pydantic
  trading_api.py      # numpy + WebSocket + RPC + SharedState
  fullstack_api.py    # CRUD + Cookie auth + file upload
tests/
  test_all_features.py      # 22 tests (11 per mode × 2)
  test_async_isolation.py   # Proves async isolation
  test_logging.py           # 4 logging tests
  test_env_var_worker.py    # Env var + decorator tests
  test_async_bridge.py      # Phase 7.2 async bridge
  e2e/                      # Manual-run drivers (ws_binary_server/client)
benchmarks/
  run_comparison.sh   # Pyronova vs FastAPI head-to-head
  run_bench.sh        # Pyronova vs Robyn
  benchmark-*.md      # Results history (14 reports)
docs/
  subinterp-safe-ecosystem.md  # Golden Path ecosystem guide
  phase-7.2-async-bridge.md    # Native async bridge design
  dual-engine-design.md        # Dual pool architecture
  gil-monitor-design.md        # GIL watchdog design
  gc-optimization-guide.md     # GC tuning
  logging-design.md            # 日志系统设计（中文）
  logging-design.en.md         # Logging system design (English)
  zero-copy-design.md          # Zero-copy design
  rpc-engine-design.md         # RPC engine design
  why-not-multiprocess.md      # Architecture rationale
  developer-experience.md      # DX philosophy
  subinterp-c-extension-compat.md  # C extension compatibility
```
