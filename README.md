# Pyronova 🔥

**High-performance Python web framework powered by Rust.**

Built on Per-Interpreter GIL (PEP 684) and a Rust async core, Pyronova runs Python handlers across all CPU cores in a single process.

- **902k req/s** on Linux (AMD Ryzen 7840HS, 8C/16T) under TechEmpower-style
  pipelined plaintext (`wrk -t8 -c256 --pipeline 16`).
- **423k req/s** on the standard single-route baseline (`wrk -t4 -c100`,
  no pipeline), **+0.8% vs v1.4.0** on the same hardware.
- **2.7× faster than Robyn** at equal scale (16 workers each), **1/3 the memory**.
- Sustained **400k QPS**: RSS grew **4 MB over 73.8M requests** in 180s
  (≈0 B/req). Zero errors, zero leaks.

### What's new in v2.6 (2026-08-01)

- **C extensions under sub-interpreters — PyO3 0.29.** Upgraded PyO3 0.28→0.29,
  which finally registers `#[pyclass]` types inside sub-interpreters (0.28
  hard-panicked, `pyo3#576`). A native Rust/PyO3 kernel now runs in every own-GIL
  sub-interpreter, zero-copy over Arrow-shaped f64 buffers: **29,260 req/s** on
  Linux (16 cores), 6,647 on macOS. See `examples/c_extension_subinterp.py`.
- **Copy-isolation for numpy / orjson / scikit-learn.** C extensions with
  process-global state (numpy, orjson, lxml) can't share one instance across
  sub-interpreters. A per-worker *physical copy* gives each its own state —
  shared-nothing, no data races, no dependence on upstream thread-safety, and
  (in principle) safer than free-threading. A 16-worker soak
  (`examples/stress_grill.py`, ~680k requests, broad numpy/orjson/sklearn API
  coverage): **zero leak / double-free / deadlock / crash**, ~75 MB/worker. Full
  status + compatibility matrix: `docs/subinterp-c-extension-status.md`.

### What's new in v2.3 (2026-04-23)

- **TPC GIL bridge: single thread → N workers.** I/O-bound `gil=True`
  handlers (numpy, pandas, `time.sleep`, blocking DB drivers) used to
  serialize on one bridge thread — the released GIL went unused while
  work piled up to 503. v2.3 replaces it with a crossbeam-MPMC worker
  pool; each released GIL is picked up by a peer worker. Default 4
  workers, knobs: `PYRONOVA_GIL_BRIDGE_WORKERS`, `_CAPACITY`. Measured
  on a 5 ms `time.sleep` handler at c=64: single-thread ~200 req/s with
  99.96% 503s → 4-thread ~777 req/s with 0 drops.
- **Sub-interpreter DB bridge now works under TPC.** The bridge used to
  panic (`Cannot start a runtime from within a runtime`) the moment a
  DB-backed handler without `gil=True` ran under TPC. Fix: `rt.spawn` +
  `sync_channel` instead of nested `block_on`. Parallelism ceiling
  becomes `min(sub_interp_workers, DATABASE_MAX_CONN)`. Arena
  `/async-db` dropped `gil=True`: 15k → **35k req/s @ c=4096**.
- **`@cached_json(ttl=...)`** — per-worker response cache for public
  read endpoints. First call within TTL runs the handler and stashes
  JSON bytes; hits short-circuit handler + `json.dumps`. 100-row JSON
  on 7840HS: 68k → **336k req/s** (5.0× throughput).
- **Access-log sampling** — `app.enable_logging(sample=100,
  always_log_status=400)`. Logs 1-in-N requests, always logs ≥ 400.
  Avoids the 25-30% throughput tax of full-traffic logging at 400k+ req/s.

### v2.0 — Renamed to Pyronova (BREAKING)

**Every import shape changed.** "Pyre" collided with Meta's type
checker; we renamed to Pyronova.

```python
# v1.x
from pyreframework import Pyre, PyreRequest, PyreResponse, PyreWebSocket, PyreStream
app = Pyre()

# v2.x
from pyronova   import Pyronova, Request,    Response,    WebSocket,    Stream
app = Pyronova()
```

Also changed: PyPI package (`pip install pyronova`), CLI (`pyronova
run/dev/routes`), env vars (`PYRONOVA_HOST`, `PYRONOVA_PORT`,
`PYRONOVA_WORKERS`, `PYRONOVA_TLS_CERT`, …), Rust crate, FFI symbols,
log targets. **No alias layer** — clean break.

Migration is mechanical:

```bash
git grep -l pyreframework | xargs sed -i 's/pyreframework/pyronova/g'
git grep -l '\bPyre\b'    | xargs sed -i 's/\bPyre\b/Pyronova/g'
git grep -l PyreRequest   | xargs sed -i 's/PyreRequest/Request/g'
git grep -l PyreResponse  | xargs sed -i 's/PyreResponse/Response/g'
# repeat for PyreWebSocket, PyreStream, PyreBodyStream, PyreRPCClient, PyreSettings
git grep -l PYRE_         | xargs sed -i 's/PYRE_/PYRONOVA_/g'
```

Full migration table + rationale: [CHANGELOG.md#v200](CHANGELOG.md#v200-2026-04-21).

### Other v2 features (carried over from v1.6 work)

- **`pyronova` CLI** — `pyronova run <module:app>` (prod),
  `pyronova dev <module:app>` (hot-reload + debug), `pyronova routes`
  (print route table). `python -m pyronova …` works the same way.
- **Kubernetes health probes** — `app.enable_health_probes()` registers
  `/livez` (always 200) and `/readyz` (runs every
  `@app.readiness_check("name")`, sync or async; any failure → 503 with
  JSON diagnostics).
- **Prometheus metrics** — `app.enable_metrics()` exposes `GET /metrics`
  with RED-style counters. Counters live in `app.state` so they
  aggregate across sub-interpreter workers.
- **X-Request-ID** — `app.enable_request_id()` mints a UUID if the
  client didn't send one, echoes it back, pushes it into per-request
  `ctx`.
- **Request-scoped context** — `from pyronova.context import ctx`;
  `ContextVar`-backed `ctx.get/set/request_id()`, reset per request.
- **`Settings`** — thin pydantic-settings base (lazy import, opt-in)
  with Pyronova-friendly defaults (case-insensitive, ignore unknown,
  `.env`).
- **TestClient v2** — `params=`, persistent cookie jar,
  `follow_redirects=False`, `OPTIONS`/`HEAD`, `.ok` /
  `.raise_for_status()`, `websocket_connect()` via the `websockets`
  package.
- **Streaming DB cursor** — `pool.fetch_iter(sql, …)` yields Postgres
  rows with O(1) memory.

### Earlier history

For the sub-interpreter memory-leak root-cause fix (v1.5), raw C-API
`_Request` type rebuild, 22-fix adversarial-review pass, and the
v1.4.0 Linux 420k req/s milestone, see [CHANGELOG.md](CHANGELOG.md).

### What others can't do, Pyronova has built-in

- **SharedState** — cross-worker memory sharing without Redis (nanosecond latency)
- **AI-native** — MCP server, MsgPack RPC, SSE streaming
- **Observable** — GIL watchdog, backpressure (503), request timeout (504)

```python
from pyronova import Pyronova

app = Pyronova()

@app.get("/")
def index(req):
    return {"hello": "world"}

@app.get("/io")
async def io_heavy(req):
    import asyncio
    await asyncio.sleep(0.1)
    return "done"

app.run()
```

## Why Pyronova?

### The problem

AI applications in Python need **high throughput and low memory**. An AI agent backend handles thousands of concurrent LLM calls, RAG queries, and tool invocations — all I/O-heavy, all in Python. A quantitative trading gateway processes hundreds of real-time data feeds simultaneously. These workloads demand the performance of C++ with the ecosystem of Python.

But Python has the GIL (Global Interpreter Lock). One lock, one core, no parallelism. Every framework before Pyronova works around this with compromises.

### What others do (and why it's not enough)

**FastAPI** chose async on a single core. Elegant for I/O, but one CPU-heavy request (JSON parsing, Pydantic validation, numpy computation) blocks the entire event loop. Scale via Gunicorn means duplicating the full Python runtime per process. At 15k req/s, you hit the ceiling.

**Robyn** replaced the Python event loop with Rust (Tokio). Better I/O, but Python handlers still run on one GIL. Scaling means 22+ OS processes (`--fast`), eating 447 MB. The Rust layer is fast; the Python layer is the bottleneck.

**Multi-threading** doesn't help. Python threads share one GIL — they take turns, not run in parallel. Adding threads adds context-switch overhead without adding throughput. `threading` in Python is concurrency theater, not parallelism.

**Free-threaded Python** (no-GIL, PEP 703) removes the lock but makes every Python object operation slower (atomic reference counting). The ecosystem isn't ready — most C extensions assume the GIL exists. It trades one problem for another.

### What Pyronova does differently

**Pyronova multiplies the GIL.** Using Per-Interpreter GIL (PEP 684), each worker gets its own independent Python interpreter with its own GIL inside a single process. True multi-core parallelism, zero memory duplication, zero IPC overhead.

```
FastAPI:  1 process × 1 GIL × async tricks     = fast I/O, slow CPU, 15k QPS
Robyn:    16 processes × 16 GILs × 16× memory  = brute force, 156k QPS, 583 MB
Pyronova:     1 process × 16 GILs × shared memory   = elegant, 429k QPS, 189 MB
```

This matters for AI:
- **LLM gateway** — thousands of concurrent `await` calls, each taking 2-5 seconds. Pyronova's async pool handles 133k concurrent I/O operations.
- **Agent orchestration** — multiple agents computing simultaneously. Each sub-interpreter runs at full CPU speed without blocking others.
- **Memory efficiency** — deploy 3x more instances on the same hardware. Pyronova 189 MB for 16 workers vs Robyn 583 MB. On a 512 MB container, Pyronova runs 16 parallel workers; Robyn fits 12 at best.
- **State sharing** — cross-worker `app.state` with nanosecond latency. No Redis, no serialization, no network hop. Session management, caching, and coordination built into the framework.

## Performance

Benchmarked on Linux (AMD Ryzen 7 7840HS, 8C/16T), Python 3.12, wrk -t4 -c100 -d10s.

Full report: [benchmarks/benchmark-14-linux.md](benchmarks/benchmark-14-linux.md)

### Throughput (requests/sec)

| Route | Pyronova | P50 | P99 |
|-------|------|-----|-----|
| GET / (plain text) | **429,000** | 185μs | 579μs |
| GET /json | **405,000** | 196μs | 599μs |
| GET /user/42 (path param) | **395,000** | 201μs | 641μs |
| POST /echo (JSON parse) | **372,000** | 214μs | 785μs |
| GET /compute (CPU-bound) | **379,000** | 211μs | 772μs |

### Pyronova vs Robyn (fair comparison)

Both frameworks given 16 workers on the same hardware. Robyn: `--processes 16 --workers 2`.

| Route | Pyronova (1 proc, 16 sub-interp) | Robyn (16 proc × 2 workers) | Ratio |
|-------|-----|-------|-------|
| GET / | **429k** req/s | 156k req/s | **2.7x** |
| GET /json | **405k** req/s | 155k req/s | **2.6x** |
| GET /user/42 | **395k** req/s | 144k req/s | **2.7x** |
| POST /echo | **372k** req/s | 144k req/s | **2.6x** |
| GET /compute | **379k** req/s | 145k req/s | **2.6x** |

### Resource efficiency

| Resource | Pyronova | Robyn (16 proc) |
|----------|------|-----------------|
| Memory | **189 MB** | 583 MB |
| Processes | **1** | 16 |
| QPS per MB | **2,268 req/s/MB** | 268 req/s/MB |
| Cross-worker state | Built-in (DashMap, nanosecond) | Needs Redis |

Pyronova achieves **2.7x the throughput with 1/3 the memory**. Per-MB efficiency is **8.5x** better.

### Stability (5-minute sustained load)

Sustained 300s stress test, wrk -t4 -c100.

| Metric | Linux (v1.4.0) | macOS (v1.2.0) |
|--------|----------------|----------------|
| Sustained QPS | **400,683 req/s** | 214,641 req/s |
| Total requests | **120,209,758** (120M) | 64,410,189 (64M) |
| Non-2xx responses | **0** | 0 |
| Socket errors | **0** | 0 |
| Memory growth | 169 → 196 MB (+27 MB) | 1712 → 752 KB |
| Max latency | 9.02ms | 39.98ms |

120 million requests, zero errors, zero memory leaks.

### Stress test (c=1024)

| Metric | Result |
|--------|--------|
| QPS | **356,328 req/s** |
| P50 / P99 | 1.40ms / 3.15ms |
| Errors | **0** |

Graceful degradation under extreme concurrency — still 356k QPS with zero errors.

### Pyronova vs Robyn: feature comparison

| Capability | Pyronova | Robyn |
|------------|------|-------|
| **Architecture** | 1 process, N sub-interpreters | N OS processes |
| **SharedState** (cross-worker) | Built-in (DashMap, nanosecond) | Not supported (needs Redis) |
| **MCP Server** (AI tool protocol) | Built-in | Supported (experimental) |
| **MsgPack RPC** | Built-in + magic client | Not supported |
| **SSE Streaming** | Built-in (Stream) | Supported |
| **GIL Watchdog** | Built-in (contention + hold time) | Not supported |
| **Backpressure** (503 overload) | Built-in (bounded channels) | Not supported |
| **Request Timeout** (504) | Built-in (30s zombie reaper) | Not supported |
| **Hybrid Dispatch** (`gil=True`) | Auto-routes to main interpreter | Not supported |
| **TestClient** | Built-in | Not built-in |
| **WebSocket** | Supported | Supported |
| **CORS** | Supported | Supported |
| **Static Files** | Supported (async, no GIL) | Supported |
| **Middleware** | before/after hooks | Supported |
| **Hot Reload** | Supported | Supported |

### Who is Pyronova for?

**AI Agent servers** — Build MCP-compatible tool servers, LLM gateways, and multi-agent orchestration backends. Handle thousands of concurrent LLM streaming responses with SSE. SharedState coordinates agents without Redis.

**Quantitative trading** — Process real-time market data feeds with sub-millisecond P50 latency. Sub-interpreter parallelism runs strategy computations across all cores without GIL contention. WebSocket support for live order book streaming.

**High-throughput microservices** — Internal service mesh nodes that need maximum req/s with minimum memory. MsgPack RPC for binary-efficient inter-service communication. Backpressure (503) protects downstream systems under load spikes.

**Edge/IoT gateways** — Run on memory-constrained devices (512 MB containers, Raspberry Pi). 67 MB for 10 parallel workers vs 447 MB for the alternatives.

## How Pyronova works

```
┌─────────────────────────────────────────────────────────┐
│                    Pyronova Architecture                     │
├─────────────────────────────────────────────────────────┤
│  Python handlers (def / async def / gil=True)           │
│      ↓                                                   │
│  Rust core (Tokio + Hyper)                              │
│  ├── Sync worker pool ──→ N sub-interpreters (OWN_GIL)  │
│  ├── Async worker pool ──→ N asyncio event loops        │
│  ├── Hybrid dispatch ──→ main interpreter (numpy/C ext) │
│  ├── SharedState ──→ DashMap (nanosecond, cross-worker) │
│  └── Backpressure ──→ bounded channels (503 on overload)│
└─────────────────────────────────────────────────────────┘
```

## Feature Comparison

### Routing & Request/Response

| Feature | Pyronova | FastAPI | Robyn |
|---------|------|---------|-------|
| Decorator routing | ✅ | ✅ | ✅ |
| Path params `/hello/{name}` | ✅ | ✅ | ✅ |
| Query params | ✅ | ✅ | ✅ |
| JSON parsing | ✅ | ✅ | ✅ |
| Pydantic validation | ✅ `model=` | ✅ native | ✅ |
| File upload (multipart) | ✅ | ✅ | ✅ |
| Cookie read/write | ✅ | ✅ | ✅ |
| Redirect | ✅ | ✅ | ✅ |
| Custom status/headers | ✅ | ✅ | ✅ |
| Static files | ✅ | ✅ | ✅ |

### Protocols

| Feature | Pyronova | FastAPI | Robyn |
|---------|------|---------|-------|
| HTTP/1.1 | ✅ | ✅ | ✅ |
| HTTP/2 | ✅ | ✅ (Hypercorn) | ✅ |
| WebSocket (text+binary) | ✅ | ✅ | ✅ |
| SSE streaming | ✅ | ✅ | ✅ |

### Middleware & Security

| Feature | Pyronova | FastAPI | Robyn |
|---------|------|---------|-------|
| before/after hooks | ✅ | ✅ middleware | ✅ |
| CORS | ✅ built-in | ✅ | ✅ |
| Body size limit | ✅ 10MB | ✅ | ✅ |
| Backpressure (503) | ✅ | ❌ | ❌ |
| Path traversal protection | ✅ | ✅ | ❌ |
| Worker panic protection | ✅ catch_unwind | N/A | N/A |

### AI & Microservices

| Feature | Pyronova | FastAPI | Robyn |
|---------|------|---------|-------|
| MCP Server (AI tools) | ✅ native | ❌ (third-party) | ✅ |
| MsgPack RPC | ✅ | ❌ | ❌ |
| Content negotiation | ✅ JSON/MsgPack | JSON only | JSON only |
| Magic RPC Client | ✅ | ❌ | ❌ |
| SharedState (no Redis) | ✅ nanosecond | ❌ needs Redis | ❌ needs Redis |

### Concurrency (Pyronova unique)

| Feature | Pyronova | FastAPI | Robyn |
|---------|------|---------|-------|
| Sub-interpreter parallelism | ✅ Per-GIL | ❌ | ❌ |
| Hybrid GIL dispatch | ✅ | ❌ | ❌ |
| Auto sync/async dual pool | ✅ zero-loss | ❌ | ❌ |
| Multi-process | — (not needed) | ✅ Gunicorn | ✅ --fast |

### Observability (Pyronova unique)

| Feature | Pyronova | FastAPI | Robyn |
|---------|------|---------|-------|
| GIL Watchdog | ✅ | ❌ | ❌ |
| Memory RSS monitoring | ✅ | ❌ | ❌ |
| Request counters | ✅ | ❌ | ❌ |
| Structured logging | ✅ | ✅ | ✅ |
| Prometheus `/metrics` | ✅ `app.enable_metrics()` | third-party | ❌ |
| Health probes `/livez` + `/readyz` | ✅ `app.enable_health_probes()` | third-party | ❌ |
| X-Request-ID + request ctx | ✅ `app.enable_request_id()` + `ctx` | third-party | ❌ |

### Developer Experience

| Feature | Pyronova | FastAPI | Robyn | Notes |
|---------|------|---------|-------|-------|
| Type stubs (.pyi) | ✅ | ✅ native | ✅ | |
| TestClient | ✅ full (cookies, redirects, ws) | ✅ | ❌ | |
| Env var config | ✅ | ✅ | ✅ | |
| Hot reload | ✅ `reload=True` / `pyronova dev` | ✅ `--reload` | ✅ | |
| CLI (`run` / `dev` / `routes`) | ✅ `pyronova` | ✅ `fastapi` (newer) | ❌ | |
| Pydantic-settings config | ✅ `Settings` | ✅ | ❌ | |
| OpenAPI docs | — | ✅ | ✅ | Pyronova uses MCP for AI discovery; type hints serve as docs |
| Dependency injection | — | ✅ `Depends()` | ✅ | Pyronova uses `before_request` hooks for the same purpose |

### C Extension Compatibility

Python C extensions (PyO3/Rust, C/C++) use global state that isn't compatible with sub-interpreters (PEP 684). This is a CPython ecosystem limitation, not a Pyronova limitation.

| Library | Sub-interp | gil=True | Why |
|---------|-----------|----------|-----|
| **pydantic** | ❌ | ✅ | pydantic-core is PyO3/Rust, global static state |
| **numpy** | ❌ | ✅ | C extension hardcodes "load once per process" |
| **pandas** | ❌ | ✅ | Depends on numpy |
| **scipy** | ❌ | ✅ | Depends on numpy |
| **orjson** | ❌ | ✅ | PyO3/Rust module |
| **sqlalchemy** | ❌ | ✅ | C extensions (greenlet, cython) |
| **pillow** | ❌ | ✅ | C extension with global state |
| **httpx** | ✅ | ✅ | Pure Python |
| **requests** | ✅ | ✅ | Pure Python |
| **json, hashlib, math** | ✅ | ✅ | stdlib, multi-interp safe |
| **asyncio, threading** | ✅ | ✅ | stdlib, per-interpreter loops |
| **dataclasses, typing** | ✅ | ✅ | Pure Python |
| **re, datetime, os** | ✅ | ✅ | stdlib |

**Rule of thumb:** if `pip show <package>` shows a `.so`/`.pyd` file, it likely needs `gil=True`. Pure Python packages always work in sub-interpreters.

**The fix is simple: add `gil=True` to routes that need C extensions.**

```python
# Fast route — sub-interpreter, 429k req/s, no C extensions needed
@app.get("/fast")
def fast(req):
    return {"hello": "world"}

# Heavy route — GIL main interpreter, full C extension support
@app.post("/analyze", model=AnalysisRequest, gil=True)
def analyze(req, data):
    import numpy as np
    import pandas as pd
    return {"mean": float(np.mean(data.values))}
```

Pyronova auto-detects which routes need GIL and dispatches accordingly. Fast routes stay at 429k req/s; GIL routes get full ecosystem access. Both run concurrently in the same server.

> **When will this be fixed?** When PyO3 and numpy add PEP 684 multi-phase init support. Tracking: [PyO3#3451](https://github.com/PyO3/pyo3/issues/3451), [numpy#24003](https://github.com/numpy/numpy/issues/24003). When they do, these libraries will run at full speed in sub-interpreters — no `gil=True` needed.

> **Why no OpenAPI?** Pyronova targets high-performance APIs and AI agents, not browser-based API explorers. For AI tool discovery, MCP is a more modern protocol. For human developers, Pydantic models + type stubs provide the same contract guarantees.
>
> **Why no dependency injection?** `before_request` hooks solve the same problem (auth, DB connections, shared logic) with less magic and better debuggability. DI adds framework coupling without performance benefit.

## Install

```bash
# From source (requires Rust toolchain + Python 3.13+)
git clone https://github.com/leocaolab/pyronova.git
cd pyronova
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release
```

## Demos

Three production-grade example applications. Each demonstrates a different real-world use case with multiple Pyronova features working together.

```bash
# Install dependencies first
pip install pydantic numpy msgpack httpx

# AI Agent Server — MCP tools, SSE streaming, session memory
python examples/ai_agent_server.py

# Trading Data API — numpy analytics, WebSocket, Pydantic, RPC
python examples/trading_api.py

# Full-stack REST API — CRUD, cookie auth, file upload
python examples/fullstack_api.py
```

### AI Agent Server (`examples/ai_agent_server.py`)

Build MCP-compatible AI tool servers with streaming token output.

```bash
python examples/ai_agent_server.py

# Chat (simulated LLM)
curl -X POST http://127.0.0.1:8000/chat \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "What is Python?", "session_id": "user1"}'

# SSE streaming (token-by-token, like ChatGPT)
curl -N http://127.0.0.1:8000/stream?prompt=hello

# MCP tool discovery (for Claude Desktop)
curl -X POST http://127.0.0.1:8000/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'

# Session memory
curl http://127.0.0.1:8000/memory/user1
```

**Features used:** MCP Server, SSE (Stream), async handlers, SharedState, Pydantic, CORS

### Trading Data API (`examples/trading_api.py`)

Real-time market data with numpy analytics and WebSocket streaming.

```bash
python examples/trading_api.py

# Market quote
curl http://127.0.0.1:8000/market/AAPL

# Submit order (Pydantic validated)
curl -X POST http://127.0.0.1:8000/order \
  -H 'Content-Type: application/json' \
  -d '{"ticker": "AAPL", "side": "buy", "quantity": 100, "price": 150.5}'

# Portfolio analytics (numpy)
curl http://127.0.0.1:8000/analytics/portfolio

# RPC call from another service
python -c "
from pyronova import RPCClient
with RPCClient('http://127.0.0.1:8000') as c:
    print(c.get_signals(tickers=['AAPL', 'TSLA']))
"
```

**Features used:** numpy (gil=True), Pydantic, WebSocket, SharedState, MsgPack RPC, CORS

### Full-stack REST API (`examples/fullstack_api.py`)

Complete CRUD application with authentication and file uploads.

```bash
python examples/fullstack_api.py

# Register + login
curl -X POST http://127.0.0.1:8000/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"username": "alice", "email": "alice@example.com", "password": "secret123"}'

curl -c cookies.txt -X POST http://127.0.0.1:8000/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username": "alice", "password": "secret123"}'

# Create item (authenticated)
curl -b cookies.txt -X POST http://127.0.0.1:8000/items \
  -H 'Content-Type: application/json' \
  -d '{"name": "Widget", "price": 9.99, "tags": ["new"]}'

# List items (with pagination)
curl http://127.0.0.1:8000/items?page=1&per_page=10
```

**Features used:** Pydantic, Cookie auth, File upload, Redirect, SharedState as DB, CORS, structured logging

## Quick Start

### Basic API

```python
from pyronova import Pyronova, Response

app = Pyronova()

@app.get("/")
def index(req):
    return {"message": "Hello from Pyronova!"}

@app.get("/user/{name}")
def greet(req):
    return {"name": req.params["name"]}

@app.get("/search")
def search(req):
    return {"q": req.query_params.get("q", "")}

@app.post("/data")
def receive(req):
    return req.json()

app.run()  # http://127.0.0.1:8000
```

### Async Handlers

`def` and `async def` coexist at full speed — auto-detected, auto-routed.

```python
@app.get("/fast")
def fast(req):              # → sync pool (429k req/s)
    return "instant"

@app.get("/io")
async def io_heavy(req):    # → async pool (133k req/s)
    result = await fetch_from_database()
    return {"data": result}
```

### Pydantic Validation

```python
from pydantic import BaseModel, Field

class Order(BaseModel):
    ticker: str = Field(max_length=5)
    amount: int = Field(gt=0)
    price: float

@app.post("/order", model=Order)
def place_order(req, order: Order):
    return {"total": order.amount * order.price}
# Invalid → 422 with validation errors
```

### CORS

```python
app.enable_cors()  # Allow all origins
app.enable_cors(allow_origins=["https://example.com"], allow_credentials=True)
```

### Cookies

```python
from pyronova.cookies import get_cookie, set_cookie, delete_cookie

@app.get("/login")
def login(req):
    return set_cookie(Response(body="ok"), "session", "abc", httponly=True)

@app.get("/me")
def me(req):
    return {"session": get_cookie(req, "session")}
```

### File Upload

```python
from pyronova.uploads import parse_multipart

@app.post("/upload")
def upload(req):
    f = parse_multipart(req)["file"]
    return {"filename": f.filename, "size": f.size}
```

### Redirect

```python
from pyronova import redirect

@app.get("/old")
def old(req):
    return redirect("/new")
```

### WebSocket

```python
@app.websocket("/ws")
def echo(ws):
    while True:
        msg = ws.recv()
        if msg is None: break
        ws.send(f"echo: {msg}")
```

### SSE Streaming

```python
from pyronova import Stream
import threading

@app.get("/stream", gil=True)
def stream(req):
    s = Stream()
    def gen():
        for token in ["Hello", " ", "World"]:
            s.send_event(token)
        s.close()
    threading.Thread(target=gen).start()
    return s
```

### MCP Server (AI Agent)

```python
@app.mcp.tool(description="Add two numbers")
def add(a: int, b: int) -> int:
    return a + b
# Claude Desktop → http://localhost:8000/mcp
```

### RPC (MsgPack)

```python
@app.rpc("/rpc/compute")
def compute(data):
    return {"result": data["a"] + data["b"]}

# Client:
from pyronova import RPCClient
with RPCClient("http://server:8000") as c:
    c.compute(a=3, b=5)  # → {"result": 8}
```

### Shared State

```python
app.state["key"] = "value"     # Write (any worker)
val = app.state["key"]         # Read (nanosecond, no Redis)
```

### numpy / C Extensions

```python
@app.get("/compute", gil=True)
def compute(req):
    import numpy as np
    return {"mean": float(np.mean(np.random.randn(10000)))}
```

## Configuration

```bash
PYRONOVA_HOST=0.0.0.0 PYRONOVA_PORT=9000 PYRONOVA_WORKERS=16 PYRONOVA_LOG=1 python app.py
```

## Monitoring

```bash
PYRONOVA_METRICS=1 python app.py   # Enable GIL watchdog
```

## Testing

```python
from pyronova.testing import TestClient

client = TestClient(app)
resp = client.get("/")
assert resp.status_code == 200
assert resp.json()["hello"] == "world"
```

## Architecture

```
Python handlers (def / async def / gil=True)
    ↓
Pyronova (Rust core, 12 modules)
├── Tokio runtime (HTTP/1+2, WebSocket, SSE)
├── Sub-interpreter pool (N independent GILs)
│   ├── Sync workers (def → 429k req/s)
│   └── Async workers (async def → 133k req/s)
├── Hybrid GIL dispatch (gil=True → numpy/C extensions)
├── SharedState (DashMap, cross-worker, nanosecond)
├── GIL Watchdog (contention + hold time + queue depth)
└── Backpressure (bounded channels, 503 on overload)
```

## Sub-interpreter Safe Ecosystem

Pyronova's sub-interpreters deliver 429k req/s, but C extensions (Pydantic, NumPy, Pandas) can't run in them. Instead of fighting the ecosystem, Pyronova offers a **Golden Path**: modern, pure-Python alternatives that are **not just safe — they're faster**.

| Category | Traditional (needs `gil=True`) | Golden Path (sub-interp safe) |
|----------|-------------------------------|-------------------------------|
| Validation | Pydantic V2 | `msgspec` / `mashumaro` |
| Data | Pandas + NumPy | `Polars` |
| HTTP Client | requests | `httpx` |
| JSON | orjson | stdlib `json` / `msgspec` |
| Database | psycopg2 | `psycopg` v3 (pure Python) |

**The rule**: pure Python = sub-interp safe. C extensions = use `gil=True`.

### Not just safe — faster

Same endpoints, same logic. Pyronova with sub-interp safe libs vs FastAPI with the traditional Pydantic stack:

| Test | FastAPI + Pydantic | Pyronova + Golden Path | Speedup | Latency reduction |
|------|-------------------|-------------------|---------|-------------------|
| Health Check | 9,031 req/s | **214,714 req/s** | **23.8x** | 11.1ms → 0.38ms |
| JSON Echo | 7,602 req/s | **209,012 req/s** | **27.5x** | 13.2ms → 0.40ms |
| CPU-bound (10k moving avg) | 263 req/s | **599 req/s** | **2.3x** | 374ms → 165ms |
| Validation | 7,345 req/s | **208,439 req/s** | **28.4x** | 13.8ms → 0.41ms |

The traditional stack is single-threaded — the GIL serializes every request. Pyronova runs 10 sub-interpreters in parallel, each with its own GIL. The Golden Path libraries are pure Python, so they load cleanly in every interpreter. The result: **24-28x throughput, 29-34x lower latency**.

Run the benchmark yourself: `bash benchmarks/run_comparison.sh`

> *"Pyronova doesn't force you to change, but it rewards you when you do."*

Full ecosystem guide: [docs/subinterp-safe-ecosystem.md](docs/subinterp-safe-ecosystem.md)

## Limitations

Pyronova's sub-interpreter architecture delivers extreme performance but comes with specific constraints. All are caused by **CPython ecosystem limitations, not Pyronova design choices**, and all have clear workarounds.

### C extensions in sub-interpreters

**What:** Libraries built with PyO3 (Rust) or C/C++ extensions cannot be imported inside sub-interpreters. This includes pydantic, numpy, pandas, orjson, and most compiled packages.

**Why:** CPython's PEP 684 requires extensions to declare multi-interpreter support via `Py_MOD_PER_INTERPRETER_GIL_SUPPORTED`. Most libraries haven't done this yet. PyO3 uses global static state that conflicts with multiple interpreters.

**Workaround:** Add `gil=True` to routes that need these libraries. They run on the main interpreter with full ecosystem access while other routes run at 429k req/s on sub-interpreters.

```python
@app.get("/fast")                    # Sub-interpreter: 429k req/s
def fast(req): return "hello"

@app.post("/analyze", gil=True)      # Main interpreter: numpy works
def analyze(req):
    import numpy as np
    return {"result": float(np.mean([1,2,3]))}
```

**When fixed:** When PyO3 ([#3451](https://github.com/PyO3/pyo3/issues/3451)) and numpy ([#24003](https://github.com/numpy/numpy/issues/24003)) add PEP 684 support.

### Python 3.13+ required

**What:** Pyronova requires Python 3.13 or later.

**Why:** Per-Interpreter GIL (PEP 684) was introduced in Python 3.12, but
v1.5.0 onwards uses `PyThreadState_GetUnchecked` and the new tstate
rebinding helper added in CPython 3.13 to close a per-request memory
leak. Earlier 3.12 builds cannot run this code path safely.

**Workaround:** None. Python 3.13+ is required. Consider using [pyenv](https://github.com/pyenv/pyenv) to manage multiple Python versions.

### Build from source

**What:** Pyronova must be compiled from source using Rust and Maturin. No pre-built wheels on PyPI yet.

**Why:** The project is pre-release. PyPI binary wheels for multiple platforms require CI/CD infrastructure.

**Workaround:** Install Rust (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`) and build with `maturin develop --release`.

### No OpenAPI auto-documentation

**What:** Pyronova doesn't generate Swagger/OpenAPI documentation from route definitions.

**Why:** Pyronova targets high-performance backends and AI agents, not browser-based API explorers. For AI tool discovery, Pyronova provides native MCP (Model Context Protocol) support, which is purpose-built for AI applications. For human developers, Pydantic models and type stubs provide compile-time contract guarantees.

### Single-process only

**What:** Pyronova runs as a single OS process. No multi-process mode like Gunicorn or Robyn `--fast`.

**Why:** This is by design. Sub-interpreters provide multi-core parallelism within one process, with 6.7x less memory than multi-process alternatives. SharedState works without Redis. Adding multi-process would destroy these advantages.

## Requirements

- Python 3.13+ (PEP 684 sub-interpreters + `PyThreadState_GetUnchecked`)
- Rust toolchain (build from source)
- macOS or Linux

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
