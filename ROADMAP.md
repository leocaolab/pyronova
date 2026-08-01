# Pyronova Roadmap

## Next — Built-in library copy-isolation `app.isolate()` (planned) ⭐

C extensions with process-global state (numpy, orjson, lxml, scipy, scikit-learn)
can't share one instance across sub-interpreters. The workaround — a per-worker
physical copy — works and is soak-proven (zero leak/double-free/deadlock, ~75 MB/worker;
see `docs/subinterp-c-extension-status.md`), but today it's **manual** (`examples/stress_grill.py`).

Make it a first-class API: `app.isolate("numpy", "orjson", ...)`. The engine clones each
lib per worker (COW: `cp --reflink` / APFS `cp -c`) and wires `sys.path` + the multi-interp
override at sub-interp init, so user handlers just `import numpy`. Copies/paths/counters
never touch user code. (Also revisit Linux `dlmopen` namespaces as a lower-memory alternative,
and `_Request` → PyO3 0.29 `#[pyclass]` to drop the hand-written FFI.)

## Phase 1 — Skeleton (DONE ✓) — 2026-03-23

- Tokio + Hyper HTTP server
- PyO3 0.28 bridge
- matchit routing
- GIL detach/attach
- Benchmark: 69k req/s vs Robyn 27k (2.5x), 10MB vs 35MB RSS

## Phase 2 — Optimization (DONE ✓) — 2026-03-23

- Multi-worker Tokio runtime (configurable)
- Keep-alive + pipeline flush
- Graceful shutdown (Ctrl+C)
- Rust-side JSON serialization (serde_json, skip Python json.dumps)
- dict/list 直接返回支持
- Connection error 静噪
- Benchmark: GIL 100k, SubInterp 216k, Robyn 76k

## Phase 3 — Developer Experience (DONE ✓) — v0.3.0, 2026-03-24

- [x] `Pyronova` 装饰器语法 (`@app.get("/")`)
- [x] `Response` — 自定义 status code / headers / content-type
- [x] `req.headers` — 请求头读取
- [x] `req.query_params` — 查询参数解析
- [x] `before_request` / `after_request` 中间件
- [x] `@app.fallback` — 自定义 404 handler
- [x] 静态文件服务 (`app.static("/prefix", "./dir")`)
- [x] 内置请求日志 (`app.enable_logging()`)
- [x] PATCH / OPTIONS / HEAD 方法支持
- [x] PyPI 就绪 (pyproject.toml + classifiers)
- Benchmark: 213k req/s SubInterp, 100k GIL (零性能回归)

## Phase 4 — True Parallelism (DONE ✓) — 2026-03-23

目标：支持两种并行模式，用户可选。

### 模式 A: Free-threaded Python (python3.14t, no-GIL)
- PyO3 0.28 已支持
- 改动最小，装 python3.14t 即可
- 所有 Tokio worker 线程共享同一解释器，真并行调用 handler
- 优先实现

### 模式 B: Per-Interpreter GIL (子解释器)
- 每个 worker 线程绑定一个独立子解释器，各自有独立 GIL
- 提供进程级隔离 + 线程级性能
- 架构：Tokio Worker N → Sub-Interpreter N (GIL-N) → handler()
- Benchmark: 达到纯 Rust 97% 性能

#### 子解释器的核心难题：PyO3 不支持

PyO3 0.28 明确阻止子解释器（`#[pymodule]` 会检查 interpreter ID，第二次 import 直接 ImportError）。
根本原因：PyO3 内部大量全局 `static` 状态，跨解释器共享会 unsound。

Tracking issues:
- https://github.com/PyO3/pyo3/issues/576
- https://github.com/PyO3/pyo3/issues/3451
- https://github.com/PyO3/pyo3/issues/4570

#### 解法：绕过 PyO3，直接 FFI

不 fork PyO3，而是在子解释器中用 raw `pyo3::ffi` 调用 CPython C API：
- `Py_NewInterpreterFromConfig` + `PyInterpreterConfig_OWN_GIL` 创建独立 GIL 子解释器
- `PyRun_String` 执行用户脚本（过滤掉框架代码）
- `PyDict_GetItemString` 提取 handler 函数指针
- `PyObject_Call` 直接调用 handler
- 纯 Python `_Request` / `_Response` 替代 PyO3 的 `#[pyclass]`

风险：裸指针、手动引用计数、无 RAII，但性能极佳。

#### 里程碑（已完成）
1. ~~Fork PyO3~~ → 改为 raw FFI 方案
2. PoC: 10 个子解释器并行处理请求 ✓
3. 集成到 Pyronova: worker pool + interpreter pool ✓
4. Benchmark: SubInterp 216k vs Robyn 76k (2.8x) ✓

## Phase 5 — 生产级子解释器 (DONE ✓ Step 1) — v0.3.1, 2026-03-24

原始问题清单（Phase 4 遗留的 raw FFI PoC 隐患）：

| 问题 | 风险等级 | 状态 | 说明 |
|------|---------|------|------|
| 裸 `*mut ffi::PyObject` 指针 | 高 | ✅ 已修复 | `PyObjRef` RAII 包装，Drop 自动 DECREF |
| 手动 `Py_INCREF/DECREF` | 高 | ✅ 已修复 | `from_owned` / `from_borrowed` / `into_raw` 安全接口 |
| 脚本过滤是字符串匹配 | 中 | ✅ 已修复 | 改用 Python `ast.parse` + `ast.unparse`，精准过滤 |
| `_Request` 是纯 Python 重写 | 中 | 🔄 保留 | 跟主解释器行为一致，暂无问题 |
| 无 `Py_EndInterpreter` 清理 | 中 | ✅ 已修复 | worker 线程退出时自动 `Py_EndInterpreter` |
| 子解释器里不能用 PyO3 扩展 | **致命** | 🔄 Step 2 | 需要 fork PyO3，见下方 |
| 队头阻塞 (Round-robin + Mutex) | 高 | ✅ 已修复 | 改为 `crossbeam-channel` 多消费者池，真负载均衡 |
| 静态文件目录穿越漏洞 | 高 | ✅ 已修复 | `trim_start_matches('/')` + `..` 检测 |
| Middleware 破坏二进制响应 | 中 | ✅ 已修复 | 非 UTF-8 body 使用 `PyBytes` 而非强转空字符串 |

### Step 1: 安全抽象层 `pyronova-interp` (DONE ✓)

在 `pyo3::ffi` 之上包一层（`src/interp.rs`, 820 行）：
- [x] `PyObjRef` RAII 包装引用计数（Drop 自动 DECREF）
- [x] `from_owned` / `from_borrowed` / `into_raw` 安全接口
- [x] `py_str` / `py_bytes` / `py_str_dict` 辅助构造器
- [x] 子解释器退出时自动 `Py_EndInterpreter`
- [x] Python AST 解析替代字符串行过滤（支持装饰器语法）
- [x] `crossbeam-channel` 多消费者 worker pool（消除队头阻塞）
- [x] `tokio::sync::oneshot` 异步响应（Tokio 任务不阻塞）

### 工程重构：模块化拆分 (DONE ✓)

从 981 行 God File 拆分为 8 个职责单一的模块：

```
src/
├── lib.rs          18 行   ← #[pymodule] + mod 声明
├── types.rs       116 行   ← Request, Response, extract_headers
├── json.rs         44 行   ← py_to_json_value (Rust-side JSON 序列化)
├── router.rs       70 行   ← RouteTable, SharedRoutes
├── response.rs    164 行   ← extract_response_data, build/error/404 response
├── handlers.rs    218 行   ← handle_request (GIL), handle_request_subinterp
├── static_fs.rs    69 行   ← try_static_file, mime_from_ext
├── app.rs         342 行   ← PyronovaApp, run_gil(), run_subinterp()
└── interp.rs      820 行   ← PyObjRef RAII, channel pool, AST filter
```

Benchmark: SubInterp 215k (channel pool, -0.5% vs mutex), GIL 104k (+3%), Robyn 83k。零回归。

### Step 2: 精准 Fork PyO3（中期，解锁第三方扩展）

**不需要重写整个 PyO3**，只改两处：
1. `pymodule.rs` — 去掉 `make_module` 中的 `AtomicI64` interpreter ID 检查
2. `#[pymodule]` 的 `static` 状态 — 迁移到 `PyModule_GetState`（per-interpreter 隔离）

改完后：
- Pyronova 自己的 `#[pymodule]` 能在子解释器中加载
- `Request` 等 `#[pyclass]` 可以直接传入子解释器，不再需要 `_Request` 纯 Python 替身
- 声明了 `Py_MOD_PER_INTERPRETER_GIL_SUPPORTED` 的第三方 C 扩展也能用（numpy、orjson 等正在逐步加这个声明）

### Step 3: 等待/贡献上游（长期）

- PyO3 tracking issue: https://github.com/PyO3/pyo3/issues/3451
- 如果 Step 2 的 fork 稳定，可以向 PyO3 上游提 PR
- ~~关注 numpy/orjson/pydantic 等库的 `Py_MOD_PER_INTERPRETER_GIL_SUPPORTED` 适配进度~~
- 🔬 **PEP 684 子解释器第三方库兼容性测试** (Python 3.14, 2026-03-27)
  - 30/30 库通过 sub-interpreter import + 基本操作测试
  - 8 并发 sub-interpreter 短时压测通过
  - `check_multi_interp_extensions: 1` 无需关闭
  - **已通过基本测试（import + 简单操作）：**
    - 数据科学：numpy 2.4, pandas 3.0, scipy 1.17, polars 1.39, pyarrow 23.0
    - ML：scikit-learn 1.8, xgboost 3.2, lightgbm 4.6
    - 序列化：orjson 3.11, msgpack 1.1, json, csv
    - 网络：requests, aiohttp, websockets
    - 数据库：sqlite3
    - 标准库：collections, dataclasses, typing, math, statistics, decimal, datetime, re, hashlib, struct, ctypes, threading, queue, logging
  - ⚠️ **风险警示：**
    - 测试仅覆盖了 import 和基本操作，**不代表所有使用场景都安全**
    - PEP 684 sub-interpreter 的 C 扩展兼容性仍是新领域，可能存在：
      - **double free / use-after-free**：C 扩展内部的全局状态在解释器销毁时可能被多次释放
      - **全局状态泄漏**：某些 C 扩展可能在 `PyModule_GetState` 外维护隐式全局状态，跨解释器共享导致数据污染
      - **内存泄漏**：解释器销毁时 C 扩展分配的内存可能不被正确回收
      - **线程安全 + 解释器安全不等价**：一个库声明了 thread-safe 不代表它是 sub-interpreter-safe
    - **未测试的库默认视为不兼容**，在生产环境使用前需要独立验证
    - 建议在非关键路径（如数据预处理）中使用，交易决策路径尽量只依赖已验证的库
    - 长时间运行（数小时/数天）的稳定性尚未验证，建议监控内存使用

## Phase 6 — 协议突破 (进行中) — v0.4.0

### WebSocket (DONE ✓) — 2026-03-24

原生 WebSocket 支持，基于 `tokio-tungstenite`。

**架构**：
```
Client ←WebSocket→ Tokio (async) ←channels→ Python handler thread (sync)
                    ↑                         ↑
            tokio-tungstenite          ws.recv() / ws.send()
            async read/write           std::sync::mpsc (incoming)
                                       tokio::sync::mpsc (outgoing)
```

- 每个 WebSocket 连接分配一个 OS 线程运行 Python handler
- `WebSocket` pyclass 提供 `recv()` (阻塞) / `send(msg)` / `close()` 接口
- 自动处理 Ping/Pong、连接关闭
- 与 HTTP 路由共存，同一端口同时服务 HTTP + WebSocket
- GIL 模式和 Hybrid 模式均支持

**API**：
```python
@app.websocket("/ws")
def echo(ws):
    while True:
        msg = ws.recv()    # 阻塞等待
        if msg is None:
            break          # 客户端断开
        ws.send(f"echo: {msg}")
```

### 性能与安全加固 (DONE ✓) — 2026-03-24

- [x] `spawn_blocking` 隔离 GIL 调用，避免 Tokio Worker 线程饥饿
- [x] 有界通道 (bounded channel) + `try_send` 背压 → 503 快速失败
- [x] `TCP_NODELAY` 禁用 Nagle 算法
- [x] 请求体 10MB 上限 → 413 Payload Too Large
- [x] orjson 自动检测（10-40x JSON 序列化加速）
- [x] async def 检测 → 清晰错误消息（非静默 `<coroutine>` 返回）
- [x] Type stubs (`engine.pyi`) → IDE 自动补全
- [x] `call_handler_with_hooks()` 共享函数消除代码重复

### SSE 流式传输 (DONE ✓) — 2026-03-24

AI Agent 核心基础设施：token-by-token 流式输出。

```python
@app.get("/stream", gil=True)
def stream(req):
    s = Stream()
    def generate():
        for token in llm.generate(prompt):
            s.send_event(token)
        s.close()
    threading.Thread(target=generate).start()
    return s
```

**架构：**
```
Python handler thread → Stream.send_event()
                       → mpsc::UnboundedSender
                       → Tokio StreamBody (chunked transfer)
                       → HTTP client (EventSource)
```

- `Stream` pyclass: `send(data)` / `send_event(data, event, id)` / `close()`
- 通道在 `__init__` 时创建，`send()` 立即可用（无需等待 connect）
- `BoxBody` 统一响应类型（`Either<Full, StreamBody>`）
- SSE 格式：`event: xxx\ndata: xxx\n\n`

### 已完成
- [x] MCP Server 装饰器（`@app.mcp.tool()`、`@app.mcp.resource()`、`@app.mcp.prompt()`）
- [x] WebSocket 二进制消息支持
- [x] AST 过滤替换为环境变量方案 (`PYRONOVA_WORKER=1`)
- [x] Pydantic 自动绑定 (`model=` 参数)

### 待完成
- [ ] HTTP/2 full duplex（deps 里已有 hyper http2 feature）
- [ ] io_uring backend (Linux, monoio)
- [ ] HTTP/3 QUIC
- [ ] Native gRPC (tonic crate)

## Phase 7 — Async Handler (DONE ✓) — 2026-03-24

### Phase 7.1: 基础 async handler (DONE ✓)

- [x] 检测 handler 返回 coroutine (`PyCoro_CheckExact`)
- [x] GIL 模式：`asyncio.run(coro)` in `spawn_blocking`
- [x] SubInterp 模式：每个 worker 维护独立 asyncio event loop，`loop.run_until_complete(coro)`
- [x] `async def` handler 功能完全可用（两种模式）

### Phase 7.2: Native Async Bridge (DONE ✓)

- [x] C-FFI bridge (`pyronova_recv`/`pyronova_send`) 释放 GIL 在 channel wait 期间
- [x] Dual worker pool: sync workers + async workers 自动检测分配
- [x] 133k req/s on I/O-bound (sleep 1ms) — 超越 Robyn 92k

## Phase 8 — 跨子解释器状态共享 (DONE ✓) — 2026-03-24

核心问题：PEP 684 子解释器内存隔离，10 个子解释器中的 `GLOBAL_CACHE = {}` 是 10 个独立的空字典。

### 设计原则

1. **状态下沉到 Rust** — Python 只是调用方，共享数据在 Rust 堆上
2. **绝不跨解释器共享 PyObject** — 边界处 Rust 原生类型 ↔ Python 对象互转
3. **无锁高并发** — `DashMap` 分片锁，不同 Key 零竞争

### 实现 (src/state.rs)

```rust
#[pyclass]
pub struct SharedState {
    inner: Arc<DashMap<String, Vec<u8>>>,  // 支持 string + binary
}
```

```python
# Python: 像普通字典一样使用
app.state["session:user_123"] = json.dumps({"role": "admin"})
session = json.loads(app.state["session:user_123"])
len(app.state)          # 条目数
"key" in app.state      # 判断存在
del app.state["key"]    # 删除
app.state.keys()        # 所有 key
app.state.set_bytes()   # 二进制写入
app.state.get_bytes()   # 二进制读取
```

### 用例

**Session 管理（零 Redis）：**
```python
@app.get("/login/{user}", gil=True)
def login(req):
    app.state[f"session:{req.params['user']}"] = json.dumps({"token": "xyz"})
    return {"ok": True}

@app.get("/auth/{user}", gil=True)
def auth(req):
    try:
        return json.loads(app.state[f"session:{req.params['user']}"])
    except KeyError:
        return Response(body={"error": "unauthorized"}, status_code=401)
```

**跨路由数据共享（numpy 计算结果 → 其他路由读取）：**
```python
@app.get("/compute", gil=True)
def compute(req):
    import numpy as np
    result = float(np.mean(np.random.randn(10000)))
    app.state["latest_mean"] = json.dumps({"mean": result})
    return {"computed": True}

@app.get("/latest", gil=True)
def latest(req):
    return json.loads(app.state["latest_mean"])
```

**实时计数器：**
```python
@app.get("/hit", gil=True)
def hit_counter(req):
    count = int(app.state.get("hits") or "0") + 1
    app.state["hits"] = str(count)
    return {"hits": count}
```

### 性能

| 操作 | 吞吐量 | 延迟 |
|------|--------|------|
| state read (GIL route) | 73,720 req/s | 4.4ms avg |
| state write | 同级 | 纳秒级 DashMap 插入 |
| sub-interp fast route (同时) | 214,841 req/s | 0.96ms avg |

### 架构优势 vs Redis

| 维度 | Pyronova SharedState | Redis |
|------|-----------------|-------|
| 延迟 | **纳秒级** (内存直读) | 毫秒级 (网络 I/O) |
| 部署 | 零依赖 | 需要独立 Redis 进程 |
| 数据类型 | String / bytes | 丰富但有序列化开销 |
| 适用场景 | Session、缓存、计数器 | 分布式、持久化 |

### 当前限制 & 拓展路线

- 当前：`app.state` 需要 `gil=True` 路由（PyO3 pyclass 不可在子解释器中直接使用）
- 计划：通过 FFI 注入 `_state` 到子解释器 globals，实现原生 sub-interp 访问
- 拓展 1：`moka` Cache — 带 TTL 过期（Session 自动清理）
- 拓展 2：`app.state.set_bytes()` 共享二进制数据（行情 Tensor）

## Phase 9 — Production Hardening (DONE ✓) — v1.2.0, 2026-03-25

### 代码质量
- [x] Bootstrap 脚本从 Rust 字符串抽离到 `_bootstrap.py` (via `include_str!`)
- [x] 删除 `filter_script_ast` 死代码
- [x] CORS/logging 从全局静态变量迁移到 `PyronovaApp` 实例字段
- [x] `PyObjRef::Drop` 加 `debug_assert!(PyGILState_Check())` GIL 安全断言
- [x] `InterpreterPool::Drop` join worker threads (修复 Ctrl+C segfault)
- [x] `cargo fmt` 全量格式化 + `cargo clippy` 32 个 warning 清零
- [x] PyO3 `downcast` → `cast` 迁移 (14 处)
- [x] 热更新改用 `watchfiles` (OS 原生事件) + 降级轮询跳过 `.venv`/`node_modules`

### 测试
- [x] 21 Rust 单元测试 (response builders, MIME, headers, query params)
- [x] 54 Python pytest 测试 (MCP, cookies, TestClient, RPC, static files, WebSocket, async, logging)
- [x] 22 集成测试 (GIL + sub-interp 全功能端到端)
- [x] 5 分钟稳定性压测: 64M 请求, 零内存泄漏, 零崩溃

### CI/CD
- [x] GitHub Actions: cargo test → pytest → integration (Python 3.13/3.14)
- [x] Blocking `cargo fmt --check` + `cargo clippy -- -D warnings`
- [x] PyPI release workflow: v* tag → 4 平台 wheels → 自动发布
- [x] `/test` Claude Code 命令

## Phase 10 — Next (规划中)

### 短期

#### Benchmark 优化（HttpArena / TechEmpower 后续 PR）
当前 PR 落地后的下一轮优化目标，按低分项倒序排列：

- [ ] **CRUD (5.6 / 12k req/s)** — 见 [`docs/optimize-crud.md`](docs/optimize-crud.md)
  - **前置**: SharedState C-FFI bridge（`src/bridge/state_bridge.rs`，~150 LOC Rust + ~30 LOC Python）—— 把 sub-interp 里的空 mock `SharedState` 换成 bridge-backed 实现，跟 `_PgPool` 同模式
  - 关键 unlock: `_CRUD_CACHE` Python dict → `SharedState`，去掉所有 `/crud/*` 的 `gil=True`
  - 预期: 12k → 60–100k（5–10x，从 GIL-bridge 4 worker 池解放到 sub-interp 64 worker）
- [ ] **Async DB (13.3 / 27k req/s)** — 见 [`docs/optimize-async-db.md`](docs/optimize-async-db.md)
  - 短期: bulk row decode + prepared statement cache (+15–20%)
  - 中期: async handler + 并发 query（需要 phase-7.2 async bridge 通到 DB）
- [ ] **JSON (30.8 / 329k req/s)** — 见 [`docs/optimize-json.md`](docs/optimize-json.md)
  - 关键: 跳过 `serde_json::Value` 中间树，Python → bytes 直写
  - 预期 +30–50%；CRUD HIT 路径和 async-db 响应都吃这个红利
- [ ] **Static (23.3 / 250k req/s)** — 见 [`docs/arena-async-db-and-static.md`](docs/arena-async-db-and-static.md) Part 2 (Design B)
  - 设计已就绪未落地: 注册时 walk 目录预加载进 fast-response map
  - 预期: 250k → 500–800k（绕过 `try_static_file` 慢路径 + 两次 canonicalize syscall）

执行顺序建议: optimize-json #1 先落（其余三项的杠杆都建在它上面）→ static preload（独立、无依赖）→ CRUD 迁 SharedState → async-db bulk decode/prepared stmt → async-db async bridge（最后，需 phase-7.2）。

#### 其他短期
- [ ] HTTP/2 full duplex
- [ ] OpenTelemetry tracing 集成
- [ ] Rate limiting middleware
- [ ] Dependency injection (轻量级, 基于 `before_request`)
- [ ] 子解释器第三方库兼容性文档 — numpy/pandas 已通过测试 (见下方)

### 中期
- [ ] io_uring backend (Linux, monoio)
- [ ] Native gRPC (tonic crate)
- [ ] Connection pooling for upstream services
- [ ] PyO3 上游 sub-interpreter 支持 (tracking: PyO3#3451)
- [ ] **Arena allocator (bumpalo) for per-request allocations**
  - Target: Bytes/header-extract/body-collect buffers on the Rust-only
    side of the request. Excludes PyronovaRequest itself (pyclass,
    can't cross Arena lifetime).
  - Expected gain: 5-10% on workloads where mimalloc cross-thread
    arena contention shows up (mostly relevant at >1M rps per core).
    On darwin localhost, TCP stack dominates — Arena改不动那层天花板.
  - Gating decision: wait for Linux 64-core benchmark data before
    committing effort. If per-worker loses >5% to allocator
    metadata contention there, revisit.

### 长期
- [ ] HTTP/3 QUIC
- [ ] Free-threaded Python (PEP 703) dual-mode support
- [ ] Cluster mode (multi-process + shared state via shared memory)
- [ ] Robyn/Flask/FastAPI 迁移兼容层

## 长期愿景
- 成为第一个同时支持 free-threaded 和 per-interpreter GIL 的 Python web 框架
- 在交易系统场景中实现微秒级 WebSocket + Orderbook 处理
- I/O + CPU + 内存 三杀所有 Python web 框架

---

## 性能历史

| 日期 | 版本 | SubInterp | GIL | Robyn | 里程碑 |
|------|------|-----------|-----|-------|--------|
| 2026-03-23 | v0.1.0 | — | 69k | 27k | Phase 1: 骨架，2.5x Robyn |
| 2026-03-23 | v0.2.0 | 216k | 100k | 76k | Phase 2+4: 子解释器，2.8x Robyn |
| 2026-03-24 | v0.3.0 | 213k | 100k | 76k | Phase 3: DX 功能补全，零回归 |
| 2026-03-24 | v0.3.1 | 215k | 104k | 83k | Phase 5.1: RAII + channel pool + 模块化拆分 + 安全修复 |
| 2026-03-24 | v0.4.0 | 215k | 104k | 83k | Phase 6: Native WebSocket + Hybrid GIL + 全面压测(54场景) |
| 2026-03-24 | v0.4.0+ | 217k | 78k(47k IO) | 81k | spawn_blocking + 背压 + TCP_NODELAY, GIL IO +535%, 胜10/14场景 |
| 2026-03-24 | v0.5.0 | — | 74k state | — | Phase 7 async + Phase 8 SharedState + DX (.pyi, async detect) |
| 2026-03-24 | v0.5.0+ | — | — | — | Phase 6: SSE streaming (Stream) for AI Agent token output |
| 2026-03-24 | v0.5.0 | 217k/67MB | 53k/GIL=0μs | 81k/~440MB | GIL Watchdog + 内存监控, SubInterp GIL=0μs 验证, 内存 6.6x 优势 |
| **2026-03-25** | **v1.2.0** | **215k/752KB** | — | — | **Phase 9: 64M req stability, 97 tests, zero clippy warnings** |
