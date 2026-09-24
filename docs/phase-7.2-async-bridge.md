# Phase 7.2: Native Async Bridge — 设计文档

> **已被部分取代（2026-09，Layer 2，`docs/design/real-engine-in-workers.md`）：** 下文的 `pyronova_recv` / `pyronova_send` C-FFI 函数（用 `PyCFunction_NewEx` 注入 worker 的 globals）已删除。worker 现在导入真正的 `pyronova.engine`，async 引擎通过 `pyronova.engine._worker_recv` / `_worker_send`（`src/python/worker_api.rs`）与 Rust 通信，等待通道时同样释放 GIL。fetcher 线程 + `run_coroutine_threadsafe` 的结构保留。


> 目标：Sub-interpreter I/O 并发从 8k → 70k+ req/s（追平/超越 Robyn 的 86k）

## 当前瓶颈分析

### 为什么 Sub-interp sleep(1ms) 只有 8k req/s

```
当前模型：
  Rust worker thread → recv(request) → acquire GIL → Python handler()
  → run_until_complete(coro) → 阻塞等待 1ms → release GIL → send(response)
  → recv(next request)

10 workers × (1000 req/s per worker) = 10,000 理论上限
实测 7,892 ≈ 触碰物理天花板
```

每个 worker 线程同一时刻只能运行一个协程。`run_until_complete` 是同步阻塞调用。

### Robyn 为什么能跑 86k

Robyn 用 `async def` + 原生 asyncio，一个线程内 Event Loop 并发挂起数千个 `await asyncio.sleep`。
底层 OS 线程不阻塞，协程调度在用户态完成。

## 目标架构：主客场互换

```
当前（Rust 驱动 Python）：
  Rust thread ──→ call_handler() ──→ 等结果 ──→ 返回
                     ↓
               Python: handler(req)
               Python: await sleep(1ms)  ← 阻塞 Rust 线程！

目标（Python 驱动异步）：
  Rust channel ──→ pyronova_recv() ──→ Python asyncio loop
                                      ↓
                                 create_task(handler(req))
                                 create_task(handler(req))  ← 数千个并发！
                                 create_task(handler(req))
                                      ↓
                                 await sleep(1ms)  ← 挂起，不阻塞线程
                                      ↓
                                 pyronova_send(req_id, response)  ──→ Rust oneshot
```

## 核心设计：3 个组件

### 组件 1：请求注册表（Rust 全局状态）

```rust
pub struct WorkerState {
    pub rx: crossbeam_channel::Receiver<WorkRequest>,
    pub response_map: Mutex<HashMap<u64, oneshot::Sender<...>>>,
    pub next_req_id: AtomicU64,
}

static WORKER_STATES: OnceLock<Mutex<HashMap<usize, Arc<WorkerState>>>> = OnceLock::new();
```

每个请求分配唯一 `req_id`，存入 `response_map`。
Python 处理完后调 `pyronova_send(req_id, response)` 通过 `oneshot` 唤醒 Tokio。

### 组件 2：C-FFI 桥接函数

```rust
// pyronova_recv(worker_id) → (req_id, handler_idx, method, path, query, body) or None
unsafe extern "C" fn pyronova_recv_cfunc(...) -> *mut PyObject {
    // 关键：释放 GIL 后再 recv()
    let saved = ffi::PyEval_SaveThread();      // ← 释放 GIL
    let req = state.rx.recv();                  // ← 阻塞等待，但不持有 GIL
    ffi::PyEval_RestoreThread(saved);           // ← 拿回 GIL
    // 打包成 Python tuple 返回
}

// pyronova_send(worker_id, req_id, status, content_type, body)
unsafe extern "C" fn pyronova_send_cfunc(...) -> *mut PyObject {
    // 从 response_map 取出 oneshot::Sender，发送响应
    let _ = tx.send(Ok(SubInterpResponse { ... }));
}
```

通过 `PyCFunction_NewEx` 注册到子解释器 globals。

### 组件 3：Python 异步引擎

```python
import asyncio, threading

async def _process_request(req_id, handler_idx, method, path, query, body):
    handler = globals()[HANDLER_NAMES[handler_idx]]
    req = _Request(method, path, {}, query, body, {})
    res = handler(req)
    if asyncio.iscoroutine(res):
        res = await res                    # ← 完美挂起，不阻塞线程！
    # before/after hooks...
    _pyronova_send(WORKER_ID, req_id, 200, "text/plain", str(res))

def _fetcher_thread(loop):
    """后台线程：从 Rust channel 拉请求，塞进 asyncio loop"""
    while True:
        req_data = _pyronova_recv(WORKER_ID)   # ← 释放 GIL 等待
        if req_data is None: break
        req_id, handler_idx, method, path, query, body = req_data
        loop.call_soon_threadsafe(          # ← 线程安全地提交到 event loop
            lambda: loop.create_task(
                _process_request(req_id, handler_idx, method, path, query, body)
            )
        )

async def _pyronova_engine():
    loop = asyncio.get_running_loop()
    t = threading.Thread(target=_fetcher_thread, args=(loop,), daemon=True)
    t.start()
    await asyncio.to_thread(t.join)        # ← 保持 event loop 运行

asyncio.run(_pyronova_engine())
```

## 线程模型变化

### 当前（Phase 7.1）
```
Worker 0: [OS thread] → recv → GIL → handler → wait → GIL release → send (串行)
Worker 1: [OS thread] → recv → GIL → handler → wait → GIL release → send (串行)
...
Worker 9: [OS thread] → recv → GIL → handler → wait → GIL release → send (串行)

总计：10 OS threads，并发能力 = 10
```

### 目标（Phase 7.2）
```
Worker 0: [OS thread] → asyncio.run(_pyronova_engine())
    ├── [fetcher thread] → pyronova_recv() (释放 GIL 等待) → call_soon_threadsafe
    └── [asyncio loop] → task1(await sleep) + task2(await sleep) + ... + task1000(await sleep)

总计：20 OS threads (10 workers × 2)，并发能力 = 10,000+
```

## 性能预期

| 场景 | 当前 | 目标 | 原因 |
|------|------|------|------|
| sleep(1ms) SubInterp | 8k | **70k+** | 每个 worker 并发数千个协程 |
| Hello World SubInterp | 215k | 215k | sync handler 无变化 |
| fib(20) SubInterp | 10k | 10k | CPU bound 无变化 |
| asyncpg query | 不可用 | **可用** | await 生态兼容 |

## 风险与缓解

| 风险 | 影响 | 缓解 |
|------|------|------|
| `PyCFunction_NewEx` static 在 sub-interp 中行为 | 可能 crash | 测试验证，必要时用 PyDict 注入 lambda 替代 |
| `call_soon_threadsafe` lambda GIL 重入 | 可能死锁 | fetcher 线程已释放 GIL，asyncio loop 自动获取 |
| sync handler 在 asyncio loop 中阻塞 | 卡死 event loop | 检测非协程返回，用 `to_thread` 包装 |
| before/after hooks 逻辑迁移 | 功能回归 | 在 Python 引擎脚本中重新实现完整 hook 链 |

## 实施步骤

1. **PoC**：只改 `worker_thread_loop`，注入 FFI 函数 + Python 引擎脚本，验证 sleep 性能
2. **Hook 迁移**：在 Python 引擎中实现 before/after_request 完整链
3. **错误处理**：traceback 捕获 + pyronova_send 错误响应
4. **Sync 兼容**：非协程 handler 自动包装 `asyncio.to_thread`
5. **指标集成**：event loop lag 监控嵌入引擎脚本
