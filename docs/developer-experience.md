# 开发者体验 (Developer Experience) 指南

## 模式选择

### 自动分流（推荐，默认行为）

**不需要手动选模式。** 框架自动检测 `def` 和 `async def`，创建双池分流：

```python
@app.get("/fast")
def fast(req):              # → sync pool (220k req/s)
    return "hello"

@app.get("/io")
async def io_heavy(req):    # → async pool (133k req/s)
    await asyncio.sleep(0.1)
    return "done"

@app.get("/numpy", gil=True)
def compute(req):           # → GIL 主解释器 (numpy 支持)
    import numpy as np
    return {"mean": float(np.mean([1,2,3]))}

app.run()                   # 自动检测，自动分流，零配置
```

启动时框架会显示分配情况：
```
  Pyronova v1.0.0 [hybrid-async mode]
  Workers: 5 sync + 5 async
  Routes: 2 sub-interp + 0 GIL + 1 async
```

### 三种路由类型

| 写法 | 路由到 | 性能 | 适用场景 |
|------|--------|------|---------|
| `def handler(req)` | sync worker pool | **220k req/s** | API、JSON、计算 |
| `async def handler(req)` | async worker pool | **133k req/s** | 数据库、网络 I/O、await |
| `@app.get(gil=True)` | 主解释器 (GIL) | **50-80k req/s** | numpy、C 扩展 |

### 决策指南

| 你的场景 | 怎么写 |
|---------|--------|
| REST API / JSON 路由 | `def handler(req)` |
| 数据库查询 (asyncpg) | `async def handler(req)` + `await` |
| 调用外部 API (httpx) | `async def handler(req)` + `await` |
| numpy / pandas 计算 | `def handler(req)` + `gil=True` |
| AI Agent (LLM 调用) | `async def handler(req)` + `await` |
| SSE 流式输出 | `def handler(req)` + `gil=True` + `Stream` |
| 混合场景 | 直接混写，框架自动分流 |

### 性能保证

- `def` handler 始终走 sync pool → **220k req/s，零损失**
- `async def` handler 走 async pool → **133k req/s，真并发 I/O**
- 两种 handler 混在一起不会互相影响（独立通道 + 独立 worker）
- `gil=True` 路由走 `spawn_blocking`，不阻塞任何 pool

### 子解释器内存效率

每个子解释器增量 ~10 MB（共享进程代码段和共享库）：

| Workers | Pyronova SubInterp | Robyn 多进程 |
|---------|---------------|-------------|
| 1 | 31 MB | ~20 MB |
| 10 | 119 MB | 447 MB |
| 32 | 333 MB | ~660 MB |

共享的：CPython 核心代码、Rust 二进制、`.so` 共享库的只读代码段。
不共享的：Python 堆（模块字典、用户对象）、GIL 状态。
C 扩展（numpy/orjson）的 `.so` 文件只 mmap 一次，32 workers 摊薄基线。

## 配置

### 优先级：参数 > 环境变量 > 默认值

```python
# 代码中直接传参（最高优先级）
app.run(host="0.0.0.0", port=9000, workers=16)

# 或用环境变量（部署时覆盖，不改代码）
# PYRONOVA_HOST=0.0.0.0 PYRONOVA_PORT=9000 PYRONOVA_WORKERS=16 python app.py
app.run()  # 自动读环境变量
```

### 所有配置项

| 参数 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| `host` | `PYRONOVA_HOST` | `127.0.0.1` | 监听地址 |
| `port` | `PYRONOVA_PORT` | `8000` | 监听端口 |
| `workers` | `PYRONOVA_WORKERS` | CPU 核心数 | Sub-interpreter 数量 |
| `mode` | — | 自动检测 | `subinterp` / `auto` |
| — | `PYRONOVA_LOG=1` | 关闭 | 启用请求日志 |
| — | `PYRONOVA_METRICS=1` | 关闭 | 启用 GIL Watchdog |

### 部署示例

```bash
# 开发
python app.py

# 生产 (Docker/K8s)
PYRONOVA_HOST=0.0.0.0 PYRONOVA_PORT=8080 PYRONOVA_WORKERS=32 PYRONOVA_LOG=1 python app.py

# 压测 (关闭日志，最大性能)
PYRONOVA_WORKERS=10 python app.py
```

### 设计原则

- **不搞配置文件** — 不需要 `pyronova.toml` / `settings.py`，环境变量足够
- **不搞 CLI** — 不需要 `pyronova run --port 8000`，直接 `python app.py`
- **容器原生** — Docker ENV / K8s ConfigMap 直接映射

## 日志

### 启用框架日志

```python
app = Pyronova()
app.enable_logging()  # 开启结构化日志
```

输出格式：
```
GIL 模式:      2026-03-24 17:30:01 [INFO ] GET /api/trade → 200 (2.3ms)
Sub-interp:    [INFO ] GET /api/trade → 200
```

### 用户自定义日志

在 handler 里直接用 Python 标准 `logging` 或 `print()`，两种模式下都正常工作：

```python
import logging
logger = logging.getLogger("myapp")

@app.get("/trade")
def trade(req):
    logger.info(f"Processing trade: {req.path}")
    print("debug info", flush=True)  # flush=True 保证立即可见
    return {"status": "ok"}
```

### 日志级别

```python
app.enable_logging(level="info")    # INFO/WARN/ERROR (默认)
app.enable_logging(level="error")   # 只显示错误
app.enable_logging(level="debug")   # 全部显示
```

## Pydantic 集成

自动解析和校验请求体：

```python
from pydantic import BaseModel, Field

class TradeOrder(BaseModel):
    ticker: str = Field(max_length=5)
    amount: int = Field(gt=0)
    price: float

@app.post("/trade", model=TradeOrder)
def trade(req, order: TradeOrder):
    # order 已经校验通过，类型安全
    return {"ticker": order.ticker, "total": order.amount * order.price}
```

无效请求自动返回 422：
```json
{"error": "ValidationError: 1 validation error for TradeOrder\namount\n  Input should be greater than 0"}
```

## RPC 调用

### 服务端

```python
@app.rpc("/rpc/add")
def add(data):
    return {"sum": data["a"] + data["b"]}
```

自动支持 MsgPack 和 JSON，根据 `Content-Type` 头切换。

### 客户端

```python
from pyronova import RPCClient

with RPCClient("http://127.0.0.1:8000") as client:
    result = client.add(a=3, b=5)  # 像本地函数一样调用
    print(result)  # {"sum": 8}
```

## MCP (AI Agent 工具集成)

```python
@app.mcp.tool(description="Add two numbers")
def add(a: int, b: int) -> int:
    return a + b

@app.mcp.resource("config://app")
def get_config():
    return {"version": "1.0.0"}
```

Claude Desktop 连接 `http://localhost:8000/mcp` 即可发现工具。

## WebSocket

```python
@app.websocket("/ws")
def echo(ws):
    while True:
        msg = ws.recv()       # 文本
        if msg is None: break
        ws.send(f"echo: {msg}")

# 二进制
data = ws.recv_bytes()
ws.send_bytes(data)

# 混合
msg_type, data = ws.recv_message()  # ("text", "hello") or ("binary", b"\x00")
```

## SSE 流式传输

```python
import threading
from pyronova import Stream

@app.get("/stream", gil=True)
def stream(req):
    s = Stream()
    def generate():
        for token in ["Hello", " ", "World"]:
            s.send_event(token)
        s.close()
    threading.Thread(target=generate).start()
    return s
```

## SharedState (跨 worker 共享)

```python
# 写入（任意 worker）
app.state["session:user_1"] = '{"role": "admin"}'

# 读取（任意 worker）
data = app.state["session:user_1"]

# 字典操作
len(app.state)
"key" in app.state
del app.state["key"]
```

纳秒级延迟，零 Redis 依赖。`gil=True` 路由中使用。

## GIL 监控

```python
# 启用 (环境变量)
# PYRONOVA_METRICS=1 python app.py

from pyronova import get_gil_metrics, reset_peaks
m = get_gil_metrics()          # Metrics 快照；读取不会清零
m.gil_wait_peak_us, m.gil_hold_peak_us, m.gil_queue_length
m.rss_bytes                    # 采样器还没读到时为 None
m.dropped_requests, m.total_requests
reset_peaks()                  # 显式开始新的峰值窗口
```

## IDE 支持

`engine.pyi` 提供完整的类型提示。IDE 中 `req.` 会自动补全：
- `req.method`, `req.path`, `req.params`, `req.query`, `req.headers`
- `req.body`, `req.text()`, `req.json()`, `req.query_params`

## 内存效率

每个子解释器增量 ~10 MB（共享进程代码段）：

```
Workers=1   31 MB
Workers=10  119 MB
Workers=32  333 MB
```

对比 Robyn 多进程：22 进程 = 427 MB。Pyronova 节省 ~50-70%。
