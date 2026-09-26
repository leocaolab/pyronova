# Pyronova 日志系统设计

## 概述

Pyronova 的日志系统基于两个原则：
1. **所有 I/O 下沉到 Rust** — Python 永远不直接接触 stdout/stderr 做日志输出
2. **关闭时零开销** — `tracing` 宏编译后仅做一次原子级别检查；被过滤时，不做字符串格式化、不做系统调用、零开销

---

## 架构

```
                    ┌─────────────────────────────────────┐
                    │         Rust tracing-subscriber      │
                    │   (EnvFilter + fmt::Layer)           │
                    │   targets: pyronova::server              │
                    │            pyronova::access              │
                    │            pyronova::app                 │
                    └──────┬─────────┬──────────┬──────────┘
                           │         │          │
              ┌────────────┘         │          └────────────┐
              │                      │                       │
     ┌────────▼────────┐   ┌────────▼────────┐    ┌────────▼────────┐
     │  服务器日志      │   │  访问日志       │    │  应用日志       │
     │  pyronova::server   │   │  pyronova::access   │    │  pyronova::app      │
     │                 │   │                 │    │                 │
     │  - 启动         │   │  - method       │    │  - Python       │
     │  - 关闭         │   │  - path         │    │    logging.*    │
     │  - GIL 看门狗   │   │  - status       │    │  - worker_id   │
     │  - WS 错误      │   │  - latency_us   │    │  - logger 名称 │
     │  - 连接错误     │   │  - mode         │    │  - 文件:行号   │
     └─────────────────┘   └─────────────────┘    └─────────────────┘
          纯 Rust              纯 Rust            Python → Rust FFI
```

---

## 三个日志目标

### 1. `pyronova::server` — 服务器生命周期

启动、关闭、看门狗告警、连接错误。

```
INFO  pyronova::server Pyronova started version="1.2.0" mode="hybrid" addr=127.0.0.1:8000
INFO  pyronova::server Shutting down gracefully...
WARN  pyronova::server GIL watchdog: main GIL congested latency_ms=52
WARN  pyronova::server Connection error error="connection reset by peer"
ERROR pyronova::server WebSocket upgrade error error="..."
```

### 2. `pyronova::access` — 请求访问日志

每个 HTTP 请求的方法、路径、状态码、延迟和执行模式。

```
INFO  pyronova::access Request handled method=GET path=/ status=200 latency_us=198 mode="subinterp"
INFO  pyronova::access Request handled method=POST path=/api/users status=201 latency_us=1542 mode="gil"
WARN  pyronova::access Client error method=GET path=/missing status=404 latency_us=12
ERROR pyronova::access Request failed method=POST path=/crash status=500 latency_us=892
```

### 3. `pyronova::app` — Python 应用日志

用户代码 `logging.info()` 通过 FFI 桥接从 Python 路由到 Rust。

```
INFO  pyronova::app Fetching users from DB worker=3 logger=myapp file=app.py line=42
ERROR pyronova::app Database connection failed worker=7 logger=db file=models.py line=88
```

---

## 配置 API

```python
from pyronova import Pyronova

# 1. 调试模式 — 全量输出，人类可读的文本格式
app = Pyronova(debug=True)

# 2. 生产模式 — 仅错误，JSON 格式（适配 ELK/Datadog）
app = Pyronova()  # 默认: level=ERROR, access_log=False, format=json

# 3. 自定义 — 精细控制
app = Pyronova(log_config={
    "level": "INFO",        # OFF, ERROR, WARN, INFO, DEBUG, TRACE
    "access_log": True,     # 开启每请求日志
    "format": "json",       # json | text
})

# 4. 静默模式 — 压测场景绝对零开销
app = Pyronova(log_config={"level": "OFF"})

# 5. enable_logging() — 激活访问日志 + Python 钩子输出
app = Pyronova()
app.enable_logging()       # 将级别提升到 INFO，开启 access_log
app.enable_logging(level="warn")  # 显式级别优先于 log_config / debug=True
```

`enable_logging(level=...)` 是显式级别的唯一写入者：它覆盖 `log_config` 或 `debug=True`
设定的级别，之后不带级别的调用（`PYRONOVA_LOG=1` 或 `run()` 时的 `debug=True`）保留它。
不带级别时，`enable_logging()` 保留已配置的级别，若为 ERROR 或 OFF 则提升到 INFO（访问日志是 INFO）。

### 环境变量

| 变量 | 效果 |
|---|---|
| `PYRONOVA_LOG=1` | 自动开启日志（等同于 `app.enable_logging()`） |
| `PYRONOVA_METRICS=1` | 开启 GIL 看门狗（10ms 探测间隔） |

---

## Python 日志桥接

### 问题

Python 默认的 `logging.StreamHandler` 在持有 GIL 时同步 `write()` 到 stderr。在 220k QPS 下，这会摧毁吞吐量。

### 解决方案

主解释器和每个子解释器用同一个 handler 类 `RustLogHandler`（`python/pyronova/_log_bridge.py`）。
主解释器直接 import（`app.py`）；worker 的 bootstrap 在能 import 包之前执行同一份源码，并带上 worker id：

```python
class RustLogHandler(logging.Handler):
    def emit(self, record):
        self._sink(                    # 即 pyronova.engine.emit_python_log
            record.levelno,
            record.name,
            msg,                       # getMessage()，logger.exception 时附上 traceback
            record.pathname or "",
            record.lineno or 0,
            self._worker_id,           # 主解释器为 None
        )
```

### 性能特征

| 场景 | 开销 |
|---|---|
| `level=OFF` | ~1ns（原子比较，分支预测跳过） |
| `level=INFO`，日志被过滤 | ~1ns（同上） |
| `level=INFO`，日志被接受 | ~50-100ns FFI 穿越 + tracing 格式化 |
| Python `logger.info("msg")` | ~200ns（getMessage + FFI） |
| Python `logger.info("data: %s", huge_dict)` | `%s` 格式化的开销（不可避免） |

---

## Rust 实现

### 修改的文件

| 文件 | 变更 |
|---|---|
| `Cargo.toml` | 添加 `tracing`、`tracing-subscriber`（env-filter, json） |
| `src/logging.rs` | **新增** — `init_logger()`、`emit_python_log()` PyO3 函数 |
| `src/lib.rs` | 注册 `logging` 模块 + 函数 |
| `src/app.rs` | 启动/关闭 → `tracing::info!`，连接错误 → `tracing::warn!` |
| `src/handlers.rs` | 访问日志：`latency_us`、`method`、`path`、`status`、`mode` |
| `src/monitor.rs` | GIL 看门狗 → `tracing::warn!` |
| `src/websocket.rs` | WebSocket 错误 → `tracing::error!`/`tracing::warn!` |

### 关键设计决策

1. **`EnvFilter` 实现零开销关闭** — 当级别为 OFF 或被过滤时，`tracing::info!` 编译为单次原子加载 + 分支跳转。CPU 分支预测器预热后命中率 100%。

2. **独立的 `pyronova::access` 目标** — 允许用户关闭访问日志但保留服务器/应用日志，反之亦然。通过 `access_log` 配置映射为 `pyronova::access=off` 指令。

3. **所有解释器用同一个函数** — 子解释器 worker 导入真正的 `pyronova.engine`（PyO3 fork 让模块按解释器各一份；Layer 2），worker 的日志 handler 带上自己的 worker id 调用 `pyronova.engine.emit_python_log`。*早期版本*当时无法在子解释器里导入 engine，所以注册了一个 C-FFI 内建函数 `_pyronova_emit_log`。

4. **`init_logger` 延迟到 `run()`；再次调用即重新配置** — 允许 `enable_logging()` 在 `run()` 之前修改日志配置。`tracing-subscriber` 每个进程只能有一个全局 subscriber，所以第一次 `init_logger` 安装它，并把 filter 和格式层放在 `reload` handle 后面；之后的调用（同一进程里的另一个 app）换上自己的级别、访问日志开关和格式。未知的级别或格式抛 `ValueError`；全局槽位已被外部 subscriber 占用则抛 `RuntimeError`。

6. **Python 级别按数值映射** — `emit_python_log` 接收记录的 `levelno`，映射到它达到的最高标准阈值（≥40 ERROR，≥30 WARN，≥20 INFO，≥10 DEBUG，其余 TRACE），因此 `logging.addLevelName(25, "NOTICE")` 这类自定义级别按 INFO 输出。

5. **启动横幅保留 `println!`** — 人类可读的启动横幅（`Pyronova v1.2.0 [hybrid mode]...`）与 `tracing::info!` 并存，因为它是始终可见的开发者体验，不是可过滤的日志输出。

---

## 测试

`tests/test_logging.py` 覆盖 8 个场景：

| 测试 | 验证内容 |
|---|---|
| `test_gil_mode_logging` | GIL 模式下的 `pyronova::access` 行（请求日志只由 Rust access log 写一次） |
| `test_subinterp_rust_logging` | 子解释器模式下 Rust tracing 访问日志 |
| `test_user_print_in_subinterp` | 子解释器中 `print()` 正常工作 |
| `test_user_logging_in_subinterp` | Python `logging.info()` 桥接到 Rust tracing |
| `test_debug_mode_tracing` | `debug=True` 产生服务器生命周期 tracing 输出 |
| `test_debug_mode_access_log` | `debug=True` 产生带延迟的访问日志 |
| `test_python_logging_bridge_main` | 主解释器日志桥接工作正常 |
| `test_json_format` | JSON 格式输出包含结构化字段 |
