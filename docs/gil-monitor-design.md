# GIL Monitor & 内存监控设计文档

> 设计日期：2026-03-24

## 目标

让 Pyronova 具备工业级可观测性（Observability）：
1. 看到每个 worker 的 GIL 持有/等待时间
2. 看到内存使用趋势
3. 看到子解释器 event loop 是否被阻塞
4. 通过 HTTP 端点实时暴露，可对接 Prometheus/Grafana

## 架构总览

```
┌─────────────────────────────────────────────────────────┐
│                    Pyronova Runtime                         │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ GIL Watchdog │  │ Memory Probe │  │ Loop Monitor │  │
│  │ (Rust thread)│  │ (Rust sysinfo)│  │ (Python coro)│  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │                 │                  │          │
│         ▼                 ▼                  ▼          │
│  ┌──────────────────────────────────────────────────┐   │
│  │         AtomicU64 / DashMap Metrics Store        │   │
│  └──────────────────────┬───────────────────────────┘   │
│                         │                               │
│                         ▼                               │
│               GET /__pyronova__/metrics                     │
└─────────────────────────────────────────────────────────┘
```

## 1. 内存监控

### 方案：Rust 层直读 OS RSS

不依赖 Python 的 `sys.getsizeof()`（统计不全），直接读操作系统的 RSS。

**实现方式 A（零依赖，macOS/Linux）：**
```rust
fn get_rss_mb() -> f64 {
    // macOS: mach_task_self / task_info
    // Linux: /proc/self/statm
    #[cfg(target_os = "macos")]
    {
        use std::mem;
        let mut info: libc::mach_task_basic_info_data_t = unsafe { mem::zeroed() };
        let mut count = (mem::size_of::<libc::mach_task_basic_info_data_t>()
                        / mem::size_of::<libc::natural_t>()) as u32;
        unsafe {
            libc::task_info(libc::mach_task_self(),
                           libc::MACH_TASK_BASIC_INFO,
                           &mut info as *mut _ as *mut _,
                           &mut count);
        }
        info.resident_size as f64 / 1024.0 / 1024.0
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
            .map(|pages| pages as f64 * 4096.0 / 1024.0 / 1024.0)
            .unwrap_or(0.0)
    }
}
```

**实现方式 B（sysinfo crate）：**
```rust
// Cargo.toml: sysinfo = "0.30"
fn get_rss_mb() -> f64 {
    let pid = sysinfo::get_current_pid().unwrap();
    let mut sys = sysinfo::System::new();
    sys.refresh_process(pid);
    sys.process(pid).map(|p| p.memory() as f64 / 1024.0 / 1024.0).unwrap_or(0.0)
}
```

### 压测时的内存采样

在 `runner.py` 中加后台线程，每秒采样一次 RSS：
```python
import psutil, threading, time

def memory_sampler(pid, samples, interval=1.0):
    proc = psutil.Process(pid)
    while not stop_event.is_set():
        samples.append({
            "time": time.monotonic(),
            "rss_mb": proc.memory_info().rss / 1024 / 1024,
        })
        time.sleep(interval)
```

## 2. GIL Monitor

### 核心原理：探针式测量

不 Hook 底层 `PyThread_type_lock`（危险且跨版本不兼容），
而是用探针线程定时尝试获取 GIL，测量获取耗时。

### 场景 A：主解释器 GIL 争用延迟

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// 最近一次 GIL 探针延迟（微秒）
pub static GIL_LATENCY_US: AtomicU64 = AtomicU64::new(0);
/// 最大 GIL 探针延迟（微秒）
pub static GIL_MAX_LATENCY_US: AtomicU64 = AtomicU64::new(0);
/// 探针总次数
pub static GIL_PROBE_COUNT: AtomicU64 = AtomicU64::new(0);
/// 探针总等待时间（微秒）
pub static GIL_TOTAL_WAIT_US: AtomicU64 = AtomicU64::new(0);

fn spawn_gil_watchdog() {
    std::thread::Builder::new()
        .name("pyronova-gil-watchdog".to_string())
        .spawn(|| {
            loop {
                let start = Instant::now();

                // 尝试获取主解释器的 GIL
                Python::with_gil(|_py| {
                    // 拿到了，什么都不做，立刻释放
                });

                let elapsed_us = start.elapsed().as_micros() as u64;

                GIL_LATENCY_US.store(elapsed_us, Ordering::Relaxed);
                GIL_PROBE_COUNT.fetch_add(1, Ordering::Relaxed);
                GIL_TOTAL_WAIT_US.fetch_add(elapsed_us, Ordering::Relaxed);
                GIL_MAX_LATENCY_US.fetch_max(elapsed_us, Ordering::Relaxed);

                if elapsed_us > 50_000 {
                    eprintln!(
                        "⚠️ [GIL MONITOR] Main GIL congested: {}ms",
                        elapsed_us / 1000
                    );
                }

                std::thread::sleep(Duration::from_millis(10));
            }
        })
        .unwrap();
}
```

**解读：**
- GIL 空闲时：探针延迟 ~1μs
- handler 持有 GIL 执行 Python：延迟上升到与 handler 执行时间成正比
- numpy 大矩阵计算（不释放 GIL）：延迟飙到 50-200ms → 触发告警

### 场景 B：子解释器 Event Loop 延迟

子解释器之间没有 GIL 争用（各自独立 GIL），真正的风险是用户写了阻塞代码
（如 `requests.get()` 而非 `httpx.get()`）卡死 event loop。

**Phase 7.2 的 asyncio 引擎中注入心跳协程：**
```python
async def _event_loop_monitor():
    import time, asyncio
    while True:
        t0 = time.perf_counter()
        await asyncio.sleep(0.01)  # 预期 10ms
        lag = (time.perf_counter() - t0 - 0.01) * 1000
        if lag > 50:
            print(f"⚠️ [WORKER {WORKER_ID}] Event loop blocked {lag:.1f}ms")
        # 可选：通过 _pyronova_send 把 lag 数据发回 Rust 汇总
```

**当前模式（串行 run_until_complete）的替代指标：**
在 `worker_thread_loop` 中记录每次 `call_handler` 的耗时：
```rust
let t0 = std::time::Instant::now();
let result = worker.call_handler(...);
let handler_us = t0.elapsed().as_micros() as u64;
// 通过 AtomicU64 汇总到全局指标
```

### 场景 C：Rust 层 per-request 埋点

主解释器的每个请求在 `handlers.rs` 的 `call_handler_with_hooks` 里计时：进入
`main_attach` 前后的差是 GIL 等待（`monitor::record_gil_wait`），handler 与 hooks 的执行时间
是 GIL 持有（`GIL_HOLD_MAX_US`），排队线程数记在 `GIL_QUEUE_LENGTH`。子解释器 worker 各有
自己的 GIL，不参与这组指标。

## 3. Metrics 端点设计

### 路由：`GET /__pyronova__/metrics`

内置系统路由，在 `handle_request` 中优先匹配（不经过用户路由表）。

### 响应格式

```json
{
  "system": {
    "memory_rss_mb": 67.2,
    "uptime_secs": 3600,
    "cpu_count": 10
  },
  "main_interpreter": {
    "gil_probe_latency_us": 12,
    "gil_max_latency_us": 45000,
    "gil_avg_latency_us": 8,
    "gil_probe_count": 360000,
    "status": "healthy"
  },
  "sub_interpreters": {
    "worker_count": 10,
    "total_requests": 21500000,
    "queue_backlog": 0,
    "per_worker": [
      {
        "id": 0,
        "requests": 2150000,
        "avg_handler_us": 4.2,
        "avg_gil_hold_us": 3.8,
        "avg_gil_wait_us": 0.1
      }
    ]
  },
  "shared_state": {
    "keys": 42,
    "memory_estimate_bytes": 8192
  }
}
```

### Prometheus 兼容格式（可选）

```
GET /__pyronova__/metrics?format=prometheus

# HELP pyronova_memory_rss_bytes Resident memory
# TYPE pyronova_memory_rss_bytes gauge
pyronova_memory_rss_bytes 70451200

# HELP pyronova_gil_latency_us GIL acquisition latency
# TYPE pyronova_gil_latency_us gauge
pyronova_gil_latency_us 12

# HELP pyronova_requests_total Total requests processed
# TYPE pyronova_requests_total counter
pyronova_requests_total{worker="0"} 2150000
pyronova_requests_total{worker="1"} 2148000
```

## 4. 压测集成

### runner.py 增强

```python
# 在每个 benchmark scenario 运行期间
# 1. 后台线程每秒采样 RSS
# 2. 结束时请求 /__pyronova__/metrics 获取 GIL 统计
# 3. 写入 results/raw.json

result = {
    "framework": "pyronova_subinterp",
    "scenario": "t1",
    "req_per_sec": 215000,
    "avg_latency_ms": 0.94,
    "memory_rss_mb": 67.2,           # 新增
    "memory_peak_mb": 72.1,          # 新增
    "gil_avg_latency_us": 8,         # 新增
    "gil_max_latency_us": 45,        # 新增
    "queue_backlog_max": 0,          # 新增
}
```

## 5. 实施计划

| 阶段 | 内容 | 难度 | 文件 |
|------|------|------|------|
| **Stage 1** | 内存采样（psutil in runner.py） | 低 | `runner.py` |
| **Stage 2** | per-request 耗时埋点（AtomicU64） | 低 | `handlers.rs`、`monitor.rs` |
| **Stage 3** | GIL watchdog 探针线程 | 中 | `app.rs` 或新 `monitor.rs` |
| **Stage 4** | `/__pyronova__/metrics` 端点 | 中 | `handlers.rs` |
| **Stage 5** | Event loop lag 监控 | 与 Phase 7.2 合并 | `_async_engine.py` |

### 优先级

Stage 1 + 2 最先做（压测立即可用），Stage 3-4 作为框架内置功能后续发布。
