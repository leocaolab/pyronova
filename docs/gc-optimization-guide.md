# Python GC 优化指南 — 金融量化场景

> 适用于 Pyronova 框架下的高频交易、实时行情、多因子计算等场景

## 为什么 GC 在金融场景下致命

Python 的 GC (Garbage Collector) 执行时会 Stop-The-World：
- Gen0 回收：~0.1ms（通常无感）
- Gen1 回收：~1-5ms
- Gen2 回收：~10-50ms（**致命：足以错过一个交易窗口**）

### Pyronova 架构下的影响

| 模式 | GC 影响 | 检测方式 |
|------|---------|---------|
| GIL 模式 | GC 冻结整个主解释器 | Watchdog 延迟飙升 |
| Sub-interp 模式 | GC 只冻结当前 worker | Event Loop lag 监控 |
| Hybrid 模式 | GIL 路由受 GC 影响，sub-interp 路由不受 | 两个指标结合分析 |

### 如何用 Watchdog 检测 GC 停顿

现象特征（`/__pyronova__/metrics` 看板）：
```json
{
    "gil_peak_us": 45000,    // 45ms 延迟毛刺 ← GC 嫌疑
    "memory_rss_mb": 120.5,  // 内存稳定（非泄漏）
    "cpu_usage": 0.3         // CPU 未打满（非计算瓶颈）
}
```

如果 GIL peak 频繁出现 10-50ms 毛刺，CPU 不满，内存震荡 — 90% 是 GC。

## 框架层面的优化策略

### 策略 0：Smart GC 模式（v2.2.1+, 开箱即用）

v2.2.1 起，Pyronova 引擎在每个 sub-interpreter 启动时自动调用
`gc.disable()`，接管 GC 触发时机。CPython 默认的 "每 700 次净分配触
发一次 Gen0 回收" 机制在 400k+ rps 下每秒会触发数百次，**这是 P99
尾延迟不可控的首号元凶**。接管后由 Rust 引擎按三种策略之一决定何时
调用 `gc.collect()`，所有策略都把 GC 停顿**挪出请求热路径**。

通过 `PYRONOVA_GC_MODE` 环境变量选择：

```bash
# 模式 1 — count (默认)
# 每 PYRONOVA_GC_THRESHOLD 个请求触发一次全量 collect。
# 实现简单，适合常规 Web 后端 / RPC 网关 / AI Agent。
PYRONOVA_GC_MODE=count
PYRONOVA_GC_THRESHOLD=100000   # 默认 100k；零循环 workload 设 0 彻底关

# 模式 2 — idle (生产级金融量化推荐)
# 只在 worker 连续一个 PYRONOVA_GC_IDLE_MS tick 没有处理任何请求时触发 collect
# （按请求计数，keep-alive 连接同样生效）。
# 流量洪峰中 PYRONOVA_GC_OOM_FAILSAFE 熔断（按请求数）强制清扫防 OOM。
# 仅 TPC 默认拓扑支持；PYRONOVA_TPC=0（线程池）或 PYRONOVA_TPC_DARWIN=fanout 下启动报错。
# **P99 永远不包含 GC 停顿**。
PYRONOVA_GC_MODE=idle
PYRONOVA_GC_IDLE_MS=100        # 默认 100ms 时间轮 tick
PYRONOVA_GC_OOM_FAILSAFE=50000 # 默认 50k req 熔断

# 模式 3 — off
# 零框架级触发。用户自己调 gc.collect() 或依赖 refcount。
# 适合 tick-driven 系统（量化交易每个 tick 结束手动 collect）。
PYRONOVA_GC_MODE=off
# PYRONOVA_TPC=0（线程池）不支持 off，启动报错；线程池下用 PYRONOVA_GC_THRESHOLD=0。

# 其他取值（拼错如 "idel"）启动即报错，不会静默退回 count。
# 数值变量同理：PYRONOVA_GC_THRESHOLD / PYRONOVA_GC_OOM_FAILSAFE 须为 ≥0 的整数，
# PYRONOVA_GC_IDLE_MS 须为 ≥1 的整数；写错（如 "5k"、IDLE_MS=0）启动即报错，
# 所有模式都会校验（启动时统一解析一次，见 src/config.rs）。
```

本地实测（7840HS, TPC-8, 零循环 hello workload）：

| 模式 | rps | p99 |
|---|---|---|
| count (default) | 459k | **231 µs** |
| idle | 449k | **238 µs** |
| off | 439k | **241 µs** |
| 对照：CPython auto（禁用接管） | ~440k | **~2 ms** |

相比 CPython 默认 GC 触发，接管后 **P99 改善 ~10 倍**，吞吐基本不变。

#### 何时选 idle 模式

**强烈推荐金融量化场景选 idle**。原因：`count` 模式有一个理论雷
区——count 阈值命中的那一刻，**有概率正好是流量高峰**，这个请求就
吃到 collect 延迟。idle 模式的定义是"worker 一整个 tick 没处理请求才 collect"，
**物理上**把停顿挤到业务空窗期。OOM 熔断（50k req 默认）是兜底——
极端行情下 worker 从不空闲，熔断触发保证内存不无限涨。

交易开盘前 5 分钟的洪峰：`idle` tick 可能被饿死，但熔断会在 50k 
请求累积后强制清扫一次，这是可控的、可观测的保护性自救。对比之下 
CPython 默认 auto 在这段时间会触发**上千次**停顿，每次 10-50ms。

#### idle 模式的监控建议

```python
# 在 /metrics 里加：
@app.readiness_check("gc_lag")
def gc_lag(req):
    # 距离上次 gc.collect() 过了多久
    elapsed = time.monotonic() - app.state.get("last_gc_ts", 0)
    # idle 模式下这个数应该稳定在 < 500ms；
    # 超过 5s 说明流量太满、熔断被频繁触发、或机器过载
    return {"status": "ok" if elapsed < 5.0 else "degraded",
            "gc_lag_seconds": elapsed}
```

### 策略 1：手动 GC 控制（推荐）

```python
import gc

# 在高频交易时段禁用自动 GC
gc.disable()

@app.get("/trade", gil=True)
def handle_trade(req):
    # 处理交易信号，零 GC 风险
    return execute_trade(req.json())

# 在闲置时段手动回收
@app.get("/gc", gil=True)
def manual_gc(req):
    collected = gc.collect(generation=0)  # 只回收年轻代
    return {"collected": collected}
```

### 策略 2：消灭隐式对象分配

```python
# ❌ 坏：每行产生临时数组，触发 GC
result = prices * weights + bias
normalized = (result - mean) / std

# ✅ 好：就地计算，零分配
np.multiply(prices, weights, out=buffer1)
np.add(buffer1, bias, out=buffer1)
np.subtract(buffer1, mean, out=buffer2)
np.divide(buffer2, std, out=result)
```

### 策略 3：Zero-copy 数据传递

```python
@app.get("/quotes", gil=True)
def get_quotes(req):
    # Rust 传来的 bytes 直接转 numpy，零 Python 对象分配
    raw_bytes = req.body
    prices = np.frombuffer(raw_bytes, dtype=np.float64)
    # prices 直接指向 Rust 内存，GC 无感知
    return {"mean": float(prices.mean())}
```

### 策略 4：对象池（适合高频重复计算）

```python
# 预分配固定大小的 numpy 数组，反复使用
class QuoteBuffer:
    def __init__(self, size=1000):
        self.prices = np.zeros(size)
        self.volumes = np.zeros(size)
        self.signals = np.zeros(size)

    def update(self, raw_data):
        # 就地更新，不分配新对象
        np.copyto(self.prices, raw_data[:1000])

# 全局单例（通过 app.state 跨 worker 共享元数据）
buffer = QuoteBuffer()
```

## Pyronova 框架未来计划

| 功能 | 状态 | 说明 |
|------|------|------|
| GIL Watchdog 延迟探测 | ✅ 已实现 | `PYRONOVA_METRICS=1` 启用 |
| 内存 RSS 监控 | ✅ 已实现 | `get_gil_metrics()` 返回 RSS |
| **Smart GC (count / idle / off)** | ✅ v2.2.1 已实现 | `PYRONOVA_GC_MODE=idle` 生产级 P99 护城河 |
| Event Loop lag 监控 | 📋 Phase 7.2 | asyncio 心跳协程 |
| `app.gc_control()` API | 📋 计划 | 框架层面的 GC 开关 |
| Zero-copy `memoryview` 传递 | 📋 计划 | Rust→Python 数据无拷贝 |
| 预分配缓冲池 API | 📋 计划 | `app.buffer_pool(size, dtype)` |
