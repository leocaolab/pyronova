# Sky-RPC 引擎设计文档

> 状态：**搁置** — 当前 MsgPack RPC over HTTP 已满足需求。
> 自定义二进制协议仅在 >500k QPS 或微秒级延迟场景才有价值。
> 220k QPS 瓶颈在 Python handler，不在 HTTP header 解析（~2% overhead）。

## 定位

Pyronova 的第二阶段产品：从 Web 框架扩展为高性能 RPC 引擎。

```
阶段 1 (当前): Pyronova Web Framework — HTTP/WS/SSE/MCP, 碾压 Robyn ✅
阶段 2 (未来): Sky-RPC Engine — 二进制协议, Proto, 微服务内部通信
```

## 核心设计：Rust 主导的 Protobuf 管道

```
网卡 DMA → io_uring/epoll → Rust Framer (16B header)
    → prost 反序列化 (Rust, 零 Python)
    → PyO3 #[pyclass] 代理视图
    → Python handler (仅业务逻辑)
    → 响应序列化 (Rust)
    → 回写 TCP
```

## 二进制帧协议 (16 字节固长 Header)

| 字段 | 字节 | 类型 | 说明 |
|------|------|------|------|
| Magic | 2 | u16 | 0x534B ("SK") 快速丢弃非法连接 |
| Flags | 1 | u8 | 压缩/心跳/单向标志 |
| Method ID | 1 | u8 | 路由 ID (0-255, O(1) 数组索引) |
| Request ID | 4 | u32 | 多路复用请求匹配 |
| Trace ID | 4 | u32 | 链路追踪 |
| Length | 4 | u32 | Payload 字节数 (最大 4GB) |

## O(1) 路由表

```rust
struct RpcDispatcher {
    routes: [Option<PyObject>; 256],  // 数组索引 = 极致 O(1)
}

fn dispatch(&self, method_id: u8, payload: &[u8]) {
    self.routes[method_id as usize]  // 无哈希、无字符串比较
}
```

## 代码生成器 (skyrpc-gen)

输入: `trade.proto`
输出:
1. `generated.rs` — prost 结构体 + PyO3 wrapper
2. `trade_pb2.pyi` — Python type stubs (IDE 补全)
3. Method ID 映射表

```python
# 用户体验
class MyTradeService(TradeServiceBase):
    def place_order(self, request: OrderRequest) -> dict:
        return {"status": "ok", "symbol": request.symbol}
```

## 与 Pyronova Web 框架的关系

```
┌─────────────────────────────────────┐
│         用户的 Python 代码           │
├──────────────┬──────────────────────┤
│  Pyronova Web    │    Sky-RPC Engine    │
│  HTTP/WS/SSE │    Binary Proto     │
│  MCP/REST    │    gRPC compat      │
├──────────────┴──────────────────────┤
│     共享层：子解释器池 + SharedState  │
│     + GIL Watchdog + Arena Pool     │
├─────────────────────────────────────┤
│     Tokio / Monoio (可插拔)         │
└─────────────────────────────────────┘
```

## 序列化选型决策

**不用 Google 官方 `protobuf` Python 包。** 原因：
1. C++ 扩展边界跨越开销（每次字段访问一次 FFI 调用）
2. 全局描述符池与子解释器不兼容（死锁/内存泄漏风险）
3. DX 极差（不支持 dataclass，类型提示残缺）

| 方案 | 性能 | DX | 子解释器安全 | 适用场景 |
|------|------|-----|------------|---------|
| **prost (Rust) + PyO3** | 极致 | 需要代码生成 | ✅ | 内部高频通信 |
| **betterproto (Python)** | 高 | 原生 dataclass | ✅ 纯 Python | 跨团队契约 |
| **MsgPack (msgpack-python)** | 极高 | 无 schema | ✅ | 极简内部 RPC |
| **grpclib + betterproto** | 中 | 标准 gRPC | ✅ 纯 asyncio | 被迫兼容 gRPC |
| ~~官方 protobuf~~ | 低 | 差 | ❌ C++ 全局状态 | 不用 |
| ~~官方 grpcio~~ | 低 | 差 | ❌ 隐式 C 线程池 | 不用 |

### MVP 路线

1. **MsgPack over HTTP/1.1** — 最快落地，Pyronova 底层零改动
2. **prost + PyO3 PyDict** — 极致性能，Rust 侧解码后直传 Python
3. **betterproto** — 如果需要 .proto 契约

## 实施计划

| 阶段 | 内容 | 前置条件 |
|------|------|---------|
| 0 | Pyronova Web 发布 PyPI | ← **当前优先** |
| 1 | 16B framer + prost 集成 | Pyronova 稳定 |
| 2 | skyrpc-gen 代码生成器 | Proto 解析器 |
| 3 | O(1) 路由 + 子解释器调度 | 复用 Pyronova 的子解释器 worker（`src/python/`） |
| 4 | gRPC 兼容层 (HTTP/2 + Proto) | HTTP/2 已有 |
| 5 | Monoio thread-per-core 引擎 | Linux 机器 |
