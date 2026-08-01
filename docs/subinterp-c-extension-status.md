# 子解释器下的 C 扩展支持现状（2026-08）

> 状态：live · 测试环境：Python 3.14.6 / macOS ARM64 (M5 Pro) · PyO3 0.29
> 本文替代旧的 `subinterp-c-extension-compat.md`（2026-03 快照，多处已过时）。
> 所有结论均为**实测**，不是引用上游声明。

## TL;DR

- C 扩展在子解释器（own-GIL, PEP 684）里加载有**两层墙**：① `Py_mod_multiple_interpreters`
  slot 声明（策略检查，可用 override 开关放行）；② 进程级全局可变状态导致的
  `cannot load module more than once`（**没有开关**，只能用物理副本绕过）。
- **大多数 C 扩展只撞第一层**（pydantic-core / msgpack / cryptography …）→ 开 override 就能多子解释器并行。
- **少数库撞第二层**（numpy / orjson / lxml）→ 必须**每 worker 一份物理副本**（各自独立全局态）。
- **PyO3 0.28 → 0.29 是关键升级**：0.28 注册 `#[pyclass]` 会在子解释器里 **hard panic**；0.29 在 override 下能加载，且每个子解释器拿到**独立的 module 实例**（真隔离）。Pyronova 已升级到 0.29。

## 一、两层墙

在 own-GIL 子解释器里 `import` 一个 C 扩展，会依次遇到：

**第 1 层：`Py_mod_multiple_interpreters` slot 检查。**
CPython 严格模式（`check_multi_interp_extensions = 1`，Pyronova 默认）会拒绝任何声明
`Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED` 的多阶段扩展：

    ImportError: module X does not support loading in subinterpreters

这是**策略检查**，有开关：

    import _imp
    _imp._override_multi_interp_extensions_check(-1)   # 只能在子解释器里调，主解释器会 RuntimeError

**第 2 层：`cannot load module more than once per process`。**
如果扩展持有**进程级全局可变状态**（`m_size == 0` 的多阶段模块，或有 C static 单例），
它在整个进程里只能加载一份。第一个子解释器成功后，第二个就：

    ImportError: cannot load module more than once per process

**这一层没有开关** —— 它不是策略，是物理事实（两份实例会共享同一个 C 全局变量）。
唯一绕过办法：给每个子解释器一份**物理独立的 `.so`**（不同路径 → CPython 视作不同模块 → 各自独立全局态）。

## 二、实测兼容矩阵（override ON，4 个 own-GIL 子解释器，crash-isolated）

| 库 | 版本 | 结果 | 需要副本？ | 备注 |
|----|------|------|-----------|------|
| pydantic / pydantic-core | 2.13 / 2.46 | ✅ 4/4 | 否 | PyO3-based，override 够 |
| msgpack | 1.2.1 | ✅ 4/4 | 否 | |
| cryptography | 50.0.0 | ✅ 4/4 | 否 | OpenSSL bindings |
| **numpy** | 2.5.1 | ❌ `cannot load more than once` | **是** | `_multiarray_umath` `m_size=0` + 全局态 |
| **orjson** | 3.11.9 | 💥 **segfault** | **是** | 第二个子解释器 module init 时 dealloc 跨解释器共享对象（`orjson_init_exec → PyModule_Add → _Py_Dealloc`）。⚠️ 旧文档说 orjson“完全正常”是 2026-03 / orjson 3.11.7 的旧结论，已回归 |
| **lxml** | 6.1.1 | ❌ 主动拒绝 "Interpreter change detected" | **是** | libxml2，自带跨解释器检查 |
| 已适配的 stdlib C 扩展 | — | ✅ | 否 | `_ctypes`/`_ssl`/`_socket`/`_lzma`/`_struct`/`_json`/`_pickle`… 3.14 已全部多阶段（torch 卡 `_ctypes` 的旧问题在 3.14 消失） |

> 结论：**需副本的核心就三类** —— numpy 生态（numpy/pandas/scipy/scikit-learn 都依赖 numpy）、orjson、lxml。其余主流 C 扩展 override 就够。

## 三、PyO3 0.28 vs 0.29

| | PyO3 0.28 | PyO3 0.29 |
|---|---|---|
| 纯 `#[pyfunction]` + override | ✅ 加载 | ✅ 加载 |
| 含 `#[pyclass]` + override | ❌ **hard panic** `pyo3#576` | ✅ **加载 + 可实例化** |
| 隔离性 | module 地址部分共享（弱） | **8 个子解释器 = 8 个不同 module 地址**（真隔离） |

这就是 Pyronova 历史上必须 `bypass pyo3`、用 raw C-API `PyType_FromSpec` 手写
`_Request` 类型（见 `pyronova_request_type.rs`）的原因 —— 卡在 0.28。**升级到 0.29 后，
纯数值 C 扩展可以直接用 PyO3 高层写，不必手写 raw C-API。**

## 四、kernel 方案谱（实测，16 worker，4M rows，own-GIL 子解释器）

| 方案 | 吞吐 | 内存 | 定位 |
|------|------|------|------|
| numpy 现成（一份） | ❌ 加载就跪 | — | 不可用 |
| **numpy 每 worker 一份副本** | 1.50 B rows/s | 932 MB | 白嫖 numpy 生态，代价 = N× 内存，**过渡方案** |
| 纯 Python UDF | 173 M rows/s | 140 MB | 最易，无 C 扩展 |
| **PyO3 0.29 kernel + override** | 3.72 B rows/s | 142 MB | Rust 写，含 `#[pyclass]`，方便 |
| **raw C-API（声明 `PER_INTERPRETER_GIL_SUPPORTED`）** | 3.72 B rows/s | 139 MB | 最强隔离，严格模式直接过（无需 override），最硬 |

PyO3 kernel 与 raw C-API **同速**（PyO3 是零开销薄封装）。相对 multiprocessing（纯 Python kernel 22 M rows/s、~1.1 GB）：**吞吐 5–8×、内存 8×**。

## 五、在 Pyronova 里用

用户 script 会在**每个子解释器**的 worker init 时执行，所以在 script 顶部翻开关即可，
**不必改引擎核心**。见 `examples/c_extension_subinterp.py`：

```python
import _imp
try:
    _imp._override_multi_interp_extensions_check(-1)   # 子解释器：放行
except RuntimeError:
    pass                                               # 主解释器：不需要

import pyo3_kernel   # Rust/PyO3 0.29 native 扩展

@app.get("/compute")
def compute(req):
    x = array.array("d", range(4096)); y = array.array("d", bytes(8*4096))
    pyo3_kernel.apply(memoryview(x), memoryview(y))    # zero-copy native kernel
    return {"sample": y[1]}
```

**Load test**（`wrk -t8 -c256 -d10s /compute`，每请求跑 native kernel in sub-interp）：
**Linux(bluewhale, 16-core):29,260 req/s** · macOS(M5 Pro):6,647 req/s —— 66k+ 请求零错误，每个子解释器独立 kernel 实例，负载下不崩(Linux epoll + 16 worker 比 macOS 快 ~4.4×)。

## 六、建议

- **纯数值 kernel**：用 PyO3 0.29 写 `#[pyfunction]`（方便、同 raw C-API 速度），子解释器 script 里 override 后 import。
- **要建 Python 类型 / 要严格模式无 override**：raw C-API 手写、声明 `PER_INTERPRETER_GIL_SUPPORTED`（如 `pyronova_request_type.rs` 的 `_Request`）。
- **numpy / orjson / lxml**：每 worker 一份物理副本（损失内存换隔离）；这对 free-threading 也是最优隔离手段（不用担心共享 bug）。
- **override 的代价**：它是进程级开关，会顺带放行 numpy 等不安全扩展去*尝试*加载（numpy 仍会因自身全局态失败）。生产里配合 crash isolation（子解释器崩不带垮 supervisor）。

## 七、16-worker soak test —— 实测稳定性 + 内存

`examples/stress_grill.py`:16 个 own-GIL 子解释器,每个一份独立副本
(numpy+scipy+sklearn+orjson,APFS clone),每请求随机走**不同** C 路径广覆盖
(不是重复一个函数):numpy(svd / matmul-BLAS / fft / sort / inv-LAPACK /
boolean-index / ufunc 链 / percentile / eigvalsh)、orjson(4 种)、sklearn
(LogReg / KMeans / Scaler / PCA fit)。

| 维度 | 结果(M5 Pro, ~7 分钟, ~68 万请求) |
|------|------|
| 吞吐 | 2,509 req/s,p99 69ms |
| 内存泄漏 | RSS 476 → 476 MB,**Δ0** |
| double-free | `MallocScribble`+`MallocPreScribble`+`MallocErrorAbort` 专项,**0** |
| deadlock | **无**(吞吐持续 + 事后即时响应) |
| 崩溃 / 非-2xx | **0**(主 grill 45 万请求 100% 成功) |

**内存代价**(eager 全加载 numpy+scipy+sklearn+orjson):

| worker 数 | RSS | 每 worker |
|---|---|---|
| 1 | 264 MB | — |
| 16 | 1403 MB | **75 MB/份** |

75MB/worker 中,大头是**可写运行时态**(Python 堆 + 各库可写全局单例 + 每请求数组),
本就必须独立;只有 `.so` 只读 text 段理论可共享(未来优化方向,Linux `dlmopen`
namespace)。**概念阶段坚决完全隔离,不过早优化** —— 把 text 共享掺进来会稀释"零共享"这个核心保证。

诚实边界:guard malloc 只覆盖 90s / 8 worker(macOS guard malloc 让 sklearn 慢到起不来);
更彻底的 double-free/UAF 检测应在 **Linux 上用 ASan/Valgrind** 补,dlopen/dlmopen 语义也需 Linux 复测。

## 八、副本隔离 vs free-threading —— 为什么理论上更安全

共享可变状态是共同的敌人:sub-interpreter 的"一进程一份"墙,和 free-threading 的
"多线程共享一份库 → data race",是同一问题的两面。

| | free-threading | 副本 sub-interpreter |
|---|---|---|
| 模型 | 一份库,多线程**共享**全局态 | 每 worker 一份独立库,**零共享** |
| 线程安全 | 依赖库自己适配(numpy free-threading 仍 experimental,有 data race) | **不需要库改任何代码** |
| 故障隔离 | 共享地址空间,一处 corrupt 全崩 | 独立全局态 + crash isolation |
| 代价 | 省内存 | ~75 MB/worker |

副本方案 = **shared-nothing**:用内存换「无 data race + 不依赖上游线程安全适配 + 强隔离」。
上面的 soak test(零 double-free / 零 deadlock)正是"零共享"的直接结果。free-threading
省这份内存,但要赌 numpy/sklearn 的 experimental 线程安全。**概念阶段选坚决隔离 = 用能承受的内存买最硬的安全保证。**

## 九、用户怎么用 —— `app.isolate()`(已实现,v2.6)⭐

一行声明要隔离的库,引擎自动为每个 worker clone 独立副本 —— 用户**完全不碰**副本/路径/counter
(见 `examples/isolate_numpy.py` + `tests/test_isolate.py`):

```python
app = Pyronova()
app.isolate("numpy", "orjson", "scikit-learn")   # 声明需 per-worker 隔离的库

@app.get("/compute")
def compute(req):
    import numpy as np      # 引擎已为本 worker 备好独立副本,正常 import 即可
    return {"r": float(np.linalg.svd(...).sum())}
```

引擎在 sub-interp pool 构建时自动:
1. 为每个 worker `cp --reflink`(Linux btrfs/xfs)/ APFS `cp -c`(macOS)克隆声明的库 —— COW,近零磁盘;
2. 每个 sub-interp init:`_override_multi_interp_extensions_check(-1)` + `sys.path` 指向自己那份;
3. 用户 handler 里 `import numpy` 直接拿到隔离副本,**完全不碰副本/路径/counter**。

内存代价照旧(~75 MB/worker,§7),但对用户透明。未做时,`examples/stress_grill.py` 是可复现的手动参考。

## 附：上游追踪（现状 2026-08）

| 项目 | Issue | 现状 |
|------|-------|------|
| numpy | [#27192](https://github.com/numpy/numpy/issues/27192) | **Closed / NOT_PLANNED**；#24755 长期 open 无人认领。押 free-threading（2.1+ 出 free-threaded wheel） |
| PyO3 | [#576](https://github.com/PyO3/pyo3/issues/576) | open / needs-design；但 0.29 已能在 override 下加载 `#[pyclass]` |
| CPython | PEP 734 | 3.14 落地 `concurrent.interpreters` |

> free-threaded numpy wheel 在多子解释器下**同样不行**（那个 `NOT_SUPPORTED` slot 是源码级、版本无关；free-threaded wheel 是同一份源码编的）。free-threading 与 sub-interpreter 是两条独立的路。
