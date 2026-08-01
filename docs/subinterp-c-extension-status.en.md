# C Extensions under Sub-interpreters — Current Status (Aug 2026)

> Status: live · Tested on: Python 3.14.6 / macOS ARM64 (M5 Pro) · PyO3 0.29
> Supersedes the older `subinterp-c-extension-compat.md` (a 2026-03 snapshot,
> now stale in several places). Every conclusion here is **measured**, not quoted
> from upstream claims.

## TL;DR

- Loading a C extension in an own-GIL sub-interpreter (PEP 684) hits **two walls**:
  ① the `Py_mod_multiple_interpreters` slot declaration (a policy check — there IS a
  switch); ② `cannot load module more than once` caused by process-global mutable
  state (**no switch** — only physical copies get around it).
- **Most C extensions only hit wall #1** (pydantic-core / msgpack / cryptography …)
  → flip the override and they run in parallel across sub-interpreters.
- **A few hit wall #2** (numpy / orjson / lxml) → each worker needs its **own physical
  copy** (independent global state).
- **PyO3 0.28 → 0.29 is the key upgrade**: 0.28 **hard-panics** when registering a
  `#[pyclass]` in a sub-interpreter; 0.29 loads it under override, and each
  sub-interpreter gets a **distinct module instance** (true isolation). Pyronova is now on 0.29.

## 1. The two walls

`import`-ing a C extension in an own-GIL sub-interpreter hits, in order:

**Wall #1: the `Py_mod_multiple_interpreters` slot check.**
CPython strict mode (`check_multi_interp_extensions = 1`, Pyronova's default) rejects
any multi-phase extension declaring `Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED`:

    ImportError: module X does not support loading in subinterpreters

This is a **policy check** with a switch:

    import _imp
    _imp._override_multi_interp_extensions_check(-1)   # sub-interpreter only; main interp raises RuntimeError

**Wall #2: `cannot load module more than once per process`.**
If the extension holds **process-global mutable state** (`m_size == 0` multi-phase
module, or C static singletons), it can exist only once per process. After the first
sub-interpreter succeeds, the second gets:

    ImportError: cannot load module more than once per process

**There is no switch for this** — it's not policy, it's a physical fact (two instances
would share the same C global). The only way around it: give each sub-interpreter a
**physically distinct `.so`** (different path → CPython treats it as a different module
→ independent global state).

## 2. Measured compatibility matrix (override ON, 4 own-GIL sub-interps, crash-isolated)

| Library | Version | Result | Needs copy? | Notes |
|---------|---------|--------|-------------|-------|
| pydantic / pydantic-core | 2.13 / 2.46 | ✅ 4/4 | No | PyO3-based; override suffices |
| msgpack | 1.2.1 | ✅ 4/4 | No | |
| cryptography | 50.0.0 | ✅ 4/4 | No | OpenSSL bindings |
| **numpy** | 2.5.1 | ❌ `cannot load more than once` | **Yes** | `_multiarray_umath` is `m_size=0` + global state |
| **orjson** | 3.11.9 | 💥 **segfault** | **Yes** | 2nd interp's module init deallocs a cross-interpreter shared object (`orjson_init_exec → PyModule_Add → _Py_Dealloc`). ⚠️ The old doc's "orjson fully works" was the 2026-03 / orjson 3.11.7 result — regressed since |
| **lxml** | 6.1.1 | ❌ self-rejects "Interpreter change detected" | **Yes** | libxml2, has its own cross-interpreter guard |
| adapted stdlib C exts | — | ✅ | No | `_ctypes`/`_ssl`/`_socket`/`_lzma`/`_struct`/`_json`/`_pickle`… all multi-phase in 3.14 (torch's old `_ctypes` blocker is gone in 3.14) |

> Bottom line: **only three groups need copies** — the numpy ecosystem
> (numpy/pandas/scipy/scikit-learn all depend on numpy), orjson, and lxml. Everything
> else mainstream is fine with just the override.

## 3. PyO3 0.28 vs 0.29

| | PyO3 0.28 | PyO3 0.29 |
|---|---|---|
| Pure `#[pyfunction]` + override | ✅ loads | ✅ loads |
| Module with `#[pyclass]` + override | ❌ **hard panic** `pyo3#576` | ✅ **loads + instantiable** |
| Isolation | module addresses partly shared (weak) | **8 sub-interps = 8 distinct module addresses** (true isolation) |

This is exactly why Pyronova historically had to `bypass pyo3` and hand-build its
`_Request` type via raw C-API `PyType_FromSpec` (see `pyronova_request_type.rs`) — it
was stuck on 0.28. **After upgrading to 0.29, a pure numeric C extension can be written
with high-level PyO3 directly, no raw C-API needed.**

## 4. Kernel spectrum (measured, 16 workers, 4M rows, own-GIL sub-interps)

| Approach | Throughput | Memory | Positioning |
|----------|-----------|--------|-------------|
| numpy stock (1 copy) | ❌ won't load | — | unusable |
| **numpy per-worker copy** | 1.50 B rows/s | 932 MB | reuse the numpy ecosystem; cost = N× memory; **transition option** |
| pure-Python UDF | 173 M rows/s | 140 MB | easiest, no C ext |
| **PyO3 0.29 kernel + override** | 3.72 B rows/s | 142 MB | write it in Rust, incl. `#[pyclass]`, convenient |
| **raw C-API (declares `PER_INTERPRETER_GIL_SUPPORTED`)** | 3.72 B rows/s | 139 MB | strongest isolation, passes strict mode with NO override, hardest |

The PyO3 kernel matches raw C-API throughput (PyO3 is a zero-overhead wrapper). Versus
multiprocessing (pure-Python kernel: 22 M rows/s, ~1.1 GB): **5–8× throughput, 8× memory**.

## 5. Using it in Pyronova

The user script runs at worker init inside **every** sub-interpreter, so flip the switch
at the top of the script — **no engine changes needed**. See `examples/c_extension_subinterp.py`:

```python
import _imp
try:
    _imp._override_multi_interp_extensions_check(-1)   # sub-interpreter: allow
except RuntimeError:
    pass                                               # main interpreter: not needed

import pyo3_kernel   # Rust/PyO3 0.29 native extension

@app.get("/compute")
def compute(req):
    x = array.array("d", range(4096)); y = array.array("d", bytes(8*4096))
    pyo3_kernel.apply(memoryview(x), memoryview(y))    # zero-copy native kernel
    return {"sample": y[1]}
```

**Load test** (`wrk -t8 -c256 -d10s /compute`, native kernel per request in a sub-interp):
**6,647 req/s, 66,740 requests, zero errors, 38 ms latency, stable** — each sub-interpreter
has its own kernel instance and does not crash under load.

## 6. Recommendations

- **Pure numeric kernel**: write a PyO3 0.29 `#[pyfunction]` (convenient, same speed as raw
  C-API); import it after the override inside the sub-interpreter script.
- **Need to build Python types / want strict mode with no override**: hand-write raw C-API
  and declare `PER_INTERPRETER_GIL_SUPPORTED` (like `_Request` in `pyronova_request_type.rs`).
- **numpy / orjson / lxml**: one physical copy per worker (memory for isolation); this is
  also the best isolation strategy for free-threading (no shared-state bugs to worry about).
- **Cost of the override**: it's a process-wide switch that also lets unsafe extensions like
  numpy *attempt* to load (numpy still fails on its own global state). Pair with crash
  isolation in production (a crashing sub-interpreter must not take the supervisor down).

## Appendix: upstream tracking (as of Aug 2026)

| Project | Issue | Status |
|---------|-------|--------|
| numpy | [#27192](https://github.com/numpy/numpy/issues/27192) | **Closed / NOT_PLANNED**; #24755 long-open, unstaffed. Betting on free-threading (free-threaded wheels since 2.1) |
| PyO3 | [#576](https://github.com/PyO3/pyo3/issues/576) | open / needs-design; but 0.29 loads `#[pyclass]` under override |
| CPython | PEP 734 | `concurrent.interpreters` landed in 3.14 |

> The free-threaded numpy wheel **also fails** under multiple sub-interpreters (that
> `NOT_SUPPORTED` slot is source-level and version-independent; the free-threaded wheel is
> built from the same source). Free-threading and sub-interpreters are two separate paths.
