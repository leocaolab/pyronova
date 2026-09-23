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
**Linux (bluewhale, 16-core): 29,260 req/s** · macOS (M5 Pro): 6,647 req/s — 66k+ requests,
zero errors, each sub-interpreter has its own kernel instance and does not crash under load
(Linux epoll + 16 workers ~4.4× faster than macOS).

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

## 7. 16-worker soak test — measured stability + memory

`examples/stress_grill.py`: 16 own-GIL sub-interpreters, each with its own copy
(numpy+scipy+sklearn+orjson, APFS clone), each request randomly taking a
**different** C path (not one repeated function): numpy (svd / matmul-BLAS / fft /
sort / inv-LAPACK / boolean-index / ufunc chain / percentile / eigvalsh), orjson
(4 variants), sklearn (LogReg / KMeans / Scaler / PCA fit).

| Dimension | Result (M5 Pro, ~7 min, ~680k requests) |
|-----------|------|
| Throughput | 2,509 req/s, p99 69ms |
| Memory leak | RSS 476 → 476 MB, **Δ0** |
| double-free | `MallocScribble`+`MallocPreScribble`+`MallocErrorAbort` pass, **0** |
| deadlock | **none** (throughput sustained + responsive afterward) |
| crashes / non-2xx | **0** (main grill: 450k requests, 100% success) |

**Memory cost** (eager-loading numpy+scipy+sklearn+orjson):

| workers | RSS | per worker |
|---|---|---|
| 1 | 264 MB | — |
| 16 | 1403 MB | **75 MB each** |

Most of the 75 MB/worker is **writable runtime state** (Python heap + each lib's
writable global singletons + per-request arrays) — which must be independent
anyway; only the read-only `.so` text segment is theoretically shareable (a future
optimization, e.g. Linux `dlmopen` namespaces). **In the concept phase we isolate
fully and don't optimize early** — sharing text would dilute the core "shared-nothing"
guarantee.

Honest boundary: guard malloc only covered 90s / 8 workers (macOS guard malloc makes
sklearn too slow to start at 16); a more thorough double-free/UAF check should be
done on **Linux with ASan/Valgrind**, and dlopen/dlmopen semantics re-tested there.

## 8. Copy isolation vs free-threading — why it's safer in principle

Shared mutable state is the common enemy: the sub-interpreter "one-per-process" wall
and free-threading's "N threads sharing one library → data race" are two faces of the
same problem.

| | free-threading | copy-per-worker sub-interpreter |
|---|---|---|
| Model | one library, N threads **share** global state | one library per worker, **zero sharing** |
| Thread safety | depends on the library adapting (numpy free-threading still experimental, has data races) | **library needs no changes** |
| Fault isolation | shared address space, one corruption crashes all | independent global state + crash isolation |
| Cost | saves memory | ~75 MB/worker |

The copy approach is **shared-nothing**: it trades memory for "no data races + no
dependence on upstream thread-safety + strong isolation". The soak test above (zero
double-free / zero deadlock) is a direct result of that zero-sharing. Free-threading
saves the memory but bets on numpy/sklearn's experimental thread safety. **In the
concept phase, choosing full isolation = buying the hardest safety guarantee with
memory you can afford.**

## 9. How users use it — `app.isolate()` (shipped in v2.6) ⭐

One line declares the libraries; the engine clones a private copy per worker — users
**never touch** copies/paths/counters (see `examples/isolate_numpy.py` +
`tests/test_isolate.py`):

```python
app = Pyronova()
app.isolate("numpy", "orjson", "scikit-learn")   # declare libs needing per-worker isolation

@app.get("/compute")
def compute(req):
    import numpy as np      # engine already prepared this worker's own copy; just import
    return {"r": float(np.linalg.svd(...).sum())}
```

When building the sub-interpreter pool, the engine automatically:
1. clones each declared lib per worker via `cp --reflink` (Linux btrfs/xfs) / APFS `cp -c`
   (macOS) — copy-on-write, near-zero disk;
2. per sub-interp init: `_override_multi_interp_extensions_check(-1)` + `sys.path` to its own copy;
3. user handlers just `import numpy` and get an isolated copy — **never touching
   copies / paths / counters**.

Memory cost is unchanged (~75 MB/worker, §7) but transparent to the user. Until this
lands, `examples/stress_grill.py` is the reproducible manual reference.

## 10. Known issues and fixes (Linux)

Three problems showed up when the 16-worker grill (`examples/stress_grill.py`) ran on
Linux (bluewhale, 16 cores, Python 3.14.4, numpy 2.5.1, scipy 1.18.0, OpenBLAS 0.3.33).
None of them is a PyO3 problem. The macOS runs in §7 didn't hit them: arm64 numpy uses
Accelerate instead of OpenBLAS, and the first one needs a cold import.

| # | Symptom | Cause | Whose | Fix |
|---|---------|-------|-------|-----|
| 1 | Startup abort, `free(): invalid size`, in `scipy/linalg/blas.py` `_get_funcs` | CPython ≥ 3.13 runs a single-phase extension's init in the **main** interpreter and gives the sub-interpreter a shallow copy of the module dict | CPython design, reached through our override | Pyronova runs the init of a worker's private copy in the worker itself |
| 2 | SIGSEGV under load in OpenBLAS `dgetrf_parallel` (`np.linalg.inv`) | Pyronova threads had Rust's 2 MiB default stack; CPython's own threads get 8 MiB, and OpenBLAS needs more than 2 MiB | Pyronova | Every thread that runs Python now has an 8 MiB stack |
| 3 | Throughput collapses under load (55 req/s at 4 workers) | Every worker calls into one BLAS whose thread pool is sized to all cores | OpenBLAS deployment (same for gunicorn/uvicorn workers) | Multi-worker runs default to 1 BLAS thread per worker |

**1. Single-phase init runs in the main interpreter.** Since 3.13, `import.c`
`import_run_extension` switches to the main interpreter before calling a single-phase
module's `PyInit_*` (`switch_to_main_interpreter`), caches the module dict there, and the
sub-interpreter receives a shallow copy of it (`reload_singlephase_extension`). CPython's
own comment on that cache says it is a problem for an interpreter with its own obmalloc
(gh-88216). Normally the sub-interpreter would refuse the module. With the override on, it
loads, and every object in the module (scipy's f2py fortran objects) lives on the main
interpreter's heap. The first mutation (`func.int_dtype = ...` grows the object's
`__dict__`) frees main's memory into the worker's allocator. Reproduced in plain CPython
with `concurrent.interpreters`, no Pyronova involved: 100% on a cold import. The
per-worker copy didn't help, because a copy only changes which file is loaded, not which
interpreter owns the objects. Pyronova's loader now calls a private copy's `PyInit_*`
itself, in the worker, and registers it with `PyState_AddModule` (what 3.12 did). Only
private copies take this path: nothing else loads that file, so its C statics are
initialized once.

**Update (unreleased): shared files.** A worker no longer loads a single-phase extension
from a shared file at all: the loader reads the binary's imported symbols (a single-phase
init calls `PyModule_Create2`; checked against the runtime answer for every extension
module numpy, scipy, sklearn, orjson, pydantic_core, msgpack and isojson load (203 on
macOS, 186 on Linux): no miss, no false hit) and refuses it before CPython runs its init in main; the
package is cloned instead. Shared files now load with CPython's own
`Py_mod_multiple_interpreters` check enforced (the loader used to set the override for them
too, so orjson, which has no guard of its own, loaded shared into every worker and
`Ctrl-C` aborted at teardown). Multi-phase modules of a private copy are also built in the
worker: CPython runs every `PyInit_*` in main, and some call `import_array()` there
(scipy's `_arpacklib`). Built-in single-phase modules (`faulthandler`) have no file to
clone and still load shared.

**2. Thread stack size.** `dgetrf_parallel` recurses and keeps a large job array on the
stack at each level. On a 2 MiB thread it overflowed; with `RUST_MIN_STACK=8M` the same
load ran clean. The stack is `PYTHON_THREAD_STACK` in `src/python/mod.rs`. It is
address space, not resident memory.

**3. BLAS threads.** `numpy` and `scipy` each ship one OpenBLAS, and the dynamic loader
maps each once per process, so every worker shares them. Each pool is sized to all cores;
N workers calling at once thrash it. Plain CPython, 4 sub-interpreters × `np.linalg.inv`
for 25 s: 82 inversions with the default pool, 128,873 with one thread.
`app.run()` in sub-interpreter mode with more than one worker now:

- sets `OPENBLAS_NUM_THREADS`, `OMP_NUM_THREADS`, `MKL_NUM_THREADS` and
  `VECLIB_MAXIMUM_THREADS` to `1` (covers BLAS loaded later);
- shrinks BLAS that is already loaded (numpy imported at the top of the app) through
  `threadpoolctl`, if installed; without it, it logs a warning saying what to do;
- leaves everything alone if you set any of those variables yourself. To give BLAS more
  threads, set e.g. `OPENBLAS_NUM_THREADS=4` before starting.

The startup banner says which one happened (`BLAS: 1 thread per worker ...`).

Grill after the three fixes, default environment, 30 s runs: 6,067 req/s at 4 workers,
10,666 at 8, 11,058 at 16, no crashes. 180 s at 16 workers, `wrk -c128`: 2.71M requests,
15,063 req/s, no errors.

## Appendix: upstream tracking (as of Aug 2026)

| Project | Issue | Status |
|---------|-------|--------|
| numpy | [#27192](https://github.com/numpy/numpy/issues/27192) | **Closed / NOT_PLANNED**; #24755 long-open, unstaffed. Betting on free-threading (free-threaded wheels since 2.1) |
| PyO3 | [#576](https://github.com/PyO3/pyo3/issues/576) | open / needs-design; but 0.29 loads `#[pyclass]` under override |
| CPython | PEP 734 | `concurrent.interpreters` landed in 3.14 |

> The free-threaded numpy wheel **also fails** under multiple sub-interpreters (that
> `NOT_SUPPORTED` slot is source-level and version-independent; the free-threaded wheel is
> built from the same source). Free-threading and sub-interpreters are two separate paths.
