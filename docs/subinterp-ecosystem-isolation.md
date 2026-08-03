# Unmodified scientific/ML ecosystem on own-GIL sub-interpreters — findings & design

**Claim being built:** run a multi-core Python server / data pipeline where the
user's numpy/scipy/sklearn/torch/polars code is **unmodified** — *pyre* handles
everything. Parallelism = own-GIL sub-interpreter workers (TPC), zero-copy Arrow
between Rust host and Python workers.

Measured on **bluewhale** (Linux, AMD 7840HS, CPython 3.14, numpy 2.5.1 /
scipy 1.18 / scikit-learn 1.9) and **mac** (ARM, CPython 3.14, numpy=Accelerate),
2026-08-02. This file is the durable record of a long investigation session.

## TL;DR

Running unmodified C-extension code in own-GIL sub-interps needs **three
prerequisites, all engine-side (user changes nothing):**

1. **Isolation** — per-worker physical copies (+ `_override_multi_interp_extensions_check`).
   Without it a single-phase C ext (numpy included) **won't even load** in the
   2nd worker: `module numpy._core._multiarray_umath does not support loading in
   subinterpreters`. This is the FOUNDATION, not a per-lib patch.
2. **PyMem → non-arena allocator** (`PYTHONMALLOC=malloc` equiv) — fixes an
   import-time cross-arena `free(): invalid size` (own-GIL ⇒ per-interp pymalloc
   arena; the ext frees across arenas). Cost ≈ −6% hot path.
3. **Single-threaded native pools per worker** (`OPENBLAS_NUM_THREADS=1`,
   `OMP_NUM_THREADS=1`, `MKL_/NUMEXPR_/RAYON_NUM_THREADS=1`, `torch.set_num_threads(1)`)
   — fixes a concurrent segfault in the lib's own thread pool. Parallelism lives
   at the worker level (= Spark's task model / gunicorn+numpy convention).

Single-request full ecosystem: works with (1)+(2). Concurrent: works with (1)+(2)+(3)
— **345k concurrent heavy numpy/scipy/sklearn requests, zero crash.**

## Capability matrix — 可以 / 不可以 (verified vs untested)

**✅ works · ❌ doesn't · ❓ untested (verify, don't infer).** Where measured is noted;
"bluewhale" = Linux, else mac (CPython 3.14) unless stated.

### Libraries — zero-mod on own-GIL sub-interpreters
| lib | verdict | prerequisites |
|---|---|---|
| numpy / scipy / sklearn | ✅ | isolate + override (+ on Linux: `PYTHONMALLOC=malloc` and `*_NUM_THREADS=1`); 345k concurrent, bluewhale |
| orjson | ✅ | reactive auto-isolate (measured this repo) |
| tokenizers | ✅ | isolate; 6.35M req, bluewhale |
| pydantic / pydantic_core | ✅ | isolate + override; 432k req, bluewhale |
| polars (in a UDF) | ✅ but | must isolate BOTH `polars` AND `_polars_runtime_32` (215 MB); for heavy work prefer polars-rs at the engine layer; 1.29M req, bluewhale |
| cryptography / rpds-py / others | ❓ | not tested — verify each; "it's PyO3 ⇒ X" is wrong both ways |

### Approaches
| approach | verdict | why |
|---|---|---|
| per-worker physical clone + transient override | ✅ | the foundation |
| meta_path finder: override around `create_module` | ✅ | load succeeds first-try, no failed attempt to pollute the process ext table |
| `os._exit` on graceful shutdown (skip finalize) | ✅ | fixes the sub-interp teardown cross-arena abort; zero hot-path cost |
| `PYTHONMALLOC=malloc` | ✅ | fixes A-class (Linux import-time abort AND teardown abort) |
| catch-then-retry the failed import | ❌ | the failed 1st attempt half-registers the ext in `_PyRuntime.imports.extensions` (uncleanable by sys.modules eviction) → "cannot load module more than once" on warm restart |
| persistent override | ❌ | masks the hard-failure signal every later undeclared single-phase ext needs to isolate itself |
| `use_main_obmalloc=1` | ❌ | removes A-class but races under load (shared pymalloc, no lock) |
| patchelf unique-SONAME per-worker | ❌ | isolates the instance but B-class still segfaults |
| free-threading (PEP 703) as the general answer | ❌ | unmodified GIL-relying code data-races; sub-interps preserve per-worker GIL, that's the bet |

### Platform
- **mac:** import-time A-class tolerated (only a warning); **teardown aborted** (fixed via `os._exit`).
- **Linux:** import-time A-class **hard-aborts** (glibc) → **without `PYTHONMALLOC=malloc` (#2), sklearn won't even import**; concurrent needs `*_NUM_THREADS=1` (#3).

**One line:** declared `app.isolate` + the above = solid; reactive auto-isolate is the convenience layer; heavy work belongs in the Rust engine. Linux production still needs #2 and #3 (below).

## The three failure classes (independent)

### Load: single-phase C ext can't load in >1 interp
- Symptom: `does not support loading in subinterpreters` on every call in workers.
- Cause: numpy/scipy/sklearn are single-phase with process-global C state; one
  shared module can't serve multiple interps.
- Fix: **per-worker physical copy** (distinct `.so` → distinct global state) + the
  override flag. This is what `app.isolate(...)` sets up. Proven required: with no
  isolation, numpy fails to load in all workers (server survives, just errors).
- Sub-fix (Linux): clone vendored `.libs` **by DISTRIBUTION name**, not import
  name — `sklearn` (import) ships libgomp in `scikit_learn.libs/`, not
  `sklearn.libs/`. Implemented in `_pyronova_isolate_libs` via
  `importlib.metadata.packages_distributions()`.

### Import-time: cross-arena free (A-class)
- Symptom: `free(): invalid size` in CPython `PyMem_Free` during sklearn import
  (`PyObject_SetAttr → PyDict_SetItemString → PyMem_Free`), glibc abort / core dump.
- Cause: own-GIL requires `use_main_obmalloc=0` → each interp has its own pymalloc
  arena; the ext's import frees a chunk allocated in another arena.
- Fix: route PyMem to a thread-safe non-arena allocator (`PYTHONMALLOC=malloc`).
  Verified. `use_main_obmalloc=1` also removes it but is **racy under load** (shared
  pymalloc has no lock) → rejected.
- mac does NOT abort here (its allocator tolerates the bad free); **Linux glibc is
  strict** → aborts.

### Concurrent: native thread-pool crash (B-class)
- Symptom: segfault under concurrent load. Backtrace = **OpenBLAS**
  `dgetrf_parallel` / `exec_blas_async_wait` / `dgemm_kernel` (from `np.linalg.inv`
  etc.). ~35 threads spawned.
- Cause: the native lib spawns/manages its OWN thread pool; driven concurrently
  from own-GIL/TPC worker threads → the pool's state corrupts → segfault.
- **Not a sharing problem:** per-worker patchelf-isolated OpenBLAS (unique SONAME,
  own instance per worker — confirmed `_iso1.so` in the crash path) **still crashes**
  in `dgetrf_parallel`. So isolating the instance does NOT help; the lib's threaded
  mode itself is incompatible with being driven from these worker threads.
- **Only fix that works: single-threaded native pools** (`*_NUM_THREADS=1`).
  345k concurrent req survived. This is the correct serving/pipeline config anyway
  (parallelism at worker level, avoid N×M oversubscription).
- **Generic, not numpy-specific:** hits any lib with its own pool — OpenBLAS/MKL/BLIS,
  OpenMP(libgomp: sklearn/xgboost/lightgbm/faiss), TBB, torch/TF/ONNX, Polars(rayon),
  numexpr, opencv. Mitigation is the uniform `*_NUM_THREADS=1` env set.

### PyO3-extension load: #576 guard (C-class, isolate does NOT fix)
- Symptom: under concurrency, intermittent handler errors (NOT segfault; server
  survives). Raw panic:
  `panicked at crates/polars-python/src/c_api/mod.rs:133:19: failed to wrap
  pymodule: ImportError('PyO3 modules do not yet support subinterpreters, see
  https://github.com/PyO3/pyo3/issues/576')`
- Measured (bluewhale, W=4, polars, isolate + PYTHONMALLOC=malloc + POLARS_MAX_THREADS=1):
  **single request WORKS** (`/pl => {"s":10,"m":25.0}`); **concurrent = 13.6%
  failures** (39080 / 286152 Non-2xx). Unstable, not usable.
- Cause: polars is a **PyO3 extension**; its PyO3 build still carries the hard
  #576 guard that refuses `#[pymodule]` init in a non-main interpreter. Distinct
  from the single-phase-C classes above: **`app.isolate` gives each worker its own
  polars `.so` but does NOT defeat PyO3's interpreter-identity guard** — this is a
  PyO3-upstream issue, unfixable engine-side (you can't rebuild the user's polars).
- Why pyre's OWN PyO3 works but polars' doesn't: pyre's engine bootstraps in the
  **main** interp, its pyclasses (PyronovaRequest) use a process-global shared type
  object, and it's built with PyO3 0.29 (pyclass-in-subinterp fix). polars re-imports
  its whole pymodule fresh in each sub-interp → trips the guard.
- **polars — CORRECTED: runs zero-mod in sub-interps with PROPER isolation; NO binary
    hack needed.** Earlier "13.6% #576 flake, upstream-blocked, use polars-rs only" was
    an INVALID test: polars 1.43 splits its Rust core into a SEPARATE top-level package
    `_polars_runtime_32` (the pure-python `polars/` dir has NO .so; `_plr.py` loads
    `_polars_runtime_32/_polars_runtime.abi3.so`, 215 MB). `app.isolate("polars")` alone
    cloned only the python dir → all workers SHARED one .so → the guard let the first
    interpreter in and rejected the rest = the 13.6%.
  - **The guard, reverse-engineered (polars built with PyO3 0.29.0):** the "#576" string
    has exactly ONE xref (`lea rcx,[rip-0x7e8c06a]` @ VA 0x8cab3cf); exactly ONE branch
    reaches the error block (`jne 0x8cab37a` @ VA 0x8cab1fd). The check:
    `id = InterpreterState_GetID(); lock cmpxchg [module_state+0x78], id; ok = (slot was
    -1 i.e. first load) OR (slot == this id i.e. same interp); if !ok -> #576`. So it is
    **per-module-instance "first interpreter wins," NOT "main-only."** Therefore a
    per-worker PHYSICAL COPY (distinct module state slot) satisfies it legitimately —
    isolation is the correct fix, not patching the binary.
  - **Measured (bluewhale, W=4, `app.isolate("polars")` + `app.isolate("_polars_runtime_32")`
    + POLARS_MAX_THREADS=1):** `/pl => {"s":10,"m":25.0}`; **1.29M req / 25s @ 51k req/s,
    ZERO Non-2xx, zero #576, RSS 259 MB.** The AI-proposed "binary hack to bypass the
    guard" is UNNECESSARY (and would be UB): the RE showed the guard is per-instance, so
    satisfying it via isolation is both correct and hack-free. Papercut: user must isolate
    BOTH `polars` and `_polars_runtime_32` — the auto-isolate TODO (catch #576 → add the
    offending module) should cover it. Note: POLARS_MAX_THREADS=1 (its rayon pool would
    else be N_workers × N_cores threads); for heavy dataframe work, polars-rs at the Rust
    ENGINE layer (real host-level multithreading) is still the better architecture — but
    polars-in-UDF now WORKS for zero-mod user code.
  - **Old PyO3 (pre-0.29) → hard #576 panic that override cannot bypass may still exist
    for other libs; test each.**
  - **Newer PyO3 → declares the CPython single-phase `Py_MOD_MULTIPLE_INTERPRETERS_
    NOT_SUPPORTED` slot, exactly like numpy. Override-bypassable → behaves as a
    single-phase C-ext: needs isolate (per-worker copy) + override, SAME recipe as
    numpy. Engine-fixable.** Proven pristine (bare CPython sub-interp, no pyre):
    - **pydantic_core 2.46.3**: WITHOUT override → `module pydantic_core._pydantic_core
      does not support loading in subinterpreters` (numpy's exact error); **WITH
      `_override_multi_interp_extensions_check(-1)` → LOADED OK, SchemaValidator present.**
      So pydantic (web-server hot path) is SAVABLE, not #576-blocked.
    - **tokenizers 0.23.1**: works in pyre WITH isolate — 6.35M concurrent req / 25s @
      252k req/s, ZERO Non-2xx (W=4, isolate + PYTHONMALLOC=malloc).
  - **Rule: test each lib.** "It's PyO3 ⇒ broken" is WRONG (tokenizers/pydantic_core are
    fine, numpy-class). "It's PyO3 ⇒ needs no isolate" is also WRONG (they're single-
    phase, need the copy). cryptography/orjson/rpds-py UNTESTED — verify, don't infer.
- **pyre isolate BUG (surfaced by pydantic_core) — FIXED & verified.** pydantic_core is
  a package (`pydantic_core/__init__.py`) whose real ext is an INTERNAL submodule .so
  (`pydantic_core/_pydantic_core...so`). Two bugs in `_pyronova_isolate_libs`:
  (1) `importlib.util.find_spec(lib)` **raises `ValueError: <lib>.__spec__ is None`** when
  the lib is already in sys.modules as a single-phase re-init stub → killed isolate at
  sub-interp 0 init. (2) even after cloning, `import` returned the cached empty stub
  (`__file__=None`, no attrs) → `cannot import name '_pydantic_core' ... (unknown location)`.
  **FIX (`python/pyronova/_bootstrap.py`):** resolve the source with
  `importlib.machinery.PathFinder().find_spec` (searches sys.path, never consults
  sys.modules → no ValueError), with `find_spec` as fallback; and after prepending
  worker_dir, **evict the lib + its submodules from sys.modules** so the fresh per-worker
  clone loads. **Verified (bluewhale, W=4, isolate pydantic+pydantic_core):** `/pyd =>
  {"name":"leo","age":42,"bad_errors":1}` (full `from pydantic import BaseModel,
  ValidationError`, coercion + ValidationError correct); **432k req / 25s @ 17k req/s,
  zero Non-2xx, no #576, no crash.** So pydantic (FastAPI hot path) runs zero-mod in
  sub-interps. (Lower throughput vs tokenizers = the test rebuilds the model per request;
  a module-level model removes it.)
- Guidance: use the lib's **Rust core at the engine layer** (polars-rs / DataFusion),
  not its Python binding in a sub-interp UDF. UDF handles only what the Rust layer can't.

## mac vs Linux (why mac "just worked")
- **BLAS backend:** mac ARM numpy uses **Apple Accelerate** (OS/GCD-managed threads)
  → no self-managed pthread pool → B-class doesn't bite. Linux uses **OpenBLAS**
  (own pool) → crashes.
- **Allocator:** mac tolerates the cross-arena free; Linux glibc aborts (A-class).
- **Linker dedup:** mac dyld dedups by realpath/inode → per-worker copies isolate
  vendored libs; Linux ld.so dedups by **SONAME** → copies do NOT isolate vendored
  libs (needs patchelf unique-SONAME) — but that isolation doesn't fix B-class, so
  it's moot.

## What did NOT work (dead ends, don't retry)
- Per-package file copies alone → don't isolate the pymalloc arena (A-class) nor the
  thread pool (B-class).
- Keeping the main interp clean → doesn't fix anything (sklearn crashes pristine).
- `use_main_obmalloc=1` → removes A-class crash but races under load.
- **patchelf unique-SONAME per-worker OpenBLAS** → isolates instances (2→6 distinct)
  but B-class **still crashes** → not needed.
- **Adding a lock** around BLAS → can't inject cleanly (BLAS is deep in numpy);
  handler-level lock serializes everything (kills concurrency).
- Free-threading (PEP 703) as "the general answer" → WRONG: FT removes the GIL, so
  unmodified GIL-relying code/exts **data-race**; FT does not deliver zero-modification.
  Sub-interps PRESERVE per-worker GIL semantics → unmodified code stays correct. That
  is the reason to bet on sub-interps, not FT.

## Scenario fit
- **Data pipeline (partition-parallel ETL/feature prep):** the three prereqs are
  near-free — setup amortized over big partitions, allocator overhead negligible
  (compute-dominated), single-thread-per-worker IS the right model (= Spark tasks).
  This is the moat's best home: zero-copy Arrow + in-process own-GIL Python UDF beats
  Spark's socket/pickle/separate-process, and the constraints don't undercut it.
- **Request server:** same three prereqs; single-thread-per-worker is the gunicorn+numpy
  convention; allocator −6% is more visible on light plaintext paths.
- **Caveat both:** a single whole-dataset heavy linalg op (wants all cores) should run
  on the main interp / Rust layer with multi-threaded BLAS, NOT fanned out to sub-interps.

## Engine work to land (TODO — user changes nothing)
1. `.libs` by distribution name — DONE in `_bootstrap.py` (committed 6f499de).
2. PyMem → mimalloc/malloc via `PyMem_SetAllocator` at pymodule init (or re-exec with
   `PYTHONMALLOC=malloc`). NOT done. Note: measured mimalloc via LD_PRELOAD did NOT
   beat glibc malloc (both ≈ −6% vs pymalloc; pymalloc's small-object specialization
   is the lost win) — so this is a real ~6% cost when the ecosystem mode is on.
3. Default the `*_NUM_THREADS=1` env set for isolated workers (before their first
   numpy import). NOT done.
4. isolate package + internal-.so layout (find_spec ValueError + sys.modules stub) —
   DONE & verified (committed 7b7a0a1). Unblocked pydantic_core; no regression.
5. **Auto-isolate — DONE & verified.** Closes the last user-visible line: an unmodified
   `import numpy` works in own-GIL workers with NO `app.isolate(...)`. Implemented in
   `_bootstrap.py`. Verified matrix (0 failures): cold + WARM restart × declared
   (`app.isolate`) + undeclared (pure reactive) × workers 1/4/8, 3 restarts each.
   - **Load-time override via a `sys.meta_path` finder — NOT catch-then-retry.** The naive
     "let the import fail, then retry" design was abandoned: a single-phase ext's FAILED
     first attempt can half-register its def in CPython's PROCESS-GLOBAL extension table
     (`_PyRuntime.imports.extensions`), which `sys.modules` eviction cannot clear, so the
     retry aborts with `cannot load module more than once per process` (flaky, warm-restart
     only). Instead, `_IsolatingExtensionFinder` (at `sys.meta_path[0]`) wraps every
     ExtensionFileLoader with `_IsolatingExtensionLoader`, which flips the transient override
     around **`create_module`** (where the .so dlopen + PyInit + the single-phase check
     actually happen — NOT `exec_module`, a no-op for single-phase exts). The load succeeds
     on the FIRST try → no failed attempt → no registry pollution.
   - **Transient override, never persistent.** `_override_multi_interp_extensions_check` is a
     per-interpreter GLOBAL switch; leaving it on lets the NEXT un-isolated single-phase ext
     load SHARED (un-isolated) instead of hard-failing (measured: after isolating orjson with
     a persistent override, numpy then loaded shared and was never isolated). So it is flipped
     on only around each clone's load and restored immediately.
   - **Declared (`app.isolate`) vs undeclared.** Declared: pre-stage the clone + put worker_dir
     on `sys.path` at bootstrap → `import numpy` resolves the clone, loads first-try under the
     finder's override. Undeclared: the finder loads the ORIGINAL into the first worker that
     touches it; a LATER worker loading the same shared file gets `cannot load module more than
     once`, which `_iso_import` (a thin `__import__` wrapper) catches → clones the top-level
     package → restarts the import → resolves the private clone (a DIFFERENT file → different
     registry key → no collision). First worker uses the original, rest use clones — valid
     per-interp isolation (same first-wins shape as the polars per-instance guard).
   - **Clone SOURCE must resolve to the original install, never a sibling clone.** Bug found &
     fixed: `_iso_worker_dir` originally inserted worker_dir on `sys.path` BEFORE the source
     was resolved; on a WARM restart the existing clone shadowed the original, the freshness
     check mismatched, and `_clone` rmtree'd-then-cp'd the clone onto itself → destroyed it
     (`exists=False`) → warm restart aborted. Fix: path insertion moved to `_iso_ensure_on_path`,
     called AFTER cloning.
   - **Cost guard (never silent):** `_iso_report` logs every auto-isolated module + its cloned
     MB via `logging.getLogger("pyronova.isolate")` (bridged to Rust tracing); WARNING past
     `PYRONOVA_ISOLATE_WARN_BYTES` (default 100 MB) so a heavy 215 MB × N-worker clone is never
     silent.
6. **Teardown abort on graceful (SIGINT) shutdown — DONE & verified.** Finalizing a worker
   that loaded an isolated single-phase ext aborts on a cross-arena free at `Py_Finalize →
   Py_EndInterpreter → type_dealloc → _PyObject_Free` (~50% on macOS libmalloc; this is
   A-class #2 surfacing at FINALIZATION, not import). NOT introduced by auto-isolate — it
   reproduces on the base `app.isolate("numpy")` path too (4/8 on graceful SIGINT); the
   existing tests never caught it because they stop the server with SIGTERM (proc.terminate),
   which the Rust ctrl_c handler doesn't catch, so the process dies before finalization. Fix
   (`app.py` `run()`): on a graceful stop in subinterp/auto mode, `os._exit(0)` AFTER shutdown
   hooks, skipping CPython finalization (the OS reclaims everything). Because the same SIGINT
   also raises `KeyboardInterrupt` on the Python main thread (often landing on the first
   shutdown hook or right before the hard exit), SIGINT is set to `SIG_IGN` before the hooks
   run. Verified: SIGABRT 0/6, shutdown hooks 6/6. Confirmed root cause independently:
   `PYTHONMALLOC=malloc` also eliminates it (0/4) — that remains the proper fix for the
   IMPORT-time A-class crash on Linux (#2), still not done.

## OPEN / next design (其他的都是要解决的)
- **Arrow Flight + HTTP server** dual-protocol node: HTTP (control/small, existing
  hyper/tokio) + Arrow Flight (bulk Arrow data plane). pyre currently has only a
  hand-rolled minimal `src/grpc.rs` (no `tonic`/`prost`/`arrow`/`arrow-flight` deps).
  Design points: DoGet/DoPut/DoExchange; zero-copy wire→Arrow→sub-interp via the
  Arrow C Data Interface; backpressure via HTTP/2 flow control + bounded worker
  channel + optional Arrow-IPC spill-to-disk; Redpanda (not Kafka) only for
  uncontrolled external ingest, never for internal node→node (Flight direct).
- **The multi-thread advantage + "how far can it scale"** — the core value question
  to design/measure next.
- rayon: pyre uses **tokio** (async I/O), not rayon. If a rayon compute pool is added
  for the Rust ETL side, budget threads across tokio + rayon + TPC workers + (worker
  native pools) — one process, one thread budget.
