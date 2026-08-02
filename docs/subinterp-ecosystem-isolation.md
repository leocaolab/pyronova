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
1. `.libs` by distribution name — DONE in `_bootstrap.py` (uncommitted).
2. PyMem → mimalloc/malloc via `PyMem_SetAllocator` at pymodule init (or re-exec with
   `PYTHONMALLOC=malloc`). NOT done. Note: measured mimalloc via LD_PRELOAD did NOT
   beat glibc malloc (both ≈ −6% vs pymalloc; pymalloc's small-object specialization
   is the lost win) — so this is a real ~6% cost when the ecosystem mode is on.
3. Default the `*_NUM_THREADS=1` env set for isolated workers (before their first
   numpy import). NOT done.
4. Auto-isolate: detect single-phase C exts (or catch the load failure) and add them
   to the isolate set automatically, so the user doesn't even write `app.isolate(...)`.
   NOT done. This closes the last user-visible line.

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
