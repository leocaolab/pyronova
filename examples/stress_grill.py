"""16-worker sub-interpreter SOAK TEST — numpy + scipy + sklearn + orjson, each on
its OWN per-worker library copy, with BROAD API coverage per request (not one
repeated function). Exercises the "per-worker physical copy" isolation strategy
for C extensions that hold process-global state (see
docs/subinterp-c-extension-status.md).

Why copies: numpy/orjson/lxml can't load into a 2nd sub-interpreter (global state).
A physically distinct copy per worker gives each its own global state → true
shared-nothing isolation (no data races, no dependence on upstream thread-safety).

Self-contained: on first run it clones the copies (APFS `cp -c` on macOS — instant,
copy-on-write) into /tmp/libcopies. Requires: numpy, scipy, scikit-learn, orjson
installed in the active venv.

Run:   PYRONOVA_WORKERS=16 python examples/stress_grill.py
Grill: wrk -t8 -c128 -d180s http://127.0.0.1:8000/grill

Measured (M5 Pro, Python 3.14.6, PyO3 0.29, ~7 min / ~680k requests):
  throughput 2,509 req/s · RSS flat 476→476 MB (zero leak) · 75 MB/worker
  zero double-free (MallocScribble+MallocErrorAbort) · zero deadlock · zero crash
"""
import os
import fcntl
import subprocess
import sysconfig

_BASE = "/tmp/libcopies"
_LIBS = ("numpy", "scipy", "sklearn", "orjson")


def _setup_copies():
    """Clone one private copy of each lib per (worker + spare). Idempotent."""
    if os.path.exists(os.path.join(_BASE, "counter")):
        return
    n = int(os.environ.get("PYRONOVA_WORKERS", "16")) + 2
    sp = sysconfig.get_paths()["purelib"]
    os.makedirs(_BASE, exist_ok=True)
    for i in range(n):
        d = os.path.join(_BASE, f"w{i}")
        os.makedirs(d, exist_ok=True)
        for lib in _LIBS:
            src = os.path.join(sp, lib)
            if os.path.isdir(src) and not os.path.exists(os.path.join(d, lib)):
                # cp -c = APFS clone on macOS (instant, COW); plain copy elsewhere
                subprocess.run(["cp", "-c", "-R", src, os.path.join(d, lib)], check=False)
    with open(os.path.join(_BASE, "counter"), "w") as f:
        f.write("0")


# --- allow C extensions to load in this sub-interpreter (main interp: no-op) ---
import _imp
try:
    _imp._override_multi_interp_extensions_check(-1)
except RuntimeError:
    pass

_setup_copies()


def _claim_copy():
    fd = os.open(os.path.join(_BASE, "counter"), os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    idx = int((os.read(fd, 16).decode().strip() or "0"))
    os.lseek(fd, 0, 0); b = str(idx + 1).encode(); os.write(fd, b); os.ftruncate(fd, len(b))
    fcntl.flock(fd, fcntl.LOCK_UN); os.close(fd)
    return idx


IDX = _claim_copy()
import sys
sys.path.insert(0, os.path.join(_BASE, f"w{IDX}"))   # this worker's OWN copies

import numpy as np
import orjson
# eager-load scipy + sklearn in every worker (so the soak/memory numbers are honest)
import scipy.linalg, scipy.fft  # noqa: E401
import sklearn.linear_model, sklearn.cluster, sklearn.preprocessing, sklearn.decomposition  # noqa: E401


def _np(rng):
    """Broad numpy C-path coverage — BLAS, LAPACK, FFT, sort, indexing, ufunc."""
    a = rng.standard_normal((160, 160))
    c = int(rng.integers(0, 9))
    if c == 0: return float(np.linalg.svd(a, compute_uv=False).sum())      # LAPACK gesdd
    if c == 1: return float((a @ a.T).trace())                            # BLAS gemm
    if c == 2: return float(np.fft.fft2(a).real.sum())                    # pocketfft
    if c == 3: return float(np.sort(a.ravel())[::3].sum())                # sort + strided slice
    if c == 4: return float(np.linalg.inv(a + 160 * np.eye(160)).sum())   # LAPACK getrf/getri
    if c == 5: return float(a[a > 0].mean())                              # boolean index
    if c == 6: return float(np.exp(np.sin(a)).std())                      # ufunc chain
    if c == 7: return float(np.percentile(a, [10, 50, 90]).sum())         # partition
    return float(np.linalg.eigvalsh(a @ a.T).sum())                       # LAPACK syevd


def _oj(rng):
    """Broad orjson coverage — nested, unicode, floats, options, loads."""
    d = {"id": int(rng.integers(0, 99999)),
         "vals": [float(x) for x in rng.random(30)],
         "nested": {"a": [1, 2, 3], "txt": "中文 unicode ✓ ሴ", "f": [1.5] * 20},
         "big": list(range(200))}
    c = int(rng.integers(0, 4))
    if c == 0: return len(orjson.dumps(d))
    if c == 1: return len(orjson.dumps(d, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))
    if c == 2: return len(orjson.loads(orjson.dumps(d)))
    return len(orjson.dumps({"k": [d, d]}))


def _ml(rng):
    """Broad sklearn coverage — fit/predict/transform across estimators."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA
    X = rng.standard_normal((120, 10)); y = (X[:, 0] + X[:, 1] > 0).astype(int)
    c = int(rng.integers(0, 4))
    if c == 0: return float(LogisticRegression(max_iter=60).fit(X, y).score(X, y))
    if c == 1: return float(StandardScaler().fit_transform(X).std())
    if c == 2: return int(KMeans(4, n_init=1).fit(X).labels_.sum())
    return float(PCA(3).fit_transform(X).sum())


from pyronova import Pyronova
app = Pyronova()


@app.get("/grill")
def grill(req):
    rng = np.random.default_rng()
    p = int(rng.integers(0, 10))
    if p < 6:
        return {"lib": "np", "r": _np(rng), "w": IDX}   # 60% numpy
    if p < 9:
        return {"lib": "oj", "r": _oj(rng), "w": IDX}   # 30% orjson
    return {"lib": "ml", "r": _ml(rng), "w": IDX}        # 10% sklearn (heavy)


@app.get("/")
def index(req):
    return {"ok": True, "worker": IDX}


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, mode="subinterp")
