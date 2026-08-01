"""
C extension under sub-interpreters — a Rust/PyO3 0.29 native kernel running
inside Pyronova's per-interpreter-GIL workers, in parallel across all cores.

Since PyO3 modules declare `NOT_SUPPORTED` for sub-interpreters, we flip the
per-process override ONCE at the top of this script (it runs inside each
sub-interpreter at worker init), then import the native kernel normally. Each
sub-interpreter gets its OWN isolated instance of the extension (PyO3 0.29:
distinct module addresses, true isolation — see docs/subinterp-c-extension-compat).

Run:
    python examples/c_extension_subinterp.py
Load test:
    wrk -t8 -c256 -d10s http://127.0.0.1:8000/compute
"""
# --- flip the multi-interp check so the PyO3 native module can load here ---
# This script runs in the main interpreter (startup) AND inside every
# sub-interpreter (worker init). The override is only needed/allowed inside a
# sub-interpreter; the main interpreter imports PyO3 fine without it.
import _imp
try:
    _imp._override_multi_interp_extensions_check(-1)
except RuntimeError:
    pass  # main interpreter — not needed

import array
import pyo3_kernel  # Rust/PyO3 0.29 native extension (apply: zero-copy f64 UDF)

from pyronova import Pyronova

app = Pyronova()

N = 4096  # per-request compute size

@app.get("/compute")
def compute(req):
    # Build an f64 column and run the native Rust kernel on it, zero-copy.
    x = array.array("d", range(N))
    y = array.array("d", bytes(8 * N))
    pyo3_kernel.apply(memoryview(x), memoryview(y))
    return {"rows": N, "sample": y[1], "kernel": "pyo3-0.29 native, in sub-interpreter"}

@app.get("/")
def index(req):
    return {"ok": True}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, mode="subinterp")
