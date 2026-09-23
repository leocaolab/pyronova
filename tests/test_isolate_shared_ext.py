"""A worker never uses an extension from a shared file unless CPython's own
multi-interpreter check allows it; everything else comes from its private copy,
built in the worker itself.

Why: since 3.13 CPython runs every extension's `PyInit_*` in the MAIN
interpreter. A single-phase module is built there and the worker gets a shallow
copy of its dict, so its objects live on main's heap (scipy's f2py `_fblas`:
`free(): invalid size` on the first setattr). And the loader used to set the
override for shared files too, which let a lib without its own guard (orjson)
load shared into every worker; finalizing those workers aborted with "pointer
being freed was not allocated" in `remove_all_subclasses`.
"""
from __future__ import annotations

import ast
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

_supported = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"),
    reason="isolate() clones via cp -c / cp --reflink (Linux/macOS)",
)

_BOOTSTRAP = Path(__file__).resolve().parent.parent / "python" / "pyronova" / "_bootstrap.py"


def _bootstrap_funcs(*names):
    """The named pure helpers from _bootstrap.py (it runs inside workers and
    can't be imported as a module)."""
    ns = {"_os": os}
    for node in ast.parse(_BOOTSTRAP.read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            exec(compile(ast.Module([node], []), str(_BOOTSTRAP), "exec"), ns)
    return [ns[n] for n in names]


def test_single_phase_check_matches_runtime():
    """The pre-init binary check agrees with the runtime answer (the def's
    m_slots) for every extension module numpy and scipy load."""
    pytest.importorskip("scipy")
    import ctypes
    import importlib
    for m in ("numpy", "scipy.linalg", "scipy.optimize"):
        importlib.import_module(m)
    (is_single,) = _bootstrap_funcs("_iso_dynamic_strtab", "_iso_is_single_phase_file")[1:]
    api = ctypes.pythonapi
    api.PyModule_GetDef.restype = ctypes.c_void_p
    api.PyModule_GetDef.argtypes = [ctypes.py_object]
    base = ctypes.sizeof(ctypes.c_ssize_t) + 4 * ctypes.sizeof(ctypes.c_void_p)
    seen_single = 0
    wrong = []
    for name, mod in list(sys.modules.items()):
        f = getattr(mod, "__file__", "") or ""
        if not f.endswith((".so", ".pyd")):
            continue
        d = api.PyModule_GetDef(mod)
        if not d:
            continue
        single = not ctypes.c_void_p.from_address(d + base + 4 * ctypes.sizeof(ctypes.c_void_p)).value
        seen_single += single
        if is_single(f) != single:
            wrong.append((name, single))
    assert seen_single, "expected scipy to load single-phase modules (f2py _fblas)"
    assert not wrong, f"binary check disagrees with runtime (name, is single-phase): {wrong}"


def test_single_phase_check_unreadable_counts_as_single(tmp_path):
    (is_single,) = _bootstrap_funcs("_iso_dynamic_strtab", "_iso_is_single_phase_file")[1:]
    junk = tmp_path / "junk.so"
    junk.write_bytes(b"not a binary")
    assert is_single(str(junk)) is True
    assert is_single(str(tmp_path / "missing.so")) is True


def _start(script_text, tmp_path, name, port, workers, env_extra=None):
    script = tmp_path / f"{name}.py"
    script.write_text(script_text.replace("{port}", str(port)))
    log = tmp_path / f"{name}.log"
    env = dict(os.environ, PYRONOVA_WORKERS=str(workers),
               PYRONOVA_ISOLATE_DIR=str(tmp_path / "copies"), **(env_extra or {}))
    proc = subprocess.Popen([sys.executable, str(script)],
                            stdout=open(log, "wb"), stderr=subprocess.STDOUT, env=env)
    return proc, log


def _get(url, proc, log, tries=400):
    for _ in range(tries):
        if proc.poll() is not None:
            pytest.fail(f"server exited early (rc={proc.returncode}):\n"
                        f"{log.read_text(errors='replace')[-3000:]}")
        try:
            r = urllib.request.urlopen(url, timeout=10)
            if r.status == 200:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            pytest.fail(f"{url} -> HTTP {e.code}:\n{log.read_text(errors='replace')[-3000:]}")
        except Exception:
            pass
        time.sleep(0.5)
    pytest.fail(f"server never became ready:\n{log.read_text(errors='replace')[-3000:]}")


def _sigint(proc):
    proc.send_signal(signal.SIGINT)
    try:
        return proc.wait(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()


_SCIPY_LAZY = '''
import os
from pyronova import Pyronova
app = Pyronova()
_D = os.path.realpath(os.environ["PYRONOVA_ISOLATE_DIR"])

@app.get("/sp")
def sp(req):
    # Not declared and not imported by main: the worker's import reaches the
    # shared site-packages files first.
    import scipy.optimize as so
    import scipy.linalg._fblas as fb
    return {"fun": float(so.minimize(lambda v: (v ** 2).sum(), [1.0, 2.0]).fun),
            "fblas_isolated": os.path.realpath(fb.__file__).startswith(_D)}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port}, mode="subinterp")
'''


@_supported
def test_shared_single_phase_ext_is_cloned_not_loaded_shared(tmp_path):
    """Before: CPython ran scipy's single-phase init in the main interpreter,
    whose own `import_array()` then hit numpy's "cannot load module more than
    once per process" in main -> every request 500 ("failed to import from
    subinterpreter due to exception"). Now the shared single-phase file is
    refused before its init runs, scipy is cloned, and `_fblas` is built in
    the worker. Finalization (no hard exit) must be clean too."""
    pytest.importorskip("scipy")
    port = 8995
    proc, log = _start(_SCIPY_LAZY, tmp_path, "scipy_lazy", port, workers=2)
    try:
        r = _get(f"http://127.0.0.1:{port}/sp", proc, log)
        assert r["fun"] < 1e-6
        assert r["fblas_isolated"], "scipy.linalg._fblas was loaded from the shared file"
    finally:
        rc = _sigint(proc)
    text = log.read_text(errors="replace")
    assert rc == 0, f"rc={rc}:\n{text[-3000:]}"
    assert "Fatal Python error" not in text, text[-3000:]


_ORJSON_TOP = '''
import os
from pyronova import Pyronova
app = Pyronova()
import orjson   # not declared; orjson has no load-once guard of its own
_D = os.path.realpath(os.environ["PYRONOVA_ISOLATE_DIR"])

@app.get("/oj")
def oj(req):
    import _interpreters, time
    time.sleep(0.02)  # hold the worker so requests spread across workers
    return {"interp": _interpreters.get_current()[0],
            "isolated": os.path.realpath(orjson.__file__).startswith(_D),
            "dumps": orjson.dumps({"a": 1}).decode()}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port}, mode="subinterp")
'''


@_supported
def test_undeclared_ext_without_own_guard_is_isolated_in_every_worker(tmp_path):
    """orjson declares it doesn't support sub-interpreters but has no guard of
    its own, so under the old always-on override every worker loaded the shared
    file and graceful shutdown aborted. Every worker must now use its own copy
    (built in the worker: orjson's pyo3-ffi def is not immortal, which also
    covers the def's refcount), and SIGINT must finalize cleanly."""
    pytest.importorskip("orjson")
    import concurrent.futures
    port = 8996
    proc, log = _start(_ORJSON_TOP, tmp_path, "orjson_top", port, workers=4)
    try:
        url = f"http://127.0.0.1:{port}/oj"
        _get(url, proc, log)
        with concurrent.futures.ThreadPoolExecutor(16) as ex:
            rs = list(ex.map(lambda _: json.loads(urllib.request.urlopen(url, timeout=10).read()),
                             range(64)))
    finally:
        rc = _sigint(proc)
    shared = sorted({r["interp"] for r in rs if not r["isolated"]})
    assert not shared, f"workers {shared} used the shared orjson"
    assert all(r["dumps"] == '{"a":1}' for r in rs)
    text = log.read_text(errors="replace")
    assert rc == 0, f"rc={rc}:\n{text[-3000:]}"
    assert "Fatal Python error" not in text, text[-3000:]


_FINALIZE = '''
import os
from pyronova import Pyronova
app = Pyronova()
app.isolate("numpy", "scipy", "orjson")
import numpy as np
import scipy.linalg
import orjson

@app.get("/x")
def x(req):
    import scipy.optimize as so
    return {"r": float(so.minimize(lambda v: (v ** 2).sum(), [1.0]).fun),
            "n": float(np.linalg.svd(np.eye(4), compute_uv=False).sum()),
            "o": len(orjson.dumps([1, 2]))}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port}, mode="subinterp")
'''


@_supported
def test_graceful_sigint_finalizes_isolated_workers(tmp_path):
    """run() no longer hard-exits: CPython finalizes after the engine ended
    every worker. Measured before the in-worker init: 6/6 SIGABRT at teardown
    ("pointer being freed was not allocated", dict_dealloc of main-owned
    objects); after: 0. Use the heaviest isolated set (numpy + scipy's
    single-phase modules + orjson)."""
    pytest.importorskip("scipy")
    pytest.importorskip("orjson")
    port = 8997
    proc, log = _start(_FINALIZE, tmp_path, "finalize", port, workers=4)
    try:
        for _ in range(8):
            _get(f"http://127.0.0.1:{port}/x", proc, log)
    finally:
        rc = _sigint(proc)
    text = log.read_text(errors="replace")
    assert rc == 0, f"rc={rc}:\n{text[-3000:]}"
    assert "Fatal Python error" not in text, text[-3000:]
