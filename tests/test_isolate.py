"""app.isolate() — per-worker C-extension copies.

Verifies numpy loads in EVERY own-GIL sub-interpreter worker (no
"cannot load module more than once") and each worker's numpy resolves to its
own isolated copy, driven concurrently so requests spread across workers.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import subprocess
import sys
import time
import urllib.request

import pytest

_supported = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"),
    reason="isolate() clones via cp -c / cp --reflink (Linux/macOS)",
)

_PORT = 8973
_SERVER = f'''
from pyronova import Pyronova
app = Pyronova()
app.isolate("numpy")

import os as _os
_ISO_DIR = _os.environ.get("PYRONOVA_ISOLATE_DIR", "/tmp/pyronova-isolate")

@app.get("/np")
def np_op(req):
    import numpy as np
    import _interpreters
    import time
    time.sleep(0.02)  # hold the worker briefly so concurrency spreads across workers
    return {{"numpy": np.__version__,
             "interp": _interpreters.get_current()[0],
             "isolated": np.__file__.startswith(_ISO_DIR)}}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={_PORT}, mode="subinterp")
'''


@pytest.fixture
def isolate_server(tmp_path):
    pytest.importorskip("numpy")
    script = tmp_path / "iso_server.py"
    script.write_text(_SERVER)
    copies = tmp_path / "copies"
    env = dict(
        os.environ,
        PYRONOVA_WORKERS="4",
        PYRONOVA_ISOLATE_DIR=str(copies),
    )
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
    )
    url = f"http://127.0.0.1:{_PORT}/np"
    try:
        ready = False
        for _ in range(160):  # workers must clone numpy + import it → allow time
            try:
                r = urllib.request.urlopen(url, timeout=2)
                if json.loads(r.read()).get("isolated"):
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.5)
        if not ready:
            pytest.fail("isolate server never became ready")
        yield url, copies
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@_supported
def test_isolate_numpy_across_workers(isolate_server):
    url, copies = isolate_server

    def hit(_):
        r = urllib.request.urlopen(url, timeout=8)
        return json.loads(r.read())

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        results = list(ex.map(hit, range(200)))

    # Every response: numpy loaded (no "cannot load module more than once") and
    # resolved to an isolated per-worker copy.
    assert all("numpy" in r for r in results)
    assert all(r["isolated"] for r in results), "some worker's numpy was not isolated"

    # The decisive check: EVERY one of the 4 workers cloned its own numpy — i.e.
    # 4 independent numpy copies coexist in one process, which is exactly what
    # bare sub-interpreters cannot do (they'd hit "cannot load module more than once").
    worker_numpys = list(copies.glob("*/w*/numpy"))
    assert len(worker_numpys) == 4, f"expected 4 per-worker numpy copies, got {len(worker_numpys)}"


# -- Reactive auto-isolate (no app.isolate() call at all) ---------------------
#
# The self-healing import hook (_bootstrap._iso_import) must make an UNMODIFIED
# `import numpy` work in every own-GIL worker: the first import trips
# "does not support loading in subinterpreters", the hook reads the offending
# module out of the error, clones numpy, and retries — with zero app.isolate().

_PORT_AUTO = 8974
_SERVER_AUTO = f'''
from pyronova import Pyronova
app = Pyronova()
# NOTE: no app.isolate(...) — isolation must happen reactively.

import os as _os
_ISO_DIR = _os.environ.get("PYRONOVA_ISOLATE_DIR", "/tmp/pyronova-isolate")

# Top-level `import numpy` (the realistic way a user writes it): every worker
# runs the script body at init, so each independently trips the isolation error
# and reactively clones its own numpy — no reliance on request load spreading
# across all workers (pyre may route a burst to a single worker).
import numpy as np

@app.get("/np")
def np_op(req):
    import _interpreters
    import time
    time.sleep(0.02)
    return {{"numpy": np.__version__,
             "interp": _interpreters.get_current()[0],
             "isolated": np.__file__.startswith(_ISO_DIR)}}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={_PORT_AUTO}, mode="subinterp")
'''


@pytest.fixture
def auto_isolate_server(tmp_path):
    pytest.importorskip("numpy")
    script = tmp_path / "auto_iso_server.py"
    script.write_text(_SERVER_AUTO)
    copies = tmp_path / "copies"
    env = dict(
        os.environ,
        PYRONOVA_WORKERS="4",
        PYRONOVA_ISOLATE_DIR=str(copies),
    )
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
    )
    url = f"http://127.0.0.1:{_PORT_AUTO}/np"
    try:
        ready = False
        for _ in range(160):  # workers must clone numpy + import it → allow time
            try:
                r = urllib.request.urlopen(url, timeout=2)
                if json.loads(r.read()).get("isolated"):
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.5)
        if not ready:
            pytest.fail("auto-isolate server never became ready")
        yield url, copies
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@_supported
def test_auto_isolate_numpy_without_declaration(auto_isolate_server):
    url, copies = auto_isolate_server

    def hit(_):
        r = urllib.request.urlopen(url, timeout=8)
        return json.loads(r.read())

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        results = list(ex.map(hit, range(200)))

    # Every response: numpy loaded and resolved to an isolated per-worker copy —
    # achieved reactively, with no app.isolate("numpy") in the server script.
    assert all("numpy" in r for r in results)
    assert all(r["isolated"] for r in results), "reactive auto-isolate did not isolate numpy"

    # Reactive isolations land under the stable `auto` bucket (no declared-lib
    # signature); each of the 4 workers cloned its own numpy there.
    worker_numpys = list(copies.glob("auto/w*/numpy"))
    assert len(worker_numpys) == 4, f"expected 4 auto-isolated numpy copies, got {len(worker_numpys)}"
