"""app.isolate() — per-worker C-extension copies.

Verifies numpy loads in EVERY own-GIL sub-interpreter worker (no
"cannot load module more than once") and each worker's numpy resolves to its
own isolated copy, driven concurrently so requests spread across workers.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import signal
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


# ---------------------------------------------------------------------------
# Coverage for the v2.7.0 changes that manual/grill runs exercised but the
# pytest suite did not: warm restart (reused clone dir), graceful SIGINT
# shutdown (no cross-arena abort + shutdown hooks still run), and the built-in
# single-phase extension fallback (faulthandler, pulled in by sklearn→joblib→
# loky) loading shared under the transient override.
# ---------------------------------------------------------------------------


def _start_server(script_text, tmp_path, name, port, workers=4, isolate_dir=None, env_extra=None):
    """Write a server script, launch it as a subprocess (logs to a file), return
    (proc, log_path). Caller waits for readiness via `_wait_ready`."""
    script = tmp_path / f"{name}.py"
    script.write_text(script_text.format(port=port))
    log = tmp_path / f"{name}.log"
    env = dict(os.environ, PYRONOVA_WORKERS=str(workers))
    if isolate_dir is not None:
        env["PYRONOVA_ISOLATE_DIR"] = str(isolate_dir)
    if env_extra:
        env.update(env_extra)
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=open(log, "wb"), stderr=subprocess.STDOUT, env=env,
    )
    return proc, log


def _wait_ready(url, proc, log, tries=200):
    for _ in range(tries):
        if proc.poll() is not None:  # died during startup — surface the real error
            pytest.fail(
                f"server exited early (rc={proc.returncode}):\n"
                f"{log.read_text(errors='replace')[-2500:]}"
            )
        try:
            r = urllib.request.urlopen(url, timeout=2)
            if r.status == 200:
                return json.loads(r.read())
        except Exception:
            pass
        time.sleep(0.5)
    pytest.fail(f"server never became ready:\n{log.read_text(errors='replace')[-2500:]}")


def _stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


_SERVER_WARM = '''
from pyronova import Pyronova
app = Pyronova()
import os as _o
_D = _o.environ.get("PYRONOVA_ISOLATE_DIR", "/tmp/pyronova-isolate")
import numpy as np   # top-level, no app.isolate() -> reactive auto-isolate
@app.get("/np")
def h(req):
    return {{"v": np.__version__, "isolated": np.__file__.startswith(_D)}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port}, mode="subinterp")
'''


@_supported
def test_auto_isolate_survives_warm_restart(tmp_path):
    """Cold run then WARM run on the SAME clone dir. The warm run reuses the
    existing per-worker clones; a regression where the clone SOURCE resolved to
    a sibling clone (worker_dir on sys.path before cloning) rmtree'd the clone
    onto itself and the retry fell back to the shared original → "cannot load
    module more than once". Both runs must serve isolated numpy."""
    pytest.importorskip("numpy")
    copies = tmp_path / "copies"
    port = 8991
    url = f"http://127.0.0.1:{port}/np"
    for run in range(2):  # run 0 = cold (creates clones); run 1 = warm (reuses)
        proc, log = _start_server(_SERVER_WARM, tmp_path, f"warm{run}", port,
                                  workers=4, isolate_dir=copies)
        try:
            r = _wait_ready(url, proc, log)
            assert r["isolated"], f"run {run} ({'cold' if run == 0 else 'warm'}): numpy not isolated"
        finally:
            _stop(proc)


_SERVER_SHUTDOWN = '''
from pyronova import Pyronova
app = Pyronova()
import os as _o
_MARK = _o.environ["SHUTDOWN_MARK"]
app.isolate("numpy")
import numpy as np   # isolated single-phase ext -> the teardown-crash trigger
@app.on_shutdown
def _bye():
    with open(_MARK, "w") as f:
        f.write("ran")
@app.get("/np")
def h(req):
    return {{"v": np.__version__}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port}, mode="subinterp")
'''


@_supported
def test_graceful_sigint_no_abort_and_hooks_run(tmp_path):
    """Finalizing a worker that loaded an isolated single-phase ext aborted on a
    cross-arena free at Py_EndInterpreter (~50% on macOS). On a graceful SIGINT
    stop, app.run() now os._exit(0)s after shutdown hooks (skipping finalization),
    and SIG_IGNs SIGINT first so the same signal can't interrupt the hooks or
    skip the hard exit. Assert: clean exit (rc==0, not SIGABRT) AND hook ran."""
    pytest.importorskip("numpy")
    mark = tmp_path / "shutdown.mark"
    port = 8992
    proc, log = _start_server(_SERVER_SHUTDOWN, tmp_path, "shutdown", port,
                              workers=4, isolate_dir=tmp_path / "copies",
                              env_extra={"SHUTDOWN_MARK": str(mark)})
    try:
        _wait_ready(f"http://127.0.0.1:{port}/np", proc, log)
        proc.send_signal(signal.SIGINT)
        rc = proc.wait(timeout=25)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert rc != -signal.SIGABRT, (
        "sub-interpreter teardown aborted (SIGABRT) on graceful shutdown:\n"
        f"{log.read_text(errors='replace')[-1800:]}"
    )
    assert rc == 0, f"expected clean exit via os._exit(0), got rc={rc}"
    assert mark.exists(), "shutdown hook did not run before the hard exit"


_SERVER_BUILTIN = '''
from pyronova import Pyronova
app = Pyronova()
import faulthandler   # built-in single-phase ext; top-level -> runs in every worker
@app.get("/fh")
def h(req):
    import faulthandler as f
    return {{"loaded": f.__name__ == "faulthandler"}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port}, mode="subinterp")
'''


@_supported
def test_builtin_single_phase_ext_loads_in_workers(tmp_path):
    """A built-in single-phase ext (faulthandler — pulled in transitively by
    sklearn→joblib→loky) has no clonable source, so it can't be per-worker
    copied; it must load SHARED under the transient override. Without that
    fallback, worker init died with "module faulthandler does not support
    loading in subinterpreters" (caught by the grill soak on Linux)."""
    port = 8993
    proc, log = _start_server(_SERVER_BUILTIN, tmp_path, "builtin", port,
                              workers=4, isolate_dir=tmp_path / "copies")
    try:
        r = _wait_ready(f"http://127.0.0.1:{port}/fh", proc, log)
        assert r["loaded"], "faulthandler did not load in the sub-interpreter workers"
    finally:
        _stop(proc)
