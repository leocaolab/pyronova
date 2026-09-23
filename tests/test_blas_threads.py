"""Multi-worker runs give each worker a single-threaded BLAS unless the user chose.

Each case runs in a fresh interpreter: the function sets process env vars and
changes the BLAS thread pool, which must not leak into other tests.
"""
import json
import os
import subprocess
import sys

import pytest

_PROBE = r"""
import json, os, sys
import numpy  # loaded BEFORE the limit, like an app that imports numpy at the top
from pyronova.app import _limit_blas_threads, _BLAS_THREAD_VARS
from threadpoolctl import threadpool_info
done = _limit_blas_threads()
print(json.dumps({
    "done": done,
    "env": {v: os.environ.get(v) for v in _BLAS_THREAD_VARS},
    "blas": [i["num_threads"] for i in threadpool_info() if i["user_api"] == "blas"],
}))
"""


def _run(env_extra):
    pytest.importorskip("numpy")
    pytest.importorskip("threadpoolctl")
    env = {k: v for k, v in os.environ.items() if k not in (
        "OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS")}
    env.update(env_extra)
    out = subprocess.run([sys.executable, "-c", _PROBE], env=env,
                         capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_defaults_to_one_thread_including_already_loaded_blas():
    r = _run({})
    assert r["done"] == "1 thread per worker"
    assert set(r["env"].values()) == {"1"}
    # Already-loaded BLAS (OpenBLAS on Linux) is shrunk at run time. macOS
    # arm64 numpy uses Accelerate, which threadpoolctl doesn't list.
    assert all(n == 1 for n in r["blas"]), r["blas"]


def test_user_setting_is_left_alone():
    r = _run({"OPENBLAS_NUM_THREADS": "4"})
    assert r["done"] == "left as set by OPENBLAS_NUM_THREADS"
    assert r["env"]["OPENBLAS_NUM_THREADS"] == "4"
    assert r["env"]["OMP_NUM_THREADS"] is None
