"""Tests for async handler timeout in sub-interpreter mode.

The async engine wraps coroutines with asyncio.wait_for(timeout=28s),
which is 2s before Rust's 30s gateway timeout. This prevents phantom
load from zombie Python tasks after client disconnects.

Covers:
- Fast async handlers complete normally
- Slow async handlers (>28s) get Python-side TimeoutError → 504
- Server remains healthy after async timeout
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
import urllib.error

import pytest


def start_server(script_path, port):
    # Old pool 28s async-handler watchdog — no TPC equivalent, see
    # test_subinterp_timeout for the same rationale.
    env = dict(os.environ)
    env["PYRONOVA_TPC"] = "0"
    proc = subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
        env=env,
    )
    # Phase 1: wait for sync endpoint (confirms Rust accept loop is up).
    for _ in range(50):
        time.sleep(0.1)
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1)
            break
        except Exception:
            if proc.poll() is not None:
                out = proc.stdout.read().decode(errors="replace")
                raise RuntimeError(f"Server exited early:\n{out}")
    else:
        # Stop it before reading: read() waits for EOF, which a live server never sends.
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        out = proc.communicate(timeout=10)[0].decode(errors="replace")
        raise RuntimeError(f"Server failed to start:\n{out}")
    # Phase 2: wait for async workers (_async_engine.py init: asyncio event
    # loop + sub-interpreter bootstrap). Workers start lazily; on this hardware
    # initialization can take 30+ seconds. One patient request is better than
    # many short-timeout requests that flood the queue without waiting.
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/fast-async", timeout=60)
    except urllib.error.HTTPError:
        pass  # non-200 still means the worker responded
    except Exception:
        pass  # timed out — test will handle it
    return proc


def stop_server(proc, port):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            pass
    subprocess.run(f"lsof -ti:{port} | xargs kill -9 2>/dev/null", shell=True)
    time.sleep(0.3)


ASYNC_TIMEOUT_SCRIPT = r'''
import os
os.environ["PYRONOVA_WORKER"] = ""
from pyronova import Pyronova

app = Pyronova()

@app.get("/")
def index(req):
    return {"ok": True}

@app.get("/slow-async")
async def slow_async(req):
    import asyncio
    await asyncio.sleep(35)
    return {"should": "never reach"}

@app.get("/fast-async")
async def fast_async(req):
    import asyncio
    await asyncio.sleep(0.01)
    return {"fast": True}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=19893, mode="subinterp")
'''


@pytest.fixture(scope="module")
def server():
    script = "/tmp/pyronova_test_async_timeout.py"
    with open(script, "w") as f:
        f.write(ASYNC_TIMEOUT_SCRIPT)
    proc = start_server(script, 19893)
    yield proc
    stop_server(proc, 19893)


def test_fast_async_succeeds(server):
    """Fast async handlers complete normally."""
    try:
        resp = urllib.request.urlopen(
            "http://127.0.0.1:19893/fast-async", timeout=10
        )
        status = resp.status
        body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
    assert status == 200
    assert body["fast"] is True


def test_slow_async_times_out(server):
    """Async handler exceeding 28s Python timeout returns 504."""
    try:
        resp = urllib.request.urlopen(
            "http://127.0.0.1:19893/slow-async", timeout=35
        )
        status = resp.status
        body = resp.read()
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
    assert status == 504, f"Expected 504, got {status}: {body}"


def test_server_healthy_after_async_timeout(server):
    """After async timeout, server still handles requests normally."""
    try:
        resp = urllib.request.urlopen(
            "http://127.0.0.1:19893/", timeout=5
        )
        status = resp.status
        body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
    assert status == 200
    assert body["ok"] is True
