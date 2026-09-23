"""Tests for sub-interpreter request timeout and zombie prevention.

Covers:
- Sync handler exceeding 30s returns 504 Gateway Timeout
- Server remains healthy after timeout (no worker pool exhaustion)
- Dead-request skip: sync workers check response_tx.is_closed() before execution
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
    # This test validates the old pool's 30s zombie-handler watchdog —
    # TPC's inline execution model has no way to interrupt a running
    # Python handler from another thread. Explicitly opt out of TPC
    # via env so we exercise the pool path.
    env = dict(os.environ)
    env["PYRONOVA_TPC"] = "0"
    proc = subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
        env=env,
    )
    for _ in range(50):
        time.sleep(0.1)
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1)
            return proc
        except Exception:
            if proc.poll() is not None:
                out = proc.stdout.read().decode(errors="replace")
                raise RuntimeError(f"Server exited early:\n{out}")
    # Stop it before reading: read() waits for EOF, which a live server never sends.
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    out = proc.communicate(timeout=10)[0].decode(errors="replace")
    raise RuntimeError(f"Server failed to start:\n{out}")


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


SLOW_SYNC_SCRIPT = r'''
import os
os.environ["PYRONOVA_WORKER"] = ""
from pyronova import Pyronova

app = Pyronova()

@app.get("/")
def index(req):
    return {"ok": True}

@app.get("/slow")
def slow(req):
    import time
    time.sleep(35)
    return {"should": "never reach"}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=19894, mode="subinterp", workers=2)
'''


@pytest.fixture(scope="module")
def server():
    script = "/tmp/pyronova_test_sync_timeout.py"
    with open(script, "w") as f:
        f.write(SLOW_SYNC_SCRIPT)
    proc = start_server(script, 19894)
    yield proc
    stop_server(proc, 19894)


def test_sync_timeout_returns_504(server):
    """Sync handler exceeding 30s Rust timeout returns 504."""
    try:
        resp = urllib.request.urlopen(
            "http://127.0.0.1:19894/slow", timeout=35
        )
        status = resp.status
        body = resp.read()
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
    assert status == 504, f"Expected 504, got {status}: {body}"


def test_server_healthy_after_timeout(server):
    """After a 504 timeout, subsequent fast requests succeed."""
    try:
        resp = urllib.request.urlopen(
            "http://127.0.0.1:19894/", timeout=5
        )
        status = resp.status
        body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
    assert status == 200
    assert body["ok"] is True
