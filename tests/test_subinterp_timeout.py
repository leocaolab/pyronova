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

from tests._helpers import bound_port, read_file


def start_server(script_path):
    """Starts the script (it binds port 0); returns the process and the bound port."""
    # This test validates the old pool's 30s zombie-handler watchdog —
    # TPC's inline execution model has no way to interrupt a running
    # Python handler from another thread. Explicitly opt out of TPC
    # via env so we exercise the pool path.
    env = dict(os.environ)
    env["PYRONOVA_TPC"] = "0"
    log_path = script_path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
            env=env,
        )
    port = bound_port(read_file(log_path), proc)
    for _ in range(50):
        time.sleep(0.1)
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1)
            return proc, port
        except Exception:
            if proc.poll() is not None:
                out = read_file(log_path)()
                raise RuntimeError(f"Server exited early:\n{out}")
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait(timeout=10)
    out = read_file(log_path)()
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
    app.run(host="127.0.0.1", port=0, mode="subinterp", workers=2)
'''


@pytest.fixture(scope="module")
def server():
    script = "/tmp/pyronova_test_sync_timeout.py"
    with open(script, "w") as f:
        f.write(SLOW_SYNC_SCRIPT)
    proc, port = start_server(script)
    yield port
    stop_server(proc, port)


def test_sync_timeout_returns_504(server):
    """Sync handler exceeding 30s Rust timeout returns 504."""
    try:
        resp = urllib.request.urlopen(
            f"http://127.0.0.1:{server}/slow", timeout=35
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
            f"http://127.0.0.1:{server}/", timeout=5
        )
        status = resp.status
        body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
    assert status == 200
    assert body["ok"] is True
