"""An async request in flight 30 s after startup completes, and the server stops cleanly.

`_async_engine.py` used to wait for its fetcher thread with a 30 s timeout that started
counting when the worker started. 30 s into normal serving it logged a false "fetcher
did not exit" error and cancelled every task on the worker's loop, so any async request
in flight at that moment failed.
"""
import threading
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest

from tests._helpers import bound_port, read_file

SCRIPT = '''
from pyronova import Pyronova
app = Pyronova()

@app.get("/")
def index(req):
    return {"ok": True}

@app.get("/a")
async def a(req):
    return {"async": True}

@app.get("/slow")
async def slow(req):
    import asyncio
    await asyncio.sleep(8)
    return {"slow": True}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, mode="subinterp", workers=2)
'''


def _get(port, path, timeout=5):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=timeout) as r:
        return r.status


def _try(port, path, timeout):
    try:
        return _get(port, path, timeout)
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001 — reported by the assertion
        return repr(e)


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="POSIX signals")
def test_async_workers_outlive_30s_and_stop_cleanly(tmp_path):
    script = tmp_path / "app.py"
    script.write_text(SCRIPT)
    log = tmp_path / "server.log"
    env = dict(os.environ, PYRONOVA_TPC="0")  # the async pool only exists outside TPC
    with open(log, "w") as out:
        proc = subprocess.Popen([sys.executable, str(script)], stdout=out,
                                stderr=subprocess.STDOUT, env=env, start_new_session=True)
    try:
        started = time.time()
        port = bound_port(read_file(str(log)), proc)
        deadline = time.time() + 60
        while True:
            try:
                if _get(port, "/a", timeout=2) == 200:
                    break
            except Exception:
                if proc.poll() is not None or time.time() > deadline:
                    raise AssertionError("server did not come up:\n" + log.read_text())
                time.sleep(0.2)

        # Straddle the 30 s mark with a request that is still running when it passes.
        time.sleep(max(0.0, 26 - (time.time() - started)))
        result = {}
        t = threading.Thread(target=lambda: result.update(status=_try(port, "/slow", 20)))
        t.start()
        t.join(30)
        assert result.get("status") == 200, (result, log.read_text()[-3000:])
        assert [_get(port, "/a") for _ in range(20)] == [200] * 20
        assert "did not exit within" not in log.read_text()

        proc.send_signal(signal.SIGINT)
        try:
            rc = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            raise AssertionError("graceful stop hung:\n" + log.read_text()[-3000:])
        assert rc == 0, log.read_text()[-3000:]
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
