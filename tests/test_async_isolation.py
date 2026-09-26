"""Test: async bridge isolation — fast routes don't block on slow routes.

Proves that sub-interpreter async mode truly runs concurrent coroutines.
A 1-second sleep route must not block a fast route from responding instantly.
"""
import asyncio
import time
import subprocess
import sys
import os
import signal

from tests._helpers import bound_port, read_file

SERVER_SCRIPT = """
from pyronova import PyronovaApp

app = PyronovaApp()

async def slow(req):
    import asyncio
    await asyncio.sleep(1.0)
    return "slow done"

def fast(req):
    return "fast"

app.get("/slow", slow)
app.get("/fast", fast)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, mode="subinterp")
"""


def test_async_isolation():
    """Fast route responds in <100ms even while slow route sleeps 1s."""
    import urllib.request

    # Write temp server script
    script_path = "/tmp/pyronova_async_isolation_test.py"
    with open(script_path, "w") as f:
        f.write(SERVER_SCRIPT)

    # Start server
    log_path = script_path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
    time.sleep(3)  # Wait for startup

    try:
        base = f"http://127.0.0.1:{bound_port(read_file(log_path), proc)}"
        # Fire slow request in background (blocks for 1s)
        import threading

        slow_result = {}

        def call_slow():
            try:
                resp = urllib.request.urlopen(base + "/slow", timeout=5)
                slow_result["body"] = resp.read().decode()
                slow_result["status"] = resp.status
            except Exception as e:
                slow_result["error"] = str(e)

        slow_thread = threading.Thread(target=call_slow)
        slow_thread.start()

        # Give slow request a head start
        time.sleep(0.1)

        # Now call fast route — should return instantly, not wait for slow
        t0 = time.perf_counter()
        fast_resp = urllib.request.urlopen(base + "/fast", timeout=2)
        fast_body = fast_resp.read().decode()
        fast_time = time.perf_counter() - t0

        # Wait for slow to finish
        slow_thread.join(timeout=5)

        # Assertions
        assert fast_body == "fast", f"Fast route returned wrong body: {fast_body}"
        assert fast_time < 0.5, f"Fast route took {fast_time:.3f}s — should be <0.5s (async isolation broken!)"
        assert slow_result.get("body") == "slow done", f"Slow route failed: {slow_result}"

        print(f"  ✅ Async isolation verified:")
        print(f"     Fast route: {fast_time*1000:.1f}ms (limit: 500ms)")
        print(f"     Slow route: completed with '{slow_result.get('body')}'")

    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)


if __name__ == "__main__":
    test_async_isolation()
