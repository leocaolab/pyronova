"""Regression for the PyErr_Print stderr storm (benchmark-17 audit bug #6).

Before the fix, a Python exception in a sub-interpreter handler ran
`PyErr_Print`, which writes synchronously and unbuffered to the
process's stderr fd. Under a 500k-rps flood that triggered many
exceptions, every worker serialized on the kernel stdio lock and
throughput collapsed.

Now handler exceptions are taken as PyO3 errors and logged through Rust's
`tracing` pipeline (non-blocking writer). We verify behaviourally that a
handler that raises does not spam stderr.
"""

import io
import subprocess
import sys
import textwrap


def test_raising_handler_does_not_spam_stderr():
    """Run a child Pyronova process that serves a route which raises, hit
    it once, and verify stderr stays quiet. The child runs with
    `mode="subinterp"` so the PyErr_Print path is the one that would
    have been hit before the fix.

    Tolerance: we don't require *zero* stderr bytes — Pyronova's startup
    prints a banner and the tracing subscriber may emit one-line
    warnings. We require that there's no raw Python traceback in
    stderr (those are the 10-20+ lines of noise the bug produced).
    """
    script = textwrap.dedent("""
        import os
        os.environ["PYRONOVA_WORKER"] = ""
        import threading, time, urllib.request
        from pyronova import Pyronova

        app = Pyronova()

        @app.get("/")
        def ok(req):
            return "ok"

        @app.get("/boom")
        def boom(req):
            raise RuntimeError("deliberate test failure")

        def main():
            t = threading.Thread(
                target=lambda: app.run(host="127.0.0.1", port=0, mode="subinterp"),
                daemon=True,
            )
            t.start()
            # Bound on port 0: read the port it got.
            for _ in range(60):
                if app._servers:
                    break
                time.sleep(0.1)
            servers = list(app._servers)
            base = f"http://127.0.0.1:{servers[0].port}" if servers else "http://127.0.0.1:0"
            for _ in range(60):
                time.sleep(0.1)
                try:
                    urllib.request.urlopen(base + "/", timeout=1)
                    break
                except Exception:
                    continue
            # Trigger the raising handler a few times.
            for _ in range(5):
                try:
                    urllib.request.urlopen(base + "/boom", timeout=2).read()
                except Exception:
                    pass

        main()
    """)

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        timeout=30,
        text=True,
    )
    combined = result.stderr + result.stdout
    # The signature of PyErr_Print is multi-line Python traceback:
    #   Traceback (most recent call last):
    #     File "...", line ...
    #   RuntimeError: deliberate test failure
    # If any of those appear raw (not JSON-encoded inside a tracing
    # record), the old path has leaked back in.
    traceback_lines = combined.count("Traceback (most recent call last):")
    # Allow 0–1 occurrences (CPython occasionally logs on interp
    # shutdown no matter what we do); > 1 is the regression.
    assert traceback_lines <= 1, (
        f"stderr contained {traceback_lines} raw Python tracebacks — "
        "PyErr_Print has leaked back onto the hot path. Output:\n"
        f"{combined[-2000:]}"
    )
