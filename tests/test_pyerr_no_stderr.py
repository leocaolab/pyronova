"""Regression for the PyErr_Print stderr storm (benchmark-17 audit bug #6).

Before the fix, a Python exception in a sub-interpreter handler ran
`PyErr_Print`, which writes synchronously and unbuffered to the
process's stderr fd. Under a 500k-rps flood that triggered many
exceptions, every worker serialized on the kernel stdio lock and
throughput collapsed.

Now the hot path routes exceptions through `log_and_clear_py_exception`,
which captures the exception string and emits it via Rust's `tracing`
pipeline (non-blocking async writer).

We verify:
  1. The helper exists and is the primary error-reporting path.
  2. Calling a handler that raises an exception does not spam stderr
     (this would be the smoking gun for a regression).
"""

import io
import pathlib
import subprocess
import sys
import textwrap


def test_log_helper_present_and_wired():
    src = "\n".join(p.read_text() for p in pathlib.Path("src/python").glob("*.rs"))
    assert "fn log_and_clear_py_exception" in src
    # Hot-path call sites: handler errors, hook errors, json.dumps,
    # run_until_complete, _Response construction, async engine exec.
    assert src.count("log_and_clear_py_exception(") >= 5, (
        "expected the new logger to cover every per-request exception "
        "path; fewer than 5 call sites means we missed a PyErr_Print"
    )
    # Hot-path PyErr_Print calls should be gone from the per-request
    # codepath. Allow a few remaining at startup (init_in_sub_interp,
    # pre-flight checks that run once per sub-interp).
    hot_paths = [
        "handler raised an exception",
        "before_request hook {hook_name",
        "loop.run_until_complete() failed",
        "json.dumps failed",
        "failed to create _Response",
    ]
    for marker in hot_paths:
        # Each marker is paired with log_and_clear_py_exception, not PyErr_Print.
        idx = src.find(marker)
        assert idx != -1, f"expected marker not found: {marker}"
        window = src[max(0, idx - 400):idx]
        assert "PyErr_Print" not in window, (
            f"hot path near {marker!r} still calls PyErr_Print — "
            "replace with log_and_clear_py_exception"
        )


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
                target=lambda: app.run(host="127.0.0.1", port=19894, mode="subinterp"),
                daemon=True,
            )
            t.start()
            for _ in range(60):
                time.sleep(0.1)
                try:
                    urllib.request.urlopen("http://127.0.0.1:19894/", timeout=1)
                    break
                except Exception:
                    continue
            # Trigger the raising handler a few times.
            for _ in range(5):
                try:
                    urllib.request.urlopen("http://127.0.0.1:19894/boom", timeout=2).read()
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
