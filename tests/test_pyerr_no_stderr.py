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

import os
import signal
import subprocess
import sys
import textwrap

import httpx

from tests._helpers import bound_port, poll_until, read_file


def test_raising_handler_does_not_spam_stderr(tmp_path):
    """Run a child Pyronova process that serves a route which raises, hit
    it a few times, and verify stderr stays quiet. The child runs with
    `mode="subinterp"` so the PyErr_Print path is the one that would
    have been hit before the fix.

    The server runs from a script file, not `python -c`: workers re-run the
    app's `__file__`, so `app.run()` needs one to serve at all.

    Tolerance: we don't require *zero* stderr bytes — Pyronova's startup
    prints a banner and the tracing subscriber may emit one-line
    warnings. We require that there's no raw Python traceback in
    stderr (those are the 10-20+ lines of noise the bug produced).
    """
    script = tmp_path / "raising_app.py"
    script.write_text(textwrap.dedent("""
        from pyronova import Pyronova

        app = Pyronova()

        @app.get("/")
        def ok(req):
            return "ok"

        @app.get("/boom")
        def boom(req):
            raise RuntimeError("deliberate test failure")

        if __name__ == "__main__":
            app.run(host="127.0.0.1", port=0, mode="subinterp")
    """))
    stdout_path = tmp_path / "stdout.log"
    stderr_path = tmp_path / "stderr.log"
    with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=out, stderr=err, cwd=str(tmp_path), start_new_session=True,
        )
    read_stdout, read_stderr = read_file(str(stdout_path)), read_file(str(stderr_path))
    try:
        port = bound_port(lambda: read_stdout() + read_stderr(), proc)
        base = f"http://127.0.0.1:{port}"

        def up():
            try:
                return httpx.get(base + "/", timeout=2).status_code == 200
            except httpx.HTTPError:
                return False

        poll_until(up, timeout=30, what="the server answering GET /")
        # Trigger the raising handler a few times.
        for _ in range(5):
            assert httpx.get(base + "/boom", timeout=5).status_code == 500
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGINT)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)

    combined = read_stderr() + read_stdout()
    # The signature of PyErr_Print is multi-line Python traceback:
    #   Traceback (most recent call last):
    #     File "...", line ...
    #   RuntimeError: deliberate test failure
    # If any of those appear raw (not JSON-encoded inside a tracing
    # record), the old path has leaked back in.
    traceback_lines = sum(
        line.lstrip().startswith("Traceback (most recent call last):")
        for line in combined.splitlines()
    )
    # Allow 0–1 occurrences (CPython occasionally logs on interp
    # shutdown no matter what we do); > 1 is the regression.
    assert traceback_lines <= 1, (
        f"stderr contained {traceback_lines} raw Python tracebacks — "
        "PyErr_Print has leaked back onto the hot path. Output:\n"
        f"{combined[-2000:]}"
    )
