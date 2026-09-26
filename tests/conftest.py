"""Shared pytest fixtures for Pyronova integration tests.

Provides a parameterised `feature_server` fixture that spins up Pyronova in
either GIL or sub-interpreter mode on an ephemeral port, yields a base
URL, and tears down cleanly. Individual tests express their routes via
the SERVER_SCRIPT string (a template) and reuse the fixture.

The old test_all_features.py used one giant `run_feature_tests()` that
bundled 13+ assertions under one server; splitting by topic means each
file pays its own startup cost but is independently runnable and
failure-isolated.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

import pytest

from tests._helpers import bound_port, read_file

HOST = "127.0.0.1"


@dataclass
class ServerHandle:
    base_url: str
    mode: str
    proc: subprocess.Popen

    def get(self, path: str, headers: dict | None = None) -> tuple[int, str, dict]:
        req = urllib.request.Request(self.base_url + path, headers=headers or {})
        try:
            resp = urllib.request.urlopen(req, timeout=5)
            return resp.status, resp.read().decode(), dict(resp.headers)
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode(), dict(e.headers)

    def post(
        self, path: str, body: bytes | str | None = None,
        headers: dict | None = None,
    ) -> tuple[int, str, dict]:
        data = body.encode() if isinstance(body, str) else body
        req = urllib.request.Request(
            self.base_url + path, data=data, headers=headers or {}, method="POST",
        )
        try:
            resp = urllib.request.urlopen(req, timeout=5)
            return resp.status, resp.read().decode(), dict(resp.headers)
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode(), dict(e.headers)


def _boot(script: str, mode: str) -> tuple[subprocess.Popen, int]:
    """Start a Pyronova server from a script string on a port the kernel picks. Returns
    the process handle and the bound port (from the server's startup line).
    `mode` controls whether app.run() uses subinterp or GIL mode — the
    script is expected to read $PYRONOVA_MODE and branch.
    """
    fd, path = tempfile.mkstemp(prefix="pyronova_test_", suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(script)
    env = dict(os.environ)
    env["PYRONOVA_MODE"] = mode
    env["PYRONOVA_PORT"] = "0"
    # Output goes to a file, not a pipe: nobody drains a pipe while tests run, so a chatty
    # server could block on a full one, and `_teardown` scans the whole log, shutdown
    # included (Layer 2, FR-12).
    log_path = path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, path],
            stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True, env=env,
        )
    proc.pyronova_log = log_path  # type: ignore[attr-defined]
    try:
        port = bound_port(read_file(log_path), proc)
    except RuntimeError as e:
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"Pyronova server ({mode} mode) failed to start: {e}") from None
    # Poll until responsive
    deadline = time.time() + 10
    last_err = None
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://{HOST}:{port}/__ping", timeout=0.5)
            return proc, port
        except Exception as e:  # noqa: BLE001 — we only care it starts
            last_err = e
            time.sleep(0.1)
    # Failed to start — harvest the output for the error message
    proc.kill()
    proc.wait(timeout=5)
    with open(log_path, errors="replace") as f:
        out = f.read()
    raise RuntimeError(
        f"Pyronova server ({mode} mode on port {port}) failed to start: "
        f"{last_err}\nServer output:\n{out[:2000]}"
    )


def _teardown(proc: subprocess.Popen) -> None:
    """Stop the server the way a user does (SIGINT → graceful shutdown), then fail if its
    log, shutdown included, contains a PyO3 fork panic."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=20)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=5)
        except Exception:
            pass
    log_path = getattr(proc, "pyronova_log", None)
    if log_path and os.path.exists(log_path):
        with open(log_path, errors="replace") as f:
            panics = fork_panic_lines(f.read())
        assert panics == [], "server log has PyO3 fork panics:\n" + "\n".join(panics)


def feature_server_factory(script: str):
    """Build a parametrised pytest fixture that runs `script` in both
    GIL and sub-interp mode. Scope is module to amortise startup cost
    across a whole file.

    Usage:
      from tests.conftest import feature_server_factory
      feature_server = feature_server_factory(SERVER_SCRIPT)
    """

    @pytest.fixture(scope="module", params=["gil", "subinterp"])
    def feature_server(request):  # type: ignore[misc]
        proc, port = _boot(script, request.param)
        try:
            yield ServerHandle(
                base_url=f"http://{HOST}:{port}",
                mode=request.param,
                proc=proc,
            )
        finally:
            _teardown(proc)

    return feature_server


# ---------------------------------------------------------------------------
# Layer 2 (FR-12): fork panic-text scan for E2E server logs
# ---------------------------------------------------------------------------

# The two refusals the PyO3 fork raises once more than one interpreter has executed the
# engine and a thread with no thread state touches Python. A grep of src/ can't see
# drops, so every Layer 2 E2E scans its server log for these instead.
FORK_PANIC_TEXTS = (
    "Python::attach was called on a thread that has no Python thread state",
    "was dropped on a thread that has no Python thread state",
)


def fork_panic_lines(log_text: str) -> list[str]:
    """Lines of `log_text` that contain a fork panic text."""
    return [
        line for line in log_text.splitlines()
        if any(t in line for t in FORK_PANIC_TEXTS)
    ]
