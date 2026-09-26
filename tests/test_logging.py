"""Test: Pyronova logging system — Rust tracing engine + Python bridge.

Verifies:
1. Framework enable_logging() produces structured output (GIL mode)
2. Rust-level sub-interp request logging works via tracing
3. User print() visible in sub-interpreter
4. User logging.info() routed through Rust tracing in sub-interpreter
5. debug=True enables access log + tracing output
6. JSON format output when configured
7. Python logging bridge works in main interpreter
"""

import subprocess
import sys
import os
import signal
import time

from tests._helpers import bound_port, read_file

PYTHON = sys.executable


def run_server_and_check(script: str, label: str, expected_strings: list[str]):
    """Start a server, send requests, check output for expected strings."""
    script_path = f"/tmp/pyronova_log_test_{label}.py"
    with open(script_path, "w") as f:
        f.write(script)

    # One file for stdout + stderr: tracing writes to stderr, println (and the
    # "Listening on" line with the bound port) to stdout.
    log_path = script_path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [PYTHON, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
    time.sleep(3)

    try:
        import urllib.request
        port = bound_port(read_file(log_path), proc)
        for _ in range(2):
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2)
    except Exception:
        pass

    time.sleep(1)
    alive = proc.poll() is None
    if alive:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    proc.wait(timeout=5)
    output = read_file(log_path)()

    passed = 0
    for s in expected_strings:
        if s in output:
            print(f"  ✅ {label}: found '{s}'")
            passed += 1
        else:
            print(f"  ❌ {label}: missing '{s}'")
            print(f"     output: {output[:2000]}")

    return passed == len(expected_strings)


def test_gil_mode_logging():
    """Framework logging in GIL mode — the Rust access log (one line per request)."""
    script = '''
from pyronova import Pyronova
app = Pyronova()
app.enable_logging()

@app.get("/", gil=True)
def index(req): return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "gil_logging", [
        "pyronova::access",
        "Request handled",
        '"method":"GET"',
        '"path":"/"',
        '"status":200',
    ])


def test_subinterp_rust_logging():
    """Rust-level request logging in sub-interpreter mode via tracing."""
    script = '''
from pyronova import Pyronova
app = Pyronova()
app.enable_logging()

@app.get("/")
def index(req): return "ok"

app.run(host="127.0.0.1", port=0, mode="subinterp")
'''
    assert run_server_and_check(script, "subinterp_logging", [
        "pyronova::access",
        "Request handled",
    ])


def test_user_print_in_subinterp():
    """User print() works in sub-interpreter handlers."""
    script = '''
from pyronova import Pyronova
app = Pyronova()

@app.get("/")
def index(req):
    print("USER_PRINT_MARKER", flush=True)
    return "ok"

app.run(host="127.0.0.1", port=0, mode="subinterp")
'''
    assert run_server_and_check(script, "user_print", [
        "USER_PRINT_MARKER",
    ])


def test_user_logging_in_subinterp():
    """Python logging module routed through Rust tracing in sub-interpreter."""
    script = '''
import logging
logger = logging.getLogger("test")

from pyronova import Pyronova
app = Pyronova(debug=True)

@app.get("/")
def index(req):
    logger.info("LOGGER_MARKER_12345")
    return "ok"

app.run(host="127.0.0.1", port=0, mode="subinterp")
'''
    assert run_server_and_check(script, "user_logging", [
        "LOGGER_MARKER_12345",
    ])


def test_debug_mode_tracing():
    """debug=True enables tracing output with access log."""
    script = '''
from pyronova import Pyronova
app = Pyronova(debug=True)

@app.get("/")
def index(req): return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "debug_mode", [
        "pyronova::server",
        "Pyronova tracing engine initialized",
        "Pyronova started",
    ])


def test_debug_mode_access_log():
    """debug=True produces access log with latency."""
    script = '''
from pyronova import Pyronova
app = Pyronova(debug=True)

@app.get("/")
def index(req): return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "debug_access_log", [
        "pyronova::access",
        "Request handled",
        "latency_us",
    ])


def test_python_logging_bridge_main():
    """Python logging in main interpreter routes through Rust tracing."""
    script = '''
import logging
from pyronova import Pyronova

app = Pyronova(debug=True)
logger = logging.getLogger("myapp")

@app.get("/")
def index(req):
    logger.info("BRIDGE_TEST_MARKER")
    return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "python_bridge_main", [
        "BRIDGE_TEST_MARKER",
        "pyronova::app",
    ])


def test_json_format():
    """JSON format output when configured."""
    script = '''
from pyronova import Pyronova
app = Pyronova(debug=True, log_config={"format": "json"})

@app.get("/")
def index(req): return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "json_format", [
        '"target":"pyronova::server"',
        '"message":"Pyronova tracing engine initialized"',
    ])


if __name__ == "__main__":
    print("=== Logging Tests ===")
    print()
    test_gil_mode_logging()
    print()
    test_subinterp_rust_logging()
    print()
    test_user_print_in_subinterp()
    print()
    test_user_logging_in_subinterp()
    print()
    test_debug_mode_tracing()
    print()
    test_debug_mode_access_log()
    print()
    test_python_logging_bridge_main()
    print()
    test_json_format()
    print()
    print("=== All logging tests passed ===")
