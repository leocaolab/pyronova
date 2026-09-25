"""Review cced8c2, milestone M1d: WebSocket limits and message typing, static-file
errors and percent-decoding, logging level mapping and config errors, named metrics.

Every test here failed on cced8c2 (see docs/design/code-review-cced8c2-roadmap.md, M1d).
"""

import asyncio
import http.client
import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.request

import pytest

PYTHON = sys.executable

WS_PORT = 19971
WS_CAP_PORT = 19972
STATIC_PORT = 19973

MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _run_script(body: str, timeout: float = 20) -> subprocess.CompletedProcess:
    """Run a Python snippet in a fresh process (tracing's global subscriber and the
    metrics statics are process-wide, so each case needs its own process)."""
    return subprocess.run(
        [PYTHON, "-c", textwrap.dedent(body)],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


class _Server:
    def __init__(self, script: str, port: int):
        fd, self.path = tempfile.mkstemp(prefix="pyronova_m1d_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.port = port
        self.proc = subprocess.Popen(
            [PYTHON, self.path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        for _ in range(100):
            time.sleep(0.1)
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1)
                return
            except Exception:
                if self.proc.poll() is not None:
                    break
        raise RuntimeError(f"server did not start:\n{self.stop()}")

    def stop(self) -> str:
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        out, _ = self.proc.communicate(timeout=10)
        os.unlink(self.path)
        return out.decode(errors="replace")


# ---------------------------------------------------------------------------
# M1d-1 / M1d-3: WebSocket limits and typed messages
# ---------------------------------------------------------------------------

WS_SERVER = f"""
from pyronova import Pyronova

app = Pyronova()

@app.websocket("/echo")
def echo(ws):
    while True:
        msg = ws.recv()
        if msg is None:
            break
        ws.send(f"echo: {{len(msg)}}")

@app.websocket("/strict")
def strict(ws):
    while True:
        try:
            msg = ws.recv()
        except TypeError:
            data = ws.recv_bytes()
            ws.send(f"binary:{{len(data)}}")
            continue
        if msg is None:
            break
        ws.send(f"text:{{msg}}")

@app.websocket("/kinds")
def kinds(ws):
    while True:
        msg = ws.recv_message()
        if msg is None:
            break
        ws.send(type(msg).__name__)

@app.websocket("/big-send")
def big_send(ws):
    ws.recv()
    try:
        ws.send("x" * (2 * {MIB}))
        ws.send("sent")
    except ValueError as e:
        ws.send(f"ValueError: {{e}}")

@app.get("/health")
def health(req):
    return {{"ok": True}}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={WS_PORT}, mode="default")
"""


@pytest.fixture(scope="module")
def ws_server():
    s = _Server(WS_SERVER, WS_PORT)
    yield s
    s.stop()


@pytest.mark.asyncio
async def test_ws_default_max_message_size_closes_with_1009(ws_server):
    """cced8c2 used tungstenite's 64 MiB default: a 2 MiB message was accepted."""
    import websockets

    # No `async with`: the server closes this connection, and re-closing it from the
    # client's __aexit__ while its send is being torn down trips a websockets/asyncio
    # teardown race (`abort` on a transport whose loop is already gone).
    ws = await websockets.connect(f"ws://127.0.0.1:{WS_PORT}/echo", max_size=None)
    # The server may close while the client is still writing, so send can raise too.
    with pytest.raises(websockets.ConnectionClosed) as info:
        await ws.send("y" * (2 * MIB))
        await asyncio.wait_for(ws.recv(), timeout=5)
    await asyncio.wait_for(ws.wait_closed(), timeout=5)
    assert info.value.rcvd is not None and info.value.rcvd.code == 1009, info.value


@pytest.mark.asyncio
async def test_ws_small_message_still_echoes(ws_server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{WS_PORT}/echo") as ws:
        await ws.send("z" * 1000)
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "echo: 1000"


@pytest.mark.asyncio
async def test_ws_send_over_max_message_size_raises_value_error(ws_server):
    """cced8c2 queued any size; the outgoing cap counted messages, not bytes."""
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{WS_PORT}/big-send", max_size=None) as ws:
        await ws.send("go")
        reply = await asyncio.wait_for(ws.recv(), timeout=5)
    assert reply.startswith("ValueError: "), reply[:200]
    assert str(2 * MIB) in reply


@pytest.mark.asyncio
async def test_ws_recv_does_not_drop_binary(ws_server):
    """cced8c2: recv() silently skipped a binary message."""
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{WS_PORT}/strict") as ws:
        await ws.send(b"\x01\x02\x03")
        await ws.send("hi")
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "binary:3"
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "text:hi"


@pytest.mark.asyncio
async def test_ws_recv_message_returns_str_or_bytes(ws_server):
    """cced8c2: recv_message returned a ("text"|"binary", data) tuple."""
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{WS_PORT}/kinds") as ws:
        await ws.send(b"\x00")
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "bytes"
        await ws.send("t")
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "str"


WS_CAP_SERVER = f"""
from pyronova import Pyronova

app = Pyronova()
app.max_websocket_connections = 1

@app.websocket("/hold")
def hold(ws):
    while ws.recv() is not None:
        ws.send("pong")

@app.get("/health")
def health(req):
    return {{"ok": True}}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={WS_CAP_PORT}, mode="default")
"""


@pytest.mark.asyncio
async def test_ws_connection_cap_answers_503_then_frees_the_slot():
    """cced8c2 spawned an OS thread per connection with no cap."""
    import websockets

    server = _Server(WS_CAP_SERVER, WS_CAP_PORT)
    try:
        url = f"ws://127.0.0.1:{WS_CAP_PORT}/hold"
        async with websockets.connect(url) as first:
            await first.send("ping")
            assert await asyncio.wait_for(first.recv(), timeout=5) == "pong"
            with pytest.raises(websockets.InvalidStatus) as info:
                async with websockets.connect(url):
                    pass
            assert info.value.response.status_code == 503

        # The slot is released once the first connection's handler has returned.
        for _ in range(50):
            try:
                async with websockets.connect(url) as again:
                    await again.send("ping")
                    assert await asyncio.wait_for(again.recv(), timeout=5) == "pong"
                break
            except websockets.InvalidStatus:
                await asyncio.sleep(0.1)
        else:
            pytest.fail("connection slot was never released")
    finally:
        server.stop()


def test_ws_limit_setters_reject_bad_values():
    from pyronova import Pyronova

    app = Pyronova()
    with pytest.raises(ValueError):
        app.max_websocket_connections = 0
    with pytest.raises(ValueError):
        app.max_websocket_message_size = 0
    with pytest.raises(TypeError):
        app.max_websocket_message_size = "1MB"


# ---------------------------------------------------------------------------
# M1d-4: static files
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def static_tree():
    base = tempfile.mkdtemp(prefix="pyronova_m1d_static_")
    root = os.path.join(base, "public")
    os.makedirs(root)
    with open(os.path.join(base, "secret.txt"), "w") as f:
        f.write("TOP-SECRET")
    with open(os.path.join(root, "my file.txt"), "w") as f:
        f.write("spaced")
    locked = os.path.join(root, "locked.txt")
    with open(locked, "w") as f:
        f.write("no read permission")
    os.chmod(locked, 0)
    os.symlink("loop", os.path.join(root, "loop"))
    yield base, root
    os.chmod(locked, 0o600)


@pytest.fixture(scope="module")
def static_server(static_tree):
    from pyronova import Pyronova
    from pyronova.testing import TestClient

    _, root = static_tree
    app = Pyronova()

    @app.get("/health")
    def health(req):
        return {"ok": True}

    app.static("/static", root)
    c = TestClient(app, port=STATIC_PORT)
    yield c
    c.close()


def _raw_get(path: str) -> tuple[int, bytes]:
    """Send the path byte-for-byte: http clients may normalise `%2e%2e`."""
    conn = http.client.HTTPConnection("127.0.0.1", STATIC_PORT, timeout=5)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def test_static_percent_decodes_the_path(static_server):
    """cced8c2 looked up `my%20file.txt` literally → 404."""
    status, body = _raw_get("/static/my%20file.txt")
    assert (status, body) == (200, b"spaced")


@pytest.mark.parametrize(
    "path",
    [
        "/static/%2e%2e/secret.txt",
        "/static/%2E%2E/secret.txt",
        "/static/..%2fsecret.txt",
        "/static/..%2Fsecret.txt",
        "/static/%2e%2e%2fsecret.txt",
    ],
)
def test_static_encoded_traversal_is_forbidden(static_server, path):
    """cced8c2 never decoded the path, so an encoded `..` was not seen as traversal."""
    status, body = _raw_get(path)
    assert status == 403, (status, body)
    assert b"TOP-SECRET" not in body


def test_static_encoded_absolute_path_cannot_replace_the_root(static_server, static_tree):
    base, _ = static_tree
    status, body = _raw_get("/static/%2f" + base.lstrip("/").replace("/", "%2f") + "%2fsecret.txt")
    assert status == 404, (status, body)
    assert b"TOP-SECRET" not in body


def test_static_nul_byte_is_bad_request(static_server):
    status, _ = _raw_get("/static/a%00b.txt")
    assert status == 400


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
def test_static_permission_denied_is_403_not_404(static_server):
    """cced8c2 turned every IO error into a silent 404."""
    status, _ = _raw_get("/static/locked.txt")
    assert status == 403


def test_static_io_error_is_500_not_404(static_server):
    """A symlink loop in the root is a server misconfiguration: 500, not 'missing'."""
    status, _ = _raw_get("/static/loop")
    assert status == 500


def test_static_missing_file_is_still_404(static_server):
    status, _ = _raw_get("/static/nope.txt")
    assert status == 404


def test_static_root_must_be_a_directory(static_tree):
    """cced8c2 accepted a regular file as the root and 404'd every request."""
    from pyronova import Pyronova

    base, _ = static_tree
    with pytest.raises(ValueError, match="not a directory"):
        Pyronova().static("/s", os.path.join(base, "secret.txt"))


def test_static_prefix_must_start_with_slash(static_tree):
    from pyronova import Pyronova

    _, root = static_tree
    with pytest.raises(ValueError, match="must start with '/'"):
        Pyronova().static("assets", root)


# ---------------------------------------------------------------------------
# M1d-5: logging
# ---------------------------------------------------------------------------

_BRIDGE = """
import logging, time
from pyronova.engine import init_logger
from pyronova.app import _setup_python_logging_bridge
"""


def test_logging_custom_level_between_info_and_warning_is_kept():
    """cced8c2 matched `levelname`; a custom level fell to TRACE and was dropped."""
    r = _run_script(_BRIDGE + """
init_logger("INFO", False, "text")
_setup_python_logging_bridge("INFO")
logging.addLevelName(25, "NOTICE")
logging.getLogger("m1d").log(25, "custom-level-25")
logging.getLogger("m1d").log(5, "below-debug-5")
time.sleep(0.5)
""")
    assert r.returncode == 0, r.stderr
    assert "custom-level-25" in r.stderr
    assert "INFO" in r.stderr.split("custom-level-25")[0].splitlines()[-1]
    assert "below-debug-5" not in r.stderr


def test_logging_invalid_level_raises():
    """cced8c2 parsed `verbose` as a target directive and returned OK."""
    r = _run_script("""
from pyronova.engine import init_logger
try:
    init_logger("verbose", False, "text")
except ValueError as e:
    print("ValueError:", e)
""")
    assert "ValueError:" in r.stdout and "verbose" in r.stdout, (r.stdout, r.stderr)


def test_logging_invalid_format_raises():
    """cced8c2 treated any unknown format as text."""
    r = _run_script("""
from pyronova.engine import init_logger
try:
    init_logger("INFO", False, "yaml")
except ValueError as e:
    print("ValueError:", e)
""")
    assert "ValueError:" in r.stdout and "yaml" in r.stdout, (r.stdout, r.stderr)


def test_logging_second_init_reconfigures_instead_of_silently_keeping_the_first():
    """cced8c2 printed 'already set' and kept filtering at the first level."""
    r = _run_script(_BRIDGE + """
init_logger("ERROR", False, "text")
init_logger("INFO", False, "json")
_setup_python_logging_bridge("INFO")
logging.getLogger("m1d").info("after-reinit")
time.sleep(0.5)
""")
    assert r.returncode == 0, r.stderr
    line = next(l for l in r.stderr.splitlines() if "after-reinit" in l)
    assert line.startswith("{"), line  # the json format took effect too


# ---------------------------------------------------------------------------
# M1d-6: metrics
# ---------------------------------------------------------------------------


def test_metrics_is_named_and_reading_does_not_reset_peaks():
    """cced8c2 returned an anonymous 9-tuple and zeroed the peaks on every read."""
    r = _run_script("""
import os
os.environ["PYRONOVA_METRICS"] = "1"
from pyronova import Pyronova, get_gil_metrics, reset_peaks
from pyronova.testing import TestClient

app = Pyronova()

@app.get("/heavy", gil=True)
def heavy(req):
    return {"t": sum(range(300_000))}

c = TestClient(app, port=19974)
assert c.get("/heavy").status_code == 200
first = get_gil_metrics()
second = get_gil_metrics()
assert first.gil_hold_peak_us > 0, first
assert second.gil_hold_peak_us == first.gil_hold_peak_us, (first, second)
assert second.total_requests >= 1, second
reset_peaks()
third = get_gil_metrics()
assert third.gil_hold_peak_us == 0 and third.gil_wait_peak_us == 0, third
c.close()
print("OK")
""", timeout=60)
    assert "OK" in r.stdout, (r.stdout, r.stderr)


def test_metrics_rss_is_none_when_never_sampled():
    """cced8c2 reported 0 bytes of RSS when the sampler had not run."""
    r = _run_script("""
from pyronova import get_gil_metrics
print(repr(get_gil_metrics().rss_bytes))
""")
    assert r.stdout.strip() == "None", (r.stdout, r.stderr)
