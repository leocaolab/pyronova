"""Review cced8c2, milestone M5: config parsed once at the edge + TPC runtime cleanup.

Every test here failed before the change it covers (see
docs/design/code-review-cced8c2-roadmap.md, M5). Servers run in their own processes:
the engine's config, the tracing subscriber and the signal handlers are process-wide.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import textwrap

import pytest

HOST = "127.0.0.1"
# A server that should have refused to start is killed after this long.
REFUSE_TIMEOUT_S = 20


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def _run(script: str, env: dict[str, str] | None = None, timeout: float = REFUSE_TIMEOUT_S):
    """Runs `script` in a fresh interpreter; a server that keeps serving is killed and
    reported as such."""
    full_env = {**os.environ, **(env or {})}
    # A file, not `-c`: sub-interpreter workers rebuild the app by executing it.
    path = os.path.join(tempfile.mkdtemp(prefix="pyronova-m5-"), "app.py")
    with open(path, "w") as f:
        f.write(textwrap.dedent(script))
    try:
        return subprocess.run(
            [sys.executable, path],
            env=full_env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b"").decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = (e.stderr or b"").decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        pytest.fail(f"the server started and kept serving instead of refusing:\n{out}\n{err}")


# ---------------------------------------------------------------------------
# 1. One typed config: a bad value is a startup error
# ---------------------------------------------------------------------------

_RAW_ENGINE = """
    from pyronova.engine import PyronovaApp

    def root(req):
        return "ok"

    app = PyronovaApp()
    app.get("/", root)
    if __name__ == "__main__":
        app.run(host="127.0.0.1", port=0, mode={mode!r})
"""


def test_a_mode_typo_is_rejected_by_the_engine():
    r = _run(_RAW_ENGINE.format(mode="subinterpter"))
    assert r.returncode != 0
    assert "ValueError" in r.stderr and "'subinterpter'" in r.stderr.replace('"', "'"), r.stderr


def test_a_mode_typo_is_rejected_by_pyronova_run():
    r = _run(
        """
        from pyronova import Pyronova
        app = Pyronova()

        @app.get("/")
        def root(req):
            return "ok"

        app.run(host="127.0.0.1", port=0, mode="asynk")
        """
    )
    assert r.returncode != 0
    assert "ValueError" in r.stderr and "asynk" in r.stderr, r.stderr


def test_mode_is_an_enum_on_both_sides():
    from pyronova.app import _ServeSettings
    from pyronova.engine import Mode

    assert Mode.parse("gil") == Mode.Gil
    assert Mode.parse("default") == Mode.Gil
    assert Mode.parse("subinterp") == Mode.Subinterp
    assert Mode.parse("auto") == Mode.Subinterp
    assert Mode.Subinterp.uses_workers and not Mode.Gil.uses_workers
    with pytest.raises(ValueError, match="hybrid"):
        Mode.parse("hybrid")
    assert _ServeSettings.resolve(port=1).mode == Mode.Subinterp
    assert _ServeSettings.resolve(port=1, mode="gil").mode == Mode.Gil
    with pytest.raises(ValueError, match="subinterpreter"):
        _ServeSettings.resolve(port=1, mode="subinterpreter")


_BAD_ENV = [
    ("PYRONOVA_TPC", "maybe"),
    ("PYRONOVA_TPC_DARWIN", "fan-out"),
    ("PYRONOVA_GC_MODE", "idel"),
    ("PYRONOVA_GC_THRESHOLD", "5k"),
    ("PYRONOVA_GC_OOM_FAILSAFE", "lots"),
    ("PYRONOVA_GC_IDLE_MS", "0"),
    ("PYRONOVA_GIL_BRIDGE_WORKERS", "four"),
    ("PYRONOVA_GIL_BRIDGE_CAPACITY", "0"),
    ("PYRONOVA_METRICS", "yes"),
]


@pytest.mark.parametrize("var,raw", _BAD_ENV, ids=[f"{v}={r}" for v, r in _BAD_ENV])
def test_a_bad_engine_env_value_is_a_startup_error(var, raw):
    r = _run(_RAW_ENGINE.format(mode="gil"), env={var: raw})
    assert r.returncode != 0
    assert var in r.stderr and raw in r.stderr, r.stderr


@pytest.mark.parametrize("raw", ["443x", "70000", "0"])
def test_a_bad_tls_port_list_is_a_startup_error(raw):
    r = _run(
        """
        from pyronova import Pyronova
        app = Pyronova()

        @app.get("/")
        def root(req):
            return "ok"

        app.run(host="127.0.0.1", port=0, mode="gil")
        """,
        env={"PYRONOVA_TLS_PORTS": raw},
    )
    assert r.returncode != 0
    assert "PYRONOVA_TLS_PORTS" in r.stderr and raw in r.stderr, r.stderr


def test_log_sampling_rejects_the_old_sentinels():
    from pyronova.engine import PyronovaApp

    app = PyronovaApp()
    with pytest.raises(ValueError, match="sample_n"):
        app.set_request_log_sampling(0)
    with pytest.raises(ValueError, match="always_status"):
        app.set_request_log_sampling(1, 0)
    app.set_request_log_sampling(100, None)
    app.set_request_log_sampling(100, 400)


def test_the_worker_log_level_comes_from_the_engine_not_the_environment():
    r = _run(
        """
        import logging, os, threading, urllib.request
        from pyronova import Pyronova

        app = Pyronova(log_config={"level": "WARN"})

        @app.get("/log")
        def log(req):
            logging.getLogger("m5").info("m5-info-line")
            logging.getLogger("m5").warning("m5-warning-line")
            return "ok"

        def probe():
            import time
            while not app._servers:  # bound on port 0: read the port it got
                time.sleep(0.05)
            port = next(iter(app._servers)).port
            for _ in range(200):
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:{port}/log", timeout=2).read()
                    break
                except Exception:
                    time.sleep(0.1)
            print("LEAKED" if "PYRONOVA_LOG_LEVEL" in os.environ else "NOT-LEAKED", flush=True)
            app._stop()

        if __name__ == "__main__":
            threading.Thread(target=probe, daemon=True).start()
            app.run(host="127.0.0.1", port=0, workers=1)
        """,
        timeout=60,
    )
    out = r.stdout + r.stderr
    assert "m5-warning-line" in out, out
    assert "m5-info-line" not in out, out
    assert "NOT-LEAKED" in out, out


# ---------------------------------------------------------------------------
# 2. Limits are per app, not process globals
# ---------------------------------------------------------------------------

# Module level: served with mode="gil", so nothing re-executes this file.
from pyronova import Pyronova  # noqa: E402
from pyronova.testing import TestClient  # noqa: E402

_small = Pyronova()
_small.max_body_size = 1000
_small.max_websocket_connections = 1
_default = Pyronova()


@_small.post("/len")
def _small_len(req):
    return {"len": len(req.body)}


@_default.post("/len")
def _default_len(req):
    return {"len": len(req.body)}


@_default.websocket("/echo")
def _default_echo(ws):
    while (msg := ws.recv()) is not None:
        ws.send(msg)


@_small.websocket("/echo")
def _small_echo(ws):
    while (msg := ws.recv()) is not None:
        ws.send(msg)


def test_two_apps_in_one_process_keep_their_own_max_body_size():
    with TestClient(_default, mode="gil") as big, TestClient(_small, mode="gil") as small:
        assert big.post("/len", body=b"x" * 5000).json() == {"len": 5000}
        assert small.post("/len", body=b"x" * 5000).status_code == 413
        assert small.post("/len", body=b"x" * 500).json() == {"len": 500}
    assert _small.max_body_size == 1000
    assert _default.max_body_size == 10 * 1024 * 1024


def test_two_apps_in_one_process_keep_their_own_websocket_cap():
    import websockets

    assert _default.max_websocket_connections == 1024
    with TestClient(_default, mode="gil") as big, TestClient(_small, mode="gil") as small:
        with big.websocket_connect("/echo") as a, big.websocket_connect("/echo") as b:
            a.send("1")
            b.send("2")
            assert (a.recv(timeout=5), b.recv(timeout=5)) == ("1", "2")
        # The small app's cap of 1 holds while the default app takes two.
        with small.websocket_connect("/echo") as only:
            only.send("3")
            assert only.recv(timeout=5) == "3"
            with pytest.raises(websockets.InvalidStatus) as refused:
                small.websocket_connect("/echo")
            assert refused.value.response.status_code == 503


# ---------------------------------------------------------------------------
# 3. One listener set, bound before anything starts, on every run path
# ---------------------------------------------------------------------------

# (label, mode, extra env)
_RUN_PATHS = [
    ("tpc-gil", "gil", {}),
    ("tpc-subinterp", "subinterp", {}),
    ("pool-gil", "gil", {"PYRONOVA_TPC": "0"}),
    ("pool-subinterp", "subinterp", {"PYRONOVA_TPC": "0"}),
]
if sys.platform == "darwin":
    _RUN_PATHS.append(("darwin-fanout", "subinterp", {"PYRONOVA_TPC_DARWIN": "fanout"}))

_TLS_APP = """
    import sys
    from pyronova import Pyronova

    app = Pyronova()

    @app.get("/")
    def root(req):
        return "hello"

    if __name__ == "__main__":
        plain, tls, mode, cert, key = sys.argv[1:6]
        app.run(host="127.0.0.1", port=int(plain), mode=mode, workers=2,
                tls_cert=cert, tls_key=key, extra_tls_ports=[int(tls)])
"""


@pytest.fixture(scope="module")
def cert_key(tmp_path_factory):
    import shutil

    if shutil.which("openssl") is None:
        pytest.skip("openssl CLI not available")
    d = tmp_path_factory.mktemp("m5_tls")
    cert, key = d / "cert.pem", d / "key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key),
         "-out", str(cert), "-days", "1", "-subj", "/CN=localhost"],
        check=True, capture_output=True,
    )
    return str(cert), str(key)


def _get(url: str, ctx=None, attempts: int = 1) -> bytes:
    import time
    import urllib.request

    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=3, context=ctx) as r:
                return r.read()
        except OSError:
            if attempt == attempts - 1:
                raise
            time.sleep(0.2)
    raise AssertionError("unreachable")


@pytest.mark.parametrize("label,mode,env", _RUN_PATHS, ids=[p[0] for p in _RUN_PATHS])
def test_an_extra_tls_port_is_served_on_every_run_path(cert_key, tmp_path, label, mode, env):
    import ssl

    from tests._helpers import bound_port, read_file

    script = tmp_path / "tls_app.py"
    script.write_text(textwrap.dedent(_TLS_APP))
    # The plain port is 0, read back from the startup line; `extra_tls_ports` refuses 0
    # (`_check_port`), so the TLS port is one the kernel just handed out.
    tls = _unused_port()
    log_path = tmp_path / "server.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script), "0", str(tls), mode, *cert_key],
            env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT, text=True,
        )
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        plain = bound_port(read_file(str(log_path)), proc)
        assert _get(f"http://127.0.0.1:{plain}/", attempts=150) == b"hello"
        assert _get(f"https://127.0.0.1:{tls}/", ctx=ctx, attempts=5) == b"hello"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    out = log_path.read_text(errors="replace")
    assert f"https://127.0.0.1:{tls}" in out, out


@pytest.mark.parametrize("label,mode,env", _RUN_PATHS, ids=[p[0] for p in _RUN_PATHS])
def test_a_port_in_use_is_an_oserror_from_run(label, mode, env):
    # A listener without SO_REUSEPORT: the server's SO_REUSEPORT bind can't join it.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as taken:
        taken.bind((HOST, 0))
        taken.listen()
        port = taken.getsockname()[1]
        r = _run(
            f"""
            import errno
            from pyronova import Pyronova

            app = Pyronova()

            @app.get("/")
            def root(req):
                return "hello"

            if __name__ == "__main__":
                try:
                    app.run(host="127.0.0.1", port={port}, mode={mode!r}, workers=2)
                except OSError as e:
                    print("OSERROR", e.errno == errno.EADDRINUSE, e, flush=True)
                    raise SystemExit(3)
                print("RETURNED-OK", flush=True)
            """,
            env=env,
        )
    assert r.returncode == 3, r.stdout + r.stderr
    assert "OSERROR True" in r.stdout, r.stdout + r.stderr
    assert f"127.0.0.1:{port}" in r.stdout, r.stdout


_bound = Pyronova()


@_bound.get("/")
def _bound_root(req):
    return "ok"


def test_testclient_reports_the_port_the_engine_bound():
    with TestClient(_bound, mode="gil") as c:
        assert c._settings.port == 0
        assert c.port == c._server.port
        assert c.port != 0
        assert c.get("/").text == "ok"
        port = c.port
    # Closed: nothing listens on the port any more.
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=2).close()


# ---------------------------------------------------------------------------
# 4. Small items
# ---------------------------------------------------------------------------


def test_a_duplicate_websocket_path_is_an_error():
    from pyronova.engine import PyronovaApp

    def first(ws):
        pass

    def second(ws):
        pass

    app = PyronovaApp()
    app.websocket("/ws", first)
    with pytest.raises(ValueError, match="/ws"):
        app.websocket("/ws", second)


def test_a_failing_async_check_fails_the_registration():
    from pyronova.engine import PyronovaApp

    class Handler:
        __name__ = "handler"

        @property
        def __call__(self):
            raise RuntimeError("m5: __call__ lookup failed")

    app = PyronovaApp()
    with pytest.raises(RuntimeError, match="m5: __call__ lookup failed"):
        app.get("/", Handler())


def test_a_programmatic_stop_on_the_main_thread_restores_sigint():
    r = _run(
        """
        import signal, threading, time, urllib.request
        from pyronova import Pyronova

        app = Pyronova()

        @app.get("/")
        def root(req):
            return "ok"

        def stop_when_up():
            while not app._servers:  # bound on port 0: read the port it got
                time.sleep(0.05)
            port = next(iter(app._servers)).port
            for _ in range(200):
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1).read()
                    break
                except Exception:
                    time.sleep(0.05)
            app._stop()

        if __name__ == "__main__":
            before = signal.getsignal(signal.SIGINT)
            threading.Thread(target=stop_when_up, daemon=True).start()
            app.run(host="127.0.0.1", port=0, mode="gil")
            print("RESTORED", signal.getsignal(signal.SIGINT) is before, flush=True)
        """,
        timeout=60,
    )
    assert "RESTORED True" in r.stdout, r.stdout + r.stderr
