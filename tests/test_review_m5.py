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
        f.write(textwrap.dedent(script).replace("__PORT__", str(_unused_port())))
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
            for _ in range(200):
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:__PORT__/log", timeout=2).read()
                    break
                except Exception:
                    time.sleep(0.1)
            print("LEAKED" if "PYRONOVA_LOG_LEVEL" in os.environ else "NOT-LEAKED", flush=True)
            app._stop()

        if __name__ == "__main__":
            threading.Thread(target=probe, daemon=True).start()
            app.run(host="127.0.0.1", port=__PORT__, workers=1)
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


def test_two_apps_in_one_process_keep_their_own_max_body_size():
    with TestClient(_default, mode="gil") as big, TestClient(_small, mode="gil") as small:
        assert big.post("/len", body=b"x" * 5000).json() == {"len": 5000}
        assert small.post("/len", body=b"x" * 5000).status_code == 413
        assert small.post("/len", body=b"x" * 500).json() == {"len": 500}
    assert _small.max_body_size == 1000
    assert _default.max_body_size == 10 * 1024 * 1024


def test_two_apps_in_one_process_keep_their_own_websocket_cap():
    assert _default.max_websocket_connections == 1024
    with TestClient(_default, mode="gil") as c:
        with c.websocket_connect("/echo") as a, c.websocket_connect("/echo") as b:
            a.send("1")
            b.send("2")
            assert (a.recv(timeout=5), b.recv(timeout=5)) == ("1", "2")
