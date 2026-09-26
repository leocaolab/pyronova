"""Sub-interpreter DB bridge smoke test.

Validates that a handler registered WITHOUT `gil=True` can execute
queries from a sub-interpreter worker through the real `PgPool`
(`src/db.rs`, sync methods via `run_on_db_rt`). This is the regression
test for the whole reason async-db / crud used to be pinned gil=True.
"""

from __future__ import annotations

import json
import os
import signal
import tempfile
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest

from tests._helpers import bound_port, read_file

PG_DSN = os.environ.get("PYRONOVA_TEST_PG_DSN")
if not PG_DSN:
    pytest.skip("PYRONOVA_TEST_PG_DSN not set", allow_module_level=True)


SERVER = '''
import json as _json
import os
from pyronova import Pyronova, Response
from pyronova.db import PgPool

DSN = os.environ["PYRONOVA_TEST_PG_DSN"]
pool = PgPool.connect(DSN, max_connections=4)

# Seed deterministic test data on startup.
pool.execute("DROP TABLE IF EXISTS bridge_test")
pool.execute("CREATE TABLE bridge_test (id INT PRIMARY KEY, label TEXT)")
for i in range(5):
    pool.execute("INSERT INTO bridge_test VALUES ($1, $2)", i, f"row{i}")

app = Pyronova()

@app.get("/__ping")
def ping(req):
    return "pong"

# Critical: NO gil=True. The handler runs inside a sub-interpreter and
# uses the worker's own (real) PgPool.
@app.get("/items")
def items(req):
    rows = pool.fetch_all("SELECT id, label FROM bridge_test ORDER BY id")
    return {"count": len(rows), "items": rows}

@app.get("/one")
def one(req):
    row = pool.fetch_one("SELECT id, label FROM bridge_test WHERE id = $1", 2)
    return row or {}

@app.get("/scalar")
def scalar(req):
    v = pool.fetch_scalar("SELECT COUNT(*) FROM bridge_test")
    return {"count": v}

@app.get("/exec")
def do_exec(req):
    n = pool.execute("UPDATE bridge_test SET label = label WHERE id >= 0")
    return {"affected": n}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ["PYRONOVA_PORT"]),
            mode="subinterp", workers=2)
'''


def _boot_server() -> tuple[subprocess.Popen, int]:
    """Starts the server on port 0; returns the process and the port it bound."""
    fd, path = tempfile.mkstemp(prefix="pyronova_db_subinterp_", suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(SERVER)
    env = dict(os.environ)
    env["PYRONOVA_PORT"] = "0"
    env["PYRONOVA_TEST_PG_DSN"] = PG_DSN
    log_path = path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, path],
            stdout=log, stderr=subprocess.STDOUT,
            preexec_fn=os.setsid, env=env,
        )
    try:
        port = bound_port(read_file(log_path), proc, timeout=15)
    except RuntimeError as e:
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"sub-interp DB server failed to start:\n{e}") from None
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/__ping", timeout=0.5)
            return proc, port
        except Exception:
            time.sleep(0.1)
    proc.kill()
    proc.wait(timeout=5)
    raise RuntimeError(
        "sub-interp DB server failed to start:\n"
        + read_file(log_path)()[:4000]
    )


@pytest.fixture(scope="module")
def server():
    proc, port = _boot_server()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=5)
        except Exception:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                pass


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read())


def test_fetch_all_from_subinterp(server):
    d = _get_json(server + "/items")
    assert d["count"] == 5
    assert [r["id"] for r in d["items"]] == [0, 1, 2, 3, 4]
    assert d["items"][0]["label"] == "row0"


def test_fetch_one_from_subinterp(server):
    d = _get_json(server + "/one")
    assert d["id"] == 2
    assert d["label"] == "row2"


def test_fetch_scalar_from_subinterp(server):
    d = _get_json(server + "/scalar")
    assert d["count"] == 5


def test_execute_from_subinterp(server):
    d = _get_json(server + "/exec")
    assert d["affected"] == 5


def test_concurrent_subinterp_queries(server):
    """N simultaneous requests across multiple sub-interp workers must
    all succeed. If the bridge had any interp-sharing bug (e.g. a
    Py<T> escaping), this would crash or deadlock.
    """
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: _get_json(server + "/items"), range(64)))
    for d in results:
        assert d["count"] == 5
