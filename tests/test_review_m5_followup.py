"""Review cced8c2, M5 follow-up: a worker resolves the script's imports as main does.

A new sub-interpreter's `sys.path` is only what the process started with. Everything main
added at run time was missing in workers: pytest's `pythonpath` / rootdir entries, the
directory `pyronova run` puts first. A worker re-executing a test file that does
`from tests.apps.… import …` then failed with `ModuleNotFoundError: No module named
'tests'` under a bare `pytest` (its `sys.path[0]` is the venv's `bin`), and
`pyronova run app:app` could not serve an app that imports a module next to it. Workers
now start from main's `sys.path` (`WorkerSpec::import_path`).
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.request

import pytest

from pyronova import Pyronova
from pyronova.testing import TestClient
from tests._helpers import listening_ports, read_file
from tests.apps.m5_followup_sibling import add_routes

# Module level: sub-interpreter workers rebuild it by executing this file, and this file
# imports its routes from a sibling module in the `tests` package.
app = Pyronova()
add_routes(app)


def test_module_app_importing_a_sibling_serves_on_workers():
    with TestClient(app) as client:
        r = client.get("/import-env")
        assert r.status_code == 200
        assert r.json()["sibling"] == "tests.apps.m5_followup_sibling"


def test_worker_sys_path_is_mains():
    # The whole of main's `sys.path`, in order, including the entries main added at run
    # time (here pytest's `tests/` and root dir), which a new interpreter never has.
    main_path = list(sys.path)
    with TestClient(app) as client:
        assert client.get("/import-env").json()["sys_path"] == main_path


HOST = "127.0.0.1"
SERVE_TIMEOUT_S = 30


@pytest.mark.parametrize("tpc", ["1", "0"], ids=["tpc", "pool"])
def test_cli_run_serves_an_app_importing_a_module_next_to_it(tpc):
    # The console script: its `sys.path[0]` is the venv's `bin`, and `pyronova run` puts
    # the working directory first on main's `sys.path` so `app` imports (cli.py). The
    # workers execute `app.py` by path; its `import helper` must resolve there too.
    cli = shutil.which("pyronova", path=os.path.dirname(sys.executable))
    assert cli is not None, "the pyronova console script is not installed next to python"
    project = tempfile.mkdtemp(prefix="pyronova-m5f-")
    with open(os.path.join(project, "helper.py"), "w") as f:
        f.write("GREETING = 'from helper'\n")
    with open(os.path.join(project, "app.py"), "w") as f:
        f.write(textwrap.dedent("""\
            import helper
            from pyronova import Pyronova

            app = Pyronova()

            @app.get("/")
            def root(req):
                return {"greeting": helper.GREETING}
            """))
    log_path = os.path.join(project, "server.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [cli, "run", "app:app", "--host", HOST, "--port", "0", "--workers", "2"],
            cwd=project,
            env={**os.environ, "PYRONOVA_TPC": tpc},
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
    try:
        body = None
        deadline = time.monotonic() + SERVE_TIMEOUT_S
        while time.monotonic() < deadline and proc.poll() is None:
            ports = listening_ports(read_file(log_path)())
            if not ports:
                time.sleep(0.1)
                continue
            try:
                with urllib.request.urlopen(f"http://{HOST}:{ports[0]}/", timeout=2) as r:
                    body = r.read().decode()
                break
            except OSError:
                time.sleep(0.1)
        if body is None:
            proc.kill()
            proc.wait(timeout=10)
            out = read_file(log_path)()
            pytest.fail(f"server did not serve (exit {proc.returncode}); output:\n{out}")
        assert "from helper" in body
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=SERVE_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        shutil.rmtree(project, ignore_errors=True)
