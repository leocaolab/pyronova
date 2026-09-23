"""E2E app (Layer 2, M3): the M3 engine API seen from a worker that executes the real engine.

Workers still get the mock `pyronova` package from `_bootstrap.py`. As in
`_l2_main_side_app.py`, this script additionally loads the real `pyronova.engine` into
each worker (user code, not a framework switch), and uses it to exercise what M3 adds:

- `_in_worker()` is true in a worker (FR-15);
- a `SharedState()` created in a worker sees the running app's map (FR-5);
- `set_max_body_size` / `configure_compression` are main-only; a worker's differing value
  logs a warning and main's value stays in force (FR-17);
- `_register_worker_app` accepts one app per worker and refuses a second (FR-4);
- pyclass types print as `pyronova.engine.*` (FR-18).
"""
import os

import pyronova.engine as _engine

_real_engine = None
if "_pyronova_emit_log" in globals():
    # Inside a worker: `_pyronova_emit_log` is injected only into worker globals.
    import importlib.machinery
    import importlib.util

    _loader = importlib.machinery.ExtensionFileLoader(
        "pyronova.engine", os.environ["L2_ENGINE_PATH"]
    )
    _real_engine = importlib.util.module_from_spec(
        importlib.util.spec_from_loader("pyronova.engine", _loader)
    )
    _loader.exec_module(_real_engine)

    _probe_app = _real_engine.PyronovaApp()
    # FR-17: differs from main's values below, so each logs a warning and changes nothing.
    _probe_app.set_max_body_size(999_999)
    _probe_app.configure_compression(True, 1)
    # FR-4: the first registration is accepted, a second app is refused.
    _probe_app._register_worker_app()
    try:
        _real_engine.PyronovaApp()._register_worker_app()
        SECOND_APP_ERROR = None
    except RuntimeError as e:
        SECOND_APP_ERROR = str(e)
else:
    # Main interpreter: the real module; tell the workers where it is.
    os.environ["L2_ENGINE_PATH"] = _engine.__file__
    SECOND_APP_ERROR = None

from pyronova import Pyronova  # noqa: E402

app = Pyronova(log_config={"level": "WARN"})
app.max_body_size = 2048  # main applies it; the worker replay (mock) is a no-op


@app.get("/w")
def worker_info(req):
    return {
        "real_engine": _real_engine is not None,
        "in_worker": _real_engine._in_worker() if _real_engine else None,
        "request_type": repr(_real_engine.Request) if _real_engine else None,
        "second_app_error": SECOND_APP_ERROR,
    }


@app.get("/incr")
def worker_incr(req):
    # A bare SharedState() in a worker: must be the running app's map (FR-5).
    _real_engine.SharedState().incr("hits", 1)
    return {"ok": True}


@app.post("/upload")
def worker_upload(req):
    return {"len": len(req.body)}


@app.get("/main_hits", gil=True)
def main_hits(req):
    return {"hits": app.state.get("hits")}


@app.get("/main_in_worker", gil=True)
def main_in_worker(req):
    return {"in_worker": _engine._in_worker()}


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ["L2_PORT"]), workers=2)
