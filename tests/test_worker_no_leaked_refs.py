"""No Python reference is dropped without its interpreter's thread state.

`PyObjRef` leaks (and logs "no attached tstate") rather than DECREF on a thread
with no thread state attached. Worker init used to hold its bootstrap and script
module references past `PyEval_SaveThread`, so every worker leaked two module
references at startup (2 per worker, in both dispatch modes).
"""
from __future__ import annotations

import sys

import pytest

from tests.test_isolate_shared_ext import _get, _sigint, _start

_APP = '''
from pyronova import Pyronova
app = Pyronova()

@app.get("/t")
def t(req):
    return {"ok": 1}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port}, mode="subinterp")
'''


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT shutdown is POSIX")
@pytest.mark.parametrize("tpc", ["1", "0"], ids=["tpc", "pool"])
def test_no_reference_dropped_without_a_thread_state(tmp_path, tpc):
    proc, log = _start(_APP, tmp_path, f"refs_{tpc}", workers=4, env_extra={"PYRONOVA_TPC": tpc})
    try:
        assert _get("/t", proc, log) == {"ok": 1}
    finally:
        rc = _sigint(proc)
    text = log.read_text(errors="replace")
    assert rc == 0, text[-3000:]
    assert "no attached tstate" not in text, text[-3000:]
