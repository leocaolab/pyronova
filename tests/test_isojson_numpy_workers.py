"""isojson 0.2 E2E-3 (CUJ-2): each worker serializes numpy from its own copy.

Four sub-interpreter workers each clone numpy (declared with
`app.isolate("numpy")`, or reactively on a bare `import numpy`). A handler
returns `isojson.dumps(payload(seed), default=raise_, option=OPT_SERIALIZE_NUMPY)`
built from that worker's numpy; the test process builds the same payload with
its own numpy and checks the bytes against orjson 3.12.0's. isojson's type
cache is per interpreter, so a worker recognizing another worker's (or main's)
numpy types would show up as a `default` call, i.e. a 500.

The payload holds only layouts isojson writes natively: C-contiguous native
arrays (f64, f32, f16, i64, u8, bool, M8[ns], 2-D) and scalars, no NaT, no
declined layout. Missing dependencies fail the test rather than skip it.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import sys
import urllib.request

# plain imports: a missing dependency fails E2E-3 instead of skipping it
import isojson
import numpy as np
import orjson
import pytest

from tests.test_isolate_shared_ext import _base, _get, _sigint, _start

pytestmark = [
    pytest.mark.skipif(
        sys.platform not in ("linux", "darwin"),
        reason="isolate() clones via cp -c / cp --reflink (Linux/macOS)",
    ),
]

# One source for the payload: exec'd in the test process and in every worker.
PAYLOAD = '''
def payload(np, seed):
    r = np.random.default_rng(seed)
    n = int(r.integers(1, 40))
    ns = r.integers(-2**62, 2**62, n)
    return {
        "seed": seed,
        "f64": r.standard_normal(n) * 10.0 ** int(r.integers(-8, 12)),
        "f32": r.standard_normal(n).astype(np.float32),
        "f16": r.standard_normal(n).astype(np.float16),
        "i64": r.integers(-2**63, 2**63 - 1, n, dtype=np.int64),
        "u8": r.integers(0, 256, n, dtype=np.uint8),
        "bool": r.integers(0, 2, n).astype(bool),
        "ns": ns.view("M8[ns]"),
        "grid": r.standard_normal((int(r.integers(1, 5)), int(r.integers(1, 5)))),
        "scalars": [np.float64(r.standard_normal()), np.float32(r.standard_normal()),
                    np.int64(r.integers(-2**40, 2**40)), np.uint8(r.integers(0, 256)),
                    np.bool_(r.integers(0, 2)), np.datetime64(int(ns[0]), "ns")],
    }
'''

_HANDLER = '''
import os
import isojson
_D = os.path.realpath(os.environ["PYRONOVA_ISOLATE_DIR"])
PAYLOAD_SRC

def raise_(o):
    raise TypeError(f"default called for {type(o)!r}")

@app.get("/s/{seed}")
def s(req):
    import _interpreters, time, numpy as np
    time.sleep(0.02)  # hold the worker so requests spread across workers
    seed = int(req.params["seed"])
    out = isojson.dumps(payload(np, seed), default=raise_, option=isojson.OPT_SERIALIZE_NUMPY)
    return {"out": out.decode(), "interp": _interpreters.get_current()[0],
            "np_file": os.path.realpath(np.__file__), "copies": _D}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port}, mode="subinterp")
'''.replace("PAYLOAD_SRC", PAYLOAD)

DECLARED = '''
from pyronova import Pyronova
app = Pyronova()
app.isolate("numpy")
import numpy
''' + _HANDLER

REACTIVE = '''
from pyronova import Pyronova
app = Pyronova()
import numpy   # no app.isolate(): each worker trips the error and clones
''' + _HANDLER


def _reference(seed):
    ns = {}
    exec(PAYLOAD, ns)  # noqa: S102
    return orjson.dumps(ns["payload"](np, seed), option=orjson.OPT_SERIALIZE_NUMPY).decode()


@pytest.mark.parametrize(
    "dispatch",
    ["tpc", "pool"],
    ids=["tpc-default", "pool-PYRONOVA_TPC=0"],
)
@pytest.mark.parametrize(
    ("script", "bucket"),
    [(DECLARED, "*"), (REACTIVE, "auto")],
    ids=["declared", "reactive"],
)
def test_each_worker_serializes_its_own_numpy(tmp_path, script, bucket, dispatch):
    assert orjson.__version__ == "3.12.0", orjson.__version__
    # isojson with native numpy (0.2); 0.1 raises "does not support OPT_SERIALIZE_NUMPY"
    assert isojson.dumps(np.float64(1.5), option=isojson.OPT_SERIALIZE_NUMPY) == b"1.5"
    env = {"PYRONOVA_TPC": "0"} if dispatch == "pool" else None
    proc, log = _start(script, tmp_path, f"isojson_np_{dispatch}", workers=4, env_extra=env)
    try:
        _get("/s/0", proc, log)
        base = _base(log) + "/s/"

        def hit(seed):
            return seed, json.loads(urllib.request.urlopen(base + str(seed), timeout=30).read())

        with concurrent.futures.ThreadPoolExecutor(16) as ex:
            results = list(ex.map(hit, range(256)))
    finally:
        rc = _sigint(proc)

    text = log.read_text(errors="replace")
    wrong = [seed for seed, r in results if r["out"] != _reference(seed)]
    assert not wrong, f"{len(wrong)} responses differ from orjson, e.g. seed {wrong[0]}"
    interps = {r["interp"] for _, r in results}
    # Which worker serves a request is pyre's scheduling, not isojson's. The
    # pool (PYRONOVA_TPC=0) hands each request to any idle worker. TPC
    # (the default) pins each connection to one thread chosen by the kernel's
    # SO_REUSEPORT, which balances on Linux but not on macOS (app.rs), so on
    # macOS every connection lands on one worker.
    spread = dispatch == "pool" or sys.platform != "darwin"
    assert len(interps) >= (2 if spread else 1), f"requests reached only interpreters {interps}"
    copies = os.path.realpath(tmp_path / "copies")
    outside = {r["np_file"] for _, r in results if not r["np_file"].startswith(copies)}
    assert not outside, f"numpy loaded from outside the copies dir: {outside}"
    clones = list((tmp_path / "copies").glob(f"{bucket}/w*/numpy"))
    assert len(clones) == 4, f"expected 4 per-worker numpy clones, got {clones}"
    assert rc == 0, f"rc={rc}:\n{text[-3000:]}"
    assert "Fatal Python error" not in text, text[-3000:]
