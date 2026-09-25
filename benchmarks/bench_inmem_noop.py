"""Rust-noop ceiling bench: no Python handler, pure fast-response path.

`add_fast_response` registers a pre-built Response served directly
from the TPC accept loop — no sub-interp GIL, no handler dispatch,
no response build. Measures the floor cost of Hyper parse + routing
+ response write, per worker.

Needs an engine built with `maturin develop --release --features bench`.
"""

import sys
from pyronova import Pyronova
from _bench_build import require_bench

app = Pyronova()

app.add_fast_response(
    "GET", "/", b"Hello from Pyronova!", content_type="text/plain"
)

# bench_inmem still needs a valid Python route to satisfy the init
# path (sub-interp bootstrap wants at least the fallback routing). But
# this route will never fire — the fast_response match short-circuits.
@app.get("/__unused__")
def _stub(req):
    return "unused"


if __name__ == "__main__":
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    conns = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    duration = int(sys.argv[3]) if len(sys.argv) > 3 else 6

    require_bench(app)
    total, elapsed = app._engine.bench_inmem(
        duration_s=duration, workers=workers, conns_per_worker=conns
    )
    rps = total / elapsed if elapsed else 0
    print(
        f"\n  [noop] workers={workers} conns={conns} duration={elapsed:.3f}s"
    )
    print(f"  total={total} req  ({rps:,.0f} req/s)\n")
