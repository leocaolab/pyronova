"""Pure-framework ceiling bench: no TCP, no wrk, no client.

Each TPC worker hosts K virtual connections paired via
`tokio::io::duplex`. Same Hyper + routing + handler pipeline as
real traffic, zero network cost.

Needs an engine built with `maturin develop --release --features bench`.
"""

import sys
from pyronova import Pyronova
from _bench_build import require_bench

app = Pyronova()


@app.get("/")
def index(req):
    return "Hello from Pyronova!"


if __name__ == "__main__":
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    conns = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    duration = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    require_bench(app)
    total, elapsed = app._engine.bench_inmem(
        duration_s=duration, workers=workers, conns_per_worker=conns
    )
    rps = total / elapsed if elapsed else 0
    print(
        f"\n  workers={workers} conns_per_worker={conns} duration={elapsed:.3f}s"
    )
    print(f"  total={total} req  ({rps:,.0f} req/s)\n")
