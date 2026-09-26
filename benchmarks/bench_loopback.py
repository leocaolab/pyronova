"""In-process loopback bench: real TCP, but client runs in the same
process as the server. Isolates kernel network stack cost from
external-client CPU contention.

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
    conns = int(sys.argv[2]) if len(sys.argv) > 2 else 32
    duration = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    require_bench(app)
    total, elapsed, port = app._engine.bench_loopback(
        duration_s=duration, workers=workers, client_conns=conns
    )
    rps = total / elapsed if elapsed else 0
    print(
        f"\n  workers={workers} client_conns={conns} port={port} duration={elapsed:.3f}s"
    )
    print(f"  total={total} req  ({rps:,.0f} req/s)\n")
