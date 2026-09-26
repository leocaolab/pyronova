"""Loopback + Rust-noop ceiling: real TCP on 127.0.0.1, fast-response
route served entirely from Rust, same-process client. Isolates the
kernel TCP stack cost from Python dispatch cost.

Pair with bench_inmem_noop.py (no TCP) to see what the loopback +
kqueue/epoll path costs relative to a pure in-memory pipeline.

Needs an engine built with `maturin develop --release --features bench`.
"""

import sys
from pyronova import Pyronova
from _bench_build import require_bench

app = Pyronova()

app.add_fast_response(
    "GET", "/", b"Hello from Pyronova!", content_type="text/plain"
)

# Dummy handler so sub-interp bootstrap has something to bind; fast-
# response on "/" short-circuits before this runs.
@app.get("/__unused__")
def _stub(req):
    return "unused"


if __name__ == "__main__":
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    conns = int(sys.argv[2]) if len(sys.argv) > 2 else 64
    duration = int(sys.argv[3]) if len(sys.argv) > 3 else 8

    require_bench(app)
    total, elapsed, port = app._engine.bench_loopback(
        duration_s=duration, workers=workers, client_conns=conns
    )
    rps = total / elapsed if elapsed else 0
    print(
        f"\n  [noop-loopback] workers={workers} client_conns={conns} port={port} duration={elapsed:.3f}s"
    )
    print(f"  total={total} req  ({rps:,.0f} req/s)\n")
