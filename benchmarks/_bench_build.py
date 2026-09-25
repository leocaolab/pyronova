"""The in-process benches (`PyronovaApp.bench_inmem` / `bench_loopback`) exist only
in an engine built with the `bench` cargo feature:

    maturin develop --release --features bench
"""

import sys

BUILD = "maturin develop --release --features bench"


def require_bench(app) -> None:
    """Exit with the build command when the installed engine has no benches."""
    if not hasattr(app._engine, "bench_inmem"):
        sys.exit(f"this engine was built without the benches; rebuild with: {BUILD}")
