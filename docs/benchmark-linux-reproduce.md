# Reproducing the Pyronova benchmark research on Linux

A step-by-step to land the same three-layer bench ceiling we measured
on macOS, but on real Linux hardware where `SO_REUSEPORT` kernel LB
and `epoll` actually work. The macOS numbers we collected were
kernel-bound (loopback at ~437k/core regardless of worker count);
Linux should show TCP scaling linearly with worker count up to
NIC/core limits.

## 0. Requirements

- Bare-metal or dedicated VM Linux box (no containers, no noisy
  neighbors). Shared cloud instances give wildly varying numbers.
- Kernel 5.x+ (for `SO_REUSEPORT` load-balancing + `TCP_DEFER_ACCEPT`
  + `TCP_QUICKACK`).
- Physical cores ≥ 8 preferred. Pyronova's TPC mode sizes itself to
  physical cores (SMT stripped via `/sys/.../thread_siblings_list`).
- Rust 1.85+ (for `Arc::leak`-adjacent APIs we rely on indirectly).
- Python 3.14 (PEP 684 sub-interpreters work).
- `wrk` for external-client comparison (optional).
- `uv` or `pip` for venv management.

```bash
# Ubuntu/Debian example
sudo apt install build-essential python3.14 python3.14-dev python3.14-venv wrk
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## 1. Clone + build

```bash
git clone git@github.com:leocaolab/pyronova.git
cd pyronova
python3.14 -m venv .venv
source .venv/bin/activate
pip install maturin
maturin develop --release --features bench   # 1-3 min on first build
```

The `bench` cargo feature compiles in `bench_inmem` / `bench_loopback`, which the
`bench_inmem*.py` / `bench_loopback*.py` scripts below call; the default build has
neither. External-client runs (wrk, section 3) work with either build.

Verify:

```bash
.venv/bin/python -c "from pyronova.engine import PyronovaApp; assert hasattr(PyronovaApp, 'bench_inmem'); print('ok')"
```

## 2. Benchmark ladder (run in this order)

The three benches peel back layers to isolate where the cost lives.
Run each with 3 samples, take the median.

### 2.1 `bench_inmem.py` — pure framework, no TCP

Virtual connections via `tokio::io::duplex`, zero kernel involvement.
Upper bound on what the framework can do with the Python handler in
the path.

```bash
# Scan: workers × connections_per_worker, 8-second measurement each
for w in 1 2 4 8 $(nproc); do
  for c in 4 8 16 32; do
    rps=$(.venv/bin/python benchmarks/bench_inmem.py "$w" "$c" 8 \
          2>&1 | awk '/req\/s/ {gsub(/[(),]/,""); print $(NF-1)}')
    printf "  python inmem w=%2d c=%2d %s\n" "$w" "$c" "$rps"
  done
done
```

**Expected on Linux 64-core Xeon/EPYC**: single-worker 1.0-1.3M,
workers should scale to 60-75% efficiency at w=nproc (framework
ceiling; atomic contention from Hyper/Tokio state is the only
remaining headwind).

On darwin M5 Pro (6 P-cores): w=1 ~1.04M, w=6 ~5.14M (79% efficient).

### 2.2 `bench_inmem_noop.py` — framework minus Python

Same as 2.1 but the route is registered via `add_fast_response`,
which serves a pre-built byte array entirely from Rust. Measures
Hyper parse + routing + response-build cost with no GIL involvement.

```bash
for w in 1 2 4 8 $(nproc); do
  rps=$(.venv/bin/python benchmarks/bench_inmem_noop.py "$w" 8 8 \
        2>&1 | awk '/req\/s/ {gsub(/[(),]/,""); print $(NF-1)}')
  printf "  rust noop w=%2d %s\n" "$w" "$rps"
done
```

**Expected Linux**: single-worker 2.0-2.5M. The gap between 2.1 and
2.2 at w=1 is the per-request Python cost (sub-interp GIL acquire +
call_handler + response extract). Should be ~50% of request budget,
matching the darwin 2.17M noop vs 1.04M python w=1 ratio.

### 2.3 `bench_loopback.py` — real TCP, client in same process

Server + client on 127.0.0.1, both inside the same process. Isolates
the kernel network path cost from external-client CPU contention.
The gap between 2.1 and 2.3 is pure syscall + TCP state + loopback
copy + epoll wakeup overhead.

```bash
# Default: 6 workers, 32 client connections, 6-second run
for w in 1 2 4 8 $(nproc); do
  for c in 32 128 512 1024; do
    rps=$(.venv/bin/python benchmarks/bench_loopback.py "$w" "$c" 8 \
          2>&1 | awk '/req\/s/ {gsub(/[(),]/,""); print $(NF-1)}')
    printf "  python loopback w=%2d c=%-4d %s\n" "$w" "$c" "$rps"
  done
done
```

**Expected Linux**: at w=8+ the numbers should scale with workers,
capping somewhere between 2.1 and 2.2 at each level. The darwin
result of `w=1 335k ≈ w=6 333k` (flat — no scaling) should NOT
reproduce; Linux `SO_REUSEPORT` distributes connections across
listeners at the kernel level.

### 2.4 `bench_loopback_noop.py` — TCP + Rust fast-response

The Rust noop over real TCP. Tightest measurable TCP-path ceiling.

```bash
for w in 1 2 4 8 $(nproc); do
  rps=$(.venv/bin/python benchmarks/bench_loopback_noop.py "$w" 128 8 \
        2>&1 | awk '/req\/s/ {gsub(/[(),]/,""); print $(NF-1)}')
  printf "  rust loopback-noop w=%2d %s\n" "$w" "$rps"
done
```

**Expected Linux**: should scale close to linearly with workers.
darwin flatlined at ~437k regardless of w (SO_REUSEPORT last-socket-
wins on kqueue). Linux should show w=64 hitting low-single-digit
millions.

## 3. External wrk comparison (optional but useful)

Shows what a real external client sees vs the in-process number —
the gap reveals how much the external client itself consumes.

```bash
# Terminal 1: start Pyronova
.venv/bin/python benchmarks/bench_plaintext.py

# Terminal 2: wrk
wrk -t4 -c100 -d10s http://127.0.0.1:8000/

# Compare to bench_loopback.py output at similar worker count.
# External wrk number ≤ bench_loopback number, by definition.
```

To pin wrk to specific cores so it doesn't steal from the server:

```bash
taskset -c 60-63 wrk -t4 -c100 -d10s http://127.0.0.1:8000/
# Pyronova already pins to core 0..N-1, wrk pinned to 60-63 = no contention
```

## 4. Sanity-check the optimizations are actually active

Three env vars gate optimizations that we measured significant for:

| Optimization | How it's applied | Verify |
|---|---|---|
| Per-worker leaked route table | Automatic | Single process, multiple TPC threads, zero Arc::clone per request on hot path — no toggle |
| Metrics gate | `PYRONOVA_METRICS=0` (default) | `grep count_request` in `src/monitor.rs` — guard should be there |
| TPC enabled | On by default | Startup log should say `[TPC mode, ...]` |
| Darwin fanout (bench only, macOS-specific) | Not relevant on Linux | N/A |

Run with explicit metrics off (they're already default off):

```bash
env PYRONOVA_METRICS=0 .venv/bin/python benchmarks/bench_inmem.py 6 8 8
```

## 5. Recording results

Update `benchmarks/baseline.json` if establishing a new Linux
baseline. Current file records AMD Ryzen 7 7840HS numbers.

Submit results with:
- CPU model + core count (physical + logical)
- Kernel version (`uname -r`)
- Python version (`python3 --version`)
- Pyronova commit hash
- Each bench's best-of-3 number

## 6. Comparing to our darwin ceiling data (M5 Pro)

| Path | darwin w=1 | darwin w=6 | Linux expected w=1 | Linux expected w=N |
|---|---|---|---|---|
| In-mem Python | 1.04M | 5.14M | ~1.0-1.3M | 60-75% scaling |
| In-mem Rust noop | 2.17M | 7.83M | ~2.0-2.5M | 60-75% scaling |
| Loopback Python | 335k | 333k (flat) | ~300-400k | Should scale! |
| Loopback Rust noop | 437k | 422k (flat) | ~400-500k | Should scale! |

The two "Loopback" rows being flat on darwin is the key observation —
kqueue-based `SO_REUSEPORT` delivers ~all traffic to one listener.
Linux should NOT reproduce the flatness.

## 7. If numbers look off

- **Loopback doesn't scale with workers on Linux**: check
  `/proc/sys/net/core/somaxconn` (should be 8192+; Pyronova's listen
  backlog is 8192, kernel caps at this value).
- **In-mem numbers much lower than darwin**: probably a debug build.
  Verify `maturin develop --release`, not plain `maturin develop`.
- **Single-worker ceiling below 1M (python) / 2M (noop)**: CPU
  frequency scaling. Disable CPU frequency governor
  (`cpupower frequency-set -g performance`) before measuring.
- **High variance between runs**: other processes competing. Pin
  server to isolated cores with `taskset` and/or kernel isolcpus=.
