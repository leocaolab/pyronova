# Release process

The one spec for cutting a Pyronova release: what runs, where, and what counts as a
pass. `.claude/commands/release.md` runs this checklist; it does not add steps of its
own.

Every shipped version has been
**(a)** tested on both release platforms (Linux x86_64, macOS arm64), including the
feature-gated and platform-only tests,
**(b)** soaked with real C extensions in sub-interpreters on both platforms,
**(c)** measured against the recorded performance baseline, and
**(d)** shown not to accumulate Python objects under sustained load.

GitHub Actions covers compile, lint and the test suites; its 4-vCPU VMs are too noisy
for (c) and too short for (d). CI green is necessary, never sufficient.

**Fail closed.** If any step is red on either platform, stop: no merge, no tag. Report
a table (step × platform → pass/fail with the number), and for a failure the real
output, not a "failed" label.

## Machines

| Machine | Role |
|---|---|
| **mac** (local, Apple Silicon) | Dev box. Tests and grill soak. Its bench numbers are recorded, never compared to the Linux baseline. |
| **bluewhale** (`ssh bluewhale`, AMD Ryzen 7 7840HS, 16 threads, Linux) | Baseline box: `benchmarks/baseline.json` was recorded here. Linux tests, grill soak, bench gate, leak gate. |

On bluewhale, test the release commit in its own worktree
(`git worktree add --detach ../pyre-<sha> <sha>`, own `.venv`), never by switching the
checkout in `~/projects/pyre`. Raise the fd limit first: `ulimit -n 4096`.

## Checklist

Run on the release commit: the tip of the branch that will merge into `main`, with the
version bump (step 9) already on it.

### 1. CI green

CI (`.github/workflows/ci.yml`) runs on a push to `main` or a pull request to `main`
only, so open the PR (a draft is fine) to get a run. All four jobs pass: Unit tests,
Integration tests (includes the `bench,fault_injection` build step), macOS, Rust lint.
Wait on a run by the commit's **full** SHA (`gh run list --commit <full sha>`; a short
SHA matches nothing).

### 2. Rust checks — mac

```bash
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
```

### 3. Full test suite — both platforms, default release build

```bash
maturin develop --release
pytest tests/ -q -rs --ignore=tests/e2e
```

Pass: 0 failed. Then audit every skip in the `-rs` summary; each must be one of:

| Skip | Where it is expected |
|---|---|
| Linux-only (`/proc`), e.g. `test_subinterp_memory_regression.py` RSS soaks | mac |
| Darwin-only (fanout topology) | Linux |
| `bench` / `fault_injection` feature not built | default build (covered by step 4) |
| Postgres tests without `PYRONOVA_TEST_PG_DSN` | when no database is set up |

On Linux, `tests/test_subinterp_memory_regression.py` must show **9 passed** (on mac: 7
passed, 2 skipped). A skip of any other kind is a failure until explained.

### 4. Feature-gated tests — both platforms

```bash
maturin develop --release --features bench,fault_injection
pytest tests/ -q -rs --ignore=tests/e2e
```

Pass: 0 failed, and no skip names the `bench` or `fault_injection` feature. The full
suite runs here, not a hand-picked list: CI's feature step selects tests by file and
`-k`, and misses feature-gated tests in other files. Rebuild without features
(`maturin develop --release`) before step 6.

### 5. Suspected flaky test

A test that fails in CI but passes locally is not "flaky" until the cause is known.
CI runners have 4 vCPUs; reproduce on bluewhale with the same core count and repeat:

```bash
for i in $(seq 20); do taskset -c 0-3 pytest tests/<file>.py -q -k <test> | tail -1; done
```

Then decide with evidence whether the test froze a wrong assumption (e.g. an order the
server doesn't guarantee, a probe that disturbs what it measures) or the code is
wrong. Report the finding before editing any existing test; don't retry CI until green.

### 6. grill soak — both platforms

numpy + scipy + scikit-learn + orjson, each isolated per worker; catches
C-extension-in-sub-interpreter regressions the test suite can't. Look at what holds
`:8000` before killing it; a stale server answers and fakes results.

```bash
lsof -i :8000 -sTCP:LISTEN          # stop a stale Pyronova server if one is there
rm -rf /tmp/pyronova-isolate
PYRONOVA_WORKERS=16 .venv/bin/python examples/stress_grill.py > /tmp/grill.log 2>&1 &
# wait until GET :8000/grill is 200 (cold start on mac takes minutes: each cloned .dylib is verified)
wrk -t8 -c128 -d10s http://127.0.0.1:8000/grill      # warm-up
# RSS before: Linux /proc/<pid>/status VmRSS, mac `ps -o rss= -p <pid>`
wrk -t8 -c128 -d60s http://127.0.0.1:8000/grill
# RSS after
```

Pass: wrk prints no `Non-2xx` line, the server is still running, `/tmp/grill.log` has
no panic / abort / double free / traceback, and RSS after is within 1% of RSS before.
Record req/s and wrk timeouts (requests slower than wrk's 2 s) in the report.

### 7. Performance gate — bluewhale

```bash
just bench-compare
```

Pass: the best of three 10 s runs is at least 95% of `benchmarks/baseline.json`.
Compare against the baseline only; don't add ad-hoc control runs.

The machine must be quiet: 1-minute load average below 1.5 and nothing compiling.
Never bench right after a build on the same box (a release build saturates all 16
threads and the load average lags); build, wait for the load to drop, then run.
A run under load is discarded, not reported as a regression.

On mac, run `benchmarks/bench_plaintext.py` with the same wrk command and record the
number for the release notes; it does not gate.

### 8. Leak gate — bluewhale

```bash
just canary-soak
```

Builds with `leak_detect`, serves `GET /` under `wrk -t4 -c100` for 5 minutes, dumps
the `pyronova_drop_rc` histogram. Pass: the recipe prints `OK — no leak suspects`, i.e.
no type outside the whitelist (below) was dropped at rc 2–8 more than the threshold
number of times. Also check that the `pyronova.engine.Request` rc=1 count matches the
request count wrk reports. Throughput here is not gated (step 7 is).

Known recipe issue: the threshold is meant to be 10% of the requests, but the recipe
reads `Requests/sec` from the server's stderr, where wrk's output isn't, so it always
falls back to 10,000,000 requests: the threshold is a fixed 1,000,000 samples.

Whitelist (legitimately rc≥2: interned or cached):

```
str, bytes, type, tuple, NoneType
```

### 9. Version and docs — on the release branch

- `version` in `Cargo.toml`, then `cargo update -p pyronova-engine --offline` so
  `Cargo.lock` agrees.
- `CHANGELOG.md`: a `## vX.Y.Z (date) — summary` section; **Breaking** first, then
  Changed / Added / Fixed. Every API name in it checked against the code.
- `README.md`: a "What's new in vX.Y" section; supported Python and wheel platforms
  stated where they appear.
- `just version-sync` passes.

Steps 1–8 run on the commit that contains these changes.

### 10. Merge, tag, publish

1. Merge the PR into `main`.
2. Tag the merge commit on `main` and push the tag:
   `git tag vX.Y.Z <main sha> && git push origin vX.Y.Z`.
   The tag push runs `.github/workflows/release.yml`: wheels for Linux x86_64
   (manylinux) and macOS arm64 on Python 3.14, plus the sdist, published to PyPI.
   A published version can't be replaced, so push the tag only after steps 1–9 pass.
3. Watch the run (`gh run watch`), then confirm PyPI lists the version with both wheels
   and the sdist.

## Build configurations

| Profile | Cargo features | Where it runs | Purpose |
|---|---|---|---|
| **release** | none | shipped to PyPI | What users get. |
| **feature test** | `bench,fault_injection` | step 4, CI | Bench harnesses and fault injection for the tests that need them. Never shipped. |
| **canary** | `leak_detect` | step 8 | Release codegen plus the drop-refcount sampler. |

`leak_detect` only adds a refcount sample where a worker drops a request's `Request`,
and two request counters; the rest of the binary is identical to release.

## Baseline management

`bench-compare` reads `benchmarks/baseline.json`, which is committed. Re-recording it
is a deliberate act, on bluewhale, on a quiet machine:

```bash
just bench-record        # writes benchmarks/baseline.json
git add benchmarks/baseline.json
git commit -m "bench: record baseline (<machine>, kernel <uname -r>, Python <version>)"
```

The file records machine, kernel, Python and time:

```json
{
  "machine": "AMD Ryzen 7 7840HS w/ Radeon 780M Graphics",
  "kernel": "7.0.0-10-generic",
  "python": "3.14.4",
  "recorded_at": "2026-04-19T22:20:05Z",
  "routes": { "GET /": { "req_per_sec": 422976 } }
}
```

## `just release-gate`

`just release-gate` = `check` + `test` + `bench-compare` + `canary-soak` +
`version-sync` on one box. On bluewhale it covers steps 3 (Linux), 7, 8 and part of 9;
steps 1, 2, 4, 5, 6 and the mac side still run separately.
