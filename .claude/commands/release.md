Cut a Pyronova release: validate on BOTH platforms, confirm CI, then tag + push.

Pyronova ships a compiled extension whose whole point is speed + zero-leak under
sub-interpreters, so a release is validated on real hardware on **both** targets
before tagging — not on GitHub Actions (its VMs are too noisy for bench/leak; see
`docs/release-pipeline.md`). `just release-gate` already bundles most of this for
one box; this runbook extends it to both platforms and adds the grill soak.

## Machine roles

- **mac** (local, Apple Silicon / ARM): the dev box. Fast iteration, but NOT the
  perf baseline (its numbers are not comparable to the Linux baseline).
- **bluewhale** (`ssh bluewhale`, `~/projects/pyre`, Linux / AMD 7840HS): the
  perf-baseline box (`benchmarks/baseline.json` was recorded here) and the closest
  mirror of the CI/production target. On bluewhale always
  `source .venv/bin/activate` first; it has numpy/scipy/scikit-learn installed.

Both boxes share `origin git@github.com:leocaolab/pyronova.git`. Get the release
commit onto bluewhale with `git fetch && git checkout <sha>` (or apply a patch);
don't rely on its working tree matching mac.

## Pre-flight (hard-won gotchas — do these or results lie)

- **Kill anything on :8000 first**, every time, before any grill/bench run:
  `lsof -ti :8000 | xargs -r kill -9; pkill -9 -f stress_grill`. A stale server
  from a previous run silently answers your curls and fakes 404s / wrong numbers.
- **Never compare a mac bench against the Linux `baseline.json`** — different
  arch/OS, apples-to-oranges. `just bench-compare`'s 5% gate is only meaningful on
  bluewhale. On mac, bench for a relative/self number, not the gate.
- **`_linux_only` RSS soak tests skip on mac** (they read `/proc`). The binding
  leak gate therefore only runs on bluewhale/CI — mac green is necessary, not
  sufficient.
- **grill startup is slow on the first-ever boot** (macOS verifies each freshly
  cloned `.dylib`; ~minutes at W=16 cold). It is one-time — reruns reuse the
  version-keyed clones and start in seconds. Give the cold run a long wait, or use
  `PYRONOVA_WORKERS=4` for a quick check.
- On bluewhale, **raise the fd limit before the full suite**: `ulimit -n 4096`
  (pytest's temp-symlink cleanup hits "Too many open files" otherwise — a teardown
  artifact, not a test failure).

## Steps

1. **Comprehensive tests — BOTH platforms.**
   - mac: `just test` (cargo test + pytest, GIL + sub-interp paths).
   - bluewhale: `ssh bluewhale 'bash -lc "cd ~/projects/pyre && source .venv/bin/activate && ulimit -n 4096 && python -m pytest tests/ -q --ignore=tests/test_ws_binary_client.py --ignore=tests/multifile_app"'`
     — this run INCLUDES the two `_linux_only` RSS soak leak gates
     (`test_rss_growth_per_request_is_bounded`, `test_sustained_concurrent_load_no_leak`).
     Confirm they actually ran (not skipped) — `tests/test_subinterp_memory_regression.py`
     should show 9 passed on Linux vs 7 passed + 2 skipped on mac.

2. **grill soak — BOTH platforms** (numpy + scipy + sklearn + orjson isolated per
   worker; catches C-extension-in-sub-interp regressions the unit tests can't).
   On each box: clean port, clean isolate dir, start server, hammer `/grill`,
   assert responses are 2xx and RSS is flat.
   ```bash
   lsof -ti :8000 | xargs -r kill -9; pkill -9 -f stress_grill; rm -rf /tmp/pyronova-isolate
   PYRONOVA_WORKERS=16 <python> examples/stress_grill.py > /tmp/grill.log 2>&1 &
   # wait until  curl -s -o /dev/null -w %{http_code} :8000/grill  == 200 (cold: allow minutes)
   wrk -t8 -c128 -d60s http://127.0.0.1:8000/grill        # expect ~0 Non-2xx
   # sample RSS before/after; on Linux read /proc/<pid>/status VmRSS, on mac `ps -o rss=`
   ```
   PASS = every response 2xx (a `{"error":"not found"}` means you hit a stale
   server — recheck the port) AND RSS essentially flat across the soak.

3. **Performance benchmark — BOTH platforms.**
   - bluewhale (authoritative gate): `just bench-compare` — fails if >5% below
     `benchmarks/baseline.json`. This is the number that gates the release.
   - mac (relative sanity): run the same plaintext bench and record the mac number
     for the release notes, but do NOT gate on the Linux baseline. If mac has its
     own recorded baseline, compare to that; otherwise report the raw req/s and
     confirm it's in the expected range for this box.
   Also worth a look: `just bench-tfb-plaintext` (pipelined) and `just canary-soak`
   (5-min leak histogram) on bluewhale — `canary-soak` is part of `release-gate`.

   Shortcut for steps 1+3+leak on bluewhale in one shot: `just release-gate`
   (= check + test + bench-compare + canary-soak + version-sync). Grill (step 2)
   and the mac side are still separate.

4. **CI all green.** Push the release branch/commit to `main` (or its PR) and
   confirm `ci.yml` is green across Python 3.13 + 3.14 (unit + integration):
   `gh run list --branch main --limit 3` / `gh pr checks`. CI is compile + tests
   only; it does NOT claim release-readiness — steps 1–3 do.

5. **Tag and push.**
   - Bump `version` in `Cargo.toml` + add the `## vX.Y.Z` heading to `CHANGELOG.md`,
     then `just version-sync` (asserts Cargo.toml ↔ CHANGELOG ↔ tag agree).
   - `git tag vX.Y.Z && git push origin vX.Y.Z` — the tag push triggers
     `release.yml`, which builds wheels (manylinux x86_64 + macOS arm) and publishes
     to PyPI. Watch it: `gh run watch`.

## Policy

Fail closed: if ANY step is red on EITHER platform, stop — do not tag. Report a
summary table (step × platform → pass/fail with the number), and for a failure
bring out the real error/output, not a "failed" label. Do not auto-bump the
version or tag until steps 1–4 are green on both boxes.
