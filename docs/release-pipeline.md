# Release pipeline design

Goal: every shipped version of Pyronova has been
**(a)** compiled in all supported configurations,
**(b)** exercised under both unit tests and a real load generator,
**(c)** validated against a recorded performance baseline, and
**(d)** proven not to accumulate PyObjects under sustained load.

Constraint: **GitHub Actions is not the right tool for (b)–(d).** Stress
testing on a `ubuntu-latest` VM gives you a noise floor in the 30%
range, and leak detection needs sustained load (tens of millions of
requests) — GH's free minutes don't cover that. The bench + leak gate
runs **locally on the owner's AMD 7840HS box** via `just` recipes,
triggered manually before tagging.

GitHub Actions keeps doing the cheap things: compile check across
Python versions, unit tests, lint. It never claims the build is
"release-ready"; `just release-gate` does.

## Build configurations

| Profile        | Cargo flags | `leak_detect` | Where it runs | Purpose |
|----------------|-------------|---------------|---------------|---------|
| **dev**        | `--profile dev` (default) | on | local + `pytest` | Fast iteration. Debug symbols, no LTO. Diagnostic hooks always live so a stray leak shows up immediately. |
| **release**    | `--profile release` (LTO fat, codegen-units=1, strip) | **off** | shipped to PyPI | What end users get. Zero cost from any diagnostic — the `cfg`-gated code does not link. |
| **canary**     | `--profile release` | **on** | local pre-release soak | Same compile flags as release, but with the leak counter wired up. If `just canary-soak` ever shows a rc≥2 growth curve on a non-whitelisted type, the release is held. |
| **ci-compile** | `--profile dev` | off | GitHub Actions | Compile-only smoke on Python 3.14, no stress. |

Rule of thumb: **release and canary share the exact same codegen.**
`leak_detect` only adds a conditional `counter!` increment at
`PyObjRef::Drop`; the rest of the binary is identical. So any
performance delta we see between canary soak and the eventual
release comes from one call site — known and bounded.

## Release gate

A tag is only cut after `just release-gate` passes. The gate is a
**composition** of smaller recipes so you can run any single step
during development without paying for the others.

```
just release-gate
├── just check            # cargo check, both feature configs
├── just test             # cargo test + pytest (unit + e2e)
├── just bench-compare    # wrk against baseline, fail if regression > 5%
├── just canary-soak      # 5-min wrk with leak_detect, fail if non-whitelist rc≥2 grows
└── just version-sync     # Cargo.toml ↔ CHANGELOG.md ↔ latest git tag agree
```

Each recipe fails fast; `just release-gate` bails on the first
failure. Total wall time on AMD 7840HS:

| Step | Time |
|---|---|
| `check` (both configs) | ~3 min |
| `test` | ~2 min |
| `bench-compare` | ~1 min (3× 10s wrk) |
| `canary-soak` | ~5 min |
| `version-sync` | <1 s |
| **Total** | **~11 min** |

## Baseline management

`bench-compare` reads from `benchmarks/baseline.json`. The file is
committed to the repo. Updating it is a **conscious act**:

```bash
just bench-record > benchmarks/baseline.json
git add benchmarks/baseline.json
git commit -m "bench: record v1.4.6 baseline (AMD 7840HS, kernel 7.0)"
```

The commit message documents the machine + kernel so future comparisons
are apples-to-apples. The file includes:

```json
{
  "machine": "AMD Ryzen 7 7840HS, 16 cores, Linux 7.0",
  "python": "3.14.4",
  "recorded_at": "2026-04-19T01:30:00Z",
  "routes": {
    "GET /": { "req_per_sec": 425000, "p99_us": 571 }
  }
}
```

## Leak gate — what counts as a failure

`just canary-soak` runs the leak-detect build for 5 minutes at 400k
req/s, dumps the histogram, and fails if ANY of:

1. A type not in the **expected-co-owner whitelist** has rc≥2 samples
   growing (i.e., count increases between t=1m and t=5m snapshots).
2. The total `dict` or `_Request` rc=1 drop count is less than
   the request count ×0.9 (meaning we're losing instances before they
   even reach the Drop path).
3. RSS growth across the 5-minute soak is more than 100 KB (catch
   arena creep independent of gc-visible objects).

Whitelist:

```
# Interned / singleton-ish — always rc≥2 legitimately
str    (interned short strings: "GET", "/", "", etc.)
bytes  (empty body singleton)
type   (class objects, re-referenced by every instance)
tuple  (small-tuple cache)
NoneType
```

Anything else showing sustained rc≥2 growth is a regression candidate
and blocks the release until investigated.

## What stays on GitHub

Unchanged: `.github/workflows/ci.yml` keeps the compile-and-unit-test
matrix on every PR. It's fast, catches compile regressions across
Python versions, and doesn't claim to certify the release.

`.github/workflows/release.yml` (tag-triggered wheel build + PyPI
publish) still fires on `git push v*`. Idea: **only push the tag
after `just release-gate` passes locally**. The GH release workflow
assumes the local gate has been run; it doesn't re-validate.

## Developer quickstart

```bash
# Normal day
just test                 # iterate

# Before pushing a significant change
just check                # both configs compile
just bench-compare        # no perf regression

# Before cutting a tag
just release-gate         # full local validation
```

If `release-gate` fails, the developer fixes the regression on a
branch and re-runs. Only green results in a tag.
