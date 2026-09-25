# Feature guide

Pyronova ships as **one wheel with every feature compiled in**. Turn them on
with a runtime call — no recompile, no Rust toolchain, no pip extras.

The one exception is `leak_detect`, a diagnostic-only Cargo feature that
adds per-object counters and is disabled in the published wheel.

## Quick reference

| Feature | How you enable it | Default |
|---|---|---|
| HTTP response compression | `app.enable_compression()` | off |
| HTTPS / TLS | `app.run(tls_cert=..., tls_key=...)` | off |
| Per-route upload streaming | `@app.post(..., stream=True)` | off |
| CORS | `app.set_cors_config(...)` | off |
| Request logging | `app.enable_request_logging(True)` | off |
| Static files | `app.static(url_prefix, dir_path)` | off |
| Shared state | `app.state` (property) | on |
| Sub-interpreter mode | `app.run(mode="subinterp")` | on (auto) |
| MCP server endpoint | any `@mcp.tool` decorator | off |
| WebSocket | `@app.websocket(path)` | off |
| SSE streaming (response) | `return Stream(...)` | off |
| gRPC benchmark service (HttpArena `GetSum`) | `app.enable_grpc_benchmark()` | off |
| `leak_detect` (diagnostic) | `maturin develop --features leak_detect` | compile-time off |
| `bench` (in-process benches) | `maturin develop --release --features bench` | compile-time off |

All items above (except the compile-time features) are zero-cost when disabled —
a single relaxed atomic load or a feature-flag branch that predicts away.

## Detailed usage

### Compression (gzip + brotli)

```python
app = Pyronova()
app.enable_compression(
    min_size=512,        # don't compress responses smaller than 512 B
    gzip=True,           # enable gzip
    brotli=True,         # enable brotli (preferred when both negotiated)
    gzip_level=6,        # 1..=9, default 6
    brotli_quality=4,    # 0..=11, default 4 (production sweet spot)
)
```

Respects the client's `Accept-Encoding` header. Skips non-text content
types (images, octet-stream), streaming responses (SSE), and responses
that already set `Content-Encoding`. See benchmarks/benchmark-15 for
comparative numbers vs actix-web.

### TLS

```python
app.run(
    host="0.0.0.0", port=443,
    tls_cert="/etc/ssl/cert.pem",
    tls_key="/etc/ssl/key.pem",
)
```

Or via env var (useful for Docker / k8s):

```bash
PYRONOVA_TLS_CERT=/etc/ssl/cert.pem PYRONOVA_TLS_KEY=/etc/ssl/key.pem \
    python app.py
```

Backed by rustls 0.23 + ring. ALPN advertises `h2` then `http/1.1`, so
HTTP/2 is negotiated automatically. Both `tls_cert` and `tls_key` must
be given together — supplying only one raises `ValueError`.

### Upload streaming

```python
@app.post("/upload", gil=True, stream=True)
def upload(req):
    total = 0
    with open("/tmp/out", "wb") as f:
        for chunk in req.stream():
            f.write(chunk)
            total += len(chunk)
    return {"bytes": total}
```

v1 scope: GIL routes, sync iterator. Sub-interp streaming and
`async for` are deferred to v2. `max_body_size` still bounds total
ingest (default 10 MB; set via `app.max_body_size = N`).

### CORS

```python
app.set_cors_config(
    origin="https://app.example.com",
    methods="GET, POST",
    headers="content-type, authorization",
    expose_headers="x-request-id",
    allow_credentials=True,
)
```

### Request logging

```python
app.enable_request_logging(True)
# or: PYRONOVA_LOG=1 in env
```

Access log goes through the `pyronova::access` tracing target. Caveat:
at 400k rps the access log alone costs ~25–30% throughput. Don't
enable it in benchmarks.

### Static files

```python
app.static("/assets", "./public")   # GET /assets/foo.css → ./public/foo.css
```

Async fs, O_NOFOLLOW (symlink-escape protection), mime detection from
extension. No GIL involvement per request.

## What's NOT a toggle — shipped always-on

These are framework-core and can't be disabled:

- mimalloc global allocator
- sub-interpreter worker pool (auto-used when `mode="subinterp"` or
  `mode="auto"`; falls back to GIL when handlers need it)
- graceful shutdown (ctrl-c drains in-flight requests up to 30 s)
- per-request body size limit (default 10 MB, changeable via `max_body_size`)
- SO_REUSEPORT on Linux (multi-accept kernel load-balancing)
- TCP_NODELAY and TCP_QUICKACK

## Compile-time features (developer / diagnostic only)

### `leak_detect`

Per-object Drop counters for sub-interpreter lifecycle audits. Adds a
`metrics` crate counter increment on every `PyObjRef` drop — not
production-safe at 400k rps.

Build:

```bash
maturin develop --release --features leak_detect
```

Read from Python:

```python
from pyronova.engine import leak_detect_dump
leak_detect_dump()   # prints top buckets to stderr
```

See `docs/memory-leak-investigation-2026-04-19.md` for how this was
used to pin down the PEP 684 tstate bug.

### `bench`

`PyronovaApp.bench_inmem` (virtual connections, no TCP) and
`PyronovaApp.bench_loopback` (real TCP, client in the same process), used by
`benchmarks/bench_inmem*.py` and `benchmarks/bench_loopback*.py`. Absent from the
default build.

```bash
maturin develop --release --features bench
python benchmarks/bench_inmem.py 4 8 8      # workers, conns per worker, seconds
```

## Why not Cargo features for Postgres / Redis / etc.?

We considered it. The math didn't work:

- Saves ~10 MB of wheel size per disabled feature
- Costs: CI matrix explosion (N × build paths), PyPI wheel fragmentation,
  and users having to memorise `pip install pyronova[pg,redis]`
- Context: one comparable framework dep (numpy) is 19 MB; pandas is 13 MB.
  A 20 MB wheel is well within norms.

Plan: everything compiles in by default. Only diagnostic/debug code
(like `leak_detect`) and benchmark harnesses (`bench`) stay behind a Cargo
feature flag: the first carries a runtime cost we can't hide behind a lazy
toggle, the second is not part of the library.

If binary size ever becomes a real problem (e.g. Lambda cold-start, edge
deployment), we'll revisit. Until then, one wheel, one `pip install`.
