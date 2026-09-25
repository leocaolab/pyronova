# Pyronova Logging System Design

## Overview

Pyronova's logging system is built on two principles:
1. **All I/O sinks to Rust** — Python never touches stdout/stderr directly for logs
2. **Zero-cost when off** — `tracing` macros compile to an atomic level-check; when filtered out, no string formatting, no syscalls, no overhead

---

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │         Rust tracing-subscriber      │
                    │   (EnvFilter + fmt::Layer)           │
                    │   targets: pyronova::server              │
                    │            pyronova::access              │
                    │            pyronova::app                 │
                    └──────┬─────────┬──────────┬──────────┘
                           │         │          │
              ┌────────────┘         │          └────────────┐
              │                      │                       │
     ┌────────▼────────┐   ┌────────▼────────┐    ┌────────▼────────┐
     │  Server Log     │   │  Access Log     │    │  App Log        │
     │  pyronova::server   │   │  pyronova::access   │    │  pyronova::app      │
     │                 │   │                 │    │                 │
     │  - Startup      │   │  - method       │    │  - Python       │
     │  - Shutdown     │   │  - path         │    │    logging.*    │
     │  - GIL watchdog │   │  - status       │    │  - worker_id   │
     │  - WS errors    │   │  - latency_us   │    │  - logger name │
     │  - Conn errors  │   │  - mode         │    │  - file:line   │
     └─────────────────┘   └─────────────────┘    └─────────────────┘
           Rust only            Rust only           Python → Rust FFI
```

---

## Three Log Targets

### 1. `pyronova::server` — Server Lifecycle

Startup, shutdown, watchdog alerts, connection errors.

```
INFO  pyronova::server Pyronova started version="1.2.0" mode="hybrid" addr=127.0.0.1:8000
INFO  pyronova::server Shutting down gracefully...
WARN  pyronova::server GIL watchdog: main GIL congested latency_ms=52
WARN  pyronova::server Connection error error="connection reset by peer"
ERROR pyronova::server WebSocket upgrade error error="..."
```

### 2. `pyronova::access` — Request Access Log

Every HTTP request with method, path, status, latency, and execution mode.

```
INFO  pyronova::access Request handled method=GET path=/ status=200 latency_us=198 mode="subinterp"
INFO  pyronova::access Request handled method=POST path=/api/users status=201 latency_us=1542 mode="gil"
WARN  pyronova::access Client error method=GET path=/missing status=404 latency_us=12
ERROR pyronova::access Request failed method=POST path=/crash status=500 latency_us=892
```

### 3. `pyronova::app` — Python Application Logs

User code `logging.info()` calls bridged from Python to Rust via FFI.

```
INFO  pyronova::app Fetching users from DB worker=3 logger=myapp file=app.py line=42
ERROR pyronova::app Database connection failed worker=7 logger=db file=models.py line=88
```

---

## Configuration API

```python
from pyronova import Pyronova

# 1. Debug mode — full output, human-readable text
app = Pyronova(debug=True)

# 2. Production — errors only, JSON for ELK/Datadog
app = Pyronova()  # defaults: level=ERROR, access_log=False, format=json

# 3. Custom — fine-grained control
app = Pyronova(log_config={
    "level": "INFO",        # OFF, ERROR, WARN, INFO, DEBUG, TRACE
    "access_log": True,     # enable per-request logging
    "format": "json",       # json | text
})

# 4. Silent mode — absolute zero overhead for benchmarks
app = Pyronova(log_config={"level": "OFF"})

# 5. enable_logging() — activates access log + Python hook output
app = Pyronova()
app.enable_logging()       # upgrades level to INFO, enables access_log
```

### Environment Variables

| Variable | Effect |
|---|---|
| `PYRONOVA_LOG=1` | Auto-enable logging (equivalent to `app.enable_logging()`) |
| `PYRONOVA_METRICS=1` | Enable GIL watchdog (10ms probe interval) |

---

## Python Logging Bridge

### Problem

Python's default `logging.StreamHandler` does synchronous `write()` to stderr while holding the GIL. At 220k QPS, this destroys throughput.

### Solution

Pyronova hijacks Python's root logger in both the main interpreter and every sub-interpreter:

**Main interpreter** (`app.py`):
```python
class PyronovaRustHandler(logging.Handler):
    def emit(self, record):
        emit_python_log(           # PyO3 FFI → Rust
            levelno=record.levelno,
            name=record.name,
            message=record.getMessage(),
            pathname=record.pathname,
            lineno=record.lineno,
        )
```

**Sub-interpreters** (`_bootstrap.py`):
```python
class _PyronovaRustHandler(logging.Handler):
    def emit(self, record):
        _emit_python_log(              # pyronova.engine.emit_python_log, the same function main uses
            record.levelno,
            record.name,
            record.getMessage(),
            record.pathname or "",
            record.lineno or 0,
            self._worker_id,
        )
```

### Performance Characteristics

| Scenario | Cost |
|---|---|
| `level=OFF` | ~1ns (atomic compare, branch predicted skip) |
| `level=INFO`, log filtered out | ~1ns (same) |
| `level=INFO`, log accepted | ~50-100ns FFI crossing + tracing format |
| Python `logger.info("msg")` | ~200ns (getMessage + FFI) |
| Python `logger.info("data: %s", huge_dict)` | Cost of `%s` formatting (unavoidable) |

---

## Rust Implementation

### Files Modified

| File | Changes |
|---|---|
| `Cargo.toml` | Added `tracing`, `tracing-subscriber` (env-filter, json) |
| `src/logging.rs` | **New** — `init_logger()`, `emit_python_log()` PyO3 functions |
| `src/lib.rs` | Registered `logging` module + functions |
| `src/app.rs` | Startup/shutdown → `tracing::info!`, conn errors → `tracing::warn!` |
| `src/handlers.rs` | Access log with `latency_us`, `method`, `path`, `status`, `mode` |
| `src/interp.rs` | *(historical)* `pyronova_emit_log_cfunc` C-FFI for sub-interpreters — removed in Layer 2; workers now import the real engine and call `emit_python_log` |
| `src/monitor.rs` | GIL watchdog → `tracing::warn!` |
| `src/websocket.rs` | WS errors → `tracing::error!`/`tracing::warn!` |

### Key Design Decisions

1. **`EnvFilter` for zero-cost OFF** — When level is OFF or filtered, `tracing::info!` compiles to a single atomic load + branch. CPU branch predictor hits 100% after warmup.

2. **Separate `pyronova::access` target** — Allows users to disable access log while keeping server/app logs, or vice versa. Controlled by `access_log` config flag mapped to `pyronova::access=off` directive.

3. **Same function in every interpreter** — Sub-interpreter workers import the real `pyronova.engine` (the PyO3 fork makes the module per-interpreter; Layer 2), so the worker's logging handler calls `pyronova.engine.emit_python_log` with its worker id. *Earlier versions* registered a C-FFI built-in `_pyronova_emit_log` instead, because the engine could not then be imported in a sub-interpreter.

4. **Deferred `init_logger` to `run()`; later calls reconfigure** — Allows `enable_logging()` to modify log config before `run()`. `tracing-subscriber` allows one global subscriber per process, so the first `init_logger` installs it with its filter and format layer behind `reload` handles, and a later call (another app in the same process) swaps in its own level, access-log switch and format. An unknown level or format raises `ValueError`; a foreign subscriber already holding the global slot raises `RuntimeError`.

6. **Python levels map by number** — `emit_python_log` takes the record's `levelno` and maps it to the highest standard threshold it reaches (≥40 ERROR, ≥30 WARN, ≥20 INFO, ≥10 DEBUG, else TRACE), so custom levels such as `logging.addLevelName(25, "NOTICE")` log at INFO.

5. **`println!` retained for startup banner** — The human-readable startup banner (`Pyronova v1.2.0 [hybrid mode]...`) is kept as `println!` alongside `tracing::info!` because it's always-visible DX, not filterable log output.

---

## Testing

`tests/test_logging.py` covers 8 scenarios:

| Test | What it verifies |
|---|---|
| `test_gil_mode_logging` | `pyronova::access` line in GIL mode (the Rust access log is the only request log) |
| `test_subinterp_rust_logging` | Rust tracing access log in subinterp mode |
| `test_user_print_in_subinterp` | `print()` works in sub-interpreter handlers |
| `test_user_logging_in_subinterp` | Python `logging.info()` bridges to Rust tracing |
| `test_debug_mode_tracing` | `debug=True` produces server lifecycle tracing |
| `test_debug_mode_access_log` | `debug=True` produces access log with latency |
| `test_python_logging_bridge_main` | Main interpreter logging bridge works |
| `test_json_format` | JSON format output with structured fields |
