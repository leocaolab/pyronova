"""Launcher — spawns ONE Pyronova process serving all ports simultaneously.

Plain HTTP on $PORT (default 8080). When TLS certs are present, HTTPS is
also served on $PORT+1 (json-tls profile) and 8443 (baseline-h2 / static-h2
profile) via PYRONOVA_TLS_PORTS — all from the same process.

Each TPC thread creates its own SO_REUSEPORT socket on every port, so all
cores serve all profiles simultaneously. This gives every profile access to
all CPUs, unlike the old two-process approach that split cores 50/50.
"""

import os
import signal
import subprocess
import sys
import threading
import time


def _cpu_count() -> int:
    try:
        return max(len(os.sched_getaffinity(0)), 1)
    except AttributeError:
        return max(os.cpu_count() or 1, 1)


def _numa_nodes() -> int:
    """How many NUMA nodes does the kernel see? 1 on UMA systems
    (laptops, Apple Silicon, single-socket AMD/Intel desktop),
    2+ on multi-CCD Threadripper/EPYC and multi-socket boxes."""
    try:
        return max(
            sum(1 for d in os.listdir("/sys/devices/system/node") if d.startswith("node")),
            1,
        )
    except (FileNotFoundError, PermissionError):
        return 1


def _parse_port(raw: str) -> int:
    """Parse the PORT env var, failing with a clear message instead of a
    raw ValueError traceback from int()."""
    try:
        port = int(raw)
    except (TypeError, ValueError):
        raise SystemExit(f"invalid PORT: {raw!r} is not an integer")
    if not (1 <= port <= 65535):
        raise SystemExit(f"invalid PORT: {port} out of range 1-65535")
    return port


def main() -> int:
    total = _cpu_count()
    per_proc = total
    io_per_proc = per_proc

    base_port = _parse_port(os.environ.get("PORT", "8080"))
    tls_cert = os.environ.get("TLS_CERT", "/certs/server.crt")
    tls_key = os.environ.get("TLS_KEY", "/certs/server.key")
    have_tls = os.path.exists(tls_cert) and os.path.exists(tls_key)

    env = dict(os.environ)
    env["PYRONOVA_WORKERS"] = str(per_proc)
    env["PYRONOVA_IO_WORKERS"] = str(io_per_proc)
    env["PYRONOVA_HOST"] = "0.0.0.0"
    env["PYRONOVA_PORT"] = str(base_port)
    # GIL-bridge sizing for gil=True routes under TPC. Default is 4 workers
    # + 16×4=64 channel depth — correct for typical apps with 1-2 numpy
    # routes. HttpArena's async-db / crud profiles hammer gil=True paths
    # at 1024+ concurrency, so a 64-deep channel overflows immediately
    # and every excess request 503s (PyronovaApp's bridge backpressure
    # contract). Widen to 16 workers + 8192 capacity so the DB-heavy
    # gcannon profiles see sustained throughput instead of a 503 storm.
    # Verified locally at c=4096: 15k req/s steady, zero drops.
    env.setdefault("PYRONOVA_GIL_BRIDGE_WORKERS", "16")
    env.setdefault("PYRONOVA_GIL_BRIDGE_CAPACITY", "8192")
    # Metrics / access log off; benchmarks care about throughput, not logs.
    env.pop("PYRONOVA_LOG", None)
    env.pop("PYRONOVA_METRICS", None)
    # Hard-silence the tracing subscriber. Default level is ERROR, which
    # still writes any `tracing::error!` call to stderr — under 4096-conn
    # load a single recurring error log (see the PyObjRef leak bug) drags
    # throughput by ~3× from log-pipe contention alone. OFF makes every
    # tracing macro a zero-cost no-op, matching what Actix / Helidon /
    # ASP.NET ship in their benchmark images.
    env["PYRONOVA_LOG_LEVEL"] = "OFF"

    if have_tls:
        tls_port = base_port + 1
        if tls_port > 65535:
            raise SystemExit(
                f"invalid TLS port: PORT={base_port} leaves no room for the "
                f"TLS companion port {tls_port} (max 65535); use PORT<=65534"
            )
        env["PYRONOVA_TLS_CERT"] = tls_cert
        env["PYRONOVA_TLS_KEY"] = tls_key
        env["PYRONOVA_TLS_PORTS"] = f"{tls_port},8443"
    else:
        env.pop("PYRONOVA_TLS_CERT", None)
        env.pop("PYRONOVA_TLS_KEY", None)
        env.pop("PYRONOVA_TLS_PORTS", None)

    try:
        proc = subprocess.Popen(["python3", "app.py"], env=env)
    except FileNotFoundError:
        raise SystemExit("launcher: 'python3' not found on PATH")
    except OSError as exc:
        raise SystemExit(f"launcher: failed to start 'python3 app.py': {exc}")

    # Guard so repeated SIGTERM/SIGINT don't each spawn a cleanup thread,
    # all racing on terminate()/kill() and os._exit (arc finding launcher-9).
    _shutting_down = threading.Event()

    def shutdown(_sig, _frame):
        # Signal handlers must not block — offload the wait+kill to a thread.
        # First signal wins; later signals are no-ops until exit.
        if _shutting_down.is_set():
            return
        _shutting_down.set()

        def _cleanup():
            import logging as _log
            try:
                proc.terminate()
            except Exception:
                _log.warning("launcher: terminate failed for pid %s", proc.pid, exc_info=True)
            # give it a moment to drain gracefully; Pyronova's graceful
            # shutdown waits up to 30s for in-flight conns — Arena harness
            # typically SIGKILLs the container anyway, but polite is polite.
            time.sleep(1)
            if proc.poll() is None:
                try:
                    proc.kill()
                except Exception:
                    _log.warning("launcher: kill failed for pid %s", proc.pid, exc_info=True)
            # Propagate the child's real exit code rather than masking every
            # shutdown as success. If the child already exited non-zero (e.g.
            # a benchmark crash that itself raised the signal), os._exit(0)
            # would hide that failure from CI (arc finding launcher-8).
            # SIGKILL is delivered asynchronously, so poll() can still return
            # None right after kill() — wait() blocks until the process is
            # actually reaped and yields the true exit code (arc finding
            # launcher-140). Bounded so a wedged child can't hang shutdown.
            try:
                rc = proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                # Child still wedged after SIGKILL + 10s — give up waiting and
                # fall back to a non-blocking poll (likely still None).
                rc = proc.poll()
            except Exception:
                # OSError or other unexpected failures from wait() — log so the
                # cause is visible rather than silently masked.
                _log.warning("launcher: wait() failed during shutdown", exc_info=True)
                rc = proc.poll()
            # os._exit terminates all threads (including this daemon thread);
            # sys.exit(0) from a daemon thread only kills the daemon thread.
            os._exit(1 if rc is None else (128 + abs(rc) if rc < 0 else rc))
        threading.Thread(target=_cleanup, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    import logging as _log
    try:
        rc = proc.wait()
        if rc != 0:
            _log.warning("launcher: process exited with code %d", rc)
        # Propagate child rc so CI/CD pipelines see real failures
        # (arc finding launcher-3). Pre-fix this always returned 0 even
        # when the underlying pyronova process exited non-zero — masking
        # benchmark failures.
        return rc
    except Exception:
        _log.warning("launcher: wait() failed", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
