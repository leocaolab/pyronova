"""Helpers shared by the server tests: deadline polling, and the port a server bound.

Every test server binds port 0 and the test reads back the port the kernel picked, so
suites can run concurrently: `TestClient.port` in-process, `bound_port` for a subprocess
server (from its "Listening on" startup line).
"""

from __future__ import annotations

import re
import subprocess
import time
from typing import Callable, TypeVar

T = TypeVar("T")

# The startup line every serving path prints once its sockets are bound, e.g.
# "  Listening on http://127.0.0.1:52341" (an IPv6 host is bracketed).
LISTENING = re.compile(r"Listening on (?P<scheme>https?)://(?P<host>\[[^\]]+\]|[^\s:/]+):(?P<port>\d+)")

POLL_INTERVAL_S = 0.05
STARTUP_TIMEOUT_S = 30.0


def poll_until(
    fetch: Callable[[], T],
    *,
    timeout: float = 5.0,
    interval: float = POLL_INTERVAL_S,
    what: str = "the condition",
) -> T:
    """Calls `fetch` until it returns a truthy value, and returns that value.

    Raises `TimeoutError` naming `what` and the last value `fetch` returned when
    `timeout` seconds pass first.
    """
    deadline = time.monotonic() + timeout
    while True:
        value = fetch()
        if value:
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"{what} not met within {timeout}s; last value: {value!r}")
        time.sleep(interval)


def settle(
    fetch: Callable[[], T],
    done: Callable[[T], bool] = bool,
    *,
    timeout: float = 5.0,
    interval: float = POLL_INTERVAL_S,
) -> T:
    """Calls `fetch` until `done` accepts its value or `timeout` seconds pass, and returns
    the last value either way: for a check that asserts on what arrived (a server's log
    writer is non-blocking, so a line may land after the response it belongs to)."""
    deadline = time.monotonic() + timeout
    while True:
        value = fetch()
        if done(value) or time.monotonic() > deadline:
            return value
        time.sleep(interval)


def lines_with(read_output: Callable[[], str], needle: str, *, timeout: float = 5.0) -> list[str]:
    """The lines of the output that contain `needle`, once at least one is there."""
    return settle(
        lambda: [line for line in read_output().splitlines() if needle in line],
        timeout=timeout,
    )


def listening_ports(output: str) -> list[int]:
    """The ports of every "Listening on" line in `output`, in order."""
    return [int(m["port"]) for m in LISTENING.finditer(output)]


def bound_port(
    read_output: Callable[[], str],
    proc: subprocess.Popen | None = None,
    *,
    timeout: float = STARTUP_TIMEOUT_S,
) -> int:
    """The port a server started with port 0 bound: the first "Listening on" line of the
    output `read_output` returns.

    Raises `RuntimeError` carrying the output when `proc` exits first or `timeout`
    seconds pass without the line.
    """
    deadline = time.monotonic() + timeout
    while True:
        output = read_output()
        ports = listening_ports(output)
        if ports:
            return ports[0]
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(
                f"server exited with {proc.returncode} before listening:\n{output[-4000:]}"
            )
        if time.monotonic() > deadline:
            raise RuntimeError(f"server did not listen within {timeout}s:\n{output[-4000:]}")
        time.sleep(POLL_INTERVAL_S)


def read_file(path: str) -> Callable[[], str]:
    """A `read_output` for a server whose output goes to the file at `path`."""

    def read() -> str:
        try:
            with open(path, errors="replace") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    return read
