"""Tests for passive GIL contention monitoring.

The GIL watchdog was replaced with passive measurement: each request
handler records GIL acquisition wait time as a byproduct, eliminating
the active probe thread (zero observer-effect overhead).

Covers:
- get_gil_metrics() returns a Metrics snapshot with typed, named fields
- Probe count increments with real requests
- GIL hold peak tracks CPU-heavy handlers
- Total requests counter works
"""

import os

import pytest
from pyronova import Metrics, Pyronova, get_gil_metrics, reset_peaks
from pyronova.testing import TestClient


@pytest.fixture(scope="module")
def client():
    # TOTAL_REQUESTS counter is gated by PYRONOVA_METRICS=1 (default off
    # to keep the cross-core atomic out of the 5M req/s hot path). Tests
    # that read total_requests need it on; flip before TestClient spawns the
    # server thread so the Rust-side init_metrics_flag() picks it up.
    os.environ["PYRONOVA_METRICS"] = "1"
    app = Pyronova()

    @app.get("/")
    def index(req):
        return {"ok": True}

    @app.get("/heavy")
    def heavy(req):
        """Simulate a handler that holds the GIL for a measurable time."""
        total = 0
        for i in range(200_000):
            total += i
        return {"total": total}

    # Main interpreter: these tests measure how long handlers hold the main GIL.
    c = TestClient(app, mode="gil")
    yield c
    c.close()


def test_metrics_shape():
    """get_gil_metrics() returns a Metrics snapshot of integer counters."""
    m = get_gil_metrics()
    assert isinstance(m, Metrics)
    for name in (
        "gil_wait_last_us",
        "gil_wait_peak_us",
        "gil_wait_count",
        "gil_wait_total_us",
        "gil_queue_length",
        "gil_hold_peak_us",
        "dropped_requests",
        "total_requests",
    ):
        assert isinstance(getattr(m, name), int), name
    assert m.rss_bytes is None or isinstance(m.rss_bytes, int)


def test_probe_count_increments(client):
    """After requests, passive probe count should reflect handler invocations."""
    reset_peaks()

    for _ in range(10):
        resp = client.get("/")
        assert resp.status_code == 200

    probes = get_gil_metrics().gil_wait_count
    assert probes >= 10, f"Expected >= 10 probes, got {probes}"


def test_total_requests_counter(client):
    """TOTAL_REQUESTS counter increments with each request."""
    before = get_gil_metrics().total_requests

    for _ in range(5):
        client.get("/")

    after = get_gil_metrics().total_requests
    assert after >= before + 5, f"Expected at least +5, got {after - before}"


def test_heavy_handler_hold_peak(client):
    """CPU-heavy handler should produce higher GIL hold times."""
    reset_peaks()

    resp = client.get("/heavy")
    assert resp.status_code == 200

    hold_peak_us = get_gil_metrics().gil_hold_peak_us
    # CPU loop should hold GIL for at least a few hundred microseconds
    assert hold_peak_us > 100, f"Expected hold_peak > 100us, got {hold_peak_us}"
