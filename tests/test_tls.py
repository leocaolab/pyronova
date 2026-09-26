"""End-to-end TLS tests — start a Pyronova server with a self-signed cert,
issue requests with the Python stdlib (`http.client` + `ssl`), verify the
response.

The cert is generated fresh per test session via `openssl`. If openssl
isn't on PATH the whole module is skipped. TLS test coverage is
intentionally minimal: mostly we're verifying that the rustls pipeline
serves plain request/response correctly and that misconfiguration raises
a clear Python error.
"""

import http.client
import shutil
import ssl
import subprocess
import threading
import time
from pathlib import Path

import pytest

from tests._helpers import poll_until

from pyronova import Pyronova


if shutil.which("openssl") is None:
    pytest.skip("openssl CLI not available", allow_module_level=True)


@pytest.fixture(scope="module")
def cert_key(tmp_path_factory):
    d = tmp_path_factory.mktemp("pyronova_tls")
    cert = d / "cert.pem"
    key = d / "key.pem"
    # 2048-bit RSA is the cheapest cert openssl generates reliably on all
    # distros; this test doesn't care about key strength, just correctness.
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-out", str(cert),
            "-days", "1", "-subj", "/CN=localhost",
        ],
        check=True,
        capture_output=True,
    )
    return cert, key


@pytest.fixture(scope="module")
def tls_server(cert_key):
    cert, key = cert_key

    app = Pyronova()

    @app.get("/")
    def root(req):
        return {"tls": True}

    @app.get("/echo/{name}")
    def echo(req):
        return {"name": req.params["name"]}

    t = threading.Thread(
        target=lambda: app.run(
            host="127.0.0.1", port=0, mode="default",
            tls_cert=str(cert), tls_key=str(key),
        ),
        daemon=True,
    )
    t.start()
    # Bound on port 0: the app's server reports the port it got.
    port = poll_until(lambda: [srv.port for srv in list(app._servers)], what="TLS bind")[0]

    # Wait for TLS listener. Plain TCP connect succeeds the instant the
    # listener binds; we need to actually complete a TLS handshake to
    # know the server is serving.
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    for _ in range(50):
        time.sleep(0.1)
        try:
            conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=1, context=ctx)
            conn.request("GET", "/")
            conn.getresponse().read()
            conn.close()
            break
        except Exception:
            continue
    else:
        pytest.fail("TLS server did not start within 5s")

    yield port, ctx


def test_https_get_returns_200(tls_server):
    port, ctx = tls_server
    conn = http.client.HTTPSConnection("127.0.0.1", port, context=ctx)
    conn.request("GET", "/")
    resp = conn.getresponse()
    body = resp.read()
    conn.close()
    assert resp.status == 200
    assert body == b'{"tls":true}'


def test_path_params_under_tls(tls_server):
    port, ctx = tls_server
    conn = http.client.HTTPSConnection("127.0.0.1", port, context=ctx)
    conn.request("GET", "/echo/alice")
    resp = conn.getresponse()
    body = resp.read()
    conn.close()
    assert resp.status == 200
    assert body == b'{"name":"alice"}'


def test_http_on_tls_port_fails(tls_server):
    """A plain HTTP request to the TLS port must not succeed — rustls
    should reject the malformed TLS handshake and close the connection."""
    port, _ = tls_server
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    with pytest.raises(Exception):
        conn.request("GET", "/")
        conn.getresponse().read()
    conn.close()


def test_only_cert_raises(cert_key, tmp_path):
    """Passing tls_cert without tls_key is a config error."""
    cert, _ = cert_key
    app = Pyronova()
    # run() must raise before accepting any connection
    with pytest.raises((ValueError, TypeError)):
        app.run(
            host="127.0.0.1", port=0,
            tls_cert=str(cert),  # tls_key missing
        )


def test_missing_cert_file_raises(tmp_path):
    """Non-existent cert path should raise a clear ValueError."""
    app = Pyronova()
    with pytest.raises((ValueError, OSError)):
        app.run(
            host="127.0.0.1", port=0,
            tls_cert=str(tmp_path / "missing.pem"),
            tls_key=str(tmp_path / "missing.pem"),
        )
