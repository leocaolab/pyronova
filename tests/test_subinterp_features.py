"""Integration tests for sub-interpreter mode features.

Tests path params, client_ip, and lifecycle hooks in sub-interpreter mode
(the mode where bugs were most likely to occur due to FFI bridging).

These run as subprocess servers to test real sub-interpreter behavior.
"""

import subprocess
import sys
import os
import signal
import time
import json
import urllib.request
import urllib.error

from tests._helpers import bound_port, read_file

PYTHON = sys.executable
PASS = 0
FAIL = 0


def start_server(script_path):
    """Starts the script (it binds port 0); returns the process and the bound port."""
    log_path = script_path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [PYTHON, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    port = bound_port(read_file(log_path), proc)
    for _ in range(50):
        time.sleep(0.1)
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1)
            return proc, port
        except Exception:
            pass
    return proc, port


def stop_server(proc, port):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=5)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            pass
    subprocess.run(f"lsof -ti:{port} | xargs kill -9 2>/dev/null", shell=True)
    time.sleep(0.5)


def http_get(port, path, headers=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, resp.read().decode(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(), dict(e.headers)
    except Exception as e:
        return 0, str(e), {}


def http_post(port, path, body=None, headers=None):
    data = body.encode() if isinstance(body, str) else body
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        headers=headers or {},
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, resp.read().decode(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(), dict(e.headers)
    except Exception as e:
        return 0, str(e), {}


def check(name, condition):
    global PASS, FAIL
    if condition:
        print(f"    ✅ {name}")
        PASS += 1
    else:
        print(f"    ❌ {name}")
        FAIL += 1


# ==========================================================================
# Server script for sub-interpreter mode
# ==========================================================================

SUBINTERP_SERVER = f'''
from pyronova import Pyronova, Response
import json

app = Pyronova()

# --- Lifecycle hooks ---
@app.on_startup
def init():
    app.state["started"] = "true"
    app.state["startup_count"] = "1"

@app.on_startup
def init2():
    count = int(app.state["startup_count"])
    app.state["startup_count"] = str(count + 1)

# --- Routes ---
@app.get("/")
def index(req):
    return {{"ok": True}}

@app.get("/ip")
def get_ip(req):
    return {{"client_ip": req.client_ip}}

@app.post("/ip-post")
def post_ip(req):
    return {{"client_ip": req.client_ip, "method": req.method}}

@app.get("/user/{{id}}")
def get_user(req):
    return {{"user_id": req.params["id"]}}

@app.get("/user/{{id}}/post/{{post_id}}")
def get_user_post(req):
    return {{"user_id": req.params["id"], "post_id": req.params["post_id"]}}

@app.get("/query-and-params/{{name}}")
def query_and_params(req):
    return {{
        "name": req.params["name"],
        "q": req.query_params.get("q", ""),
        "client_ip": req.client_ip,
    }}

@app.get("/all-fields")
def all_fields(req):
    return {{
        "method": req.method,
        "path": req.path,
        "client_ip": req.client_ip,
        "query": req.query,
        "has_headers": len(req.headers) > 0,
    }}

@app.get("/startup-check", gil=True)
def startup_check(req):
    try:
        return {{
            "started": app.state["started"],
            "startup_count": app.state["startup_count"],
        }}
    except KeyError:
        return {{"started": "false", "startup_count": "0"}}

@app.post("/echo-body")
def echo_body(req):
    return req.json()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, mode="subinterp")
'''


def main():
    global PASS, FAIL

    print("=" * 60)
    print("  Sub-interpreter Feature Tests")
    print("=" * 60)

    script = "/tmp/pyronova_test_subinterp_features.py"
    with open(script, "w") as f:
        f.write(SUBINTERP_SERVER)

    proc, port = start_server(script)
    try:
        print("\n  --- PATH PARAMS ---")

        # Single path param
        status, body, _ = http_get(port, "/user/42")
        check("Single path param", json.loads(body).get("user_id") == "42")

        # Nested path params
        status, body, _ = http_get(port, "/user/7/post/99")
        data = json.loads(body)
        check("Nested path params", data.get("user_id") == "7" and data.get("post_id") == "99")

        # Path param with special characters (matchit preserves URL encoding)
        status, body, _ = http_get(port, "/user/hello%20world")
        uid = json.loads(body).get("user_id", "")
        check("URL-encoded path param", uid == "hello%20world" or uid == "hello world")

        # Path param + query param combo
        status, body, _ = http_get(port, "/query-and-params/alice?q=search")
        data = json.loads(body)
        check("Path + query params", data.get("name") == "alice" and data.get("q") == "search")

        print("\n  --- CLIENT IP ---")

        # client_ip on GET
        status, body, _ = http_get(port, "/ip")
        data = json.loads(body)
        check("client_ip on GET", data.get("client_ip") in ("127.0.0.1", "::1"))

        # client_ip on POST
        status, body, _ = http_post(port, "/ip-post",
                                     body='{"test":1}',
                                     headers={"Content-Type": "application/json"})
        data = json.loads(body)
        check("client_ip on POST", data.get("client_ip") in ("127.0.0.1", "::1"))

        # client_ip coexists with path + query params
        status, body, _ = http_get(port, "/query-and-params/bob?q=hello")
        data = json.loads(body)
        check("client_ip with params",
              data.get("client_ip") in ("127.0.0.1", "::1")
              and data.get("name") == "bob"
              and data.get("q") == "hello")

        # All request fields populated
        status, body, _ = http_get(port, "/all-fields?foo=bar")
        data = json.loads(body)
        check("All request fields",
              data.get("method") == "GET"
              and data.get("path") == "/all-fields"
              and data.get("client_ip") in ("127.0.0.1", "::1")
              and "foo=bar" in data.get("query", "")
              and data.get("has_headers") is True)

        print("\n  --- LIFECYCLE HOOKS ---")

        # Startup hooks ran before first request
        status, body, _ = http_get(port, "/startup-check")
        data = json.loads(body)
        check("Startup hook ran", data.get("started") == "true")
        check("Multiple startup hooks", data.get("startup_count") == "2")

        print("\n  --- BODY HANDLING ---")

        # JSON body parsing
        status, body, _ = http_post(port, "/echo-body",
                                     body='{"key":"value","num":42}',
                                     headers={"Content-Type": "application/json"})
        data = json.loads(body)
        check("JSON body echo", data.get("key") == "value" and data.get("num") == 42)

        # Empty body
        status, body, _ = http_post(port, "/echo-body",
                                     body='{}',
                                     headers={"Content-Type": "application/json"})
        check("Empty JSON body", json.loads(body) == {})

    finally:
        stop_server(proc, port)

    print(f"\n{'=' * 60}")
    print(f"  Results: {PASS} passed, {FAIL} failed")
    print(f"{'=' * 60}")

    if FAIL > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
