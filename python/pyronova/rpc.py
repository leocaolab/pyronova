"""Pyronova RPC — MsgPack/JSON/Protobuf content-negotiated RPC over HTTP.

Server: @app.rpc("/method") decorator with auto-decode/encode.
Client: RPCClient with __getattr__ magic for local-like calls.
"""

from __future__ import annotations

import functools
import json
import logging
import inspect
import urllib.parse
from typing import Callable

_log = logging.getLogger("pyronova.rpc")

try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False


class RPCClient:
    """Magic RPC client — call remote methods like local functions.

    Usage::

        client = RPCClient("http://127.0.0.1:8000")
        result = client.get_market_snapshot(tickers=["AAPL", "TSLA"])
    """

    def __init__(self, base_url: str, use_msgpack: bool = True, timeout: float = 30.0):
        try:
            import httpx
        except ImportError as e:
            raise ImportError("RPCClient requires httpx; install with: pip install httpx") from e
        self.base_url = base_url.rstrip("/")
        self.use_msgpack = use_msgpack and HAS_MSGPACK
        self.timeout = timeout
        self._client = httpx.Client(
            http2=False,
            timeout=timeout,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    def __getattr__(self, method_name: str):
        if method_name.startswith("_"):
            raise AttributeError(method_name)

        encoded_name = urllib.parse.quote(method_name, safe="")

        def remote_call(**kwargs):
            if self.use_msgpack and HAS_MSGPACK:
                payload = msgpack.packb(kwargs, use_bin_type=True)
                content_type = "application/msgpack"
            else:
                payload = json.dumps(kwargs).encode("utf-8")
                content_type = "application/json"

            resp = self._client.post(
                f"{self.base_url}/rpc/{encoded_name}",
                content=payload,
                headers={
                    "Content-Type": content_type,
                    "Accept": content_type,
                },
            )
            resp.raise_for_status()

            try:
                if self.use_msgpack and "msgpack" in resp.headers.get("content-type", ""):
                    data = msgpack.unpackb(resp.content, raw=False)
                else:
                    data = resp.json()
            except Exception as e:
                raise RuntimeError(
                    f"RPC {method_name}: failed to decode response "
                    f"(status={resp.status_code}): {e}"
                ) from e

            if not isinstance(data, dict) or not data.get("ok", False):
                err = data.get("error") if isinstance(data, dict) else repr(data)
                raise RuntimeError(
                    f"RPC {method_name} at {self.base_url}: {err}"
                )

            return data.get("result", data)

        return remote_call

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def rpc_decorator(app, path: str, proto_model=None):
    """Create an RPC endpoint with content negotiation.

    Supports MsgPack, JSON, and optional Protobuf.
    Auto-wraps response in {"ok": true, "result": ...} envelope.
    """

    def decorator(fn: Callable) -> Callable:
        is_async = inspect.iscoroutinefunction(fn)

        def _decode_request(req):
            if not req.body:
                return {}
            ct = req.headers.get("content-type", "application/json").lower()
            if HAS_MSGPACK and "msgpack" in ct:
                return msgpack.unpackb(req.body, raw=False)
            elif "protobuf" in ct and proto_model:
                return proto_model().parse(req.body)
            else:
                return json.loads(req.text())

        def _encode_response(result, req):
            accept = req.headers.get("accept", req.headers.get("content-type", "")).lower()
            envelope = {"ok": True, "result": result}

            if HAS_MSGPACK and "msgpack" in accept:
                from pyronova.engine import Response
                body = msgpack.packb(envelope, use_bin_type=True)
                return Response(body=body, content_type="application/msgpack")
            else:
                return envelope  # Framework auto-serializes dict as JSON

        # Check if handler takes 2 args (req, data) or 1 (data).
        # arc finding rpc-1: pre-fix the `>= 2` check failed for
        # 0-param handlers (would call fn(data) → TypeError) and
        # **kwargs-only handlers. Validate at registration so misuse
        # surfaces at decorator time, not at first request.
        sig = inspect.signature(fn)
        positional_or_keyword = [
            p for p in sig.parameters.values()
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                          inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        n_pos = len(positional_or_keyword)
        if n_pos < 1:
            raise TypeError(
                f"RPC handler {fn.__name__!r} must accept at least 1 "
                "positional argument (data) or 2 (req, data); got 0"
            )
        takes_data = n_pos >= 2
        # The wrapper only ever supplies (req, data) or (data). Any
        # *additional* positional-or-keyword param without a default would
        # therefore TypeError at first request, not at registration —
        # check the upper bound too so misuse fails fast (arc finding rpc-43).
        _supplied = 2 if takes_data else 1
        _extra_required = [
            p.name for p in positional_or_keyword[_supplied:]
            if p.default is inspect.Parameter.empty
        ]
        if _extra_required:
            raise TypeError(
                f"RPC handler {fn.__name__!r} declares required positional "
                f"argument(s) {_extra_required} beyond the (req, data) the "
                "RPC dispatcher supplies; give them defaults or remove them"
            )

        # Any uncaught exception in an RPC handler becomes a structured
        # {ok: false, error: ...} envelope so clients don't see a raw 500.
        # The envelope carries ONLY the exception class name — never the
        # exception message — because messages routinely embed filesystem
        # paths, SQL fragments, connection strings, or config values that
        # must not cross the wire to RPC clients. The full message and stack
        # trace are preserved for operators via log.exception below; without
        # that, recurring handler crashes would be invisible on the server.

        def sync_wrapper(req):
            try:
                data = _decode_request(req)
                result = fn(req, data) if takes_data else fn(data)
                return _encode_response(result, req)
            except Exception as e:
                _log.exception("RPC handler %s raised", fn.__qualname__)
                return {"ok": False, "error": type(e).__name__}

        async def async_wrapper(req):
            try:
                data = _decode_request(req)
                result = await (fn(req, data) if takes_data else fn(data))
                return _encode_response(result, req)
            except Exception as e:
                _log.exception("RPC handler %s raised", fn.__qualname__)
                return {"ok": False, "error": type(e).__name__}

        handler = functools.wraps(fn)(async_wrapper if is_async else sync_wrapper)

        # Register as POST route with gil=True (RPC typically needs full Python)
        app._engine.route("POST", path, handler, True)
        return fn

    return decorator
