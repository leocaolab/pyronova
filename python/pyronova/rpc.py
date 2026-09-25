"""Pyronova RPC — MsgPack/JSON/Protobuf content-negotiated RPC over HTTP.

Server: @app.rpc("/method") decorator with auto-decode/encode.
Client: RPCClient with __getattr__ magic for local-like calls.
"""

from __future__ import annotations

import json
import logging
import inspect
import urllib.parse
from enum import Enum, auto
from typing import Callable, NamedTuple

from pyronova import _csrf
from pyronova._errors import log_server_error, server_error_body

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

            # A failed call still answers with the envelope (400: the reason; 500:
            # a request id to quote to the server's operator), so read it first.
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
                rid = data.get("request_id") if isinstance(data, dict) else None
                where = f" (request_id={rid})" if rid else ""
                raise RuntimeError(
                    f"RPC {method_name} at {self.base_url}: HTTP {resp.status_code} {err}{where}"
                )

            if "result" not in data:
                raise RuntimeError(
                    f"RPC {method_name} at {self.base_url}: the reply says ok but has no "
                    f"'result': {data!r}"
                )
            return data["result"]

        return remote_call

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class _Codec(Enum):
    """The body encodings an RPC endpoint speaks."""

    JSON = auto()
    MSGPACK = auto()
    PROTOBUF = auto()


_MEDIA_TYPES = {
    "application/json": _Codec.JSON,
    "application/msgpack": _Codec.MSGPACK,
    "application/x-msgpack": _Codec.MSGPACK,
    "application/protobuf": _Codec.PROTOBUF,
    "application/x-protobuf": _Codec.PROTOBUF,
}


def _accepted_media_types(proto_model) -> dict[str, _Codec]:
    """The request body types this endpoint decodes: JSON always, MsgPack when msgpack is
    installed, Protobuf when the endpoint has a ``proto_model``."""
    available = {_Codec.JSON}
    if HAS_MSGPACK:
        available.add(_Codec.MSGPACK)
    if proto_model is not None:
        available.add(_Codec.PROTOBUF)
    return {media: codec for media, codec in _MEDIA_TYPES.items() if codec in available}


def _reply_codec(req, request_codec: _Codec) -> _Codec:
    """MsgPack when the client asks for it (``Accept``, else the request's own type) and
    msgpack is installed; JSON otherwise."""
    accept = req.headers.get("accept")
    if accept is None:
        wanted = {request_codec}
    else:
        wanted = {_MEDIA_TYPES.get(_csrf.media_type(part)) for part in accept.split(",")}
    return _Codec.MSGPACK if HAS_MSGPACK and _Codec.MSGPACK in wanted else _Codec.JSON


class _Admitted(NamedTuple):
    """A request an endpoint takes: its body's codec and the decoded body."""

    codec: _Codec
    data: object


class _MalformedBody(Exception):
    """The request body could not be decoded: the client's error (400)."""


def _client_error(status: int, reason: object):
    """4xx envelope carrying the reason the request was rejected."""
    from pyronova.engine import Response

    return Response(
        body=json.dumps({"ok": False, "error": str(reason)}),
        status_code=status,
        content_type="application/json",
    )


def _server_error(fn: Callable, req):
    """500 envelope: generic text and the request id; the exception goes to the log with
    the same id (decision D4)."""
    from pyronova.engine import Response

    log_server_error(_log, req.request_id, "RPC handler %s raised", fn.__qualname__)
    return Response(
        body=json.dumps({"ok": False, **server_error_body(req.request_id)}),
        status_code=500,
        content_type="application/json",
    )


def rpc_decorator(app, path: str, proto_model=None):
    """Create an RPC endpoint with content negotiation.

    Supports MsgPack, JSON, and optional Protobuf; the ``Content-Type`` must name one of
    them (415 otherwise), and a browser request from another site is refused 403 unless
    ``app.trusted_origins`` lists it (CSRF). Auto-wraps response in
    {"ok": true, "result": ...} envelope. A body that can't be decoded answers 400
    ``{"ok": false, "error": <reason>}``; a handler that raises answers 500
    ``{"ok": false, "error": "Internal Server Error", "request_id": ...}``.
    """
    accepted = _accepted_media_types(proto_model)

    def decorator(fn: Callable) -> Callable:
        is_async = inspect.iscoroutinefunction(fn)

        def _decode_request(req, codec: _Codec):
            if not req.body:
                return {}
            try:
                match codec:
                    case _Codec.JSON:
                        return req.json()
                    case _Codec.MSGPACK:
                        return msgpack.unpackb(req.body, raw=False)
                    case _Codec.PROTOBUF:
                        return proto_model().parse(req.body)
            except ValueError as e:
                # JSONDecodeError, UnicodeDecodeError and msgpack's decode errors are
                # all ValueErrors.
                raise _MalformedBody(e) from e

        def _encode_response(result, req, codec: _Codec):
            envelope = {"ok": True, "result": result}
            match _reply_codec(req, codec):
                case _Codec.MSGPACK:
                    from pyronova.engine import Response
                    body = msgpack.packb(envelope, use_bin_type=True)
                    return Response(body=body, content_type="application/msgpack")
                case _:
                    return envelope  # Framework auto-serializes dict as JSON

        def _admit(req):
            """The request's codec and decoded body (`_Admitted`), or the 4xx response
            refusing it."""
            codec = _csrf.check(req, app._origin_policy, accepted)
            if isinstance(codec, _csrf.Refused):
                return _client_error(codec.status, codec.reason)
            try:
                return _Admitted(codec, _decode_request(req, codec))
            except _MalformedBody as e:
                return _client_error(400, e.__cause__)

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

        # An exception from the handler never crosses the wire: its message can embed
        # filesystem paths, SQL, connection strings or config values. The client gets a
        # generic 500 with the request id; the operator finds the exception and its
        # traceback on the log line with the same id.

        def sync_wrapper(req):
            admitted = _admit(req)
            if not isinstance(admitted, _Admitted):
                return admitted
            codec, data = admitted
            try:
                result = fn(req, data) if takes_data else fn(data)
                return _encode_response(result, req, codec)
            except Exception:
                return _server_error(fn, req)

        async def async_wrapper(req):
            admitted = _admit(req)
            if not isinstance(admitted, _Admitted):
                return admitted
            codec, data = admitted
            try:
                result = await (fn(req, data) if takes_data else fn(data))
                return _encode_response(result, req, codec)
            except Exception:
                return _server_error(fn, req)

        handler = async_wrapper if is_async else sync_wrapper
        # Name it after fn (sub-interp workers find a route's handler by name),
        # but no __wrapped__: the route calls handler(req), not fn's signature.
        handler.__name__ = fn.__name__
        handler.__qualname__ = fn.__qualname__
        handler.__doc__ = fn.__doc__

        # Register as POST route with gil=True (RPC typically needs full Python)
        app._route("POST", path, handler, gil=True)
        return fn

    return decorator
