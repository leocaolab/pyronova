"""MCP (Model Context Protocol) server support for Pyronova.

Implements JSON-RPC 2.0 over HTTP at the /mcp endpoint.
AI applications (Claude Desktop, etc.) can discover and invoke tools,
read resources, and use prompt templates.

Usage::

    from pyronova import Pyronova

    app = Pyronova()

    @app.mcp.tool(description="Add two numbers")
    def add(a: int, b: int) -> int:
        return a + b

    @app.mcp.resource("config://app")
    def get_config():
        return {"version": "1.0", "debug": False}

    @app.mcp.prompt("greeting", description="Generate a greeting")
    def greeting(name: str) -> str:
        return f"Hello {name}, how can I help you today?"

    app.run()
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import types
import typing
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Callable, Literal, Union

from pyronova._errors import log_server_error

_log = logging.getLogger(__name__)

# Upper bound on how long a single async tool/resource/prompt handler may
# run before it is abandoned with a timeout error. Without this an
# indefinitely-hanging coroutine blocks the dispatching thread forever
# (arc finding mcp-61).
_ASYNC_HANDLER_TIMEOUT_S = 30.0


def _drive_coro(coro):
    """Run a coroutine to completion from this blocking dispatch thread,
    bounded by ``_ASYNC_HANDLER_TIMEOUT_S``.

    A fresh loop is used because this always runs on a blocking Tokio
    thread that never has its own running asyncio loop.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            asyncio.wait_for(coro, timeout=_ASYNC_HANDLER_TIMEOUT_S)
        )
    finally:
        # On normal return, timeout, or error the coroutine may have spawned
        # child tasks (or wait_for's own cancellation may not have fully
        # propagated). Cancel and await any stragglers, then shut down async
        # generators, before closing — otherwise `loop.close()` discards them
        # in a half-cancelled state and emits "Task was destroyed but it is
        # pending" / "coroutine ignored GeneratorExit" warnings.
        try:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()


def _resolve(result):
    """If a handler returned a coroutine/awaitable, drive it to a value;
    otherwise return it unchanged. Shared by tool/resource/prompt handlers
    so async support is uniform across all three (arc finding mcp-57)."""
    if inspect.iscoroutine(result):
        return _drive_coro(result)
    return result


def _extract_schema(fn: Callable) -> dict:
    """The JSON Schema of ``fn``'s arguments, from its type hints (see ``_hint_schema``).
    A parameter without a hint accepts any JSON value; one whose hint has no JSON form
    is a registration error naming it."""
    sig = inspect.signature(fn)
    # NOTE: cannot use `fn.__annotations__` directly — this module (and any
    # user module handling mcp.tool) commonly has `from __future__ import
    # annotations`, which stores annotations as *strings* (`'int'`). Use
    # typing.get_type_hints to evaluate the strings into real type objects.
    try:
        hints = typing.get_type_hints(fn)
    except NameError as e:
        # A forward ref that doesn't resolve (a TYPE_CHECKING-only import):
        # without the real type there is no schema for it.
        raise TypeError(
            f"MCP tool {fn.__qualname__}: cannot resolve a parameter's type hint "
            f"({e}); import the type at runtime or pass input_schema="
        ) from e
    properties = {}
    required = []

    for name, param in sig.parameters.items():
        if name in ("self", "cls"):
            continue
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        try:
            properties[name] = _hint_schema(hints[name]) if name in hints else {}
        except _NoJsonForm as e:
            raise TypeError(
                f"MCP tool {fn.__qualname__}: parameter {name!r} is annotated {e.hint!r}, "
                "which has no JSON Schema form (supported: bool, int, float, str, None, "
                "Any, list[T], dict[str, T], T | U, Optional[T], Literal[...]); change the "
                "hint or pass input_schema="
            ) from None
        if param.default is inspect.Parameter.empty:
            required.append(name)

    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


_SCALAR_TYPES = {bool: "boolean", int: "integer", float: "number", str: "string", type(None): "null"}


class _NoJsonForm(Exception):
    def __init__(self, hint: object) -> None:
        super().__init__(repr(hint))
        self.hint = hint


def _hint_schema(hint: object) -> dict:
    """The JSON Schema a type hint stands for. ``_NoJsonForm`` for a hint that has none
    (a class, ``set[int]``, ``tuple``, ``dict[int, str]``…)."""
    if hint is Any:
        return {}
    if hint in _SCALAR_TYPES:
        return {"type": _SCALAR_TYPES[hint]}
    origin, args = typing.get_origin(hint), typing.get_args(hint)
    if hint is list or origin is list:
        return {"type": "array", **({"items": _hint_schema(args[0])} if args else {})}
    if hint is dict or origin is dict:
        if not args:
            return {"type": "object"}
        if args[0] is not str:
            raise _NoJsonForm(hint)
        return {"type": "object", "additionalProperties": _hint_schema(args[1])}
    if origin in (Union, types.UnionType):
        return {"anyOf": [_hint_schema(a) for a in args]}
    if origin is Literal:
        if not all(type(v) in _SCALAR_TYPES for v in args):
            raise _NoJsonForm(hint)
        return {"enum": list(args)}
    raise _NoJsonForm(hint)


# JSON Schema's primitive types, as checks on the Python value json.loads produced.
_JSON_TYPE_CHECKS: dict[str, Callable[[object], bool]] = {
    "null": lambda v: v is None,
    "boolean": lambda v: isinstance(v, bool),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "string": lambda v: isinstance(v, str),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
}


def _json_type_name(value: object) -> str:
    return next(
        (name for name in ("null", "boolean", "integer", "number", "string", "array", "object")
         if _JSON_TYPE_CHECKS[name](value)),
        type(value).__name__,
    )


class _Invalid(Exception):
    """An argument value its schema does not accept, with where and why."""


def _check_value(schema: dict, value: object, where: str) -> None:
    """Checks ``value`` against ``schema``: ``type``, ``enum``, ``const``, ``anyOf``,
    ``items``, ``properties``, ``required`` and ``additionalProperties``. Other keywords
    (``format``, ``minimum``…) are annotations here and are not checked."""
    expected = schema.get("type")
    if expected is not None:
        names = expected if isinstance(expected, list) else [expected]
        if not any(_JSON_TYPE_CHECKS[n](value) for n in names):
            raise _Invalid(f"{where}: expected {' or '.join(names)}, got {_json_type_name(value)}")
    if "enum" in schema and value not in schema["enum"]:
        raise _Invalid(f"{where}: {value!r} is not one of {schema['enum']!r}")
    if "const" in schema and value != schema["const"]:
        raise _Invalid(f"{where}: must be {schema['const']!r}, got {value!r}")
    if "anyOf" in schema:
        reasons = []
        for option in schema["anyOf"]:
            try:
                _check_value(option, value, where)
                break
            except _Invalid as e:
                reasons.append(str(e))
        else:
            raise _Invalid(" / ".join(reasons))
    if isinstance(value, list) and "items" in schema:
        for i, item in enumerate(value):
            _check_value(schema["items"], item, f"{where}[{i}]")
    if isinstance(value, dict):
        _check_object(schema, value, where)


def _check_object(schema: dict, value: dict, where: str) -> None:
    missing = [k for k in schema.get("required", ()) if k not in value]
    if missing:
        raise _Invalid(f"{where}: missing required key(s) {missing}")
    properties = schema.get("properties", {})
    extra = schema.get("additionalProperties", True)
    for key, item in value.items():
        if key in properties:
            _check_value(properties[key], item, f"{where}.{key}")
        elif extra is False:
            raise _Invalid(f"{where}: unexpected key {key!r}")
        elif isinstance(extra, dict):
            _check_value(extra, item, f"{where}.{key}")


def _check_arguments(schema: dict, arguments: dict) -> None:
    """Every argument value against its property in the tool's input schema; -32602 with
    the reason for one it does not accept."""
    properties = schema.get("properties", {})
    try:
        for name, value in arguments.items():
            if name in properties:
                _check_value(properties[name], value, f"argument {name!r}")
    except _Invalid as e:
        raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"invalid params: {e}") from None


class JsonRpcCode(IntEnum):
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    # MCP: resources/read of a URI no resource is registered for.
    RESOURCE_NOT_FOUND = -32002


class JsonRpcError(Exception):
    """A request the server rejects, with the code and message the client gets."""

    def __init__(self, code: JsonRpcCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class _Params:
    """What a handler accepts by name, read off its signature at registration."""

    names: frozenset[str]
    required: tuple[str, ...]
    accepts_any: bool  # **kwargs

    @classmethod
    def of(cls, fn: Callable) -> _Params:
        params = list(inspect.signature(fn).parameters.values())
        positional_only = [p.name for p in params if p.kind == inspect.Parameter.POSITIONAL_ONLY]
        if positional_only:
            raise TypeError(
                f"MCP handler {fn.__qualname__} has positional-only parameter(s) "
                f"{positional_only}; MCP arguments are passed by name"
            )
        named = [p for p in params if p.kind in _NAMED]
        return cls(
            names=frozenset(p.name for p in named),
            required=tuple(p.name for p in named if p.default is inspect.Parameter.empty),
            accepts_any=any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params),
        )

    def check(self, arguments: dict) -> None:
        unexpected = [] if self.accepts_any else sorted(set(arguments) - self.names)
        if unexpected:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"unexpected argument(s): {unexpected}")
        missing = [n for n in self.required if n not in arguments]
        if missing:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"missing required argument(s): {missing}")


_NAMED = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)


@dataclass(frozen=True)
class _Tool:
    name: str
    description: str
    input_schema: dict
    params: _Params
    handler: Callable


@dataclass(frozen=True)
class _Resource:
    uri: str
    name: str
    description: str
    mime_type: str
    handler: Callable


@dataclass(frozen=True)
class _Prompt:
    name: str
    description: str
    arguments: list[dict]
    params: _Params
    handler: Callable


class MCPServer:
    """MCP protocol handler — registers tools, resources, and prompts."""

    def __init__(self) -> None:
        self._tools: dict[str, _Tool] = {}
        self._resources: dict[str, _Resource] = {}
        self._prompts: dict[str, _Prompt] = {}

    def is_empty(self) -> bool:
        return not (self._tools or self._resources or self._prompts)

    # ------------------------------------------------------------------
    # Decorators
    # ------------------------------------------------------------------

    def tool(
        self,
        fn: Callable | None = None,
        *,
        name: str | None = None,
        description: str | None = None,
        input_schema: dict | None = None,
    ):
        """Register a tool that AI models can invoke."""

        def register(f: Callable) -> Callable:
            tool_name = name or f.__name__
            if tool_name in self._tools:
                _log.warning(
                    "MCP tool %r is already registered; overwriting the "
                    "previous handler", tool_name,
                )
            self._tools[tool_name] = _Tool(
                name=tool_name,
                description=description or f.__doc__ or "",
                input_schema=input_schema or _extract_schema(f),
                params=_Params.of(f),
                handler=f,
            )
            return f

        if fn is not None:
            return register(fn)
        return register

    def resource(
        self,
        uri: str,
        *,
        name: str | None = None,
        description: str | None = None,
        mime_type: str = "application/json",
    ):
        """Register a readable resource with URI template support."""

        def register(fn: Callable) -> Callable:
            if uri in self._resources:
                _log.warning(
                    "MCP resource %r is already registered; overwriting the "
                    "previous handler", uri,
                )
            self._resources[uri] = _Resource(
                uri=uri,
                name=name or fn.__name__,
                description=description or fn.__doc__ or "",
                mime_type=mime_type,
                handler=fn,
            )
            return fn

        return register

    def prompt(
        self,
        name: str,
        *,
        description: str | None = None,
        arguments: list[dict] | None = None,
    ):
        """Register a prompt template."""

        def register(fn: Callable) -> Callable:
            if name in self._prompts:
                _log.warning(
                    "MCP prompt %r is already registered; overwriting the "
                    "previous handler", name,
                )
            params = _Params.of(fn)
            self._prompts[name] = _Prompt(
                name=name,
                description=description or fn.__doc__ or "",
                arguments=arguments or [
                    {"name": p, "required": p in params.required}
                    for p in inspect.signature(fn).parameters
                    if p in params.names and p not in ("self", "cls")
                ],
                params=params,
                handler=fn,
            )
            return fn

        return register

    # ------------------------------------------------------------------
    # JSON-RPC 2.0 handler
    # ------------------------------------------------------------------

    def handle_request(self, body: str | bytes, request_id: str | None = None) -> str:
        """Process a JSON-RPC 2.0 request and return a response.

        ``request_id`` is the HTTP request's id: an internal error (-32603) reports it
        in ``error.data.request_id`` and logs it with the exception, so the two can be
        matched up (decision D4). Without one (a direct call) the error has no ``data``.
        """
        try:
            req = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            return self._error_response(None, JsonRpcCode.PARSE_ERROR, f"Parse error: {e}")

        # A valid JSON-RPC 2.0 request is an Object; primitives and arrays
        # (the latter reserved for batch requests, which this server does
        # not support) must be rejected with -32600 per the spec.
        if not isinstance(req, dict):
            return self._error_response(None, JsonRpcCode.INVALID_REQUEST, "Invalid Request")

        # JSON-RPC 2.0 §4.2: the "jsonrpc" member MUST be exactly "2.0".
        if req.get("jsonrpc") != "2.0":
            return self._error_response(
                req.get("id"), JsonRpcCode.INVALID_REQUEST, "Invalid Request: jsonrpc must be '2.0'"
            )

        # JSON-RPC 2.0 §4: absence of "id" means this is a notification —
        # the server MUST NOT reply.  We return "" which the HTTP layer
        # converts to a 204-like empty response.
        is_notification = "id" not in req
        req_id = req.get("id")
        method = req.get("method", "")
        # JSON-RPC 2.0 §5.1: when present, `params` MUST be a Structured
        # value (Object or Array). Treat absent as empty dict.
        params = req.get("params", {})
        if not isinstance(params, (dict, list)):
            return self._error_response(
                req_id, JsonRpcCode.INVALID_REQUEST, "Invalid Request: params must be Object or Array"
            )
        # Array (positional) params are structurally valid JSON-RPC, but every
        # method here takes named arguments.
        if isinstance(params, list):
            return self._error_response(
                req_id, JsonRpcCode.INVALID_PARAMS,
                "Invalid params: this server requires named parameters (Object), "
                "positional Array params are not supported",
            )

        handler_map = {
            "initialize": self._handle_initialize,
            "tools/list": self._handle_tools_list,
            "tools/call": self._handle_tools_call,
            "resources/list": self._handle_resources_list,
            "resources/read": self._handle_resources_read,
            "prompts/list": self._handle_prompts_list,
            "prompts/get": self._handle_prompts_get,
        }

        handler = handler_map.get(method)
        if handler is None:
            if is_notification:
                return ""
            return self._error_response(req_id, JsonRpcCode.METHOD_NOT_FOUND, f"Method not found: {method}")

        try:
            result = handler(params)
        except JsonRpcError as e:
            return "" if is_notification else self._error_response(req_id, e.code, e.message)
        except Exception:
            # The full exception (with traceback) goes to the operator log.
            # Do NOT echo str(e) to the client — a handler error can embed
            # file paths, DSNs, or stack fragments that leak internals to an
            # untrusted MCP caller (arc finding mcp-54).
            log_server_error(_log, request_id, "MCP handler %r raised", method)
            return "" if is_notification else self._error_response(
                req_id, JsonRpcCode.INTERNAL_ERROR, "Internal error",
                data=None if request_id is None else {"request_id": request_id},
            )
        if is_notification:
            return ""
        return json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result})

    # ------------------------------------------------------------------
    # Method handlers
    # ------------------------------------------------------------------

    def _handle_initialize(self, params: dict) -> dict:
        return {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {"listChanged": False},
                "resources": {"subscribe": False, "listChanged": False},
                "prompts": {"listChanged": False},
            },
            "serverInfo": {"name": "pyronova-mcp", "version": getattr(__import__("pyronova"), "__version__", "dev")},
        }

    def _handle_tools_list(self, params: dict) -> dict:
        tools = [
            {"name": t.name, "description": t.description, "inputSchema": t.input_schema}
            for t in self._tools.values()
        ]
        return {"tools": tools}

    def _handle_tools_call(self, params: dict) -> dict:
        tool_name = params.get("name", "")
        tool = self._tools.get(tool_name)
        if tool is None:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"Unknown tool: {tool_name}")
        arguments = _arguments_object(params)
        missing = [r for r in tool.input_schema.get("required", []) if r not in arguments]
        if missing:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"missing required argument(s): {missing}")
        tool.params.check(arguments)
        _check_arguments(tool.input_schema, arguments)

        # _resolve awaits a coroutine result on a fresh loop with a timeout
        # (safe — this handler runs on a blocking Tokio thread, never inside
        # an asyncio loop).
        result = _resolve(tool.handler(**arguments))

        # Text as is; any other result as JSON (a list, a number, None), never its repr.
        text = result if isinstance(result, str) else json.dumps(result)
        return {"content": [{"type": "text", "text": text}], "isError": False}

    def _handle_resources_list(self, params: dict) -> dict:
        resources = [
            {"uri": r.uri, "name": r.name, "description": r.description, "mimeType": r.mime_type}
            for r in self._resources.values()
        ]
        return {"resources": resources}

    def _handle_resources_read(self, params: dict) -> dict:
        uri = params.get("uri", "")
        resource = self._resources.get(uri)
        if resource is None:
            raise JsonRpcError(JsonRpcCode.RESOURCE_NOT_FOUND, f"Resource not found: {uri}")

        result = _resolve(resource.handler())
        text = result if isinstance(result, str) else json.dumps(result)
        return {"contents": [{"uri": uri, "mimeType": resource.mime_type, "text": text}]}

    def _handle_prompts_list(self, params: dict) -> dict:
        prompts = [
            {"name": p.name, "description": p.description, "arguments": p.arguments}
            for p in self._prompts.values()
        ]
        return {"prompts": prompts}

    def _handle_prompts_get(self, params: dict) -> dict:
        prompt_name = params.get("name", "")
        prompt = self._prompts.get(prompt_name)
        if prompt is None:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"Unknown prompt: {prompt_name}")
        arguments = _arguments_object(params)
        missing = [a["name"] for a in prompt.arguments if a.get("required") and a["name"] not in arguments]
        if missing:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"missing required argument(s): {missing}")
        prompt.params.check(arguments)

        result = _resolve(prompt.handler(**arguments))
        return {
            "description": prompt.description,
            "messages": [
                {"role": "user", "content": {"type": "text", "text": str(result)}}
            ],
        }

    @staticmethod
    def _error_response(
        req_id: Any, code: JsonRpcCode, message: str, data: dict | None = None
    ) -> str:
        error: dict[str, Any] = {"code": int(code), "message": message}
        if data is not None:
            error["data"] = data
        return json.dumps({"jsonrpc": "2.0", "id": req_id, "error": error})


def _arguments_object(params: dict) -> dict:
    arguments = params.get("arguments", {})
    if not isinstance(arguments, dict):
        raise JsonRpcError(
            JsonRpcCode.INVALID_PARAMS,
            f"arguments must be an object, got {type(arguments).__name__}",
        )
    return arguments
