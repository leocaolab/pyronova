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
import typing
from typing import Any, Callable, Optional

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
    """Auto-generate JSON schema from function signature."""
    sig = inspect.signature(fn)
    # NOTE: cannot use `fn.__annotations__` directly — this module (and any
    # user module handling mcp.tool) commonly has `from __future__ import
    # annotations`, which stores annotations as *strings* (`'int'`). A
    # lookup keyed by the `int` type object then silently misses and every
    # argument falls back to "string", breaking the tool schema. Use
    # typing.get_type_hints to evaluate the strings into real type objects.
    try:
        hints = typing.get_type_hints(fn)
    except Exception:
        # A reference in the annotation may not resolve in the caller's
        # scope (forward refs to runtime-only types, TYPE_CHECKING imports).
        # Fall back to raw annotations — the schema degrades to "string"
        # everywhere, which is the same behaviour as before this fix and
        # is at least not a hard crash during registration.
        hints = getattr(fn, "__annotations__", {})
    properties = {}
    required = []

    type_map = {
        int: "integer",
        float: "number",
        str: "string",
        bool: "boolean",
        list: "array",
        dict: "object",
    }

    for name, param in sig.parameters.items():
        if name in ("self", "cls"):
            continue
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        hint = hints.get(name)
        prop = {"type": type_map.get(hint, "string")}
        properties[name] = prop
        if param.default is inspect.Parameter.empty:
            required.append(name)

    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


class MCPServer:
    """MCP protocol handler — registers tools, resources, and prompts."""

    def __init__(self) -> None:
        self._tools: dict[str, dict] = {}
        self._resources: dict[str, dict] = {}
        self._prompts: dict[str, dict] = {}

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
            self._tools[tool_name] = {
                "name": tool_name,
                "description": description or f.__doc__ or "",
                "inputSchema": input_schema or _extract_schema(f),
                "handler": f,
            }
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
            self._resources[uri] = {
                "uri": uri,
                "name": name or fn.__name__,
                "description": description or fn.__doc__ or "",
                "mimeType": mime_type,
                "handler": fn,
            }
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
            self._prompts[name] = {
                "name": name,
                "description": description or fn.__doc__ or "",
                "arguments": arguments or [
                    {"name": p, "required": param.default is inspect.Parameter.empty}
                    for p, param in inspect.signature(fn).parameters.items()
                    # Skip *args / **kwargs — they aren't named MCP arguments
                    # and including them produces a bogus schema (arc mcp-58),
                    # matching _extract_schema's variadic filter.
                    if p not in ("self", "cls")
                    and param.kind not in (
                        inspect.Parameter.VAR_POSITIONAL,
                        inspect.Parameter.VAR_KEYWORD,
                    )
                ],
                "handler": fn,
            }
            return fn

        return register

    # ------------------------------------------------------------------
    # JSON-RPC 2.0 handler
    # ------------------------------------------------------------------

    def handle_request(self, body: str) -> str:
        """Process a JSON-RPC 2.0 request and return a response."""
        try:
            req = json.loads(body)
        except json.JSONDecodeError:
            return self._error_response(None, -32700, "Parse error")

        # A valid JSON-RPC 2.0 request is an Object; primitives and arrays
        # (the latter reserved for batch requests, which this server does
        # not support) must be rejected with -32600 per the spec. Without
        # this guard the .get() calls below raise AttributeError and the
        # exception escapes handle_request entirely.
        if not isinstance(req, dict):
            return self._error_response(None, -32600, "Invalid Request")

        # JSON-RPC 2.0 §4.2: the "jsonrpc" member MUST be exactly "2.0".
        # Reject anything else so we don't silently accept a v1 / malformed
        # client and behave inconsistently (arc finding mcp-60).
        if req.get("jsonrpc") != "2.0":
            return self._error_response(
                req.get("id"), -32600, "Invalid Request: jsonrpc must be '2.0'"
            )

        # JSON-RPC 2.0 §4: absence of "id" means this is a notification —
        # the server MUST NOT reply.  We return "" which the HTTP layer
        # converts to a 204-like empty response.
        is_notification = "id" not in req
        req_id = req.get("id")
        method = req.get("method", "")
        # JSON-RPC 2.0 §5.1: when present, `params` MUST be a Structured
        # value (Object or Array). A non-Structured value (e.g. number,
        # string) is an Invalid Request — without this check, handlers
        # later call `.get()` on a non-dict and crash with AttributeError
        # (arc finding mcp-1). Treat absent as empty dict.
        params = req.get("params", {})
        if not isinstance(params, (dict, list)):
            return self._error_response(req_id, -32600, "Invalid Request: params must be Object or Array")
        # Array (positional) params are a structurally valid JSON-RPC
        # request, but every method handler here consumes named arguments
        # via params.get(...). Passing a list would AttributeError deep in a
        # handler and surface as a generic -32000. Reject up front with the
        # correct -32602 Invalid params instead (arc finding mcp-55).
        if isinstance(params, list):
            return self._error_response(
                req_id, -32602,
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
            return self._error_response(req_id, -32601, f"Method not found: {method}")

        try:
            result = handler(params)
            if is_notification:
                return ""
            return json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result})
        except Exception:
            # The full exception (with traceback) goes to the operator log.
            # Do NOT echo str(e) to the client — a handler error can embed
            # file paths, DSNs, or stack fragments that leak internals to an
            # untrusted MCP caller (arc finding mcp-54). Return a generic
            # message; operators correlate via the logged exception.
            _log.exception("MCP handler %r raised", method)
            if is_notification:
                return ""
            return self._error_response(req_id, -32000, "Internal error")

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
            {
                "name": t["name"],
                "description": t["description"],
                "inputSchema": t["inputSchema"],
            }
            for t in self._tools.values()
        ]
        return {"tools": tools}

    def _handle_tools_call(self, params: dict) -> dict:
        tool_name = params.get("name", "")
        tool = self._tools.get(tool_name)
        if tool is None:
            raise ValueError(f"Unknown tool: {tool_name}")

        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            raise ValueError(f"tool arguments must be an object, got {type(arguments).__name__}")
        # Defense-in-depth: verify the declared required params are present
        # before invoking, so a missing argument yields a clear validation
        # error rather than an opaque TypeError from handler(**arguments)
        # (arc finding mcp-56).
        required = tool.get("inputSchema", {}).get("required", [])
        missing = [r for r in required if r not in arguments]
        if missing:
            raise ValueError(f"missing required argument(s): {missing}")
        handler = tool["handler"]
        # Validate that the handler can actually consume this argument set
        # before splatting, so a signature mismatch (unexpected keys,
        # positional-only params, or missing non-schema params) yields a clear
        # validation error rather than an opaque TypeError (arc finding ISSUE-37).
        try:
            sig = inspect.signature(handler)
        except (ValueError, TypeError):
            sig = None
        if sig is not None:
            sig_params = list(sig.parameters.values())
            accepts_var_kw = any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in sig_params
            )
            positional_only = [
                p.name
                for p in sig_params
                if p.kind == inspect.Parameter.POSITIONAL_ONLY
            ]
            if positional_only:
                raise ValueError(
                    f"tool handler has positional-only parameter(s) that "
                    f"cannot be supplied by name: {positional_only}"
                )
            by_name = {
                p.name
                for p in sig_params
                if p.kind
                in (
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                )
            }
            if not accepts_var_kw:
                unexpected = sorted(set(arguments) - by_name)
                if unexpected:
                    raise ValueError(f"unexpected argument(s): {unexpected}")
            missing_params = [
                p.name
                for p in sig_params
                if p.kind
                in (
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                )
                and p.default is inspect.Parameter.empty
                and p.name not in arguments
            ]
            if missing_params:
                raise ValueError(
                    f"missing required argument(s): {missing_params}"
                )
        # _resolve awaits a coroutine result on a fresh loop with a timeout
        # (safe — this handler runs on a blocking Tokio thread, never inside
        # an asyncio loop).
        result = _resolve(handler(**arguments))

        # Convert result to MCP content format
        if isinstance(result, str):
            content = [{"type": "text", "text": result}]
        elif isinstance(result, dict):
            content = [{"type": "text", "text": json.dumps(result)}]
        else:
            content = [{"type": "text", "text": str(result)}]

        return {"content": content, "isError": False}

    def _handle_resources_list(self, params: dict) -> dict:
        resources = [
            {
                "uri": r["uri"],
                "name": r["name"],
                "description": r["description"],
                "mimeType": r["mimeType"],
            }
            for r in self._resources.values()
        ]
        return {"resources": resources}

    def _handle_resources_read(self, params: dict) -> dict:
        uri = params.get("uri", "")
        resource = self._resources.get(uri)
        if resource is None:
            raise ValueError(f"Unknown resource: {uri}")

        result = _resolve(resource["handler"]())
        if isinstance(result, str):
            text = result
        else:
            text = json.dumps(result)

        return {
            "contents": [
                {"uri": uri, "mimeType": resource["mimeType"], "text": text}
            ]
        }

    def _handle_prompts_list(self, params: dict) -> dict:
        prompts = [
            {
                "name": p["name"],
                "description": p["description"],
                "arguments": p["arguments"],
            }
            for p in self._prompts.values()
        ]
        return {"prompts": prompts}

    def _handle_prompts_get(self, params: dict) -> dict:
        prompt_name = params.get("name", "")
        prompt = self._prompts.get(prompt_name)
        if prompt is None:
            raise ValueError(f"Unknown prompt: {prompt_name}")

        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            raise ValueError(f"prompt arguments must be an object, got {type(arguments).__name__}")
        # Defense-in-depth: mirror _handle_tools_call — verify declared
        # required arguments are present before invoking, so a missing
        # argument yields a clear validation error rather than an opaque
        # TypeError from handler(**arguments) (arc finding mcp-56).
        missing = [
            a["name"]
            for a in prompt["arguments"]
            if a.get("required") and a["name"] not in arguments
        ]
        if missing:
            raise ValueError(f"missing required argument(s): {missing}")
        result = _resolve(prompt["handler"](**arguments))

        return {
            "description": prompt["description"],
            "messages": [
                {"role": "user", "content": {"type": "text", "text": str(result)}}
            ],
        }

    @staticmethod
    def _error_response(req_id: Any, code: int, message: str) -> str:
        return json.dumps({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": code, "message": message},
        })
