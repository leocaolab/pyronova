"""Tests for MCP (Model Context Protocol) server."""

import json
import pytest
from pyronova.mcp import MCPServer, _extract_schema


# -- Schema extraction tests --------------------------------------------------

def test_extract_schema_basic():
    def add(a: int, b: int) -> int:
        return a + b
    schema = _extract_schema(add)
    assert schema["type"] == "object"
    assert schema["properties"]["a"]["type"] == "integer"
    assert schema["properties"]["b"]["type"] == "integer"
    assert set(schema["required"]) == {"a", "b"}


def test_extract_schema_optional():
    def greet(name: str, greeting: str = "hello"):
        pass
    schema = _extract_schema(greet)
    assert schema["required"] == ["name"]
    assert "greeting" not in schema.get("required", [])


def test_extract_schema_various_types():
    def fn(a: int, b: float, c: str, d: bool, e: list, f: dict):
        pass
    schema = _extract_schema(fn)
    assert schema["properties"]["a"]["type"] == "integer"
    assert schema["properties"]["b"]["type"] == "number"
    assert schema["properties"]["c"]["type"] == "string"
    assert schema["properties"]["d"]["type"] == "boolean"
    assert schema["properties"]["e"]["type"] == "array"
    assert schema["properties"]["f"]["type"] == "object"


# -- MCP protocol tests -------------------------------------------------------

@pytest.fixture
def mcp():
    server = MCPServer()

    @server.tool(description="Add two numbers")
    def add(a: int, b: int) -> int:
        return a + b

    @server.resource("config://app", description="App config")
    def get_config():
        return {"version": "1.0"}

    @server.prompt("greeting", description="Greet user")
    def greeting(name: str) -> str:
        return f"Hello {name}!"

    return server


def _rpc(mcp, method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}})
    return json.loads(mcp.handle_request(body))


def test_initialize(mcp):
    resp = _rpc(mcp, "initialize")
    assert resp["result"]["protocolVersion"] == "2024-11-05"
    assert "tools" in resp["result"]["capabilities"]


def test_tools_list(mcp):
    resp = _rpc(mcp, "tools/list")
    tools = resp["result"]["tools"]
    assert len(tools) == 1
    assert tools[0]["name"] == "add"
    assert tools[0]["description"] == "Add two numbers"


def test_tools_call(mcp):
    resp = _rpc(mcp, "tools/call", {"name": "add", "arguments": {"a": 3, "b": 5}})
    content = resp["result"]["content"]
    assert content[0]["text"] == "8"
    assert resp["result"]["isError"] is False


def test_tools_call_unknown():
    server = MCPServer()
    resp = _rpc(server, "tools/call", {"name": "nonexistent"})
    assert "error" in resp
    assert resp["error"]["code"] == -32602


def test_resources_list(mcp):
    resp = _rpc(mcp, "resources/list")
    resources = resp["result"]["resources"]
    assert len(resources) == 1
    assert resources[0]["uri"] == "config://app"


def test_resources_read(mcp):
    resp = _rpc(mcp, "resources/read", {"uri": "config://app"})
    contents = resp["result"]["contents"]
    assert json.loads(contents[0]["text"]) == {"version": "1.0"}


def test_resources_read_unknown(mcp):
    resp = _rpc(mcp, "resources/read", {"uri": "config://missing"})
    assert "error" in resp


def test_prompts_list(mcp):
    resp = _rpc(mcp, "prompts/list")
    prompts = resp["result"]["prompts"]
    assert len(prompts) == 1
    assert prompts[0]["name"] == "greeting"


def test_prompts_get(mcp):
    resp = _rpc(mcp, "prompts/get", {"name": "greeting", "arguments": {"name": "Alice"}})
    msg = resp["result"]["messages"][0]
    assert msg["role"] == "user"
    assert "Hello Alice!" in msg["content"]["text"]


def test_method_not_found(mcp):
    resp = _rpc(mcp, "nonexistent/method")
    assert resp["error"]["code"] == -32601


def test_parse_error(mcp):
    resp = json.loads(mcp.handle_request("not json"))
    assert resp["error"]["code"] == -32700
