"""Property tests for the parsers that take client input: the multipart/form-data parser,
RFC 8187 ``filename*`` decoding, and the MCP request / argument validator."""

from __future__ import annotations

import json
from typing import Literal
from urllib.parse import quote_from_bytes

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from pyronova.mcp import JsonRpcCode, MCPServer  # noqa: E402
from pyronova.uploads import MultipartError, UploadFile, parse_multipart  # noqa: E402

PROPERTY_SETTINGS = settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])


class FakeRequest:
    """What `parse_multipart` reads off a request: the Content-Type header and the body."""

    def __init__(self, content_type: str, body: bytes):
        self.headers = {"content-type": content_type}
        self.body = body


# ---------------------------------------------------------------------------
# multipart/form-data
# ---------------------------------------------------------------------------

# RFC 2046 §5.1.1 bchars, without the space (a boundary may not end in one).
BCHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'()+_,-./:=?"
boundaries = st.text(alphabet=BCHARS, min_size=1, max_size=70)
# A field name or filename sent as a quoted-string: any printable text but CR/LF.
header_texts = st.text(
    alphabet=st.characters(blacklist_categories=("Cs", "Cc")), min_size=1, max_size=20
)
payloads = st.binary(max_size=200) | st.sampled_from(
    [b"", b"\r\n", b"\n", b"line\r\n\r\n", b"--", b"\r\n--", b"trailing\r\n"]
)
parts = st.lists(
    st.tuples(header_texts, st.none() | header_texts, payloads), min_size=1, max_size=5
)


def _quoted(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _encode(boundary: str, fields, nl: bytes) -> bytes:
    out = b""
    for name, filename, data in fields:
        disposition = f"form-data; name={_quoted(name)}"
        if filename is not None:
            disposition += f"; filename={_quoted(filename)}"
        out += b"--" + boundary.encode() + nl
        out += f"Content-Disposition: {disposition}".encode() + nl + nl
        out += data + nl
    return out + b"--" + boundary.encode() + b"--" + nl


def _expected(fields) -> dict:
    result: dict = {}
    for name, filename, data in fields:
        upload = UploadFile(
            name=name,
            filename=filename,
            content_type="application/octet-stream" if filename else "text/plain",
            data=data,
        )
        existing = result.get(name)
        if existing is None:
            result[name] = upload
        elif isinstance(existing, list):
            existing.append(upload)
        else:
            result[name] = [existing, upload]
    return result


@PROPERTY_SETTINGS
@given(boundary=boundaries, fields=parts, nl=st.sampled_from([b"\r\n", b"\n"]))
def test_multipart_round_trips_every_part_byte_for_byte(boundary, fields, nl):
    delimiter = nl + b"--" + boundary.encode()
    hypothesis.assume(all(delimiter not in nl + data for _, _, data in fields))
    body = _encode(boundary, fields, nl)
    req = FakeRequest(f"multipart/form-data; boundary={_quoted(boundary)}", body)
    assert parse_multipart(req) == _expected(fields)


@PROPERTY_SETTINGS
@given(boundary=boundaries, body=st.binary(max_size=400))
def test_multipart_garbage_is_a_multipart_error_or_a_parse(boundary, body):
    req = FakeRequest(f"multipart/form-data; boundary={boundary}", body)
    try:
        form = parse_multipart(req)
    except MultipartError:
        return
    for value in form.values():
        for upload in value if isinstance(value, list) else [value]:
            assert isinstance(upload, UploadFile) and upload.name


@PROPERTY_SETTINGS
@given(boundary=boundaries, fields=parts, cut=st.integers(min_value=0))
def test_multipart_truncated_body_never_escapes_as_another_error(boundary, fields, cut):
    body = _encode(boundary, fields, b"\r\n")
    req = FakeRequest(f"multipart/form-data; boundary={_quoted(boundary)}", body[: cut % (len(body) + 1)])
    try:
        parse_multipart(req)
    except MultipartError:
        pass


# ---------------------------------------------------------------------------
# RFC 8187 filename*
# ---------------------------------------------------------------------------

languages = st.text(alphabet="abcdefghijklmnopqrstuvwxyz-", max_size=8)


def _filename_star_form(value: str, filename: str | None = None) -> bytes:
    disposition = f"form-data; name=\"f\"; filename*={value}"
    if filename is not None:
        disposition += f"; filename={_quoted(filename)}"
    return (
        b"--b\r\nContent-Disposition: " + disposition.encode() + b"\r\n\r\ndata\r\n--b--\r\n"
    )


def _filename(value: str, filename: str | None = None) -> str | None:
    form = parse_multipart(FakeRequest("multipart/form-data; boundary=b", _filename_star_form(value, filename)))
    return form["f"].filename


@PROPERTY_SETTINGS
@given(name=st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=30), lang=languages)
def test_filename_star_utf8_decodes_to_the_name_sent(name, lang):
    encoded = quote_from_bytes(name.encode("utf-8"), safe="")
    assert _filename(f"UTF-8'{lang}'{encoded}") == name


@PROPERTY_SETTINGS
@given(name=st.text(alphabet=st.characters(max_codepoint=0xFF, blacklist_categories=("Cs",)), max_size=30))
def test_filename_star_latin1_decodes_to_the_name_sent(name):
    encoded = quote_from_bytes(name.encode("latin-1"), safe="")
    assert _filename(f"iso-8859-1''{encoded}") == name


@PROPERTY_SETTINGS
@given(name=st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=30), plain=header_texts)
def test_filename_star_wins_over_filename(name, plain):
    encoded = quote_from_bytes(name.encode("utf-8"), safe="")
    assert _filename(f"utf-8''{encoded}", filename=plain) == name


@PROPERTY_SETTINGS
@given(value=st.text(alphabet=st.characters(blacklist_categories=("Cs", "Cc"), blacklist_characters=';"\\'), max_size=40))
def test_filename_star_any_value_decodes_or_is_a_multipart_error(value):
    try:
        name = _filename(value)
    except MultipartError:
        return
    assert isinstance(name, str)


# ---------------------------------------------------------------------------
# MCP: request shape and tool arguments
# ---------------------------------------------------------------------------

json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False, allow_infinity=False) | st.text(max_size=10),
    lambda children: st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=6), children, max_size=4),
    max_leaves=12,
)

mcp = MCPServer()


@mcp.tool()
def typed(
    count: int,
    label: str,
    ids: list[int],
    weights: dict[str, float | None],
    mode: Literal["a", "b"] = "a",
) -> str:
    return "ok"


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _accepts(args: dict) -> bool:
    """What `typed`'s hints admit, written independently of the validator."""
    if not {"count", "label", "ids", "weights"} <= set(args) or not set(args) <= {
        "count", "label", "ids", "weights", "mode"
    }:
        return False
    return (
        _is_int(args["count"])
        and isinstance(args["label"], str)
        and isinstance(args["ids"], list)
        and all(_is_int(i) for i in args["ids"])
        and isinstance(args["weights"], dict)
        and all(w is None or _is_number(w) for w in args["weights"].values())
        and args.get("mode", "a") in ("a", "b")
    )


def _call(arguments) -> dict:
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "typed", "arguments": arguments}}
    )
    return json.loads(mcp.handle_request(body))


well_typed = st.fixed_dictionaries(
    {
        "count": st.integers(),
        "label": st.text(max_size=10),
        "ids": st.lists(st.integers(), max_size=4),
        "weights": st.dictionaries(st.text(max_size=5), st.none() | st.floats(allow_nan=False, allow_infinity=False) | st.integers(), max_size=3),
    },
    optional={"mode": st.sampled_from(["a", "b"])},
)
field_names = st.sampled_from(["count", "label", "ids", "weights", "mode", "other"])


@PROPERTY_SETTINGS
@given(args=well_typed)
def test_mcp_accepts_every_well_typed_call(args):
    assert _call(args)["result"]["content"][0]["text"] == "ok"


@PROPERTY_SETTINGS
@given(args=well_typed, field=field_names, value=json_values)
def test_mcp_argument_verdict_matches_the_hints(args, field, value):
    args = {**args, field: value}
    reply = _call(args)
    if _accepts(args):
        assert "result" in reply, reply
    else:
        assert reply["error"]["code"] == JsonRpcCode.INVALID_PARAMS, reply


@PROPERTY_SETTINGS
@given(request=json_values | st.dictionaries(st.sampled_from(["jsonrpc", "id", "method", "params"]), json_values))
def test_mcp_any_json_request_gets_a_json_rpc_reply_not_an_internal_error(request):
    reply = mcp.handle_request(json.dumps(request))
    if reply == "":
        return  # a notification
    parsed = json.loads(reply)
    assert parsed["jsonrpc"] == "2.0"
    if "error" in parsed:
        assert parsed["error"]["code"] != JsonRpcCode.INTERNAL_ERROR, parsed
