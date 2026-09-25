"""Tests for the Rust-side JSON serializer (json.rs / py_to_json_value).

All cases are exercised through the HTTP handler return path so the full
serializer stack is covered without mocking internals.
"""

import collections
import math
import pytest
from pyronova import Pyronova
from pyronova.testing import TestClient


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()


@app.get("/primitives")
def primitives(req):
    return {"none": None, "true": True, "false": False, "int": 42, "float": 3.14, "str": "hello"}


@app.get("/list")
def as_list(req):
    return [1, 2, 3]


@app.get("/tuple")
def as_tuple(req):
    # tuple as a value inside a dict so json.rs serializes it
    return {"t": (1, "two", True)}


@app.get("/big-int-u64")
def big_int_u64(req):
    # u64::MAX — beyond i64::MAX; serialized as a JSON string to preserve precision
    return {"v": 18446744073709551615}


@app.get("/big-int-huge")
def big_int_huge(req):
    # Beyond u64::MAX — serialized as a JSON string to preserve precision
    return {"v": 2**65}


@app.get("/float-key-whole")
def float_key_whole(req):
    # Whole-number float key: Python json.dumps gives "1.0", not "1"
    return {1.0: "v"}


@app.get("/float-key-frac")
def float_key_frac(req):
    return {1.5: "v"}


@app.get("/bool-key")
def bool_key(req):
    # bool keys: "true"/"false", not Python's "True"/"False"
    return {True: 1, False: 0}


@app.get("/none-key")
def none_key(req):
    return {None: "v"}


@app.get("/ordered-dict")
def ordered_dict(req):
    return collections.OrderedDict([("x", 1), ("y", 2)])


@app.get("/set")
def set_handler(req):
    # set duck-typed as iterable → JSON array (order unspecified)
    return {"s": {1, 2, 3}}


@app.get("/frozenset")
def frozenset_handler(req):
    # frozenset duck-typed as iterable → JSON array
    return {"fs": frozenset([1, 2])}


@app.get("/bytes")
def bytes_handler(req):
    # bytes as a value inside a dict — json.rs must reject it → 500
    return {"b": b"hello"}


@app.get("/nan")
def nan_handler(req):
    return {"v": float("nan")}  # must raise TypeError → 500


@app.get("/infinity")
def infinity_handler(req):
    return {"v": float("inf")}  # must raise TypeError → 500


@app.get("/nested-error")
def nested_error(req):
    # set inside a nested structure — duck-typed as array
    return {"users": [1, {1, 2}]}


@app.get("/escape")
def escape(req):
    return {
        "quote": '"',
        "backslash": "\\",
        "newline": "\n",
        "carriage": "\r",
        "tab": "\t",
        "backspace": "\x08",
        "formfeed": "\x0c",
        "nul": "\x00",
        "other_ctrl": "\x1f",
    }


@app.get("/unicode")
def unicode_passthrough(req):
    return {"msg": "你好，世界！🔥"}


@pytest.fixture(scope="module")
def client():
    c = TestClient(app)
    yield c
    c.close()


def test_primitives(client):
    r = client.get("/primitives")
    assert r.status_code == 200
    d = r.json()
    assert d["none"] is None
    assert d["true"] is True
    assert d["false"] is False
    assert d["int"] == 42
    assert abs(d["float"] - 3.14) < 1e-9
    assert d["str"] == "hello"


def test_list(client):
    r = client.get("/list")
    assert r.status_code == 200
    assert r.json() == [1, 2, 3]


def test_tuple_as_array(client):
    r = client.get("/tuple")
    assert r.status_code == 200
    assert r.json() == {"t": [1, "two", True]}


def test_big_int_u64_is_number(client):
    """u64::MAX fits in orjson's unsigned 64-bit range — serialized as a JSON number."""
    r = client.get("/big-int-u64")
    assert r.status_code == 200
    assert r.json()["v"] == 18446744073709551615


def test_big_int_huge_raises_error(client):
    """Integers beyond u64::MAX — orjson raises TypeError → 500."""
    r = client.get("/big-int-huge")
    assert r.status_code == 500


def test_float_key_raises_error(client):
    """Float dict keys are not supported by orjson → 500."""
    r = client.get("/float-key-whole")
    assert r.status_code == 500


def test_float_key_frac_raises_error(client):
    """Float dict keys are not supported by orjson → 500."""
    r = client.get("/float-key-frac")
    assert r.status_code == 500


def test_bool_dict_key_raises_error(client):
    """Bool dict keys are not supported by orjson → 500."""
    r = client.get("/bool-key")
    assert r.status_code == 500


def test_none_dict_key_raises_error(client):
    """None dict keys are not supported by orjson → 500."""
    r = client.get("/none-key")
    assert r.status_code == 500


def test_ordered_dict_via_mapping(client):
    r = client.get("/ordered-dict")
    assert r.status_code == 200
    assert r.json() == {"x": 1, "y": 2}


def test_set_as_array(client):
    """set duck-typed as iterable → JSON array (order unspecified)."""
    r = client.get("/set")
    assert r.status_code == 200
    assert sorted(r.json()["s"]) == [1, 2, 3]


def test_frozenset_as_array(client):
    """frozenset duck-typed as iterable → JSON array."""
    r = client.get("/frozenset")
    assert r.status_code == 200
    assert sorted(r.json()["fs"]) == [1, 2]


def test_bytes_raises_type_error(client):
    r = client.get("/bytes")
    assert r.status_code == 500


def test_nan_becomes_null(client):
    """orjson serializes NaN as null (JSON has no NaN)."""
    r = client.get("/nan")
    assert r.status_code == 200
    assert r.json()["v"] is None


def test_infinity_becomes_null(client):
    """orjson serializes inf as null (JSON has no infinity)."""
    r = client.get("/infinity")
    assert r.status_code == 200
    assert r.json()["v"] is None


def test_nested_set_as_array(client):
    """set inside nested list duck-typed as array."""
    r = client.get("/nested-error")
    assert r.status_code == 200
    data = r.json()
    assert data["users"][0] == 1
    assert sorted(data["users"][1]) == [1, 2]


def test_string_escaping(client):
    """write_str_escaped must produce valid JSON for all special characters."""
    r = client.get("/escape")
    assert r.status_code == 200
    # json.loads round-trip proves each character was correctly escaped
    d = r.json()
    assert d["quote"] == '"'
    assert d["backslash"] == "\\"
    assert d["newline"] == "\n"
    assert d["carriage"] == "\r"
    assert d["tab"] == "\t"
    assert d["backspace"] == "\x08"
    assert d["formfeed"] == "\x0c"
    assert d["nul"] == "\x00"
    assert d["other_ctrl"] == "\x1f"
    # Raw bytes must use the short named escapes, not \u00XX, for the common ones
    raw = r.text
    assert r'\"' in raw
    assert r'\\' in raw
    assert r'\n' in raw
    assert r'\r' in raw
    assert r'\t' in raw
    assert r'\b' in raw
    assert r'\f' in raw


def test_unicode_passthrough(client):
    """Non-ASCII UTF-8 must pass through unescaped (ensure_ascii=False style)."""
    r = client.get("/unicode")
    assert r.status_code == 200
    assert r.json() == {"msg": "你好，世界！🔥"}
    # Characters must appear as UTF-8 bytes, not \uXXXX escapes
    assert "你好" in r.text
