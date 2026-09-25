"""Tests for Rust-side py_to_json_value: context-based architecture.

Covers all PyJsonError variants, path tracking, duck typing, and edge cases:
- Primitives (None, bool, int, float, str)
- BigInt precision preservation
- NaN/Inf rejection with JSON Path
- Bool/Int isolation via cast_exact
- Tuple → JSON array
- Circular reference detection with path
- Max depth enforcement with path
- Unsupported type rejection with path
- Dict key coercion (str/int/float/bool/None)
- Non-coercible dict keys rejection with path
- Surrogate string handling
- Duck typing: defaultdict, OrderedDict, deque
- Repeated (non-circular) references
- Nested error path accuracy
- Server resilience after errors
"""

import pytest
from collections import defaultdict, OrderedDict, deque
from pyronova import Pyronova
from pyronova.testing import TestClient


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Pyronova()


@app.get("/")
def health(req):
    return {"ok": True}


# --- Primitives ---

@app.get("/none")
def return_none(req):
    return None


@app.get("/bool/true")
def return_true(req):
    return {"v": True}


@app.get("/bool/false")
def return_false(req):
    return {"v": False}


@app.get("/int/zero")
def return_zero(req):
    return {"v": 0}


@app.get("/int/negative")
def return_neg(req):
    return {"v": -42}


@app.get("/int/max64")
def return_max64(req):
    return {"v": 2**63 - 1}


@app.get("/int/min64")
def return_min64(req):
    return {"v": -(2**63)}


@app.get("/int/bigint")
def return_bigint(req):
    return {"v": 2**63}


@app.get("/int/bigint_neg")
def return_bigint_neg(req):
    return {"v": -(2**63) - 1}


@app.get("/int/huge")
def return_huge(req):
    return {"v": 10**100}


@app.get("/float/normal")
def return_float(req):
    return {"v": 3.14}


@app.get("/float/zero")
def return_float_zero(req):
    return {"v": 0.0}


@app.get("/float/neg")
def return_float_neg(req):
    return {"v": -2.718}


@app.get("/float/nan")
def return_nan(req):
    return {"v": float("nan")}


@app.get("/float/inf")
def return_inf(req):
    return {"v": float("inf")}


@app.get("/float/neg_inf")
def return_neg_inf(req):
    return {"v": float("-inf")}


@app.get("/string/empty")
def return_empty_str(req):
    return {"v": ""}


@app.get("/string/unicode")
def return_unicode(req):
    return {"v": "你好世界 🔥"}


@app.get("/string/special")
def return_special(req):
    return {"v": 'tab\there\nnewline\r\n"quotes"\\backslash'}


@app.get("/string/surrogate")
def return_surrogate(req):
    s = "hello\ud800world"
    return {"v": s}


# --- Bool/Int isolation ---

@app.get("/bool_in_list")
def return_bool_in_list(req):
    return {"v": [True, False, 1, 0]}


@app.get("/bool_int_dict")
def return_bool_int_dict(req):
    return {"bool_true": True, "bool_false": False, "int_one": 1, "int_zero": 0}


# --- Tuple ---

@app.get("/tuple/simple")
def return_tuple(req):
    return {"v": (1, 2, 3)}


@app.get("/tuple/mixed")
def return_tuple_mixed(req):
    return {"v": (1, "two", 3.0, True, None)}


@app.get("/tuple/nested")
def return_tuple_nested(req):
    return {"v": ((1, 2), (3, 4))}


@app.get("/tuple/empty")
def return_tuple_empty(req):
    return {"v": ()}


@app.get("/tuple/in_list")
def return_tuple_in_list(req):
    return {"v": [(1, 2), [3, 4]]}


# --- List ---

@app.get("/list/empty")
def return_list_empty(req):
    return {"v": []}


@app.get("/list/nested")
def return_list_nested(req):
    return {"v": [[1, 2], [3, [4, 5]]]}


# --- Dict ---

@app.get("/dict/empty")
def return_dict_empty(req):
    return {}


@app.get("/dict/nested")
def return_dict_nested(req):
    return {"a": {"b": {"c": 1}}}


@app.get("/dict/mixed_values")
def return_dict_mixed(req):
    return {
        "str": "hello",
        "int": 42,
        "float": 1.5,
        "bool": True,
        "none": None,
        "list": [1, 2],
        "tuple": (3, 4),
        "dict": {"nested": True},
    }


# --- Dict key coercion ---

@app.get("/dict/int_keys")
def return_dict_int_keys(req):
    return {1: "one", 2: "two"}


@app.get("/dict/bool_keys")
def return_dict_bool_keys(req):
    return {True: "yes", False: "no"}


@app.get("/dict/none_key")
def return_dict_none_key(req):
    return {None: "nothing"}


@app.get("/dict/float_key")
def return_dict_float_key(req):
    return {3.14: "pi"}


@app.get("/dict/unsupported_key")
def return_dict_unsupported_key(req):
    return {(1, 2): "tuple key"}


@app.get("/dict/nan_key")
def return_dict_nan_key(req):
    return {float("nan"): "bad"}


# --- Circular reference ---

@app.get("/circular/list")
def return_circular_list(req):
    a = [1, 2]
    a.append(a)
    return {"v": a}


@app.get("/circular/dict")
def return_circular_dict(req):
    d = {}
    d["self"] = d
    return d


# --- Repeated (non-circular) references ---

@app.get("/repeated_ref")
def return_repeated_ref(req):
    shared = [1, 2, 3]
    return {"a": shared, "b": shared}


# --- Unsupported types ---

@app.get("/unsupported/set")
def return_set(req):
    return {"v": {1, 2, 3}}


@app.get("/unsupported/bytes")
def return_bytes(req):
    return {"v": b"hello"}


@app.get("/unsupported/complex")
def return_complex(req):
    return {"v": 1 + 2j}


@app.get("/unsupported/custom_obj")
def return_custom_obj(req):
    class Foo:
        pass
    return {"v": Foo()}


# --- Deep nesting ---

@app.get("/deep/ok")
def return_deep_ok(req):
    d = {"v": 42}
    for _ in range(50):
        d = {"nested": d}
    return d


@app.get("/deep/exceed")
def return_deep_exceed(req):
    d = {"v": 42}
    for _ in range(300):
        d = {"nested": d}
    return d


# --- Path tracking: nested errors ---

@app.get("/path/nested_nan")
def return_nested_nan(req):
    return {"users": [{"name": "alice", "score": float("nan")}]}


@app.get("/path/nested_unsupported")
def return_nested_unsupported(req):
    return {"data": {"items": [1, 2, 3+4j]}}


@app.get("/path/deep_circular")
def return_deep_circular(req):
    inner = []
    inner.append(inner)
    return {"a": {"b": [inner]}}


# --- Duck typing: defaultdict, OrderedDict, deque ---

@app.get("/duck/defaultdict")
def return_defaultdict(req):
    d = defaultdict(list)
    d["x"].append(1)
    d["y"].append(2)
    return dict(d)  # Convert to dict for now; duck typing test below


@app.get("/duck/ordereddict")
def return_ordereddict(req):
    d = OrderedDict()
    d["first"] = 1
    d["second"] = 2
    d["third"] = 3
    return d


@app.get("/duck/deque")
def return_deque(req):
    return {"v": deque([1, 2, 3, 4, 5])}


@app.get("/duck/deque_nested")
def return_deque_nested(req):
    return {"v": deque([(1, 2), deque([3, 4])])}


@app.get("/duck/defaultdict_raw")
def return_defaultdict_raw(req):
    d = defaultdict(int)
    d["a"] = 10
    d["b"] = 20
    return d  # Return raw defaultdict, not converted


@pytest.fixture(scope="module")
def client():
    c = TestClient(app, port=19895)
    yield c
    c.close()


# ========================
# Primitive tests
# ========================

class TestPrimitives:
    def test_none_returns_empty(self, client):
        resp = client.get("/none")
        assert resp.status_code == 200

    def test_bool_true(self, client):
        assert client.get("/bool/true").json()["v"] is True

    def test_bool_false(self, client):
        assert client.get("/bool/false").json()["v"] is False

    def test_int_zero(self, client):
        assert client.get("/int/zero").json()["v"] == 0

    def test_int_negative(self, client):
        assert client.get("/int/negative").json()["v"] == -42

    def test_int_max64(self, client):
        assert client.get("/int/max64").json()["v"] == 2**63 - 1

    def test_int_min64(self, client):
        assert client.get("/int/min64").json()["v"] == -(2**63)

    def test_bigint_is_number(self, client):
        """2**63 fits in orjson's u64 range — serialized as a JSON integer."""
        v = client.get("/int/bigint").json()["v"]
        assert isinstance(v, int)
        assert v == 2**63

    def test_bigint_neg_raises_error(self, client):
        """-(2**63)-1 is below i64::MIN — orjson raises TypeError → 500."""
        assert client.get("/int/bigint_neg").status_code == 500

    def test_huge_int_raises_error(self, client):
        """10**100 exceeds u64::MAX — orjson raises TypeError → 500."""
        assert client.get("/int/huge").status_code == 500


class TestFloats:
    def test_normal(self, client):
        assert abs(client.get("/float/normal").json()["v"] - 3.14) < 1e-10

    def test_zero(self, client):
        assert client.get("/float/zero").json()["v"] == 0.0

    def test_negative(self, client):
        assert abs(client.get("/float/neg").json()["v"] - (-2.718)) < 1e-10

    def test_nan_becomes_null(self, client):
        """orjson serializes NaN as null (JSON has no NaN)."""
        r = client.get("/float/nan")
        assert r.status_code == 200
        assert r.json()["v"] is None

    def test_inf_becomes_null(self, client):
        """orjson serializes inf as null (JSON has no infinity)."""
        r = client.get("/float/inf")
        assert r.status_code == 200
        assert r.json()["v"] is None

    def test_neg_inf_becomes_null(self, client):
        r = client.get("/float/neg_inf")
        assert r.status_code == 200
        assert r.json()["v"] is None

    def test_nan_no_path(self, client):
        """NaN is now serialized as null — no error, no path."""
        r = client.get("/float/nan")
        assert r.status_code == 200


class TestStrings:
    def test_empty(self, client):
        assert client.get("/string/empty").json()["v"] == ""

    def test_unicode(self, client):
        assert client.get("/string/unicode").json()["v"] == "你好世界 🔥"

    def test_special_chars(self, client):
        v = client.get("/string/special").json()["v"]
        assert "\t" in v
        assert "\n" in v
        assert '"' in v
        assert "\\" in v

    def test_surrogate_does_not_crash(self, client):
        resp = client.get("/string/surrogate")
        assert resp.status_code in (200, 500)
        if resp.status_code == 200:
            v = resp.json()["v"]
            assert "hello" in v
            assert "world" in v


class TestBoolIntIsolation:
    def test_bool_not_confused_with_int(self, client):
        v = client.get("/bool_in_list").json()["v"]
        assert v[0] is True
        assert v[1] is False
        assert v[2] == 1
        assert v[3] == 0
        assert isinstance(v[0], bool)
        assert isinstance(v[1], bool)
        assert isinstance(v[2], int)
        assert isinstance(v[3], int)

    def test_bool_int_dict_values(self, client):
        data = client.get("/bool_int_dict").json()
        assert data["bool_true"] is True
        assert data["bool_false"] is False
        assert data["int_one"] == 1
        assert data["int_zero"] == 0


class TestTuples:
    def test_simple_tuple(self, client):
        assert client.get("/tuple/simple").json()["v"] == [1, 2, 3]

    def test_mixed_tuple(self, client):
        assert client.get("/tuple/mixed").json()["v"] == [1, "two", 3.0, True, None]

    def test_nested_tuple(self, client):
        assert client.get("/tuple/nested").json()["v"] == [[1, 2], [3, 4]]

    def test_empty_tuple(self, client):
        assert client.get("/tuple/empty").json()["v"] == []

    def test_tuple_in_list(self, client):
        assert client.get("/tuple/in_list").json()["v"] == [[1, 2], [3, 4]]


class TestCollections:
    def test_empty_list(self, client):
        assert client.get("/list/empty").json()["v"] == []

    def test_nested_list(self, client):
        assert client.get("/list/nested").json()["v"] == [[1, 2], [3, [4, 5]]]

    def test_empty_dict(self, client):
        assert client.get("/dict/empty").json() == {}

    def test_nested_dict(self, client):
        assert client.get("/dict/nested").json()["a"]["b"]["c"] == 1

    def test_mixed_values(self, client):
        data = client.get("/dict/mixed_values").json()
        assert data["str"] == "hello"
        assert data["int"] == 42
        assert data["float"] == 1.5
        assert data["bool"] is True
        assert data["none"] is None
        assert data["list"] == [1, 2]
        assert data["tuple"] == [3, 4]
        assert data["dict"] == {"nested": True}


class TestDictKeyCoercion:
    def test_int_keys_raise_error(self, client):
        """orjson requires string dict keys → 500 for int keys."""
        assert client.get("/dict/int_keys").status_code == 500

    def test_bool_keys_raise_error(self, client):
        """orjson requires string dict keys → 500 for bool keys."""
        assert client.get("/dict/bool_keys").status_code == 500

    def test_none_key_raises_error(self, client):
        """orjson requires string dict keys → 500 for None key."""
        assert client.get("/dict/none_key").status_code == 500

    def test_float_key_raises_error(self, client):
        """orjson requires string dict keys → 500 for float key."""
        assert client.get("/dict/float_key").status_code == 500

    def test_unsupported_key_rejected(self, client):
        resp = client.get("/dict/unsupported_key")
        assert resp.status_code == 500

    def test_nan_key_rejected(self, client):
        resp = client.get("/dict/nan_key")
        assert resp.status_code == 500

    def test_unsupported_key_error_message(self, client):
        body = client.get("/dict/unsupported_key").text
        assert "key" in body.lower() or "tuple" in body.lower()


class TestCircularReference:
    def test_circular_list_returns_500(self, client):
        assert client.get("/circular/list").status_code == 500

    def test_circular_dict_returns_500(self, client):
        assert client.get("/circular/dict").status_code == 500

    def test_circular_error_message(self, client):
        """orjson raises 'Recursion limit reached' for circular refs."""
        body = client.get("/circular/list").text
        assert "ecursion" in body or "ircular" in body


class TestRepeatedReference:
    def test_repeated_ref_not_circular(self, client):
        resp = client.get("/repeated_ref")
        assert resp.status_code == 200
        data = resp.json()
        assert data["a"] == [1, 2, 3]
        assert data["b"] == [1, 2, 3]


class TestUnsupportedTypes:
    def test_set_as_iterable(self, client):
        """set is iterable — duck-typed as JSON array (order unspecified)."""
        resp = client.get("/unsupported/set")
        assert resp.status_code == 200
        assert sorted(resp.json()["v"]) == [1, 2, 3]

    def test_bytes_rejected(self, client):
        """bytes/bytearray must be rejected, not silently become int arrays."""
        assert client.get("/unsupported/bytes").status_code == 500

    def test_complex_rejected(self, client):
        assert client.get("/unsupported/complex").status_code == 500

    def test_custom_obj_rejected(self, client):
        assert client.get("/unsupported/custom_obj").status_code == 500

    def test_unsupported_error_message(self, client):
        body = client.get("/unsupported/complex").text
        assert "complex" in body.lower() or "serialize" in body.lower()


class TestDepthLimiting:
    def test_50_levels_ok(self, client):
        resp = client.get("/deep/ok")
        assert resp.status_code == 200
        data = resp.json()
        for _ in range(50):
            data = data["nested"]
        assert data["v"] == 42

    def test_300_levels_rejected(self, client):
        assert client.get("/deep/exceed").status_code == 500

    def test_depth_error_message(self, client):
        """orjson raises 'Recursion limit reached' for deeply nested objects."""
        body = client.get("/deep/exceed").text
        assert "ecursion" in body or "depth" in body.lower() or "limit" in body.lower()


class TestPathTracking:
    def test_nested_nan_becomes_null(self, client):
        """orjson serializes nested NaN as null — no error, no path."""
        r = client.get("/path/nested_nan")
        assert r.status_code == 200
        assert r.json()["users"][0]["score"] is None

    def test_nested_unsupported_raises_error(self, client):
        """Nested complex raises TypeError → 500 (no path in orjson errors)."""
        assert client.get("/path/nested_unsupported").status_code == 500

    def test_deep_circular_raises_error(self, client):
        """Circular ref raises Recursion limit reached → 500."""
        r = client.get("/path/deep_circular")
        assert r.status_code == 500
        assert "ecursion" in r.text or "ircular" in r.text


class TestResilience:
    """Server must not crash after any serialization error."""

    @pytest.mark.parametrize("error_paths", [
        ["/float/nan", "/float/inf"],
        ["/circular/list", "/circular/dict"],
        ["/unsupported/bytes", "/unsupported/complex", "/unsupported/custom_obj"],
        ["/deep/exceed"],
        ["/dict/unsupported_key", "/dict/nan_key"],
    ], ids=["nan", "circular", "unsupported", "depth", "bad_keys"])
    def test_server_survives(self, client, error_paths):
        for path in error_paths:
            client.get(path)
        resp = client.get("/bool/true")
        assert resp.status_code == 200
        assert resp.json()["v"] is True


# ========================
# Duck typing tests
# ========================

class TestDuckTyping:
    def test_defaultdict_via_dict(self, client):
        """defaultdict converted to dict should work."""
        resp = client.get("/duck/defaultdict")
        assert resp.status_code == 200
        data = resp.json()
        assert data["x"] == [1]
        assert data["y"] == [2]

    def test_ordereddict(self, client):
        """OrderedDict should serialize as JSON object (duck-typed via PyMapping)."""
        resp = client.get("/duck/ordereddict")
        assert resp.status_code == 200
        data = resp.json()
        assert data["first"] == 1
        assert data["second"] == 2
        assert data["third"] == 3

    def test_deque_raises_error(self, client):
        """orjson does not support deque natively → 500."""
        assert client.get("/duck/deque").status_code == 500

    def test_deque_nested_raises_error(self, client):
        """orjson does not support deque natively → 500."""
        assert client.get("/duck/deque_nested").status_code == 500

    def test_defaultdict_raw(self, client):
        """Raw defaultdict (not converted to dict) should serialize via PyMapping."""
        resp = client.get("/duck/defaultdict_raw")
        assert resp.status_code == 200
        data = resp.json()
        assert data["a"] == 10
        assert data["b"] == 20
