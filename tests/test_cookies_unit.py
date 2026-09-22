"""Unit tests for cookie utilities (no server needed)."""

import pytest
from pyronova.cookies import get_cookies, get_cookie, set_cookie, delete_cookie
from pyronova.engine import Response


class FakeRequest:
    def __init__(self, cookie_header=""):
        self.headers = {"cookie": cookie_header} if cookie_header else {}


def test_get_cookies_single():
    req = FakeRequest("session=abc123")
    cookies = get_cookies(req)
    assert cookies == {"session": "abc123"}


def test_get_cookies_multiple():
    req = FakeRequest("a=1; b=2; c=3")
    cookies = get_cookies(req)
    assert cookies == {"a": "1", "b": "2", "c": "3"}


def test_get_cookies_empty():
    req = FakeRequest("")
    assert get_cookies(req) == {}


def test_get_cookies_no_header():
    req = FakeRequest()
    req.headers = {}
    assert get_cookies(req) == {}


def test_get_cookie_found():
    req = FakeRequest("token=xyz; lang=en")
    assert get_cookie(req, "token") == "xyz"
    assert get_cookie(req, "lang") == "en"


def test_get_cookie_missing():
    req = FakeRequest("token=xyz")
    assert get_cookie(req, "missing") is None
    assert get_cookie(req, "missing", "default") == "default"


def test_set_cookie_basic():
    resp = Response(body="ok")
    result = set_cookie(resp, "session", "abc123")
    cookie_header = result.headers.get("set-cookie", "")
    assert "session=abc123" in cookie_header
    assert "Path=/" in cookie_header


def test_set_cookie_httponly():
    resp = Response(body="ok")
    result = set_cookie(resp, "token", "secret", httponly=True, secure=True)
    cookie_header = result.headers.get("set-cookie", "")
    assert "HttpOnly" in cookie_header
    assert "Secure" in cookie_header


def test_set_cookie_max_age():
    resp = Response(body="ok")
    result = set_cookie(resp, "prefs", "dark", max_age=3600)
    cookie_header = result.headers.get("set-cookie", "")
    assert "Max-Age=3600" in cookie_header


def test_delete_cookie():
    resp = Response(body="ok")
    result = delete_cookie(resp, "session")
    cookie_header = result.headers.get("set-cookie", "")
    assert "session=" in cookie_header
    assert "Max-Age=0" in cookie_header


def test_delete_cookie_forwards_httponly():
    resp = Response(body="ok")
    result = delete_cookie(resp, "session", httponly=True, secure=True)
    cookie_header = result.headers.get("set-cookie", "")
    assert "Max-Age=0" in cookie_header
    assert "HttpOnly" in cookie_header
    assert "Secure" in cookie_header


def test_delete_cookie_default_not_httponly():
    resp = Response(body="ok")
    result = delete_cookie(resp, "session")
    assert "HttpOnly" not in result.headers.get("set-cookie", "")
