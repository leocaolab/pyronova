"""Cookie utilities for Pyronova.

Read cookies from request headers, set cookies on responses.

Usage::

    from pyronova.cookies import get_cookies, set_cookie

    @app.get("/")
    def index(req):
        cookies = get_cookies(req)
        session = cookies.get("session_id", "none")
        return set_cookie(
            Response(body=f"session={session}"),
            "session_id", "abc123",
            max_age=3600, httponly=True,
        )
"""

from __future__ import annotations
from datetime import datetime, timezone
from email.utils import format_datetime
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyronova.engine import Request, Response

# Forbidden in any Set-Cookie field (RFC 6265): CR/LF would let a value inject headers
# (`\r\nSet-Cookie: admin=1`), NUL is a control character, and `;` / `,` are the
# attribute and header-list separators.
_COOKIE_FORBIDDEN = ("\r", "\n", "\0", ";", ",")


class SameSite(StrEnum):
    STRICT = "Strict"
    LAX = "Lax"
    NONE = "None"


def _reject_control_chars(field: str, value: str) -> None:
    for ch in _COOKIE_FORBIDDEN:
        if ch in value:
            raise ValueError(
                f"cookie {field} contains forbidden character "
                f"{ch!r}; refusing to emit (HTTP response splitting risk)"
            )


def get_cookies(req: Request) -> dict[str, str]:
    """Parse cookies from request headers.

    Returns a dict of cookie name → value.
    """
    cookie_header = req.headers.get("cookie", "")
    if not cookie_header:
        return {}
    cookies = {}
    for pair in cookie_header.split(";"):
        pair = pair.strip()
        if "=" in pair:
            name, _, value = pair.partition("=")
            name = name.strip()
            value = value.strip()
            # RFC 6265 allows a DQUOTE-wrapped value: unwrap a matched pair only. A lone
            # `"` is not a quoted-string and is dropped, so it can't be re-emitted.
            if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
                value = value[1:-1]
            if value == '"':
                value = ""
            # RFC 6265 cookie-names are non-empty: `Cookie: =value` is dropped.
            if not name:
                continue
            # First occurrence wins: a duplicate appended after the browser's own
            # (`session=evil`) is ignored.
            if name not in cookies:
                cookies[name] = value
    return cookies


def get_cookie(req: Request, name: str, default: str | None = None) -> str | None:
    """Get a single cookie value by name."""
    return get_cookies(req).get(name, default)


def set_cookie(
    response: Response,
    name: str,
    value: str,
    *,
    max_age: int | None = None,
    expires: datetime | None = None,
    path: str = "/",
    domain: str | None = None,
    secure: bool = False,
    httponly: bool = False,
    samesite: SameSite | None = SameSite.LAX,
) -> "Response":
    """Set a cookie on a Response.

    Returns a new Response with the Set-Cookie header appended.
    Multiple calls produce multiple Set-Cookie headers (required for
    sending more than one cookie in a single response).

    ``expires`` is a timezone-aware ``datetime``; it is sent as an HTTP-date
    in GMT (RFC 6265 §4.1.1).
    """
    from pyronova.engine import Response

    # Browsers reject or disagree on `Set-Cookie: =value`.
    if not name:
        raise ValueError("cookie name must not be empty (RFC 6265 §4.1.1)")
    _reject_control_chars("name", name)
    _reject_control_chars("value", value)
    if domain is not None:
        _reject_control_chars("domain", domain)
    if path:
        _reject_control_chars("path", path)

    parts = [f"{name}={value}"]
    if max_age is not None:
        # Interpolated verbatim: a str (`"0\r\nSet-Cookie: admin=1"`) would inject
        # headers. bool is an int subclass but never a Max-Age.
        if isinstance(max_age, bool) or not isinstance(max_age, int):
            raise ValueError(
                f"cookie max_age must be an int (got {type(max_age).__name__})"
            )
        parts.append(f"Max-Age={max_age}")
    if expires is not None:
        parts.append(f"Expires={_http_date(expires)}")
    if path:
        parts.append(f"Path={path}")
    if domain:
        parts.append(f"Domain={domain}")
    if secure:
        parts.append("Secure")
    if httponly:
        parts.append("HttpOnly")
    if samesite is not None:
        samesite = SameSite(samesite)
        if samesite is SameSite.NONE and not secure:
            raise ValueError(
                "SameSite=None requires Secure=True; browsers silently drop "
                "SameSite=None cookies that are not Secure (Chrome 80+, Firefox, Safari)"
            )
        parts.append(f"SameSite={samesite}")

    cookie_str = "; ".join(parts)
    headers = dict(getattr(response, "headers", {}) or {})
    # Append to the existing entry whatever its case: one set-cookie key, not two.
    existing_key = next((k for k in headers if k.lower() == "set-cookie"), None)
    if existing_key is None:
        headers["set-cookie"] = cookie_str
    else:
        existing = headers[existing_key]
        if isinstance(existing, list):
            headers[existing_key] = existing + [cookie_str]
        else:
            headers[existing_key] = [existing, cookie_str]

    return Response(
        body=response.body,
        status_code=getattr(response, "status_code", 200),
        content_type=getattr(response, "content_type", None),
        headers=headers,
    )


def _http_date(when: datetime) -> str:
    if not isinstance(when, datetime):
        raise TypeError(f"cookie expires must be a datetime, got {type(when).__name__}")
    if when.tzinfo is None:
        raise ValueError(
            f"cookie expires={when!r} has no timezone; pass an aware datetime "
            "so the GMT time sent to the browser is unambiguous"
        )
    return format_datetime(when.astimezone(timezone.utc), usegmt=True)


def delete_cookie(
    response: Response,
    name: str,
    *,
    path: str = "/",
    domain: str | None = None,
    secure: bool = False,
    httponly: bool = False,
    samesite: SameSite | None = SameSite.LAX,
) -> Response:
    """Delete a cookie by setting it expired.

    Forwards domain/secure/httponly/samesite so the browser's deletion
    matches the original cookie's scope. Browsers refuse to overwrite an
    HttpOnly cookie with a non-HttpOnly Set-Cookie, so deleting an HttpOnly
    cookie requires ``httponly=True`` or the cookie silently survives.
    """
    return set_cookie(
        response, name, "",
        max_age=0,
        path=path,
        domain=domain,
        secure=secure,
        httponly=httponly,
        samesite=samesite,
    )
