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
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyronova.engine import Request, Response

# Characters forbidden in cookie name/value per RFC 6265. CR (\r) and LF
# (\n) in particular enable HTTP Response Splitting: an attacker crafts a
# value containing `\r\nSet-Cookie: admin=1` and injects arbitrary headers
# into the response. NUL is a control-char trap too. Semicolons and commas
# are also forbidden to prevent header value smuggling via the separator chars.
_COOKIE_FORBIDDEN = ("\r", "\n", "\0", ";", ",")

_SAMESITE_VALID = {"Strict", "Lax", "None"}


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
            value = value.strip()
            # RFC 6265 allows DQUOTE-wrapped cookie values
            if value.startswith('"') and value.endswith('"') and len(value) >= 2:
                value = value[1:-1]
            cookies[name.strip()] = value
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
    expires: str | None = None,
    path: str = "/",
    domain: str | None = None,
    secure: bool = False,
    httponly: bool = False,
    samesite: str | None = "Lax",
) -> "Response":
    """Set a cookie on a Response.

    Returns a new Response with the Set-Cookie header appended.
    Multiple calls produce multiple Set-Cookie headers (required for
    sending more than one cookie in a single response).
    """
    from pyronova.engine import Response

    # RFC 6265 §4.1.1 cookie-name = token = 1*<CHAR>. Empty name would
    # produce `Set-Cookie: =value; ...` which browsers may reject or
    # parse ambiguously (arc finding cookies-1).
    if not name:
        raise ValueError("cookie name must not be empty (RFC 6265 §4.1.1)")
    _reject_control_chars("name", name)
    _reject_control_chars("value", value)
    if domain is not None:
        _reject_control_chars("domain", domain)
    if path:
        _reject_control_chars("path", path)
    if expires is not None:
        _reject_control_chars("expires", expires)

    parts = [f"{name}={value}"]
    if max_age is not None:
        parts.append(f"Max-Age={max_age}")
    if expires:
        parts.append(f"Expires={expires}")
    if path:
        parts.append(f"Path={path}")
    if domain:
        parts.append(f"Domain={domain}")
    if secure:
        parts.append("Secure")
    if httponly:
        parts.append("HttpOnly")
    if samesite is not None:
        samesite_norm = str(samesite).strip().title()
        if samesite_norm not in _SAMESITE_VALID:
            raise ValueError(
                f"invalid samesite={samesite!r}; must be 'Strict', 'Lax', or 'None'"
            )
        if samesite_norm == "None" and not secure:
            raise ValueError(
                "SameSite=None requires Secure=True; browsers silently drop "
                "SameSite=None cookies that are not Secure (Chrome 80+, Firefox, Safari)"
            )
        parts.append(f"SameSite={samesite_norm}")

    cookie_str = "; ".join(parts)
    # Build headers with case-normalised key to avoid duplicate set-cookie entries
    headers = dict(getattr(response, "headers", {}) or {})
    # Normalise existing key case (response may carry "Set-Cookie" or "set-cookie")
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


def delete_cookie(
    response: Response,
    name: str,
    *,
    path: str = "/",
    domain: str | None = None,
    secure: bool = False,
    samesite: str | None = "Lax",
) -> Response:
    """Delete a cookie by setting it expired.

    Forwards domain/secure/samesite so the browser's deletion matches the
    original cookie's scope.
    """
    return set_cookie(
        response, name, "",
        max_age=0,
        path=path,
        domain=domain,
        secure=secure,
        samesite=samesite,
    )
