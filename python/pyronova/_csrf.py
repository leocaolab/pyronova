"""Refusing cross-site requests to the endpoints that act on a POST body: ``/mcp`` and
``@app.rpc``.

A web page can make a browser send a cross-site POST without a CORS preflight only as a
"simple" request: ``text/plain``, a form, or a body with no type at all. So these
endpoints take only the body types they decode (anything else is 415), and refuse a
request whose ``Origin`` is another site (403) unless the app trusts it
(``app.trusted_origins``). A request without ``Origin`` (curl, an SDK, a server) and one
from the server's own host are admitted. The host is the request's authority
(``req.authority``): an HTTP/2 request may carry only ``:authority``, no ``Host``.
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from typing import Iterable, Mapping, TypeVar

T = TypeVar("T")

_DEFAULT_PORTS = {"http": 80, "https": 443}


@dataclass(frozen=True)
class Origin:
    """A web origin: scheme, host (lower-case) and port (the scheme's default when the
    text leaves it out)."""

    scheme: str
    host: str
    port: int

    @classmethod
    def parse(cls, text: str) -> Origin | None:
        """``scheme://host[:port]`` as an ``Origin``; ``None`` for anything else (``null``,
        a path, another scheme, a bad port)."""
        try:
            parts = urllib.parse.urlsplit(text)
            port = parts.port
        except ValueError:
            return None
        default_port = _DEFAULT_PORTS.get(parts.scheme)
        if default_port is None or not parts.hostname or parts.path or parts.query or parts.fragment:
            return None
        if parts.username is not None or parts.password is not None:
            return None
        return cls(parts.scheme, parts.hostname.lower(), port or default_port)

    def serves(self, host: str | None) -> bool:
        """Whether this origin is the ``host[:port]`` the request was sent to: same host
        and port, a host without a port meaning this origin's scheme default."""
        if not host:
            return False
        try:
            parts = urllib.parse.urlsplit("//" + host)
            port = parts.port
        except ValueError:
            return False
        return parts.hostname == self.host and (port or _DEFAULT_PORTS[self.scheme]) == self.port


@dataclass(frozen=True)
class OriginPolicy:
    """Which ``Origin`` values a request may carry: none at all, the server's own host,
    or one of ``trusted``."""

    trusted: frozenset[Origin] = frozenset()

    @classmethod
    def of(cls, origins: Iterable[str]) -> OriginPolicy:
        """``ValueError`` naming an entry that isn't ``scheme://host[:port]``."""
        if isinstance(origins, str):
            raise TypeError("trusted_origins must be a list of origins, not one str")
        parsed = []
        for text in origins:
            origin = Origin.parse(text) if isinstance(text, str) else None
            if origin is None:
                raise ValueError(
                    f"trusted origin {text!r} is not an origin: expected "
                    "'http[s]://host[:port]' with no path"
                )
            parsed.append(origin)
        return cls(frozenset(parsed))

    def admits(self, origin: str | None, host: str | None) -> bool:
        if origin is None:
            return True
        parsed = Origin.parse(origin)
        return parsed is not None and (parsed in self.trusted or parsed.serves(host))


@dataclass(frozen=True)
class Refused:
    """Why a request was refused: the status (403, 415) and the reason the client gets."""

    status: int
    reason: str


def media_type(content_type: str | None) -> str | None:
    """``application/json; charset=utf-8`` → ``application/json``; ``None`` without one."""
    if content_type is None:
        return None
    return content_type.split(";", 1)[0].strip().lower() or None


def check(req, policy: OriginPolicy, accepted: Mapping[str, T]) -> T | Refused:
    """What ``req``'s body is, from the media types ``accepted`` maps; or why it is
    refused: a type not in ``accepted`` (415), then an ``Origin`` the policy doesn't
    admit (403)."""
    content_type = req.headers.get("content-type")
    media = media_type(content_type)
    if media not in accepted:
        return Refused(
            415,
            f"Content-Type must be {' or '.join(sorted(accepted))}, got "
            f"{'none' if content_type is None else repr(content_type)}",
        )
    origin = req.headers.get("origin")
    if not policy.admits(origin, req.authority):
        return Refused(
            403,
            f"Origin {origin!r} is not allowed: not this server's host and not in "
            "app.trusted_origins",
        )
    return accepted[media]
