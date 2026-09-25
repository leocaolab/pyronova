"""File upload support — multipart/form-data parser.

Usage::

    from pyronova.uploads import parse_multipart

    @app.post("/upload")
    def upload(req):
        form = parse_multipart(req)
        f = form["file"]
        return {"filename": f.filename, "size": len(f.data)}

``UploadFile.filename`` is what the client sent, decoded but otherwise as is: it
may hold ``../``, an absolute path, a drive letter, control characters or be
empty. Never join it into a filesystem path; name stored files yourself (a
UUID, a content hash) and keep the client's name only as data.
"""

from __future__ import annotations
from dataclasses import dataclass
from urllib.parse import unquote_to_bytes


def _split_header_params(value: str) -> list[str]:
    """Split a header on top-level ``;`` separators, treating semicolons
    inside a quoted-string as literal.

    A naive ``value.split(";")`` corrupts any parameter whose quoted value
    contains a semicolon, e.g. ``filename="report;2024.csv"`` (arc finding
    uploads-71). RFC 2045 quoted-strings are honoured here.
    """
    parts: list[str] = []
    buf: list[str] = []
    in_quotes = False
    escaped = False
    for ch in value:
        if escaped:
            buf.append(ch)
            escaped = False
            continue
        if in_quotes and ch == "\\":
            buf.append(ch)
            escaped = True
            continue
        if ch == '"':
            in_quotes = not in_quotes
            buf.append(ch)
            continue
        if ch == ";" and not in_quotes:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    parts.append("".join(buf))
    return parts


def _unquote_param(value: str) -> str:
    """Strip surrounding DQUOTEs and unescape ``\\"`` / ``\\\\`` per RFC 2045
    quoted-string rules (arc finding uploads-73)."""
    value = value.strip()
    if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
        inner = value[1:-1]
        out: list[str] = []
        escaped = False
        for ch in inner:
            if escaped:
                out.append(ch)
                escaped = False
            elif ch == "\\":
                escaped = True
            else:
                out.append(ch)
        # A trailing backslash (malformed quoted-string per RFC 2045) leaves
        # `escaped` set with nothing to escape — preserve it as a literal
        # rather than silently dropping it.
        if escaped:
            out.append("\\")
        return "".join(out)
    return value


@dataclass(frozen=True, slots=True)
class UploadFile:
    """A single uploaded file or form field.

    Frozen because this is a DTO handed from the framework to user code.
    A request's parsed `UploadFile` objects share memory with the raw
    multipart buffer; letting a handler mutate `data` in place would
    corrupt replay logging, after_request hooks, and any async task
    still holding a reference. Immutable + slots is free and correct.
    """
    name: str
    # Client-controlled: see the module docstring before using it in a path.
    filename: str | None
    content_type: str
    data: bytes

    @property
    def text(self) -> str:
        # Uploaded bytes are arbitrary user content — may not be valid
        # UTF-8 (binary files, mojibake, partial buffers). Use `replace`
        # so calling .text on a binary upload yields a lossy string
        # instead of crashing the request with UnicodeDecodeError
        # (arc finding uploads-1). Callers who need strict decoding
        # should work with .data directly.
        return self.data.decode("utf-8", errors="replace")

    @property
    def size(self) -> int:
        return len(self.data)


# Longest run of transport padding (spaces/tabs) read after a delimiter when
# telling CRLF from LF framing.
_MAX_PADDING = 64


class MultipartError(ValueError):
    """The request body is not well-formed multipart/form-data."""


def parse_multipart(req) -> "dict[str, UploadFile | list[UploadFile]]":
    """Parse multipart/form-data from request.

    Returns dict mapping field name → UploadFile.
    For file fields, filename and content_type are set.
    For text fields, filename is None.

    Raises MultipartError when the body is not well-formed multipart/form-data.
    """
    boundary = _boundary(req.headers.get("content-type", ""))
    raw = req.body
    if raw is None:
        raise MultipartError("request body is empty")
    body = raw if isinstance(raw, bytes) else raw.encode()

    nl = _line_break(body, boundary)
    result: dict[str, UploadFile | list[UploadFile]] = {}
    for upload in (_parse_part(part, nl) for part in _split_parts(body, boundary, nl)):
        existing = result.get(upload.name)
        if existing is None:
            result[upload.name] = upload
        elif isinstance(existing, list):
            existing.append(upload)
        else:
            result[upload.name] = [existing, upload]
    return result


def _boundary(content_type: str) -> str:
    media_type, *params = _split_header_params(content_type)
    # RFC 9110 §8.3.1: the type, subtype and parameter names are case-insensitive; the
    # boundary value is not. A quoted boundary may contain ";".
    if media_type.strip().lower() != "multipart/form-data":
        raise MultipartError(f"Expected multipart/form-data, got: {content_type}")
    boundary = _header_params(params).get("boundary")
    if not boundary:
        raise MultipartError(f"Missing boundary in Content-Type: {content_type}")
    return boundary


def _header_params(params: list[str]) -> dict[str, str]:
    """``name=value`` header parameters by lowercased name, values unquoted."""
    out: dict[str, str] = {}
    for param in params:
        name, eq, value = param.partition("=")
        if eq:
            out[name.strip().lower()] = _unquote_param(value)
    return out


# RFC 8187 §3.2.1: recipients must support these two charsets.
_EXT_VALUE_CHARSETS = {"utf-8": "utf-8", "iso-8859-1": "latin-1"}


def _ext_value(value: str) -> str:
    """An RFC 8187 (RFC 5987) ext-value, ``charset'language'pct-encoded``, decoded."""
    charset, sep1, rest = value.partition("'")
    _language, sep2, encoded = rest.partition("'")
    codec = _EXT_VALUE_CHARSETS.get(charset.strip().lower())
    if not (sep1 and sep2) or codec is None:
        raise MultipartError(
            f"filename*={value!r} is not charset'language'value with charset UTF-8 or "
            "ISO-8859-1 (RFC 8187)"
        )
    try:
        return unquote_to_bytes(encoded).decode(codec)
    except UnicodeDecodeError as e:
        raise MultipartError(f"filename*={value!r} is not valid {charset}: {e}") from None


def _line_break(body: bytes, boundary: str) -> bytes:
    """The line break the body is framed with, read off the first delimiter line:
    CRLF per RFC 2046, or bare LF from clients/proxies that strip CR."""
    dash = b"--" + boundary.encode()
    start = body.find(dash)
    if start == -1:
        raise MultipartError(f"no delimiter line --{boundary} in the body")
    line_end = body[start + len(dash):start + len(dash) + _MAX_PADDING].lstrip(b" \t")
    return b"\r\n" if line_end.startswith(b"\r\n") else b"\n"


def _split_parts(body: bytes, boundary: str, nl: bytes) -> list[bytes]:
    """The content of every part, headers included.

    RFC 2046 §5.1.1: a delimiter is ``CRLF--boundary``; the CRLF belongs to the
    delimiter, not to the part before it, so a part's content ends right where
    the delimiter begins. The body is prefixed with a line break so the first
    delimiter (at the very start, with no line break before it) is anchored like
    the rest.
    """
    segments = (nl + body).split(nl + b"--" + boundary.encode())
    if len(segments) < 2:
        raise MultipartError(f"no delimiter line --{boundary} in the body")

    parts: list[bytes] = []
    for segment in segments[1:]:  # segments[0] is the preamble
        if segment.startswith(b"--"):
            return parts
        padding, sep, part = segment.partition(nl)
        if not sep or padding.strip(b" \t"):
            raise MultipartError(
                f"delimiter --{boundary} is followed by {padding[:40]!r}, not a line break"
            )
        parts.append(part)
    raise MultipartError(f"body ends without the closing delimiter --{boundary}--")


def _parse_part(part: bytes, nl: bytes) -> UploadFile:
    # A part with no header lines starts with the blank line itself.
    header_section, sep, data = (
        (b"", nl, part[len(nl):]) if part.startswith(nl) else part.partition(nl + nl)
    )
    if not sep:
        raise MultipartError(f"part has no blank line after its headers: {part[:80]!r}")

    headers = {}
    for line in header_section.decode("utf-8", errors="replace").split("\n"):
        key, colon, val = line.strip().partition(":")
        if colon:
            headers[key.strip().lower()] = val.strip()

    # RFC 2045 §5.1: parameter names are case-insensitive (NAME=, FileName=),
    # and quoted values may contain semicolons and escaped quotes. RFC 6266 §4.3:
    # filename* (RFC 8187, non-ASCII names) wins over filename.
    disposition = headers.get("content-disposition", "")
    params = _header_params(_split_header_params(disposition)[1:])
    field_name = params.get("name")
    filename = _ext_value(params["filename*"]) if "filename*" in params else params.get("filename")
    if not field_name:
        raise MultipartError(
            f"part has no field name in its Content-Disposition: {disposition!r}"
        )

    default_type = "application/octet-stream" if filename else "text/plain"
    return UploadFile(
        name=field_name,
        filename=filename,
        content_type=headers.get("content-type", default_type),
        data=data,
    )
