"""File upload support — multipart/form-data parser.

Usage::

    from pyronova.uploads import parse_multipart

    @app.post("/upload")
    def upload(req):
        form = parse_multipart(req)
        f = form["file"]
        return {"filename": f.filename, "size": len(f.data)}
"""

from __future__ import annotations
from dataclasses import dataclass


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


def parse_multipart(req) -> "dict[str, UploadFile | list[UploadFile]]":
    """Parse multipart/form-data from request.

    Returns dict mapping field name → UploadFile.
    For file fields, filename and content_type are set.
    For text fields, filename is None.
    """
    ct = req.headers.get("content-type", "")
    if "multipart/form-data" not in ct:
        raise ValueError(f"Expected multipart/form-data, got: {ct}")

    # Extract boundary. RFC 2045: Content-Type parameter names are
    # case-insensitive — `BOUNDARY=` and `boundary=` are equivalent.
    # Matching only lowercase rejects valid uppercase clients (arc
    # finding uploads-3). Compare against the lowercased token.
    boundary = None
    for part in ct.split(";"):
        part = part.strip()
        lowered = part.lower()
        if lowered.startswith("boundary="):
            # Slice from the original `part` so casing in the value
            # itself (boundaries are case-sensitive) is preserved.
            boundary = part[len("boundary="):].strip().strip('"')
            break

    if not boundary:
        raise ValueError("Missing boundary in Content-Type")

    raw = req.body
    if raw is None:
        raise ValueError("parse_multipart: request body is empty")
    body = raw if isinstance(raw, bytes) else raw.encode()

    # RFC 2046: boundary markers MUST be line-anchored (preceded by
    # CRLF). Splitting on the raw `--{boundary}` token false-splits
    # when file content contains those bytes mid-stream — a
    # data-corruption bug for any upload whose content happens to
    # include the boundary sequence (CRITICAL, arc finding
    # python-pyronova-uploads-2).
    #
    # Fix: prepend \r\n to the body so the very first boundary
    # (which has no leading CRLF when the body starts directly
    # with `--boundary`) is uniformly anchored, then split on
    # \r\n--{boundary}. LF-only framing falls back to \n--{boundary}
    # — matches the \n\n header-separator fallback below for
    # clients/proxies that strip CRLF.
    crlf_anchor = ("\r\n--" + boundary).encode()
    lf_anchor = ("\n--" + boundary).encode()
    if crlf_anchor in body:
        parts = (b"\r\n" + body).split(crlf_anchor)
    elif lf_anchor in body:
        parts = (b"\n" + body).split(lf_anchor)
    else:
        # No line-anchored boundary marker found — body is malformed
        # or degenerate. Return empty parts rather than the pre-fix
        # behavior of splitting on raw `--{boundary}` (which produced
        # subtly-wrong data instead of an empty result).
        parts = []
    result = {}

    for part in parts:
        if not part or part.strip() == b"--" or part.strip() == b"":
            continue

        # Split headers from body (separated by \r\n\r\n)
        if b"\r\n\r\n" in part:
            header_section, file_data = part.split(b"\r\n\r\n", 1)
        elif b"\n\n" in part:
            header_section, file_data = part.split(b"\n\n", 1)
        else:
            continue

        # Strip trailing \r\n
        if file_data.endswith(b"\r\n"):
            file_data = file_data[:-2]
        elif file_data.endswith(b"\n"):
            file_data = file_data[:-1]

        # Parse headers
        headers = {}
        for line in header_section.decode("utf-8", errors="replace").split("\n"):
            line = line.strip()
            if ":" in line:
                key, _, val = line.partition(":")
                headers[key.strip().lower()] = val.strip()

        # Parse Content-Disposition
        disposition = headers.get("content-disposition", "")
        field_name = None
        filename = None

        # RFC 2045 §5.1: parameter names are case-insensitive (NAME=,
        # FileName= are valid), and quoted values may contain semicolons
        # and escaped quotes. Use the quote-aware splitter + case-insensitive
        # name match (arc findings uploads-70/71/73).
        for param in _split_header_params(disposition):
            param = param.strip()
            lowered = param.lower()
            if lowered.startswith("name="):
                field_name = _unquote_param(param[5:])
            elif lowered.startswith("filename="):
                filename = _unquote_param(param[9:])

        if field_name:
            content_type = headers.get("content-type", "application/octet-stream" if filename else "text/plain")
            upload = UploadFile(
                name=field_name,
                filename=filename,
                content_type=content_type,
                data=file_data,
            )
            if field_name in result:
                existing = result[field_name]
                if isinstance(existing, list):
                    existing.append(upload)
                else:
                    result[field_name] = [existing, upload]
            else:
                result[field_name] = upload

    return result
