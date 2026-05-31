"""Pyronova — A high-performance Python web framework powered by Rust."""

from pyronova.engine import PyronovaApp, Request, Response, WebSocket, SharedState, Stream, get_gil_metrics, init_logger, emit_python_log
from pyronova.app import Pyronova
from pyronova.rpc import RPCClient
from pyronova.cookies import get_cookies, get_cookie, set_cookie, delete_cookie
from pyronova.uploads import parse_multipart, UploadFile
from pyronova.cache import cached_json


def redirect(url: str, status_code: int = 302) -> Response:
    """Return a redirect response.

    Usage::

        @app.get("/old")
        def old_page(req):
            return redirect("/new")

        @app.get("/permanent")
        def moved(req):
            return redirect("/new-home", status_code=301)
    """
    # Reject CR/LF/NUL — the same HTTP Response Splitting class of attack
    # the cookie helpers defend against. Open-redirect (scheme/host
    # allow-list) is left to the caller; we only block header injection.
    for _ch in ("\r", "\n", "\0"):
        if _ch in url:
            raise ValueError(
                f"redirect url contains forbidden control character "
                f"{_ch!r}; refusing to emit (HTTP response splitting risk)"
            )
    return Response(
        body="",
        status_code=status_code,
        headers={"location": url},
    )

__all__ = [
    "Pyronova", "PyronovaApp", "Request", "Response", "WebSocket", "SharedState", "Stream",
    "get_gil_metrics", "init_logger", "emit_python_log",
    "redirect", "RPCClient",
    "get_cookies", "get_cookie", "set_cookie", "delete_cookie",
    "parse_multipart", "UploadFile",
    "cached_json",
]
try:
    from importlib.metadata import PackageNotFoundError, version as _get_version
    __version__ = _get_version("pyronova")
except (ImportError, PackageNotFoundError):
    # ImportError: importlib.metadata unavailable (ancient Python).
    # PackageNotFoundError: running from a source checkout that was never
    # installed. Both are expected "no metadata" cases; any other error
    # (corrupt metadata, FS permission) should surface, not be hidden
    # behind a silent "dev" default (arc finding init-6).
    __version__ = "dev"
