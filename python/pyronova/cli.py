"""Pyronova command-line interface.

Entry point for the ``pyronova`` console script. Three subcommands:

- ``pyronova run <module:attr>``   production-ish launch (no reload)
- ``pyronova dev <module:attr>``   hot-reload + DEBUG logging
- ``pyronova routes <module:attr>`` print the registered route table

The target is a ``module:attr`` string identifying a Pyronova app, just like
gunicorn/uvicorn. ``attr`` defaults to ``app``.

Examples::

    pyronova run examples.hello:app --port 8080
    pyronova dev examples.hello
    pyronova routes examples.hello
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyronova.app import Pyronova


def _load_app(target: str) -> "Pyronova":
    """Resolve ``module[:attr]`` → Pyronova instance. Importing the module
    runs its top-level code, which registers routes on the app."""
    if ":" in target:
        # A module:attr spec has exactly one colon. More than one (a:b:c)
        # or a leading colon (:app) is a malformed target — fail with a
        # clear message rather than a confusing ImportError downstream
        # (arc findings cli-18, cli-21). Module names never contain colons.
        if target.count(":") > 1:
            sys.exit(
                f"pyronova: invalid target {target!r}: expected module:attr "
                "(exactly one ':')"
            )
        module_name, attr = target.split(":", 1)
        if not module_name:
            sys.exit(f"pyronova: invalid target {target!r}: module name must not be empty")
        if not attr:
            sys.exit(f"pyronova: invalid target {target!r}: attribute name must not be empty")
    else:
        module_name, attr = target, "app"

    # Make the current directory importable — same ergonomic as uvicorn.
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        sys.exit(f"pyronova: cannot import {module_name!r}: {e}")

    try:
        app = getattr(module, attr)
    except AttributeError:
        sys.exit(f"pyronova: module {module_name!r} has no attribute {attr!r}")

    from pyronova.app import Pyronova
    if not isinstance(app, Pyronova):
        sys.exit(
            f"pyronova: {target} is a {type(app).__name__}, expected pyronova.Pyronova"
        )
    return app


def _cmd_run(args: argparse.Namespace) -> None:
    if bool(args.tls_cert) != bool(args.tls_key):
        sys.exit("pyronova: --tls-cert and --tls-key must be specified together")
    app = _load_app(args.target)
    app.run(
        host=args.host,
        port=args.port,
        workers=args.workers,
        io_workers=args.io_workers,
        reload=False,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
    )


def _cmd_dev(args: argparse.Namespace) -> None:
    os.environ.setdefault("PYRONOVA_LOG", "1")
    app = _load_app(args.target)
    # Dev defaults: bind all interfaces so LAN clients can probe.
    app.run(
        host=args.host,
        port=args.port,
        workers=args.workers,
        reload=True,
    )


def _cmd_routes(args: argparse.Namespace) -> None:
    app = _load_app(args.target)
    rows: list[tuple[str, str, str, str]] = []
    for r in app.routes:
        flags = []
        if r.get("gil"):
            flags.append("gil")
        if r.get("stream"):
            flags.append("stream")
        if r.get("async"):
            flags.append("async")
        if r.get("model"):
            flags.append(f"model={r['model']}")
        rows.append((
            r.get("method", "?"),
            r.get("path", "?"),
            r.get("handler", "?"),
            ",".join(flags),
        ))
    for r in app.fast_routes:
        rows.append((
            r.get("method", "?"),
            r.get("path", "?"),
            f"<fast:{r.get('bytes', '?')}B>",
            f"status={r.get('status_code', '?')}",
        ))

    if not rows:
        print("(no routes registered)")
        return

    # Coerce every cell to str up front: a route dict may carry non-string
    # values (e.g. an int method/handler from a misbehaving registration),
    # and both len() and the {:<w} format spec raise on non-str — turning a
    # diagnostic command into a crash (arc finding cli-20).
    rows = [tuple(str(c) for c in row) for row in rows]

    widths = [max(len(r[i]) for r in rows) for i in range(4)]
    header = ("METHOD", "PATH", "HANDLER", "FLAGS")
    widths = [max(widths[i], len(header[i])) for i in range(4)]
    fmt = "  ".join("{:<%d}" % w for w in widths)
    print(fmt.format(*header))
    print(fmt.format(*("-" * w for w in widths)))
    for r in rows:
        print(fmt.format(*r))
    print(f"\n{len(rows)} route(s)")


def _add_run_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument("target", help="module[:attr], e.g. examples.hello:app")
    p.add_argument("--host", default=None, help="bind address (default 127.0.0.1)")
    p.add_argument("--port", type=int, default=None, help="bind port (default 8000)")
    p.add_argument("--workers", type=int, default=None, help="sub-interpreter count")
    p.add_argument("--io-workers", type=int, default=None, help="Tokio I/O thread count")
    p.add_argument("--tls-cert", default=None)
    p.add_argument("--tls-key", default=None)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="pyronova",
        description="Pyronova — high-performance Python web framework powered by Rust.",
    )
    try:
        from pyronova import __version__ as _v
    except (ImportError, AttributeError):
        # Only swallow the genuinely-expected lookup failures (package not
        # installed / __version__ absent). A broad `except Exception` here
        # would mask real bugs behind a silent "dev" fallback (arc cli-19).
        _v = "dev"
    parser.add_argument("--version", action="version", version=f"pyronova {_v}")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="start the server")
    _add_run_flags(p_run)
    p_run.set_defaults(func=_cmd_run)

    p_dev = sub.add_parser("dev", help="start with hot-reload + debug logging")
    p_dev.add_argument("target", help="module[:attr]")
    p_dev.add_argument("--host", default="127.0.0.1")
    p_dev.add_argument("--port", type=int, default=None)
    p_dev.add_argument("--workers", type=int, default=None)
    p_dev.set_defaults(func=_cmd_dev)

    p_routes = sub.add_parser("routes", help="print the route table")
    p_routes.add_argument("target", help="module[:attr]")
    p_routes.set_defaults(func=_cmd_routes)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
