"""FR-12 / E2E-9 (Layer 2, M0): no bare `Python::attach` outside an audited allowlist.

Once more than one interpreter has executed the engine, a bare attach on a thread with no
Python thread state is refused by the PyO3 fork, and on a thread bound to a worker it
would run under the worker's GIL. Code that enters Python from a Rust thread goes
through `run_context::main_attach` / `attach_to` instead. Each allowlisted site states
why it is safe; the counts are exact, so a new site fails and a removed one must be
taken off the list.
"""
from __future__ import annotations

import os
import re
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ATTACH = re.compile(r"Python::(attach|with_gil|try_attach)\s*\(")

# path (relative to the repo root) -> (exact count, why it is safe)
ALLOWLIST = {
    "src/run_context.rs": (
        1,
        "attach_to itself: runs only when the thread is already bound to the target "
        "interpreter, so it re-attaches that thread state",
    ),
    "src/python/worker.rs": (
        1,
        "build_request: on a worker thread whose own thread state is current "
        "(SubInterpGilGuard); a re-entrant attach",
    ),
    "src/bridge/db_bridge.rs": (
        1,
        "C-FFI DB bridge, called from worker Python with its thread state current; "
        "deleted in Layer 2 M4 (#6)",
    ),
    "src/db.rs": (
        3,
        "PgPool *_async resolvers on pyo3-async-runtimes threads; replaced by the "
        "interpreter-generic resolver in Layer 2 M1 (#3)",
    ),
}


def _strip_comments(line: str) -> str:
    return line.split("//", 1)[0]


def attach_sites(root: str) -> dict[str, list[int]]:
    """{relative path: [line numbers]} of attach calls in non-comment Rust code."""
    sites: dict[str, list[int]] = {}
    src = os.path.join(root, "src")
    for dirpath, _, files in os.walk(src):
        for name in files:
            if not name.endswith(".rs"):
                continue
            path = os.path.join(dirpath, name)
            rel = os.path.relpath(path, root)
            with open(path, encoding="utf-8") as f:
                for n, line in enumerate(f, 1):
                    if ATTACH.search(_strip_comments(line)):
                        sites.setdefault(rel, []).append(n)
    return sites


def violations(root: str) -> list[str]:
    out = []
    sites = attach_sites(root)
    for rel, lines in sorted(sites.items()):
        allowed = ALLOWLIST.get(rel, (0, ""))[0]
        if len(lines) != allowed:
            out.append(
                f"{rel}: {len(lines)} bare attach site(s) at lines {lines}, allowlist has "
                f"{allowed}. Use run_context::main_attach / attach_to, or audit the site "
                f"and update ALLOWLIST with the reason."
            )
    for rel, (allowed, _) in sorted(ALLOWLIST.items()):
        if rel not in sites:
            out.append(f"{rel}: allowlisted for {allowed} site(s) but has none; remove it")
    return out


def test_no_bare_attach_outside_allowlist():
    assert violations(ROOT) == []


def test_gate_catches_a_seeded_bare_attach(tmp_path):
    """E2E-9: the gate must fail on a new bare attach, not just pass on today's tree."""
    shutil.copytree(os.path.join(ROOT, "src"), tmp_path / "src")
    target = tmp_path / "src" / "handlers.rs"
    with open(target, "a", encoding="utf-8") as f:
        f.write("\n#[allow(dead_code)]\nfn seeded() {\n    pyo3::Python::attach(|_py| ());\n}\n")
    found = violations(str(tmp_path))
    assert len(found) == 1, found
    assert found[0].startswith("src/handlers.rs: 1 bare attach site(s)"), found
