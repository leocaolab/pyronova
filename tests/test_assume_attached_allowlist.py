"""No `Python::assume_attached` outside an audited allowlist.

`assume_attached` hands out a token without telling PyO3 the thread is attached, so a
`Py<T>` dropped under it is deferred instead of decref'd, and nothing checks that the
current thread state is the one the code thinks it is. It is right only where the
current thread state is known and `Python::attach` would pick the wrong one (a worker's
init on the main OS thread, whose gilstate thread state is main's). Each allowlisted site
states why; the counts are exact, so a new site fails and a removed one must come off.
Companion to `test_attach_allowlist.py`.
"""
from __future__ import annotations

import os
import re
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ASSUME = re.compile(r"assume_attached\s*\(")

# path (relative to the repo root) -> (exact count, why it is safe)
ALLOWLIST = {
    "src/python/worker.rs": (
        1,
        "with_current_tstate: a worker's init, on the main OS thread with the new worker's "
        "thread state current (Python::attach there would switch to main's gilstate thread "
        "state), and its end; Py<T> released in it goes through drop_ref",
    ),
}


def _strip_comments(line: str) -> str:
    return line.split("//", 1)[0]


def sites(root: str) -> dict[str, list[int]]:
    found: dict[str, list[int]] = {}
    for dirpath, _, files in os.walk(os.path.join(root, "src")):
        for name in files:
            if not name.endswith(".rs"):
                continue
            path = os.path.join(dirpath, name)
            rel = os.path.relpath(path, root)
            with open(path, encoding="utf-8") as f:
                for n, line in enumerate(f, 1):
                    if ASSUME.search(_strip_comments(line)):
                        found.setdefault(rel, []).append(n)
    return found


def violations(root: str) -> list[str]:
    out = []
    found = sites(root)
    for rel, lines in sorted(found.items()):
        allowed = ALLOWLIST.get(rel, (0, ""))[0]
        if len(lines) != allowed:
            out.append(
                f"{rel}: {len(lines)} assume_attached site(s) at lines {lines}, allowlist has "
                f"{allowed}. Use Python::attach / run_context::attach_to, or audit the site "
                f"and update ALLOWLIST with the reason."
            )
    for rel, (allowed, _) in sorted(ALLOWLIST.items()):
        if rel not in found:
            out.append(f"{rel}: allowlisted for {allowed} site(s) but has none; remove it")
    return out


def test_no_assume_attached_outside_allowlist():
    assert violations(ROOT) == []


def test_gate_catches_a_seeded_assume_attached(tmp_path):
    shutil.copytree(os.path.join(ROOT, "src"), tmp_path / "src")
    target = tmp_path / "src" / "handlers.rs"
    with open(target, "a", encoding="utf-8") as f:
        f.write(
            "\n#[allow(dead_code)]\nfn seeded() {\n"
            "    let _py = unsafe { pyo3::Python::assume_attached() };\n}\n"
        )
    found = violations(str(tmp_path))
    assert len(found) == 1, found
    assert found[0].startswith("src/handlers.rs: 1 assume_attached site(s)"), found
