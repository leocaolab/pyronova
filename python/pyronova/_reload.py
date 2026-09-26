"""Hot reload: run the app in a child process, restart it when a .py file changes.

The parent only watches; the child re-runs the exact command that started this
process, with ``CHILD_ENV`` set so it serves instead of reloading again. A child
that exits (a syntax error, a crash on startup) is restarted by the next change.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from dataclasses import dataclass

CHILD_ENV = "_PYRONOVA_RELOAD_CHILD"

_STOP_TIMEOUT_S = 3
_POLL_INTERVAL_S = 1.0
# Wait this long after a change for the editor to finish writing every file.
_DEBOUNCE_MS = 500
_SKIP_DIRS = frozenset({".git", ".venv", "venv", "node_modules", "__pycache__"})


@dataclass(frozen=True)
class ReloadTarget:
    argv: tuple[str, ...]
    watch_dir: str

    @classmethod
    def of_this_process(cls, app_file: str) -> ReloadTarget:
        """Re-run this process's own command line (``sys.orig_argv`` keeps
        ``-m pyronova dev app`` and console-script paths intact), watching the
        directory the app's source lives in."""
        return cls(
            argv=(sys.executable, *sys.orig_argv[1:]),
            watch_dir=os.path.dirname(os.path.abspath(app_file)),
        )


def is_reload_child() -> bool:
    return os.environ.get(CHILD_ENV) == "1"


def run_with_reload(target: ReloadTarget) -> None:
    try:
        import watchfiles
    except ImportError:
        print("  [reload] Install 'watchfiles' for efficient file watching:")
        print("           pip install watchfiles")
        print("  [reload] Falling back to polling mode...")
        _run_polling(target)
        return
    _run_watchfiles(watchfiles, target)


def _run_watchfiles(watchfiles, target: ReloadTarget) -> None:
    print(f"  [reload] Watching {target.watch_dir} for .py changes (watchfiles)...")
    while True:
        proc = _start(target)
        try:
            changes = next(watchfiles.watch(
                target.watch_dir,
                watch_filter=watchfiles.PythonFilter(),
                debounce=_DEBOUNCE_MS,
            ))
        except KeyboardInterrupt:
            _stop(proc)
            return
        _announce([path for _change, path in changes])
        _stop(proc)


def _run_polling(target: ReloadTarget) -> None:
    print(f"  [reload] Watching {target.watch_dir} for .py changes (polling)...")
    snapshot = _snapshot(target.watch_dir)
    while True:
        proc = _start(target)
        try:
            changed, snapshot = _wait_for_change(target.watch_dir, snapshot)
        except KeyboardInterrupt:
            _stop(proc)
            return
        _announce(changed)
        _stop(proc)


def _wait_for_change(watch_dir: str, before: dict) -> tuple[list[str], dict]:
    while _snapshot(watch_dir) == before:
        time.sleep(_POLL_INTERVAL_S)
    time.sleep(_DEBOUNCE_MS / 1000)
    after = _snapshot(watch_dir)
    return _changed(before, after), after


def _changed(before: dict, after: dict) -> list[str]:
    return sorted(f for f in before.keys() | after.keys() if before.get(f) != after.get(f))


def _snapshot(watch_dir: str) -> dict[str, tuple[int, int]]:
    """(mtime_ns, size) of every .py file under ``watch_dir``."""
    snapshot = {}
    for root, dirs, files in os.walk(watch_dir):
        dirs[:] = [d for d in dirs if d not in _SKIP_DIRS]
        for name in files:
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            try:
                st = os.stat(path)
            except FileNotFoundError:
                continue  # deleted while walking; the next snapshot sees it gone
            snapshot[path] = (st.st_mtime_ns, st.st_size)
    return snapshot


def _announce(changed: list[str]) -> None:
    print(f"\n  [reload] File changed: {', '.join(os.path.basename(p) for p in changed[:3])}")
    print("  [reload] Restarting...\n")


def _start(target: ReloadTarget) -> subprocess.Popen:
    return subprocess.Popen(list(target.argv), env={**os.environ, CHILD_ENV: "1"})


def _stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=_STOP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        proc.kill()
