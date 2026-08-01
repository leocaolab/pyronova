"""Regression for the FFI-panic UB (audit round 5 bug #5).

Since Rust 1.81 a panic crossing an `extern "C"` boundary aborts the
process (was UB before). Either way: a .unwrap() on a poisoned Mutex
or an unexpected None in pyronova_recv / pyronova_send / pyronova_emit_log would
take the whole server down at the worst possible moment.

Fix: each FFI entry point is now a one-line `extern "C"` wrapper that
calls `ffi_catch_unwind` around a safe `_inner` body. On a caught
panic the helper logs via tracing (async, non-blocking) and sets a
Python RuntimeError so the Python caller sees a normal exception.

Structural test: verify the three C-FFI entry points all go through
ffi_catch_unwind. An actual panic test would require triggering a
Rust panic from inside a C callback, which is fiddly; the structural
check stops the wrapper from being accidentally stripped.
"""

import pathlib


def test_ffi_catch_unwind_helper_defined():
    src = "\n".join(p.read_text() for p in pathlib.Path("src/python").glob("*.rs"))
    assert "unsafe fn ffi_catch_unwind" in src
    assert "std::panic::catch_unwind" in src
    # On caught panic, must log + set PyRuntimeError + return NULL.
    helper_idx = src.find("unsafe fn ffi_catch_unwind")
    helper_body = src[helper_idx:helper_idx + 2000]
    assert "PyErr_SetString" in helper_body, (
        "caught panic must set a Python RuntimeError so callers see a "
        "normal exception rather than 'returned NULL without setting "
        "an exception'"
    )
    assert 'tracing::error!' in helper_body, (
        "caught panic must also log to tracing so ops sees what happened"
    )


def test_all_three_ffi_entry_points_guarded():
    src = "\n".join(p.read_text() for p in pathlib.Path("src/python").glob("*.rs"))
    # The 3 functions registered into sub-interpreter globals as
    # _pyronova_recv / _pyronova_send / _pyronova_emit_log.
    ffi_fns = ["pyronova_recv_cfunc", "pyronova_send_cfunc", "pyronova_emit_log_cfunc"]
    for fn in ffi_fns:
        idx = src.find(f'unsafe extern "C" fn {fn}')
        assert idx != -1, f"entry point {fn} not found"
        # First 1000 chars of the function body should contain the guard.
        body = src[idx:idx + 1000]
        assert "ffi_catch_unwind" in body, (
            f"{fn} must be wrapped in ffi_catch_unwind — a Rust panic "
            f"through this extern C would abort the process"
        )
