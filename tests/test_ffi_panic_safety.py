"""A Rust panic in a worker API call must reach Python as a normal exception.

The async engine in a sub-interpreter worker pulls requests and sends responses
through `pyronova.engine._worker_recv` / `_worker_send` (`src/python/worker_api.rs`).
They used to be `extern "C"` functions injected into worker globals, each wrapped in
`ffi_catch_unwind`, because a panic crossing `extern "C"` aborts the process (Rust
1.81+). Layer 2 (M4) replaced them with PyO3 `#[pyfunction]`s. PyO3 would turn a panic
into `PanicException`, a `BaseException` the engine's fetcher thread (which catches
`Exception`) would die on, so each one maps a panic to `RuntimeError` instead.

Structural test: the pyfunctions exist and go through that mapping, and no raw
`extern "C"` entry point is left in `src/python/`. (Rewritten for Layer 2 M4, approved
by the user on 2026-09-23.)
"""

import pathlib
import re

_SRC = pathlib.Path("src/python")


def _worker_api() -> str:
    return (_SRC / "worker_api.rs").read_text()


def test_no_extern_c_entry_points_left():
    for path in _SRC.glob("*.rs"):
        code = "\n".join(line.split("//", 1)[0] for line in path.read_text().splitlines())
        assert 'extern "C" fn' not in code, (
            f"{path}: a raw extern \"C\" entry point is back; expose it as a #[pyfunction] "
            "in worker_api.rs instead"
        )


def test_panic_maps_to_runtime_error():
    src = _worker_api()
    idx = src.find("fn no_panic")
    assert idx != -1, "worker_api.rs must define the panic → RuntimeError helper"
    body = src[idx:idx + 1200]
    assert "std::panic::catch_unwind" in body
    assert "PyRuntimeError::new_err" in body, (
        "a caught panic must become a RuntimeError, not PyO3's PanicException "
        "(a BaseException the fetcher thread would die on)"
    )
    assert "tracing::error!" in body, "a caught panic must also be logged"


def test_worker_api_functions_guarded():
    src = _worker_api()
    for fn in ("_worker_recv", "_worker_send", "_worker_to_response"):
        m = re.search(r"#\[pyfunction\]\s*pub\(crate\) fn " + fn + r"\(", src)
        assert m, f"{fn} must be a #[pyfunction] in worker_api.rs"
        body = src[m.end():m.end() + 800]
        assert f'no_panic("{fn}"' in body, f"{fn} must run its body through no_panic"
