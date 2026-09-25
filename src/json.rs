//! JSON text → Python value, with isojson (the codec responses are encoded with).
//!
//! isojson holds an integer only within `i64::MIN..=u64::MAX`; a wider one comes back as a
//! float, silently rounded. A document that may hold such an integer (a run of
//! [`WIDE_DIGITS`] or more digits) is decoded by the standard library instead, which keeps
//! every integer exact and rounds floats the same way isojson does. Both reject what JSON
//! does not allow (`NaN`, `Infinity`, a number too large for a float).

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

/// The shortest digit run that can spell an integer outside `i64::MIN..=u64::MAX`:
/// `-9223372036854775809` has 19 digits.
const WIDE_DIGITS: usize = 19;

/// Per interpreter: `PyOnceLock` is per-interpreter with the PyO3 fork.
static DECODERS: PyOnceLock<Decoders> = PyOnceLock::new();

struct Decoders {
    fast: Py<PyAny>,
    exact: Py<PyAny>,
}

const EXACT_SOURCE: &std::ffi::CStr = c"
import json as _json
import math as _math


def _constant(name):
    raise ValueError(f'{name} is not a JSON value')


def _float(text):
    value = float(text)
    if not _math.isfinite(value):
        raise ValueError(f'number {text} is out of range')
    return value


def loads(text):
    return _json.loads(text, parse_constant=_constant, parse_float=_float)
";

fn decoders(py: Python<'_>) -> PyResult<&Decoders> {
    DECODERS.get_or_try_init(py, || {
        let exact = pyo3::types::PyModule::from_code(
            py,
            EXACT_SOURCE,
            c"pyronova_json_exact",
            c"pyronova_json_exact",
        )?;
        Ok(Decoders {
            fast: py.import("isojson")?.getattr("loads")?.unbind(),
            exact: exact.getattr("loads")?.unbind(),
        })
    })
}

/// `text` decoded as JSON. A document that isn't JSON raises `ValueError` (isojson's or the
/// standard library's `JSONDecodeError`).
pub(crate) fn loads<'py>(py: Python<'py>, text: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    let decoders = decoders(py)?;
    let decoder = if may_hold_wide_integer(text) {
        &decoders.exact
    } else {
        &decoders.fast
    };
    decoder
        .bind(py)
        .call1((pyo3::types::PyBytes::new(py, text),))
}

/// Whether `text` has a run of [`WIDE_DIGITS`] or more ASCII digits. A run inside a string
/// or a float's mantissa also counts: it only sends the document to the exact decoder.
fn may_hold_wide_integer(text: &[u8]) -> bool {
    text.split(|b| !b.is_ascii_digit())
        .any(|run| run.len() >= WIDE_DIGITS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_isojson_holds_take_the_fast_decoder() {
        assert!(!may_hold_wide_integer(b"{\"a\": 9223372036854775}"));
        assert!(!may_hold_wide_integer(b"[1, 2.5, -3e10, \"x\"]"));
        assert!(!may_hold_wide_integer(b""));
    }

    #[test]
    fn wider_integers_take_the_exact_decoder() {
        assert!(may_hold_wide_integer(b"{\"a\": 18446744073709551616}"));
        assert!(may_hold_wide_integer(b"-9223372036854775809"));
        assert!(may_hold_wide_integer(b"[1, 12345678901234567890123]"));
    }
}
