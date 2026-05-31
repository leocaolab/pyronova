//! String-conversion helpers across the CPython FFI boundary.
//!
//! Pure utilities that build Python `str`/`dict` objects from Rust
//! values and extract Rust `String`s back out. All return owned
//! `PyObjRef`s (see `super::ffi::PyObjRef`) and clear any pending
//! Python exception on failure.

use std::collections::HashMap;

use pyo3::ffi;

use super::ffi::*;

// ---------------------------------------------------------------------------
// Helper: create Python string from Rust &str
// ---------------------------------------------------------------------------

/// Create a new Python unicode string. Returns an owned `PyObjRef`.
pub(crate) unsafe fn py_str(s: &str) -> Option<PyObjRef> {
    PyObjRef::from_owned(ffi::PyUnicode_FromStringAndSize(
        s.as_ptr() as *const _,
        s.len() as isize,
    ))
}

/// Create a new Python dict from a HashMap<String, String>. Returns owned `PyObjRef`.
///
/// On any failure (str alloc OOM or PyDict_SetItem failure), clears the
/// pending Python exception before returning None. Callers must not
/// pass a non-NULL PyObject back to Python with a set exception state
/// (CPython raises SystemError in that case).
/// Capture the currently-set Python exception as a string and route it
/// through Rust's async `tracing` pipeline. Replaces ad-hoc `PyErr_Print`
/// calls on the hot path — `PyErr_Print` writes directly and
/// synchronously to the process's `stderr` fd, which at 500k rps under
/// an error-triggering flood serializes every worker on the kernel
/// stdio lock, burning CPU in kernel-mode context switches while
/// throughput collapses. Goes through `tracing::error!` so the
/// already-configured `tracing_appender::non_blocking` writer swallows
/// it without stalling the request path.
///
/// No-op if no exception is pending. Always clears the error indicator.
pub(crate) unsafe fn log_and_clear_py_exception(context: &str) {
    if ffi::PyErr_Occurred().is_null() {
        return;
    }
    // PyErr_GetRaisedException is the post-3.12 replacement for the
    // Fetch/Normalize/Restore triple — returns a single normalized
    // exception instance and clears the error indicator in one step.
    let exc = ffi::PyErr_GetRaisedException();
    if exc.is_null() {
        return;
    }

    let repr = ffi::PyObject_Str(exc);
    let msg = if !repr.is_null() {
        let mut size: ffi::Py_ssize_t = 0;
        let data = ffi::PyUnicode_AsUTF8AndSize(repr, &mut size);
        let s = if data.is_null() {
            "<non-utf8 exception repr>".to_string()
        } else {
            String::from_utf8_lossy(std::slice::from_raw_parts(data as *const u8, size as usize))
                .into_owned()
        };
        ffi::Py_DECREF(repr);
        s
    } else {
        "<PyObject_Str failed>".to_string()
    };

    ffi::Py_DECREF(exc);
    tracing::error!(target: "pyronova::server", %context, error = %msg, "Python exception");
}

pub(crate) unsafe fn py_str_dict(map: &HashMap<String, String>) -> Option<PyObjRef> {
    let dict = PyObjRef::from_owned(ffi::PyDict_New())?;
    for (k, v) in map {
        let pk = match py_str(k) {
            Some(p) => p,
            None => {
                ffi::PyErr_Clear();
                return None;
            }
        };
        let pv = match py_str(v) {
            Some(p) => p,
            None => {
                ffi::PyErr_Clear();
                return None;
            }
        };
        if ffi::PyDict_SetItem(dict.as_ptr(), pk.as_ptr(), pv.as_ptr()) < 0 {
            ffi::PyErr_Clear();
            return None;
        }
    }
    Some(dict)
}

/// Same as `py_str_dict` but from a Vec of key-value pairs (for path params).
///
/// Same exception-clearing discipline as `py_str_dict` — see doc there.
pub(crate) unsafe fn py_str_dict_from_vec(pairs: &[(String, String)]) -> Option<PyObjRef> {
    let dict = PyObjRef::from_owned(ffi::PyDict_New())?;
    for (k, v) in pairs {
        let pk = match py_str(k) {
            Some(p) => p,
            None => {
                ffi::PyErr_Clear();
                return None;
            }
        };
        let pv = match py_str(v) {
            Some(p) => p,
            None => {
                ffi::PyErr_Clear();
                return None;
            }
        };
        if ffi::PyDict_SetItem(dict.as_ptr(), pk.as_ptr(), pv.as_ptr()) < 0 {
            ffi::PyErr_Clear();
            return None;
        }
    }
    Some(dict)
}

/// Extract a Rust String from a Python str object (raw FFI).
pub(crate) unsafe fn pyobj_to_string(obj: *mut ffi::PyObject) -> Result<String, String> {
    let mut size: isize = 0;
    let ptr = ffi::PyUnicode_AsUTF8AndSize(obj, &mut size);
    if ptr.is_null() {
        ffi::PyErr_Clear(); // Must clear exception before any further C-API calls
        return Err("failed to extract string".to_string());
    }
    let bytes = std::slice::from_raw_parts(ptr as *const u8, size as usize);
    String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())
}
