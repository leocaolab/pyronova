//! Opt-in PyObject lifecycle diagnostics: a histogram of refcounts at the moment a worker
//! lets go of a request's objects (the `leak_detect` Cargo feature; compiled out of
//! default builds). Use it when chasing a sub-interpreter leak; see
//! docs/memory-leak-investigation-2026-04-19.md.
//!
//! Reading the histogram, per type:
//!   rc=1 → healthy (the worker's release frees it)
//!   rc=2 → one co-owner (e.g. an instance attribute), healthy where that is expected
//!   rc>=3 persistently, or rc<0 → a refcount bug
//!
//! Each sample is `metrics::counter!("pyronova_drop_rc", "type" => T, "rc" => N)` into a
//! `DebuggingRecorder`.
//!
//! How to use:
//!
//!   maturin develop --release --features leak_detect
//!   python examples/hello.py &
//!   wrk -t4 -c100 -d10s http://127.0.0.1:8000/
//!   python -c 'from pyronova.engine import leak_detect_dump; leak_detect_dump()'
//!
//! or, inline in a test:
//!
//!   @app.get("/leak_dump")
//!   def leak_dump(req):
//!       from pyronova.engine import leak_detect_dump
//!       leak_detect_dump()
//!       return "dumped"
//!
//! Output (stderr):
//!
//!   [leak_detect] pyronova_drop_rc{type="dict",rc="2"} = 8_500_000
//!   [leak_detect] pyronova_drop_rc{type="str",rc="1"} = 15_200_000
//!   [leak_detect] pyronova_drop_rc{type="Request",rc="1"} = 2_000_000

use std::sync::OnceLock;

use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
use pyo3::ffi;

/// The recorder's snapshotter, set at first use: `Some` when our `DebuggingRecorder` is
/// the process's recorder, `None` when another was installed first and no sample will ever
/// arrive, so the dump reports that instead of an empty histogram.
static SNAPSHOTTER: OnceLock<Option<Snapshotter>> = OnceLock::new();

fn ensure_recorder_installed() -> Option<&'static Snapshotter> {
    SNAPSHOTTER
        .get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snap = recorder.snapshotter();
            match recorder.install() {
                Ok(()) => Some(snap),
                Err(e) => {
                    eprintln!(
                        "[leak_detect] DebuggingRecorder::install() failed: {e}; \
                         leak diagnostic will be empty (another metrics recorder \
                         is already installed in this process)"
                    );
                    None
                }
            }
        })
        .as_ref()
}

/// Samples the refcount of a request's object in a worker (its `Request`, just before the
/// worker lets go of it: refcount 1 is healthy). Takes the type-name table's lock on
/// every call; a diagnostic build only.
#[inline(never)] // kept out of line, off the request path's instruction cache
pub fn record_drop(obj: &pyo3::Bound<'_, pyo3::PyAny>) {
    ensure_recorder_installed();
    let ptr = obj.as_ptr();

    // SAFETY: `obj` is a live object of an attached interpreter (the `Bound`'s token).
    let rc = unsafe { ffi::Py_REFCNT(ptr) };
    // SAFETY: as above; `tp_name` belongs to the type object, which outlives this
    // instance of it.
    let type_name: &'static str = unsafe {
        let t = ffi::Py_TYPE(ptr);
        if t.is_null() {
            "<null_type>"
        } else {
            let name_ptr = (*t).tp_name;
            if name_ptr.is_null() {
                "<unnamed>"
            } else {
                // SAFETY: non-null; read with a length cap (see `type_name_bounded`).
                type_name_bounded(name_ptr)
            }
        }
    };

    let rc_label = rc_label(rc);
    metrics::counter!("pyronova_drop_rc", "type" => type_name, "rc" => rc_label).increment(1);
}

/// Prints the top `pyronova_drop_rc` counters to stderr
/// (`pyronova.engine.leak_detect_dump()`).
pub fn dump_to_stderr() {
    let snap = match SNAPSHOTTER.get() {
        None => {
            eprintln!("[leak_detect] no recorder installed yet (no drops sampled)");
            return;
        }
        Some(None) => {
            eprintln!(
                "[leak_detect] recorder install failed earlier — another metrics \
                 recorder owns this process; no samples were captured"
            );
            return;
        }
        Some(Some(snap)) => snap,
    };
    let mut rows: Vec<(String, u64)> = snap
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _unit, _desc, value)| {
            let metrics_util::debugging::DebugValue::Counter(total) = value else {
                return None;
            };
            let name = key.key().name();
            if name != "pyronova_drop_rc" {
                return None;
            }
            let labels: Vec<String> = key
                .key()
                .labels()
                .map(|l| format!("{}={:?}", l.key(), l.value()))
                .collect();
            Some((format!("{}{{{}}}", name, labels.join(",")), total))
        })
        .collect();
    rows.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    eprintln!("[leak_detect] --- pyronova_drop_rc snapshot (top {DUMP_ROWS}) ---");
    for (label, n) in rows.iter().take(DUMP_ROWS) {
        eprintln!("[leak_detect]   {label} = {n}");
    }
    if rows.is_empty() {
        eprintln!("[leak_detect]   (no samples — is the feature enabled and drops flowing?)");
    }
}

/// Counter rows `dump_to_stderr` prints, largest first.
const DUMP_ROWS: usize = 30;

/// Reads a `tp_name` C string, stopping at its NUL or after `MAX` bytes, and interns it.
/// `record_drop` samples corrupted objects by design (the `"<0"` bucket), whose `tp_name`
/// may have no NUL: `CStr::from_ptr`'s unbounded scan could run into unmapped memory.
///
/// # Safety
/// `ptr` must be non-null and point at readable memory for at least its
/// NUL-terminated length (or `MAX` bytes if not terminated within `MAX`).
unsafe fn type_name_bounded(ptr: *const std::os::raw::c_char) -> &'static str {
    const MAX: usize = 256;
    let mut len = 0usize;
    while len < MAX {
        if *ptr.add(len) == 0 {
            break;
        }
        len += 1;
    }
    if len == MAX {
        // No NUL within MAX bytes — treat as corrupt rather than interning
        // arbitrarily long garbage.
        return "<corrupt_type_name>";
    }
    // Every byte in 0..len was just read above, so the slice is valid.
    let bytes = std::slice::from_raw_parts(ptr as *const u8, len);
    match std::str::from_utf8(bytes) {
        Ok(s) => intern(s),
        Err(_) => "<non_utf8_type>",
    }
}

/// Interns a type name, so a `metrics` label can be `&'static str`. Leaks one string per
/// distinct type name, a few dozen over a process's life.
fn intern(s: &str) -> &'static str {
    use parking_lot::Mutex;

    static TABLE: OnceLock<Mutex<std::collections::HashMap<String, &'static str>>> =
        OnceLock::new();
    let t = TABLE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let mut g = t.lock();
    if let Some(&cached) = g.get(s) {
        return cached;
    }
    let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
    g.insert(s.to_string(), leaked);
    leaked
}

/// A refcount's label: `"0"`..`"8"`, `"9+"`, or `"<0"`, which only a corrupted
/// `ob_refcnt` (double free, use after free, an FFI over-decref) can produce, so it is
/// never folded into `"9+"`.
fn rc_label(rc: ffi::Py_ssize_t) -> &'static str {
    const PRECOMPUTED: &[&str] = &["0", "1", "2", "3", "4", "5", "6", "7", "8"];
    if rc < 0 {
        "<0"
    } else if (0..PRECOMPUTED.len() as ffi::Py_ssize_t).contains(&rc) {
        PRECOMPUTED[rc as usize]
    } else {
        "9+"
    }
}
