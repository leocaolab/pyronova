//! Opt-in PyObject lifecycle diagnostics.
//!
//! Gated behind the `leak_detect` Cargo feature. Compiled out of default
//! builds. Use only when chasing a sub-interpreter leak.
//!
//! Why it exists:
//!   The v1.4.5 investigation (see
//!   docs/memory-leak-investigation-2026-04-19.md) turned on the fact
//!   that CPython sub-interpreters under PEP 684 OWN_GIL do NOT run
//!   Python-level finalizers. Every request's `_Request` dropped
//!   with refcount 0 but its headers/params dicts stayed alive at
//!   refcount >= 2 forever. A "refcount histogram at drop time" probe
//!   exposed the fingerprint:
//!     rc=1 → healthy (our DECREF frees it)
//!     rc=2 → one co-owner (instance field) — healthy if that's expected
//!     rc>=3 persistently on the same type → FFI refcount bug
//!
//! How it works:
//!   The hot path samples every `PyObjRef::Drop` call into a
//!   `metrics::counter!("pyronova_drop_rc", "type" => T, "rc" => N)`.
//!   The `metrics` facade compiles the per-label counter down to a
//!   single pointer chase into a `DebuggingRecorder`-owned atomic — no
//!   mutex, no HashMap lookup, no string allocation on the hot path
//!   after the first call with a given label set.
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
//!   [leak_detect] pyronova_drop_rc{type="_Request",rc="1"} = 2_000_000
//!
//! A type consistently showing at rc>=2 (other than values stored as
//! instance attributes where that's expected) is the leak.

use std::sync::OnceLock;

// arc finding leak-detect-2: record_drop's mutex .unwrap() panics on
// poisoning, cascading one panic in this diagnostic module into all
// subsequent drops failing. Use unwrap_or_else(|e| e.into_inner()) at
// every site that locks the per-type tally Mutex.

use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
use pyo3::ffi;

/// Global snapshotter slot. We install a DebuggingRecorder at first use
/// and, *only on success*, hold onto its snapshotter so the
/// Python-callable dump can render totals on demand.
///
/// The slot is `Option<Snapshotter>` so the install outcome is modeled
/// explicitly: `Some` means our recorder owns the process and samples
/// flow to this snapshotter; `None` means install failed (another
/// recorder was registered first) and no samples will ever arrive. We
/// deliberately do NOT keep the snapshotter on failure — even though it
/// would stay memory-safe (it is `Arc`-backed), it would be permanently
/// dead, and reading from it would produce a misleading empty dump that
/// hides the real cause. Storing `None` lets the dump report the actual
/// failure instead of relying on "the dead snapshotter happens to read
/// empty" (arc finding leak-detect-1).
static SNAPSHOTTER: OnceLock<Option<Snapshotter>> = OnceLock::new();

fn ensure_recorder_installed() -> Option<&'static Snapshotter> {
    SNAPSHOTTER
        .get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snap = recorder.snapshotter();
            // `install()` consumes the recorder. On success the global
            // recorder is registered (our recorder stays alive) and the
            // snapshotter is the live view of its storage. On failure the
            // recorder is dropped here; the snapshotter would be dead, so
            // we discard it and record `None`. Pre-fix this failure was
            // silent (`let _ = ...`); now surface it so an empty leak
            // diagnostic isn't mysterious.
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

/// Sample a PyObjRef drop. Called unconditionally from `PyObjRef::Drop`
/// when the `leak_detect` feature is enabled.
///
/// Hot-path cost after the first call with a given (type, rc) label
/// pair: one static str compare + one atomic increment. The `metrics`
/// facade intentionally avoids touching a mutex or a HashMap on the
/// sampled path.
///
/// # Safety
/// `ptr` must be a valid PyObject the caller owns a reference to, and
/// the caller must hold the owning sub-interpreter's GIL.
#[inline(never)] // keep cold — do not pollute icache of the real hot path
pub unsafe fn record_drop(ptr: *mut ffi::PyObject) {
    if ptr.is_null() {
        return;
    }
    ensure_recorder_installed();

    let rc = ffi::Py_REFCNT(ptr);
    // `tp_name` is a stable `const char*` owned by the type object —
    // the type object itself can't be deallocated while we hold a ref
    // to an instance of it, so the borrow is safe for the duration of
    // this call.
    let type_name: &'static str = {
        let t = ffi::Py_TYPE(ptr);
        if t.is_null() {
            "<null_type>"
        } else {
            let name_ptr = (*t).tp_name;
            if name_ptr.is_null() {
                "<unnamed>"
            } else {
                // SAFETY: `name_ptr` is non-null. We read it with a hard
                // length cap rather than `CStr::from_ptr` because this
                // probe deliberately fires on corrupted objects too — the
                // `"<0"` refcount bucket exists to surface double-free /
                // use-after-free / FFI over-DECREF. A corrupted `tp_name`
                // may be non-NUL-terminated, and `CStr::from_ptr`'s
                // unbounded `strlen` scan would then run off the end of
                // the allocation into unmapped pages (buffer over-read,
                // UB). The interned label is `&'static`.
                type_name_bounded(name_ptr)
            }
        }
    };

    let rc_label = rc_label(rc);
    metrics::counter!("pyronova_drop_rc", "type" => type_name, "rc" => rc_label).increment(1);
}

/// Print a snapshot of the `pyronova_drop_rc` counters to stderr. Called
/// from Python via `pyronova.engine.leak_detect_dump()` (the
/// function is registered in `lib.rs` only when this feature is on).
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
            let (kind, total) = match value {
                metrics_util::debugging::DebugValue::Counter(n) => ("counter", n),
                _ => return None,
            };
            let _ = kind; // we only emit counters
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
    eprintln!("[leak_detect] --- pyronova_drop_rc snapshot (top 30) ---");
    for (label, n) in rows.iter().take(30) {
        eprintln!("[leak_detect]   {label} = {n}");
    }
    if rows.is_empty() {
        eprintln!("[leak_detect]   (no samples — is the feature enabled and drops flowing?)");
    }
}

// ── Small helpers ──────────────────────────────────────────────────

/// Read a `tp_name` C string with a hard length cap and intern it.
///
/// Unlike `CStr::from_ptr`, which does an *unbounded* `strlen` scan, this
/// reads one byte at a time and stops at the terminating NUL **or** after
/// `MAX` bytes, whichever comes first. That matters because `record_drop`
/// runs on corrupted objects by design (the `"<0"` refcount bucket): a
/// corrupted `tp_name` may be non-NUL-terminated, and an unbounded scan
/// would walk off the allocation into unmapped pages (UB). Reading
/// byte-by-byte also means we never touch memory past the NUL — for a
/// healthy name the cost is identical to `strlen`.
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

/// Intern a type name into a static string table so the `metrics`
/// labels can be `&'static str`. Sub-interpreter type names are
/// enumerable — the worst case is a few dozen entries over a process
/// lifetime, so a Mutex<HashMap<String, &'static str>> is cheap
/// (contention is cold-path only; the hot path sees repeated lookups
/// hit the same &'static str and skip the mutex).
fn intern(s: &str) -> &'static str {
    use std::sync::Mutex;

    static TABLE: OnceLock<Mutex<std::collections::HashMap<String, &'static str>>> =
        OnceLock::new();
    let t = TABLE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    // Recover from poisoning — leak diagnostics must not cascade a
    // single panic into permanent failure of the whole module
    // (arc leak-detect-2).
    let mut g = t.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(&cached) = g.get(s) {
        return cached;
    }
    let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
    g.insert(s.to_string(), leaked);
    leaked
}

/// Bucket refcounts into labels. Label strings must be &'static so we
/// precompute 0..=8 and a catch-all. 99% of samples land in the small
/// range in practice.
///
/// A negative refcount is impossible in a healthy CPython heap — it means
/// the object's `ob_refcnt` field has been corrupted (double-free,
/// use-after-free, or an FFI over-DECREF). That is the single most
/// valuable signal this probe can surface, so it gets its own `"<0"`
/// bucket instead of being folded into `"9+"` where it would be
/// indistinguishable from a benign high refcount.
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
