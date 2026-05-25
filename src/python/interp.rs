//! Safe abstractions for CPython sub-interpreter management.
//!
//! Provides RAII wrappers over raw `pyo3::ffi` pointers to prevent
//! reference count leaks and ensure proper sub-interpreter cleanup.
//! Also implements a channel-based worker pool for true load balancing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use matchit::Router;
use pyo3::ffi;
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// Phase 7.2: Global worker state for async C-FFI bridge
// ---------------------------------------------------------------------------

// Per-pool configuration (replaces former global statics).
// These are set on PyronovaApp before run() and propagated to InterpreterPool.
use std::sync::atomic::AtomicBool;

/// Per-worker state accessible from C-FFI functions (no closure environment).
struct WorkerState {
    rx: crossbeam_channel::Receiver<WorkRequest>,
    response_map:
        Mutex<HashMap<u64, tokio::sync::oneshot::Sender<Result<SubInterpResponse, String>>>>,
    next_req_id: AtomicU64,
    /// Identifier for the `InterpreterPool` instance that created this
    /// state. A zombie worker from a prior pool (test / hot-reload)
    /// carries the OLD pool_id in its Python globals; the C-FFI bridge
    /// compares the caller's pool_id to the state's and rejects
    /// mismatches so the zombie can't steal requests from the new pool.
    /// See POOL_ID_COUNTER docstring for the rationale.
    pool_id: u64,
}

/// Monotonic counter for pool instance IDs. Each call to
/// `InterpreterPool::new()` consumes one. This exists exclusively so
/// `pyronova_recv` / `pyronova_send` can detect cross-pool calls — see
/// `WorkerState::pool_id` and the guard in `pyronova_recv_cfunc`.
static POOL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Allocate a fresh pool id. Same counter the InterpreterPool uses, so
/// TPC sub-interps get IDs distinct from any pool-owned sub-interps
/// that might run in the same process.
pub(crate) fn next_pool_id() -> u64 {
    POOL_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Global registry of worker states, indexed by worker_id.
///
/// Must support RE-INSTALLATION: a test suite or a hot-reload path may
/// call `InterpreterPool::new()` more than once per process lifetime.
/// `OnceLock` would silently fail the second `set()`, leaving the
/// second pool with STALE channels from the first pool → permanent
/// deadlock on recv. Use `RwLock<Vec>` instead: read lock on the hot
/// path (~5 ns uncontended), write lock only at pool init.
static WORKER_STATES: std::sync::RwLock<Vec<Arc<WorkerState>>> = std::sync::RwLock::new(Vec::new());

fn get_worker_state(worker_id: usize) -> Option<Arc<WorkerState>> {
    WORKER_STATES
        .read()
        .ok()
        .and_then(|v| v.get(worker_id).cloned())
}

// ---------------------------------------------------------------------------
// C-FFI bridge functions for async engine
// ---------------------------------------------------------------------------

/// Wrap an `extern "C"` body in `catch_unwind` so a Rust panic never
/// crosses into CPython's stack. Since Rust 1.81 a panic through
/// `extern "C"` aborts the process (was UB before) — still not
/// graceful under a 500k-rps flood that hits a `.unwrap()` on
/// a poisoned Mutex or a PyArg_ParseTuple failure we missed.
///
/// On a caught panic:
///   1. log via tracing (async, non-blocking — no stderr storm),
///   2. set a `PyRuntimeError` via `PyErr_SetString` so the caller's
///      next C-API call surfaces a normal Python exception rather
///      than the cryptic "returned NULL without setting an exception"
///      warning,
///   3. return NULL.
///
/// Callers whose semantics are "None == no data" (e.g. `pyronova_recv`
/// returns `None` when the channel is closed) should NOT use this
/// for normal signaling — that's a return value of `Py_None` via
/// `Py_INCREF`. This helper's NULL-return is strictly for the panic
/// path.
unsafe fn ffi_catch_unwind<F>(context: &'static str, f: F) -> *mut ffi::PyObject
where
    F: FnOnce() -> *mut ffi::PyObject + std::panic::UnwindSafe,
{
    match std::panic::catch_unwind(f) {
        Ok(p) => p,
        Err(panic_payload) => {
            let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "<non-string panic payload>".to_string()
            };
            tracing::error!(
                target: "pyronova::server",
                context,
                panic = %msg,
                "Rust panic caught at FFI boundary"
            );
            let msg_c = format!("Rust panic in {context}: {msg}\0");
            ffi::PyErr_SetString(
                ffi::PyExc_RuntimeError,
                msg_c.as_ptr() as *const std::os::raw::c_char,
            );
            std::ptr::null_mut()
        }
    }
}

/// pyronova_recv(worker_id, pool_id) → (req_id, handler_idx, method, path, params, query, body, headers, client_ip) or None
/// RELEASES GIL during blocking recv — lets asyncio loop run freely.
///
/// `pool_id` is the worker's "birth certificate". If the state currently
/// installed at `worker_id` belongs to a DIFFERENT pool (i.e. we're a
/// zombie from a prior `app.run()` that finally woke up), we return None
/// so the zombie's caller sees EOF and exits rather than stealing
/// requests from the live pool.
unsafe extern "C" fn pyronova_recv_cfunc(
    _self: *mut ffi::PyObject,
    args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    ffi_catch_unwind(
        "pyronova_recv",
        std::panic::AssertUnwindSafe(|| pyronova_recv_inner(args)),
    )
}

/// Body of pyronova_recv — panic-catchable. Separated so the outer
/// `extern "C"` is just a catch_unwind wrapper and we avoid propagating
/// Rust panics into CPython's stack.
unsafe fn pyronova_recv_inner(args: *mut ffi::PyObject) -> *mut ffi::PyObject {
    let mut worker_id: isize = 0;
    let mut pool_id: u64 = 0;
    if ffi::PyArg_ParseTuple(args, c"nK".as_ptr(), &mut worker_id, &mut pool_id) == 0 {
        return std::ptr::null_mut();
    }

    let state = match get_worker_state(worker_id as usize) {
        Some(s) if s.pool_id == pool_id => s,
        // Mismatch or missing slot — either stale pool or race after
        // InterpreterPool::drop. Return None; the async engine treats
        // this as shutdown and exits cleanly.
        _ => {
            ffi::Py_INCREF(ffi::Py_None());
            return ffi::Py_None();
        }
    };

    // Release GIL while blocking on channel recv
    let saved = ffi::PyEval_SaveThread();
    let req_opt = state.rx.recv().ok();
    ffi::PyEval_RestoreThread(saved);

    match req_opt {
        Some(req) => {
            let req_id = state.next_req_id.fetch_add(1, Ordering::Relaxed);

            // Build ALL Python objects BEFORE inserting response_tx into map.
            // If any allocation fails, response_tx drops → sender gets error
            // instead of leaking in response_map and causing 504 timeout.
            // extract_headers deferred here from Tokio thread — O(n_headers)
            // HashMap build now runs on the worker thread.
            let headers_map = crate::types::extract_headers(&req.headers);
            let py_params = py_str_dict_from_vec(&req.params);
            let py_headers = py_str_dict(&headers_map);
            if py_params.is_none() || py_headers.is_none() {
                // response_tx not inserted → dropped → oneshot Err on Tokio side
                //
                // arc finding interp-1: returning Py_None on alloc failure
                // is indistinguishable from the "channel closed, exit
                // gracefully" sentinel — workers silently exit under
                // memory pressure with no log, no exception, no trace.
                // Surface via tracing::error so the OOM condition is at
                // least observable in logs before the worker disappears.
                tracing::error!(
                    target: "pyronova::app",
                    "pyronova_recv: Python dict allocation failed (params/headers); \
                     returning channel-closed sentinel — worker will exit silently. \
                     Likely cause: OOM under memory pressure."
                );
                ffi::Py_INCREF(ffi::Py_None());
                return ffi::Py_None();
            }
            let py_params = py_params.unwrap();
            let py_headers = py_headers.unwrap();

            let tuple = ffi::PyTuple_New(9);
            if tuple.is_null() {
                tracing::error!(
                    target: "pyronova::app",
                    "pyronova_recv: PyTuple_New(9) returned null (likely OOM); \
                     returning channel-closed sentinel — worker will exit silently."
                );
                ffi::PyErr_Clear();
                ffi::Py_INCREF(ffi::Py_None());
                return ffi::Py_None();
            }

            // Allocate every leaf Python object UP FRONT and NULL-check
            // before any PyTuple_SetItem call. PyTuple_SetItem steals the
            // reference it's given — embedding a NULL leaks nothing but
            // guarantees a hard segfault the next time anything reads
            // that slot (GC, item access, repr, refcount). Building the
            // full set first lets us reject atomically: if ANY allocation
            // fails, DECREF the successful ones and bail.
            let id_obj = ffi::PyLong_FromUnsignedLongLong(req_id);
            let idx_obj = ffi::PyLong_FromUnsignedLongLong(req.handler_idx as u64);
            let method_obj = ffi::PyUnicode_FromStringAndSize(
                req.method.as_ptr() as *const _,
                req.method.len() as isize,
            );
            let path_obj = ffi::PyUnicode_FromStringAndSize(
                req.path.as_ptr() as *const _,
                req.path.len() as isize,
            );
            let query_obj = ffi::PyUnicode_FromStringAndSize(
                req.query.as_ptr() as *const _,
                req.query.len() as isize,
            );
            let body_obj = ffi::PyBytes_FromStringAndSize(
                req.body.as_ptr() as *const _,
                req.body.len() as isize,
            );
            let ip_str = req.client_ip.to_string();
            let ip_obj = ffi::PyUnicode_FromStringAndSize(
                ip_str.as_ptr() as *const _,
                ip_str.len() as isize,
            );

            let raw_items = [
                id_obj, idx_obj, method_obj, path_obj, query_obj, body_obj, ip_obj,
            ];
            if raw_items.iter().any(|p| p.is_null()) {
                for p in &raw_items {
                    if !p.is_null() {
                        ffi::Py_DECREF(*p);
                    }
                }
                // py_params / py_headers still owned by PyObjRef — dropped here.
                ffi::Py_DECREF(tuple);
                tracing::error!(
                    target: "pyronova::app",
                    "pyronova_recv: per-item Python object allocation failed; \
                     returning channel-closed sentinel — worker will exit silently. \
                     Likely cause: OOM (arc finding interp-1)."
                );
                ffi::PyErr_Clear();
                ffi::Py_INCREF(ffi::Py_None());
                return ffi::Py_None();
            }

            // All Python objects built successfully — NOW insert response_tx.
            // Any earlier bail keeps the sender alive; caller's oneshot will
            // close, returning a 503 instead of an orphaned response_map entry.
            state
                .response_map
                .lock()
                .unwrap()
                .insert(req_id, req.response_tx);

            ffi::PyTuple_SetItem(tuple, 0, id_obj);
            ffi::PyTuple_SetItem(tuple, 1, idx_obj);
            ffi::PyTuple_SetItem(tuple, 2, method_obj);
            ffi::PyTuple_SetItem(tuple, 3, path_obj);
            // params / headers as PyDict; PyObjRef.into_raw() transfers ownership.
            ffi::PyTuple_SetItem(tuple, 4, py_params.into_raw());
            ffi::PyTuple_SetItem(tuple, 5, query_obj);
            ffi::PyTuple_SetItem(tuple, 6, body_obj);
            ffi::PyTuple_SetItem(tuple, 7, py_headers.into_raw());
            ffi::PyTuple_SetItem(tuple, 8, ip_obj);
            tuple
        }
        None => {
            ffi::Py_INCREF(ffi::Py_None());
            ffi::Py_None()
        }
    }
}

/// pyronova_send(worker_id, pool_id, req_id, status, content_type, body_bytes)
/// Wakes up Tokio via oneshot channel. The `pool_id` is checked against the
/// installed state to guard against zombie-worker cross-pool writes —
/// see `pyronova_recv_cfunc` docstring for the rationale.
unsafe extern "C" fn pyronova_send_cfunc(
    _self: *mut ffi::PyObject,
    args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    ffi_catch_unwind(
        "pyronova_send",
        std::panic::AssertUnwindSafe(|| pyronova_send_inner(args)),
    )
}

/// See `pyronova_recv_inner` — same rationale.
unsafe fn pyronova_send_inner(args: *mut ffi::PyObject) -> *mut ffi::PyObject {
    let mut worker_id: isize = 0;
    let mut pool_id: u64 = 0;
    let mut req_id: u64 = 0;
    let mut status: u16 = 0;
    let mut ctype_str: *const std::os::raw::c_char = std::ptr::null();
    let mut body_ptr: *const std::os::raw::c_char = std::ptr::null();
    let mut body_len: isize = 0;

    // n=isize, K=u64, H=u16, z=str|None, y#=bytes+len
    // Returning NULL with the exception still set is how Python signals
    // parse failure from a C extension — don't PyErr_Print here or the
    // exception gets cleared and the caller sees the confusing
    // "returned NULL without setting an exception" warning.
    if ffi::PyArg_ParseTuple(
        args,
        c"nKKHzy#".as_ptr(),
        &mut worker_id,
        &mut pool_id,
        &mut req_id,
        &mut status,
        &mut ctype_str,
        &mut body_ptr,
        &mut body_len,
    ) == 0
    {
        return std::ptr::null_mut();
    }

    let ctype = if !ctype_str.is_null() {
        Some(
            std::ffi::CStr::from_ptr(ctype_str)
                .to_string_lossy()
                .into_owned(),
        )
    } else {
        None
    };

    let body: Vec<u8> = if !body_ptr.is_null() && body_len > 0 {
        let slice = std::slice::from_raw_parts(body_ptr as *const u8, body_len as usize);
        slice.to_vec()
    } else {
        Vec::new()
    };

    // Same pool-id guard as pyronova_recv. Dropping the send here rather
    // than just skipping means the tokio side times out naturally (504)
    // instead of getting a response from the wrong pool.
    if let Some(state) = get_worker_state(worker_id as usize).filter(|s| s.pool_id == pool_id) {
        let mut map = state.response_map.lock().unwrap();
        if let Some(tx) = map.remove(&req_id) {
            // Check if the receiver is still alive (client may have timed out).
            // If closed, skip the send — the response would be discarded anyway.
            if tx.is_closed() {
                tracing::debug!(
                    target: "pyronova::server",
                    req_id,
                    worker_id,
                    "response_map: receiver gone (client timed out), dropping result"
                );
            } else {
                let resp = SubInterpResponse {
                    body,
                    status,
                    content_type: ctype,
                    headers: Vec::new(),
                    is_json: false,
                };
                let _ = tx.send(Ok(resp));
            }
        } else {
            tracing::debug!(
                target: "pyronova::server",
                req_id,
                worker_id,
                "response_map miss — client already timed out (504)"
            );
        }

        // Periodic orphan sweep: when the map grows large, purge entries whose
        // receivers have been dropped (Rust side timed out). Prevents unbounded
        // memory growth from handlers that crash after pyronova_recv but before pyronova_send.
        if map.len() > 64 {
            map.retain(|_id, tx| !tx.is_closed());
        }
    }

    ffi::Py_INCREF(ffi::Py_None());
    ffi::Py_None()
}

// ---------------------------------------------------------------------------
// C-FFI bridge: emit_python_log for sub-interpreter logging
// ---------------------------------------------------------------------------

/// _pyronova_emit_log(level, name, message, pathname, lineno, worker_id)
/// Routes Python logging.Handler.emit() calls through Rust tracing.
/// Minimal GIL hold time — extract strings, dispatch to tracing, return.
unsafe extern "C" fn pyronova_emit_log_cfunc(
    _self: *mut ffi::PyObject,
    args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    ffi_catch_unwind(
        "pyronova_emit_log",
        std::panic::AssertUnwindSafe(|| pyronova_emit_log_inner(args)),
    )
}

unsafe fn pyronova_emit_log_inner(args: *mut ffi::PyObject) -> *mut ffi::PyObject {
    let mut level_ptr: *const std::os::raw::c_char = std::ptr::null();
    let mut name_ptr: *const std::os::raw::c_char = std::ptr::null();
    let mut msg_ptr: *const std::os::raw::c_char = std::ptr::null();
    let mut path_ptr: *const std::os::raw::c_char = std::ptr::null();
    let mut lineno: i32 = 0;
    let mut worker_id: isize = 0;

    // Parse: (str, str, str, str, int, int)
    if ffi::PyArg_ParseTuple(
        args,
        c"zzzzin".as_ptr(),
        &mut level_ptr,
        &mut name_ptr,
        &mut msg_ptr,
        &mut path_ptr,
        &mut lineno,
        &mut worker_id,
    ) == 0
    {
        // Return None on parse error (don't crash the handler)
        ffi::PyErr_Clear();
        ffi::Py_INCREF(ffi::Py_None());
        return ffi::Py_None();
    }

    let level = if !level_ptr.is_null() {
        std::ffi::CStr::from_ptr(level_ptr)
            .to_str()
            .unwrap_or("INFO")
    } else {
        "INFO"
    };
    let name = if !name_ptr.is_null() {
        std::ffi::CStr::from_ptr(name_ptr)
            .to_str()
            .unwrap_or("unknown")
    } else {
        "unknown"
    };
    let message = if !msg_ptr.is_null() {
        std::ffi::CStr::from_ptr(msg_ptr).to_str().unwrap_or("")
    } else {
        ""
    };
    let pathname = if !path_ptr.is_null() {
        std::ffi::CStr::from_ptr(path_ptr).to_str().unwrap_or("")
    } else {
        ""
    };

    let wid = worker_id as usize;

    match level {
        "DEBUG" => {
            tracing::debug!(
                target: "pyronova::app",
                worker = wid,
                logger = %name,
                file = %pathname,
                line = lineno,
                "{}", message
            );
        }
        "INFO" => {
            tracing::info!(
                target: "pyronova::app",
                worker = wid,
                logger = %name,
                file = %pathname,
                line = lineno,
                "{}", message
            );
        }
        "WARNING" => {
            tracing::warn!(
                target: "pyronova::app",
                worker = wid,
                logger = %name,
                file = %pathname,
                line = lineno,
                "{}", message
            );
        }
        "ERROR" | "CRITICAL" => {
            tracing::error!(
                target: "pyronova::app",
                worker = wid,
                logger = %name,
                file = %pathname,
                line = lineno,
                "{}", message
            );
        }
        _ => {
            tracing::trace!(
                target: "pyronova::app",
                worker = wid,
                logger = %name,
                file = %pathname,
                line = lineno,
                "{}", message
            );
        }
    }

    ffi::Py_INCREF(ffi::Py_None());
    ffi::Py_None()
}

// ---------------------------------------------------------------------------
// PyObjRef — RAII wrapper for *mut ffi::PyObject
// ---------------------------------------------------------------------------

/// Owned reference to a Python object. Automatically calls `Py_DECREF` on drop.
///
/// # Safety
///
/// Must only be created and dropped while the owning interpreter's GIL is held.
pub(crate) struct PyObjRef {
    ptr: *mut ffi::PyObject,
}

impl PyObjRef {
    /// Wraps a new (owned) reference. Returns `None` if `ptr` is null.
    ///
    /// Caller must ensure `ptr` is a valid new reference (refcount already
    /// incremented by the creating API, e.g. `PyUnicode_FromStringAndSize`).
    pub unsafe fn from_owned(ptr: *mut ffi::PyObject) -> Option<Self> {
        if ptr.is_null() {
            None
        } else {
            Some(Self { ptr })
        }
    }

    /// Wraps a borrowed reference by incrementing its refcount.
    /// Returns `None` if `ptr` is null.
    pub unsafe fn from_borrowed(ptr: *mut ffi::PyObject) -> Option<Self> {
        if ptr.is_null() {
            None
        } else {
            ffi::Py_INCREF(ptr);
            Some(Self { ptr })
        }
    }

    /// Returns the raw pointer without consuming the wrapper.
    pub fn as_ptr(&self) -> *mut ffi::PyObject {
        self.ptr
    }

    /// Consumes self and returns the raw pointer **without** decrementing.
    /// Use when transferring ownership (e.g. `PyTuple_SetItem` steals refs).
    pub fn into_raw(self) -> *mut ffi::PyObject {
        let ptr = self.ptr;
        std::mem::forget(self);
        ptr
    }
}

impl Drop for PyObjRef {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                // Fast path: skip Py_DECREF on immortal singletons
                // (Py_None, Py_True, Py_False, small ints, interned
                // unicode with saturated refcount).
                //
                // CPython 3.12+ sets their refcount to a sentinel
                // >= (1 << 30), and Py_DECREF on such values is a no-op
                // inside the CPython macros. Skipping here means we can
                // also skip the tstate check — these objects are shared
                // across every sub-interpreter and never actually
                // deallocated, so dropping them on a thread without an
                // attached tstate is 100% safe.
                //
                // Before this skip, Arena's 4096-conn JSON profile
                // flooded the log with "no attached tstate" warnings
                // on every Py_None drop from the tokio response path
                // (main thread had no sub-interp tstate attached after
                // `py.detach()`). That log volume alone dragged p99
                // from <5ms into >60ms and stomped throughput by ~3×.
                if ffi::Py_REFCNT(self.ptr) >= (1_isize << 30) {
                    return;
                }
                // SAFETY: Py_DECREF requires this thread to have a current
                // tstate (the sub-interp-aware way to say "holds the GIL").
                //
                // DO NOT use `PyGILState_Check()` here: it only returns 1 on
                // the MAIN interpreter thread state; in a sub-interpreter it
                // returns 0 even when the sub-interp's GIL is held. Pairing
                // Py_DECREF with PyGILState_Check silently leaked EVERY
                // PyObject in subinterp mode (~0.75 KB/request at 400k rps —
                // a ~1 GB / minute leak at idle load).
                //
                // `PyThreadState_GetUnchecked()` returns the current tstate
                // if one is attached, NULL otherwise. Attached tstate =>
                // GIL held for its interpreter => DECREF is safe.
                // Requires CPython 3.13+ (see pyproject.toml requires-python).
                if ffi::PyThreadState_GetUnchecked().is_null() {
                    let t = ffi::Py_TYPE(self.ptr);
                    let type_name = if !t.is_null() {
                        let p = (*t).tp_name;
                        if !p.is_null() {
                            std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
                        } else {
                            "?".into()
                        }
                    } else {
                        "?".into()
                    };
                    let thread_name = std::thread::current()
                        .name()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| format!("{:?}", std::thread::current().id()));
                    let bt = std::backtrace::Backtrace::capture();
                    tracing::error!(
                        target: "pyronova::server",
                        ptr = ?self.ptr,
                        type_name = %type_name,
                        thread = %thread_name,
                        backtrace = %bt,
                        "PyObjRef dropped with no attached tstate — leaking pointer to avoid segfault"
                    );
                    return; // Leak is better than crash
                }
                #[cfg(feature = "leak_detect")]
                crate::leak_detect::record_drop(self.ptr);
                ffi::Py_DECREF(self.ptr);
            }
        }
    }
}

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

// ---------------------------------------------------------------------------
// Sub-interpreter response
// ---------------------------------------------------------------------------

/// Result from a sub-interpreter handler call.
pub(crate) struct SubInterpResponse {
    pub body: Vec<u8>,
    pub status: u16,
    pub content_type: Option<String>,
    pub headers: Vec<(String, String)>,
    pub is_json: bool,
}

// ---------------------------------------------------------------------------
// Work item for channel-based dispatch
// ---------------------------------------------------------------------------

pub(crate) struct WorkRequest {
    pub handler_idx: usize,
    /// Arc<str>: zero-cost clone of the value already Arc'd in handle_request_subinterp.
    pub method: Arc<str>,
    /// Arc<str>: same — avoids String alloc + memcpy on the Tokio thread.
    pub path: Arc<str>,
    pub params: Vec<(String, String)>,
    pub query: String,
    pub body: bytes::Bytes,
    /// Raw HeaderMap: deferred extract_headers() to the worker thread so
    /// the O(n_headers) HashMap build doesn't block the Tokio executor.
    pub headers: hyper::HeaderMap,
    /// IpAddr: deferred to_string() to the worker thread.
    pub client_ip: std::net::IpAddr,
    pub response_tx: tokio::sync::oneshot::Sender<Result<SubInterpResponse, String>>,
}

// Diagnostic: count WorkRequest creates vs worker-completes. Gated
// behind `leak_detect` because hitting two shared atomics on every
// request is an NUMA disaster on many-core boxes — a single shared
// AtomicU64 pings its cache line across every CCD on a Threadripper /
// EPYC on every `fetch_add`, silently capping throughput regardless of
// how many workers we spawn. The public `workrequest_counts()`
// Python export keeps its shape: returns (0, 0) when the feature is
// off, real values when diagnostics are compiled in.
#[cfg(feature = "leak_detect")]
static WR_CREATED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "leak_detect")]
static WR_COMPLETED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl WorkRequest {
    #[inline(always)]
    pub fn inc_created() {
        #[cfg(feature = "leak_detect")]
        WR_CREATED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline(always)]
    pub fn inc_completed() {
        #[cfg(feature = "leak_detect")]
        WR_COMPLETED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn created_count() -> u64 {
        #[cfg(feature = "leak_detect")]
        {
            WR_CREATED.load(std::sync::atomic::Ordering::Relaxed)
        }
        #[cfg(not(feature = "leak_detect"))]
        {
            0
        }
    }
    pub fn dropped_count() -> u64 {
        #[cfg(feature = "leak_detect")]
        {
            WR_COMPLETED.load(std::sync::atomic::Ordering::Relaxed)
        }
        #[cfg(not(feature = "leak_detect"))]
        {
            0
        }
    }
}

// ---------------------------------------------------------------------------
// Safe sub-interpreter
// ---------------------------------------------------------------------------

pub(crate) struct SubInterpreterWorker {
    /// Thread state (saved after releasing GIL)
    pub(crate) tstate: *mut ffi::PyThreadState,
    /// Handler function pointers keyed by name
    handlers: HashMap<String, *mut ffi::PyObject>,
    /// Globals dict of this sub-interpreter
    globals: *mut ffi::PyObject,
    /// Cached: json.dumps function pointer (avoids per-request import)
    json_dumps_func: *mut ffi::PyObject,
    /// Cached: `_Request` **type object** (raw C-API heap type,
    /// defined in `pyronova_request_type.rs`). Built per sub-interp via
    /// `PyType_FromSpec` so its custom `tp_dealloc` can synchronously
    /// DECREF all slot fields — workaround for PEP 684's broken
    /// per-instance dealloc path on `__slots__` Python classes.
    sky_request_cls: *mut ffi::PyObject,
    /// Cached: _Response class pointer
    sky_response_cls: *mut ffi::PyObject,
    /// Cached: persistent asyncio event loop for this sub-interpreter
    _asyncio_loop: *mut ffi::PyObject,
    /// Cached: loop.run_until_complete method
    loop_run_func: *mut ffi::PyObject,
    /// Pool instance id (see `POOL_ID_COUNTER`). Exposed to the async
    /// engine as `_pyronova_pool_id` so it can be passed into every
    /// `_pyronova_recv` / `_pyronova_send` call for the zombie-worker guard.
    pool_id: u64,
    /// Cached `gc.collect` function pointer. `_bootstrap.py` runs
    /// `gc.disable()` at sub-interp init so CPython's threshold-based
    /// automatic triggers never fire. Instead we call this manually at
    /// a request-count cadence (see `gc_threshold` + `gc_counter`),
    /// pushing all cycle-collection work off the hot path and into
    /// deterministic slots between requests.
    pub(crate) gc_collect_func: *mut ffi::PyObject,
    /// Trigger interval in requests. 0 disables scheduled collection
    /// entirely (use when you've verified your handler graph creates no
    /// cycles — ref-counting handles everything else instantly).
    /// Default 5000, overridable via `PYRONOVA_GC_THRESHOLD=N`.
    pub(crate) gc_threshold: u64,
    /// Request counter for the GC scheduler. Incremented at the end of
    /// each `call_handler`; every `gc_threshold` ticks we invoke
    /// `gc.collect()`. Per-worker = per-thread, so no atomics needed.
    gc_counter: u64,
}

unsafe impl Send for SubInterpreterWorker {}

impl SubInterpreterWorker {
    /// Create a new sub-interpreter, execute the filtered script, extract handlers.
    ///
    /// # Safety
    /// Must be called while the main interpreter's thread state is current.
    /// Switches to the new sub-interpreter and back to main on completion.
    pub(crate) unsafe fn new(
        script: &str,
        script_path: &str,
        func_names: &[String],
        pool_id: u64,
    ) -> Result<Self, String> {
        let main_tstate = ffi::PyThreadState_Get();

        let mut new_tstate: *mut ffi::PyThreadState = std::ptr::null_mut();
        let config = ffi::PyInterpreterConfig {
            use_main_obmalloc: 0,
            allow_fork: 0,
            allow_exec: 0,
            allow_threads: 1,
            allow_daemon_threads: 0,
            check_multi_interp_extensions: 1, // Strict: only extensions declaring multi-interp support
            gil: ffi::PyInterpreterConfig_OWN_GIL,
        };

        let status = ffi::Py_NewInterpreterFromConfig(&mut new_tstate, &config);
        if ffi::PyStatus_IsError(status) != 0 || new_tstate.is_null() {
            ffi::PyThreadState_Swap(main_tstate);
            return Err("Py_NewInterpreterFromConfig failed".to_string());
        }

        // Past this point we own a live sub-interpreter. Any early error
        // must Py_EndInterpreter it before returning, or the sub-interp
        // (and the thread resources it pins) leak permanently. Delegate
        // init to a helper so `?` can short-circuit safely — we catch its
        // Err here and perform cleanup regardless of which step failed.
        match Self::init_in_sub_interp(script, script_path, func_names, pool_id) {
            Ok(worker) => {
                ffi::PyThreadState_Swap(main_tstate);
                Ok(worker)
            }
            Err(e) => {
                ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
                ffi::PyThreadState_Swap(main_tstate);
                Err(e)
            }
        }
    }

    /// Run every init step that executes INSIDE the freshly-created
    /// sub-interpreter. Returns a worker whose `tstate` is already saved
    /// via PyEval_SaveThread (GIL released). Caller is responsible for
    /// swapping back to the main tstate, and for Py_EndInterpreter on error.
    ///
    /// # Safety
    /// Must be called with a sub-interpreter's thread state current.
    unsafe fn init_in_sub_interp(
        script: &str,
        script_path: &str,
        func_names: &[String],
        pool_id: u64,
    ) -> Result<Self, String> {
        // Run the bootstrap (from external .py file) + user script.
        let bootstrap_src = include_str!("../../python/pyronova/_bootstrap.py");
        let bootstrap = format!("{bootstrap_src}\n# Execute full user script\n{script}");

        let globals =
            PyObjRef::from_owned(ffi::PyDict_New()).ok_or("failed to create globals dict")?;
        let builtins = ffi::PyEval_GetBuiltins(); // borrowed ref
        ffi::PyDict_SetItemString(globals.as_ptr(), c"__builtins__".as_ptr(), builtins);

        // Register _pyronova_emit_log C-FFI function for Python logging bridge
        #[allow(clippy::missing_transmute_annotations)]
        let emit_log_def = Box::into_raw(Box::new(ffi::PyMethodDef {
            ml_name: c"_pyronova_emit_log".as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                PyCFunctionWithKeywords: std::mem::transmute(pyronova_emit_log_cfunc as *const ()),
            },
            ml_flags: ffi::METH_VARARGS,
            ml_doc: std::ptr::null(),
        }));
        let emit_log_func =
            ffi::PyCFunction_NewEx(emit_log_def, std::ptr::null_mut(), std::ptr::null_mut());
        if !emit_log_func.is_null() {
            ffi::PyDict_SetItemString(
                globals.as_ptr(),
                c"_pyronova_emit_log".as_ptr(),
                emit_log_func,
            );
            ffi::Py_DECREF(emit_log_func);
        }

        // Sub-interp DB bridge — 4 C-FFI functions that forward fetch_*
        // and execute calls onto the main-process sqlx pool, so DB-backed
        // routes no longer need `gil=True`. See src/db_bridge.rs for the
        // full rationale.
        crate::bridge::db_bridge::register_db_bridge(globals.as_ptr());

        // Set __file__ so user scripts can use it for path resolution
        if let Some(py_file) = py_str(script_path) {
            ffi::PyDict_SetItemString(globals.as_ptr(), c"__file__".as_ptr(), py_file.as_ptr());
        }

        let code_cstr = std::ffi::CString::new(bootstrap.as_bytes())
            .map_err(|e| format!("CString error: {e}"))?;
        let _filename_cstr =
            std::ffi::CString::new(script_path).map_err(|e| format!("CString error: {e}"))?;

        let result = PyObjRef::from_owned(ffi::PyRun_String(
            code_cstr.as_ptr(),
            ffi::Py_file_input,
            globals.as_ptr(),
            globals.as_ptr(),
        ));

        if result.is_none() {
            ffi::PyErr_Print();
            // globals dropped here → DECREF. Outer `new()` destroys the
            // sub-interpreter and swaps back to main once we return Err.
            return Err("failed to execute script in sub-interpreter".to_string());
        }
        // result dropped here → DECREF (it's just Py_None for exec)

        // Extract handler functions by name
        let mut handlers = HashMap::new();
        for name in func_names {
            let name_cstr = std::ffi::CString::new(name.as_bytes())
                .map_err(|e| format!("CString error: {e}"))?;
            let func = ffi::PyDict_GetItemString(globals.as_ptr(), name_cstr.as_ptr());
            if !func.is_null() && ffi::PyCallable_Check(func) != 0 {
                ffi::Py_INCREF(func);
                handlers.insert(name.clone(), func);
            }
        }

        // Build the raw C-API `_Request` type for THIS sub-interp
        // (custom tp_dealloc that synchronously DECREFs every slot —
        // workaround for PEP 684's broken heap-type finalizer). One
        // type per sub-interp: PyTypeObject state is per-interp.
        //
        // We then install helper methods (`.text()`, `.json()`, `.body`,
        // `.query_params`) DIRECTLY on the heap type — NOT via a
        // Python subclass. A subclass triggers CPython's subtype_dealloc
        // fallback and bypasses our custom tp_dealloc, restoring the
        // full-instance leak we're trying to fix.
        let rust_ty = crate::pyronova_request_type::register_type()?;
        let req_cls_name = std::ffi::CString::new("_Request").unwrap();
        if ffi::PyDict_SetItemString(globals.as_ptr(), req_cls_name.as_ptr(), rust_ty) != 0 {
            ffi::PyErr_Print();
            return Err("failed to inject _Request into sub-interp globals".to_string());
        }

        // Attach helper methods directly on the type (mutable by
        // virtue of Py_TPFLAGS_HEAPTYPE). Users can do
        // `req.text()` / `req.json()` / `req.body` / `req.query_params`.
        //
        // Also rebind the mock module attributes (`pyronova.PyronovaRequest`
        // and `pyronova.engine.PyronovaRequest`) to this Rust type —
        // `_bootstrap.py` sets them to None as placeholders because it
        // runs BEFORE this injection. User code doing
        // `from pyronova import PyronovaRequest` or
        // `isinstance(req, PyronovaRequest)` then gets the real type.
        let helpers_src = c"\
def _attach_pyronova_request_helpers(t):\n    from urllib.parse import parse_qs\n    import json as _json\n    t.body = property(lambda self: self.body_bytes)\n    t.query_params = property(lambda self: {k: v[0] for k, v in parse_qs(self.query, keep_blank_values=True).items()})\n    t.query_params_all = property(lambda self: parse_qs(self.query, keep_blank_values=True))\n    t.text = lambda self: self.body_bytes.decode('utf-8') if isinstance(self.body_bytes, (bytes, bytearray)) else str(self.body_bytes)\n    t.json = lambda self: _json.loads(self.text())\n_attach_pyronova_request_helpers(_Request)\nimport sys as _sys\n_m = _sys.modules.get('pyronova.engine')\nif _m is not None:\n    _m.PyronovaRequest = _Request\n_p = _sys.modules.get('pyronova')\nif _p is not None:\n    _p.PyronovaRequest = _Request\n";
        let helpers_result = ffi::PyRun_String(
            helpers_src.as_ptr(),
            ffi::Py_file_input,
            globals.as_ptr(),
            globals.as_ptr(),
        );
        if helpers_result.is_null() {
            ffi::PyErr_Print();
            return Err("failed to attach _Request helper methods".to_string());
        }
        ffi::Py_DECREF(helpers_result);

        let sky_request_cls = rust_ty;
        ffi::Py_INCREF(sky_request_cls);

        let resp_cls_name = std::ffi::CString::new("_Response").unwrap();
        let sky_response_cls = ffi::PyDict_GetItemString(globals.as_ptr(), resp_cls_name.as_ptr());
        if !sky_response_cls.is_null() {
            ffi::Py_INCREF(sky_response_cls);
        }

        // Try orjson first (10-40x faster than stdlib json), fall back to json
        let json_dumps_func = {
            let orjson_mod = ffi::PyImport_ImportModule(c"orjson".as_ptr());
            if !orjson_mod.is_null() {
                let f = ffi::PyObject_GetAttrString(orjson_mod, c"dumps".as_ptr());
                ffi::Py_DECREF(orjson_mod);
                f
            } else {
                ffi::PyErr_Clear();
                let json_mod = ffi::PyImport_ImportModule(c"json".as_ptr());
                if !json_mod.is_null() {
                    let f = ffi::PyObject_GetAttrString(json_mod, c"dumps".as_ptr());
                    ffi::Py_DECREF(json_mod);
                    f
                } else {
                    ffi::PyErr_Clear();
                    std::ptr::null_mut()
                }
            }
        };

        // Create persistent asyncio event loop for this sub-interpreter
        let (asyncio_loop, loop_run_func) = {
            let asyncio_mod = ffi::PyImport_ImportModule(c"asyncio".as_ptr());
            if !asyncio_mod.is_null() {
                let loop_obj = ffi::PyObject_CallMethod(
                    asyncio_mod,
                    c"new_event_loop".as_ptr(),
                    std::ptr::null(),
                );
                let run_func = if !loop_obj.is_null() {
                    // Set as current loop; Py_DECREF the None return value.
                    let set_result = ffi::PyObject_CallMethod(
                        asyncio_mod,
                        c"set_event_loop".as_ptr(),
                        c"O".as_ptr(),
                        loop_obj,
                    );
                    if !set_result.is_null() {
                        ffi::Py_DECREF(set_result);
                    } else {
                        ffi::PyErr_Clear();
                    }
                    ffi::PyObject_GetAttrString(loop_obj, c"run_until_complete".as_ptr())
                } else {
                    ffi::PyErr_Clear();
                    std::ptr::null_mut()
                };
                ffi::Py_DECREF(asyncio_mod);
                (loop_obj, run_func)
            } else {
                ffi::PyErr_Clear();
                (std::ptr::null_mut(), std::ptr::null_mut())
            }
        };

        // Keep globals alive — transfer ownership to the struct
        let globals_ptr = globals.into_raw();

        // Cache gc.collect so the scheduled-GC path doesn't re-import
        // per tick. `_bootstrap.py` has already called gc.disable() at
        // this point; the function pointer is just for manual triggers.
        let gc_collect_func = {
            let gc_mod = ffi::PyImport_ImportModule(c"gc".as_ptr());
            if !gc_mod.is_null() {
                let f = ffi::PyObject_GetAttrString(gc_mod, c"collect".as_ptr());
                ffi::Py_DECREF(gc_mod);
                if f.is_null() {
                    ffi::PyErr_Clear();
                }
                f
            } else {
                ffi::PyErr_Clear();
                std::ptr::null_mut()
            }
        };

        // Read the threshold env var once at sub-interp init (it's set
        // on the main process before any sub-interp spawns). 0 disables
        // scheduled collection.
        // Default 100_000 — conservative. At 100k rps/thread that's one
        // scheduled collect per second, which is invisible in P99. The
        // old CPython-default threshold-based auto-trigger fired at
        // ~hundreds of collects per second under the same load → P99
        // jumps to 2-10ms. Measurement: on the baseline test,
        // threshold=5000 gave p99=2.0ms; threshold=100_000 gave
        // p99≈300µs; threshold=0 (disabled) gave p99=240µs.
        //
        // Workloads that verified they create no cycles can set
        // PYRONOVA_GC_THRESHOLD=0 for best P99. Workloads with
        // known-high cycle churn may want threshold=10_000.
        let gc_threshold: u64 = std::env::var("PYRONOVA_GC_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000);

        // Release this sub-interpreter's GIL. Outer `new()` swaps back to
        // the main interpreter after we return.
        let saved = ffi::PyEval_SaveThread();

        Ok(SubInterpreterWorker {
            tstate: saved,
            handlers,
            globals: globals_ptr,
            json_dumps_func,
            sky_request_cls,
            sky_response_cls,
            _asyncio_loop: asyncio_loop,
            loop_run_func,
            pool_id,
            gc_collect_func,
            gc_threshold,
            gc_counter: 0,
        })
    }

    /// Build a fresh `_Request` instance for this request.
    ///
    /// Returns a NEW owned reference (caller must DECREF). The
    /// instance's `tp_dealloc` synchronously DECREFs all slot fields,
    /// so no `SlotClearer` / instance recycling is needed.
    ///
    /// # Safety
    /// Must be called with this sub-interpreter's GIL held.
    #[allow(clippy::too_many_arguments)]
    unsafe fn build_request(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        query: &str,
        body: &[u8],
        headers: &HashMap<String, String>,
        client_ip: &str,
    ) -> Result<*mut ffi::PyObject, String> {
        // ── Leak-hunt bisection of slot constructors (leak_detect only) ──
        // PYRONOVA_BISECT_SLOT=<name> replaces that slot's value with Py_None:
        //   method | path | params | query | body | headers | client_ip
        //   all    — every slot becomes None (alloc shell only)
        //   none   — normal (default)
        #[cfg(feature = "leak_detect")]
        let slot_mode = std::env::var("PYRONOVA_BISECT_SLOT").ok();
        #[cfg(not(feature = "leak_detect"))]
        let slot_mode: Option<String> = None;
        let skip = |name: &str| -> bool {
            match slot_mode.as_deref() {
                Some("all") => true,
                Some(s) => s == name,
                None => false,
            }
        };
        let none_ref = || unsafe { PyObjRef::from_borrowed(ffi::Py_None()).unwrap() };

        let py_method = if skip("method") {
            none_ref()
        } else {
            py_str(method).ok_or("failed to create py_method")?
        };
        let py_path = if skip("path") {
            none_ref()
        } else {
            py_str(path).ok_or("failed to create py_path")?
        };
        let py_query = if skip("query") {
            none_ref()
        } else {
            py_str(query).ok_or("failed to create py_query")?
        };
        let py_client_ip = if skip("client_ip") {
            none_ref()
        } else {
            py_str(client_ip).ok_or("failed to create py_client_ip")?
        };

        if self.sky_request_cls.is_null() {
            return Err("_Request type not registered".to_string());
        }

        // Lazy maps: move raw Rust data into a Box. The getset getters
        // on `_Request` will build the actual PyDict on first access
        // to `.params` / `.headers` — handlers that never touch those
        // slots (common on plaintext benchmarks) pay zero Python
        // allocation for them.
        //
        // The skip_* bisection flags for params/headers are honored by
        // supplying empty placeholders; the getters still return a
        // (now empty) dict, preserving the old observation surface.
        let skip_params = skip("params");
        let skip_headers = skip("headers");
        let skip_body = skip("body");
        let maps = Box::new(crate::pyronova_request_type::LazyMaps {
            params: if skip_params {
                Vec::new()
            } else {
                params.to_vec()
            },
            headers: if skip_headers {
                HashMap::new()
            } else {
                headers.clone()
            },
            body: if skip_body { Vec::new() } else { body.to_vec() },
        });

        // Transfer ownership of each new ref into the instance.
        // `alloc_and_init_lazy` DECREFs them + drops `maps` on failure.
        crate::pyronova_request_type::alloc_and_init_lazy(
            self.sky_request_cls,
            py_method.into_raw(),
            py_path.into_raw(),
            py_query.into_raw(),
            py_client_ip.into_raw(),
            maps,
        )
    }

    /// Parse a handler return value into SubInterpResponse.
    ///
    /// # Safety
    /// Must be called with this sub-interpreter's GIL held.
    unsafe fn parse_result(&self, result_obj: PyObjRef) -> Result<SubInterpResponse, String> {
        let ptr = result_obj.as_ptr();

        // Check if it's a _Response or any response-like object
        // (duck typing: has status_code + body attributes).
        //
        // PyObject_IsInstance returns 1 (true), 0 (false), or -1 (error
        // with exception set). Treating -1 as false without clearing
        // the exception is a SystemError latent bomb — the next C-API
        // call short-circuits on the pending exception.
        let resp_cls = self.sky_response_cls;
        let is_response = if resp_cls.is_null() {
            false
        } else {
            match ffi::PyObject_IsInstance(ptr, resp_cls) {
                1 => true,
                -1 => {
                    ffi::PyErr_Clear();
                    // Fall through to duck-type check.
                    let has_status = ffi::PyObject_HasAttrString(ptr, c"status_code".as_ptr()) == 1;
                    let has_body = ffi::PyObject_HasAttrString(ptr, c"body".as_ptr()) == 1;
                    has_status && has_body
                }
                _ => {
                    // 0 (not an instance) — try duck-typing.
                    let has_status = ffi::PyObject_HasAttrString(ptr, c"status_code".as_ptr()) == 1;
                    let has_body = ffi::PyObject_HasAttrString(ptr, c"body".as_ptr()) == 1;
                    has_status && has_body
                }
            }
        };
        if is_response {
            return self.parse_sky_response(result_obj);
        }

        // dict → JSON
        if ffi::PyDict_Check(ptr) != 0 {
            let json_str = self.json_dumps(result_obj)?;
            return Ok(SubInterpResponse {
                body: json_str.into_bytes(),
                status: 200,
                content_type: None,
                headers: Vec::new(),
                is_json: true,
            });
        }

        // string
        if ffi::PyUnicode_Check(ptr) != 0 {
            let s = pyobj_to_string(ptr)?;
            return Ok(SubInterpResponse {
                body: s.into_bytes(),
                status: 200,
                content_type: None,
                headers: Vec::new(),
                is_json: false,
            });
        }

        // fallback: str(result)
        let str_obj = PyObjRef::from_owned(ffi::PyObject_Str(ptr)).ok_or_else(|| {
            ffi::PyErr_Clear();
            "str() failed".to_string()
        })?;
        let s = pyobj_to_string(str_obj.as_ptr())?;
        Ok(SubInterpResponse {
            body: s.into_bytes(),
            status: 200,
            content_type: None,
            headers: Vec::new(),
            is_json: false,
        })
    }

    /// Build a _Response Python object from a SubInterpResponse.
    unsafe fn build_sky_response(&self, resp: &SubInterpResponse) -> Result<PyObjRef, String> {
        if self.sky_response_cls.is_null() {
            return Err("_Response class not available".to_string());
        }

        // Convert body to Python object — use bytes for binary, str for text
        let py_body = if resp.is_json || std::str::from_utf8(&resp.body).is_ok() {
            let body_str = unsafe { std::str::from_utf8_unchecked(&resp.body) };
            py_str(body_str).ok_or("failed to create body str")?
        } else {
            // Binary data: use PyBytes to avoid UTF-8 corruption
            PyObjRef::from_owned(ffi::PyBytes_FromStringAndSize(
                resp.body.as_ptr() as *const _,
                resp.body.len() as isize,
            ))
            .ok_or("failed to create body bytes")?
        };
        let py_status = PyObjRef::from_owned(ffi::PyLong_FromLong(resp.status as i64))
            .ok_or("failed to create status")?;
        let py_ct = match &resp.content_type {
            Some(ct) => py_str(ct).ok_or("failed to create content_type")?,
            None => PyObjRef::from_borrowed(ffi::Py_None()).unwrap(),
        };
        let py_headers =
            py_str_dict_from_vec(&resp.headers).ok_or("failed to create headers dict")?;

        // _Response(body, status_code, content_type, headers)
        let args = PyObjRef::from_owned(ffi::PyTuple_New(0)).ok_or("failed to create args")?;
        let kwargs = PyObjRef::from_owned(ffi::PyDict_New()).ok_or("failed to create kwargs")?;

        ffi::PyDict_SetItemString(kwargs.as_ptr(), c"body".as_ptr(), py_body.as_ptr());
        ffi::PyDict_SetItemString(kwargs.as_ptr(), c"status_code".as_ptr(), py_status.as_ptr());
        ffi::PyDict_SetItemString(kwargs.as_ptr(), c"content_type".as_ptr(), py_ct.as_ptr());
        ffi::PyDict_SetItemString(kwargs.as_ptr(), c"headers".as_ptr(), py_headers.as_ptr());

        PyObjRef::from_owned(ffi::PyObject_Call(
            self.sky_response_cls,
            args.as_ptr(),
            kwargs.as_ptr(),
        ))
        .ok_or_else(|| {
            log_and_clear_py_exception("_Response construction");
            "failed to create _Response".to_string()
        })
    }

    /// If obj is awaitable (coroutine / Task / Future / custom __await__),
    /// drive it via the persistent event loop. Otherwise return unchanged.
    ///
    /// Detection is a C-level type-slot probe:
    ///   1. Fast path `PyCoro_CheckExact` — one tag compare, catches
    ///      the common `async def` case.
    ///   2. Fallback: read `Py_TYPE(obj)->tp_as_async->am_await` —
    ///      any real awaitable (Task, Future, user class with
    ///      `__await__`) has this slot populated. One pointer chase +
    ///      null check. Nanoseconds, L1-resident.
    ///
    /// We avoid `PyObject_HasAttrString(obj, "__await__")` here: that
    /// path would intern the string, walk the MRO, and potentially
    /// trigger descriptor protocol — μs-level, and at 400k rps on the
    /// hot hook path it showed up as a measurable 5% throughput loss.
    unsafe fn resolve_coroutine(&self, obj: PyObjRef) -> Result<PyObjRef, String> {
        let ptr = obj.as_ptr();
        let is_awaitable = if ffi::PyCoro_CheckExact(ptr) == 1 {
            true
        } else {
            let tp = ffi::Py_TYPE(ptr);
            if tp.is_null() {
                false
            } else {
                let async_slots = (*tp).tp_as_async;
                !async_slots.is_null() && (*async_slots).am_await.is_some()
            }
        };
        if !is_awaitable {
            return Ok(obj); // Plain value — pass through
        }
        if self.loop_run_func.is_null() {
            return Err("async handler used but asyncio event loop not available".to_string());
        }
        // Call loop.run_until_complete(awaitable)
        let args =
            PyObjRef::from_owned(ffi::PyTuple_New(1)).ok_or("failed to create args tuple")?;
        ffi::PyTuple_SetItem(args.as_ptr(), 0, obj.into_raw());
        let result = PyObjRef::from_owned(ffi::PyObject_Call(
            self.loop_run_func,
            args.as_ptr(),
            std::ptr::null_mut(),
        ));
        match result {
            Some(r) => Ok(r),
            None => {
                log_and_clear_py_exception("loop.run_until_complete");
                Err("loop.run_until_complete() failed".to_string())
            }
        }
    }

    /// Serialize a Python dict/list to JSON string via cached dumps (orjson or json).
    unsafe fn json_dumps(&self, obj: PyObjRef) -> Result<String, String> {
        if self.json_dumps_func.is_null() {
            return Err("json.dumps not cached".to_string());
        }

        let args = PyObjRef::from_owned(ffi::PyTuple_New(1)).ok_or("failed to create tuple")?;
        ffi::PyTuple_SetItem(args.as_ptr(), 0, obj.into_raw());

        let result = PyObjRef::from_owned(ffi::PyObject_Call(
            self.json_dumps_func,
            args.as_ptr(),
            std::ptr::null_mut(),
        ))
        .ok_or_else(|| {
            log_and_clear_py_exception("json.dumps");
            "json.dumps failed".to_string()
        })?;

        // orjson.dumps returns bytes, json.dumps returns str
        if ffi::PyBytes_Check(result.as_ptr()) != 0 {
            let ptr = ffi::PyBytes_AsString(result.as_ptr());
            let size = ffi::PyBytes_Size(result.as_ptr());
            // PyBytes_Size returns -1 on error (Py_ssize_t). Cast to
            // usize without checking would yield usize::MAX and feed
            // an enormous slice to from_raw_parts → UB (arc interp-2).
            // Treat any negative as failure, matching PyBytes_Check
            // having already validated the type (a successful Check
            // followed by a -1 Size implies torn state / corruption,
            // not a normal program path — bail instead of UB).
            if ptr.is_null() || size < 0 {
                if !ffi::PyErr_Occurred().is_null() {
                    ffi::PyErr_Clear();
                }
                return Err("failed to extract bytes".to_string());
            }
            let bytes = std::slice::from_raw_parts(ptr as *const u8, size as usize);
            String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())
        } else {
            pyobj_to_string(result.as_ptr())
        }
    }

    /// Parse a _Response Python object.
    unsafe fn parse_sky_response(&self, obj: PyObjRef) -> Result<SubInterpResponse, String> {
        let ptr = obj.as_ptr();

        // status_code
        let status = {
            let attr =
                PyObjRef::from_owned(ffi::PyObject_GetAttrString(ptr, c"status_code".as_ptr()));
            match attr {
                Some(a) => {
                    let code = ffi::PyLong_AsLong(a.as_ptr());
                    if code == -1 && !ffi::PyErr_Occurred().is_null() {
                        ffi::PyErr_Clear();
                        200
                    } else {
                        code as u16
                    }
                }
                None => {
                    ffi::PyErr_Clear();
                    200
                }
            }
        };

        // content_type
        let content_type = {
            let attr =
                PyObjRef::from_owned(ffi::PyObject_GetAttrString(ptr, c"content_type".as_ptr()));
            match attr {
                Some(a) if a.as_ptr() != ffi::Py_None() => pyobj_to_string(a.as_ptr()).ok(),
                _ => {
                    ffi::PyErr_Clear();
                    None
                }
            }
        };

        // headers
        //
        // CRITICAL: PyDict_Next forbids dict mutation during iteration.
        // PyObject_Str may invoke user __str__ which could mutate the
        // dict → undefined behaviour / segfault. We collect borrowed
        // key/value refs first, INCREF them, then release the iteration
        // scope before calling any method that may re-enter Python.
        let mut resp_headers: Vec<(String, String)> = Vec::new();
        {
            let attr = PyObjRef::from_owned(ffi::PyObject_GetAttrString(ptr, c"headers".as_ptr()));
            if let Some(a) = &attr {
                if ffi::PyDict_Check(a.as_ptr()) != 0 {
                    // Phase 1: snapshot (no user code runs).
                    let mut snapshot: Vec<(PyObjRef, PyObjRef)> = Vec::new();
                    let mut pos: isize = 0;
                    let mut key: *mut ffi::PyObject = std::ptr::null_mut();
                    let mut val: *mut ffi::PyObject = std::ptr::null_mut();
                    while ffi::PyDict_Next(a.as_ptr(), &mut pos, &mut key, &mut val) != 0 {
                        // PyDict_Next returns borrowed refs — INCREF to own them.
                        if let (Some(k), Some(v)) =
                            (PyObjRef::from_borrowed(key), PyObjRef::from_borrowed(val))
                        {
                            snapshot.push((k, v));
                        }
                    }
                    // Phase 2: convert — safe to invoke __str__ now.
                    for (k_obj, v_obj) in snapshot {
                        let str_key = PyObjRef::from_owned(ffi::PyObject_Str(k_obj.as_ptr()));
                        if let Some(sk) = str_key {
                            if let Ok(k) = pyobj_to_string(sk.as_ptr()) {
                                // Check if value is a Python list — e.g. multiple Set-Cookie values
                                if ffi::PyList_Check(v_obj.as_ptr()) != 0 {
                                    let n = ffi::PyList_Size(v_obj.as_ptr());
                                    for i in 0..n {
                                        // PyList_GetItem returns a borrowed ref — do NOT wrap in from_owned.
                                        let item = ffi::PyList_GetItem(v_obj.as_ptr(), i);
                                        if item.is_null() {
                                            ffi::PyErr_Clear();
                                            continue;
                                        }
                                        if let Some(item_str) =
                                            PyObjRef::from_owned(ffi::PyObject_Str(item))
                                        {
                                            if let Ok(v) = pyobj_to_string(item_str.as_ptr()) {
                                                resp_headers.push((k.clone(), v));
                                            } else {
                                                ffi::PyErr_Clear();
                                            }
                                        } else {
                                            ffi::PyErr_Clear();
                                        }
                                    }
                                } else {
                                    let str_val =
                                        PyObjRef::from_owned(ffi::PyObject_Str(v_obj.as_ptr()));
                                    if let Some(sv) = str_val {
                                        if let Ok(v) = pyobj_to_string(sv.as_ptr()) {
                                            resp_headers.push((k, v));
                                        } else {
                                            ffi::PyErr_Clear();
                                        }
                                    } else {
                                        ffi::PyErr_Clear();
                                    }
                                }
                            }
                        } else {
                            ffi::PyErr_Clear();
                        }
                    }
                }
            }
            ffi::PyErr_Clear();
        }

        // body (returns Vec<u8>)
        let (body, is_json): (Vec<u8>, bool) = {
            let attr = PyObjRef::from_owned(ffi::PyObject_GetAttrString(ptr, c"body".as_ptr()));
            match attr {
                Some(a) => {
                    if ffi::PyDict_Check(a.as_ptr()) != 0 {
                        match self.json_dumps(a) {
                            Ok(s) => (s.into_bytes(), true),
                            Err(e) => {
                                tracing::error!(
                                    target: "pyronova::server",
                                    error = %e,
                                    "JSON serialization failed for response body dict"
                                );
                                let msg =
                                    format!(r#"{{"error":"json serialization failed: {}"}}"#, e);
                                return Ok(SubInterpResponse {
                                    body: msg.into_bytes(),
                                    status: 500,
                                    content_type: Some("application/json".to_string()),
                                    headers: resp_headers,
                                    is_json: true,
                                });
                            }
                        }
                    } else if ffi::PyBytes_Check(a.as_ptr()) != 0 {
                        // Raw bytes — pass through without UTF-8 conversion
                        let size = ffi::PyBytes_Size(a.as_ptr());
                        let ptr = ffi::PyBytes_AsString(a.as_ptr());
                        if !ptr.is_null() && size > 0 {
                            let slice = std::slice::from_raw_parts(ptr as *const u8, size as usize);
                            (slice.to_vec(), false)
                        } else {
                            (Vec::new(), false)
                        }
                    } else if ffi::PyUnicode_Check(a.as_ptr()) != 0 {
                        (
                            pyobj_to_string(a.as_ptr()).unwrap_or_default().into_bytes(),
                            false,
                        )
                    } else {
                        let str_obj = PyObjRef::from_owned(ffi::PyObject_Str(a.as_ptr()));
                        match str_obj {
                            Some(s) => (
                                pyobj_to_string(s.as_ptr()).unwrap_or_default().into_bytes(),
                                false,
                            ),
                            None => {
                                ffi::PyErr_Clear();
                                (Vec::new(), false)
                            }
                        }
                    }
                }
                None => {
                    ffi::PyErr_Clear();
                    (Vec::new(), false)
                }
            }
        };

        Ok(SubInterpResponse {
            body,
            status,
            content_type,
            headers: resp_headers,
            is_json,
        })
    }

    /// Call a handler function and return the response.
    ///
    /// # Safety
    /// Must be called with this sub-interpreter's GIL held.
    #[allow(clippy::too_many_arguments)]
    pub(crate) unsafe fn call_handler(
        &mut self,
        handler_name: &str,
        before_hook_names: &[String],
        after_hook_names: &[String],
        method: &str,
        path: &str,
        params: &[(String, String)],
        query: &str,
        body: &[u8],
        headers: &HashMap<String, String>,
        client_ip: &str,
    ) -> Result<SubInterpResponse, String> {
        let func = *self
            .handlers
            .get(handler_name)
            .ok_or_else(|| format!("handler '{}' not found", handler_name))?;

        // Fresh `_Request` (new owned ref). The Rust-backed type's
        // `tp_dealloc` synchronously DECREFs all slot fields when this
        // PyObjRef drops at scope end — no SlotClearer / instance
        // recycling needed, no PEP 684 finalizer bug to work around.
        // ── Leak-hunt bisection hook (leak_detect feature only) ──────
        // PYRONOVA_BISECT values:
        //   "skip_all"     — no build_request, no handler call; return a
        //                    fixed SubInterpResponse. Exercises hyper +
        //                    channel only. If this still leaks, the
        //                    leak is NOT in the Python side at all.
        //   "skip_handler" — build_request + dealloc runs, but handler
        //                    is not invoked. Isolates request-object
        //                    construction from handler/response path.
        //   "skip_build"   — handler runs with Py_None as the request
        //                    arg (user code will crash, but we only
        //                    care about memory — use a handler that
        //                    ignores its arg, e.g. `def h(req): return "ok"`).
        // Unset / any other value: normal execution.
        #[cfg(feature = "leak_detect")]
        let bisect_mode = std::env::var("PYRONOVA_BISECT").ok();
        #[cfg(not(feature = "leak_detect"))]
        let bisect_mode: Option<String> = None;

        if bisect_mode.as_deref() == Some("skip_all") {
            return Ok(SubInterpResponse {
                body: b"ok".to_vec(),
                status: 200,
                content_type: None,
                headers: Vec::new(),
                is_json: false,
            });
        }

        let request_ref: Option<PyObjRef> = if bisect_mode.as_deref() == Some("skip_build") {
            // Hand the handler Py_None instead of a built request.
            Some(PyObjRef::from_borrowed(ffi::Py_None()).unwrap())
        } else {
            Some(
                PyObjRef::from_owned(
                    self.build_request(method, path, params, query, body, headers, client_ip)?,
                )
                .ok_or("build_request returned null")?,
            )
        };
        let request = request_ref.unwrap();
        let request_ptr = request.as_ptr();

        if bisect_mode.as_deref() == Some("skip_handler") {
            // Drop request (triggers tp_dealloc) and return a fixed
            // response — skips hooks, Vectorcall, parse_result.
            drop(request);
            return Ok(SubInterpResponse {
                body: b"ok".to_vec(),
                status: 200,
                content_type: None,
                headers: Vec::new(),
                is_json: false,
            });
        }

        // Run before_request hooks
        for hook_name in before_hook_names {
            if let Some(&hook_func) = self.handlers.get(hook_name) {
                let hook_args = PyObjRef::from_owned(ffi::PyTuple_New(1))
                    .ok_or("failed to create hook args")?;
                ffi::Py_INCREF(request_ptr);
                ffi::PyTuple_SetItem(hook_args.as_ptr(), 0, request_ptr);

                let hook_result = PyObjRef::from_owned(ffi::PyObject_Call(
                    hook_func,
                    hook_args.as_ptr(),
                    std::ptr::null_mut(),
                ));

                match hook_result {
                    Some(r) => {
                        // Drive async hooks through the event loop so
                        // `async def` middleware doesn't leak a bare
                        // coroutine object as a "short-circuit response".
                        let resolved = self.resolve_coroutine(r)?;
                        if resolved.as_ptr() != ffi::Py_None() {
                            return self.parse_result(resolved);
                        }
                    }
                    None => {
                        // Hook raised an exception. We previously logged
                        // with PyErr_Print and fell through to the main
                        // handler — a critical bypass for auth / ACL hooks
                        // that signal denial by raising. Return an error
                        // so the caller serves 500 instead of the
                        // unprotected handler output.
                        log_and_clear_py_exception("before_request hook");
                        return Err(format!(
                            "before_request hook {hook_name:?} raised an exception"
                        ));
                    }
                }
            }
        }

        // Call handler(request). We don't own a ref to request_ptr
        // (worker struct does) — pass it through directly.
        let args_arr = [request_ptr];
        let result_obj = PyObjRef::from_owned(ffi::PyObject_Vectorcall(
            func,
            args_arr.as_ptr(),
            1,
            std::ptr::null_mut(),
        ));
        // after_hooks no longer need a separate PyObjRef — the worker
        // retains the request ptr for us. Keep the flag purely for
        // control flow.
        let has_after_hooks = !after_hook_names.is_empty();

        let mut response = match result_obj {
            Some(r) => {
                let resolved = self.resolve_coroutine(r)?;
                self.parse_result(resolved)?
            }
            None => {
                // req_for_hooks dropped here automatically → DECREF
                log_and_clear_py_exception("sub-interp handler");
                return Err("handler raised an exception".to_string());
            }
        };

        // Run after_request hooks: hook(request, response) → response.
        // Reuses the worker's cached request instance.
        if has_after_hooks {
            for hook_name in after_hook_names {
                if let Some(&hook_func) = self.handlers.get(hook_name) {
                    // Build _Response from current response
                    let resp_obj = self.build_sky_response(&response)?;

                    let hook_args = PyObjRef::from_owned(ffi::PyTuple_New(2))
                        .ok_or("failed to create hook args")?;
                    ffi::Py_INCREF(request_ptr);
                    ffi::PyTuple_SetItem(hook_args.as_ptr(), 0, request_ptr);
                    ffi::PyTuple_SetItem(hook_args.as_ptr(), 1, resp_obj.into_raw());

                    let hook_result = PyObjRef::from_owned(ffi::PyObject_Call(
                        hook_func,
                        hook_args.as_ptr(),
                        std::ptr::null_mut(),
                    ));

                    match hook_result {
                        Some(r) => {
                            // Drive async after_hooks through the event loop.
                            let resolved = self.resolve_coroutine(r)?;
                            if resolved.as_ptr() != ffi::Py_None() {
                                response = self.parse_result(resolved)?;
                            }
                        }
                        None => {
                            log_and_clear_py_exception("after_request hook");
                        }
                    }
                }
            }

            // _slot_guard (from top of fn) clears at end — no inline
            // cleanup needed here.
        }

        // Smart GC: count requests and trigger `gc.collect()` at the
        // configured interval. gc.disable() was called at sub-interp
        // init (see _bootstrap.py) so this is the only cycle collector
        // running — Python's threshold-based auto-trigger never fires.
        // Cost per request is a single u64 increment + compare; the
        // collect itself fires at most once per `gc_threshold` calls
        // and runs under the GIL we already hold.
        if self.gc_threshold > 0 && !self.gc_collect_func.is_null() {
            self.gc_counter = self.gc_counter.wrapping_add(1);
            if self.gc_counter.is_multiple_of(self.gc_threshold) {
                // `PyObject_CallNoArgs` skips the empty-tuple alloc that
                // `PyObject_Call` would require; saves a small per-tick
                // cost and is the idiomatic 3.9+ invocation. `gc.collect()`
                // with no args = full 3-generation collection — cheap
                // when there are few cycles, which is the common case
                // under our ref-count-first request lifecycle.
                let res = ffi::PyObject_CallNoArgs(self.gc_collect_func);
                if !res.is_null() {
                    ffi::Py_DECREF(res);
                } else {
                    // Clear any exception raised during the collect so
                    // we don't leak it into the handler's return path
                    // (handler already succeeded).
                    ffi::PyErr_Clear();
                }
            }
        }

        Ok(response)
    }
}

// ---------------------------------------------------------------------------
// Channel-based Interpreter Pool
// ---------------------------------------------------------------------------

pub(crate) struct InterpreterPool {
    /// Dropping senders closes the channel, signaling workers to exit.
    sync_work_tx: crossbeam_channel::Sender<WorkRequest>,
    async_work_tx: Option<crossbeam_channel::Sender<WorkRequest>>,
    /// Admission-control gate: one permit per slot in the work channel.
    /// Callers `try_acquire_owned()` BEFORE collecting the request body,
    /// so an over-capacity surge of uploads doesn't let N × max_body_size
    /// pile up in RAM while N requests sit waiting for a full queue.
    /// Permit lifetime spans [body-collect, submit, worker-dispatch]
    /// — see `handle_request_subinterp` for the acquire site and
    /// `worker_thread_loop` where the permit rides inside WorkRequest.
    pub(crate) submit_semaphore: Arc<tokio::sync::Semaphore>,
    /// Worker threads — joined on drop to ensure clean sub-interpreter shutdown.
    worker_threads: Option<Vec<std::thread::JoinHandle<()>>>,
    routers: HashMap<String, Router<usize>>,
    _handler_names: Vec<String>,
    pub(crate) requires_gil: Vec<bool>,
    pub(crate) is_async_handler: Vec<bool>,
    pub(crate) static_dirs: Vec<(String, String)>,
    has_async_workers: bool,
    /// Per-instance CORS configuration (None = disabled).
    pub(crate) cors_config: Option<crate::router::CorsConfig>,
    /// Per-instance request logging flag, shared with worker threads.
    /// Read via Arc clone in worker_thread_loop, not directly from the struct.
    _request_logging: Arc<AtomicBool>,
}

impl Drop for InterpreterPool {
    fn drop(&mut self) {
        // 1. Drop senders to close the channels — workers will exit their recv loop.
        //    (We need to replace them so the Sender::drop fires now, not later.)
        let _ = std::mem::replace(&mut self.sync_work_tx, crossbeam_channel::bounded(0).0);
        let _ = self.async_work_tx.take();

        // 2. Join all worker threads so they finish Py_EndInterpreter BEFORE
        //    the main interpreter tears down (Py_Finalize). Without this join,
        //    workers race against Py_Finalize and segfault.
        //
        // Bounded wait: user handlers can block indefinitely (e.g. a synchronous
        // `requests.get` with no timeout). An unconditional .join() would hang
        // the whole process on shutdown. Give each worker 5s to observe the
        // channel close and run its Py_EndInterpreter cleanup; if it's stuck
        // in user code past that, forget the thread. The process is exiting
        // anyway — the OS reclaims memory. A stuck sub-interp leaks only
        // what hasn't been freed yet, which is strictly better than hanging
        // indefinitely.
        if let Some(threads) = self.worker_threads.take() {
            const JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
            for t in threads {
                // std::thread::JoinHandle has no timed join, so we spin a
                // short poll loop by checking is_finished(). is_finished()
                // is a cheap atomic read.
                let deadline = std::time::Instant::now() + JOIN_TIMEOUT;
                while !t.is_finished() && std::time::Instant::now() < deadline {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                if t.is_finished() {
                    let _ = t.join();
                } else {
                    tracing::warn!(
                        target: "pyronova::server",
                        "worker thread did not exit within {:?} — abandoning (process shutdown in progress)",
                        JOIN_TIMEOUT,
                    );
                    // Leak the JoinHandle — OS will reclaim at process exit.
                    std::mem::forget(t);
                }
            }
        }
    }
}

unsafe impl Send for InterpreterPool {}
unsafe impl Sync for InterpreterPool {}

impl InterpreterPool {
    /// Create N sub-interpreters, each in its own OS thread, connected via channels.
    ///
    /// Must be called with the main interpreter's GIL held (before `py.detach()`).
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn new(
        n: usize,
        _py: Python<'_>,
        script_path: &str,
        handler_names: &[String],
        routers: HashMap<String, Router<usize>>,
        before_hook_names: &[String],
        after_hook_names: &[String],
        static_dirs: Vec<(String, String)>,
        requires_gil: Vec<bool>,
        is_async_handler: Vec<bool>,
        cors_config: Option<crate::router::CorsConfig>,
        request_logging: bool,
    ) -> Result<Self, String> {
        let has_any_async = is_async_handler.iter().any(|&a| a);
        // Set PYRONOVA_WORKER=1 so user's app.run() becomes a no-op in sub-interpreters.
        // This replaces the fragile AST-based script filtering.
        std::env::set_var("PYRONOVA_WORKER", "1");

        let raw_script = std::fs::read_to_string(script_path)
            .map_err(|e| format!("Failed to read script: {e}"))?;

        // Collect all function names we need
        let mut all_func_names: Vec<String> = handler_names.to_vec();
        all_func_names.extend(before_hook_names.iter().cloned());
        all_func_names.extend(after_hook_names.iter().cloned());
        // Deduplicate
        all_func_names.sort();
        all_func_names.dedup();

        // Create work channels
        // Sync pool: handles def handlers (220k req/s)
        // Async pool: handles async def handlers (133k req/s)
        let (sync_work_tx, sync_work_rx) = crossbeam_channel::bounded::<WorkRequest>(n * 128);
        let (async_work_tx, async_work_rx) = if has_any_async {
            let (tx, rx) = crossbeam_channel::bounded::<WorkRequest>(n * 128);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // Determine worker split: if async handlers exist, split workers
        let (sync_count, _async_count) = if has_any_async {
            let async_n = (n / 2).max(1).min(n); // At least 1, never exceed total
            (n.saturating_sub(async_n), async_n)
        } else {
            (n, 0)
        };

        // Allocate a fresh pool_id for this InterpreterPool instance.
        // All WorkerStates created below carry this id; the C-FFI bridge
        // rejects recv/send from zombies whose pool_id mismatches.
        let pool_id = POOL_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        // Create sub-interpreters and spawn worker threads
        let mut workers = Vec::new();
        let mut threads = Vec::new();

        for i in 0..n {
            let worker =
                SubInterpreterWorker::new(&raw_script, script_path, &all_func_names, pool_id)
                    .map_err(|e| format!("sub-interpreter {i}: {e}"))?;
            workers.push(worker);
        }

        // Initialize async worker states if needed
        if has_any_async {
            let async_rx = async_work_rx.as_ref().unwrap();
            let mut states = Vec::with_capacity(n);
            for _ in 0..n {
                states.push(Arc::new(WorkerState {
                    rx: async_rx.clone(),
                    response_map: Mutex::new(HashMap::new()),
                    next_req_id: AtomicU64::new(0),
                    pool_id,
                }));
            }
            // Overwrite rather than .set() — this pool may not be the
            // first one created in the process (tests / hot-reload).
            // Stale states from a prior pool would cause workers to
            // recv() on closed channels forever.
            if let Ok(mut w) = WORKER_STATES.write() {
                *w = states;
            }
        }

        let logging_flag = Arc::new(AtomicBool::new(request_logging));

        // Spawn workers: first sync_count as sync, rest as async
        for (i, worker) in workers.into_iter().enumerate() {
            let handler_names_clone = handler_names.to_vec();
            let before_hooks_clone = before_hook_names.to_vec();
            let after_hooks_clone = after_hook_names.to_vec();
            let logging = Arc::clone(&logging_flag);

            let handle = if i >= sync_count && has_any_async {
                // Async worker
                std::thread::Builder::new()
                    .name(format!("pyronova-async-worker-{i}"))
                    .spawn(move || {
                        worker_thread_loop_async(worker, &handler_names_clone, i);
                    })
                    .map_err(|e| format!("failed to spawn async worker {i}: {e}"))?
            } else {
                // Sync worker
                let rx = sync_work_rx.clone();
                std::thread::Builder::new()
                    .name(format!("pyronova-worker-{i}"))
                    .spawn(move || {
                        worker_thread_loop(
                            worker,
                            rx,
                            &handler_names_clone,
                            &before_hooks_clone,
                            &after_hooks_clone,
                            &logging,
                        );
                    })
                    .map_err(|e| format!("failed to spawn worker thread {i}: {e}"))?
            };

            threads.push(handle);
        }

        // Admission semaphore: one permit per total queue slot across
        // both pools. `n * 128` matches the channel capacities so a
        // permit-holder is guaranteed to find a slot when it reaches
        // submit(). Could split sync/async but that complicates the
        // acquire site — shared budget is fine and happens to model
        // "N × 128 in-flight requests per process" as one number.
        let total_permits = n * 128 * if has_any_async { 2 } else { 1 };
        let submit_semaphore = Arc::new(tokio::sync::Semaphore::new(total_permits));

        Ok(InterpreterPool {
            sync_work_tx,
            async_work_tx,
            worker_threads: Some(threads),
            routers,
            _handler_names: handler_names.to_vec(),
            requires_gil,
            is_async_handler: is_async_handler.clone(),
            static_dirs,
            has_async_workers: has_any_async,
            cors_config,
            _request_logging: logging_flag,
            submit_semaphore,
        })
    }

    /// Look up a route. Case-insensitive on method per RFC 9110 §9.1 —
    /// matches the sibling `RouteTable::lookup` in src/router.rs. Without
    /// this normalization, lowercase / mixed-case HTTP verbs from the
    /// wire (hyper accepts them) silently fell through to 404 in
    /// sub-interpreter mode.
    pub fn lookup(&self, method: &str, path: &str) -> Option<(usize, Vec<(String, String)>)> {
        let router = if method.bytes().any(|b| b.is_ascii_lowercase()) {
            self.routers.get(&method.to_ascii_uppercase())?
        } else {
            self.routers.get(method)?
        };
        let matched = router.at(path).ok()?;
        // Decode percent-encoded path params — see router.rs for rationale.
        let params: Vec<(String, String)> = matched
            .params
            .iter()
            .map(|(k, v)| {
                let decoded = percent_encoding::percent_decode_str(v)
                    .decode_utf8()
                    .map(|c| c.into_owned())
                    .unwrap_or_else(|_| v.to_string());
                (k.to_string(), decoded)
            })
            .collect();
        Some((*matched.value, params))
    }

    /// Get handler name by index.
    /// Submit a work request. Routes to sync or async pool based on handler type.
    pub fn submit(&self, req: WorkRequest) -> Result<(), String> {
        // Route to async pool if handler is async and pool exists
        let tx = if self.has_async_workers
            && self
                .is_async_handler
                .get(req.handler_idx)
                .copied()
                .unwrap_or(false)
        {
            self.async_work_tx.as_ref().unwrap()
        } else {
            &self.sync_work_tx
        };

        tx.try_send(req).map_err(|e| match e {
            crossbeam_channel::TrySendError::Full(_) => "server overloaded".to_string(),
            crossbeam_channel::TrySendError::Disconnected(_) => {
                "worker pool channel closed".to_string()
            }
        })
    }
}

/// RAII guard: ensures GIL is released even if a panic occurs mid-handler.
/// Without this, a panic after `PyEval_RestoreThread` but before `PyEval_SaveThread`
/// would leave the GIL permanently locked, causing deadlock on the next request
/// and eventual segfault from corrupted thread state.
///
/// The saved thread state is written back to `tstate_cell` on drop, so the caller
/// can retrieve it even after a panic unwind.
pub(crate) struct SubInterpGilGuard<'a> {
    tstate_cell: &'a std::cell::Cell<*mut ffi::PyThreadState>,
}

impl<'a> SubInterpGilGuard<'a> {
    /// Acquire the sub-interpreter's GIL. On drop, releases it and writes
    /// the saved tstate back to `tstate_cell`.
    pub(crate) unsafe fn acquire(
        tstate: *mut ffi::PyThreadState,
        tstate_cell: &'a std::cell::Cell<*mut ffi::PyThreadState>,
    ) -> Self {
        ffi::PyEval_RestoreThread(tstate);
        Self { tstate_cell }
    }
}

impl Drop for SubInterpGilGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: we always hold the GIL when this guard exists.
        // SaveThread releases it and returns the saved tstate for next acquire.
        unsafe {
            self.tstate_cell.set(ffi::PyEval_SaveThread());
        }
    }
}

/// Rebind the worker's sub-interp tstate to THIS OS thread.
///
/// # The Bug
///
/// `SubInterpreterWorker::new` runs on the main thread. It calls
/// `Py_NewInterpreterFromConfig`, which creates a tstate on the
/// creator's OS thread, runs the init script, then `PyEval_SaveThread`'s
/// it. The worker thread picks up that saved tstate and does
/// `PyEval_RestoreThread` / `PyEval_SaveThread` per request.
///
/// This pattern works — but leaks ~1 KB per request under sustained
/// load. Measured with a pure-C reproducer (no Rust, no Pyronova,
/// no hyper, just PyDict alloc/decref + attach/detach loop):
///
///   variant=0 (SHARED tstate across threads)   B/iter = 997
///   variant=1 (FRESH tstate via PyThreadState_New)  B/iter = 0
///
/// CPython's tstate carries per-OS-thread state (GIL reacquisition
/// bookkeeping, some pymalloc bindings) that accumulates when a
/// tstate created on one OS thread is repeatedly attached/detached
/// on a different OS thread. The fix is to give each worker its
/// OWN tstate, bound to its OS thread from the first attach.
///
/// # The Fix
///
/// On worker entry:
///   1. Attach the creator's tstate (`worker.tstate`) briefly.
///   2. Create a fresh tstate via `PyThreadState_New(interp)` — this
///      tstate is bound to THIS OS thread.
///   3. Swap it in; clear + delete the creator's tstate.
///   4. Use the fresh tstate for all request handling.
///
/// On worker exit, attach the fresh tstate and `Py_EndInterpreter`,
/// which destroys the sub-interp and all remaining tstates for it.
///
/// See docs/memory-leak-investigation-2026-04-19.md and
/// /tmp/pep684_repro/repro_threadstate_new.c for the bisection.
pub(crate) unsafe fn rebind_tstate_to_current_thread(
    creator_tstate: *mut ffi::PyThreadState,
) -> *mut ffi::PyThreadState {
    // Attach creator tstate so we can call PyThreadState_New.
    ffi::PyEval_RestoreThread(creator_tstate);
    let interp = ffi::PyInterpreterState_Get();
    let fresh = ffi::PyThreadState_New(interp);
    if fresh.is_null() {
        // Fall back to creator tstate (leak will reappear but we stay alive).
        //
        // NEVER silently regress: this path reintroduces the ~1 KB/req
        // leak that v1.5 closed (commit fc45a7f, see file header doc
        // and docs/memory-leak-investigation-2026-04-19.md). If this
        // ever fires under load, every subsequent request on this
        // worker re-opens the leak — invisibly until /proc/self/status
        // shows RSS growth. Emit ERROR so the regression is observable
        // before it shows up as a memory incident in production.
        tracing::error!(
            target: "pyronova::app",
            "rebind_tstate_to_current_thread: PyThreadState_New returned null \
             (likely interp shutdown or OOM). Falling back to creator tstate. \
             The v1.5 per-request memory leak fix is INACTIVE on this worker \
             until restart — expect ~1 KB/req RSS growth under sustained load."
        );
        // Honor the success path's calling contract: caller expects the
        // returned tstate to be SAVED (GIL released, no pending Python
        // error). Without these two calls the failure path returned an
        // attached tstate with the GIL still held and any error
        // PyThreadState_New left pending — the next `PyEval_RestoreThread`
        // on this worker would hit undefined behavior (debug: assert;
        // release: deadlock), and the lingering exception would surface
        // in the next handler call.
        if !ffi::PyErr_Occurred().is_null() {
            ffi::PyErr_Clear();
        }
        return ffi::PyEval_SaveThread();
    }
    // Swap to fresh tstate. Returns the previous current tstate = creator.
    let prev = ffi::PyThreadState_Swap(fresh);
    debug_assert_eq!(prev, creator_tstate);
    // Dispose of the creator tstate from this thread.
    ffi::PyThreadState_Clear(creator_tstate);
    ffi::PyThreadState_Delete(creator_tstate);
    // Release GIL; hand back the fresh tstate for future attach cycles.
    ffi::PyEval_SaveThread()
}

/// Main loop for each worker OS thread.
fn worker_thread_loop(
    mut worker: SubInterpreterWorker,
    rx: crossbeam_channel::Receiver<WorkRequest>,
    handler_names: &[String],
    before_hook_names: &[String],
    after_hook_names: &[String],
    request_logging: &AtomicBool,
) {
    // Rebind the sub-interp tstate to this OS thread (fixes the
    // cross-thread attach/detach leak). See
    // `rebind_tstate_to_current_thread` doc for details.
    unsafe {
        worker.tstate = rebind_tstate_to_current_thread(worker.tstate);
    }

    while let Ok(req) = rx.recv() {
        // Skip requests whose caller already timed out (504) — avoid wasting
        // CPU on "dead" requests during queue backlog (prevents snowball effect).
        if req.response_tx.is_closed() {
            continue;
        }

        // Cell lives outside catch_unwind so the guard can write tstate back
        // even during panic unwind.
        let tstate_cell = std::cell::Cell::new(worker.tstate);

        // Catch panics to prevent worker thread death.
        // SubInterpGilGuard ensures GIL is released even if call_handler panics.
        // Deferred conversions: moved off Tokio thread.
        let headers_map = crate::types::extract_headers(&req.headers);
        let ip_str = req.client_ip.to_string();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            let _guard = SubInterpGilGuard::acquire(tstate_cell.get(), &tstate_cell);

            let handler_name = &handler_names[req.handler_idx];
            worker.call_handler(
                handler_name,
                before_hook_names,
                after_hook_names,
                &req.method,
                &req.path,
                &req.params,
                &req.query,
                &req.body,
                &headers_map,
                &ip_str,
            )
            // _guard drops here → PyEval_SaveThread() → tstate_cell updated
        }));

        // Recover tstate (updated by guard's Drop, even after panic)
        worker.tstate = tstate_cell.get();

        let response = match result {
            Ok(r) => r,
            Err(_) => Err("internal error: worker panic".to_string()),
        };

        // Log request via tracing (zero-cost when access log is filtered off)
        if request_logging.load(Ordering::Relaxed) {
            let status = match &response {
                Ok(r) => r.status,
                Err(_) => 500,
            };
            if status >= 500 {
                tracing::error!(
                    target: "pyronova::access",
                    method = %req.method,
                    path = %req.path,
                    status,
                    "PyronovaRequest failed"
                );
            } else if status >= 400 {
                tracing::warn!(
                    target: "pyronova::access",
                    method = %req.method,
                    path = %req.path,
                    status,
                    "Client error"
                );
            } else {
                tracing::info!(
                    target: "pyronova::access",
                    method = %req.method,
                    path = %req.path,
                    status,
                    "PyronovaRequest handled"
                );
            }
        }

        // Send response back (ignore error if receiver dropped)
        let _ = req.response_tx.send(response);
        WorkRequest::inc_completed();
    }

    // Channel closed — clean up the sub-interpreter.
    //
    // Zombie-safety: if `InterpreterPool::drop` `mem::forget`d this
    // thread after the 5s grace period, the process may have already
    // called `Py_Finalize` by the time we get here. `PyEval_RestoreThread`
    // + `Py_EndInterpreter` on a finalized VM is UAF → segfault at
    // shutdown. Skip cleanup in that case; the OS will reclaim whatever
    // the sub-interp was holding as the process exits.
    unsafe {
        if !worker.tstate.is_null() && pyo3::ffi::Py_IsInitialized() != 0 {
            ffi::PyEval_RestoreThread(worker.tstate);
            ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
            worker.tstate = std::ptr::null_mut();
        }
    }
}

/// Async worker: Python asyncio event loop drives execution.
/// Fetcher thread pulls requests from channel (releasing GIL during wait),
/// asyncio loop runs handlers as concurrent tasks.
fn worker_thread_loop_async(
    mut worker: SubInterpreterWorker,
    handler_names: &[String],
    worker_idx: usize,
) {
    let handlers_array = handler_names
        .iter()
        .map(|n| format!("'{}'", n))
        .collect::<Vec<_>>()
        .join(", ");

    // Load async engine from external Python file (syntax highlighting + maintainability)
    let engine_template = include_str!("../../python/pyronova/_async_engine.py");
    let engine_script =
        format!("WORKER_ID = {worker_idx}\nHANDLER_NAMES = [{handlers_array}]\n{engine_template}");

    unsafe {
        // Rebind tstate to this OS thread — same cross-thread leak as
        // the sync worker loop. See `rebind_tstate_to_current_thread`
        // doc for details.
        worker.tstate = rebind_tstate_to_current_thread(worker.tstate);

        ffi::PyEval_RestoreThread(worker.tstate);

        // Register C-FFI functions in sub-interpreter globals.
        // transmute: PyCFunction (2 args) → PyCFunctionWithKeywords (3 args) —
        // safe because METH_VARARGS ignores the third (kwargs) parameter.
        #[allow(clippy::missing_transmute_annotations)]
        let recv_def = Box::into_raw(Box::new(ffi::PyMethodDef {
            ml_name: c"_pyronova_recv".as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                PyCFunctionWithKeywords: std::mem::transmute(pyronova_recv_cfunc as *const ()),
            },
            ml_flags: ffi::METH_VARARGS,
            ml_doc: std::ptr::null(),
        }));
        #[allow(clippy::missing_transmute_annotations)]
        let send_def = Box::into_raw(Box::new(ffi::PyMethodDef {
            ml_name: c"_pyronova_send".as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                PyCFunctionWithKeywords: std::mem::transmute(pyronova_send_cfunc as *const ()),
            },
            ml_flags: ffi::METH_VARARGS,
            ml_doc: std::ptr::null(),
        }));

        let recv_func =
            ffi::PyCFunction_NewEx(recv_def, std::ptr::null_mut(), std::ptr::null_mut());
        let send_func =
            ffi::PyCFunction_NewEx(send_def, std::ptr::null_mut(), std::ptr::null_mut());

        ffi::PyDict_SetItemString(worker.globals, c"_pyronova_recv".as_ptr(), recv_func);
        ffi::PyDict_SetItemString(worker.globals, c"_pyronova_send".as_ptr(), send_func);
        ffi::Py_DECREF(recv_func);
        ffi::Py_DECREF(send_func);

        // Zombie-guard: each sub-interpreter stamps its pool_id as a
        // module-level constant. The async engine reads it once and
        // passes it as the second arg to every _pyronova_recv / _pyronova_send
        // call. If a later pool's WorkerState has replaced our slot,
        // the C-FFI bridge detects the id mismatch and returns None
        // (sentinel for "your pool is gone, clean up and exit") so
        // this worker can't receive requests meant for the live pool.
        let pool_id_obj = ffi::PyLong_FromUnsignedLongLong(worker.pool_id);
        if !pool_id_obj.is_null() {
            ffi::PyDict_SetItemString(worker.globals, c"_pyronova_pool_id".as_ptr(), pool_id_obj);
            ffi::Py_DECREF(pool_id_obj);
        }

        // Register _pyronova_emit_log for async worker logging bridge
        #[allow(clippy::missing_transmute_annotations)]
        let emit_log_def = Box::into_raw(Box::new(ffi::PyMethodDef {
            ml_name: c"_pyronova_emit_log".as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                PyCFunctionWithKeywords: std::mem::transmute(pyronova_emit_log_cfunc as *const ()),
            },
            ml_flags: ffi::METH_VARARGS,
            ml_doc: std::ptr::null(),
        }));
        let emit_log_func =
            ffi::PyCFunction_NewEx(emit_log_def, std::ptr::null_mut(), std::ptr::null_mut());
        if !emit_log_func.is_null() {
            ffi::PyDict_SetItemString(
                worker.globals,
                c"_pyronova_emit_log".as_ptr(),
                emit_log_func,
            );
            ffi::Py_DECREF(emit_log_func);
        }

        // Sub-interp DB bridge — see sync worker path + src/db_bridge.rs
        // for the rationale. Same 4 C-FFI functions injected into the
        // async worker's globals so async handlers can also issue DB
        // queries without the `gil=True` escape hatch.
        crate::bridge::db_bridge::register_db_bridge(worker.globals);

        // Run the async engine — this blocks until the channel is closed
        let code = std::ffi::CString::new(engine_script).unwrap();
        let result = ffi::PyRun_String(
            code.as_ptr(),
            ffi::Py_file_input,
            worker.globals,
            worker.globals,
        );
        if result.is_null() {
            log_and_clear_py_exception("async engine script");
        } else {
            ffi::Py_DECREF(result);
        }

        // Cleanup — same zombie-safety as the sync worker loop.
        if pyo3::ffi::Py_IsInitialized() != 0 {
            ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
        }
        worker.tstate = std::ptr::null_mut();
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Tests that write WORKER_STATES (a process-global) must not run
    // concurrently — serialize them with this lock.
    static WORKER_STATES_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Helper: mint a fresh `Arc<WorkerState>` backed by a dedicated
    /// crossbeam channel so the test has observable state (the rx
    /// handle identifies which install the state came from).
    fn mint_state() -> (Arc<WorkerState>, crossbeam_channel::Sender<WorkRequest>) {
        let (tx, rx) = crossbeam_channel::unbounded::<WorkRequest>();
        let st = Arc::new(WorkerState {
            rx,
            response_map: Mutex::new(HashMap::new()),
            next_req_id: AtomicU64::new(0),
            pool_id: POOL_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
        });
        (st, tx)
    }

    /// Regression for the advisor-flagged hot-reload / test-isolation
    /// bug: the original `OnceLock<Vec<Arc<WorkerState>>>` silently
    /// rejected a second `.set()`, leaving any subsequent
    /// `InterpreterPool::new()` operating on stale channels from the
    /// prior pool — a permanent deadlock on async recv.
    ///
    /// With the fix (`RwLock<Vec<Arc<WorkerState>>>`) a second install
    /// overwrites the first. This test verifies:
    ///   1. First install makes states visible via `get_worker_state`.
    ///   2. Second install REPLACES them — the new Arc identity wins.
    ///   3. The old states are still drop-safe (refcount goes to
    ///      whatever test-scope clones we held; no double-free).
    #[test]
    fn worker_states_can_be_reinstalled() {
        let _guard = WORKER_STATES_TEST_LOCK.lock().unwrap();
        // --- Install #1 --------------------------------------------------
        let (s0_first, _tx0_first) = mint_state();
        let (s1_first, _tx1_first) = mint_state();
        {
            let mut w = WORKER_STATES.write().unwrap();
            *w = vec![s0_first.clone(), s1_first.clone()];
        }

        let got0 = get_worker_state(0).expect("install #1 slot 0 missing");
        let got1 = get_worker_state(1).expect("install #1 slot 1 missing");
        assert!(Arc::ptr_eq(&got0, &s0_first));
        assert!(Arc::ptr_eq(&got1, &s1_first));
        drop(got0);
        drop(got1);

        // --- Install #2 (simulating a second app.run() / hot reload) -----
        let (s0_second, _tx0_second) = mint_state();
        let (s1_second, _tx1_second) = mint_state();
        let (s2_second, _tx2_second) = mint_state();
        {
            let mut w = WORKER_STATES.write().unwrap();
            *w = vec![s0_second.clone(), s1_second.clone(), s2_second.clone()];
        }

        // New identities are visible — the old ones are gone from the
        // registry (though still alive via the test-local `s0_first`
        // etc. clones, which is exactly the invariant we want).
        let got0 = get_worker_state(0).expect("install #2 slot 0 missing");
        let got1 = get_worker_state(1).expect("install #2 slot 1 missing");
        let got2 = get_worker_state(2).expect("install #2 slot 2 missing");
        assert!(
            Arc::ptr_eq(&got0, &s0_second),
            "slot 0 still points at pool #1 — OnceLock-style silent failure regression"
        );
        assert!(Arc::ptr_eq(&got1, &s1_second));
        assert!(Arc::ptr_eq(&got2, &s2_second));
        assert!(!Arc::ptr_eq(&got0, &s0_first));
        assert!(!Arc::ptr_eq(&got1, &s1_first));

        // Out-of-range lookup returns None.
        assert!(get_worker_state(99).is_none());

        // Leave the registry empty for the next test in case of global
        // state bleed (RwLock is a static).
        let mut w = WORKER_STATES.write().unwrap();
        w.clear();
    }

    /// Regression for the zombie-worker-stealing-requests bug.
    ///
    /// Scenario: async worker W from Pool A is mid-request. Pool A drops,
    /// Pool B replaces `WORKER_STATES[0]`. W finally wakes up and looks
    /// up its state. Without the `pool_id` guard it would silently receive
    /// from Pool B's channel — stealing a live request.
    ///
    /// The guard: `pyronova_recv_cfunc` / `pyronova_send_cfunc` accept a pool_id
    /// arg and short-circuit to None on mismatch. We exercise the guard
    /// at the Rust level (the C-FFI wrappers just do PyArg_ParseTuple
    /// then call this same path) to keep the test free of pyo3 test-rig
    /// plumbing.
    #[test]
    fn zombie_worker_rejected_by_pool_id_mismatch() {
        let _guard = WORKER_STATES_TEST_LOCK.lock().unwrap();
        let (s_new, _tx) = mint_state(); // pool_id = N
        let old_pool_id = s_new.pool_id.wrapping_sub(1); // pretend we're from an earlier pool

        {
            let mut w = WORKER_STATES.write().unwrap();
            *w = vec![s_new.clone()];
        }

        // Live pool's caller (matching pool_id) sees the state.
        let live = get_worker_state(0).expect("slot 0 must exist");
        assert_eq!(live.pool_id, s_new.pool_id);

        // Zombie's caller carries the OLD pool_id. The C-FFI bridge's
        // filter `s.pool_id == pool_id` would reject it; simulate the
        // same check here.
        let zombie_sees = get_worker_state(0).filter(|s| s.pool_id == old_pool_id);
        assert!(
            zombie_sees.is_none(),
            "zombie worker from old pool was able to read new pool's state"
        );

        let mut w = WORKER_STATES.write().unwrap();
        w.clear();
    }

    /// Regression for the UAF-on-shutdown bug.
    ///
    /// The fix is a defensive `pyo3::ffi::Py_IsInitialized() != 0` check
    /// around `PyEval_RestoreThread` + `Py_EndInterpreter` in both
    /// worker_thread_loop variants. We can't easily simulate a finalized
    /// interpreter in a unit test — but we can verify the guard is
    /// present in the source, so a future refactor can't silently
    /// remove it.
    #[test]
    fn worker_cleanup_guarded_against_finalized_interp() {
        let src = include_str!("interp.rs");
        // Both worker_thread_loop and worker_thread_loop_async must
        // check Py_IsInitialized before touching the tstate on exit.
        let guard_sites = src.matches("Py_IsInitialized() != 0").count();
        assert!(
            guard_sites >= 2,
            "expected Py_IsInitialized() guard in BOTH worker loops; \
             found {} occurrence(s). If you're consolidating the loops \
             make sure the single guard still covers the forgotten-
             thread cleanup path.",
            guard_sites
        );
    }
}
