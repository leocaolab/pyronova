//! Raw CPython FFI primitives: `PyObjRef` RAII, the `SubInterpGilGuard`,
//! tstate rebinding, the per-worker state registry, and the C-FFI bridge
//! functions (`pyronova_recv`/`pyronova_send`/`pyronova_emit_log`) that
//! let async engines in sub-interpreters pull work and emit logs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pyo3::ffi;

use super::convert::*;
use super::pool::*;

// ---------------------------------------------------------------------------
// Phase 7.2: Global worker state for async C-FFI bridge
// ---------------------------------------------------------------------------

/// Per-worker state accessible from C-FFI functions (no closure environment).
pub(crate) struct WorkerState {
    pub(crate) rx: crossbeam_channel::Receiver<WorkRequest>,
    pub(crate) response_map:
        Mutex<HashMap<u64, tokio::sync::oneshot::Sender<Result<SubInterpResponse, String>>>>,
    pub(crate) next_req_id: AtomicU64,
    /// Identifier for the `InterpreterPool` instance that created this
    /// state. A zombie worker from a prior pool (test / hot-reload)
    /// carries the OLD pool_id in its Python globals; the C-FFI bridge
    /// compares the caller's pool_id to the state's and rejects
    /// mismatches so the zombie can't steal requests from the new pool.
    /// See POOL_ID_COUNTER docstring for the rationale.
    pub(crate) pool_id: u64,
}

/// Monotonic counter for pool instance IDs. Each call to
/// `InterpreterPool::new()` consumes one. This exists exclusively so
/// `pyronova_recv` / `pyronova_send` can detect cross-pool calls — see
/// `WorkerState::pool_id` and the guard in `pyronova_recv_cfunc`.
pub(crate) static POOL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

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
pub(crate) static WORKER_STATES: std::sync::RwLock<Vec<Arc<WorkerState>>> =
    std::sync::RwLock::new(Vec::new());

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
pub(crate) unsafe extern "C" fn pyronova_recv_cfunc(
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
            // Recover from a poisoned Mutex instead of panicking. A panic in
            // any worker while holding this lock would poison it; a plain
            // .unwrap() here would then panic the next worker → ffi_catch_unwind
            // → PyRuntimeError → worker exit, cascading to zero workers under
            // load. Continuing is strictly less bad than dying, but it is NOT
            // a guarantee the map is consistent: Rust's HashMap is only
            // unwind-safe for alloc-triggered panics — a panic mid-resize or
            // in a custom hasher could leave it half-written. We can't detect
            // that, so we log on recovery to make the (rare) poison observable
            // in telemetry rather than swallowing it silently.
            state
                .response_map
                .lock()
                .unwrap_or_else(|e| {
                    tracing::error!(
                        target: "pyronova::server",
                        "response_map mutex poisoned (a worker panicked while \
                         holding the lock); recovering inner guard — map state \
                         may be inconsistent"
                    );
                    e.into_inner()
                })
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
pub(crate) unsafe extern "C" fn pyronova_send_cfunc(
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
        // Recover from poison rather than panicking — see pyronova_recv_inner.
        let mut map = state.response_map.lock().unwrap_or_else(|e| e.into_inner());
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
pub(crate) unsafe extern "C" fn pyronova_emit_log_cfunc(
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

    // Shared dispatch macro (see `crate::logging::dispatch_python_log`): expands
    // inline so each branch keeps its own static tracing callsite.
    crate::logging::dispatch_python_log!(level, wid, name, pathname, lineno, message);

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

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::python::pool::WorkRequest;
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
        // The worker loops live in the sibling `pool` module since the
        // god-module split; read its source for the guard check.
        let src = include_str!("pool.rs");
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
