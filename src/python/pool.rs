//! Channel-based interpreter pool: domain types (`WorkRequest`,
//! `SubInterpResponse`), the `InterpreterPool` orchestrator, and the
//! per-OS-thread worker loops (sync + async) that drive
//! `SubInterpreterWorker`s.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use matchit::Router;
use pyo3::ffi;
use pyo3::prelude::*;

use super::convert::*;
use super::ffi::*;
use super::worker::*;

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
        // Route to async pool if handler is async and pool exists.
        // `async_work_tx.is_some()` is the single source of truth for
        // "async workers exist" — set iff `has_any_async` at construction.
        let tx = if self.async_work_tx.is_some()
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
            // Account for the skipped request so the leak_detect invariant
            // (inc_created == inc_completed at steady state) holds. A dropped
            // dead request is still a fully-accounted WorkRequest, not a leak.
            WorkRequest::inc_completed();
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

        // Build the logging-bridge PyMethodDef up front so the registration
        // closure below can reclaim its Box on any early-failure path.
        #[allow(clippy::missing_transmute_annotations)]
        let emit_log_def = Box::into_raw(Box::new(ffi::PyMethodDef {
            ml_name: c"_pyronova_emit_log".as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                PyCFunctionWithKeywords: std::mem::transmute(pyronova_emit_log_cfunc as *const ()),
            },
            ml_flags: ffi::METH_VARARGS,
            ml_doc: std::ptr::null(),
        }));

        // Register the C-FFI async bridge (_pyronova_recv/_pyronova_send), the
        // zombie-guard pool_id, and the logging bridge (_pyronova_emit_log) into
        // the sub-interpreter globals. Every one of these is on the async
        // engine's hot path: the fetcher thread calls _pyronova_recv /
        // _pyronova_send on each request, reads _pyronova_pool_id to detect a
        // replaced slot, and routes logs through _pyronova_emit_log. So a null
        // from PyCFunction_NewEx / PyLong_FromUnsignedLongLong (OOM or corrupted
        // interpreter state) is FATAL — continuing would spawn a worker that
        // crashes with AttributeError on its very first request. We must not let
        // that worker run; we abort it instead (see the bail-out below).
        //
        // (PyDict_SetItemString also rejects null values per the CPython
        // contract — passing one would segfault — so the null check is required
        // for safety as well as correctness.)
        //
        // Ownership: on success PyCFunction_NewEx borrows the PyMethodDef for the
        // function object's lifetime, so we intentionally leak the Box; on a null
        // path no object took ownership, so we reclaim every not-yet-consumed Box
        // to avoid leaking ~80 bytes each (mirrors db_bridge.rs).
        let globals = worker.globals;
        let pool_id = worker.pool_id;
        // Already inside the function's outer `unsafe` block (above), so the
        // closure body inherits that context — no inner `unsafe` needed.
        let register_bridge = || -> bool {
            let recv_func =
                ffi::PyCFunction_NewEx(recv_def, std::ptr::null_mut(), std::ptr::null_mut());
            if recv_func.is_null() {
                let _ = Box::from_raw(recv_def);
                let _ = Box::from_raw(send_def);
                let _ = Box::from_raw(emit_log_def);
                return false;
            }
            // PyDict_SetItemString returns -1 on failure (e.g. allocation
            // failure growing the dict). Unchecked, the bridge function would
            // be silently missing from globals → AttributeError on the first
            // async request, with the root cause hidden. DECREF the function
            // we still own (SetItemString does not steal the ref), reclaim the
            // not-yet-consumed defs, and bail so the caller's
            // log_and_clear_pyerr surfaces it. Mirrors the null branches above.
            let rc = ffi::PyDict_SetItemString(globals, c"_pyronova_recv".as_ptr(), recv_func);
            ffi::Py_DECREF(recv_func);
            if rc != 0 {
                let _ = Box::from_raw(send_def);
                let _ = Box::from_raw(emit_log_def);
                return false;
            }

            let send_func =
                ffi::PyCFunction_NewEx(send_def, std::ptr::null_mut(), std::ptr::null_mut());
            if send_func.is_null() {
                let _ = Box::from_raw(send_def);
                let _ = Box::from_raw(emit_log_def);
                return false;
            }
            let rc = ffi::PyDict_SetItemString(globals, c"_pyronova_send".as_ptr(), send_func);
            ffi::Py_DECREF(send_func);
            if rc != 0 {
                let _ = Box::from_raw(emit_log_def);
                return false;
            }

            // Zombie-guard: each sub-interpreter stamps its pool_id as a
            // module-level constant. The async engine reads it once and
            // passes it as the second arg to every _pyronova_recv /
            // _pyronova_send call. If a later pool's WorkerState has replaced
            // our slot, the C-FFI bridge detects the id mismatch and returns
            // None (sentinel for "your pool is gone, clean up and exit") so
            // this worker can't receive requests meant for the live pool.
            let pool_id_obj = ffi::PyLong_FromUnsignedLongLong(pool_id);
            if pool_id_obj.is_null() {
                let _ = Box::from_raw(emit_log_def);
                return false;
            }
            let rc = ffi::PyDict_SetItemString(globals, c"_pyronova_pool_id".as_ptr(), pool_id_obj);
            ffi::Py_DECREF(pool_id_obj);
            if rc != 0 {
                let _ = Box::from_raw(emit_log_def);
                return false;
            }

            let emit_log_func =
                ffi::PyCFunction_NewEx(emit_log_def, std::ptr::null_mut(), std::ptr::null_mut());
            if emit_log_func.is_null() {
                let _ = Box::from_raw(emit_log_def);
                return false;
            }
            let rc = ffi::PyDict_SetItemString(globals, c"_pyronova_emit_log".as_ptr(), emit_log_func);
            ffi::Py_DECREF(emit_log_func);
            if rc != 0 {
                return false;
            }

            true
        };

        if !register_bridge() {
            tracing::error!(
                target: "pyronova::server",
                worker = worker_idx,
                "failed to register C-FFI bridge in sub-interpreter (OOM or \
                 corrupted interpreter state); aborting async worker instead of \
                 running an engine that would crash on first request"
            );
            // Clean up the half-initialized sub-interpreter and exit the worker.
            // Same zombie-safety guard as the normal cleanup path below.
            if pyo3::ffi::Py_IsInitialized() != 0 {
                ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
            }
            worker.tstate = std::ptr::null_mut();
            return;
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
