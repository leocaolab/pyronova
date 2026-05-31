//! `SubInterpreterWorker` — owns one CPython sub-interpreter and runs
//! request handlers inside it. This is the densest concentration of
//! `unsafe` + raw `pyo3::ffi` in the codebase.

use std::collections::HashMap;

use pyo3::ffi;

use super::convert::*;
use super::ffi::*;
use super::pool::*;

// ---------------------------------------------------------------------------
// Safe sub-interpreter
// ---------------------------------------------------------------------------

pub(crate) struct SubInterpreterWorker {
    /// Thread state (saved after releasing GIL)
    pub(crate) tstate: *mut ffi::PyThreadState,
    /// Handler function pointers keyed by name
    handlers: HashMap<String, *mut ffi::PyObject>,
    /// Globals dict of this sub-interpreter
    pub(crate) globals: *mut ffi::PyObject,
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
    pub(crate) pool_id: u64,
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
