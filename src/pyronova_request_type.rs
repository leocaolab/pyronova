//! `_Request` as a raw C-API heap type.
//!
//! PEP 684 sub-interpreters have a bug where a normal Python-class
//! `tp_dealloc` does not reliably DECREF `__slots__` members —
//! per-request string and bytes values orphan-leak in the sub-interp's
//! PyMalloc arena (~1000 B/request in the hello bench, ~500 MB/s
//! at load).
//!
//! pyo3 0.28 refuses to register `#[pyclass]` types in sub-interpreters
//! (pymodule.rs hard-codes the rejection). So we bypass pyo3 entirely:
//! build the type directly via `PyType_FromSpec` + `PyMemberDef` with
//! a custom `tp_dealloc` that synchronously `Py_XDECREF`s every slot.
//!
//! One type object is created per sub-interpreter (types are per-interp
//! state) and injected into the sub-interp's globals as `_Request`.
//!
//! **Defensive invariant: NEVER add `Py_TPFLAGS_BASETYPE` to the flags.**
//! If Python can subclass this type, CPython's `subtype_dealloc` takes
//! over and silently bypasses our custom `tp_dealloc` — the whole leak
//! fix becomes a no-op. We learned this the hard way when per-request
//! dealloc count stayed at 0 despite tp_dealloc being wired correctly.
//! Helper methods (`.text()`, `.json()`, `.body`, `.query_params`) are
//! instead monkey-patched onto the heap type at sub-interp init.
//!
//! ## Lazy dict slots (params, headers)
//!
//! The two map-typed slots — `params` and `headers` — are built lazily.
//! The per-request hot path was paying for a `PyDict_New` plus
//! `PyUnicode_FromStringAndSize` times (2 × number_of_headers) on every
//! request, even when the handler never touched `req.headers`. For a
//! typical 3-header request that's 7+ PyObject allocations at 400k+ rps
//! = several million needless PyObject allocs per second.
//!
//! The raw Rust data (`Vec<(String,String)>` for params,
//! `HashMap<String,String>` for headers) is stored in a heap-allocated
//! `LazyMaps` struct pinned to the `_Request` instance. The getter
//! (`PyGetSetDef`) checks the cache slot first; if null, builds the
//! dict from the raw data, stores in cache, returns. Second access is
//! a bare pointer read + `Py_INCREF`. The 5 simple slots (method, path,
//! query, body_bytes, client_ip) stay eager — their single
//! `PyUnicode_FromStringAndSize` / `PyBytes_FromStringAndSize` is cheap.

use pyo3::ffi;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void};

#[cfg(feature = "leak_detect")]
use std::sync::atomic::{AtomicUsize, Ordering};

// ---------------------------------------------------------------------------
// Diagnostics (leak_detect feature only)
// ---------------------------------------------------------------------------

#[cfg(feature = "leak_detect")]
pub(crate) static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "leak_detect")]
pub(crate) static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "leak_detect")]
pub(crate) static SLOT_RC: [[AtomicUsize; 8]; 7] =
    [const { [const { AtomicUsize::new(0) }; 8] }; 7];

#[cfg(feature = "leak_detect")]
unsafe fn record_slot_rc(ordinal: usize, ptr: *mut ffi::PyObject) {
    if ptr.is_null() {
        return;
    }
    let rc = ffi::Py_REFCNT(ptr);
    let bucket = if rc < 0 {
        0
    } else if rc >= 7 {
        7
    } else {
        rc as usize
    };
    SLOT_RC[ordinal][bucket].fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "leak_detect")]
pub fn slot_rc_report() -> String {
    let slots = [
        "method",
        "path",
        "params",
        "query",
        "body_bytes",
        "headers",
        "client_ip",
    ];
    let mut s = String::from("slot rc@dealloc histogram:\n");
    for (i, name) in slots.iter().enumerate() {
        s.push_str(&format!("  {:10}", name));
        for (b, cell) in SLOT_RC[i].iter().enumerate() {
            let n = cell.load(Ordering::Relaxed);
            let label = if b == 7 {
                "7+".to_string()
            } else {
                b.to_string()
            };
            s.push_str(&format!(" rc{}:{}", label, n));
        }
        s.push('\n');
    }
    s
}

// ---------------------------------------------------------------------------
// Lazy dict storage
// ---------------------------------------------------------------------------

/// Owned Rust-side storage for the two lazy-built dict slots. Heap
/// allocated once per request (Box<LazyMaps>) and freed in
/// `pyronova_request_dealloc`. Lifetime is tied to the _Request instance.
pub(crate) struct LazyMaps {
    pub params: Vec<(String, String)>,
    pub headers: HashMap<String, String>,
    /// Request body as raw Rust-owned bytes. Lazily materialized to a
    /// PyBytes on first access to `req.body_bytes`. For simple handlers
    /// (and nearly all plaintext/JSON benchmarks) the body is never read
    /// and we skip a potentially-megabyte PyBytes_FromStringAndSize
    /// copy entirely. Quant/AI workloads that DO read the body pay the
    /// copy once, same wall-clock as the old eager path.
    pub body: Vec<u8>,
}

// ---------------------------------------------------------------------------
// RequestInner
// ---------------------------------------------------------------------------

/// C-layout storage backing a `_Request` instance.
///
/// Eager slots (method, path, query, body_bytes, client_ip) carry owned
/// `*mut PyObject` strong refs, set by `alloc_and_init_lazy` / `tp_init` and
/// `Py_XDECREF`'d by `pyronova_request_dealloc`.
///
/// Lazy slots (params_cache, headers_cache) start null on
/// `alloc_and_init_lazy` and fill on first getter call. `lazy_maps`
/// owns the Rust-side raw data; dropped in the dealloc.
#[repr(C)]
pub(crate) struct RequestInner {
    pub ob_base: ffi::PyObject,
    pub method: *mut ffi::PyObject,
    pub path: *mut ffi::PyObject,
    pub query: *mut ffi::PyObject,
    pub client_ip: *mut ffi::PyObject,
    pub params_cache: *mut ffi::PyObject,
    pub headers_cache: *mut ffi::PyObject,
    /// Lazy PyBytes cache for the request body. Starts null; filled by
    /// `get_body_bytes` on first access by copying `LazyMaps.body`.
    pub body_bytes_cache: *mut ffi::PyObject,
    /// Non-null when built via `alloc_and_init_lazy`. Dropped via
    /// `Box::from_raw` in dealloc. Null when built via `tp_init`
    /// (Python-side _Request(...)) — in that case caches are
    /// pre-populated eagerly.
    pub lazy_maps: *mut LazyMaps,
}

// --- tp_dealloc ---------------------------------------------------------
//
// NOTE: We deliberately do NOT set `Py_TPFLAGS_HAVE_GC` on this type.
// See commit history / original docstring for the GC vs leak rationale
// — tl;dr: enabling GC tracking reintroduced a per-request leak under
// PEP 684 OWN_GIL sub-interps. Safe to opt out because our objects
// don't form cycles.

unsafe extern "C" fn pyronova_request_dealloc(obj: *mut ffi::PyObject) {
    #[cfg(feature = "leak_detect")]
    DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);

    let inner = obj as *mut RequestInner;
    let tp = ffi::Py_TYPE(obj);

    #[cfg(feature = "leak_detect")]
    {
        record_slot_rc(0, (*inner).method);
        record_slot_rc(1, (*inner).path);
        record_slot_rc(2, (*inner).params_cache);
        record_slot_rc(3, (*inner).query);
        record_slot_rc(4, (*inner).body_bytes_cache);
        record_slot_rc(5, (*inner).headers_cache);
        record_slot_rc(6, (*inner).client_ip);
    }

    ffi::Py_XDECREF((*inner).method);
    ffi::Py_XDECREF((*inner).path);
    ffi::Py_XDECREF((*inner).query);
    ffi::Py_XDECREF((*inner).client_ip);
    ffi::Py_XDECREF((*inner).params_cache);
    ffi::Py_XDECREF((*inner).headers_cache);
    ffi::Py_XDECREF((*inner).body_bytes_cache);

    if !(*inner).lazy_maps.is_null() {
        // Reclaim the Box — its Drop frees the String/HashMap data.
        let _ = Box::from_raw((*inner).lazy_maps);
    }

    if let Some(free) = (*tp).tp_free {
        free(obj as *mut c_void);
    } else {
        ffi::PyObject_Free(obj as *mut c_void);
    }
    ffi::Py_DECREF(tp as *mut ffi::PyObject);
}

// --- tp_init (Python-side constructor compat) ---------------------------

unsafe extern "C" fn pyronova_request_init(
    self_: *mut ffi::PyObject,
    args: *mut ffi::PyObject,
    _kwargs: *mut ffi::PyObject,
) -> c_int {
    let inner = self_ as *mut RequestInner;
    let mut method: *mut ffi::PyObject = std::ptr::null_mut();
    let mut path: *mut ffi::PyObject = std::ptr::null_mut();
    let mut params: *mut ffi::PyObject = std::ptr::null_mut();
    let mut query: *mut ffi::PyObject = std::ptr::null_mut();
    let mut body_bytes: *mut ffi::PyObject = std::ptr::null_mut();
    let mut headers: *mut ffi::PyObject = std::ptr::null_mut();
    let mut client_ip: *mut ffi::PyObject = std::ptr::null_mut();
    let fmt = c"OOOOOOO";
    if ffi::PyArg_ParseTuple(
        args,
        fmt.as_ptr(),
        &mut method,
        &mut path,
        &mut params,
        &mut query,
        &mut body_bytes,
        &mut headers,
        &mut client_ip,
    ) == 0
    {
        return -1;
    }
    // Py_SETREF-style: INCREF new refs before DECREFing old.
    ffi::Py_INCREF(method);
    ffi::Py_INCREF(path);
    ffi::Py_INCREF(params);
    ffi::Py_INCREF(query);
    ffi::Py_INCREF(body_bytes);
    ffi::Py_INCREF(headers);
    ffi::Py_INCREF(client_ip);

    let old_method = (*inner).method;
    let old_path = (*inner).path;
    let old_params_cache = (*inner).params_cache;
    let old_query = (*inner).query;
    let old_body_cache = (*inner).body_bytes_cache;
    let old_headers_cache = (*inner).headers_cache;
    let old_client_ip = (*inner).client_ip;

    (*inner).method = method;
    (*inner).path = path;
    (*inner).params_cache = params;
    (*inner).query = query;
    (*inner).body_bytes_cache = body_bytes;
    (*inner).headers_cache = headers;
    (*inner).client_ip = client_ip;

    // Python-side init drops any previously-attached lazy_maps — the
    // caller is providing the values directly, no raw-data fallback.
    if !(*inner).lazy_maps.is_null() {
        let _ = Box::from_raw((*inner).lazy_maps);
        (*inner).lazy_maps = std::ptr::null_mut();
    }

    ffi::Py_XDECREF(old_method);
    ffi::Py_XDECREF(old_path);
    ffi::Py_XDECREF(old_params_cache);
    ffi::Py_XDECREF(old_query);
    ffi::Py_XDECREF(old_body_cache);
    ffi::Py_XDECREF(old_headers_cache);
    ffi::Py_XDECREF(old_client_ip);
    0
}

// --- Lazy getters -------------------------------------------------------
//
// `params` returns a list of (key, value) tuples matching the existing
// interface used by handlers. `headers` returns a dict.
//
// Returning a dict on second+ access returns the SAME PyDict pointer
// each time — user code that mutates `req.headers` sees those mutations
// persist across calls within the same request (intended). A fresh
// request always gets a fresh dict (raw_headers is cloned per request
// on the Rust side; the cache lives one request only).

unsafe fn build_str_dict_from_hashmap(m: &HashMap<String, String>) -> *mut ffi::PyObject {
    let dict = ffi::PyDict_New();
    if dict.is_null() {
        return std::ptr::null_mut();
    }
    for (k, v) in m {
        let kobj = ffi::PyUnicode_FromStringAndSize(
            k.as_ptr() as *const c_char,
            k.len() as ffi::Py_ssize_t,
        );
        if kobj.is_null() {
            ffi::Py_DECREF(dict);
            return std::ptr::null_mut();
        }
        let vobj = ffi::PyUnicode_FromStringAndSize(
            v.as_ptr() as *const c_char,
            v.len() as ffi::Py_ssize_t,
        );
        if vobj.is_null() {
            ffi::Py_DECREF(kobj);
            ffi::Py_DECREF(dict);
            return std::ptr::null_mut();
        }
        // PyDict_SetItem INCREFs both — we DECREF our owned refs.
        // Check return: -1 on failure leaves a Python exception set
        // and the dict in an inconsistent (partial) state. Pre-fix
        // the loop continued and returned a partial dict to the caller
        // who only NULL-checks — exception state leaks into the next
        // operation (arc request-type-1/-2).
        let rc = ffi::PyDict_SetItem(dict, kobj, vobj);
        ffi::Py_DECREF(kobj);
        ffi::Py_DECREF(vobj);
        if rc != 0 {
            ffi::Py_DECREF(dict);
            return std::ptr::null_mut();
        }
    }
    dict
}

unsafe fn build_str_dict_from_pairs(pairs: &[(String, String)]) -> *mut ffi::PyObject {
    let dict = ffi::PyDict_New();
    if dict.is_null() {
        return std::ptr::null_mut();
    }
    for (k, v) in pairs {
        let kobj = ffi::PyUnicode_FromStringAndSize(
            k.as_ptr() as *const c_char,
            k.len() as ffi::Py_ssize_t,
        );
        if kobj.is_null() {
            ffi::Py_DECREF(dict);
            return std::ptr::null_mut();
        }
        let vobj = ffi::PyUnicode_FromStringAndSize(
            v.as_ptr() as *const c_char,
            v.len() as ffi::Py_ssize_t,
        );
        if vobj.is_null() {
            ffi::Py_DECREF(kobj);
            ffi::Py_DECREF(dict);
            return std::ptr::null_mut();
        }
        ffi::PyDict_SetItem(dict, kobj, vobj);
        ffi::Py_DECREF(kobj);
        ffi::Py_DECREF(vobj);
    }
    dict
}

unsafe extern "C" fn get_params(
    obj: *mut ffi::PyObject,
    _closure: *mut c_void,
) -> *mut ffi::PyObject {
    let inner = obj as *mut RequestInner;
    if !(*inner).params_cache.is_null() {
        let p = (*inner).params_cache;
        ffi::Py_INCREF(p);
        return p;
    }
    let built = if !(*inner).lazy_maps.is_null() {
        build_str_dict_from_pairs(&(*(*inner).lazy_maps).params)
    } else {
        ffi::PyDict_New()
    };
    if built.is_null() {
        return std::ptr::null_mut();
    }
    // Cache owns 1 ref, return value gets +1.
    (*inner).params_cache = built;
    ffi::Py_INCREF(built);
    built
}

unsafe extern "C" fn get_headers(
    obj: *mut ffi::PyObject,
    _closure: *mut c_void,
) -> *mut ffi::PyObject {
    let inner = obj as *mut RequestInner;
    if !(*inner).headers_cache.is_null() {
        let p = (*inner).headers_cache;
        ffi::Py_INCREF(p);
        return p;
    }
    let built = if !(*inner).lazy_maps.is_null() {
        build_str_dict_from_hashmap(&(*(*inner).lazy_maps).headers)
    } else {
        ffi::PyDict_New()
    };
    if built.is_null() {
        return std::ptr::null_mut();
    }
    (*inner).headers_cache = built;
    ffi::Py_INCREF(built);
    built
}

// ---------------------------------------------------------------------------
// Buffer protocol — zero-copy body access via `memoryview(req)`
// ---------------------------------------------------------------------------
//
// Users of big-body workloads (AI agent 10 MB JSON, file upload, arena
// upload profile) care about avoiding the PyBytes_FromStringAndSize
// memcpy. The buffer protocol lets Python get a direct view into our
// Rust-owned `LazyMaps.body: Vec<u8>` without copying a byte:
//
//     view = memoryview(req)          # zero-copy view of body
//     arr  = np.frombuffer(view, dtype=np.uint8)  # numpy view, no copy
//     text = bytes(view)              # if user wants a copy, one call
//
// Safety story: `PyMemoryView_FromObject(req)` INCREFs `req` and stores
// it as the memoryview's backing object. As long as the memoryview is
// alive, `req` is alive → `lazy_maps` is alive → `Vec<u8>` is alive →
// the raw pointer we handed out is valid. Our tp_dealloc only fires
// after the memoryview releases its ref. No UAF.
//
// We support flat readonly 1D views only. Callers requesting writable
// get -1 (PyExc_BufferError). Callers on the tp_init-built request
// (where `lazy_maps` is null, e.g. the async_engine bridge) also get
// -1 — they should read `req.body_bytes` instead.

static BUFFER_FORMAT_UBYTE: &[u8] = b"B\0";

unsafe extern "C" fn request_getbuffer(
    obj: *mut ffi::PyObject,
    view: *mut ffi::Py_buffer,
    flags: c_int,
) -> c_int {
    if view.is_null() {
        ffi::PyErr_SetString(
            ffi::PyExc_BufferError,
            c"NULL view in request_getbuffer".as_ptr(),
        );
        return -1;
    }
    // Refuse writable requests — body is readonly once request is built.
    // PyBUF_WRITABLE bit is 0x0001 relative to PyBUF_SIMPLE; checked via
    // the `PyBUF_WRITABLE` mask.
    if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
        ffi::PyErr_SetString(
            ffi::PyExc_BufferError,
            c"pyronova _Request buffer is readonly".as_ptr(),
        );
        (*view).obj = std::ptr::null_mut();
        return -1;
    }

    let inner = obj as *mut RequestInner;
    if (*inner).lazy_maps.is_null() {
        // tp_init-constructed request has no Rust-owned body buffer.
        // Users on that path should access `req.body_bytes` directly.
        ffi::PyErr_SetString(
            ffi::PyExc_BufferError,
            c"_Request has no buffer (Python-constructed — use body_bytes instead)".as_ptr(),
        );
        (*view).obj = std::ptr::null_mut();
        return -1;
    }

    let body = &(*(*inner).lazy_maps).body;
    let len = body.len() as ffi::Py_ssize_t;

    // Fill view. `shape` and `strides` point INTO the Py_buffer itself
    // (the `len` and `itemsize` fields act as the single-element arrays
    // for a 1D contiguous buffer). CPython's PyBuffer_Release will NOT
    // free those — it assumes they're either heap-owned by the filler
    // or self-contained in the view. Pointing at struct fields is the
    // CPython convention for "stored by-value in the Py_buffer itself."
    (*view).buf = body.as_ptr() as *mut std::ffi::c_void;
    (*view).len = len;
    (*view).itemsize = 1;
    (*view).readonly = 1;
    (*view).ndim = 1;
    // Format: only report it if caller asked for PyBUF_FORMAT.
    (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
        BUFFER_FORMAT_UBYTE.as_ptr() as *mut c_char
    } else {
        std::ptr::null_mut()
    };
    // Shape / strides: required when caller asks for PyBUF_ND or
    // PyBUF_STRIDES. For a 1D contiguous bytes buffer we point at the
    // `len` / `itemsize` fields in the Py_buffer itself.
    if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
        (*view).shape = &mut (*view).len;
    } else {
        (*view).shape = std::ptr::null_mut();
    }
    if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
        (*view).strides = &mut (*view).itemsize;
    } else {
        (*view).strides = std::ptr::null_mut();
    }
    (*view).suboffsets = std::ptr::null_mut();
    (*view).internal = std::ptr::null_mut();

    // INCREF self — the view now owns a strong ref to keep `req` alive
    // for the duration of the view. CPython's PyBuffer_Release will
    // DECREF this automatically when the memoryview is dropped.
    ffi::Py_INCREF(obj);
    (*view).obj = obj;
    0
}

unsafe extern "C" fn request_releasebuffer(_obj: *mut ffi::PyObject, _view: *mut ffi::Py_buffer) {
    // No-op: we didn't allocate anything in getbuffer that needs
    // freeing. PyBuffer_Release handles DECREF(view.obj) itself — we
    // just have to not do it again. The Vec's backing storage is
    // owned by `lazy_maps`, freed in tp_dealloc of `req` — which can
    // only fire once all memoryviews of it have been released.
}

unsafe extern "C" fn get_body_bytes(
    obj: *mut ffi::PyObject,
    _closure: *mut c_void,
) -> *mut ffi::PyObject {
    let inner = obj as *mut RequestInner;
    if !(*inner).body_bytes_cache.is_null() {
        let p = (*inner).body_bytes_cache;
        ffi::Py_INCREF(p);
        return p;
    }
    // Build PyBytes from the raw body bytes stored in lazy_maps. Handlers
    // that never touch `req.body_bytes` (plaintext/JSON simple responders
    // that return a literal) skip this memcpy entirely — worthwhile when
    // the body is multi-megabyte (AI agent payloads, file uploads).
    let built = if !(*inner).lazy_maps.is_null() {
        let body = &(*(*inner).lazy_maps).body;
        ffi::PyBytes_FromStringAndSize(
            body.as_ptr() as *const c_char,
            body.len() as ffi::Py_ssize_t,
        )
    } else {
        // tp_init path without a body supplied: return empty bytes.
        ffi::PyBytes_FromStringAndSize(std::ptr::null(), 0)
    };
    if built.is_null() {
        return std::ptr::null_mut();
    }
    (*inner).body_bytes_cache = built;
    ffi::Py_INCREF(built);
    built
}

// Intentionally NO setters: `params` and `headers` stay READONLY, matching
// the semantics of the pre-lazy PyMemberDef path (flags = Py_READONLY).
// Handler code that needed to mutate should use a local dict. Tightening
// this also removes a concurrency-style footgun — sub-interp workers
// processing batched requests could otherwise end up with another task's
// cache slot values.

// --- PyMemberDef table (eager slots only) -------------------------------

const READONLY: c_int = 1; // Py_READONLY
const TYPE_OBJECT_EX: c_int = ffi::Py_T_OBJECT_EX;

static MEMBER_NAME_METHOD: &[u8] = b"method\0";
static MEMBER_NAME_PATH: &[u8] = b"path\0";
static MEMBER_NAME_QUERY: &[u8] = b"query\0";
static MEMBER_NAME_CLIENT_IP: &[u8] = b"client_ip\0";

fn members_table() -> [ffi::PyMemberDef; 5] {
    use std::mem::offset_of;
    [
        ffi::PyMemberDef {
            name: MEMBER_NAME_METHOD.as_ptr() as *const c_char,
            type_code: TYPE_OBJECT_EX,
            offset: offset_of!(RequestInner, method) as ffi::Py_ssize_t,
            flags: READONLY,
            doc: std::ptr::null(),
        },
        ffi::PyMemberDef {
            name: MEMBER_NAME_PATH.as_ptr() as *const c_char,
            type_code: TYPE_OBJECT_EX,
            offset: offset_of!(RequestInner, path) as ffi::Py_ssize_t,
            flags: READONLY,
            doc: std::ptr::null(),
        },
        ffi::PyMemberDef {
            name: MEMBER_NAME_QUERY.as_ptr() as *const c_char,
            type_code: TYPE_OBJECT_EX,
            offset: offset_of!(RequestInner, query) as ffi::Py_ssize_t,
            flags: READONLY,
            doc: std::ptr::null(),
        },
        ffi::PyMemberDef {
            name: MEMBER_NAME_CLIENT_IP.as_ptr() as *const c_char,
            type_code: TYPE_OBJECT_EX,
            offset: offset_of!(RequestInner, client_ip) as ffi::Py_ssize_t,
            flags: READONLY,
            doc: std::ptr::null(),
        },
        ffi::PyMemberDef {
            name: std::ptr::null(),
            type_code: 0,
            offset: 0,
            flags: 0,
            doc: std::ptr::null(),
        },
    ]
}

// --- PyGetSetDef table (lazy slots) -------------------------------------

static GETSET_NAME_PARAMS: &[u8] = b"params\0";
static GETSET_NAME_HEADERS: &[u8] = b"headers\0";
static GETSET_NAME_BODY_BYTES: &[u8] = b"body_bytes\0";

fn getset_table() -> [ffi::PyGetSetDef; 4] {
    [
        ffi::PyGetSetDef {
            name: GETSET_NAME_PARAMS.as_ptr() as *const c_char,
            get: Some(get_params),
            set: None,
            doc: std::ptr::null(),
            closure: std::ptr::null_mut(),
        },
        ffi::PyGetSetDef {
            name: GETSET_NAME_HEADERS.as_ptr() as *const c_char,
            get: Some(get_headers),
            set: None,
            doc: std::ptr::null(),
            closure: std::ptr::null_mut(),
        },
        ffi::PyGetSetDef {
            name: GETSET_NAME_BODY_BYTES.as_ptr() as *const c_char,
            get: Some(get_body_bytes),
            set: None,
            doc: std::ptr::null(),
            closure: std::ptr::null_mut(),
        },
        ffi::PyGetSetDef {
            name: std::ptr::null(),
            get: None,
            set: None,
            doc: std::ptr::null(),
            closure: std::ptr::null_mut(),
        },
    ]
}

// --- Type registration --------------------------------------------------

static TYPE_NAME: &[u8] = b"pyronova._Request\0";

pub(crate) unsafe fn register_type() -> Result<*mut ffi::PyObject, String> {
    let members = Box::leak(Box::new(members_table()));
    let getset = Box::leak(Box::new(getset_table()));

    let slots = Box::leak(Box::new([
        ffi::PyType_Slot {
            slot: ffi::Py_tp_dealloc,
            pfunc: pyronova_request_dealloc as *mut c_void,
        },
        ffi::PyType_Slot {
            slot: ffi::Py_tp_init,
            pfunc: pyronova_request_init as *mut c_void,
        },
        ffi::PyType_Slot {
            slot: ffi::Py_tp_members,
            pfunc: members.as_mut_ptr() as *mut c_void,
        },
        ffi::PyType_Slot {
            slot: ffi::Py_tp_getset,
            pfunc: getset.as_mut_ptr() as *mut c_void,
        },
        // Buffer protocol — `memoryview(req)` zero-copy into Rust body.
        ffi::PyType_Slot {
            slot: ffi::Py_bf_getbuffer,
            pfunc: request_getbuffer as *mut c_void,
        },
        ffi::PyType_Slot {
            slot: ffi::Py_bf_releasebuffer,
            pfunc: request_releasebuffer as *mut c_void,
        },
        ffi::PyType_Slot {
            slot: 0,
            pfunc: std::ptr::null_mut(),
        },
    ]));

    let spec = Box::leak(Box::new(ffi::PyType_Spec {
        name: TYPE_NAME.as_ptr() as *const c_char,
        basicsize: std::mem::size_of::<RequestInner>() as c_int,
        itemsize: 0,
        flags: ffi::Py_TPFLAGS_DEFAULT as u32,
        slots: slots.as_mut_ptr(),
    }));

    let ty = ffi::PyType_FromSpec(spec as *mut ffi::PyType_Spec);
    if ty.is_null() {
        return Err("PyType_FromSpec(_Request) failed".to_string());
    }
    Ok(ty)
}

// --- Allocation: eager path (tp_init-compatible) ------------------------

// --- Allocation: lazy fast path -----------------------------------------

/// Fast allocator for the Rust→Python handler bridge. Eager slots take
/// already-built PyObjects (cheap to build — single UTF-8 string / bytes
/// each). The two expensive dict slots stay null and are built on first
/// getter access.
///
/// `maps` ownership transfers into the instance; its Box is reclaimed
/// in `pyronova_request_dealloc`.
///
/// On alloc failure, the eager PyObjects are DECREF'd and `maps` is
/// dropped.
#[allow(clippy::too_many_arguments)]
pub(crate) unsafe fn alloc_and_init_lazy(
    type_obj: *mut ffi::PyObject,
    method: *mut ffi::PyObject,
    path: *mut ffi::PyObject,
    query: *mut ffi::PyObject,
    client_ip: *mut ffi::PyObject,
    maps: Box<LazyMaps>,
) -> Result<*mut ffi::PyObject, String> {
    #[cfg(feature = "leak_detect")]
    ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);

    let obj = ffi::PyType_GenericAlloc(type_obj as *mut ffi::PyTypeObject, 0);
    if obj.is_null() {
        ffi::Py_XDECREF(method);
        ffi::Py_XDECREF(path);
        ffi::Py_XDECREF(query);
        ffi::Py_XDECREF(client_ip);
        drop(maps);
        return Err("PyType_GenericAlloc(_Request) failed".to_string());
    }
    let inner = obj as *mut RequestInner;
    (*inner).method = method;
    (*inner).path = path;
    (*inner).query = query;
    (*inner).client_ip = client_ip;
    (*inner).params_cache = std::ptr::null_mut();
    (*inner).headers_cache = std::ptr::null_mut();
    (*inner).body_bytes_cache = std::ptr::null_mut();
    (*inner).lazy_maps = Box::into_raw(maps);
    Ok(obj)
}
