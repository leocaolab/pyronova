//! Sub-interpreter DB bridge (C-FFI).
//!
//! The `pyronova.engine` #[pymodule] does not carry a
//! `Py_mod_multiple_interpreters` slot, so CPython 3.12+ refuses to load
//! it in sub-interpreters. That's why `_bootstrap.py` hand-rolls a mock
//! `pyronova.engine` for the sub-interp side. The mock has no real
//! `PgPool`, which is the reason DB-backed routes have historically been
//! pinned to `gil=True` (main-interp only).
//!
//! This module bridges that gap. Each sub-interpreter receives four C
//! functions in its globals (`_pyronova_db_fetch_all` and friends) that
//! the bootstrap's `_PgPool` proxy forwards to. The functions:
//!
//! 1. Run under the sub-interp's GIL at entry (CPython guarantees this
//!    for `PyCFunction` callbacks).
//! 2. Parse a `(sql, params_tuple)` argument tuple using the existing
//!    `extract_param` helper from `db.rs`.
//! 3. Release the GIL via `py.detach()` and drive the query on the
//!    shared tokio runtime + shared `sqlx::PgPool` (both live in
//!    process-global `OnceLock`s in `db.rs`).
//! 4. Reacquire the GIL, materialize the result as native Python objects
//!    in the sub-interp, and return.
//!
//! Key invariant: no `Py<T>` crosses the interpreter boundary. Every
//! Python object we touch is created in, or borrowed from, the
//! sub-interp whose C-FFI call we're servicing. The pool itself is
//! `Arc<sqlx::Pool<Postgres>>` in Rust — it has no Python-object
//! identity and therefore no interpreter affinity.
//!
//! Concurrency: N sub-interp workers can call these functions
//! simultaneously. Each releases its GIL before blocking on sqlx;
//! sqlx's pool handles the fan-in. Parallelism ceiling moves from 1
//! (main-interp GIL) to `min(sub_interp_workers, max_connections)`.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};

use crate::db::{bind_params_raw, column_to_py, extract_param, pool_ref, row_to_dict, runtime};

/// Run a `Send + 'static` future on the dedicated DB runtime and
/// synchronously wait for the result on the calling thread.
///
/// Motivation: sub-interpreter workers under TPC run on a tokio
/// `current_thread` runtime (one per TPC thread). Calling
/// `rt.block_on(...)` from inside another tokio runtime context
/// panics with "Cannot start a runtime from within a runtime", which
/// is the whole reason DB-backed sub-interp routes used to be pinned
/// to `gil=True` (main-interp bridge, which is on a plain std::thread).
///
/// `rt.spawn(...)` has no such check — it just queues a task onto the
/// target runtime's worker pool. Pairing spawn with a sync mpsc
/// channel gives us "block the caller while the DB future runs on
/// the DB runtime", with no nested-runtime hazard.
///
/// The caller is responsible for releasing the GIL (via `py.detach`)
/// around this call so peer sub-interpreters can make progress while
/// we wait.
fn run_on_db_rt<F, T>(fut: F) -> Result<T, &'static str>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let rt = runtime();
    let (tx, rx) = std::sync::mpsc::sync_channel::<T>(1);
    rt.spawn(async move {
        // `let _ = ...` so a panic in fut.await doesn't double-panic the
        // runtime. If the spawned task panics the sender drops unsent,
        // the recv below sees Err, and we surface it as a Python
        // exception (NOT .expect() — this is called inside an extern "C"
        // cfunc and Rust 1.81+ aborts the process on an unwind across
        // the FFI boundary).
        let _ = tx.send(fut.await);
    });
    rx.recv()
        .map_err(|_| "pyronova-db runtime task panicked (spawned future did not complete)")
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

/// Extract `(sql, params_tuple)` from the argument tuple.
///
/// CPython passes the function args as a single `PyTuple`. We accept
/// two positional args: a string SQL statement and a tuple/list of
/// parameters already normalized to Python builtin types.
fn unpack_args<'py>(
    py: Python<'py>,
    args: *mut ffi::PyObject,
) -> PyResult<(String, Vec<crate::db::BoundParam>)> {
    // SAFETY: CPython calls us with a valid tuple. `from_borrowed_ptr` is
    // correct here because the tuple's refcount is managed by the caller —
    // we borrow for the duration of this function only.
    let args_tuple = unsafe { Bound::from_borrowed_ptr(py, args) };
    let tup = args_tuple
        .cast::<PyTuple>()
        .map_err(|_| PyValueError::new_err("db bridge: expected positional argument tuple"))?;
    if tup.len() < 2 {
        return Err(PyValueError::new_err(
            "db bridge: expected (sql: str, params: tuple|list)",
        ));
    }

    let sql_obj = tup.get_item(0)?;
    let sql: String = sql_obj
        .cast::<PyString>()
        .map_err(|_| PyValueError::new_err("db bridge: sql must be str"))?
        .to_cow()?
        .into_owned();

    let params_obj = tup.get_item(1)?;
    // Accept both tuple and list of params; normalize by iterating.
    let mut bound = Vec::with_capacity(params_obj.len().unwrap_or(0));
    for item in params_obj.try_iter()? {
        bound.push(extract_param(&item?)?);
    }

    Ok((sql, bound))
}

/// Standard entry-point shape for every bridge cfunc: with_gil, unpack,
/// run query, convert result. The `run` closure does the sqlx work +
/// Python materialization in one step (under the GIL again after the
/// detach block completes).
fn dispatch<F>(args: *mut ffi::PyObject, run: F) -> *mut ffi::PyObject
where
    F: for<'py> FnOnce(Python<'py>, &str, &[crate::db::BoundParam]) -> PyResult<Py<PyAny>>,
{
    Python::attach(|py| {
        let result: PyResult<Py<PyAny>> = (|| {
            let (sql, params) = unpack_args(py, args)?;
            run(py, &sql, &params)
        })();
        match result {
            Ok(obj) => obj.into_ptr(),
            Err(e) => {
                e.restore(py);
                std::ptr::null_mut()
            }
        }
    })
}

// ---------------------------------------------------------------------------
// _pyronova_db_fetch_all(sql, params) -> list[dict]
// ---------------------------------------------------------------------------

pub(crate) unsafe extern "C" fn pyronova_db_fetch_all_cfunc(
    _self: *mut ffi::PyObject,
    args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    dispatch(args, |py, sql, params| {
        let pool = pool_ref()?;
        // Clone onto owned storage: the future we spawn onto the DB
        // runtime must be 'static, so borrowed `sql` / `params` from
        // the Python argument unpacker can't be captured directly.
        // One clone per query is a rounding error next to the PG
        // round-trip.
        let sql = sql.to_string();
        let params = params.to_vec();
        let rows = py
            .detach(|| {
                run_on_db_rt(async move {
                    let q = sqlx::query(&sql);
                    bind_params_raw(q, &params).fetch_all(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("db fetch_all runtime: {e}")))?
            .map_err(|e| PyRuntimeError::new_err(format!("db fetch_all: {e}")))?;

        let list = PyList::empty(py);
        for row in &rows {
            let d = row_to_dict(py, row)?;
            list.append(d)?;
        }
        Ok(list.into_any().unbind())
    })
}

// ---------------------------------------------------------------------------
// _pyronova_db_fetch_one(sql, params) -> dict | None
// ---------------------------------------------------------------------------

pub(crate) unsafe extern "C" fn pyronova_db_fetch_one_cfunc(
    _self: *mut ffi::PyObject,
    args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    dispatch(args, |py, sql, params| {
        let pool = pool_ref()?;
        let sql = sql.to_string();
        let params = params.to_vec();
        let row_opt = py
            .detach(|| {
                run_on_db_rt(async move {
                    let q = sqlx::query(&sql);
                    bind_params_raw(q, &params).fetch_optional(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("db fetch_one runtime: {e}")))?
            .map_err(|e| PyRuntimeError::new_err(format!("db fetch_one: {e}")))?;

        match row_opt {
            Some(row) => Ok(row_to_dict(py, &row)?.into_any()),
            None => Ok(py.None()),
        }
    })
}

// ---------------------------------------------------------------------------
// _pyronova_db_fetch_scalar(sql, params) -> value | None
// ---------------------------------------------------------------------------

pub(crate) unsafe extern "C" fn pyronova_db_fetch_scalar_cfunc(
    _self: *mut ffi::PyObject,
    args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    dispatch(args, |py, sql, params| {
        use sqlx::Row;
        let pool = pool_ref()?;
        let sql = sql.to_string();
        let params = params.to_vec();
        let row_opt = py
            .detach(|| {
                run_on_db_rt(async move {
                    let q = sqlx::query(&sql);
                    bind_params_raw(q, &params).fetch_optional(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("db fetch_scalar runtime: {e}")))?
            .map_err(|e| PyRuntimeError::new_err(format!("db fetch_scalar: {e}")))?;

        let Some(row) = row_opt else {
            return Ok(py.None());
        };
        let raw = row
            .try_get_raw(0)
            .map_err(|e| PyRuntimeError::new_err(format!("db fetch_scalar col 0: {e}")))?;
        column_to_py(py, raw)
    })
}

// ---------------------------------------------------------------------------
// _pyronova_db_execute(sql, params) -> int  (rows affected)
// ---------------------------------------------------------------------------

pub(crate) unsafe extern "C" fn pyronova_db_execute_cfunc(
    _self: *mut ffi::PyObject,
    args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    dispatch(args, |py, sql, params| {
        let pool = pool_ref()?;
        let sql = sql.to_string();
        let params = params.to_vec();
        let result = py
            .detach(|| {
                run_on_db_rt(async move {
                    let q = sqlx::query(&sql);
                    bind_params_raw(q, &params).execute(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("db execute runtime: {e}")))?
            .map_err(|e| PyRuntimeError::new_err(format!("db execute: {e}")))?;
        let affected: u64 = result.rows_affected();
        Ok(affected
            .into_pyobject(py)
            .map_err(|e| PyRuntimeError::new_err(format!("int conv: {e}")))?
            .into_any()
            .unbind())
    })
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register all four DB C-FFI functions in a sub-interpreter's globals
/// dict. Called from `interp.rs` during worker bootstrap, once per
/// sub-interpreter (sync + async pools both use this).
///
/// SAFETY: caller must hold the target sub-interpreter's GIL and
/// `globals` must be a valid `PyDict *` owned by that interpreter.
pub(crate) unsafe fn register_db_bridge(globals: *mut ffi::PyObject) {
    unsafe fn register_one(
        globals: *mut ffi::PyObject,
        name: &'static std::ffi::CStr,
        cfunc: unsafe extern "C" fn(*mut ffi::PyObject, *mut ffi::PyObject) -> *mut ffi::PyObject,
    ) {
        #[allow(clippy::missing_transmute_annotations)]
        let def = Box::into_raw(Box::new(ffi::PyMethodDef {
            ml_name: name.as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                PyCFunction: std::mem::transmute(cfunc as *const ()),
            },
            ml_flags: ffi::METH_VARARGS,
            ml_doc: std::ptr::null(),
        }));
        let func = ffi::PyCFunction_NewEx(def, std::ptr::null_mut(), std::ptr::null_mut());
        if !func.is_null() {
            // PyDict_SetItemString returns -1 on failure (e.g. allocation
            // failure growing the dict). Unchecked, the bridge function would
            // be silently missing from globals → NameError at request time
            // with the root cause hidden — the same failure mode the
            // PyCFunction_NewEx null branch below guards against. Clear the
            // pending error and log so it stays diagnosable.
            if ffi::PyDict_SetItemString(globals, name.as_ptr(), func) == -1 {
                if !ffi::PyErr_Occurred().is_null() {
                    ffi::PyErr_Clear();
                }
                let name_str = std::ffi::CStr::from_ptr(name.as_ptr())
                    .to_string_lossy();
                tracing::error!(
                    target: "pyronova::app",
                    function = %name_str,
                    "PyDict_SetItemString failed while registering DB \
                     bridge function — sub-interp will see NameError at \
                     runtime when handlers try to call it"
                );
            }
            ffi::Py_DECREF(func);
        } else {
            // PyCFunction_NewEx returning null = alloc failure or
            // invalid PyMethodDef. The success path intentionally leaks
            // `def` (CPython requires the PyMethodDef to outlive the
            // function object), but here no function object took
            // ownership, so reclaim the box to avoid leaking ~80 bytes
            // per failed registration.
            let _ = Box::from_raw(def);
            // Pre-fix this was silent → sub-interp missing the DB bridge
            // function → NameError at request time, root cause hidden
            // (arc finding bridge-db-bridge-2). Clear any pending error
            // so subsequent registrations don't inherit it; log so the
            // missing-function failure is diagnosable.
            if !ffi::PyErr_Occurred().is_null() {
                ffi::PyErr_Clear();
            }
            let name_str = std::ffi::CStr::from_ptr(name.as_ptr())
                .to_string_lossy();
            tracing::error!(
                target: "pyronova::app",
                function = %name_str,
                "PyCFunction_NewEx returned null while registering DB \
                 bridge function — sub-interp will see NameError at \
                 runtime when handlers try to call it"
            );
        }
    }

    register_one(
        globals,
        c"_pyronova_db_fetch_all",
        pyronova_db_fetch_all_cfunc,
    );
    register_one(
        globals,
        c"_pyronova_db_fetch_one",
        pyronova_db_fetch_one_cfunc,
    );
    register_one(
        globals,
        c"_pyronova_db_fetch_scalar",
        pyronova_db_fetch_scalar_cfunc,
    );
    register_one(globals, c"_pyronova_db_execute", pyronova_db_execute_cfunc);
}
