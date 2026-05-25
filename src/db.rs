//! Async Postgres support via sqlx::PgPool.
//!
//! One process, one pool. `PgPool.connect(dsn)` populates a global
//! `OnceLock<sqlx::PgPool>` + a dedicated tokio runtime that drives the
//! pool's futures. All handlers (GIL or sub-interpreter) share the same
//! connection pool — no per-interp duplication.
//!
//! v1 scope:
//!   * sync API only (`pool.fetch_one(sql, *params)` blocks the worker
//!     until the future completes). v2 adds async-awaitable wrappers.
//!   * supported param types: int, float, str, bool, bytes, None, dict
//!     (JSON), list (JSON). datetime / uuid / decimal → v2.
//!   * supported row types: same set, read back via PgValueRef type OIDs.
//!
//! Architecture notes:
//!   * Using a dedicated tokio runtime rather than the hyper server's
//!     runtime avoids cross-runtime coupling and keeps DB I/O off the
//!     accept loop. Worker threads (spawn_blocking in the GIL path or
//!     sub-interp pool threads) block on `rt.block_on(future)`; the
//!     pool-runtime drives the future to completion while the blocking
//!     thread waits.
//!   * `py.detach()` around block_on so the GIL is released during DB I/O.
//!     That's the whole point — other Python threads make progress while
//!     this one waits on the wire.
//!   * sqlx::PgPool is `Clone`-via-`Arc` internally, so the static
//!     reference works fine from arbitrary threads and sub-interpreters.

use std::sync::{Mutex, OnceLock};

use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use pyo3::BoundObject;
use sqlx::postgres::{PgPoolOptions, PgRow, PgValueRef};
use sqlx::{Column, Row, TypeInfo, ValueRef};
use tokio::runtime::Runtime;

/// Channel capacity for `PgCursor`. 8 rows in flight keeps memory
/// bounded while allowing enough prefetch to hide server-round-trip
/// latency on the common case of a fast Python consumer.
const CURSOR_CAPACITY: usize = 8;

static PG_POOL: OnceLock<sqlx::PgPool> = OnceLock::new();
static PG_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub(crate) fn runtime() -> &'static Runtime {
    // .expect() panic crosses FFI into Python → UB. The arc db-4 fix
    // recommendation was a signature change to PyResult, but `runtime()`
    // is called from many internal sites; keeping the &'static return.
    // Build failure during init is fatal anyway — converting to abort
    // via `eprintln + std::process::abort()` makes the failure mode
    // explicit and avoids the unwind-into-FFI UB. In practice tokio
    // multi_thread Builder::build only fails on syscall exhaustion
    // (threads, fds) at process startup, which is unrecoverable.
    PG_RUNTIME.get_or_init(|| match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("pyronova-db")
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("[pyronova-db] fatal: failed to build pg runtime: {e}");
            std::process::abort();
        }
    })
}

pub(crate) fn pool_ref() -> PyResult<&'static sqlx::PgPool> {
    PG_POOL.get().ok_or_else(|| {
        PyRuntimeError::new_err("PgPool not initialized — call PgPool.connect() first")
    })
}

// ---------------------------------------------------------------------------
// Python value → sqlx param
// ---------------------------------------------------------------------------

/// A single bound parameter extracted from Python, normalized into the
/// concrete Rust type sqlx expects. This sidesteps the need for trait-object
/// parameter binding (which sqlx doesn't directly support) — we build a
/// `sqlx::query::Query` and call the matching `bind::<T>()` per param.
#[derive(Clone)]
pub(crate) enum BoundParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// Dict or list → JSON-encoded value, bound as jsonb.
    Json(serde_json::Value),
}

pub(crate) fn extract_param(obj: &Bound<'_, PyAny>) -> PyResult<BoundParam> {
    if obj.is_none() {
        return Ok(BoundParam::Null);
    }
    // Order matters: PyBool is a subclass of PyInt; check bool first.
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(BoundParam::Bool(b.is_true()));
    }
    if let Ok(i) = obj.cast::<PyInt>() {
        return Ok(BoundParam::Int(i.extract::<i64>()?));
    }
    if let Ok(f) = obj.cast::<PyFloat>() {
        return Ok(BoundParam::Float(f.extract::<f64>()?));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok(BoundParam::Text(s.to_string()));
    }
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(BoundParam::Bytes(b.as_bytes().to_vec()));
    }
    // dict / list → JSON. Goes through pythonize → serde_json::Value,
    // bound as jsonb on the Postgres side.
    if obj.is_instance_of::<PyDict>() || obj.is_instance_of::<PyList>() {
        let val: serde_json::Value = pythonize::depythonize(obj).map_err(|e| {
            PyValueError::new_err(format!("JSON convert error on dict/list param: {e}"))
        })?;
        return Ok(BoundParam::Json(val));
    }
    Err(PyValueError::new_err(format!(
        "unsupported parameter type: {} (supported: int, float, str, bool, bytes, None, dict, list)",
        obj.get_type().name()?
    )))
}

pub(crate) fn bind_params_raw<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    params: &'q [BoundParam],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for p in params {
        query = match p {
            BoundParam::Null => query.bind(None::<i64>),
            BoundParam::Bool(v) => query.bind(*v),
            BoundParam::Int(v) => query.bind(*v),
            BoundParam::Float(v) => query.bind(*v),
            BoundParam::Text(v) => query.bind(v.as_str()),
            BoundParam::Bytes(v) => query.bind(v.as_slice()),
            BoundParam::Json(v) => query.bind(sqlx::types::Json(v)),
        };
    }
    query
}

// ---------------------------------------------------------------------------
// sqlx row → Python dict
// ---------------------------------------------------------------------------

/// Convert a single column cell into a Python object. Dispatches on the
/// Postgres type OID name (sqlx exposes it via `TypeInfo::name()`).
pub(crate) fn column_to_py(py: Python<'_>, value: PgValueRef<'_>) -> PyResult<Py<PyAny>> {
    if value.is_null() {
        return Ok(py.None());
    }

    // sqlx's `Type` trait info name returns strings like "INT4", "TEXT", "JSONB".
    // Decode each cell into our supported set, falling back to text for
    // unknown types.
    let type_name = value.type_info().name().to_ascii_uppercase();

    // Clone-decode: sqlx wants owned values for Decode. Decode always
    // consumes the value, so we build back by name.
    match type_name.as_str() {
        // Integers — Postgres int2/int4/int8.
        "INT2" => decode_scalar::<i16>(py, value),
        "INT4" | "INT" => decode_scalar::<i32>(py, value),
        "INT8" | "BIGINT" => decode_scalar::<i64>(py, value),
        // Floats — float4/float8 / numeric (numeric we'd need a decimal crate).
        "FLOAT4" => decode_scalar::<f32>(py, value),
        "FLOAT8" | "DOUBLE PRECISION" => decode_scalar::<f64>(py, value),
        // Bool.
        "BOOL" | "BOOLEAN" => decode_scalar::<bool>(py, value),
        // Text-like.
        "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" | "CITEXT" => {
            decode_scalar::<String>(py, value)
        }
        // Bytes.
        "BYTEA" => {
            let v: Vec<u8> = <Vec<u8> as sqlx::Decode<sqlx::Postgres>>::decode(value)
                .map_err(|e| PyRuntimeError::new_err(format!("decode bytea: {e}")))?;
            Ok(PyBytes::new(py, &v).into_any().unbind())
        }
        // JSON → dict/list via pythonize.
        "JSON" | "JSONB" => {
            let v: serde_json::Value = <sqlx::types::Json<serde_json::Value> as sqlx::Decode<
                sqlx::Postgres,
            >>::decode(value)
            .map(|j| j.0)
            .map_err(|e| PyRuntimeError::new_err(format!("decode json: {e}")))?;
            pythonize::pythonize(py, &v)
                .map(|b| b.unbind())
                .map_err(|e| PyRuntimeError::new_err(format!("pythonize json: {e}")))
        }
        // Unknown type — graceful fallback.
        //
        // Previously this branch forced `<String as Decode>::decode(value)`,
        // which assumes the column value is a UTF-8 byte sequence. That's
        // fine for text-format types but **fails hard** on binary-format
        // types like UUID (16-byte big-endian), TIMESTAMP (8-byte micros),
        // INET (tagged variable), etc. — sqlx returns a decode error and
        // the whole query bubbles up as PyRuntimeError, so any table with
        // a UUID column became un-queryable via Pyronova.
        //
        // New fallback: try String first (covers extensions like citext,
        // ltree, tsvector that really are text), and on failure return the
        // raw column bytes as `bytes`. Callers who need the typed value
        // should either `SELECT col::text` to coerce server-side or wait
        // for explicit UUID/TIMESTAMP decoders (tracked as follow-up;
        // needs the uuid + chrono sqlx features).
        _ => {
            // Snapshot the raw wire bytes first — `sqlx::Decode::decode`
            // consumes the PgValueRef by value, so we can't retry on
            // fallback. Grab the bytes before any decode attempt.
            let raw = value.as_bytes().ok().map(|b| b.to_vec());
            match <String as sqlx::Decode<sqlx::Postgres>>::decode(value) {
                Ok(s) => Ok(PyString::new(py, &s).into_any().unbind()),
                Err(_) => match raw {
                    Some(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
                    None => Err(PyRuntimeError::new_err(format!(
                        "unsupported column type {type_name}: no raw bytes available"
                    ))),
                },
            }
        }
    }
}

fn decode_scalar<'r, T>(py: Python<'r>, value: PgValueRef<'r>) -> PyResult<Py<PyAny>>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + pyo3::IntoPyObject<'r> + 'r,
    for<'py> <T as pyo3::IntoPyObject<'py>>::Error: std::fmt::Display,
{
    let v: T = <T as sqlx::Decode<sqlx::Postgres>>::decode(value)
        .map_err(|e| PyRuntimeError::new_err(format!("decode column: {e}")))?;
    // pyo3 0.28: IntoPyObject is the successor to ToPyObject. Returns Bound.
    let bound = v
        .into_pyobject(py)
        .map_err(|e| PyRuntimeError::new_err(format!("into_pyobject: {e}")))?;
    // Some IntoPyObject impls return Bound<PyAny>, others return a concrete
    // Bound type. into_any() normalizes.
    Ok(bound.into_any().unbind())
}

pub(crate) fn row_to_dict(py: Python<'_>, row: &PgRow) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for col in row.columns() {
        let name = col.name();
        let raw_val = row
            .try_get_raw(col.ordinal())
            .map_err(|e| PyRuntimeError::new_err(format!("get column {name}: {e}")))?;
        let py_val = column_to_py(py, raw_val)?;
        dict.set_item(name, py_val)?;
    }
    Ok(dict.unbind())
}

// ---------------------------------------------------------------------------
// Streaming cursor (fetch_iter)
// ---------------------------------------------------------------------------

/// Message sent from the cursor's background driver task to the
/// Python-facing iterator. `None` on the receiver == EOF (sender
/// dropped at end of stream or on connection loss).
enum CursorMsg {
    Row(PgRow),
    Err(String),
}

/// Streaming result-set iterator. Constructed by `PgPool.fetch_iter(sql, ...)`.
///
/// Memory contract: at any point in time at most `CURSOR_CAPACITY` rows
/// (8) are buffered on the Rust side and 1 PyDict is live on the Python
/// side (the one just yielded from `__next__`). This bounds memory at
/// O(1) — compare to `fetch_all`, which peaks at O(2N) (Rust Vec<PgRow>
/// AND Python list<dict> both alive while the conversion loop runs).
///
/// A dedicated task on the pool's tokio runtime drives the sqlx stream
/// and pushes each row through a bounded mpsc channel. Python's
/// blocking `__next__` reads from the receiver end with the GIL
/// released. Dropping the cursor before EOF closes the channel, which
/// aborts the driver task on its next send attempt — the sqlx
/// connection returns to the pool cleanly.
#[pyclass]
pub(crate) struct PgCursor {
    rx: Mutex<Option<tokio::sync::mpsc::Receiver<CursorMsg>>>,
}

#[pymethods]
impl PgCursor {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Pull the next row as a dict, or raise StopIteration at EOF,
    /// or raise RuntimeError on a database error.
    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        // .unwrap() on poisoned mutex panics across FFI → UB per PyO3.
        // Convert poisoning to PyRuntimeError so Python sees a normal
        // exception (arc finding db-3). Same pattern on the put-back
        // path below.
        let rx_opt = {
            self.rx
                .lock()
                .map_err(|e| PyRuntimeError::new_err(format!("cursor mutex poisoned: {e}")))?
                .take()
        };
        let Some(mut rx) = rx_opt else {
            return Err(PyStopIteration::new_err("cursor exhausted"));
        };
        let msg = py.detach(|| rx.blocking_recv());
        match msg {
            Some(CursorMsg::Row(row)) => {
                *self
                    .rx
                    .lock()
                    .map_err(|e| PyRuntimeError::new_err(format!("cursor mutex poisoned: {e}")))? = Some(rx);
                row_to_dict(py, &row)
            }
            Some(CursorMsg::Err(e)) => Err(PyRuntimeError::new_err(e)),
            None => Err(PyStopIteration::new_err(py.None())),
        }
    }

    /// Eager-drain helper: materialize every remaining row as a list
    /// of dicts. Same API shape as `fetch_all` — useful for tests that
    /// want to verify a cursor's contents without writing a loop.
    fn to_list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        loop {
            match self.__next__(py) {
                Ok(row) => list.append(row)?,
                Err(e) if e.is_instance_of::<PyStopIteration>(py) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(list)
    }
}

// ---------------------------------------------------------------------------
// Python-visible PgPool
// ---------------------------------------------------------------------------

#[pyclass(frozen)]
pub(crate) struct PgPool;

#[pymethods]
impl PgPool {
    /// Initialize the global pool. Idempotent — calling `.connect()` again
    /// after the pool exists returns a handle to the existing pool without
    /// re-opening connections. The DSN from the first call wins; subsequent
    /// calls with different DSNs are silently ignored (document this in
    /// your app setup).
    #[classmethod]
    #[pyo3(signature = (dsn, max_connections = 10, acquire_timeout_secs = 30))]
    fn connect(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        dsn: &str,
        max_connections: u32,
        acquire_timeout_secs: u64,
    ) -> PyResult<Self> {
        if PG_POOL.get().is_some() {
            return Ok(PgPool);
        }

        let rt = runtime();
        let dsn_owned = dsn.to_string();
        let pool = py
            .detach(|| {
                rt.block_on(async move {
                    PgPoolOptions::new()
                        .max_connections(max_connections)
                        .acquire_timeout(std::time::Duration::from_secs(acquire_timeout_secs))
                        .connect(&dsn_owned)
                        .await
                })
            })
            .map_err(|e| PyConnectionError::new_err(format!("PgPool connect: {e}")))?;

        let _ = PG_POOL.set(pool); // race-safe: first writer wins
        Ok(PgPool)
    }

    /// Fetch exactly one row or None. Extra rows are ignored (no error).
    #[pyo3(signature = (sql, *params))]
    fn fetch_one(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let pool = pool_ref()?;
        let rt = runtime();
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        let row_opt = py
            .detach(|| {
                rt.block_on(async {
                    let q = sqlx::query(sql);
                    bind_params_raw(q, &bound).fetch_optional(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("fetch_one: {e}")))?;

        match row_opt {
            Some(row) => Ok(Some(row_to_dict(py, &row)?)),
            None => Ok(None),
        }
    }

    /// PyronovaStream rows one at a time via an iterator cursor — keeps memory
    /// O(1) instead of fetch_all's O(2N) peak (Rust Vec<PgRow> plus
    /// Python list<dict> both alive simultaneously during conversion).
    ///
    /// Use for large result sets (data exports, full-table scans,
    /// log paging). For typical API queries returning ≤ 50 rows,
    /// `fetch_all` is simpler and the O(2N) peak is inconsequential.
    ///
    /// Python side:
    ///
    /// ```python
    /// for row in pool.fetch_iter("SELECT * FROM users"):
    ///     process(row)
    /// # Cursor drops on loop exit → Rust driver task aborts → PG
    /// # connection returns to the pool even if we broke early.
    /// ```
    #[pyo3(signature = (sql, *params))]
    fn fetch_iter(&self, py: Python<'_>, sql: &str, params: Vec<Py<PyAny>>) -> PyResult<PgCursor> {
        let pool = pool_ref()?;
        let rt = runtime();
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        let (tx, rx) = tokio::sync::mpsc::channel::<CursorMsg>(CURSOR_CAPACITY);
        let sql_owned = sql.to_string();

        // Spawn the driver on the pool's tokio runtime. The `async move`
        // owns `sql_owned` and `bound`; sqlx's Query borrows from them
        // for the lifetime of the async block, which is fine because
        // both live as long as the block runs. `pool` is `&'static`.
        rt.spawn(async move {
            use futures_util::StreamExt;
            let q = sqlx::query(&sql_owned);
            let mut stream = bind_params_raw(q, &bound).fetch(pool);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(row) => {
                        if tx.send(CursorMsg::Row(row)).await.is_err() {
                            // Receiver dropped (Python broke from the
                            // loop or cursor went out of scope). Drop
                            // the sqlx stream — connection returns to
                            // the pool.
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(CursorMsg::Err(e.to_string())).await;
                        return;
                    }
                }
            }
            // Normal EOF: tx drops at scope end → Python side sees
            // channel close → StopIteration.
        });

        Ok(PgCursor {
            rx: Mutex::new(Some(rx)),
        })
    }

    /// Fetch all matching rows into a list of dicts.
    #[pyo3(signature = (sql, *params))]
    fn fetch_all(&self, py: Python<'_>, sql: &str, params: Vec<Py<PyAny>>) -> PyResult<Py<PyList>> {
        let pool = pool_ref()?;
        let rt = runtime();
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        let rows = py
            .detach(|| {
                rt.block_on(async {
                    let q = sqlx::query(sql);
                    bind_params_raw(q, &bound).fetch_all(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("fetch_all: {e}")))?;

        let py_list = PyList::empty(py);
        for row in &rows {
            py_list.append(row_to_dict(py, row)?)?;
        }
        Ok(py_list.unbind())
    }

    /// Fetch a single column of a single row. Raises if no rows; returns
    /// None for SQL NULL. Useful for `SELECT count(*) FROM ...`.
    #[pyo3(signature = (sql, *params))]
    fn fetch_scalar(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let pool = pool_ref()?;
        let rt = runtime();
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        let row = py
            .detach(|| {
                rt.block_on(async {
                    let q = sqlx::query(sql);
                    bind_params_raw(q, &bound).fetch_one(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("fetch_scalar: {e}")))?;

        let raw = row
            .try_get_raw(0)
            .map_err(|e| PyRuntimeError::new_err(format!("fetch_scalar col 0: {e}")))?;
        column_to_py(py, raw)
    }

    // ----------------------------------------------------------------
    // Async variants — return Python awaitables.
    //
    // Each `*_async` method kicks off the sqlx future on the pool's
    // dedicated tokio runtime and hands back a Python awaitable via
    // `pyo3_async_runtimes::tokio::future_into_py`. From an `async def`
    // handler the caller writes `await pool.fetch_one_async(sql, ...)`
    // exactly like any asyncio coroutine.
    //
    // Why separate from the sync methods: a single method that magically
    // does the right thing based on caller context would mean "returns
    // dict or coroutine depending on where you call it" — confusing and
    // brittle. Explicit `_async` suffix makes the cost model obvious.
    //
    // The tokio runtime that pyo3-async-runtimes uses is globally
    // configured on first use; we don't pass our `runtime()` handle to
    // it because that would bind two runtimes together. Instead pool
    // futures are spawned via sqlx::query which internally drives
    // itself on the runtime that's `current` at spawn time.
    // ----------------------------------------------------------------

    #[pyo3(signature = (sql, *params))]
    fn fetch_one_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let q = sqlx::query(&sql);
            let row_opt = bind_params_raw(q, &bound)
                .fetch_optional(pool)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("fetch_one_async: {e}")))?;

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                match row_opt {
                    Some(row) => Ok(row_to_dict(py, &row)?.into_any()),
                    None => Ok(py.None()),
                }
            })
        })
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_all_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let q = sqlx::query(&sql);
            let rows = bind_params_raw(q, &bound)
                .fetch_all(pool)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("fetch_all_async: {e}")))?;

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let py_list = PyList::empty(py);
                for row in &rows {
                    py_list.append(row_to_dict(py, row)?)?;
                }
                Ok(py_list.into_any().unbind())
            })
        })
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_scalar_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let q = sqlx::query(&sql);
            let row = bind_params_raw(q, &bound)
                .fetch_one(pool)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("fetch_scalar_async: {e}")))?;

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let raw = row.try_get_raw(0).map_err(|e| {
                    PyRuntimeError::new_err(format!("fetch_scalar_async col 0: {e}"))
                })?;
                column_to_py(py, raw)
            })
        })
    }

    #[pyo3(signature = (sql, *params))]
    fn execute_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let q = sqlx::query(&sql);
            let result = bind_params_raw(q, &bound)
                .execute(pool)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("execute_async: {e}")))?;
            Ok(result.rows_affected())
        })
    }

    /// Execute a statement that doesn't return rows. Returns the number of
    /// rows affected.
    #[pyo3(signature = (sql, *params))]
    fn execute(&self, py: Python<'_>, sql: &str, params: Vec<Py<PyAny>>) -> PyResult<u64> {
        let pool = pool_ref()?;
        let rt = runtime();
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        let result = py
            .detach(|| {
                rt.block_on(async {
                    let q = sqlx::query(sql);
                    bind_params_raw(q, &bound).execute(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("execute: {e}")))?;
        Ok(result.rows_affected())
    }
}
