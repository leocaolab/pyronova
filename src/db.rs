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
//!     accept loop. Callers hand the future to that runtime with
//!     `run_on_db_rt` and wait on a std channel. They never call
//!     `Runtime::block_on`, which panics on a thread already inside a
//!     Tokio context (a TPC worker runs its handlers inside one).
//!   * `py.detach()` around the wait so the GIL is released during DB I/O.
//!     That's the whole point — other Python threads make progress while
//!     this one waits on the wire.
//!   * sqlx::PgPool is `Clone`-via-`Arc` internally, so the static
//!     reference works fine from arbitrary threads and sub-interpreters.

use std::sync::{Mutex, OnceLock};

use pyo3::exceptions::{
    PyConnectionError, PyNotImplementedError, PyRuntimeError, PyStopIteration, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use pyo3::BoundObject;
use sqlx::postgres::{PgPoolOptions, PgRow, PgValueRef};
use sqlx::{Column, Row, TypeInfo, ValueRef};
use tokio::runtime::Runtime;

use crate::run_context::{attach_to, main_attach, Interp};

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
    PG_RUNTIME.get_or_init(|| {
        match tokio::runtime::Builder::new_multi_thread()
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
        }
    })
}

pub(crate) fn pool_ref() -> PyResult<&'static sqlx::PgPool> {
    PG_POOL.get().ok_or_else(|| {
        PyRuntimeError::new_err("PgPool not initialized — call PgPool.connect() first")
    })
}

/// Run a `Send + 'static` future on the DB runtime and wait for its result on the
/// calling thread.
///
/// `Runtime::block_on` panics with "Cannot start a runtime from within a runtime" when
/// the calling thread is already inside a Tokio context, which is where TPC workers run
/// handlers (a `current_thread` runtime + `LocalSet`). `spawn` has no such check, and
/// the caller waits on a std channel instead. Callers release the GIL (`py.detach`)
/// around this call.
pub(crate) fn run_on_db_rt<F, T>(fut: F) -> Result<T, &'static str>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel::<T>(1);
    runtime().spawn(async move {
        // If the future panics, the sender drops unsent and `recv` below reports it.
        let _ = tx.send(fut.await);
    });
    rx.recv()
        .map_err(|_| "pyronova-db runtime task panicked (spawned future did not complete)")
}

/// Wait for the next messages on a cursor channel without `blocking_recv`.
///
/// `tokio::sync::mpsc::Receiver::blocking_recv` panics inside a Tokio context (a TPC
/// worker). The receive runs on the DB runtime instead, and this thread waits on a std
/// channel (`run_on_db_rt`). One round trip returns the first message plus whatever
/// else is already buffered, up to `max`. An empty batch means the sender is gone (EOF).
/// The receiver is moved into the task and handed back with the batch.
pub(crate) fn recv_batch<T: Send + 'static>(
    mut rx: tokio::sync::mpsc::Receiver<T>,
    max: usize,
) -> Result<(tokio::sync::mpsc::Receiver<T>, Vec<T>), &'static str> {
    run_on_db_rt(async move {
        let mut batch = Vec::new();
        if let Some(first) = rx.recv().await {
            batch.push(first);
            while batch.len() < max {
                match rx.try_recv() {
                    Ok(m) => batch.push(m),
                    Err(_) => break,
                }
            }
        }
        (rx, batch)
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
            // Keep the Err from `as_bytes` instead of discarding it with
            // `.ok()` — if both the String decode and the raw-byte snapshot
            // fail, the final error should report *why* the bytes were
            // unavailable rather than a generic placeholder.
            let raw: Result<Vec<u8>, String> = value
                .as_bytes()
                .map(|b| b.to_vec())
                .map_err(|e| e.to_string());
            match <String as sqlx::Decode<sqlx::Postgres>>::decode(value) {
                Ok(s) => Ok(PyString::new(py, &s).into_any().unbind()),
                Err(_) => match raw {
                    Ok(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
                    Err(raw_err) => Err(PyRuntimeError::new_err(format!(
                        "unsupported column type {type_name}: failed to read raw bytes: {raw_err}"
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
/// Memory contract: at most `CURSOR_CAPACITY` rows (8) are buffered in the channel
/// plus at most `CURSOR_CAPACITY` in `buf`, and 1 PyDict is live on the Python side
/// (the one just yielded from `__next__`). This bounds memory at O(1), compared with
/// `fetch_all`, which peaks at O(2N) (Rust Vec<PgRow> AND Python list<dict> both alive
/// while the conversion loop runs).
///
/// A dedicated task on the pool's tokio runtime drives the sqlx stream and pushes each
/// row through a bounded async channel, so a slow consumer applies backpressure without
/// blocking a runtime thread. `__next__` takes rows from that channel with `recv_batch`
/// (GIL released), which works from any thread, including one inside a Tokio context.
/// Dropping the cursor before EOF closes the channel, which stops the driver task on
/// its next send; the sqlx connection returns to the pool cleanly.
#[pyclass(module = "pyronova.engine")]
pub(crate) struct PgCursor {
    state: Mutex<CursorState>,
}

struct CursorState {
    /// `None` once the driver has finished (EOF or error delivered).
    rx: Option<tokio::sync::mpsc::Receiver<CursorMsg>>,
    buf: std::collections::VecDeque<CursorMsg>,
}

#[pymethods]
impl PgCursor {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Pull the next row as a dict, or raise StopIteration at EOF,
    /// or raise RuntimeError on a database error.
    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        // A poisoned mutex becomes a PyRuntimeError, never a panic across FFI
        // (arc finding db-3).
        let lock = || {
            self.state
                .lock()
                .map_err(|e| PyRuntimeError::new_err(format!("cursor mutex poisoned: {e}")))
        };
        // Take the receiver out and release the lock before waiting. Waiting with the
        // lock held while the GIL is released deadlocks against a second thread that
        // holds the GIL and wants the lock.
        let rx = {
            let mut st = lock()?;
            match st.buf.pop_front() {
                Some(m) => return deliver(&self.state, py, Some(m)),
                None => st.rx.take(),
            }
        };
        let msg = match rx {
            None => None,
            Some(rx) => {
                let (rx, batch) = py
                    .detach(|| recv_batch(rx, CURSOR_CAPACITY))
                    .map_err(PyRuntimeError::new_err)?;
                let mut st = lock()?;
                if !batch.is_empty() {
                    st.rx = Some(rx);
                }
                st.buf.extend(batch);
                st.buf.pop_front()
            }
        };
        deliver(&self.state, py, msg)
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

#[pyclass(frozen, module = "pyronova.engine")]
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

        let dsn_owned = dsn.to_string();
        let pool = py
            .detach(|| {
                run_on_db_rt(async move {
                    PgPoolOptions::new()
                        .max_connections(max_connections)
                        .acquire_timeout(std::time::Duration::from_secs(acquire_timeout_secs))
                        .connect(&dsn_owned)
                        .await
                })
            })
            .map_err(|e| PyConnectionError::new_err(format!("PgPool connect runtime: {e}")))?
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
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        // The future runs on the DB runtime, so it must own its inputs.
        let sql = sql.to_string();
        let row_opt = py
            .detach(|| {
                run_on_db_rt(async move {
                    let q = sqlx::query(&sql);
                    bind_params_raw(q, &bound).fetch_optional(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("fetch_one runtime: {e}")))?
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
            state: Mutex::new(CursorState {
                rx: Some(rx),
                buf: std::collections::VecDeque::new(),
            }),
        })
    }

    /// Fetch all matching rows into a list of dicts.
    #[pyo3(signature = (sql, *params))]
    fn fetch_all(&self, py: Python<'_>, sql: &str, params: Vec<Py<PyAny>>) -> PyResult<Py<PyList>> {
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        // The future runs on the DB runtime, so it must own its inputs.
        let sql = sql.to_string();
        let rows = py
            .detach(|| {
                run_on_db_rt(async move {
                    let q = sqlx::query(&sql);
                    bind_params_raw(q, &bound).fetch_all(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("fetch_all runtime: {e}")))?
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
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        // The future runs on the DB runtime, so it must own its inputs.
        let sql = sql.to_string();
        let row = py
            .detach(|| {
                run_on_db_rt(async move {
                    let q = sqlx::query(&sql);
                    bind_params_raw(q, &bound).fetch_one(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("fetch_scalar runtime: {e}")))?
            .map_err(|e| PyRuntimeError::new_err(format!("fetch_scalar: {e}")))?;

        let raw = row
            .try_get_raw(0)
            .map_err(|e| PyRuntimeError::new_err(format!("fetch_scalar col 0: {e}")))?;
        column_to_py(py, raw)
    }

    // ----------------------------------------------------------------
    // Async variants — return Python awaitables.
    //
    // `await pool.fetch_one_async(sql, ...)` from an `async def` handler. The query
    // runs on the `pyronova-db` runtime and its result reaches the caller's asyncio
    // loop through `await_on_loop` (see there).
    //
    // Why separate from the sync methods: a single method that returns a dict or a
    // coroutine depending on where it is called is confusing and brittle. The
    // explicit `_async` suffix makes the cost model obvious.
    //
    // Main interpreter only: workers keep these fail-closed (Layer 2 design, FR-9).
    // ----------------------------------------------------------------

    #[pyo3(signature = (sql, *params))]
    fn fetch_one_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        refuse_async_in_worker(py, "fetch_one")?;
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        await_on_loop(
            py,
            async move {
                let q = sqlx::query(&sql);
                bind_params_raw(q, &bound)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| format!("fetch_one_async: {e}"))
            },
            |py, row_opt| match row_opt {
                Some(row) => Ok(row_to_dict(py, &row)?.into_any()),
                None => Ok(py.None()),
            },
        )
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_all_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        refuse_async_in_worker(py, "fetch_all")?;
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        await_on_loop(
            py,
            async move {
                let q = sqlx::query(&sql);
                bind_params_raw(q, &bound)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| format!("fetch_all_async: {e}"))
            },
            |py, rows| {
                let py_list = PyList::empty(py);
                for row in &rows {
                    py_list.append(row_to_dict(py, row)?)?;
                }
                Ok(py_list.into_any().unbind())
            },
        )
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_scalar_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        refuse_async_in_worker(py, "fetch_scalar")?;
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        await_on_loop(
            py,
            async move {
                let q = sqlx::query(&sql);
                bind_params_raw(q, &bound)
                    .fetch_one(pool)
                    .await
                    .map_err(|e| format!("fetch_scalar_async: {e}"))
            },
            |py, row| {
                let raw = row.try_get_raw(0).map_err(|e| {
                    PyRuntimeError::new_err(format!("fetch_scalar_async col 0: {e}"))
                })?;
                column_to_py(py, raw)
            },
        )
    }

    #[pyo3(signature = (sql, *params))]
    fn execute_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        refuse_async_in_worker(py, "execute")?;
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        await_on_loop(
            py,
            async move {
                let q = sqlx::query(&sql);
                bind_params_raw(q, &bound)
                    .execute(pool)
                    .await
                    .map_err(|e| format!("execute_async: {e}"))
            },
            |py, result| {
                Ok(result
                    .rows_affected()
                    .into_pyobject(py)?
                    .into_any()
                    .unbind())
            },
        )
    }

    /// Execute a statement that doesn't return rows. Returns the number of
    /// rows affected.
    #[pyo3(signature = (sql, *params))]
    fn execute(&self, py: Python<'_>, sql: &str, params: Vec<Py<PyAny>>) -> PyResult<u64> {
        let pool = pool_ref()?;
        let bound = params
            .iter()
            .map(|p| extract_param(p.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;

        // The future runs on the DB runtime, so it must own its inputs.
        let sql = sql.to_string();
        let result = py
            .detach(|| {
                run_on_db_rt(async move {
                    let q = sqlx::query(&sql);
                    bind_params_raw(q, &bound).execute(pool).await
                })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("execute runtime: {e}")))?
            .map_err(|e| PyRuntimeError::new_err(format!("execute: {e}")))?;
        Ok(result.rows_affected())
    }
}

/// Turns one cursor message into the iterator's result.
fn deliver(
    state: &Mutex<CursorState>,
    py: Python<'_>,
    msg: Option<CursorMsg>,
) -> PyResult<Py<PyDict>> {
    match msg {
        Some(CursorMsg::Row(row)) => row_to_dict(py, &row),
        Some(CursorMsg::Err(e)) => {
            // The driver stops after an error; nothing follows it.
            if let Ok(mut st) = state.lock() {
                st.rx = None;
                st.buf.clear();
            }
            Err(PyRuntimeError::new_err(e))
        }
        None => Err(PyStopIteration::new_err(py.None())),
    }
}

// ---------------------------------------------------------------------------
// Async results for any interpreter (Layer 2, C9)
// ---------------------------------------------------------------------------

/// `*_async` is main-interpreter only for now (Layer 2 design, FR-9). The message is the one
/// the worker bootstrap already gives.
fn refuse_async_in_worker(py: Python<'_>, name: &str) -> PyResult<()> {
    if Interp::current(py).is_main() {
        return Ok(());
    }
    Err(PyNotImplementedError::new_err(format!(
        "{name}_async is not available in sub-interpreter workers; \
         use {name} (sync) or route with gil=True"
    )))
}

/// Runs `fut` on the `pyronova-db` runtime and returns an asyncio future, created on the
/// caller's running loop, that completes with `convert(result)`.
///
/// The result is delivered by attaching to the interpreter captured here, never by a bare
/// attach from a runtime thread: once more than one interpreter has executed the engine, the
/// PyO3 fork refuses those (Layer 2 spike, R-3). `pyo3-async-runtimes` did exactly that.
///
/// - The asyncio future is set from its own loop, through `call_soon_threadsafe`, and only
///   if it isn't done yet: a cancelled caller is left alone.
/// - Cancelling the asyncio future aborts the DB task.
/// - Every path completes the future or finds it done, including a panic in the query or in
///   `convert`, and drops the captured loop and future while attached to their interpreter.
/// - A closed loop can't be reached; the result is dropped and the failure is logged.
///
/// Concurrency: queries run on the 2-thread `pyronova-db` runtime (`runtime()`); results are
/// delivered from its blocking pool, so a busy GIL never stalls a runtime thread.
fn await_on_loop<'py, T, F, C>(py: Python<'py>, fut: F, convert: C) -> PyResult<Bound<'py, PyAny>>
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T, String>> + Send + 'static,
    C: for<'a> FnOnce(Python<'a>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    let py_fut = event_loop.call_method0("create_future")?;
    let delivery = Delivery {
        interp: Interp::current(py),
        target: Some((event_loop.unbind(), py_fut.clone().unbind())),
    };

    let task = runtime().spawn(async move {
        let result = fut.await;
        // A failure here only means the runtime is shutting down; `delivery` was moved into
        // the closure, and its Drop completes the future either way.
        let _ = tokio::task::spawn_blocking(move || {
            delivery.complete(move |py| match result {
                Ok(value) => convert(py, value),
                Err(msg) => Err(PyRuntimeError::new_err(msg)),
            });
        })
        .await;
    });
    py_fut.call_method1("add_done_callback", (AbortOnCancel(task.abort_handle()),))?;
    Ok(py_fut)
}

/// The asyncio loop and future one `await_on_loop` call delivers to, and their interpreter.
struct Delivery {
    interp: Interp,
    /// `(loop, future)`; taken by the first completion.
    target: Option<(Py<PyAny>, Py<PyAny>)>,
}

impl Delivery {
    /// Sets the future to `outcome` from its loop (unless it is already done) and releases
    /// the loop and future, all while attached to their interpreter.
    fn complete<O>(mut self, outcome: O)
    where
        O: for<'a> FnOnce(Python<'a>) -> PyResult<Py<PyAny>>,
    {
        self.complete_with(outcome);
    }

    fn complete_with<O>(&mut self, outcome: O)
    where
        O: for<'a> FnOnce(Python<'a>) -> PyResult<Py<PyAny>>,
    {
        let Some((event_loop, py_fut)) = self.target.take() else {
            return;
        };
        // SAFETY: always safe to call.
        if unsafe { pyo3::ffi::Py_IsInitialized() == 0 || pyo3::ffi::Py_IsFinalizing() != 0 } {
            // Nothing left to deliver to, and no interpreter to release into.
            std::mem::forget((event_loop, py_fut));
            return;
        }
        let interp = self.interp;
        let run = move |py: Python<'_>| {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| outcome(py)))
                .unwrap_or_else(|_| {
                    Err(PyRuntimeError::new_err(
                        "converting the database result to Python panicked",
                    ))
                });
            let (ok, value) = match outcome {
                Ok(v) => (true, v),
                Err(e) => (false, e.into_value(py).into_any()),
            };
            let sent = resolver(py).and_then(|resolve| {
                event_loop
                    .bind(py)
                    .call_method1(
                        "call_soon_threadsafe",
                        (resolve, py_fut.bind(py), ok, value),
                    )
                    .map(drop)
            });
            if let Err(e) = sent {
                tracing::warn!(
                    target: "pyronova::server",
                    "async database result dropped: the asyncio loop can't be reached: {e}"
                );
            }
            drop(event_loop);
            drop(py_fut);
        };
        if interp.is_main() {
            main_attach(run);
        } else {
            attach_to(interp, run);
        }
    }
}

impl Drop for Delivery {
    /// Reached without a completion only if the task ended early: the query panicked, the
    /// task was aborted (the caller cancelled, and then the future is already done), or the
    /// runtime shut down.
    fn drop(&mut self) {
        self.complete_with(|_py| {
            Err(PyRuntimeError::new_err(
                "pyronova-db task ended without a result (it panicked, or the runtime shut down)",
            ))
        });
    }
}

/// `resolve(fut, ok, value)`: sets `fut` unless it is already done. Compiled once per
/// interpreter (`PyOnceLock` is per interpreter with the PyO3 fork).
static RESOLVE: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();

fn resolver(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    RESOLVE
        .get_or_try_init(py, || {
            let module = pyo3::types::PyModule::from_code(
                py,
                c"def resolve(fut, ok, value):\n    if fut.done():\n        return\n    if ok:\n        fut.set_result(value)\n    else:\n        fut.set_exception(value)\n",
                c"pyronova_db_async",
                c"pyronova_db_async",
            )?;
            Ok::<_, PyErr>(module.getattr("resolve")?.unbind())
        })
        .map(|f| f.bind(py).clone())
}

/// Done-callback on the asyncio future: aborts the DB task if the future was cancelled.
#[pyclass(frozen)]
struct AbortOnCancel(tokio::task::AbortHandle);

#[pymethods]
impl AbortOnCancel {
    fn __call__(&self, fut: &Bound<'_, PyAny>) -> PyResult<()> {
        if fut.call_method0("cancelled")?.is_truthy()? {
            self.0.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A TPC worker runs handlers inside a `current_thread` runtime. These tests run the
    /// waits from inside one.
    fn inside_current_thread_runtime<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn recv_batch_drains_a_cursor_channel_from_a_tokio_context() {
        let got = inside_current_thread_runtime(async {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(CURSOR_CAPACITY);
            runtime().spawn(async move {
                for i in 0..100 {
                    tx.send(i).await.unwrap();
                }
            });
            let mut got = Vec::new();
            loop {
                let (r, batch) = recv_batch(rx, CURSOR_CAPACITY).unwrap();
                assert!(batch.len() <= CURSOR_CAPACITY);
                if batch.is_empty() {
                    break;
                }
                got.extend(batch);
                rx = r;
            }
            got
        });
        assert_eq!(got, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn run_on_db_rt_waits_from_a_tokio_context() {
        let v = inside_current_thread_runtime(async { run_on_db_rt(async { 7 }).unwrap() });
        assert_eq!(v, 7);
    }

    /// The failure `recv_batch` replaces: what `PgCursor.__next__` used to call.
    #[test]
    #[should_panic(expected = "Cannot block the current thread from within a runtime")]
    fn blocking_recv_panics_in_a_tokio_context() {
        inside_current_thread_runtime(async {
            let (_tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
            rx.blocking_recv()
        });
    }

    /// The failure `run_on_db_rt` replaces: what the sync `PgPool` methods used to call.
    #[test]
    #[should_panic(expected = "Cannot start a runtime from within a runtime")]
    fn block_on_panics_in_a_tokio_context() {
        inside_current_thread_runtime(async { runtime().block_on(async { 7 }) });
    }
}
