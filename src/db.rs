//! Async Postgres support via sqlx::PgPool.
//!
//! One process, one pool. `PgPool.connect(dsn)` populates a global `OnceLock` + a
//! dedicated tokio runtime that drives the pool's futures. All handlers (GIL or
//! sub-interpreter) share the same connection pool — no per-interp duplication. A later
//! `connect()` reuses it when every setting it names matches; one asking for another DSN,
//! `max_connections` or `acquire_timeout_secs` raises `ValueError`.
//!
//! Scope:
//!   * sync API (`pool.fetch_one(sql, *params)` blocks the calling thread
//!     until the future completes, GIL released), usable from the main
//!     interpreter and from sub-interpreter workers.
//!   * `*_async` awaitables (`await_on_loop`), main interpreter only; a
//!     worker gets `NotImplementedError`.
//!   * parameters are encoded as the statement declares them (`param`); columns decode
//!     by a closed type kind (`kind`, `cell`). Python counterparts: bool, int, float,
//!     str, bytes, dict/list (json/jsonb), decimal.Decimal (numeric), uuid.UUID,
//!     datetime.date, datetime.datetime (timestamp naive, timestamptz aware UTC).
//!     Every other type travels as its raw binary wire bytes.
//!   * failures are `DbError`; queries raise `DatabaseError` with `.sqlstate` (`error`).
//!
//! Architecture notes:
//!   * The pool runs on its own tokio runtime, not the server's, so DB I/O stays off the
//!     accept loop. Callers hand the future to it with `run_on_db_rt` and wait on a std
//!     channel, never `Runtime::block_on`, which panics on a thread already inside a
//!     Tokio context (a TPC worker runs its handlers inside one).
//!   * The wait releases the GIL (`py.detach()`), so other Python threads run meanwhile.

mod cell;
mod error;
mod kind;
mod numeric;
mod param;
mod py_types;

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use futures_util::TryStreamExt;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use sqlx::postgres::{PgArguments, PgConnection, PgPoolOptions, PgRow};
use sqlx::{Either, Executor, Postgres, Statement as _};
use tokio::runtime::Runtime;

use crate::run_context::{attach_to, main_attach, Interp};
use error::{DbError, Reconfigured, TaskError};
use param::PyParam;

/// Rows in flight to a `PgCursor`: bounded memory, with enough prefetch to hide the
/// server round trip from a fast consumer.
const CURSOR_CAPACITY: usize = 8;

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
/// Worker threads of the `pyronova-db` runtime: they only drive socket I/O.
const DB_RUNTIME_THREADS: usize = 2;

static PG_POOL: OnceLock<Connected> = OnceLock::new();
static PG_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub(crate) fn runtime() -> &'static Runtime {
    // A runtime that can't be built (threads or fds exhausted) is unrecoverable: abort
    // with the reason rather than unwind a panic into Python.
    PG_RUNTIME.get_or_init(|| {
        match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(DB_RUNTIME_THREADS)
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

/// Adds the DB classes and exceptions to the engine module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PgPool>()?;
    m.add_class::<PgCursor>()?;
    error::register(m)
}

pub(crate) fn pool_ref() -> Result<&'static sqlx::PgPool, DbError> {
    PG_POOL.get().map(|c| &c.pool).ok_or(DbError::NotConnected)
}

/// Run a `Send + 'static` future on the DB runtime and wait for its result on the
/// calling thread.
///
/// `Runtime::block_on` panics with "Cannot start a runtime from within a runtime" when
/// the calling thread is already inside a Tokio context, which is where TPC workers run
/// handlers (a `current_thread` runtime + `LocalSet`). `spawn` has no such check, and
/// the caller waits on a std channel instead. Callers release the GIL (`py.detach`)
/// around this call.
pub(crate) fn run_on_db_rt<F, T>(fut: F) -> Result<T, TaskError>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let task = runtime().spawn(fut);
    runtime().spawn(async move {
        // The caller blocks on `rx` until this arrives; a failed send means it is gone.
        tx.send(task.await.map_err(TaskError::from)).ok();
    });
    rx.recv().map_err(|_| TaskError::RuntimeGone)?
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
) -> Result<(tokio::sync::mpsc::Receiver<T>, Vec<T>), TaskError> {
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
// The pool and its settings
// ---------------------------------------------------------------------------

struct Connected {
    settings: PoolSettings,
    pool: sqlx::PgPool,
}

#[derive(Clone)]
struct PoolSettings {
    dsn: String,
    max_connections: u32,
    acquire_timeout: Duration,
}

/// What a `PgPool.connect()` call asked for. `None` means "whatever the pool has".
struct PoolRequest {
    dsn: String,
    max_connections: Option<u32>,
    acquire_timeout: Option<Duration>,
}

impl PoolRequest {
    fn settings(&self) -> PoolSettings {
        PoolSettings {
            dsn: self.dsn.clone(),
            max_connections: self.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS),
            acquire_timeout: self.acquire_timeout.unwrap_or(DEFAULT_ACQUIRE_TIMEOUT),
        }
    }

    /// How the process's pool, opened with `existing`, differs from what this call asked for.
    fn differences(&self, existing: &PoolSettings) -> Vec<Reconfigured> {
        let dsn = (self.dsn != existing.dsn).then_some(Reconfigured::Dsn);
        let max_connections = self
            .max_connections
            .filter(|&asked| asked != existing.max_connections)
            .map(|asked| Reconfigured::MaxConnections {
                existing: existing.max_connections,
                asked,
            });
        let acquire_timeout = self
            .acquire_timeout
            .filter(|&asked| asked != existing.acquire_timeout)
            .map(|asked| Reconfigured::AcquireTimeout {
                existing: existing.acquire_timeout,
                asked,
            });
        [dsn, max_connections, acquire_timeout]
            .into_iter()
            .flatten()
            .collect()
    }
}

/// Whether this call may use the process's pool: only if every setting it names matches.
/// A process has one pool, so a call asking for another DSN or another sizing cannot get
/// what it asked for and is refused. Settings left out (`None`) match any pool.
fn reuse(request: &PoolRequest, existing: &PoolSettings) -> Result<(), Reconfigured> {
    match request.differences(existing).into_iter().next() {
        Some(difference) => Err(difference),
        None => Ok(()),
    }
}

/// Opens the process's pool, or checks that the one already open can serve `request`.
fn connect_pool(py: Python<'_>, request: PoolRequest) -> Result<(), DbError> {
    if let Some(existing) = PG_POOL.get() {
        return Ok(reuse(&request, &existing.settings)?);
    }

    let settings = request.settings();
    let options = PgPoolOptions::new()
        .max_connections(settings.max_connections)
        .acquire_timeout(settings.acquire_timeout);
    let dsn = settings.dsn.clone();
    let pool = py
        .detach(|| run_on_db_rt(async move { options.connect(&dsn).await }))?
        .map_err(DbError::Connect)?;

    // Two first calls can race; the loser closes its pool and is checked against the winner.
    // The winner's settings came from its own request, so it needs no check.
    match PG_POOL.set(Connected { settings, pool }) {
        Ok(()) => Ok(()),
        Err(lost) => {
            runtime().spawn(async move { lost.pool.close().await });
            Ok(reuse(&request, &PG_POOL.wait().settings)?)
        }
    }
}

// ---------------------------------------------------------------------------
// Running a statement
// ---------------------------------------------------------------------------

/// A statement and its arguments, owned so the query can run on the DB runtime.
struct Statement {
    sql: String,
    params: Vec<PyParam>,
}

impl Statement {
    fn from_py(py: Python<'_>, sql: String, params: &[Py<PyAny>]) -> PyResult<Self> {
        let params = (1..)
            .zip(params)
            .map(|(index, p)| PyParam::extract(p.bind(py), index))
            .collect::<Result<_, _>>()
            .map_err(|e| DbError::from(e).into_pyerr(py))?;
        Ok(Self { sql, params })
    }
}

/// Prepares `sql` on `conn` (cached per connection after the first time) and encodes
/// `params` as the parameter types the server declared for it.
async fn bind<'q>(
    conn: &mut PgConnection,
    op: &'static str,
    sql: &'q str,
    params: Vec<PyParam>,
) -> Result<sqlx::query::Query<'q, Postgres, PgArguments>, DbError> {
    let prepared = conn
        .prepare(sql)
        .await
        .map_err(|source| DbError::Query { op, source })?;
    let declared = match prepared.parameters() {
        Some(Either::Left(types)) => types,
        // Postgres always describes parameter types; a bare count carries none.
        Some(Either::Right(_)) | None => &[],
    };
    let args = param::encode_args(declared, params)?;
    Ok(sqlx::query_with(sql, args))
}

/// The ways to run a statement to completion.
#[derive(Clone, Copy)]
enum Fetch {
    One,
    All,
    Scalar,
    Execute,
}

impl Fetch {
    fn name(self) -> &'static str {
        match self {
            Self::One => "fetch_one",
            Self::All => "fetch_all",
            Self::Scalar => "fetch_scalar",
            Self::Execute => "execute",
        }
    }
}

/// What each `Fetch` produces.
enum Outcome {
    Row(Option<PgRow>),
    Rows(Vec<PgRow>),
    Scalar(PgRow),
    Affected(u64),
}

impl Outcome {
    fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(match self {
            Self::Row(Some(row)) => cell::row_to_dict(py, &row)?.into_any(),
            Self::Row(None) => py.None(),
            Self::Rows(rows) => {
                let list = PyList::empty(py);
                for row in &rows {
                    list.append(cell::row_to_dict(py, row)?)?;
                }
                list.into_any().unbind()
            }
            Self::Scalar(row) => cell::column_value(py, &row, 0)?,
            Self::Affected(n) => n.into_pyobject(py)?.into_any().unbind(),
        })
    }
}

async fn run(pool: &sqlx::PgPool, stmt: Statement, fetch: Fetch) -> Result<Outcome, DbError> {
    let op = fetch.name();
    let query_error = |source| DbError::Query { op, source };
    let mut conn = pool.acquire().await.map_err(query_error)?;
    let query = bind(&mut conn, op, &stmt.sql, stmt.params).await?;
    match fetch {
        Fetch::One => query.fetch_optional(&mut *conn).await.map(Outcome::Row),
        Fetch::All => query.fetch_all(&mut *conn).await.map(Outcome::Rows),
        Fetch::Scalar => query.fetch_one(&mut *conn).await.map(Outcome::Scalar),
        Fetch::Execute => query
            .execute(&mut *conn)
            .await
            .map(|done| Outcome::Affected(done.rows_affected())),
    }
    .map_err(query_error)
}

/// The sync API: runs the statement on the DB runtime while this thread waits, GIL released.
fn run_blocking(
    py: Python<'_>,
    fetch: Fetch,
    sql: String,
    params: Vec<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let pool = pool_ref().map_err(|e| e.into_pyerr(py))?;
    let stmt = Statement::from_py(py, sql, &params)?;
    let outcome = py
        .detach(|| run_on_db_rt(run(pool, stmt, fetch)))
        .map_err(DbError::from)
        .and_then(|done| done)
        .map_err(|e| e.into_pyerr(py))?;
    outcome.into_py(py)
}

/// The `*_async` API: an asyncio future completed from the DB runtime (`await_on_loop`).
fn run_awaitable<'py>(
    py: Python<'py>,
    fetch: Fetch,
    sql: String,
    params: Vec<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    refuse_async_in_worker(py, fetch.name())?;
    let pool = pool_ref().map_err(|e| e.into_pyerr(py))?;
    let stmt = Statement::from_py(py, sql, &params)?;
    await_on_loop(py, run(pool, stmt, fetch), |py, outcome: Outcome| {
        outcome.into_py(py)
    })
}

// ---------------------------------------------------------------------------
// Streaming cursor (fetch_iter)
// ---------------------------------------------------------------------------

const FETCH_ITER: &str = "fetch_iter";

/// A message from the cursor's driver task to the iterator. The channel closing (the
/// driver finished or failed) is EOF.
enum CursorMsg {
    Row(PgRow),
    /// The last message: the driver stops after an error.
    Err(DbError),
}

/// Runs the statement and sends its rows to the cursor, then an error if one occurred.
async fn drive_cursor(
    pool: &sqlx::PgPool,
    stmt: Statement,
    tx: tokio::sync::mpsc::Sender<CursorMsg>,
) {
    if let Err(e) = stream_rows(pool, stmt, &tx).await {
        // A failed send means the cursor was dropped, and nobody wants the error.
        tx.send(CursorMsg::Err(e)).await.ok();
    }
}

async fn stream_rows(
    pool: &sqlx::PgPool,
    stmt: Statement,
    tx: &tokio::sync::mpsc::Sender<CursorMsg>,
) -> Result<(), DbError> {
    let query_error = |source| DbError::Query {
        op: FETCH_ITER,
        source,
    };
    let mut conn = pool.acquire().await.map_err(query_error)?;
    let query = bind(&mut conn, FETCH_ITER, &stmt.sql, stmt.params).await?;
    let mut rows = query.fetch(&mut *conn);
    while let Some(row) = rows.try_next().await.map_err(query_error)? {
        if tx.send(CursorMsg::Row(row)).await.is_err() {
            // The cursor was dropped: stop, and the connection returns to the pool.
            return Ok(());
        }
    }
    Ok(())
}

/// Streaming result-set iterator. Constructed by `PgPool.fetch_iter(sql, ...)`.
///
/// Memory contract: at most `CURSOR_CAPACITY` rows in the channel plus as many in `buf`,
/// and the one dict just yielded: O(1), where `fetch_all` peaks at O(2N) (the rows and
/// the list of dicts both alive during conversion).
///
/// A dedicated task on the pool's tokio runtime drives the sqlx stream and pushes each
/// row through a bounded async channel, so a slow consumer applies backpressure without
/// blocking a runtime thread. `__next__` takes rows from that channel with `recv_batch`
/// (GIL released), which works from any thread, including one inside a Tokio context.
/// Dropping the cursor before EOF closes the channel, which stops the driver task on
/// its next send; the sqlx connection returns to the pool cleanly.
///
/// Threads sharing a cursor take turns: while one waits on the channel, another finds
/// the receiver checked out and waits on `returned` (GIL released) for a batch or EOF.
#[pyclass(module = "pyronova.engine")]
pub(crate) struct PgCursor {
    state: Mutex<CursorState>,
    /// Signalled when a thread gives the receiver back.
    returned: Condvar,
}

struct CursorState {
    rx: Receiver,
    buf: VecDeque<CursorMsg>,
}

enum Receiver {
    Here(tokio::sync::mpsc::Receiver<CursorMsg>),
    /// A thread is waiting on the channel with it.
    CheckedOut,
    /// End of stream.
    Closed,
}

/// What a thread calling `__next__` does next.
enum Turn {
    Deliver(Option<CursorMsg>),
    Receive(tokio::sync::mpsc::Receiver<CursorMsg>),
}

impl CursorState {
    /// This thread's turn, or `None` while another thread holds the receiver.
    fn claim(&mut self) -> Option<Turn> {
        if let Some(msg) = self.buf.pop_front() {
            return Some(Turn::Deliver(Some(msg)));
        }
        match std::mem::replace(&mut self.rx, Receiver::CheckedOut) {
            Receiver::Here(rx) => Some(Turn::Receive(rx)),
            Receiver::CheckedOut => None,
            Receiver::Closed => {
                self.rx = Receiver::Closed;
                Some(Turn::Deliver(None))
            }
        }
    }
}

impl PgCursor {
    fn new(rx: tokio::sync::mpsc::Receiver<CursorMsg>) -> Self {
        Self {
            state: Mutex::new(CursorState {
                rx: Receiver::Here(rx),
                buf: VecDeque::new(),
            }),
            returned: Condvar::new(),
        }
    }

    /// A poisoned mutex becomes a PyRuntimeError, never a panic across FFI.
    fn lock(&self) -> PyResult<MutexGuard<'_, CursorState>> {
        self.state
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("cursor mutex poisoned: {e}")))
    }

    /// Blocks until this thread's turn. Call with the GIL released: the thread holding
    /// the receiver needs the GIL to give it back.
    fn wait_for_turn(&self) -> PyResult<Turn> {
        let mut st = self.lock()?;
        loop {
            if let Some(turn) = st.claim() {
                return Ok(turn);
            }
            st = self
                .returned
                .wait(st)
                .map_err(|e| PyRuntimeError::new_err(format!("cursor mutex poisoned: {e}")))?;
        }
    }

    /// Waits for the next batch (GIL released), gives the receiver back, and takes the
    /// first message of what is buffered.
    fn receive(
        &self,
        py: Python<'_>,
        rx: tokio::sync::mpsc::Receiver<CursorMsg>,
    ) -> PyResult<Option<CursorMsg>> {
        let received = py.detach(|| recv_batch(rx, CURSOR_CAPACITY));
        let next = {
            let mut st = self.lock()?;
            match received {
                Ok((rx, batch)) => {
                    st.rx = if batch.is_empty() {
                        Receiver::Closed
                    } else {
                        Receiver::Here(rx)
                    };
                    st.buf.extend(batch);
                    Ok(st.buf.pop_front())
                }
                Err(e) => {
                    st.rx = Receiver::Closed;
                    Err(DbError::from(e))
                }
            }
        };
        self.returned.notify_all();
        next.map_err(|e| e.into_pyerr(py))
    }
}

#[pymethods]
impl PgCursor {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next row as a dict; `StopIteration` at EOF, `DatabaseError` on a database error.
    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let claimed = self.lock()?.claim();
        let turn = match claimed {
            Some(turn) => turn,
            None => py.detach(|| self.wait_for_turn())?,
        };
        let msg = match turn {
            Turn::Deliver(msg) => msg,
            Turn::Receive(rx) => self.receive(py, rx)?,
        };
        match msg {
            Some(CursorMsg::Row(row)) => cell::row_to_dict(py, &row),
            Some(CursorMsg::Err(e)) => Err(e.into_pyerr(py)),
            None => Err(PyStopIteration::new_err(py.None())),
        }
    }

    /// Every remaining row, as a list of dicts (the shape `fetch_all` returns).
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
    /// Open the process's pool, or return a handle to the one already open. A later call
    /// with another DSN, `max_connections` or `acquire_timeout_secs` raises ValueError.
    /// `max_connections` / `acquire_timeout_secs` default to 10 / 30 on the first call;
    /// on a later one they may be left out, and then match whatever the pool has.
    #[classmethod]
    #[pyo3(signature = (dsn, max_connections = None, acquire_timeout_secs = None))]
    fn connect(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        dsn: String,
        max_connections: Option<u32>,
        acquire_timeout_secs: Option<u64>,
    ) -> PyResult<Self> {
        let request = PoolRequest {
            dsn,
            max_connections,
            acquire_timeout: acquire_timeout_secs.map(Duration::from_secs),
        };
        connect_pool(py, request).map_err(|e| e.into_pyerr(py))?;
        Ok(PgPool)
    }

    /// Fetch exactly one row or None. Extra rows are ignored (no error).
    #[pyo3(signature = (sql, *params))]
    fn fetch_one(
        &self,
        py: Python<'_>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        run_blocking(py, Fetch::One, sql, params)
    }

    /// Fetch all matching rows into a list of dicts.
    #[pyo3(signature = (sql, *params))]
    fn fetch_all(
        &self,
        py: Python<'_>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        run_blocking(py, Fetch::All, sql, params)
    }

    /// Fetch a single column of a single row. Raises if no rows; returns
    /// None for SQL NULL. Useful for `SELECT count(*) FROM ...`.
    #[pyo3(signature = (sql, *params))]
    fn fetch_scalar(
        &self,
        py: Python<'_>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        run_blocking(py, Fetch::Scalar, sql, params)
    }

    /// Execute a statement that doesn't return rows. Returns the number of
    /// rows affected.
    #[pyo3(signature = (sql, *params))]
    fn execute(&self, py: Python<'_>, sql: String, params: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        run_blocking(py, Fetch::Execute, sql, params)
    }

    /// Rows one at a time through a `PgCursor`, in O(1) memory (see there). For large
    /// result sets; for a typical API query `fetch_all` is simpler.
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
    fn fetch_iter(
        &self,
        py: Python<'_>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<PgCursor> {
        let pool = pool_ref().map_err(|e| e.into_pyerr(py))?;
        let stmt = Statement::from_py(py, sql, &params)?;
        let (tx, rx) = tokio::sync::mpsc::channel(CURSOR_CAPACITY);
        runtime().spawn(drive_cursor(pool, stmt, tx));
        Ok(PgCursor::new(rx))
    }

    // ----------------------------------------------------------------
    // Async variants: `await pool.fetch_one_async(sql, ...)` from an `async def` handler.
    // The query runs on the `pyronova-db` runtime and its result reaches the caller's
    // asyncio loop through `await_on_loop`. Separate methods, so one call never returns a
    // dict in one place and a coroutine in another. Main interpreter only
    // (`refuse_async_in_worker`).
    // ----------------------------------------------------------------

    #[pyo3(signature = (sql, *params))]
    fn fetch_one_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, Fetch::One, sql, params)
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_all_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, Fetch::All, sql, params)
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_scalar_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, Fetch::Scalar, sql, params)
    }

    #[pyo3(signature = (sql, *params))]
    fn execute_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, Fetch::Execute, sql, params)
    }
}

// ---------------------------------------------------------------------------
// Async results
// ---------------------------------------------------------------------------

/// `*_async` runs on the main interpreter only: a worker gets `NotImplementedError`
/// naming the sync method and `gil=True`.
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
/// PyO3 fork refuses those (which rules out `pyo3-async-runtimes`).
///
/// - The asyncio future is set from its own loop, through `call_soon_threadsafe`, and only
///   if it isn't done yet: a cancelled caller is left alone.
/// - Cancelling the asyncio future aborts the query task.
/// - Every path completes the future or finds it done. A panic in the query arrives as
///   its payload (`TaskError::Panicked`); one in `convert` is caught in `Delivery`.
/// - A closed loop can't be reached; the result is dropped and the failure is logged.
///
/// Concurrency: queries run on the `pyronova-db` runtime (`runtime()`); results are
/// delivered from its blocking pool, so a busy GIL never stalls a runtime thread.
fn await_on_loop<'py, T, F, C>(py: Python<'py>, fut: F, convert: C) -> PyResult<Bound<'py, PyAny>>
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T, DbError>> + Send + 'static,
    C: for<'a> FnOnce(Python<'a>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    let py_fut = event_loop.call_method0("create_future")?;
    let delivery = Delivery {
        interp: Interp::current(py),
        target: Some((event_loop.unbind(), py_fut.clone().unbind())),
    };

    let query = runtime().spawn(fut);
    let abort = query.abort_handle();
    runtime().spawn(async move {
        let result = match query.await {
            Ok(done) => done,
            Err(join_error) => Err(DbError::from(TaskError::from(join_error))),
        };
        // Err only when the runtime is shutting down; `delivery` was moved into the
        // closure, and its Drop completes the future either way.
        tokio::task::spawn_blocking(move || {
            delivery.complete(move |py| match result {
                Ok(value) => convert(py, value),
                Err(e) => Err(e.into_pyerr(py)),
            });
        })
        .await
        .ok();
    });
    py_fut.call_method1("add_done_callback", (AbortOnCancel(abort),))?;
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
    /// Reached without a completion only if the delivering task never ran to the end: the
    /// runtime shut down.
    fn drop(&mut self) {
        self.complete_with(|_py| {
            Err(PyRuntimeError::new_err(
                "pyronova-db task ended without a result: the runtime shut down",
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

    /// Why `recv_batch` exists: `blocking_recv` in a TPC worker's context panics.
    #[test]
    #[should_panic(expected = "Cannot block the current thread from within a runtime")]
    fn blocking_recv_panics_in_a_tokio_context() {
        inside_current_thread_runtime(async {
            let (_tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
            rx.blocking_recv()
        });
    }

    /// Why `run_on_db_rt` exists: `block_on` in a TPC worker's context panics.
    #[test]
    #[should_panic(expected = "Cannot start a runtime from within a runtime")]
    fn block_on_panics_in_a_tokio_context() {
        inside_current_thread_runtime(async { runtime().block_on(async { 7 }) });
    }
}

/// Task failures keep their cause; pool settings are checked.
#[cfg(test)]
mod pool_settings_tests {
    use super::*;

    #[test]
    fn a_panicking_task_reports_its_panic_message() {
        let err = run_on_db_rt::<_, ()>(async { panic!("boom {}", 42) }).unwrap_err();
        match err {
            TaskError::Panicked(msg) => assert_eq!(msg, "boom 42"),
            other => panic!("expected Panicked, got {other:?}"),
        }
    }

    fn request(dsn: &str, max: Option<u32>, timeout: Option<u64>) -> PoolRequest {
        PoolRequest {
            dsn: dsn.to_owned(),
            max_connections: max,
            acquire_timeout: timeout.map(Duration::from_secs),
        }
    }

    #[test]
    fn a_request_matches_the_pool_it_describes() {
        let existing = request("postgres://a", Some(4), None).settings();
        assert!(request("postgres://a", None, None)
            .differences(&existing)
            .is_empty());
        assert!(request("postgres://a", Some(4), Some(30))
            .differences(&existing)
            .is_empty());
    }

    #[test]
    fn every_difference_is_named() {
        let existing = request("postgres://a", Some(4), None).settings();
        let found = request("postgres://b", Some(5), Some(5)).differences(&existing);
        assert!(matches!(
            found.as_slice(),
            [
                Reconfigured::Dsn,
                Reconfigured::MaxConnections {
                    existing: 4,
                    asked: 5
                },
                Reconfigured::AcquireTimeout { .. },
            ]
        ));
    }

    #[test]
    fn any_difference_is_refused() {
        let existing = request("postgres://a", Some(4), None).settings();
        assert!(matches!(
            reuse(&request("postgres://b", None, None), &existing),
            Err(Reconfigured::Dsn)
        ));
        assert!(matches!(
            reuse(&request("postgres://a", Some(5), None), &existing),
            Err(Reconfigured::MaxConnections {
                existing: 4,
                asked: 5
            })
        ));
        assert!(matches!(
            reuse(&request("postgres://a", None, Some(5)), &existing),
            Err(Reconfigured::AcquireTimeout { .. })
        ));
    }

    #[test]
    fn settings_left_out_or_equal_are_reused() {
        let existing = request("postgres://a", Some(4), None).settings();
        assert!(reuse(&request("postgres://a", None, None), &existing).is_ok());
        assert!(reuse(&request("postgres://a", Some(4), Some(30)), &existing).is_ok());
    }
}
