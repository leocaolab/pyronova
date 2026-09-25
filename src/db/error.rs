//! Typed failures of the Postgres layer, and the Python exceptions they become.
//!
//! Python sees `DatabaseError` (a `RuntimeError`) for every failed query, with the
//! server's SQLSTATE in `.sqlstate` (`None` when the failure never reached the server:
//! pool timeout, dropped connection). Integrity violations (SQLSTATE class 23) raise
//! `IntegrityError`, and duplicate keys (23505) its subclass `UniqueViolation`, so a
//! handler can answer 409 without parsing message text. A value its parameter can't take
//! raises `ParamError`, a subclass of both `TypeError` and `ValueError`, before the query
//! is sent: a handler can answer it 422 without catching every `TypeError`.

use std::time::Duration;

use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PerInterpreterCell;
use pyo3::types::{PyTuple, PyType};

use super::numeric::NumericError;

pyo3::create_exception!(
    pyronova.engine,
    DatabaseError,
    PyRuntimeError,
    "A query failed. `sqlstate` is the server's SQLSTATE code, or None when the server never reported one."
);
pyo3::create_exception!(
    pyronova.engine,
    IntegrityError,
    DatabaseError,
    "An integrity constraint was violated (SQLSTATE class 23)."
);
pyo3::create_exception!(
    pyronova.engine,
    UniqueViolation,
    IntegrityError,
    "A unique or primary-key constraint was violated (SQLSTATE 23505)."
);

const INTEGRITY_CLASS: &str = "23";
const UNIQUE_VIOLATION: &str = "23505";

#[derive(Debug, thiserror::Error)]
pub(crate) enum DbError {
    #[error("PgPool not initialized — call PgPool.connect() first")]
    NotConnected,
    #[error(transparent)]
    Reconfigured(#[from] Reconfigured),
    #[error("PgPool connect: {0}")]
    Connect(#[source] sqlx::Error),
    /// `op` names the API call (`fetch_one`, `fetch_iter`, …) for the message.
    #[error("{op}: {source}")]
    Query {
        op: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error(transparent)]
    Param(#[from] ParamError),
    #[error(transparent)]
    Task(#[from] TaskError),
}

impl DbError {
    /// SQLSTATE the server reported, if the failure came from the server.
    pub(crate) fn sqlstate(&self) -> Option<String> {
        match self {
            Self::Connect(source) | Self::Query { source, .. } => source
                .as_database_error()
                .and_then(|e| e.code())
                .map(|code| code.into_owned()),
            _ => None,
        }
    }

    pub(crate) fn into_pyerr(self, py: Python<'_>) -> PyErr {
        let message = self.to_string();
        match self {
            Self::NotConnected | Self::Task(_) => PyRuntimeError::new_err(message),
            Self::Reconfigured(_) => PyValueError::new_err(message),
            Self::Connect(_) => PyConnectionError::new_err(message),
            // The statement's arity is the caller's code, not a value.
            Self::Param(ParamError::Count { .. }) => PyTypeError::new_err(message),
            Self::Param(_) => match param_error_type(py) {
                Ok(ty) => PyErr::from_type(ty.clone(), message),
                Err(e) => e,
            },
            Self::Query { .. } => database_error(py, message, self.sqlstate()),
        }
    }
}

/// Builds the `DatabaseError` subclass that `sqlstate` selects, with `.sqlstate` set.
fn database_error(py: Python<'_>, message: String, sqlstate: Option<String>) -> PyErr {
    let err = match sqlstate.as_deref() {
        Some(UNIQUE_VIOLATION) => UniqueViolation::new_err(message),
        Some(code) if code.starts_with(INTEGRITY_CLASS) => IntegrityError::new_err(message),
        _ => DatabaseError::new_err(message),
    };
    match err.value(py).setattr("sqlstate", sqlstate) {
        Ok(()) => err,
        Err(set_failed) => {
            set_failed.set_cause(py, Some(err));
            set_failed
        }
    }
}

/// `pyronova.engine.ParamError`, this interpreter's class: `class ParamError(TypeError,
/// ValueError)`, so code written against either base still catches it.
/// (`create_exception!` takes one base.)
fn param_error_type(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE_OBJECT: PerInterpreterCell<Py<PyType>> = PerInterpreterCell::new();
    let ty = TYPE_OBJECT.get_or_try_init(py, || {
        let bases = PyTuple::new(
            py,
            [py.get_type::<PyTypeError>(), py.get_type::<PyValueError>()],
        )?;
        // SAFETY: attached; NUL-terminated strings; `bases` is a tuple of exception
        // classes. Returns a new reference or NULL with an exception set.
        unsafe {
            Bound::from_owned_ptr_or_err(
                py,
                pyo3::ffi::PyErr_NewExceptionWithDoc(
                    c"pyronova.engine.ParamError".as_ptr(),
                    c"A value a statement parameter can't take (wrong type, out of range, not \
                      encodable), refused before the query is sent. Both a TypeError and a \
                      ValueError."
                        .as_ptr(),
                    bases.as_ptr(),
                    std::ptr::null_mut(),
                ),
            )
        }
        .and_then(|t| Ok(t.cast_into::<PyType>()?.unbind()))
    })?;
    Ok(ty.bind(py))
}

/// Adds the exception classes to the engine module. `sqlstate` defaults to None on the
/// class, so an instance raised by Python code also has it.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let base = py.get_type::<DatabaseError>();
    base.setattr("sqlstate", py.None())?;
    m.add("DatabaseError", base)?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("UniqueViolation", py.get_type::<UniqueViolation>())?;
    m.add("ParamError", param_error_type(py)?)?;
    Ok(())
}

/// How a later `PgPool.connect()` differs from the pool the process has; any difference
/// refuses the call. The DSN is never echoed: it may carry a password.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Reconfigured {
    #[error("PgPool is already connected to a different DSN; a process has one pool")]
    Dsn,
    #[error("PgPool is already connected with max_connections={existing}, not {asked}")]
    MaxConnections { existing: u32, asked: u32 },
    #[error(
        "PgPool is already connected with acquire_timeout_secs={}, not {}",
        existing.as_secs(),
        asked.as_secs()
    )]
    AcquireTimeout { existing: Duration, asked: Duration },
}

/// A Python argument that the statement's declared parameter type cannot take.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ParamError {
    #[error("the statement takes {expected} parameter(s), {given} given")]
    Count { expected: usize, given: usize },
    #[error("parameter ${index} is {pg_type}; a Python {given} cannot be sent as {pg_type}")]
    Type {
        index: usize,
        pg_type: String,
        given: &'static str,
    },
    #[error("parameter ${index}: {problem}")]
    Value { index: usize, problem: ValueProblem },
    #[error("parameter ${index}: {source}")]
    Encode {
        index: usize,
        #[source]
        source: sqlx::error::BoxDynError,
    },
    #[error(
        "parameter ${index}: unsupported parameter type {python_type} (supported: int, \
         float, str, bool, bytes, None, dict, list, datetime.date, datetime.datetime, \
         uuid.UUID, decimal.Decimal)"
    )]
    Unsupported { index: usize, python_type: String },
    /// Reading the value out of Python raised (a `str` with a lone surrogate, a `dict`
    /// that isn't JSON).
    #[error("parameter ${index}: {source}")]
    Unreadable {
        index: usize,
        #[source]
        source: PyErr,
    },
}

/// Why a value of an accepted Python type still does not fit its parameter.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ValueProblem {
    /// `value` as Python shows it (an int of any size, a float).
    #[error("{value} is out of range for {pg_type}")]
    OutOfRange {
        value: String,
        pg_type: &'static str,
    },
    #[error(transparent)]
    Numeric(#[from] NumericError),
}

/// A task on the `pyronova-db` runtime that ended without producing its output.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskError {
    #[error("pyronova-db task panicked: {0}")]
    Panicked(String),
    #[error("pyronova-db task was cancelled")]
    Cancelled,
    #[error("pyronova-db runtime shut down before the task finished")]
    RuntimeGone,
}

impl From<tokio::task::JoinError> for TaskError {
    fn from(e: tokio::task::JoinError) -> Self {
        match e.try_into_panic() {
            Ok(payload) => Self::Panicked(crate::error::panic_message(&*payload)),
            Err(_cancelled) => Self::Cancelled,
        }
    }
}
