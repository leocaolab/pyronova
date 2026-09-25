//! Typed failures of the Postgres layer, and the Python exceptions they become.
//!
//! Python sees `DatabaseError` (a `RuntimeError`) for every failed query, with the
//! server's SQLSTATE in `.sqlstate` (`None` when the failure never reached the server:
//! pool timeout, dropped connection). Integrity violations (SQLSTATE class 23) raise
//! `IntegrityError`, and duplicate keys (23505) its subclass `UniqueViolation`, so a
//! handler can answer 409 without parsing message text.

use std::time::Duration;

use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;

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
            Self::Param(ParamError::Count { .. } | ParamError::Type { .. }) => {
                PyTypeError::new_err(message)
            }
            Self::Param(ParamError::Value { .. } | ParamError::Encode { .. }) => {
                PyValueError::new_err(message)
            }
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

/// Adds the exception classes to the engine module. `sqlstate` defaults to None on the
/// class, so an instance raised by Python code also has it.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let base = py.get_type::<DatabaseError>();
    base.setattr("sqlstate", py.None())?;
    m.add("DatabaseError", base)?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("UniqueViolation", py.get_type::<UniqueViolation>())?;
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
}

/// Why a value of an accepted Python type still does not fit its parameter.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ValueProblem {
    #[error("{value} is out of range for {pg_type}")]
    OutOfRange { value: i64, pg_type: &'static str },
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
            Ok(payload) => Self::Panicked(panic_message(payload)),
            Err(_cancelled) => Self::Cancelled,
        }
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(s) => *s,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(s) => (*s).to_owned(),
            Err(_) => "the panic payload is not a string".to_owned(),
        },
    }
}
