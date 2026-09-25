//! The Python standard-library types Postgres values map to, looked up once per interpreter.
//!
//! Plain attribute lookups rather than PyO3's datetime C-API bindings: those cache one
//! capsule process-wide, and every sub-interpreter has its own `datetime` module.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;

/// Days from 0001-01-01 (`date.toordinal() == 1`) to 2000-01-01, the Postgres epoch.
pub(crate) const PG_EPOCH_ORDINAL: i32 = 730_120;

pub(crate) struct PyTypes {
    pub(crate) date: Py<PyAny>,
    pub(crate) datetime: Py<PyAny>,
    pub(crate) timedelta: Py<PyAny>,
    /// `datetime(2000, 1, 1)`, naive.
    pub(crate) pg_epoch: Py<PyAny>,
    /// `datetime(2000, 1, 1, tzinfo=timezone.utc)`.
    pub(crate) pg_epoch_utc: Py<PyAny>,
    pub(crate) one_microsecond: Py<PyAny>,
    pub(crate) uuid: Py<PyAny>,
    pub(crate) decimal: Py<PyAny>,
}

static TYPES: PyOnceLock<PyTypes> = PyOnceLock::new();

impl PyTypes {
    pub(crate) fn get(py: Python<'_>) -> PyResult<&PyTypes> {
        TYPES.get_or_try_init(py, || Self::load(py))
    }

    fn load(py: Python<'_>) -> PyResult<Self> {
        let dt = py.import("datetime")?;
        let datetime = dt.getattr("datetime")?;
        let timedelta = dt.getattr("timedelta")?;
        let utc = dt.getattr("timezone")?.getattr("utc")?;
        let epoch_utc_kwargs = PyDict::new(py);
        epoch_utc_kwargs.set_item("tzinfo", utc)?;
        Ok(Self {
            date: dt.getattr("date")?.unbind(),
            pg_epoch: datetime.call1((2000, 1, 1))?.unbind(),
            pg_epoch_utc: datetime
                .call((2000, 1, 1), Some(&epoch_utc_kwargs))?
                .unbind(),
            one_microsecond: timedelta.call1((0, 0, 1))?.unbind(),
            datetime: datetime.unbind(),
            timedelta: timedelta.unbind(),
            uuid: py.import("uuid")?.getattr("UUID")?.unbind(),
            decimal: py.import("decimal")?.getattr("Decimal")?.unbind(),
        })
    }
}
