//! Postgres result cells → Python objects, dispatched on the column's `PgKind`.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};
use sqlx::postgres::{PgRow, PgValueRef};
use sqlx::{Column, Decode, Postgres, Row, ValueRef};

use super::kind::{PgKind, JSONB_VERSION};
use super::numeric;
use super::py_types::{PyTypes, PG_EPOCH_ORDINAL};

pub(crate) fn row_to_dict(py: Python<'_>, row: &PgRow) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for col in row.columns() {
        dict.set_item(col.name(), column_value(py, row, col.ordinal())?)?;
    }
    Ok(dict.unbind())
}

/// The value of column `ordinal` of `row`.
pub(crate) fn column_value(py: Python<'_>, row: &PgRow, ordinal: usize) -> PyResult<Py<PyAny>> {
    let value = row
        .try_get_raw(ordinal)
        .map_err(|e| PyRuntimeError::new_err(format!("get column {ordinal}: {e}")))?;
    cell_to_py(py, value)
}

fn cell_to_py(py: Python<'_>, value: PgValueRef<'_>) -> PyResult<Py<PyAny>> {
    if value.is_null() {
        return Ok(py.None());
    }
    let kind = PgKind::of(&value.type_info());
    let bytes = value.as_bytes().map_err(wire_error)?;
    Ok(match kind {
        PgKind::Bool => decode::<bool>(value)?
            .into_pyobject(py)?
            .to_owned()
            .into_any(),
        PgKind::Int2 => decode::<i16>(value)?.into_pyobject(py)?.into_any(),
        PgKind::Int4 => decode::<i32>(value)?.into_pyobject(py)?.into_any(),
        PgKind::Int8 => decode::<i64>(value)?.into_pyobject(py)?.into_any(),
        PgKind::Float4 => decode::<f32>(value)?.into_pyobject(py)?.into_any(),
        PgKind::Float8 => decode::<f64>(value)?.into_pyobject(py)?.into_any(),
        PgKind::Text => PyString::new(py, decode::<&str>(value)?).into_any(),
        PgKind::Bytea | PgKind::Other => PyBytes::new(py, bytes).into_any(),
        PgKind::Json => json(py, bytes)?,
        PgKind::Jsonb => match bytes.split_first() {
            Some((&JSONB_VERSION, text)) => json(py, text)?,
            _ => {
                return Err(PyRuntimeError::new_err(
                    "jsonb value without version 1 header",
                ))
            }
        },
        PgKind::Numeric => {
            let text = numeric::decode(bytes).map_err(wire_error)?;
            PyTypes::get(py)?.decimal.bind(py).call1((text,))?
        }
        PgKind::Uuid => {
            let kwargs = PyDict::new(py);
            kwargs.set_item("bytes", PyBytes::new(py, bytes))?;
            PyTypes::get(py)?.uuid.bind(py).call((), Some(&kwargs))?
        }
        PgKind::Date => date(py, decode::<i32>(value)?)?,
        PgKind::Timestamp => timestamp(py, decode::<i64>(value)?, false)?,
        PgKind::Timestamptz => timestamp(py, decode::<i64>(value)?, true)?,
    }
    .unbind())
}

fn decode<'r, T: Decode<'r, Postgres>>(value: PgValueRef<'r>) -> PyResult<T> {
    T::decode(value).map_err(wire_error)
}

fn wire_error(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("undecodable value: {e}"))
}

/// A `json`/`jsonb` value with the codec `req.json()` uses, so a wide integer stays exact.
fn json<'py>(py: Python<'py>, text: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    crate::json::loads(py, text).map_err(wire_error)
}

fn date(py: Python<'_>, days: i32) -> PyResult<Bound<'_, PyAny>> {
    if days == i32::MAX || days == i32::MIN {
        return Err(infinity("date"));
    }
    let ordinal = i64::from(days) + i64::from(PG_EPOCH_ORDINAL);
    PyTypes::get(py)?
        .date
        .bind(py)
        .call_method1("fromordinal", (ordinal,))
}

fn timestamp(py: Python<'_>, micros: i64, utc: bool) -> PyResult<Bound<'_, PyAny>> {
    if micros == i64::MAX || micros == i64::MIN {
        return Err(infinity(if utc { "timestamptz" } else { "timestamp" }));
    }
    let types = PyTypes::get(py)?;
    let epoch = if utc {
        &types.pg_epoch_utc
    } else {
        &types.pg_epoch
    };
    let offset = types.timedelta.bind(py).call1((0, 0, micros))?;
    epoch.bind(py).add(offset)
}

/// Postgres `infinity` / `-infinity`, which `datetime` cannot represent.
fn infinity(pg_type: &str) -> PyErr {
    PyValueError::new_err(format!(
        "{pg_type} ±infinity has no Python equivalent; select it as text (col::text)"
    ))
}
