//! Python arguments → Postgres parameters, encoded as the statement declares them.
//!
//! sqlx caches a prepared statement per SQL text, with the parameter types of its first
//! execution. Choosing each parameter's type from the Python value would make the cached
//! types depend on whichever values came first, and later values of another type would be
//! sent in the wrong binary form. So the statement is prepared untyped (the server infers
//! every parameter from the SQL), and each value is encoded as the type the server
//! declared: `None` becomes a NULL of that type, `5` an int4 or a numeric as needed.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::types::Oid;
use sqlx::postgres::{PgArgumentBuffer, PgArguments, PgTypeInfo};
use sqlx::{Arguments, Postgres, TypeInfo};

use super::error::{ParamError, ValueProblem};
use super::kind::{PgKind, JSONB_VERSION};
use super::numeric;
use super::py_types::{PyTypes, PG_EPOCH_ORDINAL};

/// A Python argument copied out of the interpreter, so the query can run without the GIL.
pub(crate) enum PyParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// A dict or list.
    Json(serde_json::Value),
    Uuid([u8; 16]),
    /// Plain decimal text: `-12.50`, `NaN`, `Infinity`.
    Decimal(String),
    /// Days since 2000-01-01.
    Date(i32),
    /// Microseconds since 2000-01-01 00:00, naive.
    Timestamp(i64),
    /// Microseconds since 2000-01-01 00:00 UTC.
    TimestampTz(i64),
}

impl PyParam {
    pub(crate) fn extract(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        if obj.is_none() {
            return Ok(Self::Null);
        }
        // PyBool is a subclass of PyInt: check bool first.
        if let Ok(b) = obj.cast::<PyBool>() {
            return Ok(Self::Bool(b.is_true()));
        }
        if let Ok(i) = obj.cast::<PyInt>() {
            return Ok(Self::Int(i.extract()?));
        }
        if let Ok(f) = obj.cast::<PyFloat>() {
            return Ok(Self::Float(f.value()));
        }
        if let Ok(s) = obj.cast::<PyString>() {
            return Ok(Self::Text(s.to_str()?.to_owned()));
        }
        if let Ok(b) = obj.cast::<PyBytes>() {
            return Ok(Self::Bytes(b.as_bytes().to_vec()));
        }
        if obj.is_instance_of::<PyDict>() || obj.is_instance_of::<PyList>() {
            let value = pythonize::depythonize(obj).map_err(|e| {
                PyValueError::new_err(format!("JSON convert error on dict/list param: {e}"))
            })?;
            return Ok(Self::Json(value));
        }
        Self::extract_stdlib(obj)
    }

    /// `datetime`, `date`, `uuid.UUID`, `decimal.Decimal`.
    fn extract_stdlib(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let py = obj.py();
        let types = PyTypes::get(py)?;
        // datetime is a subclass of date: check datetime first.
        if obj.is_instance(types.datetime.bind(py))? {
            let naive = obj.call_method0("utcoffset")?.is_none();
            let epoch = if naive {
                &types.pg_epoch
            } else {
                &types.pg_epoch_utc
            };
            let micros: i64 = obj
                .sub(epoch.bind(py))?
                .floor_div(types.one_microsecond.bind(py))?
                .extract()?;
            return Ok(if naive {
                Self::Timestamp(micros)
            } else {
                Self::TimestampTz(micros)
            });
        }
        if obj.is_instance(types.date.bind(py))? {
            let ordinal: i32 = obj.call_method0("toordinal")?.extract()?;
            return Ok(Self::Date(ordinal - PG_EPOCH_ORDINAL));
        }
        if obj.is_instance(types.uuid.bind(py))? {
            let bytes = obj.getattr("bytes")?;
            return Ok(Self::Uuid(
                bytes
                    .cast::<PyBytes>()?
                    .as_bytes()
                    .try_into()
                    .map_err(|_| PyValueError::new_err("uuid.UUID.bytes is not 16 bytes"))?,
            ));
        }
        if obj.is_instance(types.decimal.bind(py))? {
            return Ok(Self::Decimal(
                obj.call_method1("__format__", ("f",))?.extract()?,
            ));
        }
        Err(PyValueError::new_err(format!(
            "unsupported parameter type: {} (supported: int, float, str, bool, bytes, None, \
             dict, list, datetime.date, datetime.datetime, uuid.UUID, decimal.Decimal)",
            obj.get_type().name()?
        )))
    }

    fn python_type(&self) -> &'static str {
        match self {
            Self::Null => "None",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Text(_) => "str",
            Self::Bytes(_) => "bytes",
            Self::Json(_) => "dict/list",
            Self::Uuid(_) => "uuid.UUID",
            Self::Decimal(_) => "decimal.Decimal",
            Self::Date(_) => "datetime.date",
            Self::Timestamp(_) => "naive datetime.datetime",
            Self::TimestampTz(_) => "aware datetime.datetime",
        }
    }
}

/// Encodes `params` as the statement's `declared` parameter types.
pub(crate) fn encode_args(
    declared: &[PgTypeInfo],
    params: Vec<PyParam>,
) -> Result<PgArguments, ParamError> {
    if declared.len() != params.len() {
        return Err(ParamError::Count {
            expected: declared.len(),
            given: params.len(),
        });
    }
    let mut args = PgArguments::default();
    for (index, (ty, param)) in (1..).zip(declared.iter().zip(params)) {
        let wire = to_wire(ty, param, index)?;
        args.add(PgParam {
            ty: ty.clone(),
            wire,
        })
        .map_err(|source| ParamError::Encode { index, source })?;
    }
    Ok(args)
}

/// The binary wire value of `param` as type `ty`; `None` is SQL NULL.
fn to_wire(ty: &PgTypeInfo, param: PyParam, index: usize) -> Result<Option<Vec<u8>>, ParamError> {
    let out_of_range = |value: i64, pg_type: &'static str| ParamError::Value {
        index,
        problem: ValueProblem::OutOfRange { value, pg_type },
    };
    let numeric = |text: &str| {
        let mut wire = Vec::new();
        numeric::encode(text, &mut wire)
            .map(|()| wire)
            .map_err(|e| ParamError::Value {
                index,
                problem: e.into(),
            })
    };

    let wire = match (PgKind::of(ty), param) {
        (_, PyParam::Null) => return Ok(None),
        (PgKind::Bool, PyParam::Bool(b)) => vec![u8::from(b)],
        (PgKind::Int2, PyParam::Int(i)) => i16::try_from(i)
            .map_err(|_| out_of_range(i, "int2"))?
            .to_be_bytes()
            .to_vec(),
        (PgKind::Int4, PyParam::Int(i)) => i32::try_from(i)
            .map_err(|_| out_of_range(i, "int4"))?
            .to_be_bytes()
            .to_vec(),
        (PgKind::Int8, PyParam::Int(i)) => i.to_be_bytes().to_vec(),
        (PgKind::Float4, PyParam::Float(f)) => (f as f32).to_be_bytes().to_vec(),
        (PgKind::Float4, PyParam::Int(i)) => (i as f32).to_be_bytes().to_vec(),
        (PgKind::Float8, PyParam::Float(f)) => f.to_be_bytes().to_vec(),
        (PgKind::Float8, PyParam::Int(i)) => (i as f64).to_be_bytes().to_vec(),
        (PgKind::Numeric, PyParam::Int(i)) => numeric(&i.to_string())?,
        (PgKind::Numeric, PyParam::Float(f)) => numeric(&f.to_string())?,
        (PgKind::Numeric, PyParam::Decimal(text)) => numeric(&text)?,
        (PgKind::Text, PyParam::Text(s)) => s.into_bytes(),
        (PgKind::Bytea, PyParam::Bytes(b)) => b,
        (PgKind::Json, PyParam::Json(v)) => v.to_string().into_bytes(),
        (PgKind::Jsonb, PyParam::Json(v)) => [&[JSONB_VERSION], v.to_string().as_bytes()].concat(),
        (PgKind::Uuid, PyParam::Uuid(u)) => u.to_vec(),
        (PgKind::Date, PyParam::Date(days)) => days.to_be_bytes().to_vec(),
        (PgKind::Timestamp, PyParam::Timestamp(us)) => us.to_be_bytes().to_vec(),
        (PgKind::Timestamptz, PyParam::TimestampTz(us)) => us.to_be_bytes().to_vec(),
        // The counterpart of reading an `Other` column: bytes are its raw wire value.
        (PgKind::Other, PyParam::Bytes(b)) => b,
        (_, param) => {
            return Err(ParamError::Type {
                index,
                pg_type: ty.name().to_ascii_lowercase(),
                given: param.python_type(),
            })
        }
    };
    Ok(Some(wire))
}

/// A wire value together with the parameter type the statement declared for it.
struct PgParam {
    ty: PgTypeInfo,
    wire: Option<Vec<u8>>,
}

impl sqlx::Type<Postgres> for PgParam {
    /// Unspecified. Never consulted: `produces` always names the declared type.
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(0))
    }
}

impl sqlx::Encode<'_, Postgres> for PgParam {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        Ok(match &self.wire {
            None => IsNull::Yes,
            Some(bytes) => {
                buf.extend_from_slice(bytes);
                IsNull::No
            }
        })
    }

    fn produces(&self) -> Option<PgTypeInfo> {
        Some(self.ty.clone())
    }
}
