//! Python arguments → Postgres parameters, encoded as the statement declares them.
//!
//! sqlx caches a prepared statement per SQL text, with the parameter types of its first
//! execution. Choosing each parameter's type from the Python value would make the cached
//! types depend on whichever values came first, and later values of another type would be
//! sent in the wrong binary form. So the statement is prepared untyped (the server infers
//! every parameter from the SQL), and each value is encoded as the type the server
//! declared: `None` becomes a NULL of that type, `5` an int4 or a numeric as needed.

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
    /// An int past i64, as decimal text: it can still be a NUMERIC.
    BigInt(String),
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
    /// Copies parameter `index` (1-based, as `$n`) out of Python.
    pub(crate) fn extract(obj: &Bound<'_, PyAny>, index: usize) -> Result<Self, ParamError> {
        let unreadable = |source| ParamError::Unreadable { index, source };
        if obj.is_none() {
            return Ok(Self::Null);
        }
        // PyBool is a subclass of PyInt: check bool first.
        if let Ok(b) = obj.cast::<PyBool>() {
            return Ok(Self::Bool(b.is_true()));
        }
        if let Ok(i) = obj.cast::<PyInt>() {
            return Ok(match i.extract::<i64>() {
                Ok(small) => Self::Int(small),
                // Only an overflow fails here: past i64 it travels as its decimal text.
                Err(_) => Self::BigInt(i.str().map_err(unreadable)?.to_string()),
            });
        }
        if let Ok(f) = obj.cast::<PyFloat>() {
            return Ok(Self::Float(f.value()));
        }
        if let Ok(s) = obj.cast::<PyString>() {
            return Ok(Self::Text(s.to_str().map_err(unreadable)?.to_owned()));
        }
        if let Ok(b) = obj.cast::<PyBytes>() {
            return Ok(Self::Bytes(b.as_bytes().to_vec()));
        }
        if obj.is_instance_of::<PyDict>() || obj.is_instance_of::<PyList>() {
            let value = pythonize::depythonize(obj).map_err(|e| unreadable(e.into()))?;
            return Ok(Self::Json(value));
        }
        Self::extract_stdlib(obj)
            .map_err(unreadable)?
            .ok_or_else(|| ParamError::Unsupported {
                index,
                python_type: obj.get_type().name().map_or_else(
                    |e| format!("<type name unavailable: {e}>"),
                    |n| n.to_string(),
                ),
            })
    }

    /// `datetime`, `date`, `uuid.UUID`, `decimal.Decimal`; `None` for any other type.
    fn extract_stdlib(obj: &Bound<'_, PyAny>) -> PyResult<Option<Self>> {
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
            return Ok(Some(if naive {
                Self::Timestamp(micros)
            } else {
                Self::TimestampTz(micros)
            }));
        }
        if obj.is_instance(types.date.bind(py))? {
            let ordinal: i32 = obj.call_method0("toordinal")?.extract()?;
            return Ok(Some(Self::Date(ordinal - PG_EPOCH_ORDINAL)));
        }
        if obj.is_instance(types.uuid.bind(py))? {
            let bytes: [u8; 16] = obj.getattr("bytes")?.extract()?;
            return Ok(Some(Self::Uuid(bytes)));
        }
        if obj.is_instance(types.decimal.bind(py))? {
            return Ok(Some(Self::Decimal(
                obj.call_method1("__format__", ("f",))?.extract()?,
            )));
        }
        Ok(None)
    }

    fn python_type(&self) -> &'static str {
        match self {
            Self::Null => "None",
            Self::Bool(_) => "bool",
            Self::Int(_) | Self::BigInt(_) => "int",
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
    let out_of_range = |value: &dyn std::fmt::Display, pg_type: &'static str| ParamError::Value {
        index,
        problem: ValueProblem::OutOfRange {
            value: value.to_string(),
            pg_type,
        },
    };
    // Narrowing a finite float8 to float4 must not overflow to infinity.
    let float4 = |f: f64| {
        let narrowed = f as f32;
        if narrowed.is_infinite() && f.is_finite() {
            Err(out_of_range(&f, "float4"))
        } else {
            Ok(narrowed.to_be_bytes().to_vec())
        }
    };
    let big_float = |text: &str| {
        text.parse::<f64>()
            .map_err(|_| out_of_range(&text, "float8"))
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
            .map_err(|_| out_of_range(&i, "int2"))?
            .to_be_bytes()
            .to_vec(),
        (PgKind::Int4, PyParam::Int(i)) => i32::try_from(i)
            .map_err(|_| out_of_range(&i, "int4"))?
            .to_be_bytes()
            .to_vec(),
        (PgKind::Int8, PyParam::Int(i)) => i.to_be_bytes().to_vec(),
        (PgKind::Int2, PyParam::BigInt(text)) => return Err(out_of_range(&text, "int2")),
        (PgKind::Int4, PyParam::BigInt(text)) => return Err(out_of_range(&text, "int4")),
        (PgKind::Int8, PyParam::BigInt(text)) => return Err(out_of_range(&text, "int8")),
        (PgKind::Float4, PyParam::Float(f)) => float4(f)?,
        (PgKind::Float4, PyParam::Int(i)) => (i as f32).to_be_bytes().to_vec(),
        (PgKind::Float4, PyParam::BigInt(text)) => float4(big_float(&text)?)?,
        (PgKind::Float8, PyParam::Float(f)) => f.to_be_bytes().to_vec(),
        (PgKind::Float8, PyParam::Int(i)) => (i as f64).to_be_bytes().to_vec(),
        (PgKind::Float8, PyParam::BigInt(text)) => big_float(&text)?.to_be_bytes().to_vec(),
        (PgKind::Numeric, PyParam::Int(i)) => numeric(&i.to_string())?,
        (PgKind::Numeric, PyParam::BigInt(text)) => numeric(&text)?,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(oid: u32, param: PyParam) -> Result<Option<Vec<u8>>, ParamError> {
        to_wire(&PgTypeInfo::with_oid(Oid(oid)), param, 1)
    }

    const INT8: u32 = 20;
    const FLOAT4: u32 = 700;
    const FLOAT8: u32 = 701;
    const NUMERIC: u32 = 1700;

    #[test]
    fn a_big_int_is_a_numeric() {
        let big = "123456789012345678901234567890".to_string();
        let mut expected = Vec::new();
        numeric::encode(&big, &mut expected).unwrap();
        assert_eq!(wire(NUMERIC, PyParam::BigInt(big)).unwrap(), Some(expected));
    }

    #[test]
    fn a_big_int_is_out_of_range_for_int8() {
        let err = wire(INT8, PyParam::BigInt("9223372036854775808".into())).unwrap_err();
        assert_eq!(
            err.to_string(),
            "parameter $1: 9223372036854775808 is out of range for int8"
        );
    }

    #[test]
    fn float4_narrowing_to_infinity_is_out_of_range() {
        let err = wire(FLOAT4, PyParam::Float(1e300)).unwrap_err();
        assert!(err.to_string().contains("out of range for float4"), "{err}");
        // Infinity itself is a float4 value, as are the ones that fit.
        assert!(wire(FLOAT4, PyParam::Float(f64::INFINITY)).is_ok());
        assert_eq!(
            wire(FLOAT4, PyParam::Float(1.5)).unwrap(),
            Some(1.5f32.to_be_bytes().to_vec())
        );
        let big = "1".repeat(60);
        assert!(wire(FLOAT4, PyParam::BigInt(big.clone())).is_err());
        assert!(wire(FLOAT8, PyParam::BigInt(big)).is_ok());
    }
}
