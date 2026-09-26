//! The closed set of Postgres types this layer converts to and from Python.

use sqlx::postgres::{PgTypeInfo, PgTypeKind};

/// JSONB's binary form is this version byte followed by the JSON text.
pub(crate) const JSONB_VERSION: u8 = 1;

/// Built-in type OIDs (`pg_type.dat`); stable across Postgres versions.
mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    /// Type of an untyped literal; sent in text form.
    pub const UNKNOWN: u32 = 705;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// How a value of a Postgres type crosses to Python. `Other` covers every type without a
/// Python counterpart here: its values travel as the raw binary wire bytes, never
/// reinterpreted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PgKind {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Bytea,
    Json,
    Jsonb,
    Uuid,
    Date,
    Timestamp,
    Timestamptz,
    Other,
}

impl PgKind {
    /// `ty` comes from a described statement, so user-defined types are resolved.
    pub(crate) fn of(ty: &PgTypeInfo) -> Self {
        let builtin = ty.oid().map_or(Self::Other, |oid| Self::from_oid(oid.0));
        if builtin != Self::Other {
            return builtin;
        }
        match ty.kind() {
            // An enum's binary form is its label; a domain's is its base type's.
            PgTypeKind::Enum(_) => Self::Text,
            PgTypeKind::Domain(base) => Self::of(base),
            _ => Self::Other,
        }
    }

    fn from_oid(oid: u32) -> Self {
        match oid {
            oid::BOOL => Self::Bool,
            oid::INT2 => Self::Int2,
            oid::INT4 => Self::Int4,
            oid::INT8 => Self::Int8,
            oid::FLOAT4 => Self::Float4,
            oid::FLOAT8 => Self::Float8,
            oid::NUMERIC => Self::Numeric,
            oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::UNKNOWN => Self::Text,
            oid::BYTEA => Self::Bytea,
            oid::JSON => Self::Json,
            oid::JSONB => Self::Jsonb,
            oid::UUID => Self::Uuid,
            oid::DATE => Self::Date,
            oid::TIMESTAMP => Self::Timestamp,
            oid::TIMESTAMPTZ => Self::Timestamptz,
            _ => Self::Other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::types::Oid;

    #[test]
    fn builtin_oids_map_to_their_kind() {
        let kind = |o| PgKind::of(&PgTypeInfo::with_oid(Oid(o)));
        assert_eq!(kind(oid::INT4), PgKind::Int4);
        assert_eq!(kind(oid::VARCHAR), PgKind::Text);
        assert_eq!(kind(oid::UUID), PgKind::Uuid);
        assert_eq!(kind(oid::NUMERIC), PgKind::Numeric);
    }

    #[test]
    fn a_builtin_type_without_a_python_counterpart_is_other() {
        const MACADDR: u32 = 829;
        const INT4_ARRAY: u32 = 1007;
        assert_eq!(PgKind::from_oid(MACADDR), PgKind::Other);
        assert_eq!(PgKind::from_oid(INT4_ARRAY), PgKind::Other);
    }
}
