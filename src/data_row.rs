use crate::row::{Row, RowKind};
use serde::ser::{SerializeStruct, Serializer};
use std::sync::Arc;

// ── TypeError ─────────────────────────────────────────────────────────────────

/// Error returned by [`DataRow::try_get`].
#[cfg_attr(docsrs, doc(cfg(feature = "sea-ql")))]
#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    /// A column name passed to `try_get` does not exist in this row.
    #[error("column '{0}' not found")]
    ColumnNotFound(String),
    /// A column index passed to `try_get` is out of bounds.
    #[error("column index {0} is out of bounds")]
    IndexOutOfBounds(usize),
    /// The column value is `NULL` but the target type is not `Option<T>`.
    #[error("column value is NULL")]
    UnexpectedNull,
    /// The stored `Value` variant cannot be converted to the requested type.
    #[error("cannot convert {got} to {expected}")]
    TypeMismatch {
        expected: &'static str,
        got: &'static str,
    },
    /// The stored numeric value is outside the representable range of the target type.
    #[error("value out of range for {0}")]
    OutOfRange(&'static str),
}

// ── ColumnIndex ────────────────────────────────────────────────────────────────

mod col_index_sealed {
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for &str {}
}

/// Types that can index into a [`DataRow`] column: `usize` (position) or `&str` (name).
#[cfg_attr(docsrs, doc(cfg(feature = "sea-ql")))]
pub trait ColumnIndex: col_index_sealed::Sealed {
    #[doc(hidden)]
    fn get_index(&self, row: &DataRow) -> Result<usize, TypeError>;
}

impl ColumnIndex for usize {
    fn get_index(&self, row: &DataRow) -> Result<usize, TypeError> {
        if *self < row.values.len() {
            Ok(*self)
        } else {
            Err(TypeError::IndexOutOfBounds(*self))
        }
    }
}

impl ColumnIndex for &str {
    fn get_index(&self, row: &DataRow) -> Result<usize, TypeError> {
        row.columns
            .iter()
            .position(|c| c.as_ref() == *self)
            .ok_or_else(|| TypeError::ColumnNotFound(self.to_string()))
    }
}

// ── FromValue trait ────────────────────────────────────────────────────────────

/// Extract a concrete Rust value from a [`sea_query::Value`].
///
/// Implemented for standard numeric and string types with flexible cross-type
/// numeric conversions. Implement for custom types to use with [`DataRow::try_get`].
#[cfg_attr(docsrs, doc(cfg(feature = "sea-ql")))]
pub trait FromValue: Sized {
    /// Convert a [`sea_query::Value`] reference into `Self`.
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError>;
}

// ── Private helpers ────────────────────────────────────────────────────────────

/// Outcome of converting a [`sea_query::Value`] to `i128`.
enum NumericI128 {
    Got(i128),
    /// Numeric type but the value doesn't fit in `i128` (e.g. very large float).
    Overflow,
    /// The variant is not a numeric type.
    NotNumeric,
}

fn try_as_i128(val: &sea_query::Value) -> NumericI128 {
    use sea_query::Value as V;
    match val {
        V::Bool(Some(b)) => NumericI128::Got(*b as i128),
        V::TinyInt(Some(v)) => NumericI128::Got(*v as i128),
        V::SmallInt(Some(v)) => NumericI128::Got(*v as i128),
        V::Int(Some(v)) => NumericI128::Got(*v as i128),
        V::BigInt(Some(v)) => NumericI128::Got(*v as i128),
        V::TinyUnsigned(Some(v)) => NumericI128::Got(*v as i128),
        V::SmallUnsigned(Some(v)) => NumericI128::Got(*v as i128),
        V::Unsigned(Some(v)) => NumericI128::Got(*v as i128),
        V::BigUnsigned(Some(v)) => NumericI128::Got(*v as i128),
        V::Float(Some(f)) => f64_truncate_to_i128(*f as f64),
        V::Double(Some(f)) => f64_truncate_to_i128(*f),
        V::Decimal(Some(d)) => {
            use sea_query::prelude::rust_decimal::prelude::ToPrimitive;
            match d.to_i128() {
                Some(i) => NumericI128::Got(i),
                None => NumericI128::Overflow,
            }
        }
        V::BigDecimal(Some(d)) => {
            use sea_query::prelude::bigdecimal::ToPrimitive;
            match d.to_i128() {
                Some(i) => NumericI128::Got(i),
                None => NumericI128::Overflow,
            }
        }
        _ => NumericI128::NotNumeric,
    }
}

fn try_as_f64(val: &sea_query::Value) -> Option<f64> {
    use sea_query::Value as V;
    match val {
        V::Bool(Some(b)) => Some(*b as u8 as f64),
        V::TinyInt(Some(v)) => Some(*v as f64),
        V::SmallInt(Some(v)) => Some(*v as f64),
        V::Int(Some(v)) => Some(*v as f64),
        V::BigInt(Some(v)) => Some(*v as f64),
        V::TinyUnsigned(Some(v)) => Some(*v as f64),
        V::SmallUnsigned(Some(v)) => Some(*v as f64),
        V::Unsigned(Some(v)) => Some(*v as f64),
        V::BigUnsigned(Some(v)) => Some(*v as f64),
        V::Float(Some(f)) => Some(*f as f64),
        V::Double(Some(f)) => Some(*f),
        V::Decimal(Some(d)) => {
            use sea_query::prelude::rust_decimal::prelude::ToPrimitive;
            d.to_f64()
        }
        V::BigDecimal(Some(d)) => {
            use sea_query::prelude::bigdecimal::ToPrimitive;
            d.to_f64()
        }
        _ => None,
    }
}

/// Truncate a finite `f64` to `i128`, returning `Overflow` if out of range.
fn f64_truncate_to_i128(f: f64) -> NumericI128 {
    if !f.is_finite() {
        return NumericI128::Overflow;
    }
    // i128::MIN = -2^127 is exactly representable as f64 (power of two).
    // -(i128::MIN as f64) = 2^127, which is i128::MAX + 1, so strict < gives the right bound.
    const MIN: f64 = i128::MIN as f64;
    const MAX_EXCL: f64 = -(i128::MIN as f64);
    if f >= MIN && f < MAX_EXCL {
        NumericI128::Got(f as i128)
    } else {
        NumericI128::Overflow
    }
}

/// Returns `true` if `val` is a NULL variant of any type.
fn is_null(val: &sea_query::Value) -> bool {
    use sea_query::Value as V;
    matches!(
        val,
        V::Bool(None)
            | V::TinyInt(None)
            | V::SmallInt(None)
            | V::Int(None)
            | V::BigInt(None)
            | V::TinyUnsigned(None)
            | V::SmallUnsigned(None)
            | V::Unsigned(None)
            | V::BigUnsigned(None)
            | V::Float(None)
            | V::Double(None)
            | V::String(None)
            | V::Char(None)
            | V::Bytes(None)
            | V::BigDecimal(None)
            | V::Decimal(None)
            | V::ChronoDate(None)
            | V::ChronoDateTime(None)
            | V::ChronoTime(None)
            | V::Uuid(None)
            | V::Json(None)
    )
}

/// Human-readable name of the [`sea_query::Value`] variant (for error messages).
fn value_variant_name(val: &sea_query::Value) -> &'static str {
    use sea_query::Value as V;
    match val {
        V::Bool(_) => "Bool",
        V::TinyInt(_) => "TinyInt (i8)",
        V::SmallInt(_) => "SmallInt (i16)",
        V::Int(_) => "Int (i32)",
        V::BigInt(_) => "BigInt (i64)",
        V::TinyUnsigned(_) => "TinyUnsigned (u8)",
        V::SmallUnsigned(_) => "SmallUnsigned (u16)",
        V::Unsigned(_) => "Unsigned (u32)",
        V::BigUnsigned(_) => "BigUnsigned (u64)",
        V::Float(_) => "Float (f32)",
        V::Double(_) => "Double (f64)",
        V::String(_) => "String",
        V::Char(_) => "Char",
        V::Bytes(_) => "Bytes",
        V::Json(_) => "Json",
        V::Decimal(_) => "Decimal",
        V::BigDecimal(_) => "BigDecimal",
        V::Uuid(_) => "Uuid",
        V::ChronoDate(_) => "ChronoDate",
        V::ChronoDateTime(_) => "ChronoDateTime",
        V::ChronoTime(_) => "ChronoTime",
        _ => "unknown",
    }
}

// ── FromValue implementations ──────────────────────────────────────────────────

impl FromValue for bool {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        use sea_query::Value as V;
        if is_null(val) {
            return Err(TypeError::UnexpectedNull);
        }
        match val {
            V::Bool(Some(b)) => Ok(*b),
            _ => match try_as_i128(val) {
                NumericI128::Got(0) => Ok(false),
                NumericI128::Got(1) => Ok(true),
                NumericI128::Got(_) | NumericI128::Overflow => Err(TypeError::OutOfRange("bool")),
                NumericI128::NotNumeric => Err(TypeError::TypeMismatch {
                    expected: "bool",
                    got: value_variant_name(val),
                }),
            },
        }
    }
}

/// Implements `FromValue` for integer types via `i128` as an intermediate.
macro_rules! impl_from_value_int {
    ($t:ty) => {
        impl FromValue for $t {
            fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
                if is_null(val) {
                    return Err(TypeError::UnexpectedNull);
                }
                match try_as_i128(val) {
                    NumericI128::Got(n) => {
                        <$t>::try_from(n).map_err(|_| TypeError::OutOfRange(stringify!($t)))
                    }
                    NumericI128::Overflow => Err(TypeError::OutOfRange(stringify!($t))),
                    NumericI128::NotNumeric => Err(TypeError::TypeMismatch {
                        expected: stringify!($t),
                        got: value_variant_name(val),
                    }),
                }
            }
        }
    };
}

impl_from_value_int!(i8);
impl_from_value_int!(i16);
impl_from_value_int!(i32);
impl_from_value_int!(i64);
impl_from_value_int!(i128);
impl_from_value_int!(u8);
impl_from_value_int!(u16);
impl_from_value_int!(u32);
impl_from_value_int!(u64);
impl_from_value_int!(u128);

impl FromValue for f64 {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        if is_null(val) {
            return Err(TypeError::UnexpectedNull);
        }
        try_as_f64(val).ok_or_else(|| TypeError::TypeMismatch {
            expected: "f64",
            got: value_variant_name(val),
        })
    }
}

impl FromValue for f32 {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        use sea_query::Value as V;
        if is_null(val) {
            return Err(TypeError::UnexpectedNull);
        }
        // Prefer the native f32 to avoid precision changes for Float columns.
        if let V::Float(Some(f)) = val {
            return Ok(*f);
        }
        try_as_f64(val)
            .map(|f| f as f32)
            .ok_or_else(|| TypeError::TypeMismatch {
                expected: "f32",
                got: value_variant_name(val),
            })
    }
}

impl FromValue for String {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        use sea_query::Value as V;
        if is_null(val) {
            return Err(TypeError::UnexpectedNull);
        }
        match val {
            V::String(Some(s)) => Ok(s.clone()),
            V::Char(Some(c)) => Ok(c.to_string()),
            _ => Err(TypeError::TypeMismatch {
                expected: "String",
                got: value_variant_name(val),
            }),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        use sea_query::Value as V;
        if is_null(val) {
            return Err(TypeError::UnexpectedNull);
        }
        match val {
            V::Bytes(Some(b)) => Ok(b.clone()),
            _ => Err(TypeError::TypeMismatch {
                expected: "Vec<u8>",
                got: value_variant_name(val),
            }),
        }
    }
}

impl FromValue for sea_query::value::prelude::Decimal {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        use sea_query::Value as V;
        use sea_query::value::prelude::Decimal;
        if is_null(val) {
            return Err(TypeError::UnexpectedNull);
        }
        match val {
            V::Decimal(Some(d)) => return Ok(*d),
            V::BigDecimal(Some(bd)) => {
                use std::str::FromStr;
                return Decimal::from_str(&bd.to_string())
                    .map_err(|_| TypeError::OutOfRange("Decimal"));
            }
            V::Float(Some(f)) => {
                return Decimal::try_from(*f as f64).map_err(|_| TypeError::OutOfRange("Decimal"));
            }
            V::Double(Some(f)) => {
                return Decimal::try_from(*f).map_err(|_| TypeError::OutOfRange("Decimal"));
            }
            _ => {}
        }
        // Integer / Bool variants
        match try_as_i128(val) {
            NumericI128::Got(n) => {
                use std::str::FromStr;
                Decimal::from_str(&n.to_string()).map_err(|_| TypeError::OutOfRange("Decimal"))
            }
            NumericI128::Overflow => Err(TypeError::OutOfRange("Decimal")),
            NumericI128::NotNumeric => Err(TypeError::TypeMismatch {
                expected: "Decimal",
                got: value_variant_name(val),
            }),
        }
    }
}

impl FromValue for sea_query::value::prelude::BigDecimal {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        use sea_query::Value as V;
        use sea_query::value::prelude::BigDecimal;
        if is_null(val) {
            return Err(TypeError::UnexpectedNull);
        }
        match val {
            V::BigDecimal(Some(bd)) => return Ok((**bd).clone()),
            V::Decimal(Some(d)) => {
                use std::str::FromStr;
                return BigDecimal::from_str(&d.to_string())
                    .map_err(|_| TypeError::OutOfRange("BigDecimal"));
            }
            V::Float(Some(f)) => {
                let f = *f as f64;
                if !f.is_finite() {
                    return Err(TypeError::OutOfRange("BigDecimal"));
                }
                use std::str::FromStr;
                return BigDecimal::from_str(&f.to_string())
                    .map_err(|_| TypeError::OutOfRange("BigDecimal"));
            }
            V::Double(Some(f)) => {
                if !f.is_finite() {
                    return Err(TypeError::OutOfRange("BigDecimal"));
                }
                use std::str::FromStr;
                return BigDecimal::from_str(&f.to_string())
                    .map_err(|_| TypeError::OutOfRange("BigDecimal"));
            }
            _ => {}
        }
        // Integer / Bool variants
        match try_as_i128(val) {
            NumericI128::Got(n) => {
                use sea_query::prelude::bigdecimal::num_bigint::BigInt;
                Ok(BigDecimal::new(BigInt::from(n), 0))
            }
            NumericI128::Overflow => Err(TypeError::OutOfRange("BigDecimal")),
            NumericI128::NotNumeric => Err(TypeError::TypeMismatch {
                expected: "BigDecimal",
                got: value_variant_name(val),
            }),
        }
    }
}

impl FromValue for sea_query::value::prelude::Uuid {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        use sea_query::Value as V;
        if is_null(val) {
            return Err(TypeError::UnexpectedNull);
        }
        match val {
            V::Uuid(Some(u)) => Ok(*u),
            _ => Err(TypeError::TypeMismatch {
                expected: "Uuid",
                got: value_variant_name(val),
            }),
        }
    }
}

impl FromValue for sea_query::prelude::serde_json::Value {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        use sea_query::Value as V;
        if is_null(val) {
            return Err(TypeError::UnexpectedNull);
        }
        match val {
            V::Json(Some(j)) => Ok((**j).clone()),
            _ => Err(TypeError::TypeMismatch {
                expected: "serde_json::Value",
                got: value_variant_name(val),
            }),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(val: &sea_query::Value) -> Result<Self, TypeError> {
        if is_null(val) {
            return Ok(None);
        }
        T::from_value(val).map(Some)
    }
}

/// A dynamically-typed row returned by [`crate::query::DataRowCursor`].
///
/// Column names are shared across all rows from the same cursor via [`Arc`],
/// making each row cheap to construct (one `Arc` clone + one `Vec` alloc).
///
/// # Example
///
/// ```ignore
/// let mut cursor = client
///     .query("SELECT number, toString(number) AS s FROM system.numbers LIMIT 3")
///     .fetch_rows()?;
///
/// while let Some(row) = cursor.next().await? {
///     for (col, val) in row.columns.iter().zip(&row.values) {
///         println!("{col}: {val:?}");
///     }
/// }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "sea-ql")))]
#[derive(Debug, Clone, PartialEq)]
pub struct DataRow {
    /// Column names in schema order, shared with all other rows from the same query.
    pub columns: Arc<[Arc<str>]>,
    /// Per-column values decoded from `RowBinaryWithNamesAndTypes`.
    pub values: Vec<sea_query::Value>,
}

impl DataRow {
    /// Extract column `idx` as type `T`.
    ///
    /// `idx` can be a column name (`&str`) or a zero-based index (`usize`).
    ///
    /// Numeric conversions are flexible: any integer or float column can be decoded
    /// into any compatible numeric type, with range-checked narrowing. Fractional
    /// floats are truncated when converting to integers. `Option<T>` decodes `NULL`
    /// as `None` instead of returning an error.
    ///
    /// # Errors
    ///
    /// Returns [`TypeError`] if the column is not found, the value is `NULL` (for
    /// non-`Option` targets), or the conversion is not possible / out of range.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let id: i64 = row.try_get("id")?;
    /// let name: String = row.try_get("name")?;
    /// let score: Option<f64> = row.try_get(2)?;
    /// ```
    pub fn try_get<T, I>(&self, idx: I) -> Result<T, TypeError>
    where
        T: FromValue,
        I: ColumnIndex,
    {
        let i = idx.get_index(self)?;
        T::from_value(&self.values[i])
    }
}

/// This is only to satisfy the trait bounds of insert.
impl Row for DataRow {
    const NAME: &'static str = "DataRow";
    /// Column names are dynamic; this is always empty.
    /// Use [`crate::Client::insert_data_row`] which reads names from the instance.
    const COLUMN_NAMES: &'static [&'static str] = &[];
    const COLUMN_COUNT: usize = 0;
    const KIND: RowKind = RowKind::Struct;
    type Value<'a> = DataRow;
}

impl serde::Serialize for DataRow {
    /// Serializes as a struct, emitting each value in column order.
    ///
    /// `None` values are serialized as `serialize_none()` (nullable NULL).
    /// `Some(v)` values are serialized as the inner primitive (non-nullable style).
    ///
    /// **Limitation**: nullable columns with non-null values will be missing the null
    /// flag byte. Use [`crate::Client::insert_data_row`] + [`crate::insert::DataRowInsert::write_row`]
    /// for correct handling of all nullable columns.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("DataRow", self.values.len())?;
        for val in &self.values {
            state.serialize_field("_", &SeaValueSer(val))?;
        }
        state.end()
    }
}

/// Newtype wrapper that maps a [`sea_query::Value`] to the correct serde call
/// for the `RowBinary` serializer.
struct SeaValueSer<'a>(&'a sea_query::Value);

impl serde::Serialize for SeaValueSer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use sea_query::Value;
        match self.0 {
            // None -> nullable null
            Value::Bool(None)
            | Value::TinyInt(None)
            | Value::SmallInt(None)
            | Value::Int(None)
            | Value::BigInt(None)
            | Value::TinyUnsigned(None)
            | Value::SmallUnsigned(None)
            | Value::Unsigned(None)
            | Value::BigUnsigned(None)
            | Value::Float(None)
            | Value::Double(None)
            | Value::String(None)
            | Value::Char(None)
            | Value::Bytes(None)
            | Value::Json(None) => serializer.serialize_none(),

            // Primitives
            Value::Bool(Some(v)) => serializer.serialize_bool(*v),
            Value::TinyInt(Some(v)) => serializer.serialize_i8(*v),
            Value::SmallInt(Some(v)) => serializer.serialize_i16(*v),
            Value::Int(Some(v)) => serializer.serialize_i32(*v),
            Value::BigInt(Some(v)) => serializer.serialize_i64(*v),
            Value::TinyUnsigned(Some(v)) => serializer.serialize_u8(*v),
            Value::SmallUnsigned(Some(v)) => serializer.serialize_u16(*v),
            Value::Unsigned(Some(v)) => serializer.serialize_u32(*v),
            Value::BigUnsigned(Some(v)) => serializer.serialize_u64(*v),
            Value::Float(Some(v)) => serializer.serialize_f32(*v),
            Value::Double(Some(v)) => serializer.serialize_f64(*v),
            Value::String(Some(s)) => serializer.serialize_str(s),
            Value::Bytes(Some(b)) => serializer.serialize_bytes(b),
            Value::Json(Some(j)) => {
                let s = j.to_string();
                serializer.serialize_str(&s)
            }

            other => Err(serde::ser::Error::custom(format!(
                "Cannot serialize {other:?} via serde; \
                 use Client::insert_data_row for complex types (Date, UUID, Decimal, …)"
            ))),
        }
    }
}

/// A column-oriented batch of dynamically-typed rows.
///
/// Instead of one [`sea_query::Value`] vector per row (as in [`DataRow`]),
/// `RowBatch` stores one `Vec<Value>` per column. This layout is efficient
/// for columnar processing and is the natural precursor to Apache Arrow
/// record batches.
///
/// All `column_data` vectors have exactly `num_rows` entries.
///
/// Obtain batches via [`crate::query::DataRowCursor::next_batch`].
///
/// # Example
///
/// ```ignore
/// let mut cursor = client
///     .query("SELECT number, toString(number) AS s FROM system.numbers LIMIT 100")
///     .fetch_rows()?;
///
/// while let Some(batch) = cursor.next_batch(32).await? {
///     println!("{} rows, {} columns", batch.num_rows, batch.columns.len());
///     for (name, col) in batch.columns.iter().zip(&batch.column_data) {
///         println!("  {name}: {} values", col.len());
///     }
/// }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "sea-ql")))]
#[derive(Debug, Clone, PartialEq)]
pub struct RowBatch {
    /// Column names in schema order, shared with all other batches from the same query.
    pub columns: Arc<[Arc<str>]>,
    /// Per-column value vectors; `column_data[i]` holds all values for `columns[i]`.
    ///
    /// Every inner `Vec` has exactly `num_rows` entries.
    pub column_data: Vec<Vec<sea_query::Value>>,
    /// Number of rows in this batch.
    pub num_rows: usize,
}
