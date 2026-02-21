use crate::row::{Row, RowKind};
use serde::ser::{SerializeStruct, Serializer};
use std::sync::Arc;

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
            // None → nullable null
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
