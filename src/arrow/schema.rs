use std::sync::Arc;

use arrow::datatypes::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use clickhouse_types::DataTypeNode;
use clickhouse_types::data_types::{
    Column, DateTimePrecision, DecimalType, EnumType, IntervalType,
};
use sea_orm_arrow::arrow;

/// Builds an Arrow [`Schema`] from a slice of ClickHouse [`Column`] definitions.
///
/// ## Type mapping notes
///
/// | ClickHouse | Arrow |
/// |---|---|
/// | `UInt128` / `UInt256` | `FixedSizeBinary(16/32)` – no native unsigned wide-int in Arrow |
/// | `Int128` / `Int256` | `Decimal128(38,0)` / `Decimal256(76,0)` |
/// | `BFloat16` | `Float16` – closest half-float available |
/// | `UUID` | `FixedSizeBinary(16)` |
/// | `IPv4` | `UInt32` |
/// | `IPv6` | `FixedSizeBinary(16)` |
/// | `Nullable(T)` | same as `T` – nullability lives on [`Field`], not [`DataType`] |
/// | `LowCardinality(T)` | same as `T` – storage optimisation only |
/// | `AggregateFunction` | `Binary` – opaque state bytes |
/// | `Dynamic` / `JSON` | `LargeUtf8` |
pub fn from_columns(columns: &[Column]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let nullable = matches!(col.data_type, DataTypeNode::Nullable(_));
            Field::new(&col.name, data_type_to_arrow(&col.data_type), nullable)
        })
        .collect();
    Schema::new(fields)
}

/// Converts a ClickHouse [`DataTypeNode`] to an Apache Arrow [`DataType`].
fn data_type_to_arrow(node: &DataTypeNode) -> DataType {
    match node {
        DataTypeNode::Bool => DataType::Boolean,

        DataTypeNode::UInt8 => DataType::UInt8,
        DataTypeNode::UInt16 => DataType::UInt16,
        DataTypeNode::UInt32 => DataType::UInt32,
        DataTypeNode::UInt64 => DataType::UInt64,
        // Arrow has no native u128/u256; FixedSizeBinary preserves all bits.
        DataTypeNode::UInt128 => DataType::FixedSizeBinary(16),
        DataTypeNode::UInt256 => DataType::FixedSizeBinary(32),

        DataTypeNode::Int8 => DataType::Int8,
        DataTypeNode::Int16 => DataType::Int16,
        DataTypeNode::Int32 => DataType::Int32,
        DataTypeNode::Int64 => DataType::Int64,
        // Signed 128/256-bit integers map to signed Arrow Decimal types.
        DataTypeNode::Int128 => DataType::Decimal128(38, 0),
        DataTypeNode::Int256 => DataType::Decimal256(76, 0),

        DataTypeNode::Float32 => DataType::Float32,
        DataTypeNode::Float64 => DataType::Float64,
        // BFloat16 has no native Arrow equivalent; Float16 is the closest.
        DataTypeNode::BFloat16 => DataType::Float16,

        DataTypeNode::Decimal(precision, scale, decimal_type) => {
            let (p, s) = (*precision, *scale as i8);
            match decimal_type {
                DecimalType::Decimal32 => DataType::Decimal32(p, s),
                DecimalType::Decimal64 => DataType::Decimal64(p, s),
                DecimalType::Decimal128 => DataType::Decimal128(p, s),
                DecimalType::Decimal256 => DataType::Decimal256(p, s),
            }
        }

        DataTypeNode::String => DataType::Utf8,
        DataTypeNode::FixedString(size) => DataType::FixedSizeBinary(*size as i32),
        // UUID is a 128-bit value stored as 16 raw bytes.
        DataTypeNode::UUID => DataType::FixedSizeBinary(16),

        DataTypeNode::Date => DataType::Date32,
        DataTypeNode::Date32 => DataType::Date32,

        DataTypeNode::DateTime(tz) => {
            DataType::Timestamp(TimeUnit::Second, tz.as_deref().map(Arc::from))
        }
        DataTypeNode::DateTime64(precision, tz) => DataType::Timestamp(
            precision_to_time_unit(precision),
            tz.as_deref().map(Arc::from),
        ),

        DataTypeNode::Time => DataType::Time32(TimeUnit::Second),
        DataTypeNode::Time64(precision) => precision_to_time_type(precision),

        DataTypeNode::Interval(interval_type) => match interval_type {
            IntervalType::Nanosecond => DataType::Duration(TimeUnit::Nanosecond),
            IntervalType::Microsecond => DataType::Duration(TimeUnit::Microsecond),
            IntervalType::Millisecond => DataType::Duration(TimeUnit::Millisecond),
            IntervalType::Second | IntervalType::Minute | IntervalType::Hour => {
                DataType::Duration(TimeUnit::Second)
            }
            IntervalType::Day | IntervalType::Week => DataType::Interval(IntervalUnit::DayTime),
            IntervalType::Month | IntervalType::Quarter | IntervalType::Year => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
        },

        DataTypeNode::IPv4 => DataType::UInt32,
        // IPv6 is a 128-bit address stored as 16 raw bytes.
        DataTypeNode::IPv6 => DataType::FixedSizeBinary(16),

        // Nullability is expressed at the Field level, not in DataType.
        DataTypeNode::Nullable(inner) => data_type_to_arrow(inner),
        // LowCardinality is a storage optimisation; the logical value type is unchanged.
        DataTypeNode::LowCardinality(inner) => data_type_to_arrow(inner),

        DataTypeNode::Array(inner) => {
            let item = Arc::new(Field::new("item", data_type_to_arrow(inner), true));
            DataType::List(item)
        }

        DataTypeNode::Tuple(elements) => {
            let fields: Fields = elements
                .iter()
                .enumerate()
                .map(|(i, t)| Arc::new(Field::new(i.to_string(), data_type_to_arrow(t), true)))
                .collect();
            DataType::Struct(fields)
        }

        DataTypeNode::Map([key, value]) => {
            let key_field = Arc::new(Field::new("key", data_type_to_arrow(key), false));
            let value_field = Arc::new(Field::new("value", data_type_to_arrow(value), true));
            let entries = Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![key_field, value_field])),
                false,
            ));
            DataType::Map(entries, false)
        }

        DataTypeNode::Enum(EnumType::Enum8, _) => DataType::Int8,
        DataTypeNode::Enum(EnumType::Enum16, _) => DataType::Int16,

        // Aggregate function states are opaque binary blobs.
        DataTypeNode::AggregateFunction(_, _) => DataType::Binary,

        DataTypeNode::Variant(types) => {
            let (type_ids, fields): (Vec<i8>, Vec<Arc<Field>>) = types
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    (
                        i as i8,
                        Arc::new(Field::new(i.to_string(), data_type_to_arrow(t), true)),
                    )
                })
                .unzip();
            DataType::Union(
                UnionFields::try_new(type_ids, fields).expect("valid union fields"),
                UnionMode::Dense,
            )
        }

        DataTypeNode::Dynamic | DataTypeNode::JSON => DataType::LargeUtf8,

        // Geo types – ClickHouse stores coordinates as pairs of Float64 values.
        DataTypeNode::Point => point_type(),
        DataTypeNode::Ring | DataTypeNode::LineString => {
            DataType::List(Arc::new(Field::new("item", point_type(), false)))
        }
        DataTypeNode::Polygon | DataTypeNode::MultiLineString => {
            let ring = DataType::List(Arc::new(Field::new("item", point_type(), false)));
            DataType::List(Arc::new(Field::new("item", ring, false)))
        }
        DataTypeNode::MultiPolygon => {
            let ring = DataType::List(Arc::new(Field::new("item", point_type(), false)));
            let polygon = DataType::List(Arc::new(Field::new("item", ring, false)));
            DataType::List(Arc::new(Field::new("item", polygon, false)))
        }

        // Fallback for any future non-exhaustive variants.
        _ => DataType::Binary,
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Maps a [`DateTimePrecision`] to the coarsest Arrow [`TimeUnit`] that is at
/// least as fine-grained as the ClickHouse precision.
///
/// Arrow only supports four granularities, so precisions 1–3 round up to
/// milliseconds, 4–6 to microseconds, and 7–9 to nanoseconds.
fn precision_to_time_unit(precision: &DateTimePrecision) -> TimeUnit {
    match precision {
        DateTimePrecision::Precision0 => TimeUnit::Second,
        DateTimePrecision::Precision1
        | DateTimePrecision::Precision2
        | DateTimePrecision::Precision3 => TimeUnit::Millisecond,
        DateTimePrecision::Precision4
        | DateTimePrecision::Precision5
        | DateTimePrecision::Precision6 => TimeUnit::Microsecond,
        DateTimePrecision::Precision7
        | DateTimePrecision::Precision8
        | DateTimePrecision::Precision9 => TimeUnit::Nanosecond,
    }
}

/// Maps a [`DateTimePrecision`] to the appropriate Arrow time-of-day type.
///
/// Arrow `Time32` covers Second and Millisecond; `Time64` covers Microsecond
/// and Nanosecond.
fn precision_to_time_type(precision: &DateTimePrecision) -> DataType {
    match precision {
        DateTimePrecision::Precision0 => DataType::Time32(TimeUnit::Second),
        DateTimePrecision::Precision1
        | DateTimePrecision::Precision2
        | DateTimePrecision::Precision3 => DataType::Time32(TimeUnit::Millisecond),
        DateTimePrecision::Precision4
        | DateTimePrecision::Precision5
        | DateTimePrecision::Precision6 => DataType::Time64(TimeUnit::Microsecond),
        DateTimePrecision::Precision7
        | DateTimePrecision::Precision8
        | DateTimePrecision::Precision9 => DataType::Time64(TimeUnit::Nanosecond),
    }
}

/// Arrow `DataType` for a ClickHouse `Point` (two `Float64` coordinates).
fn point_type() -> DataType {
    DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse_types::data_types::DecimalType;

    #[test]
    fn test_primitive_types() {
        assert_eq!(data_type_to_arrow(&DataTypeNode::Bool), DataType::Boolean);
        assert_eq!(data_type_to_arrow(&DataTypeNode::UInt8), DataType::UInt8);
        assert_eq!(data_type_to_arrow(&DataTypeNode::UInt16), DataType::UInt16);
        assert_eq!(data_type_to_arrow(&DataTypeNode::UInt32), DataType::UInt32);
        assert_eq!(data_type_to_arrow(&DataTypeNode::UInt64), DataType::UInt64);
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::UInt128),
            DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::UInt256),
            DataType::FixedSizeBinary(32)
        );
        assert_eq!(data_type_to_arrow(&DataTypeNode::Int8), DataType::Int8);
        assert_eq!(data_type_to_arrow(&DataTypeNode::Int16), DataType::Int16);
        assert_eq!(data_type_to_arrow(&DataTypeNode::Int32), DataType::Int32);
        assert_eq!(data_type_to_arrow(&DataTypeNode::Int64), DataType::Int64);
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Int128),
            DataType::Decimal128(38, 0)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Int256),
            DataType::Decimal256(76, 0)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Float32),
            DataType::Float32
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Float64),
            DataType::Float64
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::BFloat16),
            DataType::Float16
        );
    }

    #[test]
    fn test_decimal_types() {
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Decimal(7, 2, DecimalType::Decimal32)),
            DataType::Decimal32(7, 2)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Decimal(12, 4, DecimalType::Decimal64)),
            DataType::Decimal64(12, 4)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Decimal(27, 6, DecimalType::Decimal128)),
            DataType::Decimal128(27, 6)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Decimal(42, 8, DecimalType::Decimal256)),
            DataType::Decimal256(42, 8)
        );
    }

    #[test]
    fn test_string_types() {
        assert_eq!(data_type_to_arrow(&DataTypeNode::String), DataType::Utf8);
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::FixedString(16)),
            DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::UUID),
            DataType::FixedSizeBinary(16)
        );
    }

    #[test]
    fn test_date_time_types() {
        assert_eq!(data_type_to_arrow(&DataTypeNode::Date), DataType::Date32);
        assert_eq!(data_type_to_arrow(&DataTypeNode::Date32), DataType::Date32);
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::DateTime(None)),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::DateTime(Some("UTC".to_string()))),
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC")))
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::DateTime64(
                DateTimePrecision::Precision0,
                None
            )),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::DateTime64(
                DateTimePrecision::Precision3,
                None
            )),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::DateTime64(
                DateTimePrecision::Precision6,
                None
            )),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::DateTime64(
                DateTimePrecision::Precision9,
                None
            )),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn test_time_types() {
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Time),
            DataType::Time32(TimeUnit::Second)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Time64(DateTimePrecision::Precision0)),
            DataType::Time32(TimeUnit::Second)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Time64(DateTimePrecision::Precision3)),
            DataType::Time32(TimeUnit::Millisecond)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Time64(DateTimePrecision::Precision6)),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Time64(DateTimePrecision::Precision9)),
            DataType::Time64(TimeUnit::Nanosecond)
        );
    }

    #[test]
    fn test_nullable_strips_wrapper() {
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Nullable(Box::new(DataTypeNode::Int32))),
            DataType::Int32
        );
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::LowCardinality(Box::new(
                DataTypeNode::String
            ))),
            DataType::Utf8
        );
    }

    #[test]
    fn test_array_type() {
        assert_eq!(
            data_type_to_arrow(&DataTypeNode::Array(Box::new(DataTypeNode::Int32))),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
    }

    #[test]
    fn test_tuple_type() {
        let tuple = DataTypeNode::Tuple(vec![DataTypeNode::UInt64, DataTypeNode::String]);
        let fields: Fields = vec![
            Arc::new(Field::new("0", DataType::UInt64, true)),
            Arc::new(Field::new("1", DataType::Utf8, true)),
        ]
        .into_iter()
        .collect();
        assert_eq!(data_type_to_arrow(&tuple), DataType::Struct(fields));
    }

    #[test]
    fn test_from_columns() {
        let columns = vec![
            Column::new("id".to_string(), DataTypeNode::UInt64),
            Column::new(
                "name".to_string(),
                DataTypeNode::Nullable(Box::new(DataTypeNode::String)),
            ),
        ];
        let schema = from_columns(&columns);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert!(schema.field(1).is_nullable());
    }
}
