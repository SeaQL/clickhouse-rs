use clickhouse_types::DataTypeNode;
use clickhouse_types::data_types::{DateTimePrecision, DecimalType, IntervalType};
use sea_orm_arrow::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

/// Converts an Arrow [`DataType`] to the corresponding ClickHouse [`DataTypeNode`].
///
/// This is the reverse of `data_type_to_arrow` in `src/arrow/schema.rs`.
/// Some Arrow types are ambiguous in reverse (e.g. `FixedSizeBinary(16)` could
/// be `UUID`, `IPv6`, or `FixedString(16)`); this function uses the most general
/// mapping (`FixedString`).
pub fn arrow_to_data_type(dtype: &DataType) -> DataTypeNode {
    match dtype {
        DataType::Boolean => DataTypeNode::Bool,

        DataType::UInt8 => DataTypeNode::UInt8,
        DataType::UInt16 => DataTypeNode::UInt16,
        DataType::UInt32 => DataTypeNode::UInt32,
        DataType::UInt64 => DataTypeNode::UInt64,

        DataType::Int8 => DataTypeNode::Int8,
        DataType::Int16 => DataTypeNode::Int16,
        DataType::Int32 => DataTypeNode::Int32,
        DataType::Int64 => DataTypeNode::Int64,

        DataType::Float16 => DataTypeNode::BFloat16,
        DataType::Float32 => DataTypeNode::Float32,
        DataType::Float64 => DataTypeNode::Float64,

        DataType::Utf8 => DataTypeNode::String,
        DataType::LargeUtf8 => DataTypeNode::String,
        DataType::Binary => DataTypeNode::String,
        DataType::LargeBinary => DataTypeNode::String,
        DataType::FixedSizeBinary(n) => DataTypeNode::FixedString(*n as usize),

        DataType::Date32 => DataTypeNode::Date,
        DataType::Date64 => DataTypeNode::Date32,

        DataType::Timestamp(unit, tz) => {
            let tz = tz.as_ref().map(|s| s.to_string());
            match unit {
                TimeUnit::Second => DataTypeNode::DateTime(tz),
                TimeUnit::Millisecond => {
                    DataTypeNode::DateTime64(DateTimePrecision::Precision3, tz)
                }
                TimeUnit::Microsecond => {
                    DataTypeNode::DateTime64(DateTimePrecision::Precision6, tz)
                }
                TimeUnit::Nanosecond => DataTypeNode::DateTime64(DateTimePrecision::Precision9, tz),
            }
        }

        DataType::Time32(unit) => match unit {
            TimeUnit::Second => DataTypeNode::Time,
            TimeUnit::Millisecond => DataTypeNode::Time64(DateTimePrecision::Precision3),
            _ => DataTypeNode::Time,
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => DataTypeNode::Time64(DateTimePrecision::Precision6),
            TimeUnit::Nanosecond => DataTypeNode::Time64(DateTimePrecision::Precision9),
            _ => DataTypeNode::Time64(DateTimePrecision::Precision6),
        },

        DataType::Decimal32(p, s) => DataTypeNode::Decimal(*p, *s as u8, DecimalType::Decimal32),
        DataType::Decimal64(p, s) => DataTypeNode::Decimal(*p, *s as u8, DecimalType::Decimal64),
        DataType::Decimal128(p, s) => DataTypeNode::Decimal(*p, *s as u8, DecimalType::Decimal128),
        DataType::Decimal256(p, s) => DataTypeNode::Decimal(*p, *s as u8, DecimalType::Decimal256),

        DataType::Duration(unit) => {
            let interval = match unit {
                TimeUnit::Second => IntervalType::Second,
                TimeUnit::Millisecond => IntervalType::Millisecond,
                TimeUnit::Microsecond => IntervalType::Microsecond,
                TimeUnit::Nanosecond => IntervalType::Nanosecond,
            };
            DataTypeNode::Interval(interval)
        }

        DataType::Interval(iu) => match iu {
            IntervalUnit::DayTime => DataTypeNode::Interval(IntervalType::Day),
            IntervalUnit::YearMonth => DataTypeNode::Interval(IntervalType::Month),
            IntervalUnit::MonthDayNano => DataTypeNode::Interval(IntervalType::Day),
        },

        DataType::List(field) | DataType::LargeList(field) => {
            DataTypeNode::Array(Box::new(arrow_to_data_type(field.data_type())))
        }

        DataType::FixedSizeList(field, 2) if matches!(field.data_type(), DataType::Float64) => {
            DataTypeNode::Point
        }
        DataType::FixedSizeList(field, _) => {
            DataTypeNode::Array(Box::new(arrow_to_data_type(field.data_type())))
        }

        DataType::Struct(fields) => {
            let types = fields
                .iter()
                .map(|f| arrow_to_data_type(f.data_type()))
                .collect();
            DataTypeNode::Tuple(types)
        }

        DataType::Map(entries_field, _) => {
            if let DataType::Struct(fields) = entries_field.data_type() {
                if fields.len() == 2 {
                    let key = arrow_to_data_type(fields[0].data_type());
                    let value = arrow_to_data_type(fields[1].data_type());
                    return DataTypeNode::Map([Box::new(key), Box::new(value)]);
                }
            }
            DataTypeNode::String
        }

        DataType::Union(union_fields, _) => {
            let types = union_fields
                .iter()
                .map(|(_, f)| arrow_to_data_type(f.data_type()))
                .collect();
            DataTypeNode::Variant(types)
        }

        _ => DataTypeNode::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse_types::data_types::{Column, DateTimePrecision, DecimalType};
    use sea_orm_arrow::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    use crate::schema::{ClickHouseSchema, Engine};

    fn trim_indent(s: &str) -> String {
        let lines: Vec<&str> = s.lines().collect();
        let min_indent = lines
            .iter()
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.len() - l.trim_start().len())
            .min()
            .unwrap_or(0);
        let trimmed: Vec<&str> = lines
            .iter()
            .map(|l| {
                if l.len() >= min_indent {
                    &l[min_indent..]
                } else {
                    l.trim()
                }
            })
            .collect();
        let s = trimmed.join("\n");
        s.trim_matches('\n').to_string()
    }

    #[test]
    fn test_timestamp_nanos_to_datetime64_9() {
        let dt = arrow_to_data_type(&DataType::Timestamp(TimeUnit::Nanosecond, None));
        assert_eq!(
            dt,
            DataTypeNode::DateTime64(DateTimePrecision::Precision9, None)
        );
    }

    #[test]
    fn test_timestamp_with_tz() {
        let dt = arrow_to_data_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("UTC")),
        ));
        assert_eq!(
            dt,
            DataTypeNode::DateTime64(DateTimePrecision::Precision6, Some("UTC".to_string()))
        );
    }

    #[test]
    fn test_timestamp_second_to_datetime() {
        let dt = arrow_to_data_type(&DataType::Timestamp(TimeUnit::Second, None));
        assert_eq!(dt, DataTypeNode::DateTime(None));
    }

    #[test]
    fn test_decimal128() {
        let dt = arrow_to_data_type(&DataType::Decimal128(38, 4));
        assert_eq!(dt, DataTypeNode::Decimal(38, 4, DecimalType::Decimal128));
    }

    #[test]
    fn test_date32_to_date() {
        assert_eq!(arrow_to_data_type(&DataType::Date32), DataTypeNode::Date);
    }

    #[test]
    fn test_date64_to_date32() {
        assert_eq!(arrow_to_data_type(&DataType::Date64), DataTypeNode::Date32);
    }

    #[test]
    fn test_list_to_array() {
        let dt = arrow_to_data_type(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))));
        assert_eq!(dt, DataTypeNode::Array(Box::new(DataTypeNode::Int32)));
    }

    #[test]
    fn test_fixed_size_list_2_float64_to_point() {
        let dt = arrow_to_data_type(&DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float64, false)),
            2,
        ));
        assert_eq!(dt, DataTypeNode::Point);
    }

    #[test]
    fn test_from_arrow_full_schema() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "recorded_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("sensor_id", DataType::Int32, false),
            Field::new("temperature", DataType::Float64, true),
            Field::new("voltage", DataType::Decimal128(38, 4), false),
        ]);

        let ddl = ClickHouseSchema::from_arrow(&schema)
            .table_name("sensor_data")
            .engine(Engine::ReplacingMergeTree)
            .primary_key(["recorded_at", "sensor_id"])
            .index_granularity(8192)
            .to_string();

        assert_eq!(
            ddl,
            trim_indent(
                r#"
                CREATE TABLE sensor_data (
                    id UInt64,
                    recorded_at DateTime64(9),
                    sensor_id Int32,
                    temperature Nullable(Float64),
                    voltage Decimal(38, 4)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (recorded_at, sensor_id)
                SETTINGS index_granularity = 8192
            "#
            )
        );
    }

    #[test]
    fn test_round_trip_primitives() {
        use crate::arrow::schema::from_columns;

        let original_columns = vec![
            Column::new("a".into(), DataTypeNode::Bool),
            Column::new("b".into(), DataTypeNode::UInt8),
            Column::new("c".into(), DataTypeNode::UInt16),
            Column::new("d".into(), DataTypeNode::UInt32),
            Column::new("e".into(), DataTypeNode::UInt64),
            Column::new("f".into(), DataTypeNode::Int8),
            Column::new("g".into(), DataTypeNode::Int16),
            Column::new("h".into(), DataTypeNode::Int32),
            Column::new("i".into(), DataTypeNode::Int64),
            Column::new("j".into(), DataTypeNode::Float32),
            Column::new("k".into(), DataTypeNode::Float64),
            Column::new("l".into(), DataTypeNode::String),
            Column::new("m".into(), DataTypeNode::Date),
            Column::new("n".into(), DataTypeNode::DateTime(None)),
        ];

        let arrow_schema = from_columns(&original_columns);
        let rebuilt = ClickHouseSchema::from_arrow(&arrow_schema);

        for (orig, rebuilt) in original_columns.iter().zip(rebuilt.columns.iter()) {
            assert_eq!(orig.name, rebuilt.name);
            assert_eq!(
                orig.data_type, rebuilt.data_type,
                "round-trip failed for column '{}'",
                orig.name
            );
        }
    }
}
