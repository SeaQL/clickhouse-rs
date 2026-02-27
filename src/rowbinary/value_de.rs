use crate::error::{Error, Result};
#[cfg(feature = "bigdecimal")]
use crate::types::{Int256, UInt256};
use clickhouse_types::DataTypeNode;
#[cfg(any(feature = "chrono", feature = "time"))]
use clickhouse_types::data_types::DateTimePrecision;
use clickhouse_types::data_types::{DecimalType, EnumType};
use sea_query::Value;
#[cfg(feature = "bigdecimal")]
use sea_query::value::prelude::BigDecimal;
#[cfg(feature = "rust_decimal")]
use sea_query::value::prelude::Decimal;
#[cfg(feature = "uuid")]
use sea_query::value::prelude::Uuid;
use sea_query::value::prelude::serde_json;
#[cfg(feature = "chrono")]
use sea_query::value::prelude::{NaiveDate, NaiveDateTime, NaiveTime};
#[cfg(feature = "bigdecimal")]
use std::str::FromStr;

// Days from the chrono CE epoch (0001-01-01) to the Unix epoch (1970-01-01).
#[cfg(any(feature = "chrono", feature = "time"))]
const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

// Julian day for the chrono CE epoch day-1 (so julian = ce_days + this offset).
#[cfg(all(feature = "time", not(feature = "chrono")))]
const CE_DAYS_TO_JULIAN_OFFSET: i32 = 1_721_425;

// ── public entry point ───────────────────────────────────────────────────────

/// Decode one complete row (one value per column type) from `input`,
/// advancing the slice by the exact number of bytes consumed.
pub(crate) fn decode_row(input: &mut &[u8], column_types: &[DataTypeNode]) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(column_types.len());
    for dt in column_types {
        values.push(decode_value(input, dt)?);
    }
    Ok(values)
}

// ── low-level helpers ────────────────────────────────────────────────────────

#[inline]
fn read_bytes<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if input.len() < n {
        return Err(Error::NotEnoughData);
    }
    let (chunk, rest) = input.split_at(n);
    *input = rest;
    Ok(chunk)
}

#[inline]
fn read_leb128(input: &mut &[u8]) -> Result<usize> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let [byte, rest @ ..] = *input else {
            return Err(Error::NotEnoughData);
        };
        *input = rest;
        value |= (*byte as u64 & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(Error::NotEnoughData);
        }
    }
    usize::try_from(value).map_err(|_| Error::NotEnoughData)
}

#[cfg(any(feature = "chrono", feature = "time"))]
fn precision_scale(p: &DateTimePrecision) -> i64 {
    match p {
        DateTimePrecision::Precision0 => 1,
        DateTimePrecision::Precision1 => 10,
        DateTimePrecision::Precision2 => 100,
        DateTimePrecision::Precision3 => 1_000,
        DateTimePrecision::Precision4 => 10_000,
        DateTimePrecision::Precision5 => 100_000,
        DateTimePrecision::Precision6 => 1_000_000,
        DateTimePrecision::Precision7 => 10_000_000,
        DateTimePrecision::Precision8 => 100_000_000,
        DateTimePrecision::Precision9 => 1_000_000_000,
    }
}

#[cfg(feature = "chrono")]
fn datetime_from_ticks(ticks: i64, scale: i64) -> Result<NaiveDateTime> {
    let total_secs = ticks.div_euclid(scale);
    let sub = ticks.rem_euclid(scale);
    let nsecs = (sub * (1_000_000_000 / scale)) as u32;

    let day_secs = total_secs.rem_euclid(86_400) as u32;
    let days = total_secs.div_euclid(86_400);
    let days_from_ce = i32::try_from(days + UNIX_EPOCH_DAYS_FROM_CE as i64)
        .map_err(|_| Error::Custom("DateTime64 out of i32 range".to_string()))?;

    let date = NaiveDate::from_num_days_from_ce_opt(days_from_ce)
        .ok_or_else(|| Error::Custom("DateTime64 date out of range".to_string()))?;
    let time = NaiveTime::from_num_seconds_from_midnight_opt(day_secs, nsecs)
        .ok_or_else(|| Error::Custom("DateTime64 time out of range".to_string()))?;
    Ok(NaiveDateTime::new(date, time))
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn datetime_from_ticks_time(
    ticks: i64,
    scale: i64,
) -> Result<sea_query::value::prelude::PrimitiveDateTime> {
    let total_secs = ticks.div_euclid(scale);
    let sub = ticks.rem_euclid(scale);
    let nsecs = (sub * (1_000_000_000 / scale)) as u32;

    let day_secs = total_secs.rem_euclid(86_400) as u32;
    let days = total_secs.div_euclid(86_400);
    let days_from_ce = i32::try_from(days + UNIX_EPOCH_DAYS_FROM_CE as i64)
        .map_err(|_| Error::Custom("DateTime64 out of i32 range".to_string()))?;

    let julian_day = days_from_ce + CE_DAYS_TO_JULIAN_OFFSET;
    let date = sea_query::value::prelude::time::Date::from_julian_day(julian_day)
        .map_err(|_| Error::Custom("DateTime64 date out of range".to_string()))?;
    let time = time_from_day_secs_nanos(day_secs, nsecs)?;
    Ok(sea_query::value::prelude::PrimitiveDateTime::new(
        date, time,
    ))
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn time_from_day_secs_nanos(
    secs: u32,
    nanos: u32,
) -> Result<sea_query::value::prelude::time::Time> {
    let h = (secs / 3600) as u8;
    let m = ((secs % 3600) / 60) as u8;
    let s = (secs % 60) as u8;
    sea_query::value::prelude::time::Time::from_hms_nano(h, m, s, nanos)
        .map_err(|_| Error::Custom("time out of range".to_string()))
}

#[cfg(feature = "bigdecimal")]
fn bigdecimal_with_scale(int_str: &str, scale: u8) -> Result<BigDecimal> {
    let bd = BigDecimal::from_str(int_str)
        .map_err(|e| Error::Custom(format!("BigDecimal parse error: {e}")))?;
    let (big_int, _) = bd.into_bigint_and_exponent();
    Ok(BigDecimal::new(big_int, scale as i64))
}

// ── typed null ───────────────────────────────────────────────────────────────

/// Return the appropriate "None" variant for a given type.
fn typed_null(dt: &DataTypeNode) -> Result<Value> {
    let dt = dt.remove_low_cardinality();
    Ok(match dt {
        DataTypeNode::Nullable(inner) => return typed_null(inner),
        DataTypeNode::Bool => Value::Bool(None),
        DataTypeNode::Int8 => Value::TinyInt(None),
        DataTypeNode::Int16 => Value::SmallInt(None),
        DataTypeNode::Int32 => Value::Int(None),
        DataTypeNode::Int64 => Value::BigInt(None),
        #[cfg(feature = "bigdecimal")]
        DataTypeNode::Int128 | DataTypeNode::Int256 => Value::BigDecimal(None),
        #[cfg(not(feature = "bigdecimal"))]
        DataTypeNode::Int128 | DataTypeNode::Int256 => {
            return Err(Error::Unsupported(
                "Int128/Int256 requires the `bigdecimal` feature".into(),
            ));
        }
        DataTypeNode::UInt8 => Value::TinyUnsigned(None),
        DataTypeNode::UInt16 => Value::SmallUnsigned(None),
        DataTypeNode::UInt32 => Value::Unsigned(None),
        DataTypeNode::UInt64 => Value::BigUnsigned(None),
        #[cfg(feature = "bigdecimal")]
        DataTypeNode::UInt128 | DataTypeNode::UInt256 => Value::BigDecimal(None),
        #[cfg(not(feature = "bigdecimal"))]
        DataTypeNode::UInt128 | DataTypeNode::UInt256 => {
            return Err(Error::Unsupported(
                "UInt128/UInt256 requires the `bigdecimal` feature".into(),
            ));
        }
        DataTypeNode::Float32 | DataTypeNode::BFloat16 => Value::Float(None),
        DataTypeNode::Float64 => Value::Double(None),
        DataTypeNode::String => Value::String(None),
        DataTypeNode::FixedString(_) => Value::Bytes(None),

        #[cfg(feature = "uuid")]
        DataTypeNode::UUID => Value::Uuid(None),
        #[cfg(not(feature = "uuid"))]
        DataTypeNode::UUID => {
            return Err(Error::Unsupported(
                "UUID requires the `uuid` feature".into(),
            ));
        }

        #[cfg(feature = "chrono")]
        DataTypeNode::Date | DataTypeNode::Date32 => Value::ChronoDate(None),
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Date | DataTypeNode::Date32 => Value::TimeDate(None),
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Date | DataTypeNode::Date32 => {
            return Err(Error::Unsupported(
                "Date requires the `chrono` or `time` feature".into(),
            ));
        }

        #[cfg(feature = "chrono")]
        DataTypeNode::DateTime(_) | DataTypeNode::DateTime64(_, _) => Value::ChronoDateTime(None),
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::DateTime(_) | DataTypeNode::DateTime64(_, _) => Value::TimeDateTime(None),
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::DateTime(_) | DataTypeNode::DateTime64(_, _) => {
            return Err(Error::Unsupported(
                "DateTime requires the `chrono` or `time` feature".into(),
            ));
        }

        #[cfg(feature = "chrono")]
        DataTypeNode::Time | DataTypeNode::Time64(_) => Value::ChronoTime(None),
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Time | DataTypeNode::Time64(_) => Value::TimeTime(None),
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Time | DataTypeNode::Time64(_) => {
            return Err(Error::Unsupported(
                "Time requires the `chrono` or `time` feature".into(),
            ));
        }

        #[cfg(feature = "rust_decimal")]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal32)
        | DataTypeNode::Decimal(_, _, DecimalType::Decimal64) => Value::Decimal(None),
        #[cfg(all(feature = "bigdecimal", not(feature = "rust_decimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal32)
        | DataTypeNode::Decimal(_, _, DecimalType::Decimal64) => Value::BigDecimal(None),
        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal32)
        | DataTypeNode::Decimal(_, _, DecimalType::Decimal64) => {
            return Err(Error::Unsupported(
                "Decimal requires the `rust_decimal` or `bigdecimal` feature".into(),
            ));
        }

        #[cfg(all(feature = "rust_decimal", feature = "bigdecimal"))]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal128) => {
            if *scale <= 28 {
                Value::Decimal(None)
            } else {
                Value::BigDecimal(None)
            }
        }
        #[cfg(all(feature = "rust_decimal", not(feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal128) => Value::Decimal(None),
        #[cfg(all(feature = "bigdecimal", not(feature = "rust_decimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal128) => Value::BigDecimal(None),
        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal128) => {
            return Err(Error::Unsupported(
                "Decimal128 requires the `rust_decimal` or `bigdecimal` feature".into(),
            ));
        }

        #[cfg(feature = "bigdecimal")]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal256) => Value::BigDecimal(None),
        #[cfg(not(feature = "bigdecimal"))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal256) => {
            return Err(Error::Unsupported(
                "Decimal256 requires the `bigdecimal` feature".into(),
            ));
        }
        DataTypeNode::IPv4 | DataTypeNode::IPv6 => Value::String(None),
        DataTypeNode::Enum(_, _) => Value::String(None),
        DataTypeNode::Interval(_) => Value::BigInt(None),
        // Arrays, Maps, Tuples, JSON, geo types -> null JSON
        _ => Value::Json(None),
    })
}

// ── value_to_json helper for Array / Tuple / Map ─────────────────────────────

fn value_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Bool(Some(b)) => serde_json::Value::Bool(b),
        Value::TinyInt(Some(v)) => serde_json::json!(v),
        Value::SmallInt(Some(v)) => serde_json::json!(v),
        Value::Int(Some(v)) => serde_json::json!(v),
        Value::BigInt(Some(v)) => serde_json::json!(v),
        Value::TinyUnsigned(Some(v)) => serde_json::json!(v),
        Value::SmallUnsigned(Some(v)) => serde_json::json!(v),
        Value::Unsigned(Some(v)) => serde_json::json!(v),
        Value::BigUnsigned(Some(v)) => serde_json::json!(v),
        Value::Float(Some(v)) => serde_json::json!(v),
        Value::Double(Some(v)) => serde_json::json!(v),
        Value::String(Some(s)) => serde_json::Value::String(s),
        Value::Bytes(Some(b)) => serde_json::json!(b),
        #[cfg(feature = "bigdecimal")]
        Value::BigDecimal(Some(bd)) => serde_json::Value::String(bd.to_string()),
        #[cfg(feature = "rust_decimal")]
        Value::Decimal(Some(d)) => serde_json::Value::String(d.to_string()),
        #[cfg(feature = "chrono")]
        Value::ChronoDate(Some(d)) => serde_json::Value::String(d.to_string()),
        #[cfg(feature = "chrono")]
        Value::ChronoTime(Some(t)) => serde_json::Value::String(t.to_string()),
        #[cfg(feature = "chrono")]
        Value::ChronoDateTime(Some(dt)) => serde_json::Value::String(dt.to_string()),
        #[cfg(feature = "time")]
        Value::TimeDate(Some(d)) => serde_json::Value::String(d.to_string()),
        #[cfg(feature = "time")]
        Value::TimeTime(Some(t)) => serde_json::Value::String(t.to_string()),
        #[cfg(feature = "time")]
        Value::TimeDateTime(Some(dt)) => serde_json::Value::String(dt.to_string()),
        #[cfg(feature = "uuid")]
        Value::Uuid(Some(u)) => serde_json::Value::String(u.to_string()),
        Value::Json(Some(j)) => *j,
        _ => serde_json::Value::Null,
    }
}

// ── main decoder ─────────────────────────────────────────────────────────────

fn decode_value(input: &mut &[u8], dt: &DataTypeNode) -> Result<Value> {
    // Strip transparent wrapper first.
    let dt = dt.remove_low_cardinality();

    match dt {
        // ── Nullable ──────────────────────────────────────────────────────
        DataTypeNode::Nullable(inner) => {
            let null_flag = read_bytes(input, 1)?[0];
            if null_flag != 0 {
                return typed_null(inner);
            }
            decode_value(input, inner)
        }

        // ── Bool ──────────────────────────────────────────────────────────
        DataTypeNode::Bool => {
            let v = read_bytes(input, 1)?[0] != 0;
            Ok(Value::Bool(Some(v)))
        }

        // ── Signed integers ───────────────────────────────────────────────
        DataTypeNode::Int8 => {
            let b = read_bytes(input, 1)?;
            Ok(Value::TinyInt(Some(b[0] as i8)))
        }
        DataTypeNode::Int16 => {
            let b = read_bytes(input, 2)?;
            Ok(Value::SmallInt(Some(i16::from_le_bytes(
                b.try_into().unwrap(),
            ))))
        }
        DataTypeNode::Int32 => {
            let b = read_bytes(input, 4)?;
            Ok(Value::Int(Some(i32::from_le_bytes(b.try_into().unwrap()))))
        }
        DataTypeNode::Int64 => {
            let b = read_bytes(input, 8)?;
            Ok(Value::BigInt(Some(i64::from_le_bytes(
                b.try_into().unwrap(),
            ))))
        }
        #[cfg(feature = "bigdecimal")]
        DataTypeNode::Int128 => {
            let b = read_bytes(input, 16)?;
            let v = i128::from_le_bytes(b.try_into().unwrap());
            let bd = bigdecimal_with_scale(&v.to_string(), 0)?;
            Ok(Value::BigDecimal(Some(Box::new(bd))))
        }
        #[cfg(not(feature = "bigdecimal"))]
        DataTypeNode::Int128 => {
            let _ = read_bytes(input, 16)?;
            Err(Error::Unsupported(
                "Int128 decoding requires the `bigdecimal` feature".into(),
            ))
        }
        #[cfg(feature = "bigdecimal")]
        DataTypeNode::Int256 => {
            let b = read_bytes(input, 32)?;
            let v = Int256::from_le_bytes(b.try_into().unwrap());
            let bd = bigdecimal_with_scale(&v.to_string(), 0)?;
            Ok(Value::BigDecimal(Some(Box::new(bd))))
        }
        #[cfg(not(feature = "bigdecimal"))]
        DataTypeNode::Int256 => {
            let _ = read_bytes(input, 32)?;
            Err(Error::Unsupported(
                "Int256 decoding requires the `bigdecimal` feature".into(),
            ))
        }

        // ── Unsigned integers ─────────────────────────────────────────────
        DataTypeNode::UInt8 => {
            let b = read_bytes(input, 1)?;
            Ok(Value::TinyUnsigned(Some(b[0])))
        }
        DataTypeNode::UInt16 => {
            let b = read_bytes(input, 2)?;
            Ok(Value::SmallUnsigned(Some(u16::from_le_bytes(
                b.try_into().unwrap(),
            ))))
        }
        DataTypeNode::UInt32 => {
            let b = read_bytes(input, 4)?;
            Ok(Value::Unsigned(Some(u32::from_le_bytes(
                b.try_into().unwrap(),
            ))))
        }
        DataTypeNode::UInt64 => {
            let b = read_bytes(input, 8)?;
            Ok(Value::BigUnsigned(Some(u64::from_le_bytes(
                b.try_into().unwrap(),
            ))))
        }
        #[cfg(feature = "bigdecimal")]
        DataTypeNode::UInt128 => {
            let b = read_bytes(input, 16)?;
            let v = u128::from_le_bytes(b.try_into().unwrap());
            let bd = bigdecimal_with_scale(&v.to_string(), 0)?;
            Ok(Value::BigDecimal(Some(Box::new(bd))))
        }
        #[cfg(not(feature = "bigdecimal"))]
        DataTypeNode::UInt128 => {
            let _ = read_bytes(input, 16)?;
            Err(Error::Unsupported(
                "UInt128 decoding requires the `bigdecimal` feature".into(),
            ))
        }
        #[cfg(feature = "bigdecimal")]
        DataTypeNode::UInt256 => {
            let b = read_bytes(input, 32)?;
            let v = UInt256::from_le_bytes(b.try_into().unwrap());
            let bd = bigdecimal_with_scale(&v.to_string(), 0)?;
            Ok(Value::BigDecimal(Some(Box::new(bd))))
        }
        #[cfg(not(feature = "bigdecimal"))]
        DataTypeNode::UInt256 => {
            let _ = read_bytes(input, 32)?;
            Err(Error::Unsupported(
                "UInt256 decoding requires the `bigdecimal` feature".into(),
            ))
        }

        // ── Floats ────────────────────────────────────────────────────────
        DataTypeNode::Float32 => {
            let b = read_bytes(input, 4)?;
            Ok(Value::Float(Some(f32::from_le_bytes(
                b.try_into().unwrap(),
            ))))
        }
        DataTypeNode::Float64 => {
            let b = read_bytes(input, 8)?;
            Ok(Value::Double(Some(f64::from_le_bytes(
                b.try_into().unwrap(),
            ))))
        }
        DataTypeNode::BFloat16 => {
            let b = read_bytes(input, 2)?;
            let bits = u16::from_le_bytes(b.try_into().unwrap());
            let v = f32::from_bits((bits as u32) << 16);
            Ok(Value::Float(Some(v)))
        }

        // ── String / Bytes ────────────────────────────────────────────────
        DataTypeNode::String => {
            let len = read_leb128(input)?;
            let bytes = read_bytes(input, len)?;
            let s = String::from_utf8_lossy(bytes).into_owned();
            Ok(Value::String(Some(s)))
        }
        DataTypeNode::FixedString(n) => {
            let bytes = read_bytes(input, *n)?;
            Ok(Value::Bytes(Some(bytes.to_vec())))
        }

        // ── UUID ──────────────────────────────────────────────────────────
        #[cfg(feature = "uuid")]
        DataTypeNode::UUID => {
            let b = read_bytes(input, 16)?;
            // ClickHouse stores UUID as two LE u64 values (high, then low).
            let hi = u64::from_le_bytes(b[0..8].try_into().unwrap());
            let lo = u64::from_le_bytes(b[8..16].try_into().unwrap());
            let uuid = Uuid::from_u128((hi as u128) << 64 | lo as u128);
            Ok(Value::Uuid(Some(uuid)))
        }
        #[cfg(not(feature = "uuid"))]
        DataTypeNode::UUID => Err(Error::Unsupported(
            "UUID decoding requires the `uuid` feature".into(),
        )),

        // ── Dates ─────────────────────────────────────────────────────────
        #[cfg(feature = "chrono")]
        DataTypeNode::Date => {
            let b = read_bytes(input, 2)?;
            let days = u16::from_le_bytes(b.try_into().unwrap()) as i32;
            let date = NaiveDate::from_num_days_from_ce_opt(days + UNIX_EPOCH_DAYS_FROM_CE)
                .ok_or_else(|| Error::Custom("Date out of range".to_string()))?;
            Ok(Value::ChronoDate(Some(date)))
        }
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Date => {
            let b = read_bytes(input, 2)?;
            let days = u16::from_le_bytes(b.try_into().unwrap()) as i32;
            let julian = days + UNIX_EPOCH_DAYS_FROM_CE + CE_DAYS_TO_JULIAN_OFFSET;
            let date = sea_query::value::prelude::time::Date::from_julian_day(julian)
                .map_err(|_| Error::Custom("Date out of range".to_string()))?;
            Ok(Value::TimeDate(Some(date)))
        }
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Date => {
            let _ = read_bytes(input, 2)?;
            Err(Error::Unsupported(
                "Date decoding requires the `chrono` or `time` feature".into(),
            ))
        }

        #[cfg(feature = "chrono")]
        DataTypeNode::Date32 => {
            let b = read_bytes(input, 4)?;
            let days = i32::from_le_bytes(b.try_into().unwrap());
            let date = NaiveDate::from_num_days_from_ce_opt(days + UNIX_EPOCH_DAYS_FROM_CE)
                .ok_or_else(|| Error::Custom("Date32 out of range".to_string()))?;
            Ok(Value::ChronoDate(Some(date)))
        }
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Date32 => {
            let b = read_bytes(input, 4)?;
            let days = i32::from_le_bytes(b.try_into().unwrap());
            let julian = days + UNIX_EPOCH_DAYS_FROM_CE + CE_DAYS_TO_JULIAN_OFFSET;
            let date = sea_query::value::prelude::time::Date::from_julian_day(julian)
                .map_err(|_| Error::Custom("Date32 out of range".to_string()))?;
            Ok(Value::TimeDate(Some(date)))
        }
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Date32 => {
            let _ = read_bytes(input, 4)?;
            Err(Error::Unsupported(
                "Date32 decoding requires the `chrono` or `time` feature".into(),
            ))
        }

        // ── DateTimes ─────────────────────────────────────────────────────
        #[cfg(feature = "chrono")]
        DataTypeNode::DateTime(_tz) => {
            let b = read_bytes(input, 4)?;
            let secs = u32::from_le_bytes(b.try_into().unwrap()) as i64;
            Ok(Value::ChronoDateTime(Some(datetime_from_ticks(secs, 1)?)))
        }
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::DateTime(_tz) => {
            let b = read_bytes(input, 4)?;
            let secs = u32::from_le_bytes(b.try_into().unwrap()) as i64;
            Ok(Value::TimeDateTime(Some(datetime_from_ticks_time(
                secs, 1,
            )?)))
        }
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::DateTime(_tz) => {
            let _ = read_bytes(input, 4)?;
            Err(Error::Unsupported(
                "DateTime decoding requires the `chrono` or `time` feature".into(),
            ))
        }

        #[cfg(feature = "chrono")]
        DataTypeNode::DateTime64(prec, _tz) => {
            let b = read_bytes(input, 8)?;
            let ticks = i64::from_le_bytes(b.try_into().unwrap());
            let scale = precision_scale(prec);
            Ok(Value::ChronoDateTime(Some(datetime_from_ticks(
                ticks, scale,
            )?)))
        }
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::DateTime64(prec, _tz) => {
            let b = read_bytes(input, 8)?;
            let ticks = i64::from_le_bytes(b.try_into().unwrap());
            let scale = precision_scale(prec);
            Ok(Value::TimeDateTime(Some(datetime_from_ticks_time(
                ticks, scale,
            )?)))
        }
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::DateTime64(_prec, _tz) => {
            let _ = read_bytes(input, 8)?;
            Err(Error::Unsupported(
                "DateTime64 decoding requires the `chrono` or `time` feature".into(),
            ))
        }

        // ── Time ──────────────────────────────────────────────────────────
        #[cfg(feature = "chrono")]
        DataTypeNode::Time => {
            let b = read_bytes(input, 4)?;
            let secs = i32::from_le_bytes(b.try_into().unwrap());
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs.max(0) as u32, 0)
                .ok_or_else(|| Error::Custom("Time out of range".to_string()))?;
            Ok(Value::ChronoTime(Some(time)))
        }
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Time => {
            let b = read_bytes(input, 4)?;
            let secs = i32::from_le_bytes(b.try_into().unwrap()).max(0) as u32;
            let t = time_from_day_secs_nanos(secs, 0)?;
            Ok(Value::TimeTime(Some(t)))
        }
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Time => {
            let _ = read_bytes(input, 4)?;
            Err(Error::Unsupported(
                "Time decoding requires the `chrono` or `time` feature".into(),
            ))
        }

        #[cfg(feature = "chrono")]
        DataTypeNode::Time64(prec) => {
            let b = read_bytes(input, 8)?;
            let ticks = i64::from_le_bytes(b.try_into().unwrap());
            let scale = precision_scale(prec);
            let secs = ticks.div_euclid(scale) as u32;
            let nsecs = (ticks.rem_euclid(scale) * (1_000_000_000 / scale)) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nsecs)
                .ok_or_else(|| Error::Custom("Time64 out of range".to_string()))?;
            Ok(Value::ChronoTime(Some(time)))
        }
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Time64(prec) => {
            let b = read_bytes(input, 8)?;
            let ticks = i64::from_le_bytes(b.try_into().unwrap());
            let scale = precision_scale(prec);
            let secs = ticks.div_euclid(scale) as u32;
            let nsecs = (ticks.rem_euclid(scale) * (1_000_000_000 / scale)) as u32;
            let t = time_from_day_secs_nanos(secs, nsecs)?;
            Ok(Value::TimeTime(Some(t)))
        }
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Time64(_prec) => {
            let _ = read_bytes(input, 8)?;
            Err(Error::Unsupported(
                "Time64 decoding requires the `chrono` or `time` feature".into(),
            ))
        }

        // ── Decimals ──────────────────────────────────────────────────────
        #[cfg(feature = "rust_decimal")]
        DataTypeNode::Decimal(_precision, scale, DecimalType::Decimal32) => {
            let b = read_bytes(input, 4)?;
            let raw = i32::from_le_bytes(b.try_into().unwrap());
            Ok(Value::Decimal(Some(Decimal::from_i128_with_scale(
                raw as i128,
                *scale as u32,
            ))))
        }
        #[cfg(all(feature = "bigdecimal", not(feature = "rust_decimal")))]
        DataTypeNode::Decimal(_precision, scale, DecimalType::Decimal32) => {
            let b = read_bytes(input, 4)?;
            let raw = i32::from_le_bytes(b.try_into().unwrap());
            let bd = bigdecimal_with_scale(&raw.to_string(), *scale)?;
            Ok(Value::BigDecimal(Some(Box::new(bd))))
        }
        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal32) => {
            let _ = read_bytes(input, 4)?;
            Err(Error::Unsupported(
                "Decimal requires the `rust_decimal` or `bigdecimal` feature".into(),
            ))
        }

        #[cfg(feature = "rust_decimal")]
        DataTypeNode::Decimal(_precision, scale, DecimalType::Decimal64) => {
            let b = read_bytes(input, 8)?;
            let raw = i64::from_le_bytes(b.try_into().unwrap());
            Ok(Value::Decimal(Some(Decimal::from_i128_with_scale(
                raw as i128,
                *scale as u32,
            ))))
        }
        #[cfg(all(feature = "bigdecimal", not(feature = "rust_decimal")))]
        DataTypeNode::Decimal(_precision, scale, DecimalType::Decimal64) => {
            let b = read_bytes(input, 8)?;
            let raw = i64::from_le_bytes(b.try_into().unwrap());
            let bd = bigdecimal_with_scale(&raw.to_string(), *scale)?;
            Ok(Value::BigDecimal(Some(Box::new(bd))))
        }
        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal64) => {
            let _ = read_bytes(input, 8)?;
            Err(Error::Unsupported(
                "Decimal requires the `rust_decimal` or `bigdecimal` feature".into(),
            ))
        }

        #[cfg(all(feature = "rust_decimal", feature = "bigdecimal"))]
        DataTypeNode::Decimal(_precision, scale, DecimalType::Decimal128) => {
            let b = read_bytes(input, 16)?;
            let raw = i128::from_le_bytes(b.try_into().unwrap());
            if let Ok(dec) = Decimal::try_from_i128_with_scale(raw, *scale as u32) {
                Ok(Value::Decimal(Some(dec)))
            } else {
                let bd = bigdecimal_with_scale(&raw.to_string(), *scale)?;
                Ok(Value::BigDecimal(Some(Box::new(bd))))
            }
        }
        #[cfg(all(feature = "rust_decimal", not(feature = "bigdecimal")))]
        DataTypeNode::Decimal(_precision, scale, DecimalType::Decimal128) => {
            let b = read_bytes(input, 16)?;
            let raw = i128::from_le_bytes(b.try_into().unwrap());
            Decimal::try_from_i128_with_scale(raw, *scale as u32)
                .map(|dec| Value::Decimal(Some(dec)))
                .map_err(|_| {
                    Error::Custom(
                        "Decimal128 value exceeds rust_decimal range; \
                         enable the `bigdecimal` feature for full Decimal128 support"
                            .to_string(),
                    )
                })
        }
        #[cfg(all(feature = "bigdecimal", not(feature = "rust_decimal")))]
        DataTypeNode::Decimal(_precision, scale, DecimalType::Decimal128) => {
            let b = read_bytes(input, 16)?;
            let raw = i128::from_le_bytes(b.try_into().unwrap());
            let bd = bigdecimal_with_scale(&raw.to_string(), *scale)?;
            Ok(Value::BigDecimal(Some(Box::new(bd))))
        }
        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal128) => {
            let _ = read_bytes(input, 16)?;
            Err(Error::Unsupported(
                "Decimal128 requires the `rust_decimal` or `bigdecimal` feature".into(),
            ))
        }

        #[cfg(feature = "bigdecimal")]
        DataTypeNode::Decimal(_precision, scale, DecimalType::Decimal256) => {
            let b = read_bytes(input, 32)?;
            let int256 = Int256::from_le_bytes(b.try_into().unwrap());
            let bd = bigdecimal_with_scale(&int256.to_string(), *scale)?;
            Ok(Value::BigDecimal(Some(Box::new(bd))))
        }
        #[cfg(not(feature = "bigdecimal"))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal256) => {
            let _ = read_bytes(input, 32)?;
            Err(Error::Unsupported(
                "Decimal256 requires the `bigdecimal` feature".into(),
            ))
        }

        // ── IP addresses ──────────────────────────────────────────────────
        DataTypeNode::IPv4 => {
            let b = read_bytes(input, 4)?;
            let n = u32::from_le_bytes(b.try_into().unwrap());
            let ip = std::net::Ipv4Addr::from(n);
            Ok(Value::String(Some(ip.to_string())))
        }
        DataTypeNode::IPv6 => {
            let b = read_bytes(input, 16)?;
            let ip = std::net::Ipv6Addr::from(<[u8; 16]>::try_from(b).unwrap());
            Ok(Value::String(Some(ip.to_string())))
        }

        // ── Enums ─────────────────────────────────────────────────────────
        DataTypeNode::Enum(EnumType::Enum8, map) => {
            let idx = read_bytes(input, 1)?[0] as i8 as i16;
            let name = map
                .get(&idx)
                .ok_or_else(|| Error::Custom(format!("Enum8: unknown discriminant {idx}")))?;
            Ok(Value::String(Some(name.clone())))
        }
        DataTypeNode::Enum(EnumType::Enum16, map) => {
            let b = read_bytes(input, 2)?;
            let idx = i16::from_le_bytes(b.try_into().unwrap());
            let name = map
                .get(&idx)
                .ok_or_else(|| Error::Custom(format!("Enum16: unknown discriminant {idx}")))?;
            Ok(Value::String(Some(name.clone())))
        }

        // ── Interval ──────────────────────────────────────────────────────
        DataTypeNode::Interval(_) => {
            let b = read_bytes(input, 8)?;
            let v = i64::from_le_bytes(b.try_into().unwrap());
            Ok(Value::BigInt(Some(v)))
        }

        // ── Array -> JSON array ────────────────────────────────────────────
        DataTypeNode::Array(inner) => {
            let count = read_leb128(input)?;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(value_to_json(decode_value(input, inner)?));
            }
            Ok(Value::Json(Some(Box::new(serde_json::Value::Array(items)))))
        }

        // ── Tuple -> JSON array ────────────────────────────────────────────
        DataTypeNode::Tuple(elements) => {
            let mut arr = Vec::with_capacity(elements.len());
            for el in elements {
                arr.push(value_to_json(decode_value(input, el)?));
            }
            Ok(Value::Json(Some(Box::new(serde_json::Value::Array(arr)))))
        }

        // ── Map -> JSON object ─────────────────────────────────────────────
        DataTypeNode::Map([key_type, val_type]) => {
            let count = read_leb128(input)?;
            let mut obj = serde_json::Map::new();
            for _ in 0..count {
                let k = decode_value(input, key_type)?;
                let v = decode_value(input, val_type)?;
                let key_str = match k {
                    Value::String(Some(s)) => s,
                    other => format!("{other:?}"),
                };
                obj.insert(key_str, value_to_json(v));
            }
            Ok(Value::Json(Some(Box::new(serde_json::Value::Object(obj)))))
        }

        // ── JSON ──────────────────────────────────────────────────────────
        DataTypeNode::JSON => {
            let len = read_leb128(input)?;
            let bytes = read_bytes(input, len)?;
            let s = std::str::from_utf8(bytes)
                .map_err(|e| Error::Custom(format!("JSON: invalid UTF-8: {e}")))?;
            let json: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| Error::Custom(format!("JSON: parse error: {e}")))?;
            Ok(Value::Json(Some(Box::new(json))))
        }

        // ── Geo types -> JSON ──────────────────────────────────────────────
        DataTypeNode::Point => {
            let b = read_bytes(input, 16)?;
            let x = f64::from_le_bytes(b[0..8].try_into().unwrap());
            let y = f64::from_le_bytes(b[8..16].try_into().unwrap());
            Ok(Value::Json(Some(Box::new(serde_json::json!([x, y])))))
        }
        DataTypeNode::Ring | DataTypeNode::LineString => {
            let count = read_leb128(input)?;
            let mut pts = Vec::with_capacity(count);
            for _ in 0..count {
                let b = read_bytes(input, 16)?;
                let x = f64::from_le_bytes(b[0..8].try_into().unwrap());
                let y = f64::from_le_bytes(b[8..16].try_into().unwrap());
                pts.push(serde_json::json!([x, y]));
            }
            Ok(Value::Json(Some(Box::new(serde_json::Value::Array(pts)))))
        }
        DataTypeNode::MultiLineString | DataTypeNode::Polygon => {
            let outer_count = read_leb128(input)?;
            let mut outer = Vec::with_capacity(outer_count);
            for _ in 0..outer_count {
                let inner_count = read_leb128(input)?;
                let mut inner_arr = Vec::with_capacity(inner_count);
                for _ in 0..inner_count {
                    let b = read_bytes(input, 16)?;
                    let x = f64::from_le_bytes(b[0..8].try_into().unwrap());
                    let y = f64::from_le_bytes(b[8..16].try_into().unwrap());
                    inner_arr.push(serde_json::json!([x, y]));
                }
                outer.push(serde_json::Value::Array(inner_arr));
            }
            Ok(Value::Json(Some(Box::new(serde_json::Value::Array(outer)))))
        }
        DataTypeNode::MultiPolygon => {
            let poly_count = read_leb128(input)?;
            let mut polys = Vec::with_capacity(poly_count);
            for _ in 0..poly_count {
                let ring_count = read_leb128(input)?;
                let mut rings = Vec::with_capacity(ring_count);
                for _ in 0..ring_count {
                    let pt_count = read_leb128(input)?;
                    let mut pts = Vec::with_capacity(pt_count);
                    for _ in 0..pt_count {
                        let b = read_bytes(input, 16)?;
                        let x = f64::from_le_bytes(b[0..8].try_into().unwrap());
                        let y = f64::from_le_bytes(b[8..16].try_into().unwrap());
                        pts.push(serde_json::json!([x, y]));
                    }
                    rings.push(serde_json::Value::Array(pts));
                }
                polys.push(serde_json::Value::Array(rings));
            }
            Ok(Value::Json(Some(Box::new(serde_json::Value::Array(polys)))))
        }

        // ── Unsupported complex types ─────────────────────────────────────
        _ => Err(Error::Unsupported(format!(
            "Type `{dt}` is not supported in DataRowCursor; \
             consider fetching as String or using a typed Row instead"
        ))),
    }
}
