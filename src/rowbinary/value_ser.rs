use crate::data_row::DataRow;
use crate::error::{Error, Result};
use bytes::BufMut;
#[cfg(any(feature = "chrono", feature = "time"))]
use clickhouse_types::data_types::DateTimePrecision;
use clickhouse_types::data_types::{DecimalType, EnumType};
use clickhouse_types::put_leb128;
use clickhouse_types::{Column, DataTypeNode};
use sea_query::Value;
#[cfg(feature = "rust_decimal")]
use sea_query::value::prelude::Decimal;
#[cfg(feature = "uuid")]
use sea_query::value::prelude::Uuid;
#[cfg(feature = "chrono")]
use sea_query::value::prelude::{NaiveDate, NaiveDateTime, NaiveTime};

/// Serialize a [`DataRow`] into the RowBinary format.
///
/// When `columns` is provided (validation / schema-aware mode), each value is
/// encoded according to its exact ClickHouse column type. Nullable columns receive
/// the 1-byte null flag; Date/DateTime/UUID values use their correct wire formats.
///
/// When `columns` is `None` (no-schema mode), a best-effort encoding is used:
/// - `None` values emit `1u8` (nullable null).
/// - `Some(v)` values are written directly without a null byte.
/// - Complex types (Date, DateTime, UUID, BigDecimal, Decimal, JSON) return an error;
///   call [`crate::Client::insert_data_row`] with validation enabled for those.
pub(crate) fn serialize_data_row<B: BufMut>(
    buf: &mut B,
    row: &DataRow,
    columns: Option<&[Column]>,
) -> Result<()> {
    if let Some(cols) = columns {
        if cols.len() != row.values.len() {
            return Err(Error::Custom(format!(
                "DataRow has {} values but {} columns in schema",
                row.values.len(),
                cols.len()
            )));
        }
        for (col, val) in cols.iter().zip(&row.values) {
            serialize_typed(buf, val, &col.data_type)?;
        }
    } else {
        for val in &row.values {
            serialize_untyped(buf, val)?;
        }
    }
    Ok(())
}

fn is_null(val: &Value) -> bool {
    #[allow(unused_mut)]
    let mut null = matches!(
        val,
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
            | Value::Json(None)
    );
    #[cfg(feature = "rust_decimal")]
    {
        null = null || matches!(val, Value::Decimal(None));
    }
    #[cfg(feature = "bigdecimal")]
    {
        null = null || matches!(val, Value::BigDecimal(None));
    }
    #[cfg(feature = "chrono")]
    {
        null = null
            || matches!(
                val,
                Value::ChronoDate(None) | Value::ChronoDateTime(None) | Value::ChronoTime(None)
            );
    }
    #[cfg(feature = "time")]
    {
        null = null
            || matches!(
                val,
                Value::TimeDate(None) | Value::TimeDateTime(None) | Value::TimeTime(None)
            );
    }
    #[cfg(feature = "uuid")]
    {
        null = null || matches!(val, Value::Uuid(None));
    }
    null
}

fn serialize_typed<B: BufMut>(buf: &mut B, val: &Value, dt: &DataTypeNode) -> Result<()> {
    let dt = dt.remove_low_cardinality();
    match dt {
        DataTypeNode::Nullable(inner) => {
            if is_null(val) {
                buf.put_u8(1);
                return Ok(());
            }
            buf.put_u8(0);
            serialize_typed(buf, val, inner)
        }

        DataTypeNode::Bool => match val {
            Value::Bool(Some(v)) => Ok(buf.put_u8(*v as u8)),
            _ => Err(type_mismatch("Bool", val)),
        },

        DataTypeNode::Int8 => match val {
            Value::TinyInt(Some(v)) => Ok(buf.put_i8(*v)),
            _ => Err(type_mismatch("Int8", val)),
        },
        DataTypeNode::Int16 => match val {
            Value::SmallInt(Some(v)) => Ok(buf.put_i16_le(*v)),
            _ => Err(type_mismatch("Int16", val)),
        },
        DataTypeNode::Int32 => match val {
            Value::Int(Some(v)) => Ok(buf.put_i32_le(*v)),
            _ => Err(type_mismatch("Int32", val)),
        },
        DataTypeNode::Int64 | DataTypeNode::Interval(_) => match val {
            Value::BigInt(Some(v)) => Ok(buf.put_i64_le(*v)),
            _ => Err(type_mismatch("Int64/Interval", val)),
        },

        DataTypeNode::UInt8 => match val {
            Value::TinyUnsigned(Some(v)) => Ok(buf.put_u8(*v)),
            _ => Err(type_mismatch("UInt8", val)),
        },
        DataTypeNode::UInt16 => match val {
            Value::SmallUnsigned(Some(v)) => Ok(buf.put_u16_le(*v)),
            _ => Err(type_mismatch("UInt16", val)),
        },
        DataTypeNode::UInt32 => match val {
            Value::Unsigned(Some(v)) => Ok(buf.put_u32_le(*v)),
            _ => Err(type_mismatch("UInt32", val)),
        },
        DataTypeNode::UInt64 => match val {
            Value::BigUnsigned(Some(v)) => Ok(buf.put_u64_le(*v)),
            _ => Err(type_mismatch("UInt64", val)),
        },

        DataTypeNode::Float32 | DataTypeNode::BFloat16 => match val {
            Value::Float(Some(v)) => Ok(buf.put_f32_le(*v)),
            _ => Err(type_mismatch("Float32", val)),
        },
        DataTypeNode::Float64 => match val {
            Value::Double(Some(v)) => Ok(buf.put_f64_le(*v)),
            _ => Err(type_mismatch("Float64", val)),
        },

        DataTypeNode::String => match val {
            Value::String(Some(s)) => {
                put_leb128(&mut *buf, s.len() as u64);
                buf.put_slice(s.as_bytes());
                Ok(())
            }
            _ => Err(type_mismatch("String", val)),
        },
        DataTypeNode::FixedString(n) => match val {
            Value::Bytes(Some(b)) => {
                if b.len() > *n {
                    return Err(Error::Custom(format!(
                        "FixedString({n}): value is {} bytes, exceeds column size",
                        b.len()
                    )));
                }
                buf.put_slice(b);
                for _ in b.len()..*n {
                    buf.put_u8(0);
                }
                Ok(())
            }
            _ => Err(type_mismatch("FixedString", val)),
        },

        #[cfg(feature = "uuid")]
        DataTypeNode::UUID => match val {
            Value::Uuid(Some(uuid)) => Ok(encode_uuid(buf, uuid)),
            _ => Err(type_mismatch("UUID", val)),
        },
        #[cfg(not(feature = "uuid"))]
        DataTypeNode::UUID => Err(Error::Unsupported(
            "UUID serialization requires the `uuid` feature".into(),
        )),

        #[cfg(feature = "chrono")]
        DataTypeNode::Date => match val {
            Value::ChronoDate(Some(d)) => {
                buf.put_u16_le(chrono_days_from_unix_epoch(d) as u16);
                Ok(())
            }
            _ => Err(type_mismatch("Date", val)),
        },
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Date => match val {
            Value::TimeDate(Some(d)) => {
                buf.put_u16_le(time_days_from_unix_epoch(d) as u16);
                Ok(())
            }
            _ => Err(type_mismatch("Date", val)),
        },
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Date => Err(Error::Unsupported(
            "Date serialization requires the `chrono` or `time` feature".into(),
        )),

        #[cfg(feature = "chrono")]
        DataTypeNode::Date32 => match val {
            Value::ChronoDate(Some(d)) => {
                buf.put_i32_le(chrono_days_from_unix_epoch(d));
                Ok(())
            }
            _ => Err(type_mismatch("Date32", val)),
        },
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Date32 => match val {
            Value::TimeDate(Some(d)) => {
                buf.put_i32_le(time_days_from_unix_epoch(d));
                Ok(())
            }
            _ => Err(type_mismatch("Date32", val)),
        },
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Date32 => Err(Error::Unsupported(
            "Date32 serialization requires the `chrono` or `time` feature".into(),
        )),

        #[cfg(feature = "chrono")]
        DataTypeNode::DateTime(_tz) => match val {
            Value::ChronoDateTime(Some(dt)) => {
                buf.put_u32_le(chrono_datetime_timestamp(dt) as u32);
                Ok(())
            }
            _ => Err(type_mismatch("DateTime", val)),
        },
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::DateTime(_tz) => match val {
            Value::TimeDateTime(Some(dt)) => {
                buf.put_u32_le(time_datetime_timestamp(dt) as u32);
                Ok(())
            }
            _ => Err(type_mismatch("DateTime", val)),
        },
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::DateTime(_tz) => Err(Error::Unsupported(
            "DateTime serialization requires the `chrono` or `time` feature".into(),
        )),

        #[cfg(feature = "chrono")]
        DataTypeNode::DateTime64(prec, _tz) => match val {
            Value::ChronoDateTime(Some(dt)) => {
                let scale = precision_scale(prec);
                let secs = chrono_datetime_timestamp(dt);
                let subsec_nanos = chrono_datetime_subsec_nanos(dt) as i64;
                let ticks = secs * scale + subsec_nanos / (1_000_000_000 / scale);
                buf.put_i64_le(ticks);
                Ok(())
            }
            _ => Err(type_mismatch("DateTime64", val)),
        },
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::DateTime64(prec, _tz) => match val {
            Value::TimeDateTime(Some(dt)) => {
                let scale = precision_scale(prec);
                let secs = time_datetime_timestamp(dt);
                let subsec_nanos = time_datetime_subsec_nanos(dt) as i64;
                let ticks = secs * scale + subsec_nanos / (1_000_000_000 / scale);
                buf.put_i64_le(ticks);
                Ok(())
            }
            _ => Err(type_mismatch("DateTime64", val)),
        },
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::DateTime64(_prec, _tz) => Err(Error::Unsupported(
            "DateTime64 serialization requires the `chrono` or `time` feature".into(),
        )),

        #[cfg(feature = "chrono")]
        DataTypeNode::Time => match val {
            Value::ChronoTime(Some(t)) => {
                buf.put_i32_le(chrono_time_secs(t));
                Ok(())
            }
            _ => Err(type_mismatch("Time", val)),
        },
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Time => match val {
            Value::TimeTime(Some(t)) => {
                buf.put_i32_le(time_time_secs(t));
                Ok(())
            }
            _ => Err(type_mismatch("Time", val)),
        },
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Time => Err(Error::Unsupported(
            "Time serialization requires the `chrono` or `time` feature".into(),
        )),

        #[cfg(feature = "chrono")]
        DataTypeNode::Time64(prec) => match val {
            Value::ChronoTime(Some(t)) => {
                let scale = precision_scale(prec);
                let secs = chrono_time_secs(t) as i64;
                let nanos = chrono_time_subsec_nanos(t) as i64;
                let ticks = secs * scale + nanos / (1_000_000_000 / scale);
                buf.put_i64_le(ticks);
                Ok(())
            }
            _ => Err(type_mismatch("Time64", val)),
        },
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataTypeNode::Time64(prec) => match val {
            Value::TimeTime(Some(t)) => {
                let scale = precision_scale(prec);
                let secs = time_time_secs(t) as i64;
                let nanos = time_time_subsec_nanos(t) as i64;
                let ticks = secs * scale + nanos / (1_000_000_000 / scale);
                buf.put_i64_le(ticks);
                Ok(())
            }
            _ => Err(type_mismatch("Time64", val)),
        },
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataTypeNode::Time64(_prec) => Err(Error::Unsupported(
            "Time64 serialization requires the `chrono` or `time` feature".into(),
        )),

        #[cfg(feature = "rust_decimal")]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal32) => match val {
            Value::Decimal(Some(d)) => {
                buf.put_i32_le(decimal_to_raw_i64(d, *scale) as i32);
                Ok(())
            }
            _ => Err(type_mismatch("Decimal32", val)),
        },
        #[cfg(all(feature = "bigdecimal", not(feature = "rust_decimal")))]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal32) => match val {
            Value::BigDecimal(Some(d)) => {
                buf.put_i32_le(bigdecimal_to_raw_i128(d, *scale)? as i32);
                Ok(())
            }
            _ => Err(type_mismatch("Decimal32", val)),
        },
        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal32) => Err(Error::Unsupported(
            "Decimal serialization requires the `rust_decimal` or `bigdecimal` feature".into(),
        )),

        #[cfg(feature = "rust_decimal")]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal64) => match val {
            Value::Decimal(Some(d)) => {
                buf.put_i64_le(decimal_to_raw_i64(d, *scale));
                Ok(())
            }
            _ => Err(type_mismatch("Decimal64", val)),
        },
        #[cfg(all(feature = "bigdecimal", not(feature = "rust_decimal")))]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal64) => match val {
            Value::BigDecimal(Some(d)) => {
                buf.put_i64_le(bigdecimal_to_raw_i128(d, *scale)? as i64);
                Ok(())
            }
            _ => Err(type_mismatch("Decimal64", val)),
        },
        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal64) => Err(Error::Unsupported(
            "Decimal serialization requires the `rust_decimal` or `bigdecimal` feature".into(),
        )),

        #[cfg(all(feature = "rust_decimal", feature = "bigdecimal"))]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal128) => {
            let raw = match val {
                Value::Decimal(Some(d)) => decimal_to_raw_i128(d, *scale),
                Value::BigDecimal(Some(d)) => bigdecimal_to_raw_i128(d, *scale)?,
                _ => return Err(type_mismatch("Decimal128", val)),
            };
            buf.put_i128_le(raw);
            Ok(())
        }
        #[cfg(all(feature = "rust_decimal", not(feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal128) => match val {
            Value::Decimal(Some(d)) => {
                buf.put_i128_le(decimal_to_raw_i128(d, *scale));
                Ok(())
            }
            _ => Err(type_mismatch("Decimal128", val)),
        },
        #[cfg(all(feature = "bigdecimal", not(feature = "rust_decimal")))]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal128) => match val {
            Value::BigDecimal(Some(d)) => {
                buf.put_i128_le(bigdecimal_to_raw_i128(d, *scale)?);
                Ok(())
            }
            _ => Err(type_mismatch("Decimal128", val)),
        },
        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal128) => Err(Error::Unsupported(
            "Decimal128 serialization requires the `rust_decimal` or `bigdecimal` feature".into(),
        )),

        #[cfg(all(feature = "rust_decimal", feature = "bigdecimal"))]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal256) => {
            let le32 = match val {
                Value::Decimal(Some(d)) => {
                    let raw = decimal_to_raw_i128(d, *scale);
                    let mut b = [if raw < 0 { 0xffu8 } else { 0u8 }; 32];
                    b[..16].copy_from_slice(&raw.to_le_bytes());
                    b
                }
                Value::BigDecimal(Some(d)) => bigdecimal_to_raw_le256(d, *scale)?,
                _ => return Err(type_mismatch("Decimal256", val)),
            };
            buf.put_slice(&le32);
            Ok(())
        }
        #[cfg(all(feature = "rust_decimal", not(feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal256) => match val {
            Value::Decimal(Some(d)) => {
                let raw = decimal_to_raw_i128(d, *scale);
                let mut b = [if raw < 0 { 0xffu8 } else { 0u8 }; 32];
                b[..16].copy_from_slice(&raw.to_le_bytes());
                buf.put_slice(&b);
                Ok(())
            }
            _ => Err(type_mismatch("Decimal256", val)),
        },
        #[cfg(all(feature = "bigdecimal", not(feature = "rust_decimal")))]
        DataTypeNode::Decimal(_, scale, DecimalType::Decimal256) => match val {
            Value::BigDecimal(Some(d)) => {
                buf.put_slice(&bigdecimal_to_raw_le256(d, *scale)?);
                Ok(())
            }
            _ => Err(type_mismatch("Decimal256", val)),
        },
        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal")))]
        DataTypeNode::Decimal(_, _, DecimalType::Decimal256) => Err(Error::Unsupported(
            "Decimal256 serialization requires the `rust_decimal` or `bigdecimal` feature".into(),
        )),
        DataTypeNode::Int128
        | DataTypeNode::UInt128
        | DataTypeNode::Int256
        | DataTypeNode::UInt256 => Err(Error::Unsupported(format!(
            "Insert from DataRow is not yet supported for type {dt}"
        ))),

        DataTypeNode::IPv4 => match val {
            Value::String(Some(s)) => {
                let ip: std::net::Ipv4Addr = s
                    .parse()
                    .map_err(|e| Error::Custom(format!("IPv4 parse error for '{s}': {e}")))?;
                buf.put_u32_le(u32::from(ip));
                Ok(())
            }
            _ => Err(type_mismatch("IPv4", val)),
        },
        DataTypeNode::IPv6 => match val {
            Value::String(Some(s)) => {
                let ip: std::net::Ipv6Addr = s
                    .parse()
                    .map_err(|e| Error::Custom(format!("IPv6 parse error for '{s}': {e}")))?;
                buf.put_slice(&ip.octets());
                Ok(())
            }
            _ => Err(type_mismatch("IPv6", val)),
        },

        DataTypeNode::Enum(enum_type, map) => match val {
            Value::String(Some(s)) => {
                let discriminant = map
                    .iter()
                    .find_map(|(k, v)| if v == s.as_str() { Some(*k) } else { None })
                    .ok_or_else(|| Error::Custom(format!("Enum: unknown value '{s}'")))?;
                match enum_type {
                    EnumType::Enum8 => buf.put_i8(discriminant as i8),
                    EnumType::Enum16 => buf.put_i16_le(discriminant),
                }
                Ok(())
            }
            _ => Err(type_mismatch("Enum", val)),
        },

        DataTypeNode::Array(_)
        | DataTypeNode::Tuple(_)
        | DataTypeNode::Map(_)
        | DataTypeNode::JSON => match val {
            Value::Json(Some(j)) => {
                let s = j.to_string();
                put_leb128(&mut *buf, s.len() as u64);
                buf.put_slice(s.as_bytes());
                Ok(())
            }
            _ => Err(type_mismatch("Array/Tuple/Map/JSON", val)),
        },

        _ => Err(Error::Unsupported(format!(
            "Insert from DataRow is not supported for type {dt}"
        ))),
    }
}

fn serialize_untyped<B: BufMut>(buf: &mut B, val: &Value) -> Result<()> {
    match val {
        Value::Bool(Some(v)) => buf.put_u8(*v as u8),
        Value::Bool(None) => buf.put_u8(1),

        Value::TinyInt(Some(v)) => buf.put_i8(*v),
        Value::TinyInt(None) => buf.put_u8(1),

        Value::SmallInt(Some(v)) => buf.put_i16_le(*v),
        Value::SmallInt(None) => buf.put_u8(1),

        Value::Int(Some(v)) => buf.put_i32_le(*v),
        Value::Int(None) => buf.put_u8(1),

        Value::BigInt(Some(v)) => buf.put_i64_le(*v),
        Value::BigInt(None) => buf.put_u8(1),

        Value::TinyUnsigned(Some(v)) => buf.put_u8(*v),
        Value::TinyUnsigned(None) => buf.put_u8(1),

        Value::SmallUnsigned(Some(v)) => buf.put_u16_le(*v),
        Value::SmallUnsigned(None) => buf.put_u8(1),

        Value::Unsigned(Some(v)) => buf.put_u32_le(*v),
        Value::Unsigned(None) => buf.put_u8(1),

        Value::BigUnsigned(Some(v)) => buf.put_u64_le(*v),
        Value::BigUnsigned(None) => buf.put_u8(1),

        Value::Float(Some(v)) => buf.put_f32_le(*v),
        Value::Float(None) => buf.put_u8(1),

        Value::Double(Some(v)) => buf.put_f64_le(*v),
        Value::Double(None) => buf.put_u8(1),

        Value::String(Some(s)) => {
            put_leb128(&mut *buf, s.len() as u64);
            buf.put_slice(s.as_bytes());
        }
        Value::String(None) => buf.put_u8(1),

        Value::Bytes(Some(b)) => {
            put_leb128(&mut *buf, b.len() as u64);
            buf.put_slice(b);
        }
        Value::Bytes(None) => buf.put_u8(1),

        Value::Json(Some(j)) => {
            let s = j.to_string();
            put_leb128(&mut *buf, s.len() as u64);
            buf.put_slice(s.as_bytes());
        }
        Value::Json(None) => buf.put_u8(1),

        _ => {
            return Err(Error::Custom(format!(
                "Cannot serialize {val:?} without column type info; \
                 use Client::insert_data_row with validation enabled for complex types"
            )));
        }
    }
    Ok(())
}

// ── helpers ──────────────────────────────────────────────────────────────────

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

#[cfg(feature = "uuid")]
fn encode_uuid<B: BufMut>(buf: &mut B, uuid: &Uuid) {
    let bits = uuid.as_u128();
    buf.put_u64_le((bits >> 64) as u64);
    buf.put_u64_le(bits as u64);
}

// ── chrono helpers ───────────────────────────────────────────────────────────

#[cfg(feature = "chrono")]
fn chrono_days_from_unix_epoch(d: &NaiveDate) -> i32 {
    let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date");
    d.signed_duration_since(unix_epoch).num_days() as i32
}

#[cfg(feature = "chrono")]
fn chrono_datetime_timestamp(dt: &NaiveDateTime) -> i64 {
    dt.and_utc().timestamp()
}

#[cfg(feature = "chrono")]
fn chrono_datetime_subsec_nanos(dt: &NaiveDateTime) -> u32 {
    dt.and_utc().timestamp_subsec_nanos()
}

#[cfg(feature = "chrono")]
fn chrono_time_secs(t: &NaiveTime) -> i32 {
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("valid time");
    t.signed_duration_since(midnight).num_seconds() as i32
}

#[cfg(feature = "chrono")]
fn chrono_time_subsec_nanos(t: &NaiveTime) -> u32 {
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("valid time");
    let dur = t.signed_duration_since(midnight);
    let total_nanos = dur.num_nanoseconds().unwrap_or(0);
    let secs_nanos = dur.num_seconds() * 1_000_000_000;
    (total_nanos - secs_nanos) as u32
}

// ── time helpers ─────────────────────────────────────────────────────────────

/// Julian day of the Unix epoch (1970-01-01).
#[cfg(all(feature = "time", not(feature = "chrono")))]
const UNIX_EPOCH_JULIAN_DAY: i32 = 2_440_588;

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn time_days_from_unix_epoch(d: &sea_query::value::prelude::time::Date) -> i32 {
    d.to_julian_day() - UNIX_EPOCH_JULIAN_DAY
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn time_datetime_timestamp(dt: &sea_query::value::prelude::PrimitiveDateTime) -> i64 {
    dt.assume_utc().unix_timestamp()
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn time_datetime_subsec_nanos(dt: &sea_query::value::prelude::PrimitiveDateTime) -> u32 {
    dt.nanosecond()
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn time_time_secs(t: &sea_query::value::prelude::time::Time) -> i32 {
    (t.hour() as i32) * 3600 + (t.minute() as i32) * 60 + t.second() as i32
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn time_time_subsec_nanos(t: &sea_query::value::prelude::time::Time) -> u32 {
    t.nanosecond()
}

// ── decimal helpers ──────────────────────────────────────────────────────────

#[cfg(feature = "rust_decimal")]
fn decimal_to_raw_i64(d: &Decimal, target_scale: u8) -> i64 {
    let mantissa = d.mantissa();
    let diff = target_scale as i32 - d.scale() as i32;
    if diff >= 0 {
        mantissa.saturating_mul(10i128.pow(diff as u32)) as i64
    } else {
        (mantissa / 10i128.pow((-diff) as u32)) as i64
    }
}

#[cfg(feature = "rust_decimal")]
fn decimal_to_raw_i128(d: &Decimal, target_scale: u8) -> i128 {
    let mantissa = d.mantissa();
    let diff = target_scale as i32 - d.scale() as i32;
    if diff >= 0 {
        mantissa.saturating_mul(10i128.pow(diff as u32))
    } else {
        mantissa / 10i128.pow((-diff) as u32)
    }
}

#[cfg(feature = "bigdecimal")]
fn bigdecimal_to_raw_i128(d: &sea_query::prelude::BigDecimal, target_scale: u8) -> Result<i128> {
    use sea_query::prelude::bigdecimal::ToPrimitive;
    let rescaled = d.clone().with_scale(target_scale as i64);
    let (bigint, _) = rescaled.into_bigint_and_exponent();
    bigint
        .to_i128()
        .ok_or_else(|| Error::Custom(format!("Decimal128: value out of i128 range")))
}

#[cfg(feature = "bigdecimal")]
fn bigdecimal_to_raw_le256(
    d: &sea_query::prelude::BigDecimal,
    target_scale: u8,
) -> Result<[u8; 32]> {
    use sea_query::prelude::bigdecimal::num_bigint::Sign;
    let rescaled = d.clone().with_scale(target_scale as i64);
    let (bigint, _) = rescaled.into_bigint_and_exponent();
    let (sign, magnitude) = bigint.to_bytes_be();
    if magnitude.len() > 32 {
        return Err(Error::Custom(
            "Decimal256: value exceeds 256-bit range".into(),
        ));
    }
    let mut be = [0u8; 32];
    let start = 32 - magnitude.len();
    be[start..].copy_from_slice(&magnitude);
    if sign == Sign::Minus {
        for b in &mut be {
            *b = !*b;
        }
        let mut carry = true;
        for b in be.iter_mut().rev() {
            let (nb, c) = b.overflowing_add(carry as u8);
            *b = nb;
            carry = c;
            if !carry {
                break;
            }
        }
    }
    be.reverse();
    Ok(be)
}

fn type_mismatch(expected: &str, val: &Value) -> Error {
    Error::Custom(format!(
        "Type mismatch while serializing DataRow: expected {expected}, got {val:?}"
    ))
}
