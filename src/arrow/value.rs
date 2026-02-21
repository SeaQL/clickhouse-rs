use sea_orm_arrow::arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Decimal256Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, TimeUnit, i256},
};
use sea_query::{
    Value,
    prelude::{Decimal, Uuid, chrono},
};

use crate::error::{Error, Result};

/// Converts the element at `row` in an Arrow array to a [`sea_query::Value`].
///
/// Dispatches on the Arrow [`DataType`] from the batch schema.
/// Null slots (Arrow validity bitmap) produce `Value::X(None)`.
///
/// # Supported types
///
/// | Arrow `DataType` | `sea_query::Value` |
/// |---|---|
/// | Boolean | `Bool` |
/// | Int8/16/32/64, UInt8/16/32/64 | `TinyInt` / `SmallInt` / `Int` / `BigInt` (and unsigned) |
/// | Float32/64 | `Float` / `Double` |
/// | Utf8/LargeUtf8 | `String` |
/// | Binary/LargeBinary | `Bytes` |
/// | FixedSizeBinary(16) | `Uuid` (interpret bytes as UUID) |
/// | FixedSizeBinary(_) | `Bytes` |
/// | Date32/Date64 | `ChronoDate` |
/// | Timestamp(s/ms/us/ns) | `ChronoDateTime` |
/// | Time32(s/ms)/Time64(us/ns) | `ChronoTime` |
/// | Decimal128 | `Decimal` (rust_decimal, or `BigDecimal` if scale > 28) |
/// | Decimal256 | `BigDecimal` |
pub(crate) fn element_to_value(array: &dyn Array, dtype: &DataType, row: usize) -> Result<Value> {
    if array.is_null(row) {
        return Ok(null_value(dtype));
    }

    let v = match dtype {
        DataType::Boolean => {
            let a = downcast::<BooleanArray>(array, "Boolean")?;
            Value::Bool(Some(a.value(row)))
        }

        DataType::Int8 => Value::TinyInt(Some(downcast::<Int8Array>(array, "Int8")?.value(row))),
        DataType::Int16 => {
            Value::SmallInt(Some(downcast::<Int16Array>(array, "Int16")?.value(row)))
        }
        DataType::Int32 => Value::Int(Some(downcast::<Int32Array>(array, "Int32")?.value(row))),
        DataType::Int64 => Value::BigInt(Some(downcast::<Int64Array>(array, "Int64")?.value(row))),

        DataType::UInt8 => {
            Value::TinyUnsigned(Some(downcast::<UInt8Array>(array, "UInt8")?.value(row)))
        }
        DataType::UInt16 => {
            Value::SmallUnsigned(Some(downcast::<UInt16Array>(array, "UInt16")?.value(row)))
        }
        DataType::UInt32 => {
            Value::Unsigned(Some(downcast::<UInt32Array>(array, "UInt32")?.value(row)))
        }
        DataType::UInt64 => {
            Value::BigUnsigned(Some(downcast::<UInt64Array>(array, "UInt64")?.value(row)))
        }

        DataType::Float32 => {
            Value::Float(Some(downcast::<Float32Array>(array, "Float32")?.value(row)))
        }
        DataType::Float64 => {
            Value::Double(Some(downcast::<Float64Array>(array, "Float64")?.value(row)))
        }

        DataType::Utf8 => {
            let s = downcast::<StringArray>(array, "Utf8")?.value(row);
            Value::String(Some(s.to_owned()))
        }
        DataType::LargeUtf8 => {
            let s = downcast::<LargeStringArray>(array, "LargeUtf8")?.value(row);
            Value::String(Some(s.to_owned()))
        }

        DataType::Binary => {
            let b = downcast::<BinaryArray>(array, "Binary")?.value(row);
            Value::Bytes(Some(b.to_vec()))
        }
        DataType::LargeBinary => {
            let b = downcast::<LargeBinaryArray>(array, "LargeBinary")?.value(row);
            Value::Bytes(Some(b.to_vec()))
        }

        // FixedSizeBinary(16) is used for UUID in this crate's Arrow schema.
        // Interpret as UUID so that serialize_typed handles it correctly for UUID columns.
        DataType::FixedSizeBinary(16) => {
            let b = downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary(16)")?.value(row);
            let bytes: [u8; 16] = b
                .try_into()
                .map_err(|_| Error::Other("FixedSizeBinary(16): unexpected slice length".into()))?;
            Value::Uuid(Some(Uuid::from_bytes(bytes)))
        }
        DataType::FixedSizeBinary(_) => {
            let b = downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary")?.value(row);
            Value::Bytes(Some(b.to_vec()))
        }

        DataType::Date32 => arrow_to_chrono_date(array, row)?,
        DataType::Date64 => arrow_to_chrono_date(array, row)?,

        DataType::Timestamp(TimeUnit::Second, _) => {
            let secs = downcast::<TimestampSecondArray>(array, "Timestamp(s)")?.value(row);
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
                .ok_or_else(|| {
                    Error::Other(format!("timestamp seconds out of range: {secs}").into())
                })?
                .naive_utc();
            Value::ChronoDateTime(Some(dt))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ms = downcast::<TimestampMillisecondArray>(array, "Timestamp(ms)")?.value(row);
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
                .ok_or_else(|| Error::Other(format!("timestamp ms out of range: {ms}").into()))?
                .naive_utc();
            Value::ChronoDateTime(Some(dt))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let us = downcast::<TimestampMicrosecondArray>(array, "Timestamp(us)")?.value(row);
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(us)
                .ok_or_else(|| Error::Other(format!("timestamp us out of range: {us}").into()))?
                .naive_utc();
            Value::ChronoDateTime(Some(dt))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let ns = downcast::<TimestampNanosecondArray>(array, "Timestamp(ns)")?.value(row);
            let secs = ns.div_euclid(1_000_000_000);
            let nsec = ns.rem_euclid(1_000_000_000) as u32;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsec)
                .ok_or_else(|| Error::Other(format!("timestamp ns out of range: {ns}").into()))?
                .naive_utc();
            Value::ChronoDateTime(Some(dt))
        }

        DataType::Time32(TimeUnit::Second) => {
            let secs = downcast::<Time32SecondArray>(array, "Time32(s)")?.value(row) as u32;
            let t = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, 0)
                .ok_or_else(|| Error::Other(format!("Time32Second out of range: {secs}").into()))?;
            Value::ChronoTime(Some(t))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let ms = downcast::<Time32MillisecondArray>(array, "Time32(ms)")?.value(row);
            let secs = (ms / 1_000) as u32;
            let nanos = ((ms % 1_000) * 1_000_000) as u32;
            let t = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).ok_or_else(
                || Error::Other(format!("Time32Millisecond out of range: {ms}").into()),
            )?;
            Value::ChronoTime(Some(t))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let us = downcast::<Time64MicrosecondArray>(array, "Time64(us)")?.value(row);
            let secs = (us / 1_000_000) as u32;
            let nanos = ((us % 1_000_000) * 1_000) as u32;
            let t = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).ok_or_else(
                || Error::Other(format!("Time64Microsecond out of range: {us}").into()),
            )?;
            Value::ChronoTime(Some(t))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let ns = downcast::<Time64NanosecondArray>(array, "Time64(ns)")?.value(row);
            let secs = (ns / 1_000_000_000) as u32;
            let nanos = (ns % 1_000_000_000) as u32;
            let t = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).ok_or_else(
                || Error::Other(format!("Time64Nanosecond out of range: {ns}").into()),
            )?;
            Value::ChronoTime(Some(t))
        }

        DataType::Decimal128(_, scale) => {
            let a = downcast::<Decimal128Array>(array, "Decimal128")?;
            let raw = a.value(row);
            let scale = *scale;
            if scale >= 0 && scale <= 28 {
                let dec = Decimal::from_i128_with_scale(raw, scale as u32);
                Value::Decimal(Some(dec))
            } else {
                bigdecimal_from_i128(raw, scale as i64)?
            }
        }

        DataType::Decimal256(_, scale) => {
            let a = downcast::<Decimal256Array>(array, "Decimal256")?;
            decimal256_to_value(a.value(row), *scale)?
        }

        other => {
            return Err(Error::Other(
                format!("unsupported Arrow type for ClickHouse insert: {other:?}").into(),
            ));
        }
    };

    Ok(v)
}

/// Returns the appropriate `Value::X(None)` for a given Arrow [`DataType`].
fn null_value(dtype: &DataType) -> Value {
    match dtype {
        DataType::Boolean => Value::Bool(None),
        DataType::Int8 => Value::TinyInt(None),
        DataType::Int16 => Value::SmallInt(None),
        DataType::Int32 => Value::Int(None),
        DataType::Int64 => Value::BigInt(None),
        DataType::UInt8 => Value::TinyUnsigned(None),
        DataType::UInt16 => Value::SmallUnsigned(None),
        DataType::UInt32 => Value::Unsigned(None),
        DataType::UInt64 => Value::BigUnsigned(None),
        DataType::Float32 => Value::Float(None),
        DataType::Float64 => Value::Double(None),
        DataType::Utf8 | DataType::LargeUtf8 => Value::String(None),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Value::Bytes(None)
        }
        DataType::Date32 | DataType::Date64 => Value::ChronoDate(None),
        DataType::Timestamp(_, _) => Value::ChronoDateTime(None),
        DataType::Time32(_) | DataType::Time64(_) => Value::ChronoTime(None),
        DataType::Decimal128(_, _) => Value::Decimal(None),
        DataType::Decimal256(_, _) => Value::BigDecimal(None),
        _ => Value::Bytes(None),
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn downcast<'a, T: 'static>(array: &'a dyn Array, name: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::Other(
            format!(
                "Arrow type mismatch: expected {name}, got {:?}",
                array.data_type()
            )
            .into(),
        )
    })
}

fn arrow_to_chrono_date(array: &dyn Array, row: usize) -> Result<Value> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    if let Some(a) = array.as_any().downcast_ref::<Date32Array>() {
        let days = a.value(row);
        let date = epoch
            .checked_add_signed(chrono::Duration::days(days as i64))
            .ok_or_else(|| Error::Other(format!("Date32 value {days} out of range").into()))?;
        return Ok(Value::ChronoDate(Some(date)));
    }
    if let Some(a) = array.as_any().downcast_ref::<Date64Array>() {
        let ms = a.value(row);
        let date = epoch
            .checked_add_signed(chrono::Duration::milliseconds(ms))
            .ok_or_else(|| Error::Other(format!("Date64 value {ms} out of range").into()))?;
        return Ok(Value::ChronoDate(Some(date)));
    }
    Err(Error::Other(
        format!(
            "Arrow type mismatch: expected Date32 or Date64, got {:?}",
            array.data_type()
        )
        .into(),
    ))
}

/// Falls back to `BigDecimal` for Decimal128 values whose scale exceeds rust_decimal limits.
fn bigdecimal_from_i128(value: i128, scale: i64) -> Result<Value> {
    use sea_query::prelude::bigdecimal::{BigDecimal, num_bigint::BigInt};
    let bigint = BigInt::from(value);
    let dec = BigDecimal::new(bigint, scale);
    Ok(Value::BigDecimal(Some(Box::new(dec))))
}

/// Converts an Arrow [`i256`] with the given `scale` to `Value::BigDecimal`.
fn decimal256_to_value(value: i256, scale: i8) -> Result<Value> {
    use sea_query::prelude::bigdecimal::{
        BigDecimal,
        num_bigint::{BigInt, Sign},
    };

    let bytes = value.to_be_bytes();

    let (sign, magnitude) = if value.is_negative() {
        // Two's-complement negate to recover the magnitude.
        let mut abs = [0u8; 32];
        let mut carry = true;
        for i in (0..32).rev() {
            let b = !bytes[i];
            let (nb, c) = b.overflowing_add(carry as u8);
            abs[i] = nb;
            carry = c;
        }
        (Sign::Minus, abs.to_vec())
    } else if value == i256::ZERO {
        (Sign::NoSign, vec![0])
    } else {
        let first_nonzero = bytes.iter().position(|&b| b != 0).unwrap_or(31);
        (Sign::Plus, bytes[first_nonzero..].to_vec())
    };

    let bigint = BigInt::from_bytes_be(sign, &magnitude);
    let dec = BigDecimal::new(bigint, scale as i64);
    Ok(Value::BigDecimal(Some(Box::new(dec))))
}
