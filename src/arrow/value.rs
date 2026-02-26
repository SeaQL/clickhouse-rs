#[cfg(any(feature = "chrono", feature = "time"))]
use sea_orm_arrow::arrow::array::{
    Date32Array, Date64Array, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use sea_orm_arrow::arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::{DataType, TimeUnit, i256},
};
use sea_query::Value;
use sea_query::prelude::Decimal;
#[cfg(feature = "uuid")]
use sea_query::prelude::Uuid;

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
/// | FixedSizeBinary(16) | `Uuid` (when `uuid` feature enabled) |
/// | FixedSizeBinary(_) | `Bytes` |
/// | Date32/Date64 | `ChronoDate` or `TimeDate` |
/// | Timestamp(s/ms/us/ns) | `ChronoDateTime` or `TimeDateTime` |
/// | Time32(s/ms)/Time64(us/ns) | `ChronoTime` or `TimeTime` |
/// | Decimal128 | `Decimal` (rust_decimal, or `BigDecimal` if scale > 28) |
/// | Decimal256 | `BigDecimal` |
pub(crate) fn element_to_value(array: &dyn Array, dtype: &DataType, row: usize) -> Result<Value> {
    if array.is_null(row) {
        return null_value(dtype);
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

        #[cfg(feature = "uuid")]
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

        DataType::Date32 | DataType::Date64 => arrow_to_date(array, dtype, row)?,

        DataType::Timestamp(unit, _) => arrow_to_datetime(array, unit, row)?,

        DataType::Time32(unit) | DataType::Time64(unit) => arrow_to_time(array, dtype, unit, row)?,

        DataType::Decimal128(_, scale) => {
            let a = downcast::<Decimal128Array>(array, "Decimal128")?;
            let raw = a.value(row);
            let scale = *scale;
            if let Ok(dec) = Decimal::try_from_i128_with_scale(raw, scale as u32) {
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
fn null_value(dtype: &DataType) -> Result<Value> {
    Ok(match dtype {
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

        #[cfg(feature = "chrono")]
        DataType::Date32 | DataType::Date64 => Value::ChronoDate(None),
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataType::Date32 | DataType::Date64 => Value::TimeDate(None),
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataType::Date32 | DataType::Date64 => {
            return Err(Error::Unsupported(
                "Date requires the `chrono` or `time` feature".into(),
            ));
        }

        #[cfg(feature = "chrono")]
        DataType::Timestamp(_, _) => Value::ChronoDateTime(None),
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataType::Timestamp(_, _) => Value::TimeDateTime(None),
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataType::Timestamp(_, _) => {
            return Err(Error::Unsupported(
                "Timestamp requires the `chrono` or `time` feature".into(),
            ));
        }

        #[cfg(feature = "chrono")]
        DataType::Time32(_) | DataType::Time64(_) => Value::ChronoTime(None),
        #[cfg(all(feature = "time", not(feature = "chrono")))]
        DataType::Time32(_) | DataType::Time64(_) => Value::TimeTime(None),
        #[cfg(not(any(feature = "chrono", feature = "time")))]
        DataType::Time32(_) | DataType::Time64(_) => {
            return Err(Error::Unsupported(
                "Time requires the `chrono` or `time` feature".into(),
            ));
        }

        DataType::Decimal128(_, _) => Value::Decimal(None),
        DataType::Decimal256(_, _) => Value::BigDecimal(None),
        _ => Value::Bytes(None),
    })
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

// ── Date ─────────────────────────────────────────────────────────────────────

#[cfg(feature = "chrono")]
fn arrow_to_date(array: &dyn Array, dtype: &DataType, row: usize) -> Result<Value> {
    use sea_query::prelude::chrono;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    match dtype {
        DataType::Date32 => {
            let days = downcast::<Date32Array>(array, "Date32")?.value(row);
            let date = epoch
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or_else(|| Error::Other(format!("Date32 value {days} out of range").into()))?;
            Ok(Value::ChronoDate(Some(date)))
        }
        DataType::Date64 => {
            let ms = downcast::<Date64Array>(array, "Date64")?.value(row);
            let date = epoch
                .checked_add_signed(chrono::Duration::milliseconds(ms))
                .ok_or_else(|| Error::Other(format!("Date64 value {ms} out of range").into()))?;
            Ok(Value::ChronoDate(Some(date)))
        }
        _ => unreachable!(),
    }
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn arrow_to_date(array: &dyn Array, dtype: &DataType, row: usize) -> Result<Value> {
    const UNIX_EPOCH_JULIAN: i32 = 2_440_588;
    match dtype {
        DataType::Date32 => {
            let days = downcast::<Date32Array>(array, "Date32")?.value(row);
            let date =
                sea_query::value::prelude::time::Date::from_julian_day(days + UNIX_EPOCH_JULIAN)
                    .map_err(|_| {
                        Error::Other(format!("Date32 value {days} out of range").into())
                    })?;
            Ok(Value::TimeDate(Some(date)))
        }
        DataType::Date64 => {
            let ms = downcast::<Date64Array>(array, "Date64")?.value(row);
            let days = (ms / 86_400_000) as i32;
            let date =
                sea_query::value::prelude::time::Date::from_julian_day(days + UNIX_EPOCH_JULIAN)
                    .map_err(|_| Error::Other(format!("Date64 value {ms} out of range").into()))?;
            Ok(Value::TimeDate(Some(date)))
        }
        _ => unreachable!(),
    }
}

#[cfg(not(any(feature = "chrono", feature = "time")))]
fn arrow_to_date(_array: &dyn Array, _dtype: &DataType, _row: usize) -> Result<Value> {
    Err(Error::Unsupported(
        "Date decoding requires the `chrono` or `time` feature".into(),
    ))
}

// ── DateTime / Timestamp ─────────────────────────────────────────────────────

#[cfg(feature = "chrono")]
fn arrow_to_datetime(array: &dyn Array, unit: &TimeUnit, row: usize) -> Result<Value> {
    use sea_query::prelude::chrono;
    let dt = match unit {
        TimeUnit::Second => {
            let secs = downcast::<TimestampSecondArray>(array, "Timestamp(s)")?.value(row);
            chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
                .ok_or_else(|| {
                    Error::Other(format!("timestamp seconds out of range: {secs}").into())
                })?
                .naive_utc()
        }
        TimeUnit::Millisecond => {
            let ms = downcast::<TimestampMillisecondArray>(array, "Timestamp(ms)")?.value(row);
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
                .ok_or_else(|| Error::Other(format!("timestamp ms out of range: {ms}").into()))?
                .naive_utc()
        }
        TimeUnit::Microsecond => {
            let us = downcast::<TimestampMicrosecondArray>(array, "Timestamp(us)")?.value(row);
            chrono::DateTime::<chrono::Utc>::from_timestamp_micros(us)
                .ok_or_else(|| Error::Other(format!("timestamp us out of range: {us}").into()))?
                .naive_utc()
        }
        TimeUnit::Nanosecond => {
            let ns = downcast::<TimestampNanosecondArray>(array, "Timestamp(ns)")?.value(row);
            let secs = ns.div_euclid(1_000_000_000);
            let nsec = ns.rem_euclid(1_000_000_000) as u32;
            chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsec)
                .ok_or_else(|| Error::Other(format!("timestamp ns out of range: {ns}").into()))?
                .naive_utc()
        }
    };
    Ok(Value::ChronoDateTime(Some(dt)))
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn arrow_to_datetime(array: &dyn Array, unit: &TimeUnit, row: usize) -> Result<Value> {
    use sea_query::value::prelude::{PrimitiveDateTime, time};
    let (secs, nanos) = match unit {
        TimeUnit::Second => {
            let s = downcast::<TimestampSecondArray>(array, "Timestamp(s)")?.value(row);
            (s, 0u32)
        }
        TimeUnit::Millisecond => {
            let ms = downcast::<TimestampMillisecondArray>(array, "Timestamp(ms)")?.value(row);
            (ms / 1_000, ((ms % 1_000) * 1_000_000) as u32)
        }
        TimeUnit::Microsecond => {
            let us = downcast::<TimestampMicrosecondArray>(array, "Timestamp(us)")?.value(row);
            (us / 1_000_000, ((us % 1_000_000) * 1_000) as u32)
        }
        TimeUnit::Nanosecond => {
            let ns = downcast::<TimestampNanosecondArray>(array, "Timestamp(ns)")?.value(row);
            (
                ns.div_euclid(1_000_000_000),
                ns.rem_euclid(1_000_000_000) as u32,
            )
        }
    };
    let odt = time::OffsetDateTime::from_unix_timestamp(secs)
        .map_err(|_| Error::Other(format!("timestamp out of range: {secs}s").into()))?
        + time::Duration::nanoseconds(nanos as i64);
    let dt = PrimitiveDateTime::new(odt.date(), odt.time());
    Ok(Value::TimeDateTime(Some(dt)))
}

#[cfg(not(any(feature = "chrono", feature = "time")))]
fn arrow_to_datetime(_array: &dyn Array, _unit: &TimeUnit, _row: usize) -> Result<Value> {
    Err(Error::Unsupported(
        "Timestamp decoding requires the `chrono` or `time` feature".into(),
    ))
}

// ── Time ─────────────────────────────────────────────────────────────────────

#[cfg(feature = "chrono")]
fn arrow_to_time(
    array: &dyn Array,
    dtype: &DataType,
    unit: &TimeUnit,
    row: usize,
) -> Result<Value> {
    use sea_query::prelude::chrono;
    let (secs, nanos) = match (dtype, unit) {
        (DataType::Time32(_), TimeUnit::Second) => (
            downcast::<Time32SecondArray>(array, "Time32(s)")?.value(row) as u32,
            0u32,
        ),
        (DataType::Time32(_), TimeUnit::Millisecond) => {
            let ms = downcast::<Time32MillisecondArray>(array, "Time32(ms)")?.value(row);
            ((ms / 1_000) as u32, ((ms % 1_000) * 1_000_000) as u32)
        }
        (DataType::Time64(_), TimeUnit::Microsecond) => {
            let us = downcast::<Time64MicrosecondArray>(array, "Time64(us)")?.value(row);
            ((us / 1_000_000) as u32, ((us % 1_000_000) * 1_000) as u32)
        }
        (DataType::Time64(_), TimeUnit::Nanosecond) => {
            let ns = downcast::<Time64NanosecondArray>(array, "Time64(ns)")?.value(row);
            ((ns / 1_000_000_000) as u32, (ns % 1_000_000_000) as u32)
        }
        _ => unreachable!(),
    };
    let t = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
        .ok_or_else(|| Error::Other(format!("Arrow time out of range: {secs}s").into()))?;
    Ok(Value::ChronoTime(Some(t)))
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
fn arrow_to_time(
    array: &dyn Array,
    dtype: &DataType,
    unit: &TimeUnit,
    row: usize,
) -> Result<Value> {
    let (secs, nanos) = match (dtype, unit) {
        (DataType::Time32(_), TimeUnit::Second) => (
            downcast::<Time32SecondArray>(array, "Time32(s)")?.value(row) as u32,
            0u32,
        ),
        (DataType::Time32(_), TimeUnit::Millisecond) => {
            let ms = downcast::<Time32MillisecondArray>(array, "Time32(ms)")?.value(row);
            ((ms / 1_000) as u32, ((ms % 1_000) * 1_000_000) as u32)
        }
        (DataType::Time64(_), TimeUnit::Microsecond) => {
            let us = downcast::<Time64MicrosecondArray>(array, "Time64(us)")?.value(row);
            ((us / 1_000_000) as u32, ((us % 1_000_000) * 1_000) as u32)
        }
        (DataType::Time64(_), TimeUnit::Nanosecond) => {
            let ns = downcast::<Time64NanosecondArray>(array, "Time64(ns)")?.value(row);
            ((ns / 1_000_000_000) as u32, (ns % 1_000_000_000) as u32)
        }
        _ => unreachable!(),
    };
    let h = (secs / 3600) as u8;
    let m = ((secs % 3600) / 60) as u8;
    let s = (secs % 60) as u8;
    let t = sea_query::value::prelude::time::Time::from_hms_nano(h, m, s, nanos)
        .map_err(|_| Error::Other(format!("Arrow time out of range: {secs}s").into()))?;
    Ok(Value::TimeTime(Some(t)))
}

#[cfg(not(any(feature = "chrono", feature = "time")))]
fn arrow_to_time(
    _array: &dyn Array,
    _dtype: &DataType,
    _unit: &TimeUnit,
    _row: usize,
) -> Result<Value> {
    Err(Error::Unsupported(
        "Time decoding requires the `chrono` or `time` feature".into(),
    ))
}

// ── Decimal helpers ──────────────────────────────────────────────────────────

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
