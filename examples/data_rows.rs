//! Demonstrates `Query::fetch_rows()` — dynamic, schema-driven decoding
//! without any compile-time struct definition.
//!
//! Run with:
//!   cargo run --example data_rows --features sea-query

use clickhouse::{Client, DataRow, error::Result};
use sea_query::Value;
use sea_query::value::prelude::{Decimal, NaiveDate, NaiveDateTime, NaiveTime, serde_json};

async fn test_types(client: &Client) -> Result<()> {
    let mut cursor = client
        .query(
            "SELECT
                1::UInt8                                 AS u8_col,
                -2::Int32                                AS i32_col,
                3.14::Float64                            AS f64_col,
                'hello'::String                          AS str_col,
                true::Bool                               AS bool_col,
                toDate('2024-01-15')                     AS date_col,
                toDateTime('2024-01-15 12:34:56')        AS dt_col,
                toDecimal64(123.45, 2)                   AS dec_col,
                NULL::Nullable(Int32)                    AS null_col,
                ['a', 'b', 'c']::Array(String)           AS arr_col
            ",
        )
        .fetch_rows()?;

    let row = cursor.next().await?.expect("expected one row");
    assert!(cursor.next().await?.is_none(), "expected exactly one row");

    let DataRow { columns, values } = &row;

    // Column names
    let col_names: Vec<&str> = columns.iter().map(|c| c.as_ref()).collect();
    assert_eq!(
        col_names,
        [
            "u8_col", "i32_col", "f64_col", "str_col", "bool_col", "date_col", "dt_col", "dec_col",
            "null_col", "arr_col"
        ]
    );

    assert_eq!(values[0], Value::TinyUnsigned(Some(1)));
    assert_eq!(values[1], Value::Int(Some(-2)));
    assert_eq!(values[2], Value::Double(Some(3.14)));
    assert_eq!(values[3], Value::String(Some("hello".into())));
    assert_eq!(values[4], Value::Bool(Some(true)));

    assert_eq!(
        values[5],
        Value::ChronoDate(Some(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()))
    );
    assert_eq!(
        values[6],
        Value::ChronoDateTime(Some(NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(12, 34, 56).unwrap(),
        )))
    );
    assert_eq!(
        values[7],
        Value::Decimal(Some(Decimal::from_i128_with_scale(12345, 2)))
    );

    // Nullable null → typed null
    assert_eq!(values[8], Value::Int(None));

    // Array(String) → Json array
    let expected_arr = serde_json::json!(["a", "b", "c"]);
    assert_eq!(values[9], Value::Json(Some(Box::new(expected_arr))));

    for value in values {
        println!("{value:?}");
    }

    println!("test_types: OK");
    Ok(())
}

async fn test_numbers(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT number, number * number AS squared FROM system.numbers LIMIT 5")
        .fetch_rows()?;

    let mut rows = Vec::new();
    while let Some(row) = cursor.next().await? {
        rows.push(row);
    }

    assert_eq!(rows.len(), 5);

    let col_names: Vec<&str> = rows[0].columns.iter().map(|c| c.as_ref()).collect();
    assert_eq!(col_names, ["number", "squared"]);

    for (i, row) in rows.iter().enumerate() {
        let n = i as u64;
        assert_eq!(row.values[0], Value::BigUnsigned(Some(n)));
        assert_eq!(row.values[1], Value::BigUnsigned(Some(n * n)));
    }

    println!("test_numbers: OK");
    Ok(())
}

async fn test_nullable(client: &Client) -> Result<()> {
    let mut cursor = client
        .query(
            "SELECT *
             FROM (
                 SELECT 1::Nullable(UInt8) AS v
                 UNION ALL
                 SELECT NULL::Nullable(UInt8) AS v
             )
             ORDER BY v NULLS LAST",
        )
        .fetch_rows()?;

    let row1 = cursor.next().await?.unwrap();
    let row2 = cursor.next().await?.unwrap();
    assert!(cursor.next().await?.is_none());

    assert_eq!(row1.values[0], Value::TinyUnsigned(Some(1)));
    assert_eq!(row2.values[0], Value::TinyUnsigned(None));

    println!("test_nullable: OK");
    Ok(())
}

/// Math/string built-in functions whose return types differ from their input types.
async fn test_math_functions(client: &Client) -> Result<()> {
    let mut cursor = client
        .query(
            "SELECT
                sqrt(9::UInt64)          AS sqrt_val,
                floor(3.9::Float64)      AS floor_val,
                length('clickhouse')     AS len_val,
                upper('hello')           AS upper_val,
                toString(42::UInt32)     AS to_str_val
            ",
        )
        .fetch_rows()?;

    let row = cursor.next().await?.expect("expected one row");
    assert!(cursor.next().await?.is_none());

    let DataRow { columns: _, values } = &row;

    // sqrt(UInt64) → Float64
    assert_eq!(values[0], Value::Double(Some(3.0)));
    // floor(Float64) → Float64
    assert_eq!(values[1], Value::Double(Some(3.0)));
    // length(String) → UInt64
    assert_eq!(values[2], Value::BigUnsigned(Some(10)));
    // upper(String) → String
    assert_eq!(values[3], Value::String(Some("HELLO".into())));
    // toString(UInt32) → String
    assert_eq!(values[4], Value::String(Some("42".into())));

    println!("test_math_functions: OK");
    Ok(())
}

/// Date extraction functions whose return types are narrower unsigned integers.
async fn test_date_functions(client: &Client) -> Result<()> {
    let mut cursor = client
        .query(
            "SELECT
                toYear(toDate('2024-06-15'))                                AS year_val,
                toMonth(toDate('2024-06-15'))                               AS month_val,
                toDayOfMonth(toDate('2024-06-15'))                          AS day_val,
                dateDiff('day', toDate('2024-01-01'), toDate('2024-01-15')) AS diff_val
            ",
        )
        .fetch_rows()?;

    let row = cursor.next().await?.expect("expected one row");
    assert!(cursor.next().await?.is_none());

    let DataRow { columns: _, values } = &row;

    // toYear → UInt16
    assert_eq!(values[0], Value::SmallUnsigned(Some(2024)));
    // toMonth → UInt8
    assert_eq!(values[1], Value::TinyUnsigned(Some(6)));
    // toDayOfMonth → UInt8
    assert_eq!(values[2], Value::TinyUnsigned(Some(15)));
    // dateDiff('day', ...) → Int64
    assert_eq!(values[3], Value::BigInt(Some(14)));

    println!("test_date_functions: OK");
    Ok(())
}

/// Arithmetic type promotion rules: result types are often wider than either operand.
///
/// Key surprises:
///   UInt8 + UInt16  → UInt32  (not UInt16!)
///   UInt32 + Int32  → Int64   (mixed sign widens to next signed)
///   Float32 + Float64 → Float64 (float64 wins)
///   if(cond, UInt8, UInt8) → UInt8  (same type, no widening)
async fn test_type_promotion(client: &Client) -> Result<()> {
    let mut cursor = client
        .query(
            "SELECT
                1::UInt8 + 1::UInt16          AS u8_u16,
                1::Float32 + 1::Float64       AS f32_f64,
                100::UInt32 + 100::Int32      AS u32_i32,
                if(1=1, 42::UInt8, 0::UInt8)  AS cond_same,
                if(1=2, 'yes', 'no')          AS cond_str
            ",
        )
        .fetch_rows()?;

    let row = cursor.next().await?.expect("expected one row");
    assert!(cursor.next().await?.is_none());

    let DataRow { columns: _, values } = &row;

    // UInt8 + UInt16 → UInt32 (ClickHouse promotes past UInt16)
    assert_eq!(values[0], Value::Unsigned(Some(2)));
    // Float32 + Float64 → Float64
    assert_eq!(values[1], Value::Double(Some(2.0)));
    // UInt32 + Int32 → Int64 (mixed sign → signed, widened)
    assert_eq!(values[2], Value::BigInt(Some(200)));
    // if with same-type branches → that type
    assert_eq!(values[3], Value::TinyUnsigned(Some(42)));
    // if with string literals → String
    assert_eq!(values[4], Value::String(Some("no".into())));

    println!("test_type_promotion: OK");
    Ok(())
}

async fn test_empty_result(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT 1::UInt8 AS x WHERE 1 = 0")
        .fetch_rows()?;

    assert!(cursor.next().await?.is_none());

    println!("test_empty_result: OK");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:18123");

    test_types(&client).await?;
    test_numbers(&client).await?;
    test_nullable(&client).await?;
    test_empty_result(&client).await?;
    test_math_functions(&client).await?;
    test_date_functions(&client).await?;
    test_type_promotion(&client).await?;

    println!("All tests OK");
    Ok(())
}
