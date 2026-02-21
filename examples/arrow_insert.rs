//! Demonstrates `Client::insert_arrow` and `DataRowInsert::write_batch`.
//!
//! Builds an Arrow `RecordBatch` in memory, inserts it via `insert_arrow` /
//! `write_batch`, then reads the rows back as `DataRow`s and asserts that the
//! round-trip is lossless.
//!
//! Run with:
//!   cargo run --example arrow_insert --features arrow

use std::sync::Arc;

use clickhouse::{Client, DataRow, error::Result};
use sea_orm_arrow::arrow::{
    array::{Decimal128Array, Decimal256Array, Float64Array, StringArray, UInt32Array},
    datatypes::{DataType, Field, Schema, i256},
    record_batch::RecordBatch,
};
use sea_query::Value;
use sea_query::prelude::{
    Decimal,
    bigdecimal::{BigDecimal, num_bigint::BigInt},
};

const TABLE: &str = "arrow_insert_example";

// Decimal128(4): precision 38, scale 4  ->  raw i128 unit = 0.0001
// Decimal256(8): precision 76, scale 8  ->  raw i256 unit = 0.00000001
const D128_SCALE: i8 = 4;
const D256_SCALE: i8 = 8;

async fn ddl(client: &Client) -> Result<()> {
    client
        .query(&format!("DROP TABLE IF EXISTS {TABLE}"))
        .execute()
        .await?;
    client
        .query(&format!(
            "CREATE TABLE {TABLE} (
                id     UInt32,
                label  String,
                value  Float64,
                price  Decimal128(4),
                weight Decimal256(8)
             )
             ENGINE = MergeTree
             ORDER BY id"
        ))
        .execute()
        .await
}

fn make_batch(
    ids: &[u32],
    labels: &[&str],
    values: &[f64],
    prices: &[i128], // raw i128 with D128_SCALE decimal places
    weights: &[i64], // raw i64 cast to i256 with D256_SCALE decimal places
) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("price", DataType::Decimal128(38, D128_SCALE), false),
        Field::new("weight", DataType::Decimal256(76, D256_SCALE), false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(labels.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
            Arc::new(
                Decimal128Array::from(prices.to_vec())
                    .with_precision_and_scale(38, D128_SCALE)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(
                    weights
                        .iter()
                        .map(|&w| i256::from_i128(w as i128))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(76, D256_SCALE)
                .unwrap(),
            ),
        ],
    )
    .expect("valid batch")
}

async fn select_all(client: &Client) -> Result<Vec<DataRow>> {
    let mut cursor = client
        .query(&format!(
            "SELECT id, label, value, price, weight FROM {TABLE} ORDER BY id"
        ))
        .fetch_rows()?;

    let mut rows = Vec::new();
    while let Some(row) = cursor.next().await? {
        rows.push(row);
    }
    Ok(rows)
}

/// Construct the `Value::Decimal` that `value_de` produces for a Decimal128 raw integer
/// with scale ≤ 28 (mirrors the `Decimal::from_i128_with_scale` branch in value_de.rs).
fn expected_dec(raw: i64, scale: i8) -> Value {
    Value::Decimal(Some(Decimal::from_i128_with_scale(
        raw as i128,
        scale as u32,
    )))
}

/// Construct the `Value::BigDecimal` that `value_de` produces for a raw integer
/// with the given scale (mirrors `bigdecimal_with_scale` in value_de.rs).
fn expected_bd(raw: i64, scale: i8) -> Value {
    Value::BigDecimal(Some(Box::new(BigDecimal::new(
        BigInt::from(raw),
        scale as i64,
    ))))
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:18123");

    ddl(&client).await?;

    // Raw Decimal128(4) values: 1 unit = 0.0001
    //   0.0000, 1.2345, 2.5000, 3.1415, -7.5000
    let prices: &[i128] = &[0, 12345, 25000, 31415, -75000];

    // Raw Decimal256(8) values: 1 unit = 0.00000001
    //   0.00000000, 3.14159265, 2.71828182, 1.23456789, -1.00000000
    let weights: &[i64] = &[0, 314159265, 271828182, 123456789, -100000000];

    let batch1 = make_batch(
        &[0, 1, 2],
        &["a", "b", "c"],
        &[0.0, 1.5, 3.0],
        &prices[..3],
        &weights[..3],
    );
    let batch2 = make_batch(
        &[3, 4],
        &["d", "e"],
        &[4.5, 6.0],
        &prices[3..],
        &weights[3..],
    );

    let mut insert = client.insert_arrow(TABLE, &batch1).await?;
    insert.write_batch(&batch1).await?;
    insert.write_batch(&batch2).await?;
    insert.end().await?;
    println!(
        "Inserted {} + {} rows.",
        batch1.num_rows(),
        batch2.num_rows()
    );

    // --- verify round-trip ---
    let rows = select_all(&client).await?;
    assert_eq!(rows.len(), 5, "expected 5 rows");

    let expected_labels = ["a", "b", "c", "d", "e"];
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.values[0], Value::Unsigned(Some(i as u32)));
        assert_eq!(
            row.values[1],
            Value::String(Some(expected_labels[i].to_owned()))
        );
        assert_eq!(row.values[2], Value::Double(Some(i as f64 * 1.5)));
        assert_eq!(row.values[3], expected_dec(prices[i] as i64, D128_SCALE));
        assert_eq!(row.values[4], expected_bd(weights[i], D256_SCALE));
    }
    println!("Round-trip assertions: OK");

    println!("All assertions passed.");
    Ok(())
}
