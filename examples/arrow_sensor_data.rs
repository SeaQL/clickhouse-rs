//! Generates synthetic sensor readings using ClickHouse SQL, streams them
//! back as Arrow `RecordBatch`es, inserts the batches back into a real table
//! via `insert_arrow` / `write_batch`, then queries the table to confirm the
//! round-trip.
//!
//! Run with:
//!   cargo run --example arrow_sensor_data --features arrow

use std::env;

use clickhouse::{Client, error::Result};
use sea_orm_arrow::arrow::{array::RecordBatch, util::pretty};

const SQL: &str = r#"
    SELECT
        toUInt64(number) + 1                                      AS id,
        toDateTime('2024-01-01 00:00:00') + (rand() % 86400)      AS recorded_at,
        toInt32(100 + rand() % 10)                                AS sensor_id,
        -10.0 + randUniform(0.0, 50.0)                            AS temperature,
        toDecimal128(3.0 + toFloat64(rand() % 5000) / 10000.0, 4) AS voltage
    FROM system.numbers
    LIMIT 20
"#;

const TABLE: &str = "sensor_data";

async fn ddl(client: &Client) -> Result<()> {
    client
        .query(&format!("DROP TABLE IF EXISTS {TABLE}"))
        .execute()
        .await?;
    client
        .query(&format!(
            "CREATE TABLE {TABLE} (
                id          UInt64,
                recorded_at DateTime,
                sensor_id   Int32,
                temperature Float64,
                voltage     Decimal128(4)
             )
             ENGINE = MergeTree
             ORDER BY id"
        ))
        .execute()
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default()
        .with_url(env::var("CH_URL").unwrap_or("http://localhost:18123".to_owned()));

    // --- fetch synthetic data as Arrow batches ---
    let mut cursor = client.query(SQL).fetch_rows()?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = cursor.next_arrow_batch(10).await? {
        batches.push(batch);
    }
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 20);

    println!("Schema: {}", batches[0].schema());

    // --- create table and insert the batches back ---
    ddl(&client).await?;

    let mut insert = client.insert_arrow(TABLE, &batches[0]).await?;
    for batch in &batches {
        insert.write_batch(batch).await?;
    }
    insert.end().await?;
    println!("Inserted {} batches.", batches.len());

    // --- read back and display ---
    let mut cursor = client
        .query(&format!("SELECT * FROM {TABLE} ORDER BY id"))
        .fetch_rows()?;
    let mut result: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = cursor.next_arrow_batch(10).await? {
        result.push(batch);
    }
    println!("Round-trip result:");
    pretty::print_batches(&result).unwrap();

    assert_eq!(batches, result);

    Ok(())
}
