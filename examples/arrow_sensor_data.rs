//! Generates synthetic sensor readings using ClickHouse SQL and streams them
//! back as Arrow `RecordBatch`es, printing the result in tabular form.
//!
//! Run with:
//!   cargo run --example arrow_sensor_data --features arrow

use std::env;

use clickhouse::{Client, Compression, error::Result};
use sea_orm_arrow::arrow::util::pretty;

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

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default()
        .with_url(env::var("CH_URL").unwrap_or("http://localhost:18123".to_owned()));

    let mut cursor = client.query(SQL).fetch_rows()?;

    while let Some(batch) = cursor.next_arrow_batch(10).await? {
        pretty::print_batches(&[batch]).unwrap();
    }

    Ok(())
}
