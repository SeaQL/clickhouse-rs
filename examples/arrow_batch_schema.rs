//! Demonstrates deriving a `CREATE TABLE` DDL from an Arrow `RecordBatch`.
//!
//! This is the Arrow equivalent of `data_row_schema.rs`:
//! 1. Runs a query with complex SQL expressions so the resulting types
//!    are determined by the ClickHouse server.
//! 2. Fetches the result as Arrow `RecordBatch`es.
//! 3. Builds a `ClickHouseSchema` from the Arrow schema and generates DDL.
//! 4. Creates the table, inserts all batches, then selects back to verify
//!    an exact round-trip.
//!
//! Run with:
//!   cargo run --example arrow_batch_schema --features arrow,chrono,rust_decimal

use clickhouse::Client;
use clickhouse::error::Result;
use clickhouse::schema::{ClickHouseSchema, Engine};
use sea_orm_arrow::arrow::array::RecordBatch;

const TABLE: &str = "arrow_batch_schema_example";

const QUERY: &str = r#"
    SELECT
        toUInt64(number + 1)                                             AS id,
        toDateTime64('2026-06-15 08:30:00', 6)
            + toIntervalSecond(rand() % 86400)
            + toIntervalMillisecond(rand() % 1000)                       AS recorded_at,
        toString(concat('sensor-', toString(rand() % 5)))                AS device,
        if(rand() % 4 = 0, NULL, -20.0 + randUniform(0.0, 60.0))         AS temperature,
        toDecimal128(1.0 + toFloat64(rand() % 9000) / 10000.0, 4)        AS voltage,
        hex(rand64())                                                    AS trace_id
    FROM system.numbers
    LIMIT 20
"#;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default()
        .with_url(std::env::var("CH_URL").unwrap_or("http://localhost:18123".to_owned()));

    // ── 1. Query rows as Arrow batches ─────────────────────────────────────
    println!("Fetching 20 synthetic rows as Arrow batches…");
    let mut cursor = client.query(QUERY).fetch_rows()?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = cursor.next_arrow_batch(20).await? {
        batches.push(batch);
    }
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 20, "expected 20 rows");

    let schema = batches[0].schema();
    println!(
        "Columns: {:?}",
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
    );
    println!(
        "Arrow types: {:?}",
        schema
            .fields()
            .iter()
            .map(|f| f.data_type().to_string())
            .collect::<Vec<_>>()
    );

    // ── 2. Derive DDL from the Arrow schema ────────────────────────────────
    let ddl = {
        let mut s = ClickHouseSchema::from_arrow(&schema);
        s.table_name(TABLE)
            .engine(Engine::ReplacingMergeTree)
            .order_by(["recorded_at", "device", "id"])
            .primary_key(["recorded_at", "device"]);
        s.find_column_mut("device").set_low_cardinality(true);
        s.to_string()
    };
    println!("\nGenerated DDL:\n{ddl}\n");
    assert_eq!(
        ddl,
        r#"CREATE TABLE arrow_batch_schema_example (
    id UInt64,
    recorded_at DateTime64(6),
    device LowCardinality(String),
    temperature Nullable(Float64),
    voltage Decimal(38, 4),
    trace_id String
) ENGINE = ReplacingMergeTree()
ORDER BY (recorded_at, device, id)
PRIMARY KEY (recorded_at, device)"#
    );

    // ── 3. Create table ──────────────────────────────────────────────────────
    client
        .query(&format!("DROP TABLE IF EXISTS {TABLE}"))
        .execute()
        .await?;
    client.query(&ddl).execute().await?;
    println!("Table `{TABLE}` created.");

    // ── 4. Insert all batches ────────────────────────────────────────────────
    let mut insert = client.insert_arrow(TABLE, &batches[0].schema()).await?;
    for batch in &batches {
        insert.write_batch(batch).await?;
    }
    insert.end().await?;
    println!("Inserted {total_rows} rows in {} batch(es).", batches.len());

    // ── 5. Select back and verify exact round-trip ───────────────────────────
    let select_sql = format!("SELECT * FROM {TABLE} FINAL ORDER BY id");
    let mut cursor = client.query(&select_sql).fetch_rows()?;
    let mut result: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = cursor.next_arrow_batch(20).await? {
        result.push(batch);
    }
    let result_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    sea_orm_arrow::arrow::util::pretty::print_batches(&result).unwrap();
    assert_eq!(result_rows, total_rows, "row count mismatch");
    assert_eq!(batches, result, "batch content mismatch");
    println!("Round-trip verified: all {result_rows} rows match.");

    Ok(())
}
