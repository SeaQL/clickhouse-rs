//! Demonstrates deriving a `CREATE TABLE` DDL from a dynamic query result.
//!
//! 1. Runs a query with complex SQL expressions so the resulting types
//!    (DateTime64, LowCardinality, Nullable, Decimal, etc.) are determined
//!    by the ClickHouse server, not hard-coded here.
//! 2. Fetches the first row as a `DataRow` (which carries column types).
//! 3. Builds a `ClickHouseSchema` from that row and generates the DDL.
//! 4. Creates the table, inserts all rows, then selects back to verify
//!    an exact round-trip.
//!
//! Run with:
//!   cargo run --example data_row_schema --features sea-ql,chrono,rust_decimal

use clickhouse::schema::{ClickHouseSchema, Engine};
use clickhouse::{Client, DataRow, error::Result};

const TABLE: &str = "data_row_schema_example";

const QUERY: &str = r#"
    SELECT
        toUInt64(number + 1)                                             AS id,
        toDateTime64('2026-06-15 08:30:00', 6)
            + toIntervalSecond(rand() % 86400)
            + toIntervalMillisecond(rand() % 1000)                       AS recorded_at,
        toString(concat('sensor-', toString(rand() % 5)))                AS device,
        if(rand() % 4 = 0, NULL, -20.0 + randUniform(0.0, 60.0))         AS temperature,
        toDecimal64(1.0 + toFloat64(rand() % 9000) / 10000.0, 4)         AS voltage,
        toFixedString(hex(rand64()), 16)                                 AS trace_id
    FROM system.numbers
    LIMIT 20
"#;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default()
        .with_url(std::env::var("CH_URL").unwrap_or("http://localhost:18123".to_owned()));

    // ── 1. Query rows dynamically ────────────────────────────────────────────
    println!("Fetching 20 synthetic rows…");
    let mut cursor = client.query(QUERY).fetch_rows()?;
    let mut rows: Vec<DataRow> = Vec::new();
    while let Some(row) = cursor.next().await? {
        rows.push(row);
    }
    assert_eq!(rows.len(), 20, "expected 20 rows");

    println!(
        "Columns: {:?}",
        rows[0]
            .column_names
            .iter()
            .map(|c| c.as_ref())
            .collect::<Vec<_>>()
    );
    println!(
        "Types:   {:?}",
        rows[0]
            .column_types
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
    );

    // ── 2. Derive DDL from the first DataRow ─────────────────────────────────
    let ddl = {
        let mut schema = ClickHouseSchema::from_data_row(&rows[0]);
        schema
            .table_name(TABLE)
            .engine(Engine::ReplacingMergeTree)
            .order_by(["recorded_at", "device", "id"])
            .primary_key(["recorded_at", "device"]);
        // Mark the device column as LowCardinality
        schema.find_column_mut("device").set_low_cardinality(true);
        schema.to_string()
    };
    println!("\nGenerated DDL:\n{ddl}\n");
    assert_eq!(
        ddl,
        r#"CREATE TABLE data_row_schema_example (
    id UInt64,
    recorded_at DateTime64(6),
    device LowCardinality(String),
    temperature Nullable(Float64),
    voltage Decimal(18, 4),
    trace_id FixedString(16)
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

    // ── 4. Insert all rows ───────────────────────────────────────────────────
    let mut insert = client.insert_data_row(TABLE, &rows[0]).await?;
    for row in &rows {
        insert.write_row(row).await?;
    }
    insert.end().await?;
    println!("Inserted {} rows.", rows.len());

    // ── 5. Select back and verify exact round-trip ───────────────────────────
    let select_sql = format!("SELECT * FROM {TABLE} FINAL ORDER BY id");
    let mut cursor = client.query(&select_sql).fetch_rows()?;
    let mut result: Vec<DataRow> = Vec::new();
    while let Some(row) = cursor.next().await? {
        result.push(row);
    }
    assert_eq!(result.len(), rows.len(), "row count mismatch");

    for (i, (orig, read)) in rows.iter().zip(result.iter()).enumerate() {
        assert_eq!(orig.values, read.values, "value mismatch at row {i}");
    }
    println!("Round-trip verified: all {} rows match.", result.len());

    Ok(())
}
