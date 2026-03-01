//! End-to-end example: SeaORM entity -> Arrow RecordBatch -> ClickHouse table.
//!
//! 1. Defines a SeaORM entity with `arrow_schema` to derive an Arrow schema.
//! 2. Builds `ActiveModel` instances and converts them to an Arrow `RecordBatch`.
//! 3. Creates the ClickHouse table and inserts the batch via `insert_arrow`.
//! 4. Reads the data back and verifies the round-trip.
//!
//! Run with:
//!   cargo run -p sea-orm-arrow-example

mod measurement {
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "measurement", arrow_schema)]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i32,
        pub recorded_at: ChronoDateTime,
        pub sensor_id: i32,
        pub temperature: f64,
        #[sea_orm(column_type = "Decimal(Some((38, 4)))")]
        pub voltage: Decimal,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

use std::env;

use clickhouse::{Client, error::Result};
use sea_orm::prelude::*;
use sea_orm::{ArrowSchema, Set};
use sea_orm_arrow::arrow::util::pretty;

const TABLE: &str = "seaorm_measurement";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default()
        .with_url(env::var("CH_URL").unwrap_or("http://localhost:18123".to_owned()));

    // ── 1. Create the ClickHouse table ─────────────────────────────────────
    client
        .query(&format!("DROP TABLE IF EXISTS {TABLE}"))
        .execute()
        .await?;
    client
        .query(&format!(
            "CREATE TABLE {TABLE} (
                id          Int32,
                recorded_at DateTime64(6),
                sensor_id   Int32,
                temperature Float64,
                voltage     Decimal128(4)
             )
             ENGINE = MergeTree
             ORDER BY id"
        ))
        .execute()
        .await?;
    println!("Table `{TABLE}` created.");

    // ── 2. Build ActiveModels and convert to Arrow RecordBatch ──────────────
    let base_ts = chrono::NaiveDate::from_ymd_opt(2026, 6, 15)
        .unwrap()
        .and_hms_milli_opt(8, 0, 0, 0)
        .unwrap();

    let models: Vec<measurement::ActiveModel> = (1..=10)
        .map(|i| {
            let millis = i as u64 * 60_000 + ((i as u64 * 137 + 42) % 1000);
            measurement::ActiveModel {
                id: Set(i),
                recorded_at: Set(base_ts + std::time::Duration::from_millis(millis)),
                sensor_id: Set(100 + (i % 3)),
                temperature: Set(20.0 + i as f64 * 0.5),
                voltage: Set(Decimal::new(30000 + i as i64 * 100, 4)),
            }
        })
        .collect();

    let schema = measurement::Entity::arrow_schema();
    let batch =
        measurement::ActiveModel::to_arrow(&models, &schema).expect("to_arrow should succeed");

    println!("Arrow schema: {}", batch.schema());
    println!("Built RecordBatch with {} rows.", batch.num_rows());

    // ── 3. Insert the Arrow batch into ClickHouse ──────────────────────────
    let mut insert = client.insert_arrow(TABLE, &batch.schema()).await?;
    insert.write_batch(&batch).await?;
    insert.end().await?;
    println!("Inserted {} rows.", batch.num_rows());

    // ── 4. Read back and display ───────────────────────────────────────────
    let mut cursor = client
        .query(&format!("SELECT * FROM {TABLE} ORDER BY id"))
        .fetch_rows()?;
    let mut result = Vec::new();
    while let Some(b) = cursor.next_arrow_batch(10).await? {
        result.push(b);
    }
    println!("\nRound-trip result:");
    pretty::print_batches(&result).unwrap();

    let total: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 10, "expected 10 rows back");
    println!("Round-trip verified: {total} rows match.");

    Ok(())
}
