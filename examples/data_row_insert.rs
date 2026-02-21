//! Demonstrates dynamic `DataRow` insert, select, mutate, and re-insert.
//!
//! In ClickHouse, "update by primary key" is modelled as a new insert followed
//! by a `SELECT … FINAL` on a `ReplacingMergeTree` table, which returns only
//! the most recently inserted version of each key.
//!
//! Run with:
//!   cargo run --example data_row_insert --features sea-ql

use std::sync::Arc;

use clickhouse::{Client, DataRow, error::Result};
use sea_query::Value;

const TABLE: &str = "data_row_example";

async fn ddl(client: &Client) -> Result<()> {
    client
        .query(&format!("DROP TABLE IF EXISTS {TABLE}"))
        .execute()
        .await?;
    client
        .query(&format!(
            "CREATE TABLE {TABLE} (
                id    UInt32,
                name  String,
                score Float64
             )
             ENGINE = ReplacingMergeTree
             ORDER BY id"
        ))
        .execute()
        .await
}

fn make_row(columns: Arc<[Arc<str>]>, id: u32, name: &str, score: f64) -> DataRow {
    DataRow {
        columns,
        values: vec![
            Value::Unsigned(Some(id)),
            Value::String(Some(name.into())),
            Value::Double(Some(score)),
        ],
    }
}

async fn insert(client: &Client, rows: &[DataRow]) -> Result<()> {
    let mut insert = client.insert_data_row(TABLE, &rows[0]).await?;
    for row in rows {
        insert.write_row(row).await?;
    }
    insert.end().await
}

async fn select_all(client: &Client) -> Result<Vec<DataRow>> {
    let mut cursor = client
        .query(&format!(
            "SELECT id, name, score FROM {TABLE} FINAL ORDER BY id"
        ))
        .fetch_rows()?;

    let mut rows = Vec::new();
    while let Some(row) = cursor.next().await? {
        rows.push(row);
    }
    Ok(rows)
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:18123");

    ddl(&client).await?;

    // Build a shared column list for all rows in this insert batch.
    let columns: Arc<[Arc<str>]> = Arc::from(["id".into(), "name".into(), "score".into()]);

    // --- initial insert ---
    let initial: Vec<DataRow> = (0u32..5)
        .map(|i| make_row(columns.clone(), i, "original", i as f64 * 1.5))
        .collect();

    insert(&client, &initial).await?;
    println!("Inserted {} rows.", initial.len());

    // --- select back and verify ---
    let mut rows = select_all(&client).await?;
    assert_eq!(rows.len(), 5, "expected 5 rows after initial insert");
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.values[0], Value::Unsigned(Some(i as u32)));
        assert_eq!(row.values[1], Value::String(Some("original".into())));
        assert_eq!(row.values[2], Value::Double(Some(i as f64 * 1.5)));
    }
    println!("Select after initial insert: OK");

    // --- mutate in place, then re-insert (ClickHouse update-by-PK pattern) ---
    for row in &mut rows {
        row.values[1] = Value::String(Some("updated".into()));
        if let Value::Double(Some(v)) = row.values[2] {
            row.values[2] = Value::Double(Some(v * 2.0));
        }
    }

    insert(&client, &rows).await?;
    println!("Re-inserted {} rows (mutations).", rows.len());

    // --- select again and verify mutations ---
    let final_rows = select_all(&client).await?;
    assert_eq!(final_rows.len(), 5, "expected 5 rows after re-insert");
    for (i, row) in final_rows.iter().enumerate() {
        assert_eq!(row.values[0], Value::Unsigned(Some(i as u32)));
        assert_eq!(row.values[1], Value::String(Some("updated".into())));
        assert_eq!(row.values[2], Value::Double(Some(i as f64 * 3.0)));
    }
    println!("Select after re-insert: OK");

    println!("All assertions passed.");
    Ok(())
}
