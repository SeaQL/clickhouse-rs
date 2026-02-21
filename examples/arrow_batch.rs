//! Demonstrates `DataRowCursor::next_arrow_batch()` — Arrow `RecordBatch` output.
//!
//! Run with:
//!   cargo run --example arrow_batch --features arrow

use clickhouse::{Client, error::Result};
use sea_orm_arrow::arrow::array::{Array, Int32Array, StringArray, UInt64Array};
use sea_orm_arrow::arrow::datatypes::DataType;

/// Basic column layout: schema fields and typed array values are correct.
async fn test_column_layout(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT number::UInt64 AS n, number * 2 AS doubled FROM system.numbers LIMIT 6")
        .fetch_rows()?;

    let batch = cursor.next_arrow_batch(6).await?.expect("expected a batch");
    assert!(cursor.next_arrow_batch(6).await?.is_none());

    assert_eq!(batch.num_rows(), 6);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "n");
    assert_eq!(batch.schema().field(1).name(), "doubled");
    assert_eq!(*batch.schema().field(0).data_type(), DataType::UInt64);
    assert_eq!(*batch.schema().field(1).data_type(), DataType::UInt64);

    let ns = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let doubled = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    for i in 0..6usize {
        assert_eq!(ns.value(i), i as u64);
        assert_eq!(doubled.value(i), i as u64 * 2);
    }

    println!("test_column_layout: OK");
    Ok(())
}

/// Multiple batches: 10 rows with max_rows=4 → batches of 4, 4, 2.
async fn test_multiple_batches(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT number::UInt64 AS n FROM system.numbers LIMIT 10")
        .fetch_rows()?;

    let mut all_values: Vec<u64> = Vec::new();
    let mut batch_sizes: Vec<usize> = Vec::new();

    while let Some(batch) = cursor.next_arrow_batch(4).await? {
        batch_sizes.push(batch.num_rows());
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            all_values.push(col.value(i));
        }
    }

    assert_eq!(batch_sizes, [4, 4, 2]);
    assert_eq!(all_values, (0u64..10).collect::<Vec<_>>());

    println!("test_multiple_batches: OK");
    Ok(())
}

/// Nullable column: schema field is nullable; null rows become Arrow nulls.
async fn test_nullable(client: &Client) -> Result<()> {
    let mut cursor = client
        .query(
            "SELECT *
             FROM (
                 SELECT 1::Nullable(Int32) AS v
                 UNION ALL
                 SELECT NULL::Nullable(Int32) AS v
             )
             ORDER BY v NULLS LAST",
        )
        .fetch_rows()?;

    let batch = cursor
        .next_arrow_batch(10)
        .await?
        .expect("expected a batch");
    assert!(cursor.next_arrow_batch(10).await?.is_none());

    assert_eq!(batch.num_rows(), 2);
    assert!(batch.schema().field(0).is_nullable());
    assert_eq!(*batch.schema().field(0).data_type(), DataType::Int32);

    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(col.is_valid(0));
    assert_eq!(col.value(0), 1);
    assert!(col.is_null(1));

    println!("test_nullable: OK");
    Ok(())
}

/// Mixed types: UInt64 and String columns.
async fn test_mixed_types(client: &Client) -> Result<()> {
    let mut cursor = client
        .query(
            "SELECT number::UInt64 AS id, toString(number) AS label
             FROM system.numbers LIMIT 4",
        )
        .fetch_rows()?;

    let batch = cursor.next_arrow_batch(4).await?.expect("expected a batch");
    assert!(cursor.next_arrow_batch(4).await?.is_none());

    assert_eq!(batch.num_rows(), 4);
    assert_eq!(*batch.schema().field(0).data_type(), DataType::UInt64);
    assert_eq!(*batch.schema().field(1).data_type(), DataType::Utf8);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let labels = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    for i in 0..4usize {
        assert_eq!(ids.value(i), i as u64);
        assert_eq!(labels.value(i), i.to_string());
    }

    println!("test_mixed_types: OK");
    Ok(())
}

/// Empty result: next_arrow_batch returns None immediately.
async fn test_empty(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT 1::UInt8 AS x WHERE 1 = 0")
        .fetch_rows()?;

    assert!(cursor.next_arrow_batch(100).await?.is_none());

    println!("test_empty: OK");
    Ok(())
}

/// Schema is shared across batches (same Arc pointer).
async fn test_schema_shared(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT number::UInt64 AS n FROM system.numbers LIMIT 6")
        .fetch_rows()?;

    let batch1 = cursor.next_arrow_batch(3).await?.expect("first batch");
    let batch2 = cursor.next_arrow_batch(3).await?.expect("second batch");
    assert!(cursor.next_arrow_batch(3).await?.is_none());

    assert!(
        std::sync::Arc::ptr_eq(batch1.schema_ref(), batch2.schema_ref()),
        "schema Arc should be the same pointer across batches",
    );

    println!("test_schema_shared: OK");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:18123");

    test_column_layout(&client).await?;
    test_multiple_batches(&client).await?;
    test_nullable(&client).await?;
    test_mixed_types(&client).await?;
    test_empty(&client).await?;
    test_schema_shared(&client).await?;

    println!("All tests OK");
    Ok(())
}
