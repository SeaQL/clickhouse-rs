//! Demonstrates `DataRowCursor::next_batch()` — column-oriented batch reading.
//!
//! `RowBatch` stores one `Vec<Value>` per column rather than one `Vec<Value>`
//! per row, making it a natural stepping stone toward Apache Arrow record batches.
//!
//! Run with:
//!   cargo run --example row_batch --features sea-query

use clickhouse::{Client, error::Result};
use sea_query::Value;

/// Basic column orientation: `column_data[i]` holds all values for column `i`.
async fn test_column_layout(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT number::UInt64 AS n, number * 2 AS doubled FROM system.numbers LIMIT 6")
        .fetch_rows()?;

    // Read all 6 rows in one batch.
    let batch = cursor.next_batch(6).await?.expect("expected a batch");
    assert!(
        cursor.next_batch(6).await?.is_none(),
        "expected no more batches"
    );

    assert_eq!(batch.num_rows, 6);
    assert_eq!(batch.columns.len(), 2);
    assert_eq!(batch.column_data.len(), 2);

    let col_names: Vec<&str> = batch.columns.iter().map(|c| c.as_ref()).collect();
    assert_eq!(col_names, ["n", "doubled"]);

    // column_data[0] = all values of "n"
    let ns: Vec<u64> = batch.column_data[0]
        .iter()
        .map(|v| match v {
            Value::BigUnsigned(Some(n)) => *n,
            other => panic!("unexpected value: {other:?}"),
        })
        .collect();
    assert_eq!(ns, [0, 1, 2, 3, 4, 5]);

    // column_data[1] = all values of "doubled"
    let doubled: Vec<u64> = batch.column_data[1]
        .iter()
        .map(|v| match v {
            Value::BigUnsigned(Some(n)) => *n,
            other => panic!("unexpected value: {other:?}"),
        })
        .collect();
    assert_eq!(doubled, [0, 2, 4, 6, 8, 10]);

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

    while let Some(batch) = cursor.next_batch(4).await? {
        batch_sizes.push(batch.num_rows);
        for v in &batch.column_data[0] {
            if let Value::BigUnsigned(Some(n)) = v {
                all_values.push(*n);
            }
        }
    }

    assert_eq!(batch_sizes, [4, 4, 2]);
    assert_eq!(all_values, (0u64..10).collect::<Vec<_>>());

    println!("test_multiple_batches: OK");
    Ok(())
}

/// Empty result: next_batch returns None immediately.
async fn test_empty_result(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT 1::UInt8 AS x WHERE 1 = 0")
        .fetch_rows()?;

    assert!(cursor.next_batch(100).await?.is_none());

    println!("test_empty_result: OK");
    Ok(())
}

/// Batch size of 1 behaves like next() but in column-oriented form.
async fn test_batch_size_one(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT number::UInt64 AS n FROM system.numbers LIMIT 3")
        .fetch_rows()?;

    for expected in 0u64..3 {
        let batch = cursor.next_batch(1).await?.expect("expected a batch");
        assert_eq!(batch.num_rows, 1);
        assert_eq!(batch.column_data[0].len(), 1);
        assert_eq!(batch.column_data[0][0], Value::BigUnsigned(Some(expected)));
    }
    assert!(cursor.next_batch(1).await?.is_none());

    println!("test_batch_size_one: OK");
    Ok(())
}

/// Column names are shared (same Arc) across all batches from one cursor.
async fn test_columns_shared(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT number::UInt64 AS n FROM system.numbers LIMIT 6")
        .fetch_rows()?;

    let batch1 = cursor.next_batch(3).await?.expect("first batch");
    let batch2 = cursor.next_batch(3).await?.expect("second batch");
    assert!(cursor.next_batch(3).await?.is_none());

    // Both batches should carry the same column names.
    assert_eq!(
        batch1
            .columns
            .iter()
            .map(|c| c.as_ref())
            .collect::<Vec<_>>(),
        batch2
            .columns
            .iter()
            .map(|c| c.as_ref())
            .collect::<Vec<_>>(),
    );

    // Each inner Vec has exactly num_rows entries.
    assert_eq!(batch1.column_data[0].len(), batch1.num_rows);
    assert_eq!(batch2.column_data[0].len(), batch2.num_rows);

    println!("test_columns_shared: OK");
    Ok(())
}

/// Mixed types: batch correctly stores different Value variants per column.
async fn test_mixed_types(client: &Client) -> Result<()> {
    let mut cursor = client
        .query(
            "SELECT
                number::UInt64          AS id,
                toString(number)        AS label,
                number % 2 = 0          AS is_even
             FROM system.numbers LIMIT 4",
        )
        .fetch_rows()?;

    let batch = cursor.next_batch(4).await?.expect("expected a batch");
    assert!(cursor.next_batch(4).await?.is_none());

    assert_eq!(batch.num_rows, 4);

    let col_names: Vec<&str> = batch.columns.iter().map(|c| c.as_ref()).collect();
    assert_eq!(col_names, ["id", "label", "is_even"]);

    // id column: UInt64 values 0..4
    for (i, v) in batch.column_data[0].iter().enumerate() {
        assert_eq!(*v, Value::BigUnsigned(Some(i as u64)));
    }

    // label column: String values "0".."3"
    for (i, v) in batch.column_data[1].iter().enumerate() {
        assert_eq!(*v, Value::String(Some(i.to_string())));
    }

    // is_even column: Bool alternating true/false
    let expected_bools = [true, false, true, false];
    for (v, expected) in batch.column_data[2].iter().zip(expected_bools) {
        assert_eq!(*v, Value::TinyUnsigned(Some(expected as u8)));
    }

    println!("test_mixed_types: OK");
    Ok(())
}

/// Batch larger than the result: returns all rows in a single partial batch.
async fn test_batch_larger_than_result(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT number::UInt64 AS n FROM system.numbers LIMIT 3")
        .fetch_rows()?;

    let batch = cursor.next_batch(1000).await?.expect("expected a batch");
    assert!(cursor.next_batch(1000).await?.is_none());

    assert_eq!(batch.num_rows, 3);
    assert_eq!(batch.column_data[0].len(), 3);

    println!("test_batch_larger_than_result: OK");
    Ok(())
}

/// Interleaving next() and next_batch() on the same cursor works correctly.
async fn test_interleave_next_and_batch(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT number::UInt64 AS n FROM system.numbers LIMIT 5")
        .fetch_rows()?;

    // Consume first row with next().
    let row = cursor.next().await?.expect("expected row 0");
    assert_eq!(row.values[0], Value::BigUnsigned(Some(0)));

    // Read the remaining 4 as a batch.
    let batch = cursor.next_batch(10).await?.expect("expected batch");
    assert!(cursor.next_batch(10).await?.is_none());

    assert_eq!(batch.num_rows, 4);
    for (i, v) in batch.column_data[0].iter().enumerate() {
        assert_eq!(*v, Value::BigUnsigned(Some((i + 1) as u64)));
    }

    println!("test_interleave_next_and_batch: OK");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:18123");

    test_column_layout(&client).await?;
    test_multiple_batches(&client).await?;
    test_empty_result(&client).await?;
    test_batch_size_one(&client).await?;
    test_columns_shared(&client).await?;
    test_mixed_types(&client).await?;
    test_batch_larger_than_result(&client).await?;
    test_interleave_next_and_batch(&client).await?;

    println!("All tests OK");
    Ok(())
}
