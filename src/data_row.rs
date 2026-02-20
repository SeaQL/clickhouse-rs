use std::sync::Arc;

/// A dynamically-typed row returned by [`crate::query::DataRowCursor`].
///
/// Column names are shared across all rows from the same cursor via [`Arc`],
/// making each row cheap to construct (one `Arc` clone + one `Vec` alloc).
///
/// # Example
///
/// ```ignore
/// let mut cursor = client
///     .query("SELECT number, toString(number) AS s FROM system.numbers LIMIT 3")
///     .fetch_rows()?;
///
/// while let Some(row) = cursor.next().await? {
///     for (col, val) in row.columns.iter().zip(&row.values) {
///         println!("{col}: {val:?}");
///     }
/// }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "sea-query")))]
pub struct DataRow {
    /// Column names in schema order, shared with all other rows from the same query.
    pub columns: Arc<[Arc<str>]>,
    /// Per-column values decoded from `RowBinaryWithNamesAndTypes`.
    pub values: Vec<sea_query::Value>,
}

/// A column-oriented batch of dynamically-typed rows.
///
/// Instead of one [`sea_query::Value`] vector per row (as in [`DataRow`]),
/// `RowBatch` stores one `Vec<Value>` per column. This layout is efficient
/// for columnar processing and is the natural precursor to Apache Arrow
/// record batches.
///
/// All `column_data` vectors have exactly `num_rows` entries.
///
/// Obtain batches via [`crate::query::DataRowCursor::next_batch`].
///
/// # Example
///
/// ```ignore
/// let mut cursor = client
///     .query("SELECT number, toString(number) AS s FROM system.numbers LIMIT 100")
///     .fetch_rows()?;
///
/// while let Some(batch) = cursor.next_batch(32).await? {
///     println!("{} rows, {} columns", batch.num_rows, batch.columns.len());
///     for (name, col) in batch.columns.iter().zip(&batch.column_data) {
///         println!("  {name}: {} values", col.len());
///     }
/// }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "sea-query")))]
pub struct RowBatch {
    /// Column names in schema order, shared with all other batches from the same query.
    pub columns: Arc<[Arc<str>]>,
    /// Per-column value vectors; `column_data[i]` holds all values for `columns[i]`.
    ///
    /// Every inner `Vec` has exactly `num_rows` entries.
    pub column_data: Vec<Vec<sea_query::Value>>,
    /// Number of rows in this batch.
    pub num_rows: usize,
}
