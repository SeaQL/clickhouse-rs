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
