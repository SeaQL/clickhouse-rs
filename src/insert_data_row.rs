use crate::{
    AccessType, Client, Result, data_row::DataRow, error::Error, formats,
    insert_formatted::BufInsertFormatted, row_metadata::RowMetadata, rowbinary::serialize_data_row,
};
use clickhouse_types::{Column, put_rbwnat_columns_header};
use std::{future::Future, time::Duration};

// Match the buffer constants used by Insert<T>.
const BUFFER_SIZE: usize = 256 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 2048;

/// An INSERT specialised for [`DataRow`] values.
///
/// Obtained via [`crate::Client::insert_data_row`].
///
/// When the client has validation enabled (the default), column types are fetched
/// from the server schema and used to correctly encode Nullable columns,
/// Date/DateTime/UUID values, and so on.
///
/// Call [`DataRowInsert::end`] to finalise the INSERT.
#[cfg_attr(docsrs, doc(cfg(feature = "sea-ql")))]
#[must_use]
pub struct DataRowInsert {
    insert: BufInsertFormatted,
    /// Schema metadata used to write the RowBinaryWithNamesAndTypes header on the
    /// first write.  `None` when validation is disabled.
    row_metadata: Option<RowMetadata>,
    /// Schema columns in DataRow column order, used for type-guided serialization.
    /// `None` when validation is disabled (best-effort RowBinary encoding).
    columns: Option<Box<[Column]>>,
}

impl Client {
    /// Starts an INSERT for dynamically-typed [`DataRow`] values.
    ///
    /// `proto` is used to determine column names; all rows passed to
    /// [`DataRowInsert::write_row`] must have values in the same column order.
    ///
    /// When validation is enabled (the default), the table schema is fetched once
    /// and cached. This enables correct encoding of Nullable columns,
    /// Date/DateTime/UUID types, and schema mismatch detection.
    ///
    /// When validation is disabled, a best-effort RowBinary encoding is used that
    /// works correctly only for non-nullable primitive columns.
    #[cfg_attr(docsrs, doc(cfg(feature = "sea-ql")))]
    pub async fn insert_data_row(&self, table: &str, proto: &DataRow) -> Result<DataRowInsert> {
        // Build escaped field list from proto column names.
        let fields: String =
            proto
                .columns
                .iter()
                .enumerate()
                .fold(String::new(), |mut s, (i, col)| {
                    if i > 0 {
                        s.push(',');
                    }
                    crate::sql::escape::identifier(col.as_ref(), &mut s).expect("impossible");
                    s
                });

        if self.get_validation() {
            let meta = self.get_insert_metadata(table).await?;

            // Resolve each DataRow column to its schema column (in DataRow order).
            let mut ordered_columns: Vec<Column> = Vec::with_capacity(proto.columns.len());
            for col_name in proto.columns.iter() {
                let idx = meta
                    .column_lookup
                    .get(col_name.as_ref())
                    .copied()
                    .ok_or_else(|| {
                        Error::SchemaMismatch(format!(
                            "insert_data_row: column '{col_name}' not found in table '{table}'"
                        ))
                    })?;
                if meta.column_default_kinds[idx].is_immutable() {
                    return Err(Error::SchemaMismatch(format!(
                        "insert_data_row: column '{col_name}' is immutable ({})",
                        meta.column_default_kinds[idx],
                    )));
                }
                ordered_columns.push(meta.row_metadata.columns[idx].clone());
            }

            let row_metadata = RowMetadata {
                columns: ordered_columns.clone(),
                access_type: AccessType::WithSeqAccess,
            };

            let sql = format!(
                "INSERT INTO {table}({fields}) FORMAT {}",
                formats::ROW_BINARY_WITH_NAMES_AND_TYPES
            );
            Ok(DataRowInsert::new(
                self,
                sql,
                Some(row_metadata),
                Some(ordered_columns.into_boxed_slice()),
            ))
        } else {
            let sql = format!(
                "INSERT INTO {table}({fields}) FORMAT {}",
                formats::ROW_BINARY
            );
            Ok(DataRowInsert::new(self, sql, None, None))
        }
    }
}

impl DataRowInsert {
    /// Creates a new `DataRowInsert`.
    ///
    /// * `sql`           – complete `INSERT INTO … FORMAT …` statement.
    /// * `row_metadata`  – schema metadata for writing the RBWNAT header; `None` for plain RowBinary.
    /// * `columns`       – column definitions in DataRow order for type-guided serialisation.
    pub(crate) fn new(
        client: &Client,
        sql: String,
        row_metadata: Option<RowMetadata>,
        columns: Option<Box<[Column]>>,
    ) -> Self {
        Self {
            insert: client
                .insert_formatted_with(sql)
                .buffered_with_capacity(BUFFER_SIZE),
            row_metadata,
            columns,
        }
    }

    /// Sets send/end timeouts; see [`crate::insert::Insert::with_timeouts`] for details.
    pub fn with_timeouts(
        mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) -> Self {
        self.insert.set_timeouts(send_timeout, end_timeout);
        self
    }

    /// Serializes `row` and appends it to the INSERT buffer.
    ///
    /// Flushes the buffer to the network when it exceeds the internal chunk size.
    pub fn write_row<'a>(
        &'a mut self,
        row: &'a DataRow,
    ) -> impl Future<Output = Result<()>> + 'a + Send {
        let result = self.do_write_row(row);
        async move {
            result?;
            if self.insert.buf_len() >= MIN_CHUNK_SIZE {
                self.insert.flush().await?;
            }
            Ok(())
        }
    }

    /// Finalises the INSERT, causing the server to process all buffered data.
    pub async fn end(mut self) -> Result<()> {
        self.insert.end().await
    }

    fn do_write_row(&mut self, row: &DataRow) -> Result<()> {
        let fresh = self.insert.init_request_if_required()?;
        if fresh {
            if let Some(metadata) = &self.row_metadata {
                put_rbwnat_columns_header(&metadata.columns, self.insert.buffer_mut())
                    .inspect_err(|_| self.insert.abort())?;
            }
        }

        let result = serialize_data_row(self.insert.buffer_mut(), row, self.columns.as_deref());
        if result.is_err() {
            self.insert.abort();
        }
        result
    }
}

#[cfg(feature = "arrow")]
impl Client {
    /// Starts an INSERT for an Arrow [`RecordBatch`].
    ///
    /// Column names are taken from the batch schema. All batches passed to
    /// [`DataRowInsert::write_batch`] must have the same schema.
    ///
    /// Internally builds a prototype [`DataRow`] from the schema and delegates
    /// to [`Client::insert_data_row`], so validation and typed encoding apply
    /// in the same way.
    ///
    /// [`RecordBatch`]: sea_orm_arrow::arrow::array::RecordBatch
    #[cfg_attr(docsrs, doc(cfg(feature = "arrow")))]
    pub async fn insert_arrow(
        &self,
        table: &str,
        batch: &sea_orm_arrow::arrow::array::RecordBatch,
    ) -> Result<DataRowInsert> {
        use std::sync::Arc;

        let columns: Arc<[Arc<str>]> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| Arc::from(f.name().as_str()))
            .collect();
        let proto = DataRow {
            columns,
            values: vec![],
        };
        self.insert_data_row(table, &proto).await
    }
}

#[cfg(feature = "arrow")]
impl DataRowInsert {
    /// Serializes every row in `batch` and appends them to the INSERT buffer.
    ///
    /// Each Arrow column element is converted to a [`sea_query::Value`] via
    /// [`sea_orm_arrow::arrow_array_to_value`], then written as a [`DataRow`].
    /// The buffer is flushed to the network whenever it exceeds the internal
    /// chunk size, so large batches stream incrementally.
    #[cfg_attr(docsrs, doc(cfg(feature = "arrow")))]
    pub async fn write_batch(
        &mut self,
        batch: &sea_orm_arrow::arrow::array::RecordBatch,
    ) -> Result<()> {
        use std::sync::Arc;

        let columns: Arc<[Arc<str>]> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| Arc::from(f.name().as_str()))
            .collect();

        let schema = batch.schema();
        let arrow_columns = batch.columns();

        for row in 0..batch.num_rows() {
            let values = arrow_columns
                .iter()
                .zip(schema.fields())
                .map(|(col, field)| {
                    crate::arrow::value::element_to_value(col.as_ref(), field.data_type(), row)
                })
                .collect::<Result<Vec<_>>>()?;

            let data_row = DataRow {
                columns: columns.clone(),
                values,
            };
            self.do_write_row(&data_row)?;

            if self.insert.buf_len() >= MIN_CHUNK_SIZE {
                self.insert.flush().await?;
            }
        }
        Ok(())
    }
}
