use std::sync::Arc;

#[cfg(feature = "arrow")]
use sea_orm_arrow::arrow;

use clickhouse_types::error::TypesError;
use clickhouse_types::{DataTypeNode, parse_rbwnat_columns_header};

use crate::{
    bytes_ext::BytesExt,
    cursors::RawCursor,
    data_row::{DataRow, RowBatch},
    error::{Error, Result},
    response::Response,
    rowbinary::value_de::decode_row,
};

/// A cursor that emits dynamically-typed [`DataRow`]s decoded from
/// `RowBinaryWithNamesAndTypes`.
///
/// Obtain one via [`crate::query::Query::fetch_rows`].
#[must_use]
pub struct DataRowCursor {
    raw: RawCursor,
    bytes: BytesExt,
    /// Column names, shared across all rows from this cursor.
    columns: Option<Arc<[Arc<str>]>>,
    /// Column types parsed once from the RBWNAT header.
    column_types: Option<Arc<[DataTypeNode]>>,
    #[cfg(feature = "arrow")]
    arrow_schema: Option<Arc<arrow::datatypes::Schema>>,
}

impl DataRowCursor {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            columns: None,
            column_types: None,
            #[cfg(feature = "arrow")]
            arrow_schema: None,
        }
    }

    /// Returns column names after the header has been read (i.e., after the
    /// first [`next`] call returns).
    ///
    /// [`next`]: DataRowCursor::next
    pub fn columns(&self) -> Option<&[Arc<str>]> {
        self.columns.as_deref()
    }

    /// Returns the total size in bytes received from the CH server since the
    /// cursor was created.
    ///
    /// This counts only the payload size (no HTTP headers).
    #[inline]
    pub fn received_bytes(&self) -> u64 {
        self.raw.received_bytes()
    }

    /// Returns the total size in bytes decompressed since the cursor was created.
    #[inline]
    pub fn decoded_bytes(&self) -> u64 {
        self.raw.decoded_bytes()
    }

    /// Emits the next row.
    ///
    /// Returns `Ok(None)` when all rows have been consumed.
    /// The result is unspecified if called after an `Err` is returned.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe.
    pub async fn next(&mut self) -> Result<Option<DataRow>> {
        if self.column_types.is_none() {
            self.read_header().await?;
        }

        // Clone the Arcs once per call — two cheap pointer increments — so
        // neither reference borrows `self` across the `.await` below.
        let column_types = self
            .column_types
            .as_ref()
            .expect("just initialised")
            .clone();
        let columns = self.columns.as_ref().expect("just initialised").clone();

        loop {
            if self.bytes.remaining() > 0 {
                let mut slice = self.bytes.slice();
                match decode_row(&mut slice, &*column_types) {
                    Ok(values) => {
                        // `set_remaining` takes `&self` (interior Cell), so holding
                        // the immutable `slice` borrow is fine here.
                        self.bytes.set_remaining(slice.len());
                        return Ok(Some(DataRow { columns, values }));
                    }
                    Err(Error::NotEnoughData) => {
                        // Fall through to fetch more data below.
                    }
                    Err(err) => return Err(err),
                }
            }

            // `slice` is dropped here, so the immutable borrow on `self.bytes`
            // ends before we call `extend` (which needs `&mut self.bytes`).
            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None if self.bytes.remaining() > 0 => {
                    // Partial row at EOF — usually a schema/type mismatch.
                    return Err(Error::NotEnoughData);
                }
                None => return Ok(None),
            }
        }
    }

    /// Reads up to `max_rows` rows and returns them as a column-oriented [`RowBatch`].
    ///
    /// Returns `Ok(None)` when all rows have been consumed.
    /// The result is unspecified if called after an `Err` is returned.
    pub async fn next_batch(&mut self, max_rows: usize) -> Result<Option<RowBatch>> {
        if self.column_types.is_none() {
            self.read_header().await?;
        }

        let column_count = self.column_types.as_ref().expect("just initialised").len();

        let mut column_data: Vec<Vec<sea_query::Value>> = (0..column_count)
            .map(|_| Vec::with_capacity(max_rows))
            .collect();
        let mut num_rows = 0;

        while num_rows < max_rows {
            match self.next().await? {
                Some(row) => {
                    for (col, value) in column_data.iter_mut().zip(row.values) {
                        col.push(value);
                    }
                    num_rows += 1;
                }
                None => break,
            }
        }

        if num_rows == 0 {
            return Ok(None);
        }

        let columns = self.columns.as_ref().expect("header was read").clone();
        Ok(Some(RowBatch {
            columns,
            column_data,
            num_rows,
        }))
    }

    /// Reads up to `max_rows` rows and returns them as an Arrow [`RecordBatch`].
    ///
    /// The schema is derived from the `RowBinaryWithNamesAndTypes` header using
    /// [`crate::arrow::schema::from_columns`] and is available on the returned
    /// batch via [`RecordBatch::schema`].
    ///
    /// Returns `Ok(None)` when all rows have been consumed.
    /// The result is unspecified if called after an `Err` is returned.
    ///
    /// [`RecordBatch`]: sea_orm_arrow::arrow::array::RecordBatch
    #[cfg(feature = "arrow")]
    pub async fn next_arrow_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Option<sea_orm_arrow::arrow::array::RecordBatch>> {
        use std::sync::Arc;

        let batch = match self.next_batch(max_rows).await? {
            Some(b) => b,
            None => return Ok(None),
        };

        let schema = self
            .arrow_schema
            .as_ref()
            .expect("header was read by next_batch")
            .clone();

        let columns = schema
            .fields()
            .iter()
            .zip(batch.column_data.iter())
            .map(
                |(field, values): (&Arc<arrow::datatypes::Field>, &Vec<sea_query::Value>)| {
                    sea_orm_arrow::values_to_arrow_array(values, field.data_type())
                        .map_err(|e| crate::error::Error::Other(Box::new(e)))
                },
            )
            .collect::<Result<Vec<_>>>()?;

        sea_orm_arrow::arrow::array::RecordBatch::try_new(schema, columns)
            .map_err(|e| crate::error::Error::Other(Box::new(e)))
            .map(Some)
    }

    #[cold]
    #[inline(never)]
    async fn read_header(&mut self) -> Result<()> {
        loop {
            if self.bytes.remaining() > 0 {
                let mut slice = self.bytes.slice();
                match parse_rbwnat_columns_header(&mut slice) {
                    Ok(cols) if !cols.is_empty() => {
                        self.bytes.set_remaining(slice.len());
                        #[cfg(feature = "arrow")]
                        {
                            self.arrow_schema =
                                Some(Arc::new(crate::arrow::schema::from_columns(&cols)));
                        }
                        let columns: Arc<[Arc<str>]> =
                            cols.iter().map(|c| Arc::from(c.name.as_str())).collect();
                        let types: Arc<[DataTypeNode]> =
                            cols.into_iter().map(|c| c.data_type).collect();
                        self.columns = Some(columns);
                        self.column_types = Some(types);
                        return Ok(());
                    }
                    Ok(_) => {
                        return Err(Error::BadResponse(
                            "Expected at least one column in the header".to_string(),
                        ));
                    }
                    Err(TypesError::NotEnoughData(_)) => {
                        // Need more bytes; fall through to fetch.
                    }
                    Err(err) => {
                        return Err(Error::InvalidColumnsHeader(err.into()));
                    }
                }
            }

            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None => {
                    return Err(Error::BadResponse(
                        "Could not read columns header".to_string(),
                    ));
                }
            }
        }
    }
}
