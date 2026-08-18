//! Typed, bounded query-result batches.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// A database cell used by editable table data.
///
/// Text values intentionally retain their database representation so drivers
/// can bind them with the destination column type. Binary and `NULL` values
/// remain distinct, while `Default` is only valid for inserted values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CellValue {
    Null,
    Text(String),
    Binary(Vec<u8>),
    Default,
}

/// One result column: its name plus the raw type name reported by the database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub database_type: String,
}

impl ColumnSchema {
    #[must_use]
    pub fn new(name: impl Into<String>, database_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            database_type: database_type.into(),
        }
    }
}

/// Column-major storage for one column of database text values.
///
/// Drivers already render every cell to its database text form, so a single
/// byte buffer plus an offset table replaces one heap allocation per cell.
/// `NULL` is tracked separately from the empty string.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextColumn {
    data: Vec<u8>,
    /// Byte offsets into `data`; always holds `rows + 1` entries.
    offsets: Vec<usize>,
    /// Bitmap where a set bit marks a `NULL` cell.
    nulls: Vec<u64>,
    rows: usize,
}

impl TextColumn {
    #[must_use]
    pub fn with_capacity(rows: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
        Self {
            data: Vec::new(),
            offsets,
            nulls: Vec::new(),
            rows: 0,
        }
    }

    pub fn push(&mut self, value: Option<&str>) {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        match value {
            Some(text) => self.data.extend_from_slice(text.as_bytes()),
            None => self.set_null(self.rows),
        }
        self.offsets.push(self.data.len());
        self.rows += 1;
    }

    fn set_null(&mut self, row: usize) {
        let word = row / 64;
        if word >= self.nulls.len() {
            self.nulls.resize(word + 1, 0);
        }
        self.nulls[word] |= 1u64 << (row % 64);
    }

    fn is_null(&self, row: usize) -> bool {
        self.nulls
            .get(row / 64)
            .is_some_and(|word| word & (1u64 << (row % 64)) != 0)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.rows
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Return the value at `row`, or `None` when the cell is `NULL`.
    #[must_use]
    pub fn value(&self, row: usize) -> Option<&str> {
        if row >= self.rows || self.is_null(row) {
            return None;
        }
        let start = self.offsets[row];
        let end = self.offsets[row + 1];
        // Only valid UTF-8 is ever pushed, so the slice cannot be malformed.
        std::str::from_utf8(&self.data[start..end]).ok()
    }

    /// Bytes occupied by `len` rows starting at `offset`.
    fn byte_span(&self, offset: usize, len: usize) -> usize {
        if len == 0 || offset >= self.rows {
            return 0;
        }
        let end = (offset + len).min(self.rows);
        self.offsets[end] - self.offsets[offset]
    }
}

/// A window over column-major result rows.
///
/// Slicing is O(1): the column buffers are shared and only the row window
/// moves, so paging a buffered result never copies cell data.
#[derive(Debug, Clone)]
pub struct RowBatch {
    schema: Arc<[ColumnSchema]>,
    columns: Arc<[TextColumn]>,
    offset: usize,
    rows: usize,
}

impl RowBatch {
    #[must_use]
    pub fn new(schema: Arc<[ColumnSchema]>, columns: Vec<TextColumn>) -> Self {
        let rows = columns.first().map_or(0, TextColumn::len);
        Self {
            schema,
            columns: columns.into(),
            offset: 0,
            rows,
        }
    }

    #[must_use]
    pub fn schema(&self) -> Arc<[ColumnSchema]> {
        Arc::clone(&self.schema)
    }

    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows
    }

    /// Return a cell, or `None` when it is `NULL` or out of range.
    #[must_use]
    pub fn value(&self, row: usize, column: usize) -> Option<&str> {
        if row >= self.rows {
            return None;
        }
        self.columns.get(column)?.value(self.offset + row)
    }

    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let offset = offset.min(self.rows);
        let rows = length.min(self.rows - offset);
        Self {
            schema: Arc::clone(&self.schema),
            columns: Arc::clone(&self.columns),
            offset: self.offset + offset,
            rows,
        }
    }

    /// Approximate retained bytes for this window only.
    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        let cells: usize = self
            .columns
            .iter()
            .map(|column| column.byte_span(self.offset, self.rows))
            .sum();
        let offsets = self.rows * self.columns.len() * size_of::<usize>();
        let nulls = self.rows.div_ceil(8) * self.columns.len();
        cells + offsets + nulls
    }
}

/// Accumulates rows for one [`RowBatch`].
///
/// Rows are staged before they are committed so a decode failure can never
/// leave the columns at different lengths.
#[derive(Debug)]
pub struct RowBatchBuilder {
    schema: Arc<[ColumnSchema]>,
    columns: Vec<TextColumn>,
    staged: Vec<Option<String>>,
}

impl RowBatchBuilder {
    #[must_use]
    pub fn new(schema: Arc<[ColumnSchema]>, row_capacity: usize) -> Self {
        let columns = (0..schema.len())
            .map(|_| TextColumn::with_capacity(row_capacity))
            .collect();
        Self {
            schema,
            columns,
            staged: Vec::new(),
        }
    }

    #[must_use]
    pub fn schema(&self) -> Arc<[ColumnSchema]> {
        Arc::clone(&self.schema)
    }

    /// Append one row by decoding each column in order.
    ///
    /// # Errors
    ///
    /// Propagates the decode error unchanged; the builder is left untouched.
    pub fn push_row<E>(
        &mut self,
        mut decode: impl FnMut(usize) -> Result<Option<String>, E>,
    ) -> Result<(), E> {
        self.staged.clear();
        for index in 0..self.columns.len() {
            self.staged.push(decode(index)?);
        }
        for (column, value) in self.columns.iter_mut().zip(&self.staged) {
            column.push(value.as_deref());
        }
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.first().map_or(0, TextColumn::len)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Emit the accumulated rows and reset the builder for the next batch.
    #[must_use]
    pub fn take_batch(&mut self) -> RowBatch {
        let columns = std::mem::replace(
            &mut self.columns,
            (0..self.schema.len())
                .map(|_| TextColumn::with_capacity(0))
                .collect(),
        );
        RowBatch::new(Arc::clone(&self.schema), columns)
    }
}

/// Schema metadata emitted before the first row of a result set.
#[derive(Debug, Clone)]
pub enum DataSchema {
    Tabular(Arc<[ColumnSchema]>),
    Documents,
    KeyValues,
}

impl DataSchema {
    #[must_use]
    pub fn from_batch(batch: &DataBatch) -> Self {
        match batch {
            DataBatch::Tabular(batch) => Self::Tabular(batch.schema()),
            DataBatch::Documents(_) => Self::Documents,
            DataBatch::KeyValues(_) => Self::KeyValues,
        }
    }
}

/// A typed binary-safe key/value entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyValueEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub value_type: String,
    pub ttl_millis: Option<u64>,
}

/// A batch that can be rendered by one of the built-in generic viewers.
#[derive(Debug, Clone)]
pub enum DataBatch {
    Tabular(RowBatch),
    Documents(Vec<serde_json::Value>),
    KeyValues(Vec<KeyValueEntry>),
}

impl DataBatch {
    #[must_use]
    pub fn row_count(&self) -> usize {
        match self {
            Self::Tabular(batch) => batch.row_count(),
            Self::Documents(documents) => documents.len(),
            Self::KeyValues(entries) => entries.len(),
        }
    }

    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        match self {
            Self::Tabular(batch) => batch.estimated_bytes(),
            Self::Documents(documents) => documents
                .iter()
                .map(|document| document.to_string().len())
                .sum(),
            Self::KeyValues(entries) => entries
                .iter()
                .map(|entry| {
                    entry.key.len()
                        + entry.value.len()
                        + entry.value_type.len()
                        + size_of::<Option<u64>>()
                })
                .sum(),
        }
    }

    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let available = self.row_count().saturating_sub(offset);
        let length = length.min(available);

        match self {
            Self::Tabular(batch) => Self::Tabular(batch.slice(offset, length)),
            Self::Documents(documents) => {
                Self::Documents(documents[offset.min(documents.len())..][..length].to_vec())
            }
            Self::KeyValues(entries) => {
                Self::KeyValues(entries[offset.min(entries.len())..][..length].to_vec())
            }
        }
    }

    #[must_use]
    pub fn documents(&self) -> Option<&[serde_json::Value]> {
        match self {
            Self::Documents(documents) => Some(documents),
            Self::Tabular(_) | Self::KeyValues(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Accepted { rows: usize, limit_reached: bool },
    RejectedByLimit,
    LimitAlreadyReached,
}

/// Retains only the bounded portion of an interactive query result.
#[derive(Debug)]
pub struct ResultBuffer {
    limits: BufferLimits,
    batches: Vec<DataBatch>,
    row_count: usize,
    estimated_bytes: usize,
    limit_reached: bool,
}

impl ResultBuffer {
    #[must_use]
    pub fn new(limits: BufferLimits) -> Self {
        Self {
            limits,
            batches: Vec::new(),
            row_count: 0,
            estimated_bytes: 0,
            limit_reached: limits.max_rows == 0 || limits.max_bytes == 0,
        }
    }

    #[must_use]
    pub fn append(&mut self, batch: DataBatch) -> AppendOutcome {
        if self.limit_reached {
            return AppendOutcome::LimitAlreadyReached;
        }

        let remaining_rows = self.limits.max_rows.saturating_sub(self.row_count);
        let requested_rows = batch.row_count().min(remaining_rows);
        let remaining_bytes = self.limits.max_bytes.saturating_sub(self.estimated_bytes);
        let accepted_rows = rows_fitting_bytes(&batch, requested_rows, remaining_bytes);

        if accepted_rows == 0 && batch.row_count() > 0 {
            self.limit_reached = true;
            return AppendOutcome::RejectedByLimit;
        }

        let accepted = batch.slice(0, accepted_rows);
        let accepted_bytes = accepted.estimated_bytes();
        self.row_count += accepted_rows;
        self.estimated_bytes += accepted_bytes;
        self.batches.push(accepted);
        self.limit_reached = accepted_rows < batch.row_count()
            || self.row_count >= self.limits.max_rows
            || self.estimated_bytes >= self.limits.max_bytes;

        AppendOutcome::Accepted {
            rows: accepted_rows,
            limit_reached: self.limit_reached,
        }
    }

    #[must_use]
    pub fn batches(&self) -> &[DataBatch] {
        &self.batches
    }

    #[must_use]
    pub fn limits(&self) -> BufferLimits {
        self.limits
    }

    #[must_use]
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    #[must_use]
    pub fn limit_reached(&self) -> bool {
        self.limit_reached
    }

    /// Return a logical row slice while preserving the original batch variants.
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Vec<DataBatch> {
        if length == 0 || offset >= self.row_count {
            return Vec::new();
        }

        let mut rows_to_skip = offset;
        let mut rows_remaining = length.min(self.row_count - offset);
        let mut result = Vec::new();
        for batch in &self.batches {
            if rows_remaining == 0 {
                break;
            }
            let batch_rows = batch.row_count();
            if rows_to_skip >= batch_rows {
                rows_to_skip -= batch_rows;
                continue;
            }
            let rows = (batch_rows - rows_to_skip).min(rows_remaining);
            result.push(batch.slice(rows_to_skip, rows));
            rows_remaining -= rows;
            rows_to_skip = 0;
        }
        result
    }
}

fn rows_fitting_bytes(batch: &DataBatch, upper_bound: usize, remaining_bytes: usize) -> usize {
    if upper_bound == 0 {
        return 0;
    }

    if batch.slice(0, upper_bound).estimated_bytes() <= remaining_bytes {
        return upper_bound;
    }

    let mut low = 0;
    let mut high = upper_bound;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if batch.slice(0, middle).estimated_bytes() <= remaining_bytes {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    low
}
