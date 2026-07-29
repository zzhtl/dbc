//! Typed, bounded query-result batches.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

/// Schema metadata emitted before the first row of a result set.
#[derive(Debug, Clone)]
pub enum DataSchema {
    Tabular(SchemaRef),
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
    Tabular(RecordBatch),
    Documents(Vec<serde_json::Value>),
    KeyValues(Vec<KeyValueEntry>),
}

impl DataBatch {
    #[must_use]
    pub fn row_count(&self) -> usize {
        match self {
            Self::Tabular(batch) => batch.num_rows(),
            Self::Documents(documents) => documents.len(),
            Self::KeyValues(entries) => entries.len(),
        }
    }

    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        match self {
            Self::Tabular(batch) => batch.get_array_memory_size(),
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
            Self::Tabular(batch) => Self::Tabular(batch.slice(offset.min(batch.num_rows()), length)),
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
