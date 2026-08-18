use std::{
    fmt,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::atomic_file::AtomicFile;
use crate::text_format::{encode_base64, markdown_cell, write_csv_record};
use dbc_core::{
    driver::{DatabaseSession, QueryEvent},
    error::DriverError,
    query::QueryRequest,
};
use dbc_core::result::{DataBatch, DataSchema, ResultBuffer};
use futures_util::TryStreamExt;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub const FULL_EXPORT_MAX_ROWS: u64 = 1_000_000;
pub const FULL_EXPORT_QUERY_ROWS: usize = 1_000_001;
pub const FULL_EXPORT_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const EXPORT_CHANNEL_CAPACITY: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Csv,
    JsonLines,
    /// A Markdown table, for pasting into a review or an issue.
    Markdown,
}

impl ExportFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::JsonLines => "jsonl",
            Self::Markdown => "md",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::JsonLines => "JSONL",
            Self::Markdown => "Markdown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportLimits {
    pub max_rows: u64,
    pub max_bytes: u64,
}

impl ExportLimits {
    pub const FULL: Self = Self {
        max_rows: FULL_EXPORT_MAX_ROWS,
        max_bytes: FULL_EXPORT_MAX_BYTES,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportSummary {
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("没有可导出的结果集")]
    NoResultSet,
    #[error("查询返回了多个不兼容的结果集")]
    IncompatibleSchema,
    #[error("导出超过 {limit} 行硬限制")]
    RowLimitExceeded { limit: u64 },
    #[error("导出超过 {limit} 字节硬限制")]
    ByteLimitExceeded { limit: u64 },
    #[error("查询结果流未正常结束")]
    IncompleteStream,
    #[error("导出已取消")]
    Cancelled,
    #[error("导出查询失败：{0}")]
    Driver(#[from] DriverError),
    #[error("导出写入线程失败：{0}")]
    Join(String),
    #[error("JSON 写入失败：{0}")]
    Json(#[from] serde_json::Error),
    #[error("文件操作失败：{0}")]
    Io(#[source] io::Error),
}

#[cfg(test)]
fn export_buffer(
    path: &Path,
    format: ExportFormat,
    schema: Option<&DataSchema>,
    buffer: &ResultBuffer,
    limits: ExportLimits,
) -> Result<ExportSummary, ExportError> {
    if schema.is_none() && buffer.batches().is_empty() {
        return Err(ExportError::NoResultSet);
    }
    write_atomic(
        path,
        format,
        limits,
        ExportSource::Buffer { schema, buffer },
        None,
    )
}

pub fn export_buffer_cancellable(
    path: &Path,
    format: ExportFormat,
    schema: Option<&DataSchema>,
    buffer: &ResultBuffer,
    limits: ExportLimits,
    cancellation: &CancellationToken,
) -> Result<ExportSummary, ExportError> {
    if schema.is_none() && buffer.batches().is_empty() {
        return Err(ExportError::NoResultSet);
    }
    write_atomic(
        path,
        format,
        limits,
        ExportSource::Buffer { schema, buffer },
        Some(cancellation),
    )
}

pub async fn export_query(
    session: Arc<dyn DatabaseSession>,
    request: QueryRequest,
    path: PathBuf,
    format: ExportFormat,
    cancellation: CancellationToken,
) -> Result<ExportSummary, ExportError> {
    let (sender, receiver) = mpsc::channel(EXPORT_CHANNEL_CAPACITY);
    let writer_cancellation = cancellation.clone();
    let writer = tokio::task::spawn_blocking(move || {
        write_atomic(
            &path,
            format,
            ExportLimits::FULL,
            ExportSource::Receiver(receiver),
            Some(&writer_cancellation),
        )
    });

    let producer = async {
        let mut stream = session.execute(request, cancellation).await?;
        while let Some(event) = stream.try_next().await? {
            let input = match event {
                QueryEvent::Schema(schema) => Some(ExportInput::Schema(schema)),
                QueryEvent::Rows(batch) => Some(ExportInput::Batch(batch)),
                QueryEvent::Finished(_) => Some(ExportInput::Finish),
                QueryEvent::Message(_) | QueryEvent::AffectedRows(_) => None,
            };
            if let Some(input) = input
                && sender.send(input).await.is_err()
            {
                break;
            }
        }
        Ok::<(), DriverError>(())
    }
    .await;
    drop(sender);

    let writer_result = writer
        .await
        .map_err(|error| ExportError::Join(error.to_string()))?;
    producer.map_err(ExportError::Driver)?;
    writer_result
}

enum ExportInput {
    Schema(DataSchema),
    Batch(DataBatch),
    Finish,
}

enum ExportSource<'a> {
    Buffer {
        schema: Option<&'a DataSchema>,
        buffer: &'a ResultBuffer,
    },
    Receiver(mpsc::Receiver<ExportInput>),
}

fn write_atomic(
    path: &Path,
    format: ExportFormat,
    limits: ExportLimits,
    source: ExportSource<'_>,
    cancellation: Option<&CancellationToken>,
) -> Result<ExportSummary, ExportError> {
    check_cancellation(cancellation)?;
    let mut temporary = AtomicFile::create(path).map_err(map_io)?;

    let (rows, bytes) = {
        let limited = LimitedWriter::new(temporary.writer(), limits.max_bytes);
        let mut writer = ExportWriter::new(format, limited, limits.max_rows);
        match source {
            ExportSource::Buffer { schema, buffer } => {
                if let Some(schema) = schema {
                    writer.write_schema(schema)?;
                }
                for batch in buffer.batches() {
                    check_cancellation(cancellation)?;
                    writer.write_batch(batch)?;
                }
            }
            ExportSource::Receiver(mut receiver) => {
                let mut finished = false;
                while let Some(input) = receiver.blocking_recv() {
                    check_cancellation(cancellation)?;
                    match input {
                        ExportInput::Schema(schema) => writer.write_schema(&schema)?,
                        ExportInput::Batch(batch) => writer.write_batch(&batch)?,
                        ExportInput::Finish => {
                            finished = true;
                            break;
                        }
                    }
                }
                if !finished {
                    return Err(ExportError::IncompleteStream);
                }
            }
        }
        let (limited, rows) = writer.finish()?;
        (rows, limited.written())
    };

    check_cancellation(cancellation)?;
    // `commit` flushes, syncs and renames; dropping without it leaves the
    // existing target untouched.
    temporary.commit().map_err(map_io)?;
    Ok(ExportSummary { rows, bytes })
}

fn check_cancellation(
    cancellation: Option<&CancellationToken>,
) -> Result<(), ExportError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        Err(ExportError::Cancelled)
    } else {
        Ok(())
    }
}

enum ExportWriter<W: Write> {
    Csv(Box<CsvExporter<W>>),
    JsonLines(JsonLinesExporter<W>),
    Markdown(Box<MarkdownExporter<W>>),
}

impl<W: Write> ExportWriter<W> {
    fn new(format: ExportFormat, writer: W, max_rows: u64) -> Self {
        match format {
            ExportFormat::Csv => Self::Csv(Box::new(CsvExporter::new(writer, max_rows))),
            ExportFormat::JsonLines => {
                Self::JsonLines(JsonLinesExporter::new(writer, max_rows))
            }
            ExportFormat::Markdown => {
                Self::Markdown(Box::new(MarkdownExporter::new(writer, max_rows)))
            }
        }
    }

    fn write_schema(&mut self, schema: &DataSchema) -> Result<(), ExportError> {
        match self {
            Self::Csv(writer) => writer.write_schema(schema),
            Self::JsonLines(writer) => writer.write_schema(schema),
            Self::Markdown(writer) => writer.write_schema(schema),
        }
    }

    fn write_batch(&mut self, batch: &DataBatch) -> Result<(), ExportError> {
        match self {
            Self::Csv(writer) => writer.write_batch(batch),
            Self::JsonLines(writer) => writer.write_batch(batch),
            Self::Markdown(writer) => writer.write_batch(batch),
        }
    }

    fn finish(self) -> Result<(W, u64), ExportError> {
        match self {
            Self::Csv(writer) => writer.finish(),
            Self::JsonLines(writer) => writer.finish(),
            Self::Markdown(writer) => writer.finish(),
        }
    }
}

struct CsvExporter<W: Write> {
    writer: W,
    schema: Option<DataSchema>,
    rows: u64,
    max_rows: u64,
}

impl<W: Write> CsvExporter<W> {
    fn new(writer: W, max_rows: u64) -> Self {
        Self {
            writer,
            schema: None,
            rows: 0,
            max_rows,
        }
    }

    fn write_schema(&mut self, schema: &DataSchema) -> Result<(), ExportError> {
        if let Some(existing) = self.schema.as_ref() {
            return ensure_compatible_schema(existing, schema);
        }
        match schema {
            DataSchema::Tabular(schema) => write_csv_record(
                &mut self.writer,
                schema.iter().map(|column| column.name.as_str()),
            )
            .map_err(map_io)?,
            DataSchema::Documents => {
                write_csv_record(&mut self.writer, ["document"]).map_err(map_io)?;
            }
            DataSchema::KeyValues => {
                write_csv_record(&mut self.writer, ["key", "value", "type", "ttl_millis"])
                    .map_err(map_io)?;
            }
        }
        self.schema = Some(schema.clone());
        Ok(())
    }

    fn write_batch(&mut self, batch: &DataBatch) -> Result<(), ExportError> {
        self.ensure_batch_schema(batch)?;
        match batch {
            DataBatch::Tabular(batch) => {
                for row in 0..batch.row_count() {
                    self.check_row_limit()?;
                    let record = (0..batch.column_count())
                        .map(|column| batch.value(row, column).unwrap_or_default())
                        .collect::<Vec<_>>();
                    write_csv_record(&mut self.writer, record).map_err(map_io)?;
                    self.rows += 1;
                }
            }
            DataBatch::Documents(documents) => {
                for document in documents {
                    self.check_row_limit()?;
                    write_csv_record(&mut self.writer, [serde_json::to_string(document)?])
                        .map_err(map_io)?;
                    self.rows += 1;
                }
            }
            DataBatch::KeyValues(entries) => {
                for entry in entries {
                    self.check_row_limit()?;
                    write_csv_record(
                        &mut self.writer,
                        [
                            encode_base64(&entry.key),
                            encode_base64(&entry.value),
                            entry.value_type.clone(),
                            entry
                                .ttl_millis
                                .map(|ttl| ttl.to_string())
                                .unwrap_or_default(),
                        ],
                    )
                    .map_err(map_io)?;
                    self.rows += 1;
                }
            }
        }
        Ok(())
    }

    fn ensure_batch_schema(&mut self, batch: &DataBatch) -> Result<(), ExportError> {
        let inferred = DataSchema::from_batch(batch);
        self.write_schema(&inferred)
    }

    fn check_row_limit(&self) -> Result<(), ExportError> {
        if self.rows >= self.max_rows {
            Err(ExportError::RowLimitExceeded {
                limit: self.max_rows,
            })
        } else {
            Ok(())
        }
    }

    fn finish(mut self) -> Result<(W, u64), ExportError> {
        self.writer.flush().map_err(map_io)?;
        Ok((self.writer, self.rows))
    }
}

struct JsonLinesExporter<W: Write> {
    writer: W,
    schema: Option<DataSchema>,
    rows: u64,
    max_rows: u64,
}

impl<W: Write> JsonLinesExporter<W> {
    fn new(writer: W, max_rows: u64) -> Self {
        Self {
            writer,
            schema: None,
            rows: 0,
            max_rows,
        }
    }

    fn write_schema(&mut self, schema: &DataSchema) -> Result<(), ExportError> {
        if let Some(existing) = self.schema.as_ref() {
            return ensure_compatible_schema(existing, schema);
        }
        if let DataSchema::Tabular(schema) = schema {
            let columns = schema
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>();
            let types = schema
                .iter()
                .map(|column| column.database_type.as_str())
                .collect::<Vec<_>>();
            write_json_line(
                &mut self.writer,
                &json!({
                    "type": "schema",
                    "columns": columns,
                    "types": types,
                }),
            )?;
        }
        self.schema = Some(schema.clone());
        Ok(())
    }

    fn write_batch(&mut self, batch: &DataBatch) -> Result<(), ExportError> {
        self.ensure_batch_schema(batch)?;
        match batch {
            DataBatch::Tabular(batch) => {
                for row in 0..batch.row_count() {
                    self.check_row_limit()?;
                    let values = (0..batch.column_count())
                        .map(|column| match batch.value(row, column) {
                            Some(text) => Value::String(text.to_owned()),
                            None => Value::Null,
                        })
                        .collect::<Vec<_>>();
                    write_json_line(&mut self.writer, &json!({ "values": values }))?;
                    self.rows += 1;
                }
            }
            DataBatch::Documents(documents) => {
                for document in documents {
                    self.check_row_limit()?;
                    write_json_line(&mut self.writer, document)?;
                    self.rows += 1;
                }
            }
            DataBatch::KeyValues(entries) => {
                for entry in entries {
                    self.check_row_limit()?;
                    write_json_line(
                        &mut self.writer,
                        &json!({
                            "key": encode_base64(&entry.key),
                            "value": encode_base64(&entry.value),
                            "type": entry.value_type,
                            "ttl_millis": entry.ttl_millis,
                        }),
                    )?;
                    self.rows += 1;
                }
            }
        }
        Ok(())
    }

    fn ensure_batch_schema(&mut self, batch: &DataBatch) -> Result<(), ExportError> {
        let inferred = DataSchema::from_batch(batch);
        self.write_schema(&inferred)
    }

    fn check_row_limit(&self) -> Result<(), ExportError> {
        if self.rows >= self.max_rows {
            Err(ExportError::RowLimitExceeded {
                limit: self.max_rows,
            })
        } else {
            Ok(())
        }
    }

    fn finish(mut self) -> Result<(W, u64), ExportError> {
        self.writer.flush().map_err(map_io)?;
        Ok((self.writer, self.rows))
    }
}

/// A GitHub-flavoured Markdown table, for pasting into a review or an issue.
struct MarkdownExporter<W: Write> {
    writer: W,
    schema: Option<DataSchema>,
    columns: usize,
    rows: u64,
    max_rows: u64,
}

impl<W: Write> MarkdownExporter<W> {
    fn new(writer: W, max_rows: u64) -> Self {
        Self {
            writer,
            schema: None,
            columns: 0,
            rows: 0,
            max_rows,
        }
    }

    fn write_header(&mut self, headers: &[String]) -> Result<(), ExportError> {
        self.columns = headers.len();
        self.write_row(headers)?;
        let separator = headers.iter().map(|_| "---".to_owned()).collect::<Vec<_>>();
        self.write_row(&separator)
    }

    fn write_row(&mut self, cells: &[String]) -> Result<(), ExportError> {
        self.writer.write_all(b"|").map_err(map_io)?;
        for cell in cells {
            self.writer
                .write_all(format!(" {} |", markdown_cell(cell)).as_bytes())
                .map_err(map_io)?;
        }
        self.writer.write_all(b"\n").map_err(map_io)
    }

    fn write_schema(&mut self, schema: &DataSchema) -> Result<(), ExportError> {
        if let Some(existing) = self.schema.as_ref() {
            return ensure_compatible_schema(existing, schema);
        }
        let headers = match schema {
            DataSchema::Tabular(schema) => {
                schema.iter().map(|column| column.name.clone()).collect()
            }
            DataSchema::Documents => vec!["document".to_owned()],
            DataSchema::KeyValues => vec![
                "key".to_owned(),
                "value".to_owned(),
                "type".to_owned(),
                "ttl_millis".to_owned(),
            ],
        };
        self.write_header(&headers)?;
        self.schema = Some(schema.clone());
        Ok(())
    }

    fn write_batch(&mut self, batch: &DataBatch) -> Result<(), ExportError> {
        let inferred = DataSchema::from_batch(batch);
        self.write_schema(&inferred)?;
        match batch {
            DataBatch::Tabular(batch) => {
                for row in 0..batch.row_count() {
                    self.check_row_limit()?;
                    let cells = (0..batch.column_count())
                        .map(|column| batch.value(row, column).unwrap_or_default().to_owned())
                        .collect::<Vec<_>>();
                    self.write_row(&cells)?;
                    self.rows += 1;
                }
            }
            DataBatch::Documents(documents) => {
                for document in documents {
                    self.check_row_limit()?;
                    self.write_row(&[serde_json::to_string(document)?])?;
                    self.rows += 1;
                }
            }
            DataBatch::KeyValues(entries) => {
                for entry in entries {
                    self.check_row_limit()?;
                    self.write_row(&[
                        encode_base64(&entry.key),
                        encode_base64(&entry.value),
                        entry.value_type.clone(),
                        entry
                            .ttl_millis
                            .map(|ttl| ttl.to_string())
                            .unwrap_or_default(),
                    ])?;
                    self.rows += 1;
                }
            }
        }
        Ok(())
    }

    fn check_row_limit(&self) -> Result<(), ExportError> {
        if self.rows >= self.max_rows {
            return Err(ExportError::RowLimitExceeded {
                limit: self.max_rows,
            });
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(W, u64), ExportError> {
        self.writer.flush().map_err(map_io)?;
        Ok((self.writer, self.rows))
    }
}

fn ensure_compatible_schema(
    existing: &DataSchema,
    incoming: &DataSchema,
) -> Result<(), ExportError> {
    let compatible = match (existing, incoming) {
        (DataSchema::Tabular(left), DataSchema::Tabular(right)) => left == right,
        (DataSchema::Documents, DataSchema::Documents)
        | (DataSchema::KeyValues, DataSchema::KeyValues) => true,
        _ => false,
    };
    if compatible {
        Ok(())
    } else {
        Err(ExportError::IncompatibleSchema)
    }
}

fn write_json_line<W: Write>(writer: &mut W, value: &Value) -> Result<(), ExportError> {
    let encoded = serde_json::to_vec(value)?;
    writer.write_all(&encoded).map_err(map_io)?;
    writer.write_all(b"\n").map_err(map_io)
}

struct LimitedWriter<W> {
    writer: W,
    written: u64,
    limit: u64,
}

impl<W> LimitedWriter<W> {
    fn new(writer: W, limit: u64) -> Self {
        Self {
            writer,
            written: 0,
            limit,
        }
    }

    fn written(&self) -> u64 {
        self.written
    }
}

impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let requested = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
        if self.written.saturating_add(requested) > self.limit {
            return Err(io::Error::other(ByteLimitMarker { limit: self.limit }));
        }
        let written = self.writer.write(buffer)?;
        self.written = self
            .written
            .saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Debug)]
struct ByteLimitMarker {
    limit: u64,
}

impl fmt::Display for ByteLimitMarker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "export byte limit exceeded: {}", self.limit)
    }
}

impl std::error::Error for ByteLimitMarker {}

fn map_io(error: io::Error) -> ExportError {
    if let Some(marker) = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ByteLimitMarker>())
    {
        ExportError::ByteLimitExceeded {
            limit: marker.limit,
        }
    } else {
        ExportError::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dbc_core::result::{BufferLimits, ColumnSchema, KeyValueEntry, RowBatchBuilder};
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::{
        ExportError, ExportFormat, ExportLimits, export_buffer,
        export_buffer_cancellable,
    };
    use dbc_core::result::{DataBatch, DataSchema, ResultBuffer};

    fn tabular_buffer() -> (DataSchema, ResultBuffer) {
        let schema: Arc<[ColumnSchema]> = vec![
            ColumnSchema::new("duplicate", "bigint"),
            ColumnSchema::new("duplicate", "text"),
        ]
        .into();
        let mut builder = RowBatchBuilder::new(Arc::clone(&schema), 2);
        for row in [[Some("1"), Some("nested")], [None, Some("value")]] {
            builder
                .push_row(|index| Ok::<_, ()>(row[index].map(str::to_owned)))
                .expect("tabular fixture should decode");
        }
        let mut buffer = ResultBuffer::new(BufferLimits {
            max_rows: 100,
            max_bytes: usize::MAX,
        });
        let _outcome = buffer.append(DataBatch::Tabular(builder.take_batch()));
        (DataSchema::Tabular(schema), buffer)
    }

    #[test]
    fn csv_preserves_duplicate_headers_and_nulls() {
        let directory = tempfile::tempdir().expect("temporary directory should exist");
        let path = directory.path().join("result.csv");
        let (schema, buffer) = tabular_buffer();

        let summary = export_buffer(
            &path,
            ExportFormat::Csv,
            Some(&schema),
            &buffer,
            ExportLimits {
                max_rows: 100,
                max_bytes: 1024 * 1024,
            },
        )
        .expect("CSV export should succeed");
        let content = std::fs::read_to_string(path).expect("CSV should be readable");

        assert_eq!(summary.rows, 2);
        assert_eq!(
            content,
            "duplicate,duplicate\n1,nested\n,value\n"
        );
    }

    #[test]
    fn json_lines_writes_schema_before_value_arrays() {
        let directory = tempfile::tempdir().expect("temporary directory should exist");
        let path = directory.path().join("result.jsonl");
        let (schema, buffer) = tabular_buffer();

        export_buffer(
            &path,
            ExportFormat::JsonLines,
            Some(&schema),
            &buffer,
            ExportLimits {
                max_rows: 100,
                max_bytes: 1024 * 1024,
            },
        )
        .expect("JSONL export should succeed");
        let content = std::fs::read_to_string(path).expect("JSONL should be readable");
        let lines = content.lines().collect::<Vec<_>>();
        let schema_line: Value =
            serde_json::from_str(lines[0]).expect("schema line should be JSON");
        let second_row: Value =
            serde_json::from_str(lines[2]).expect("row line should be JSON");

        assert_eq!(schema_line["type"], "schema");
        assert_eq!(
            schema_line["columns"],
            json!(["duplicate", "duplicate"])
        );
        assert_eq!(schema_line["types"], json!(["bigint", "text"]));
        assert_eq!(second_row["values"], json!([null, "value"]));
    }

    #[test]
    fn markdown_export_writes_a_table_and_escapes_pipes() {
        let directory = tempfile::tempdir().expect("temporary directory should exist");
        let path = directory.path().join("result.md");
        let schema: Arc<[ColumnSchema]> = vec![ColumnSchema::new("note", "text")].into();
        let mut builder = RowBatchBuilder::new(Arc::clone(&schema), 2);
        for value in [Some("a|b"), None] {
            builder
                .push_row(|_| Ok::<_, ()>(value.map(str::to_owned)))
                .expect("fixture rows should decode");
        }
        let mut buffer = ResultBuffer::new(BufferLimits {
            max_rows: 100,
            max_bytes: usize::MAX,
        });
        let _outcome = buffer.append(DataBatch::Tabular(builder.take_batch()));

        export_buffer(
            &path,
            ExportFormat::Markdown,
            Some(&DataSchema::Tabular(schema)),
            &buffer,
            ExportLimits {
                max_rows: 100,
                max_bytes: 1024 * 1024,
            },
        )
        .expect("Markdown export should succeed");

        let content = std::fs::read_to_string(path).expect("Markdown should be readable");

        assert_eq!(content, "| note |\n| --- |\n| a\\|b |\n|  |\n");
    }

    #[test]
    fn documents_and_key_values_keep_nested_and_binary_data() {
        let directory = tempfile::tempdir().expect("temporary directory should exist");
        let documents_path = directory.path().join("documents.jsonl");
        let key_values_path = directory.path().join("keys.jsonl");
        let mut documents = ResultBuffer::new(BufferLimits {
            max_rows: 10,
            max_bytes: usize::MAX,
        });
        let _outcome = documents.append(DataBatch::Documents(vec![json!({
            "nested": {"enabled": true},
            "items": [1, 2]
        })]));
        let mut key_values = ResultBuffer::new(BufferLimits {
            max_rows: 10,
            max_bytes: usize::MAX,
        });
        let _outcome = key_values.append(DataBatch::KeyValues(vec![KeyValueEntry {
            key: vec![0, 255],
            value: vec![1, 2, 3],
            value_type: "bytes".to_owned(),
            ttl_millis: None,
        }]));

        export_buffer(
            &documents_path,
            ExportFormat::JsonLines,
            Some(&DataSchema::Documents),
            &documents,
            ExportLimits {
                max_rows: 10,
                max_bytes: 1024 * 1024,
            },
        )
        .expect("document export should succeed");
        export_buffer(
            &key_values_path,
            ExportFormat::JsonLines,
            Some(&DataSchema::KeyValues),
            &key_values,
            ExportLimits {
                max_rows: 10,
                max_bytes: 1024 * 1024,
            },
        )
        .expect("key/value export should succeed");

        let document: Value = serde_json::from_str(
            &std::fs::read_to_string(documents_path)
                .expect("documents should be readable"),
        )
        .expect("document line should be JSON");
        let key_value: Value = serde_json::from_str(
            &std::fs::read_to_string(key_values_path)
                .expect("key/value output should be readable"),
        )
        .expect("key/value line should be JSON");
        assert_eq!(document["nested"]["enabled"], true);
        assert_eq!(key_value["key"], "AP8=");
        assert_eq!(key_value["value"], "AQID");
    }

    #[test]
    fn hard_limits_leave_existing_target_unchanged() {
        let directory = tempfile::tempdir().expect("temporary directory should exist");
        let path = directory.path().join("existing.csv");
        std::fs::write(&path, "original").expect("target fixture should be writable");
        let (schema, buffer) = tabular_buffer();

        let error = export_buffer(
            &path,
            ExportFormat::Csv,
            Some(&schema),
            &buffer,
            ExportLimits {
                max_rows: 1,
                max_bytes: 1024 * 1024,
            },
        )
        .expect_err("row limit should reject the export");

        assert!(matches!(
            error,
            ExportError::RowLimitExceeded { limit: 1 }
        ));
        assert_eq!(
            std::fs::read_to_string(&path).expect("target should remain readable"),
            "original"
        );
        assert_eq!(
            std::fs::read_dir(directory.path())
                .expect("directory should be readable")
                .count(),
            1
        );
    }

    #[test]
    fn byte_limit_is_reported_without_replacing_target() {
        let directory = tempfile::tempdir().expect("temporary directory should exist");
        let path = directory.path().join("existing.jsonl");
        std::fs::write(&path, "original").expect("target fixture should be writable");
        let (schema, buffer) = tabular_buffer();

        let error = export_buffer(
            &path,
            ExportFormat::JsonLines,
            Some(&schema),
            &buffer,
            ExportLimits {
                max_rows: 100,
                max_bytes: 8,
            },
        )
        .expect_err("byte limit should reject the export");

        assert!(matches!(
            error,
            ExportError::ByteLimitExceeded { limit: 8 }
        ));
        assert_eq!(
            std::fs::read_to_string(path).expect("target should remain readable"),
            "original"
        );
    }

    #[test]
    fn cancellation_does_not_replace_target() {
        let directory = tempfile::tempdir().expect("temporary directory should exist");
        let path = directory.path().join("existing.csv");
        std::fs::write(&path, "original").expect("target fixture should be writable");
        let (schema, buffer) = tabular_buffer();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = export_buffer_cancellable(
            &path,
            ExportFormat::Csv,
            Some(&schema),
            &buffer,
            ExportLimits {
                max_rows: 100,
                max_bytes: 1024 * 1024,
            },
            &cancellation,
        )
        .expect_err("cancelled export should fail");

        assert!(matches!(error, ExportError::Cancelled));
        assert_eq!(
            std::fs::read_to_string(path).expect("target should remain readable"),
            "original"
        );
    }
}
