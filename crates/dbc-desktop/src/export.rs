use std::{
    fmt,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use dbc_core::{
    driver::{DatabaseSession, QueryEvent},
    error::DriverError,
    query::QueryRequest,
};
use dbc_data::{DataBatch, DataSchema, ResultBuffer};
use futures_util::TryStreamExt;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
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
}

impl ExportFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::JsonLines => "jsonl",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::JsonLines => "JSONL",
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
    #[error("不支持导出 Arrow 类型：{0}")]
    UnsupportedArrowType(String),
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
    #[error("CSV 写入失败：{0}")]
    Csv(String),
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
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temporary = NamedTempFile::new_in(parent).map_err(map_io)?;

    let (rows, bytes) = {
        let limited = LimitedWriter::new(temporary.as_file_mut(), limits.max_bytes);
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
    temporary.as_file_mut().sync_all().map_err(map_io)?;
    check_cancellation(cancellation)?;
    temporary.persist(path).map_err(|error| map_io(error.error))?;
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
}

impl<W: Write> ExportWriter<W> {
    fn new(format: ExportFormat, writer: W, max_rows: u64) -> Self {
        match format {
            ExportFormat::Csv => Self::Csv(Box::new(CsvExporter::new(writer, max_rows))),
            ExportFormat::JsonLines => {
                Self::JsonLines(JsonLinesExporter::new(writer, max_rows))
            }
        }
    }

    fn write_schema(&mut self, schema: &DataSchema) -> Result<(), ExportError> {
        match self {
            Self::Csv(writer) => writer.write_schema(schema),
            Self::JsonLines(writer) => writer.write_schema(schema),
        }
    }

    fn write_batch(&mut self, batch: &DataBatch) -> Result<(), ExportError> {
        match self {
            Self::Csv(writer) => writer.write_batch(batch),
            Self::JsonLines(writer) => writer.write_batch(batch),
        }
    }

    fn finish(self) -> Result<(W, u64), ExportError> {
        match self {
            Self::Csv(writer) => writer.finish(),
            Self::JsonLines(writer) => writer.finish(),
        }
    }
}

struct CsvExporter<W: Write> {
    writer: csv::Writer<W>,
    schema: Option<DataSchema>,
    rows: u64,
    max_rows: u64,
}

impl<W: Write> CsvExporter<W> {
    fn new(writer: W, max_rows: u64) -> Self {
        Self {
            writer: csv::WriterBuilder::new().from_writer(writer),
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
            DataSchema::Tabular(schema) => self
                .writer
                .write_record(schema.fields().iter().map(|field| field.name()))
                .map_err(map_csv)?,
            DataSchema::Documents => self
                .writer
                .write_record(["document"])
                .map_err(map_csv)?,
            DataSchema::KeyValues => self
                .writer
                .write_record(["key", "value", "type", "ttl_millis"])
                .map_err(map_csv)?,
        }
        self.schema = Some(schema.clone());
        Ok(())
    }

    fn write_batch(&mut self, batch: &DataBatch) -> Result<(), ExportError> {
        self.ensure_batch_schema(batch)?;
        match batch {
            DataBatch::Tabular(batch) => {
                for row_index in 0..batch.num_rows() {
                    self.check_row_limit()?;
                    let record = batch
                        .columns()
                        .iter()
                        .map(|array| csv_cell(array.as_ref(), row_index))
                        .collect::<Result<Vec<_>, _>>()?;
                    self.writer.write_record(record).map_err(map_csv)?;
                    self.rows += 1;
                }
            }
            DataBatch::Documents(documents) => {
                for document in documents {
                    self.check_row_limit()?;
                    self.writer
                        .write_record([serde_json::to_string(document)?])
                        .map_err(map_csv)?;
                    self.rows += 1;
                }
            }
            DataBatch::KeyValues(entries) => {
                for entry in entries {
                    self.check_row_limit()?;
                    self.writer
                        .write_record([
                            BASE64.encode(&entry.key),
                            BASE64.encode(&entry.value),
                            entry.value_type.clone(),
                            entry
                                .ttl_millis
                                .map(|ttl| ttl.to_string())
                                .unwrap_or_default(),
                        ])
                        .map_err(map_csv)?;
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
        let writer = self
            .writer
            .into_inner()
            .map_err(|error| map_io(error.into_error()))?;
        Ok((writer, self.rows))
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
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>();
            let types = schema
                .fields()
                .iter()
                .map(|field| field.data_type().to_string())
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
                for row_index in 0..batch.num_rows() {
                    self.check_row_limit()?;
                    let values = batch
                        .columns()
                        .iter()
                        .map(|array| json_cell(array.as_ref(), row_index))
                        .collect::<Result<Vec<_>, _>>()?;
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
                            "key": BASE64.encode(&entry.key),
                            "value": BASE64.encode(&entry.value),
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

fn csv_cell(array: &dyn Array, row_index: usize) -> Result<String, ExportError> {
    if array.is_null(row_index) {
        return Ok(String::new());
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(row_index).to_owned());
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(array.value(row_index).to_owned());
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(array.value(row_index).to_string());
    }
    macro_rules! numeric_cell {
        ($array_type:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$array_type>() {
                return Ok(array.value(row_index).to_string());
            }
        };
    }
    numeric_cell!(Int8Array);
    numeric_cell!(Int16Array);
    numeric_cell!(Int32Array);
    numeric_cell!(Int64Array);
    numeric_cell!(UInt8Array);
    numeric_cell!(UInt16Array);
    numeric_cell!(UInt32Array);
    numeric_cell!(UInt64Array);
    numeric_cell!(Float32Array);
    numeric_cell!(Float64Array);
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(BASE64.encode(array.value(row_index)));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(BASE64.encode(array.value(row_index)));
    }
    Err(ExportError::UnsupportedArrowType(
        array.data_type().to_string(),
    ))
}

fn json_cell(array: &dyn Array, row_index: usize) -> Result<Value, ExportError> {
    if array.is_null(row_index) {
        return Ok(Value::Null);
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Value::String(array.value(row_index).to_owned()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Value::String(array.value(row_index).to_owned()));
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Value::Bool(array.value(row_index)));
    }
    macro_rules! integer_cell {
        ($array_type:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$array_type>() {
                return Ok(Value::from(array.value(row_index)));
            }
        };
    }
    integer_cell!(Int8Array);
    integer_cell!(Int16Array);
    integer_cell!(Int32Array);
    integer_cell!(Int64Array);
    integer_cell!(UInt8Array);
    integer_cell!(UInt16Array);
    integer_cell!(UInt32Array);
    integer_cell!(UInt64Array);
    macro_rules! float_cell {
        ($array_type:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$array_type>() {
                let value = f64::from(array.value(row_index));
                return Ok(serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .unwrap_or_else(|| Value::String(value.to_string())));
            }
        };
    }
    float_cell!(Float32Array);
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        let value = array.value(row_index);
        return Ok(serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string())));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Value::String(BASE64.encode(array.value(row_index))));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(Value::String(BASE64.encode(array.value(row_index))));
    }
    Err(ExportError::UnsupportedArrowType(
        array.data_type().to_string(),
    ))
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

fn map_csv(error: csv::Error) -> ExportError {
    let message = error.to_string();
    match error.into_kind() {
        csv::ErrorKind::Io(error) => map_io(error),
        _ => ExportError::Csv(message),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use dbc_data::{BufferLimits, KeyValueEntry};
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::{
        ExportError, ExportFormat, ExportLimits, export_buffer,
        export_buffer_cancellable,
    };
    use dbc_data::{DataBatch, DataSchema, ResultBuffer};

    fn tabular_buffer() -> (DataSchema, ResultBuffer) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("duplicate", DataType::Int64, true),
            Field::new("duplicate", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("nested"), Some("value")])),
            ],
        )
        .expect("tabular fixture should be valid");
        let mut buffer = ResultBuffer::new(BufferLimits {
            max_rows: 100,
            max_bytes: usize::MAX,
        });
        let _outcome = buffer.append(DataBatch::Tabular(batch));
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
        assert_eq!(second_row["values"], json!([null, "value"]));
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
