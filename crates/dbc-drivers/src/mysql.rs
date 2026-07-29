use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    future::Future,
    mem,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_stream::try_stream;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use dbc_core::{
    capability::QueryLanguage,
    diagnostics::{
        ExecutionPlan, ExplainMode, ExplainRequest, PlanFormat, SlowQueryEntry, SlowQueryOrder,
        SlowQueryPage, SlowQueryRequest,
    },
    driver::{
        ConnectionProfile, DatabaseSession, DriverDescriptor, DriverFactory, QueryEvent,
        QueryStats, QueryStream, SecretValue,
    },
    error::DriverError,
    metadata::{
        DatabaseObject, DatabaseObjectKind, ObjectListRequest, ObjectPage, ObjectPath,
    },
    query::QueryRequest,
    sql::{
        SqlDialect, StatementRisk, classify_sql, is_single_statement,
        is_single_statement_for,
    },
};
use dbc_data::{DataBatch, DataSchema};
use futures_util::TryStreamExt;
use sqlx::{
    AssertSqlSafe, Column, Decode, Either, Error as SqlxError, Executor, MySql, Row,
    SqlSafeStr, Statement, Type, TypeInfo, ValueRef,
    mysql::{
        MySqlColumn, MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow,
        types::MySqlTime,
    },
};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::mysql_descriptor;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const QUERY_BATCH_ROWS: usize = 4_096;

const LIST_SCHEMAS_SQL: &str = r#"
SELECT
    CONCAT('mysql:schema:', SCHEMA_NAME) AS object_id,
    SCHEMA_NAME AS object_name,
    'schema' AS object_kind,
    TRUE AS has_children,
    JSON_OBJECT(
        'character_set', DEFAULT_CHARACTER_SET_NAME,
        'collation', DEFAULT_COLLATION_NAME,
        'system', SCHEMA_NAME IN ('information_schema', 'mysql', 'performance_schema', 'sys')
    ) AS properties
FROM information_schema.SCHEMATA
WHERE
    ? OR SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
ORDER BY SCHEMA_NAME
LIMIT ? OFFSET ?
"#;

const LIST_SCHEMA_OBJECTS_SQL: &str = r#"
SELECT object_id, object_name, object_kind, has_children, properties
FROM (
    SELECT
        CONCAT('mysql:table:', TABLE_SCHEMA, ':', TABLE_NAME) AS object_id,
        TABLE_NAME AS object_name,
        CASE WHEN TABLE_TYPE = 'VIEW' THEN 'view' ELSE 'table' END AS object_kind,
        TRUE AS has_children,
        JSON_OBJECT(
            'engine', ENGINE,
            'estimated_rows', TABLE_ROWS,
            'data_bytes', DATA_LENGTH,
            'index_bytes', INDEX_LENGTH
        ) AS properties
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = ?

    UNION ALL

    SELECT
        CONCAT('mysql:routine:', ROUTINE_SCHEMA, ':', SPECIFIC_NAME) AS object_id,
        ROUTINE_NAME AS object_name,
        'routine' AS object_kind,
        FALSE AS has_children,
        JSON_OBJECT(
            'routine_type', ROUTINE_TYPE,
            'data_type', DATA_TYPE
        ) AS properties
    FROM information_schema.ROUTINES
    WHERE ROUTINE_SCHEMA = ?
) AS objects
ORDER BY object_kind, object_name, object_id
LIMIT ? OFFSET ?
"#;

const LIST_RELATION_CHILDREN_SQL: &str = r#"
SELECT object_id, object_name, object_kind, has_children, properties
FROM (
    SELECT
        CONCAT('mysql:column:', TABLE_SCHEMA, ':', TABLE_NAME, ':', ORDINAL_POSITION)
            AS object_id,
        COLUMN_NAME AS object_name,
        'column' AS object_kind,
        FALSE AS has_children,
        JSON_OBJECT(
            'data_type', COLUMN_TYPE,
            'nullable', IS_NULLABLE = 'YES',
            'ordinal', ORDINAL_POSITION,
            'default', COLUMN_DEFAULT,
            'extra', EXTRA
        ) AS properties
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?

    UNION ALL

    SELECT
        CONCAT('mysql:index:', TABLE_SCHEMA, ':', TABLE_NAME, ':', INDEX_NAME) AS object_id,
        INDEX_NAME AS object_name,
        'index' AS object_kind,
        FALSE AS has_children,
        JSON_OBJECT(
            'unique', MIN(NON_UNIQUE) = 0,
            'type', MIN(INDEX_TYPE),
            'columns', GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ')
        ) AS properties
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME

    UNION ALL

    SELECT
        CONCAT('mysql:constraint:', CONSTRAINT_SCHEMA, ':', TABLE_NAME, ':', CONSTRAINT_NAME)
            AS object_id,
        CONSTRAINT_NAME AS object_name,
        'constraint' AS object_kind,
        FALSE AS has_children,
        JSON_OBJECT('constraint_type', CONSTRAINT_TYPE) AS properties
    FROM information_schema.TABLE_CONSTRAINTS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?

    UNION ALL

    SELECT
        CONCAT('mysql:trigger:', TRIGGER_SCHEMA, ':', TRIGGER_NAME) AS object_id,
        TRIGGER_NAME AS object_name,
        'trigger' AS object_kind,
        FALSE AS has_children,
        JSON_OBJECT(
            'timing', ACTION_TIMING,
            'event', EVENT_MANIPULATION,
            'statement', ACTION_STATEMENT
        ) AS properties
    FROM information_schema.TRIGGERS
    WHERE EVENT_OBJECT_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?
) AS objects
ORDER BY object_kind, object_name, object_id
LIMIT ? OFFSET ?
"#;

/// Creates native MySQL and MariaDB sessions backed by a bounded SQLx pool.
#[derive(Debug, Clone)]
pub struct MySqlFactory {
    descriptor: DriverDescriptor,
}

impl MySqlFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            descriptor: mysql_descriptor(),
        }
    }
}

impl Default for MySqlFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DriverFactory for MySqlFactory {
    fn descriptor(&self) -> &DriverDescriptor {
        &self.descriptor
    }

    async fn connect(
        &self,
        profile: &ConnectionProfile,
        secret: Option<&SecretValue>,
    ) -> Result<Arc<dyn DatabaseSession>, DriverError> {
        if profile.driver_id != self.descriptor.id {
            return Err(DriverError::Connection(
                "connection profile does not target MySQL".to_owned(),
            ));
        }
        let endpoint = profile.endpoint.trim();
        if !endpoint.starts_with("mysql://") {
            return Err(DriverError::Connection(
                "MySQL endpoint must use mysql://".to_owned(),
            ));
        }

        let mut options = MySqlConnectOptions::from_str(endpoint)
            .map_err(|_| DriverError::Connection("MySQL endpoint is not a valid URL".to_owned()))?;
        if let Some(user) = profile.user.as_deref() {
            options = options.username(user);
        }
        if let Some(database) = profile.database.as_deref() {
            options = options.database(database);
        }
        if let Some(secret) = secret {
            options = options.password(secret.expose());
        }

        let connect = MySqlPoolOptions::new()
            .max_connections(4)
            .connect_with(options);
        let pool = timeout(CONNECT_TIMEOUT, connect)
            .await
            .map_err(|_| DriverError::Timeout)?
            .map_err(map_connect_error)?;
        let version_row = timeout(
            CONNECT_TIMEOUT,
            sqlx::query("SELECT VERSION()").fetch_one(&pool),
        )
        .await
        .map_err(|_| DriverError::Timeout)?
        .map_err(map_connect_error)?;
        let version = version_row.try_get::<String, _>(0).map_err(|_| {
            DriverError::Connection("MySQL returned an invalid server version".to_owned())
        })?;
        Ok(Arc::new(MySqlSession {
            pool,
            is_mariadb: version.to_ascii_lowercase().contains("mariadb"),
        }))
    }
}

#[derive(Debug)]
struct MySqlSession {
    pool: MySqlPool,
    is_mariadb: bool,
}

#[async_trait]
impl DatabaseSession for MySqlSession {
    async fn execute(
        &self,
        request: QueryRequest,
        cancellation: CancellationToken,
    ) -> Result<QueryStream, DriverError> {
        request
            .validate()
            .map_err(|error| DriverError::Query(error.to_string()))?;
        if request.language != QueryLanguage::Sql {
            return Err(DriverError::Unsupported(
                "MySQL accepts SQL requests only".to_owned(),
            ));
        }
        if !is_single_statement_for(&request.text, SqlDialect::MySql) {
            return Err(DriverError::Query(
                "MySQL execution requires exactly one SQL statement".to_owned(),
            ));
        }

        let deadline = Instant::now()
            .checked_add(request.timeout)
            .ok_or_else(|| DriverError::Query("query timeout is too large".to_owned()))?;
        let text = request.text;
        let row_limit = request.row_limit;
        let pool = self.pool.clone();
        let is_write = classify_sql(&text) != StatementRisk::ReadOnly;

        Ok(Box::pin(try_stream! {
            let started = Instant::now();
            let mut returned_rows = 0_u64;
            let mut affected_rows = 0_u64;
            let statement = controlled(
                deadline,
                &cancellation,
                pool.prepare(AssertSqlSafe(text.as_str()).into_sql_str()),
            )
            .await?
            .map_err(map_query_error)?;
            let returns_rows = !statement.columns().is_empty();
            let mut batch = TextBatchBuilder::from_columns(statement.columns());
            if returns_rows {
                yield QueryEvent::Schema(DataSchema::Tabular(batch.schema()));
            }
            drop(statement);

            // Editor input reaches this boundary only after DBC policy evaluation.
            let mut results =
                sqlx::raw_sql(AssertSqlSafe(text.as_str())).fetch_many(&pool);
            while returned_rows < usize_to_u64(row_limit) {
                let next = controlled(deadline, &cancellation, results.try_next())
                    .await?
                    .map_err(map_query_error)?;
                let Some(result) = next else {
                    break;
                };
                match result {
                    Either::Right(row) => {
                        batch.push(&row)?;
                        returned_rows = returned_rows.saturating_add(1);
                        if batch.len() >= QUERY_BATCH_ROWS
                            || returned_rows >= usize_to_u64(row_limit)
                        {
                            let finished_batch = mem::replace(
                                &mut batch,
                                TextBatchBuilder::from_columns(row.columns()),
                            )
                            .finish()?;
                            yield QueryEvent::Rows(DataBatch::Tabular(finished_batch));
                        }
                    }
                    Either::Left(result) => {
                        let affected = result.rows_affected();
                        if is_write && (affected > 0 || !returns_rows) {
                            affected_rows = affected_rows.saturating_add(affected);
                            yield QueryEvent::AffectedRows(affected);
                        }
                    }
                }
            }
            if !batch.is_empty() {
                yield QueryEvent::Rows(DataBatch::Tabular(batch.finish()?));
            }

            yield QueryEvent::Finished(QueryStats {
                elapsed_millis: duration_millis(started.elapsed()),
                returned_rows,
                affected_rows,
                row_limit_reached: returned_rows >= usize_to_u64(row_limit),
            });
        }))
    }

    async fn list_objects(
        &self,
        request: ObjectListRequest,
        cancellation: CancellationToken,
    ) -> Result<ObjectPage, DriverError> {
        request
            .validate()
            .map_err(|error| DriverError::Query(error.to_string()))?;
        let deadline = operation_deadline(request.timeout)?;
        let offset = decode_cursor(request.cursor.as_deref())?;
        let fetch_limit = request
            .limit
            .checked_add(1)
            .ok_or_else(|| DriverError::Query("object page limit is too large".to_owned()))?;
        let fetch_limit = usize_to_i64(fetch_limit, "object page limit")?;
        let offset_i64 = usize_to_i64(offset, "object page cursor")?;
        let parent = request.parent.unwrap_or_default();

        let rows = match parent.segments.as_slice() {
            [] => {
                controlled(
                    deadline,
                    &cancellation,
                    sqlx::query(LIST_SCHEMAS_SQL)
                        .bind(request.include_system)
                        .bind(fetch_limit)
                        .bind(offset_i64)
                        .fetch_all(&self.pool),
                )
                .await?
                .map_err(map_query_error)?
            }
            [schema] => {
                controlled(
                    deadline,
                    &cancellation,
                    sqlx::query(LIST_SCHEMA_OBJECTS_SQL)
                        .bind(schema)
                        .bind(schema)
                        .bind(fetch_limit)
                        .bind(offset_i64)
                        .fetch_all(&self.pool),
                )
                .await?
                .map_err(map_query_error)?
            }
            [schema, relation] => {
                controlled(
                    deadline,
                    &cancellation,
                    sqlx::query(LIST_RELATION_CHILDREN_SQL)
                        .bind(schema)
                        .bind(relation)
                        .bind(schema)
                        .bind(relation)
                        .bind(schema)
                        .bind(relation)
                        .bind(schema)
                        .bind(relation)
                        .bind(fetch_limit)
                        .bind(offset_i64)
                        .fetch_all(&self.pool),
                )
                .await?
                .map_err(map_query_error)?
            }
            _ => {
                return Err(DriverError::Unsupported(
                    "MySQL object paths deeper than a table".to_owned(),
                ));
            }
        };

        build_object_page(rows, &parent, request.limit, offset)
    }

    async fn explain(
        &self,
        request: ExplainRequest,
        cancellation: CancellationToken,
    ) -> Result<ExecutionPlan, DriverError> {
        request
            .validate()
            .map_err(|error| DriverError::Query(error.to_string()))?;
        if !is_single_statement(&request.text) {
            return Err(DriverError::Query(
                "execution plans require exactly one valid SQL statement".to_owned(),
            ));
        }
        if request.mode == ExplainMode::Analyze
            && classify_sql(&request.text) != StatementRisk::ReadOnly
        {
            return Err(DriverError::Permission(
                "EXPLAIN ANALYZE is limited to read-only statements".to_owned(),
            ));
        }

        let analyzed = request.mode == ExplainMode::Analyze;
        let (prefix, format) = match (analyzed, self.is_mariadb) {
            (false, _) => ("EXPLAIN FORMAT=JSON", PlanFormat::Json),
            (true, true) => ("ANALYZE FORMAT=JSON", PlanFormat::Json),
            (true, false) => ("EXPLAIN ANALYZE", PlanFormat::Text),
        };
        let statement = format!("{prefix}\n{}", request.text);
        let deadline = operation_deadline(request.timeout)?;
        let row = controlled(
            deadline,
            &cancellation,
            sqlx::query(AssertSqlSafe(statement)).fetch_one(&self.pool),
        )
        .await?
        .map_err(map_query_error)?;
        let document = plan_document(&row)?;

        Ok(ExecutionPlan {
            engine: if self.is_mariadb {
                "mariadb".to_owned()
            } else {
                "mysql".to_owned()
            },
            format,
            analyzed,
            document,
            metadata: BTreeMap::new(),
        })
    }

    async fn slow_queries(
        &self,
        request: SlowQueryRequest,
        cancellation: CancellationToken,
    ) -> Result<SlowQueryPage, DriverError> {
        request
            .validate()
            .map_err(|error| DriverError::Query(error.to_string()))?;
        let deadline = operation_deadline(request.timeout)?;
        let statement = slow_query_statement(request.order);
        let limit = usize_to_i64(request.limit, "slow-query page limit")?;
        let rows = controlled(
            deadline,
            &cancellation,
            sqlx::query(AssertSqlSafe(statement))
                .bind(request.minimum_mean_time_millis.unwrap_or(0.0))
                .bind(limit)
                .fetch_all(&self.pool),
        )
        .await?
        .map_err(map_slow_query_error)?;
        let entries = rows
            .iter()
            .map(slow_query_entry)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SlowQueryPage {
            source: "performance_schema.events_statements_summary_by_digest".to_owned(),
            entries,
        })
    }

    async fn close(&self) -> Result<(), DriverError> {
        self.pool.close().await;
        Ok(())
    }
}

async fn controlled<T, F>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: F,
) -> Result<T, DriverError>
where
    F: Future<Output = T>,
{
    tokio::select! {
        _ = cancellation.cancelled() => Err(DriverError::Cancelled),
        result = tokio::time::timeout_at(deadline, future) => {
            result.map_err(|_| DriverError::Timeout)
        }
    }
}

fn operation_deadline(duration: Duration) -> Result<Instant, DriverError> {
    Instant::now()
        .checked_add(duration)
        .ok_or_else(|| DriverError::Query("operation timeout is too large".to_owned()))
}

fn decode_cursor(cursor: Option<&str>) -> Result<usize, DriverError> {
    cursor
        .map(str::parse)
        .transpose()
        .map(|offset| offset.unwrap_or_default())
        .map_err(|_| DriverError::Query("object page cursor is invalid".to_owned()))
}

fn usize_to_i64(value: usize, field: &str) -> Result<i64, DriverError> {
    i64::try_from(value).map_err(|_| DriverError::Query(format!("{field} is too large")))
}

fn build_object_page(
    rows: Vec<MySqlRow>,
    parent: &ObjectPath,
    limit: usize,
    offset: usize,
) -> Result<ObjectPage, DriverError> {
    let has_next_page = rows.len() > limit;
    let items = rows
        .iter()
        .take(limit)
        .map(|row| database_object(row, parent))
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = if has_next_page {
        Some(
            offset
                .checked_add(limit)
                .ok_or_else(|| DriverError::Query("object page cursor overflowed".to_owned()))?
                .to_string(),
        )
    } else {
        None
    };
    Ok(ObjectPage { items, next_cursor })
}

fn database_object(row: &MySqlRow, parent: &ObjectPath) -> Result<DatabaseObject, DriverError> {
    let id = object_column::<String>(row, "object_id")?;
    let name = object_column::<String>(row, "object_name")?;
    let kind = object_kind(&object_column::<String>(row, "object_kind")?);
    let has_children = object_column::<bool>(row, "has_children")?;
    let properties = json_column(row, "properties")?;

    Ok(DatabaseObject {
        id,
        path: parent.child(name.clone()),
        name,
        kind,
        has_children,
        properties: json_properties(properties),
    })
}

fn object_column<T>(row: &MySqlRow, name: &str) -> Result<T, DriverError>
where
    for<'value> T: Decode<'value, MySql> + Type<MySql>,
{
    row.try_get::<T, _>(name).map_err(|_| {
        DriverError::Internal(format!(
            "MySQL object discovery returned an invalid `{name}` column"
        ))
    })
}

fn json_column(row: &MySqlRow, name: &str) -> Result<serde_json::Value, DriverError> {
    if let Ok(value) = row.try_get::<serde_json::Value, _>(name) {
        return Ok(value);
    }
    let text = row.try_get::<String, _>(name).map_err(|_| {
        DriverError::Internal(format!("MySQL returned an invalid `{name}` JSON column"))
    })?;
    serde_json::from_str(&text)
        .map_err(|_| DriverError::Internal(format!("MySQL returned malformed `{name}` JSON")))
}

fn json_properties(value: serde_json::Value) -> BTreeMap<String, serde_json::Value> {
    match value {
        serde_json::Value::Object(properties) => properties.into_iter().collect(),
        _ => BTreeMap::new(),
    }
}

fn object_kind(kind: &str) -> DatabaseObjectKind {
    match kind {
        "schema" => DatabaseObjectKind::Schema,
        "table" => DatabaseObjectKind::Table,
        "view" => DatabaseObjectKind::View,
        "column" => DatabaseObjectKind::Column,
        "index" => DatabaseObjectKind::Index,
        "constraint" => DatabaseObjectKind::Constraint,
        "trigger" => DatabaseObjectKind::Trigger,
        "routine" => DatabaseObjectKind::Routine,
        other => DatabaseObjectKind::Other(other.to_owned()),
    }
}

fn plan_document(row: &MySqlRow) -> Result<serde_json::Value, DriverError> {
    if let Ok(value) = row.try_get::<serde_json::Value, _>(0) {
        return Ok(value);
    }
    let text = row.try_get::<String, _>(0).map_err(|_| {
        DriverError::Internal("MySQL returned an invalid execution plan".to_owned())
    })?;
    Ok(serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)))
}

fn slow_query_statement(order: SlowQueryOrder) -> String {
    let order_by = match order {
        SlowQueryOrder::MeanTime => {
            "mean_time_millis DESC, total_time_millis DESC"
        }
        SlowQueryOrder::TotalTime => {
            "total_time_millis DESC, mean_time_millis DESC"
        }
        SlowQueryOrder::Calls => "calls DESC, total_time_millis DESC",
    };
    format!(
        r#"
SELECT
    DIGEST AS fingerprint,
    SCHEMA_NAME AS database_name,
    DIGEST_TEXT AS query_text,
    COUNT_STAR AS calls,
    CAST(SUM_TIMER_WAIT AS DOUBLE) / 1000000000.0 AS total_time_millis,
    CAST(AVG_TIMER_WAIT AS DOUBLE) / 1000000000.0 AS mean_time_millis,
    CAST(MAX_TIMER_WAIT AS DOUBLE) / 1000000000.0 AS max_time_millis,
    SUM_ROWS_SENT AS result_rows,
    SUM_ROWS_EXAMINED AS rows_examined,
    SUM_NO_INDEX_USED AS no_index_used,
    SUM_NO_GOOD_INDEX_USED AS no_good_index_used
FROM performance_schema.events_statements_summary_by_digest
WHERE
    DIGEST_TEXT IS NOT NULL
    AND CAST(AVG_TIMER_WAIT AS DOUBLE) / 1000000000.0 >= ?
ORDER BY {order_by}
LIMIT ?
"#
    )
}

fn slow_query_entry(row: &MySqlRow) -> Result<SlowQueryEntry, DriverError> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "rows_examined".to_owned(),
        serde_json::Value::from(slow_column::<u64>(row, "rows_examined")?),
    );
    metadata.insert(
        "no_index_used".to_owned(),
        serde_json::Value::from(slow_column::<u64>(row, "no_index_used")?),
    );
    metadata.insert(
        "no_good_index_used".to_owned(),
        serde_json::Value::from(slow_column::<u64>(row, "no_good_index_used")?),
    );

    Ok(SlowQueryEntry {
        fingerprint: slow_column(row, "fingerprint")?,
        database: slow_column(row, "database_name")?,
        user: None,
        query: slow_column(row, "query_text")?,
        calls: slow_column(row, "calls")?,
        total_time_millis: slow_column(row, "total_time_millis")?,
        mean_time_millis: slow_column(row, "mean_time_millis")?,
        max_time_millis: Some(slow_column(row, "max_time_millis")?),
        rows: slow_column(row, "result_rows")?,
        metadata,
    })
}

fn slow_column<T>(row: &MySqlRow, name: &str) -> Result<T, DriverError>
where
    for<'value> T: Decode<'value, MySql> + Type<MySql>,
{
    row.try_get::<T, _>(name).map_err(|_| {
        DriverError::Internal(format!(
            "MySQL performance schema returned an invalid `{name}` column"
        ))
    })
}

struct TextBatchBuilder {
    schema: SchemaRef,
    columns: Vec<Vec<Option<String>>>,
}

impl TextBatchBuilder {
    fn from_columns(columns: &[MySqlColumn]) -> Self {
        let fields = columns
            .iter()
            .map(|column| {
                let mut metadata = HashMap::new();
                metadata.insert(
                    "dbc.database_type".to_owned(),
                    column.type_info().name().to_owned(),
                );
                Field::new(column.name(), DataType::Utf8, true).with_metadata(metadata)
            })
            .collect::<Vec<_>>();
        Self {
            schema: Arc::new(Schema::new(fields)),
            columns: (0..columns.len()).map(|_| Vec::new()).collect(),
        }
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn push(&mut self, row: &MySqlRow) -> Result<(), DriverError> {
        for (index, values) in self.columns.iter_mut().enumerate() {
            values.push(decode_cell(row, index)?);
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.columns.first().map_or(0, Vec::len)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn finish(mut self) -> Result<RecordBatch, DriverError> {
        let arrays = self
            .columns
            .iter_mut()
            .map(|values| Arc::new(StringArray::from(mem::take(values))) as ArrayRef)
            .collect::<Vec<_>>();
        RecordBatch::try_new(self.schema, arrays)
            .map_err(|_| DriverError::Internal("could not build MySQL result batch".to_owned()))
    }
}

fn decode_cell(row: &MySqlRow, index: usize) -> Result<Option<String>, DriverError> {
    let raw = row.try_get_raw(index).map_err(|_| {
        DriverError::Internal(format!("could not access MySQL column {index}"))
    })?;
    if raw.is_null() {
        return Ok(None);
    }

    let type_name = row.column(index).type_info().name();
    let decoded = match type_name {
        "BOOLEAN" => display_value::<bool>(row, index),
        "TINYINT" => display_value::<i8>(row, index),
        "TINYINT UNSIGNED" => display_value::<u8>(row, index),
        "SMALLINT" => display_value::<i16>(row, index),
        "SMALLINT UNSIGNED" => display_value::<u16>(row, index),
        "MEDIUMINT" | "INT" => display_value::<i32>(row, index),
        "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => display_value::<u32>(row, index),
        "BIGINT" => display_value::<i64>(row, index),
        "BIGINT UNSIGNED" => display_value::<u64>(row, index),
        "FLOAT" => display_value::<f32>(row, index),
        "DOUBLE" => display_value::<f64>(row, index),
        "DECIMAL" => display_value::<BigDecimal>(row, index),
        "DATE" => display_value::<NaiveDate>(row, index),
        "DATETIME" => display_value::<NaiveDateTime>(row, index),
        "TIMESTAMP" => display_value::<DateTime<Utc>>(row, index),
        "TIME" => display_value::<MySqlTime>(row, index),
        "YEAR" => display_value::<u16>(row, index),
        "JSON" => row
            .try_get::<serde_json::Value, _>(index)
            .and_then(|value| serde_json::to_string(&value).map_err(SqlxError::decode)),
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT"
        | "ENUM" | "SET" => display_value::<String>(row, index),
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB"
        | "BIT" | "GEOMETRY" => row
            .try_get::<Vec<u8>, _>(index)
            .map(|value| encode_hex(&value)),
        _ => Ok(format!("<unsupported MySQL type: {type_name}>")),
    }
    .map_err(|_| {
        DriverError::Internal(format!(
            "could not decode MySQL column {index} as {type_name}"
        ))
    })?;
    Ok(Some(decoded))
}

fn display_value<T>(row: &MySqlRow, index: usize) -> Result<String, SqlxError>
where
    for<'value> T: Decode<'value, MySql> + Type<MySql>,
    T: Display,
{
    row.try_get::<T, _>(index).map(|value| value.to_string())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2).saturating_add(2));
    encoded.push_str("0x");
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn map_connect_error(error: SqlxError) -> DriverError {
    match error {
        SqlxError::Database(database_error)
            if matches!(database_error.code().as_deref(), Some("1044" | "1045")) =>
        {
            DriverError::Authentication("MySQL rejected the credentials".to_owned())
        }
        SqlxError::PoolTimedOut => DriverError::Timeout,
        SqlxError::Configuration(_) => {
            DriverError::Connection("MySQL connection settings are invalid".to_owned())
        }
        _ => DriverError::Connection("could not establish the MySQL connection".to_owned()),
    }
}

fn map_query_error(error: SqlxError) -> DriverError {
    match error {
        SqlxError::Database(database_error) => {
            let code = database_error
                .code()
                .map(|code| format!(" [{code}]"))
                .unwrap_or_default();
            DriverError::Query(format!("{}{}", database_error.message(), code))
        }
        SqlxError::PoolTimedOut => DriverError::Timeout,
        SqlxError::PoolClosed => {
            DriverError::Connection("MySQL connection pool is closed".to_owned())
        }
        _ => DriverError::Internal("MySQL query execution failed".to_owned()),
    }
}

fn map_slow_query_error(error: SqlxError) -> DriverError {
    match &error {
        SqlxError::Database(database_error) => match database_error.code().as_deref() {
            Some("1142" | "1227") => DriverError::Permission(
                "reading MySQL performance_schema requires additional privileges".to_owned(),
            ),
            Some("1146") => DriverError::Unsupported(
                "MySQL performance_schema statement digests are not available".to_owned(),
            ),
            _ => map_query_error(error),
        },
        _ => map_query_error(error),
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{decode_cursor, encode_hex, object_kind, slow_query_statement};
    use dbc_core::{
        diagnostics::SlowQueryOrder,
        metadata::DatabaseObjectKind,
    };

    #[test]
    fn binary_values_use_a_stable_hex_representation() {
        assert_eq!(encode_hex(&[0, 15, 16, 255]), "0x000f10ff");
    }

    #[test]
    fn mysql_object_kinds_and_cursors_are_portable() {
        assert_eq!(object_kind("table"), DatabaseObjectKind::Table);
        assert_eq!(decode_cursor(Some("12")).expect("cursor should parse"), 12);
        assert!(decode_cursor(Some("bad")).is_err());
    }

    #[test]
    fn slow_query_order_is_selected_from_a_closed_enum() {
        let statement = slow_query_statement(SlowQueryOrder::Calls);
        assert!(statement.contains("ORDER BY calls DESC"));
        assert!(statement.contains("performance_schema.events_statements_summary_by_digest"));
    }
}
