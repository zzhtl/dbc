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
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use dbc_core::{
    capability::QueryLanguage,
    diagnostics::{ExecutionPlan, ExplainMode, ExplainRequest, PlanFormat},
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
    table_data::{
        StatementParameter, TableApplyResult, TableBrowseRequest, TableChangePlan,
        TableChangeRequest, TableColumn, TableKind, TableMetadata, TablePage, TableRef,
        TableStatementKind, UniqueKey,
    },
};
use dbc_data::{CellValue, DataBatch, DataSchema};
use futures_util::TryStreamExt;
use sqlx::{
    AssertSqlSafe, Column, Decode, Either, Error as SqlxError, Executor, Row, Sqlite,
    SqlSafeStr, Statement, Type, TypeInfo, ValueRef,
    query::Query,
    sqlite::{
        SqliteArguments, SqliteColumn, SqliteConnectOptions, SqlitePool, SqlitePoolOptions,
        SqliteRow,
    },
};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    relational::{
        RelationDialect, build_browse_plan, build_change_plan, is_binary_type,
        validate_change_plan,
    },
    sqlite_descriptor,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const QUERY_BATCH_ROWS: usize = 4_096;
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

const TABLE_KIND_SQL: &str = r#"
SELECT type
FROM sqlite_schema
WHERE name = ? AND type IN ('table', 'view')
LIMIT 1
"#;

const TABLE_COLUMNS_SQL: &str = r#"
SELECT cid, name, type, "notnull", dflt_value, pk, hidden
FROM pragma_table_xinfo(?)
WHERE hidden <> 1
ORDER BY cid
"#;

const TABLE_UNIQUE_INDEXES_SQL: &str = r#"
SELECT name
FROM pragma_index_list(?)
WHERE "unique" = 1 AND partial = 0 AND origin <> 'pk'
ORDER BY name
"#;

const TABLE_INDEX_COLUMNS_SQL: &str = r#"
SELECT name
FROM pragma_index_info(?)
ORDER BY seqno
"#;

const LIST_ROOT_OBJECTS_SQL: &str = r#"
SELECT
    'sqlite:' || type || ':' || name AS object_id,
    name AS object_name,
    type AS object_kind,
    TRUE AS has_children,
    json_object('definition', sql) AS properties
FROM sqlite_schema
WHERE
    type IN ('table', 'view')
    AND (? OR name NOT LIKE 'sqlite_%')
ORDER BY type, name
LIMIT ? OFFSET ?
"#;

const LIST_RELATION_CHILDREN_SQL: &str = r#"
SELECT object_id, object_name, object_kind, has_children, properties
FROM (
    SELECT
        'sqlite:column:' || ? || ':' || cid AS object_id,
        name AS object_name,
        'column' AS object_kind,
        FALSE AS has_children,
        json_object(
            'data_type', type,
            'nullable', "notnull" = 0,
            'ordinal', cid,
            'default', dflt_value,
            'primary_key_position', pk,
            'hidden', hidden
        ) AS properties
    FROM pragma_table_xinfo(?)

    UNION ALL

    SELECT
        'sqlite:index:' || ? || ':' || name AS object_id,
        name AS object_name,
        'index' AS object_kind,
        FALSE AS has_children,
        json_object(
            'unique', "unique" = 1,
            'origin', origin,
            'partial', partial = 1
        ) AS properties
    FROM pragma_index_list(?)

    UNION ALL

    SELECT
        'sqlite:trigger:' || name AS object_id,
        name AS object_name,
        'trigger' AS object_kind,
        FALSE AS has_children,
        json_object('definition', sql) AS properties
    FROM sqlite_schema
    WHERE type = 'trigger' AND tbl_name = ?
)
ORDER BY object_kind, object_name, object_id
LIMIT ? OFFSET ?
"#;

/// Creates embedded SQLite sessions using SQLx's bundled native driver.
#[derive(Debug, Clone)]
pub struct SqliteFactory {
    descriptor: DriverDescriptor,
}

impl SqliteFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            descriptor: sqlite_descriptor(),
        }
    }
}

impl Default for SqliteFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DriverFactory for SqliteFactory {
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
                "connection profile does not target SQLite".to_owned(),
            ));
        }
        if secret.is_some() {
            return Err(DriverError::Unsupported(
                "encrypted SQLite files require a SQLCipher plugin".to_owned(),
            ));
        }
        let endpoint = profile.endpoint.trim();
        if !endpoint.starts_with("sqlite:") {
            return Err(DriverError::Connection(
                "SQLite endpoint must use sqlite:".to_owned(),
            ));
        }

        let options = SqliteConnectOptions::from_str(endpoint)
            .map_err(|_| DriverError::Connection("SQLite endpoint is not valid".to_owned()))?
            .create_if_missing(true);
        let connect = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options);
        let pool = timeout(CONNECT_TIMEOUT, connect)
            .await
            .map_err(|_| DriverError::Timeout)?
            .map_err(map_connect_error)?;
        Ok(Arc::new(SqliteSession { pool }))
    }
}

#[derive(Debug)]
struct SqliteSession {
    pool: SqlitePool,
}

#[async_trait]
impl DatabaseSession for SqliteSession {
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
                "SQLite accepts SQL requests only".to_owned(),
            ));
        }
        if !is_single_statement_for(&request.text, SqlDialect::SQLite) {
            return Err(DriverError::Query(
                "SQLite execution requires exactly one SQL statement".to_owned(),
            ));
        }

        let deadline = operation_deadline(request.timeout)?;
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
                    sqlx::query(LIST_ROOT_OBJECTS_SQL)
                        .bind(request.include_system)
                        .bind(fetch_limit)
                        .bind(offset_i64)
                        .fetch_all(&self.pool),
                )
                .await?
                .map_err(map_query_error)?
            }
            [relation] => {
                controlled(
                    deadline,
                    &cancellation,
                    sqlx::query(LIST_RELATION_CHILDREN_SQL)
                        .bind(relation)
                        .bind(relation)
                        .bind(relation)
                        .bind(relation)
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
                    "SQLite object paths deeper than a table".to_owned(),
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
        if request.mode == ExplainMode::Analyze {
            return Err(DriverError::Unsupported(
                "SQLite does not provide EXPLAIN ANALYZE".to_owned(),
            ));
        }
        if !is_single_statement(&request.text) {
            return Err(DriverError::Query(
                "execution plans require exactly one valid SQL statement".to_owned(),
            ));
        }

        let statement = format!("EXPLAIN QUERY PLAN\n{}", request.text);
        let deadline = operation_deadline(request.timeout)?;
        let rows = controlled(
            deadline,
            &cancellation,
            sqlx::query(AssertSqlSafe(statement)).fetch_all(&self.pool),
        )
        .await?
        .map_err(map_query_error)?;
        let nodes = rows
            .iter()
            .map(|row| {
                Ok(serde_json::json!({
                    "id": plan_column::<i64>(row, 0)?,
                    "parent": plan_column::<i64>(row, 1)?,
                    "detail": plan_column::<String>(row, 3)?,
                }))
            })
            .collect::<Result<Vec<_>, DriverError>>()?;

        Ok(ExecutionPlan {
            engine: "sqlite".to_owned(),
            format: PlanFormat::Json,
            analyzed: false,
            document: serde_json::Value::Array(nodes),
            metadata: BTreeMap::new(),
        })
    }

    async fn table_metadata(
        &self,
        table: TableRef,
        cancellation: CancellationToken,
    ) -> Result<TableMetadata, DriverError> {
        if !table.qualifiers.is_empty()
            && table.qualifiers.as_slice() != ["main"]
        {
            return Err(DriverError::Unsupported(
                "SQLite table editing currently supports the main database only".to_owned(),
            ));
        }
        let deadline = operation_deadline(METADATA_TIMEOUT)?;
        let relation = controlled(
            deadline,
            &cancellation,
            sqlx::query(TABLE_KIND_SQL)
                .bind(&table.name)
                .fetch_optional(&self.pool),
        )
        .await?
        .map_err(map_query_error)?
        .ok_or_else(|| DriverError::Unsupported("SQLite relation does not exist".to_owned()))?;
        let kind = match relation.try_get::<String, _>("type").map_err(map_query_error)? {
            value if value == "table" => TableKind::Table,
            value if value == "view" => TableKind::View,
            _ => {
                return Err(DriverError::Unsupported(
                    "SQLite object is not a table or view".to_owned(),
                ));
            }
        };
        let column_rows = controlled(
            deadline,
            &cancellation,
            sqlx::query(TABLE_COLUMNS_SQL)
                .bind(&table.name)
                .fetch_all(&self.pool),
        )
        .await?
        .map_err(map_query_error)?;
        let mut primary_columns = Vec::new();
        let mut columns = column_rows
            .iter()
            .map(|row| sqlite_table_column(row, &mut primary_columns))
            .collect::<Result<Vec<_>, _>>()?;
        if columns.is_empty() {
            return Err(DriverError::Unsupported(
                "SQLite relation has no visible columns".to_owned(),
            ));
        }
        primary_columns.sort_by_key(|(position, _)| *position);
        let mut unique_keys = Vec::new();
        if !primary_columns.is_empty() {
            unique_keys.push(UniqueKey {
                name: "sqlite_primary_key".to_owned(),
                columns: primary_columns
                    .iter()
                    .map(|(_, name)| name.clone())
                    .collect(),
                primary: true,
            });
        }
        if primary_columns.len() == 1 {
            let primary_name = &primary_columns[0].1;
            if let Some(column) = columns.iter_mut().find(|column| &column.name == primary_name) {
                column.auto_increment = column.database_type.eq_ignore_ascii_case("INTEGER");
            }
        }

        if kind == TableKind::Table {
            let index_rows = controlled(
                deadline,
                &cancellation,
                sqlx::query(TABLE_UNIQUE_INDEXES_SQL)
                    .bind(&table.name)
                    .fetch_all(&self.pool),
            )
            .await?
            .map_err(map_query_error)?;
            for row in index_rows {
                let name = row.try_get::<String, _>("name").map_err(map_query_error)?;
                let index_columns = controlled(
                    deadline,
                    &cancellation,
                    sqlx::query(TABLE_INDEX_COLUMNS_SQL)
                        .bind(&name)
                        .fetch_all(&self.pool),
                )
                .await?
                .map_err(map_query_error)?;
                let names = index_columns
                    .iter()
                    .map(|row| {
                        row.try_get::<Option<String>, _>("name")
                            .map_err(map_query_error)
                    })
                    .collect::<Result<Option<Vec<_>>, _>>()?;
                if let Some(names) = names.filter(|names| !names.is_empty()) {
                    unique_keys.push(UniqueKey {
                        name,
                        columns: names,
                        primary: false,
                    });
                }
            }
        }

        Ok(TableMetadata {
            table,
            kind,
            columns,
            unique_keys,
        })
    }

    async fn browse_table(
        &self,
        request: TableBrowseRequest,
        cancellation: CancellationToken,
    ) -> Result<TablePage, DriverError> {
        let metadata = self
            .table_metadata(request.table.clone(), cancellation.clone())
            .await?;
        let browse = build_browse_plan(&metadata, &request, RelationDialect::SQLite)?;
        let deadline = operation_deadline(request.timeout)?;
        let count = controlled(
            deadline,
            &cancellation,
            bind_sqlite_query(&browse.count_sql, &browse.parameters)?
                .fetch_one(&self.pool),
        )
        .await?
        .map_err(map_query_error)?
        .try_get::<i64, _>(0)
        .map_err(map_query_error)?;
        let rows = controlled(
            deadline,
            &cancellation,
            bind_sqlite_query(&browse.page_sql, &browse.parameters)?
                .fetch_all(&self.pool),
        )
        .await?
        .map_err(map_query_error)?;
        let values = rows
            .iter()
            .map(|row| {
                (0..metadata.columns.len())
                    .map(|index| decode_table_cell(row, index))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TablePage {
            request_id: request.id,
            table: request.table,
            columns: metadata.columns,
            rows: values,
            total_rows: u64::try_from(count).map_err(|_| {
                DriverError::Internal("SQLite returned a negative table row count".to_owned())
            })?,
            page_index: request.page_index,
            page_size: request.page_size,
        })
    }

    async fn plan_table_changes(
        &self,
        request: TableChangeRequest,
        cancellation: CancellationToken,
    ) -> Result<TableChangePlan, DriverError> {
        let metadata = self
            .table_metadata(request.table.clone(), cancellation)
            .await?;
        build_change_plan(&metadata, request, RelationDialect::SQLite)
    }

    async fn apply_table_changes(
        &self,
        plan: TableChangePlan,
        cancellation: CancellationToken,
    ) -> Result<TableApplyResult, DriverError> {
        validate_change_plan(&plan, RelationDialect::SQLite)?;
        let deadline = operation_deadline(plan.timeout)?;
        let request_id = plan.request_id;
        let summary = plan.summary;
        controlled(deadline, &cancellation, async {
            let mut transaction = self.pool.begin().await.map_err(map_query_error)?;
            for statement in &plan.statements {
                let result = bind_sqlite_query(&statement.sql, &statement.parameters)?
                    .execute(&mut *transaction)
                    .await
                    .map_err(map_query_error)?;
                if matches!(
                    statement.kind,
                    TableStatementKind::Update | TableStatementKind::Delete
                ) && result.rows_affected() != 1
                {
                    transaction.rollback().await.map_err(map_query_error)?;
                    return Err(DriverError::Conflict(
                        "an edited row was changed or removed by another transaction".to_owned(),
                    ));
                }
            }
            transaction.commit().await.map_err(map_query_error)
        })
        .await??;
        Ok(TableApplyResult {
            request_id,
            summary,
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
    rows: Vec<SqliteRow>,
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

fn database_object(row: &SqliteRow, parent: &ObjectPath) -> Result<DatabaseObject, DriverError> {
    let id = object_column::<String>(row, "object_id")?;
    let name = object_column::<String>(row, "object_name")?;
    let kind = object_kind(&object_column::<String>(row, "object_kind")?);
    let has_children = object_column::<bool>(row, "has_children")?;
    let properties = object_column::<String>(row, "properties")
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        .map(json_properties)
        .unwrap_or_default();
    Ok(DatabaseObject {
        id,
        path: parent.child(name.clone()),
        name,
        kind,
        has_children,
        properties,
    })
}

fn object_column<T>(row: &SqliteRow, name: &str) -> Result<T, DriverError>
where
    for<'value> T: Decode<'value, Sqlite> + Type<Sqlite>,
{
    row.try_get::<T, _>(name).map_err(|_| {
        DriverError::Internal(format!(
            "SQLite object discovery returned an invalid `{name}` column"
        ))
    })
}

fn plan_column<T>(row: &SqliteRow, index: usize) -> Result<T, DriverError>
where
    for<'value> T: Decode<'value, Sqlite> + Type<Sqlite>,
{
    row.try_get::<T, _>(index)
        .map_err(|_| DriverError::Internal("SQLite returned an invalid query plan".to_owned()))
}

fn json_properties(value: serde_json::Value) -> BTreeMap<String, serde_json::Value> {
    match value {
        serde_json::Value::Object(properties) => properties.into_iter().collect(),
        _ => BTreeMap::new(),
    }
}

fn object_kind(kind: &str) -> DatabaseObjectKind {
    match kind {
        "table" => DatabaseObjectKind::Table,
        "view" => DatabaseObjectKind::View,
        "column" => DatabaseObjectKind::Column,
        "index" => DatabaseObjectKind::Index,
        "trigger" => DatabaseObjectKind::Trigger,
        other => DatabaseObjectKind::Other(other.to_owned()),
    }
}

fn sqlite_table_column(
    row: &SqliteRow,
    primary_columns: &mut Vec<(i64, String)>,
) -> Result<TableColumn, DriverError> {
    let cid = row.try_get::<i64, _>("cid").map_err(map_query_error)?;
    let name = row.try_get::<String, _>("name").map_err(map_query_error)?;
    let database_type = row.try_get::<String, _>("type").map_err(map_query_error)?;
    let primary_position = row.try_get::<i64, _>("pk").map_err(map_query_error)?;
    if primary_position > 0 {
        primary_columns.push((primary_position, name.clone()));
    }
    let ordinal = cid
        .checked_add(1)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| DriverError::Internal("SQLite returned an invalid column id".to_owned()))?;
    Ok(TableColumn {
        name,
        database_type,
        nullable: primary_position == 0
            && row
                .try_get::<i64, _>("notnull")
                .map_err(map_query_error)?
                == 0,
        ordinal,
        default_expression: row
            .try_get::<Option<String>, _>("dflt_value")
            .map_err(map_query_error)?,
        generated: row
            .try_get::<i64, _>("hidden")
            .map_err(map_query_error)?
            != 0,
        auto_increment: false,
    })
}

fn bind_sqlite_query<'a>(
    sql: &'a str,
    parameters: &'a [StatementParameter],
) -> Result<Query<'a, Sqlite, SqliteArguments>, DriverError> {
    let mut query = sqlx::query(AssertSqlSafe(sql).into_sql_str());
    for parameter in parameters {
        query = match &parameter.value {
            CellValue::Null
                if is_binary_type(&parameter.database_type, RelationDialect::SQLite) =>
            {
                query.bind(Option::<&[u8]>::None)
            }
            CellValue::Null => query.bind(Option::<&str>::None),
            CellValue::Text(value) => query.bind(value.as_str()),
            CellValue::Binary(value) => query.bind(value.as_slice()),
            CellValue::Default => {
                return Err(DriverError::Query(
                    "DEFAULT cannot be bound as a SQLite parameter".to_owned(),
                ));
            }
        };
    }
    Ok(query)
}

struct TextBatchBuilder {
    schema: SchemaRef,
    columns: Vec<Vec<Option<String>>>,
}

impl TextBatchBuilder {
    fn from_columns(columns: &[SqliteColumn]) -> Self {
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

    fn push(&mut self, row: &SqliteRow) -> Result<(), DriverError> {
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
            .map_err(|_| DriverError::Internal("could not build SQLite result batch".to_owned()))
    }
}

fn decode_cell(row: &SqliteRow, index: usize) -> Result<Option<String>, DriverError> {
    let raw = row.try_get_raw(index).map_err(|_| {
        DriverError::Internal(format!("could not access SQLite column {index}"))
    })?;
    if raw.is_null() {
        return Ok(None);
    }
    let type_name = row.column(index).type_info().name();
    let decoded = match type_name {
        "INTEGER" => display_value::<i64>(row, index),
        "REAL" => display_value::<f64>(row, index),
        "TEXT" => display_value::<String>(row, index),
        "BLOB" => row
            .try_get::<Vec<u8>, _>(index)
            .map(|value| encode_hex(&value)),
        "BOOLEAN" => display_value::<bool>(row, index),
        "DATE" => display_value::<NaiveDate>(row, index),
        "TIME" => display_value::<NaiveTime>(row, index),
        "DATETIME" => display_value::<NaiveDateTime>(row, index),
        _ => Ok(format!("<unsupported SQLite type: {type_name}>")),
    }
    .map_err(|_| {
        DriverError::Internal(format!(
            "could not decode SQLite column {index} as {type_name}"
        ))
    })?;
    Ok(Some(decoded))
}

fn decode_table_cell(row: &SqliteRow, index: usize) -> Result<CellValue, DriverError> {
    let raw = row.try_get_raw(index).map_err(|_| {
        DriverError::Internal(format!("could not access SQLite column {index}"))
    })?;
    if raw.is_null() {
        return Ok(CellValue::Null);
    }
    if row.column(index).type_info().name() == "BLOB" {
        return row
            .try_get::<Vec<u8>, _>(index)
            .map(CellValue::Binary)
            .map_err(|_| {
                DriverError::Internal(format!(
                    "could not decode SQLite binary column {index}"
                ))
            });
    }
    decode_cell(row, index)?
        .map(CellValue::Text)
        .ok_or_else(|| DriverError::Internal("non-null SQLite cell decoded as NULL".to_owned()))
}

fn display_value<T>(row: &SqliteRow, index: usize) -> Result<String, SqlxError>
where
    for<'value> T: Decode<'value, Sqlite> + Type<Sqlite>,
    T: Display,
{
    row.try_get::<T, _>(index).map(|value| value.to_string())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2).saturating_add(2));
    encoded.push_str("X'");
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded.push('\'');
    encoded
}

fn map_connect_error(error: SqlxError) -> DriverError {
    match error {
        SqlxError::PoolTimedOut => DriverError::Timeout,
        SqlxError::Configuration(_) => {
            DriverError::Connection("SQLite connection settings are invalid".to_owned())
        }
        _ => DriverError::Connection("could not open the SQLite database".to_owned()),
    }
}

fn map_query_error(error: SqlxError) -> DriverError {
    match error {
        SqlxError::Database(database_error) => {
            DriverError::Query(database_error.message().to_owned())
        }
        SqlxError::PoolTimedOut => DriverError::Timeout,
        SqlxError::PoolClosed => {
            DriverError::Connection("SQLite connection pool is closed".to_owned())
        }
        _ => DriverError::Internal("SQLite query execution failed".to_owned()),
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
    use super::{decode_cursor, encode_hex, object_kind};
    use dbc_core::metadata::DatabaseObjectKind;

    #[test]
    fn blobs_use_sqlite_hex_literal_notation() {
        assert_eq!(encode_hex(&[0, 15, 16, 255]), "X'000f10ff'");
    }

    #[test]
    fn sqlite_tree_values_are_portable() {
        assert_eq!(object_kind("view"), DatabaseObjectKind::View);
        assert_eq!(decode_cursor(Some("9")).expect("cursor should parse"), 9);
        assert!(decode_cursor(Some("bad")).is_err());
    }
}
