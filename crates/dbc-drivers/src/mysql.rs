use std::{
    collections::BTreeMap,
    fmt::Display,
    future::Future,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

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
    table_data::{
        StatementParameter, TableApplyResult, TableBrowseRequest, TableChangePlan,
        TableChangeRequest, TableColumn, TableKind, TableMetadata, TablePage, TableRef,
        TableStatementKind, UniqueKey,
    },
};
use dbc_core::result::{CellValue, ColumnSchema, DataBatch, DataSchema, RowBatchBuilder};
use futures_util::TryStreamExt;
use sqlx::{
    AssertSqlSafe, Column, Decode, Either, Error as SqlxError, Executor, MySql, Row,
    SqlSafeStr, Statement, Type, TypeInfo, ValueRef,
    mysql::{
        MySqlArguments, MySqlColumn, MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow,
        types::MySqlTime,
    },
    query::Query,
};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    mysql_descriptor,
    relational::{
        RelationDialect, build_browse_plan, build_change_plan, is_binary_type,
        validate_change_plan,
    },
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const QUERY_BATCH_ROWS: usize = 4_096;
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

const TABLE_KIND_SQL: &str = r#"
SELECT TABLE_TYPE AS relation_kind
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
LIMIT 1
"#;

const TABLE_COLUMNS_SQL: &str = r#"
SELECT
    COLUMN_NAME AS column_name,
    COLUMN_TYPE AS database_type,
    IS_NULLABLE = 'YES' AS nullable,
    ORDINAL_POSITION AS ordinal,
    COLUMN_DEFAULT AS default_expression,
    EXTRA LIKE '%GENERATED%' AS `generated`,
    EXTRA LIKE '%auto_increment%' AS auto_increment
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
"#;

const TABLE_UNIQUE_KEY_COLUMNS_SQL: &str = r#"
SELECT
    INDEX_NAME AS key_name,
    INDEX_NAME = 'PRIMARY' AS primary_key,
    COLUMN_NAME AS column_name,
    SUB_PART AS prefix_length
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = ?
  AND TABLE_NAME = ?
  AND NON_UNIQUE = 0
ORDER BY INDEX_NAME = 'PRIMARY' DESC, INDEX_NAME, SEQ_IN_INDEX
"#;

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
            // Pin the query to one connection so a cancellation knows which
            // session to kill; without this the request only stopped being read
            // client-side and kept running to completion on the server.
            let mut connection = controlled(deadline, &cancellation, pool.acquire())
                .await?
                .map_err(map_query_error)?;
            let connection_id = controlled(
                deadline,
                &cancellation,
                sqlx::query_scalar::<_, u64>("SELECT CONNECTION_ID()")
                    .fetch_one(&mut *connection),
            )
            .await?
            .map_err(map_query_error)?;
            let _cancel_guard =
                NativeCancelGuard::spawn(pool.clone(), cancellation.clone(), connection_id);

            let statement = controlled(
                deadline,
                &cancellation,
                connection.prepare(AssertSqlSafe(text.as_str()).into_sql_str()),
            )
            .await?
            .map_err(map_query_error)?;
            let returns_rows = !statement.columns().is_empty();
            let mut batch =
                RowBatchBuilder::new(column_schema(statement.columns()), QUERY_BATCH_ROWS);
            if returns_rows {
                yield QueryEvent::Schema(DataSchema::Tabular(batch.schema()));
            }
            drop(statement);

            // Editor input reaches this boundary only after DBC policy evaluation.
            let mut results =
                sqlx::raw_sql(AssertSqlSafe(text.as_str())).fetch_many(&mut *connection);
            while returned_rows < usize_to_u64(row_limit) {
                let next = controlled(deadline, &cancellation, results.try_next())
                    .await?
                    .map_err(map_query_error)?;
                let Some(result) = next else {
                    break;
                };
                match result {
                    Either::Right(row) => {
                        batch.push_row(|index| decode_cell(&row, index))?;
                        returned_rows = returned_rows.saturating_add(1);
                        if batch.len() >= QUERY_BATCH_ROWS
                            || returned_rows >= usize_to_u64(row_limit)
                        {
                            yield QueryEvent::Rows(DataBatch::Tabular(batch.take_batch()));
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
                yield QueryEvent::Rows(DataBatch::Tabular(batch.take_batch()));
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

    async fn table_metadata(
        &self,
        table: TableRef,
        cancellation: CancellationToken,
    ) -> Result<TableMetadata, DriverError> {
        if table.qualifiers.len() > 1 {
            return Err(DriverError::Unsupported(
                "MySQL table references may contain one database qualifier".to_owned(),
            ));
        }
        let deadline = operation_deadline(METADATA_TIMEOUT)?;
        let database = if let Some(database) = table.qualifiers.first() {
            database.clone()
        } else {
            controlled(
                deadline,
                &cancellation,
                sqlx::query_scalar::<_, Option<String>>("SELECT DATABASE()")
                    .fetch_one(&self.pool),
            )
            .await?
            .map_err(map_query_error)?
            .ok_or_else(|| {
                DriverError::Connection(
                    "MySQL connection has no current database".to_owned(),
                )
            })?
        };
        let relation_kind = controlled(
            deadline,
            &cancellation,
            sqlx::query(TABLE_KIND_SQL)
                .bind(&database)
                .bind(&table.name)
                .fetch_optional(&self.pool),
        )
        .await?
        .map_err(map_query_error)?
        .ok_or_else(|| DriverError::Unsupported("MySQL relation does not exist".to_owned()))?
        .try_get::<String, _>("relation_kind")
        .map_err(map_query_error)?;
        let kind = if relation_kind.eq_ignore_ascii_case("VIEW") {
            TableKind::View
        } else {
            TableKind::Table
        };
        let column_rows = controlled(
            deadline,
            &cancellation,
            sqlx::query(TABLE_COLUMNS_SQL)
                .bind(&database)
                .bind(&table.name)
                .fetch_all(&self.pool),
        )
        .await?
        .map_err(map_query_error)?;
        let columns = column_rows
            .iter()
            .map(mysql_table_column)
            .collect::<Result<Vec<_>, _>>()?;
        if columns.is_empty() {
            return Err(DriverError::Unsupported(
                "MySQL relation has no visible columns".to_owned(),
            ));
        }
        let unique_keys = if kind == TableKind::Table {
            let rows = controlled(
                deadline,
                &cancellation,
                sqlx::query(TABLE_UNIQUE_KEY_COLUMNS_SQL)
                    .bind(&database)
                    .bind(&table.name)
                    .fetch_all(&self.pool),
            )
            .await?
            .map_err(map_query_error)?;
            mysql_unique_keys(&rows)?
        } else {
            Vec::new()
        };

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
        let browse = build_browse_plan(&metadata, &request, RelationDialect::MySql)?;
        let deadline = operation_deadline(request.timeout)?;
        let count = controlled(
            deadline,
            &cancellation,
            bind_mysql_query(&browse.count_sql, &browse.parameters)?
                .fetch_one(&self.pool),
        )
        .await?
        .map_err(map_query_error)?
        // MySQL reports `COUNT(*)` through the binary protocol as signed
        // BIGINT, so decoding it as `u64` fails the type check outright.
        .try_get::<i64, _>(0)
        .map_err(map_query_error)
        .and_then(|count| {
            u64::try_from(count).map_err(|_| {
                DriverError::Internal("MySQL returned a negative row count".to_owned())
            })
        })?;
        let rows = controlled(
            deadline,
            &cancellation,
            bind_mysql_query(&browse.page_sql, &browse.parameters)?
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
            total_rows: count,
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
        build_change_plan(&metadata, request, RelationDialect::MySql)
    }

    async fn apply_table_changes(
        &self,
        plan: TableChangePlan,
        cancellation: CancellationToken,
    ) -> Result<TableApplyResult, DriverError> {
        validate_change_plan(&plan, RelationDialect::MySql)?;
        let deadline = operation_deadline(plan.timeout)?;
        let request_id = plan.request_id;
        let summary = plan.summary;
        controlled(deadline, &cancellation, async {
            let mut transaction = self.pool.begin().await.map_err(map_query_error)?;
            for statement in &plan.statements {
                let result = bind_mysql_query(&statement.sql, &statement.parameters)?
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

/// Sends `KILL QUERY` when the caller cancels.
///
/// Dropping the guard normally stops the watcher; dropping it *because* of a
/// cancellation lets the watcher finish so the server actually gets told.
struct NativeCancelGuard {
    handle: tokio::task::JoinHandle<()>,
    cancellation: CancellationToken,
}

impl NativeCancelGuard {
    fn spawn(pool: MySqlPool, cancellation: CancellationToken, connection_id: u64) -> Self {
        let watcher = cancellation.clone();
        let handle = tokio::spawn(async move {
            watcher.cancelled().await;
            // `KILL` takes no placeholders; the id is a `u64` this driver read
            // back from the server, so formatting it cannot inject anything.
            let statement = format!("KILL QUERY {connection_id}");
            let _ignored = sqlx::raw_sql(AssertSqlSafe(statement.as_str()))
                .execute(&pool)
                .await;
        });
        Self {
            handle,
            cancellation,
        }
    }
}

impl Drop for NativeCancelGuard {
    fn drop(&mut self) {
        if !self.cancellation.is_cancelled() {
            self.handle.abort();
        }
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

fn mysql_table_column(row: &MySqlRow) -> Result<TableColumn, DriverError> {
    let ordinal = row
        .try_get::<u64, _>("ordinal")
        .map_err(map_query_error)?;
    Ok(TableColumn {
        name: row
            .try_get::<String, _>("column_name")
            .map_err(map_query_error)?,
        database_type: row
            .try_get::<String, _>("database_type")
            .map_err(map_query_error)?,
        nullable: row
            .try_get::<bool, _>("nullable")
            .map_err(map_query_error)?,
        ordinal: u32::try_from(ordinal).map_err(|_| {
            DriverError::Internal("MySQL returned an invalid column ordinal".to_owned())
        })?,
        default_expression: row
            .try_get::<Option<String>, _>("default_expression")
            .map_err(map_query_error)?,
        generated: row
            .try_get::<bool, _>("generated")
            .map_err(map_query_error)?,
        auto_increment: row
            .try_get::<bool, _>("auto_increment")
            .map_err(map_query_error)?,
    })
}

fn mysql_unique_keys(rows: &[MySqlRow]) -> Result<Vec<UniqueKey>, DriverError> {
    let mut keys: BTreeMap<String, (bool, Vec<String>, bool)> = BTreeMap::new();
    for row in rows {
        let name = row
            .try_get::<String, _>("key_name")
            .map_err(map_query_error)?;
        let primary = row
            .try_get::<bool, _>("primary_key")
            .map_err(map_query_error)?;
        let column = row
            .try_get::<Option<String>, _>("column_name")
            .map_err(map_query_error)?;
        let prefix = row
            .try_get::<Option<u64>, _>("prefix_length")
            .map_err(map_query_error)?;
        let key = keys
            .entry(name)
            .or_insert_with(|| (primary, Vec::new(), true));
        if let Some(column) = column {
            key.1.push(column);
        } else {
            key.2 = false;
        }
        if prefix.is_some() {
            key.2 = false;
        }
    }
    Ok(keys
        .into_iter()
        .filter_map(|(name, (primary, columns, usable))| {
            (usable && !columns.is_empty()).then_some(UniqueKey {
                name,
                columns,
                primary,
            })
        })
        .collect())
}

fn bind_mysql_query<'a>(
    sql: &'a str,
    parameters: &'a [StatementParameter],
) -> Result<Query<'a, MySql, MySqlArguments>, DriverError> {
    let mut query = sqlx::query(AssertSqlSafe(sql).into_sql_str());
    for parameter in parameters {
        query = match &parameter.value {
            CellValue::Null
                if is_binary_type(&parameter.database_type, RelationDialect::MySql) =>
            {
                query.bind(Option::<&[u8]>::None)
            }
            CellValue::Null => query.bind(Option::<&str>::None),
            CellValue::Text(value) => query.bind(value.as_str()),
            CellValue::Binary(value) => query.bind(value.as_slice()),
            CellValue::Default => {
                return Err(DriverError::Query(
                    "DEFAULT cannot be bound as a MySQL parameter".to_owned(),
                ));
            }
        };
    }
    Ok(query)
}

/// Project driver column metadata onto the shared result schema.
fn column_schema(columns: &[MySqlColumn]) -> Arc<[ColumnSchema]> {
    columns
        .iter()
        .map(|column| ColumnSchema::new(column.name(), column.type_info().name()))
        .collect()
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

fn decode_table_cell(row: &MySqlRow, index: usize) -> Result<CellValue, DriverError> {
    let raw = row.try_get_raw(index).map_err(|_| {
        DriverError::Internal(format!("could not access MySQL column {index}"))
    })?;
    if raw.is_null() {
        return Ok(CellValue::Null);
    }
    if is_binary_type(
        row.column(index).type_info().name(),
        RelationDialect::MySql,
    ) {
        return row
            .try_get::<Vec<u8>, _>(index)
            .map(CellValue::Binary)
            .map_err(|_| {
                DriverError::Internal(format!(
                    "could not decode MySQL binary column {index}"
                ))
            });
    }
    decode_cell(row, index)?
        .map(CellValue::Text)
        .ok_or_else(|| DriverError::Internal("non-null MySQL cell decoded as NULL".to_owned()))
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
        error => DriverError::Connection(format!("could not establish the MySQL connection: {error}")),
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
        error => DriverError::Internal(format!("MySQL query execution failed: {error}")),
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
