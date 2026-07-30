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
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
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
use dbc_data::{CellValue, DataBatch, DataSchema};
use futures_util::TryStreamExt;
use sqlx::{
    AssertSqlSafe, Column, Decode, Either, Error as SqlxError, Executor, Postgres, Row,
    SqlSafeStr, Statement, Type, TypeInfo, ValueRef,
    postgres::{
        PgArguments, PgColumn, PgConnectOptions, PgPool, PgPoolOptions, PgRow, PgValueFormat,
        types::{Oid, PgInterval},
    },
    query::Query,
};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    postgres_descriptor,
    relational::{
        RelationDialect, build_browse_plan, build_change_plan, is_binary_type,
        validate_change_plan,
    },
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const QUERY_BATCH_ROWS: usize = 4_096;
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

const TABLE_KIND_SQL: &str = r#"
SELECT c.relkind::text AS relation_kind
FROM pg_catalog.pg_class AS c
INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = $1
  AND c.relname = $2
  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
LIMIT 1
"#;

const TABLE_COLUMNS_SQL: &str = r#"
SELECT
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS database_type,
    NOT a.attnotnull AS nullable,
    a.attnum::integer AS ordinal,
    pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expression,
    a.attgenerated <> '' AS generated,
    (
        a.attidentity <> ''
        OR COALESCE(
            pg_catalog.pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%',
            false
        )
    ) AS auto_increment
FROM pg_catalog.pg_attribute AS a
INNER JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_attrdef AS d
    ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = $1
  AND c.relname = $2
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
"#;

const TABLE_UNIQUE_KEYS_SQL: &str = r#"
SELECT
    index_class.relname AS key_name,
    index_info.indisprimary AS primary_key,
    array_agg(attribute.attname ORDER BY key_column.ordinality) AS columns
FROM pg_catalog.pg_index AS index_info
INNER JOIN pg_catalog.pg_class AS relation
    ON relation.oid = index_info.indrelid
INNER JOIN pg_catalog.pg_namespace AS namespace
    ON namespace.oid = relation.relnamespace
INNER JOIN pg_catalog.pg_class AS index_class
    ON index_class.oid = index_info.indexrelid
INNER JOIN unnest(index_info.indkey::smallint[])
    WITH ORDINALITY AS key_column(attnum, ordinality)
    ON true
INNER JOIN pg_catalog.pg_attribute AS attribute
    ON attribute.attrelid = relation.oid
    AND attribute.attnum = key_column.attnum
WHERE namespace.nspname = $1
  AND relation.relname = $2
  AND index_info.indisunique
  AND index_info.indisvalid
  AND index_info.indisready
  AND index_info.indpred IS NULL
  AND index_info.indexprs IS NULL
  AND key_column.ordinality <= index_info.indnkeyatts
GROUP BY index_class.relname, index_info.indisprimary
ORDER BY index_info.indisprimary DESC, index_class.relname
"#;

const LIST_SCHEMAS_SQL: &str = r#"
SELECT
    'pg:schema:' || n.oid::text AS object_id,
    n.nspname AS object_name,
    'schema'::text AS object_kind,
    true AS has_children,
    jsonb_build_object(
        'system',
        n.nspname = 'information_schema' OR n.nspname LIKE 'pg\_%' ESCAPE '\'
    ) AS properties
FROM pg_catalog.pg_namespace AS n
WHERE
    $1::boolean
    OR (
        n.nspname <> 'information_schema'
        AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
    )
ORDER BY n.nspname
LIMIT $2 OFFSET $3
"#;

const LIST_SCHEMA_OBJECTS_SQL: &str = r#"
WITH objects AS (
    SELECT
        'pg:relation:' || c.oid::text AS object_id,
        c.relname AS object_name,
        CASE c.relkind
            WHEN 'r' THEN 'table'
            WHEN 'p' THEN 'partitioned_table'
            WHEN 'v' THEN 'view'
            WHEN 'm' THEN 'materialized_view'
            WHEN 'f' THEN 'foreign_table'
            WHEN 'S' THEN 'sequence'
            ELSE 'other'
        END AS object_kind,
        c.relkind <> 'S' AS has_children,
        jsonb_build_object(
            'estimated_rows', c.reltuples::bigint,
            'total_bytes', pg_catalog.pg_total_relation_size(c.oid)
        ) AS properties
    FROM pg_catalog.pg_class AS c
    INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = $1
      AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')

    UNION ALL

    SELECT
        'pg:routine:' || p.oid::text AS object_id,
        p.proname || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')'
            AS object_name,
        'routine'::text AS object_kind,
        false AS has_children,
        jsonb_build_object(
            'result_type', pg_catalog.pg_get_function_result(p.oid),
            'routine_kind', p.prokind::text
        ) AS properties
    FROM pg_catalog.pg_proc AS p
    INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
    WHERE n.nspname = $1
)
SELECT object_id, object_name, object_kind, has_children, properties
FROM objects
ORDER BY object_kind, object_name, object_id
LIMIT $2 OFFSET $3
"#;

const LIST_RELATION_CHILDREN_SQL: &str = r#"
WITH relation AS (
    SELECT c.oid
    FROM pg_catalog.pg_class AS c
    INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = $2
    LIMIT 1
),
objects AS (
    SELECT
        'pg:column:' || r.oid::text || ':' || a.attnum::text AS object_id,
        a.attname AS object_name,
        'column'::text AS object_kind,
        false AS has_children,
        jsonb_build_object(
            'data_type', pg_catalog.format_type(a.atttypid, a.atttypmod),
            'nullable', NOT a.attnotnull,
            'ordinal', a.attnum,
            'default', pg_catalog.pg_get_expr(d.adbin, d.adrelid)
        ) AS properties
    FROM relation AS r
    INNER JOIN pg_catalog.pg_attribute AS a ON a.attrelid = r.oid
    LEFT JOIN pg_catalog.pg_attrdef AS d
        ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE a.attnum > 0 AND NOT a.attisdropped

    UNION ALL

    SELECT
        'pg:index:' || i.indexrelid::text AS object_id,
        ci.relname AS object_name,
        'index'::text AS object_kind,
        false AS has_children,
        jsonb_build_object(
            'unique', i.indisunique,
            'primary', i.indisprimary,
            'valid', i.indisvalid,
            'definition', pg_catalog.pg_get_indexdef(i.indexrelid)
        ) AS properties
    FROM relation AS r
    INNER JOIN pg_catalog.pg_index AS i ON i.indrelid = r.oid
    INNER JOIN pg_catalog.pg_class AS ci ON ci.oid = i.indexrelid

    UNION ALL

    SELECT
        'pg:constraint:' || c.oid::text AS object_id,
        c.conname AS object_name,
        'constraint'::text AS object_kind,
        false AS has_children,
        jsonb_build_object(
            'constraint_type', c.contype::text,
            'definition', pg_catalog.pg_get_constraintdef(c.oid, true)
        ) AS properties
    FROM relation AS r
    INNER JOIN pg_catalog.pg_constraint AS c ON c.conrelid = r.oid

    UNION ALL

    SELECT
        'pg:trigger:' || t.oid::text AS object_id,
        t.tgname AS object_name,
        'trigger'::text AS object_kind,
        false AS has_children,
        jsonb_build_object(
            'enabled', t.tgenabled::text,
            'definition', pg_catalog.pg_get_triggerdef(t.oid, true)
        ) AS properties
    FROM relation AS r
    INNER JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = r.oid
    WHERE NOT t.tgisinternal
)
SELECT object_id, object_name, object_kind, has_children, properties
FROM objects
ORDER BY object_kind, object_name, object_id
LIMIT $3 OFFSET $4
"#;

const PG_STAT_STATEMENTS_SCHEMA_SQL: &str = r#"
SELECT n.nspname
FROM pg_catalog.pg_extension AS e
INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = e.extnamespace
WHERE e.extname = 'pg_stat_statements'
"#;

/// Creates native PostgreSQL sessions backed by a bounded SQLx pool.
#[derive(Debug, Clone)]
pub struct PostgresFactory {
    descriptor: DriverDescriptor,
}

impl PostgresFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            descriptor: postgres_descriptor(),
        }
    }
}

impl Default for PostgresFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DriverFactory for PostgresFactory {
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
                "connection profile does not target PostgreSQL".to_owned(),
            ));
        }

        let endpoint = profile.endpoint.trim();
        if !endpoint.starts_with("postgres://") && !endpoint.starts_with("postgresql://") {
            return Err(DriverError::Connection(
                "PostgreSQL endpoint must use postgres:// or postgresql://".to_owned(),
            ));
        }

        let mut options = PgConnectOptions::from_str(endpoint).map_err(|_| {
            DriverError::Connection("PostgreSQL endpoint is not a valid URL".to_owned())
        })?;
        if let Some(user) = profile.user.as_deref() {
            options = options.username(user);
        }
        if let Some(database) = profile.database.as_deref() {
            options = options.database(database);
        }
        if let Some(secret) = secret {
            options = options.password(secret.expose());
        }

        let connect = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options);
        let pool = timeout(CONNECT_TIMEOUT, connect)
            .await
            .map_err(|_| DriverError::Timeout)?
            .map_err(map_connect_error)?;

        Ok(Arc::new(PostgresSession { pool }))
    }
}

#[derive(Debug)]
struct PostgresSession {
    pool: PgPool,
}

#[async_trait]
impl DatabaseSession for PostgresSession {
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
                "PostgreSQL accepts SQL requests only".to_owned(),
            ));
        }
        if !is_single_statement_for(&request.text, SqlDialect::PostgreSql) {
            return Err(DriverError::Query(
                "PostgreSQL execution requires exactly one SQL statement".to_owned(),
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
                        .bind(fetch_limit)
                        .bind(offset_i64)
                        .fetch_all(&self.pool),
                )
                .await?
                .map_err(map_query_error)?
            }
            _ => {
                return Err(DriverError::Unsupported(
                    "PostgreSQL object paths deeper than a relation".to_owned(),
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
        let prefix = if analyzed {
            "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON)"
        } else {
            "EXPLAIN (COSTS, VERBOSE, SETTINGS, FORMAT JSON)"
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
        let document = row.try_get::<serde_json::Value, _>(0).map_err(|_| {
            DriverError::Internal("PostgreSQL returned an invalid JSON execution plan".to_owned())
        })?;

        Ok(ExecutionPlan {
            engine: "postgresql".to_owned(),
            format: PlanFormat::Json,
            analyzed,
            metadata: execution_plan_metadata(&document),
            document,
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
        let extension_row = controlled(
            deadline,
            &cancellation,
            sqlx::query(PG_STAT_STATEMENTS_SCHEMA_SQL).fetch_optional(&self.pool),
        )
        .await?
        .map_err(map_slow_query_error)?;
        let Some(extension_row) = extension_row else {
            return Err(DriverError::Unsupported(
                "pg_stat_statements is not installed in this database".to_owned(),
            ));
        };
        let extension_schema = extension_row.try_get::<String, _>(0).map_err(|_| {
            DriverError::Internal(
                "could not read the pg_stat_statements extension schema".to_owned(),
            )
        })?;
        let statement = slow_query_statement(&extension_schema, request.order);
        let minimum_mean_time = request.minimum_mean_time_millis.unwrap_or(0.0);
        let limit = usize_to_i64(request.limit, "slow-query page limit")?;
        let rows = controlled(
            deadline,
            &cancellation,
            sqlx::query(AssertSqlSafe(statement))
                .bind(minimum_mean_time)
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
            source: "pg_stat_statements".to_owned(),
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
                "PostgreSQL table references may contain one schema qualifier".to_owned(),
            ));
        }
        let deadline = operation_deadline(METADATA_TIMEOUT)?;
        let schema = if let Some(schema) = table.qualifiers.first() {
            schema.clone()
        } else {
            controlled(
                deadline,
                &cancellation,
                sqlx::query_scalar::<_, String>("SELECT current_schema()")
                    .fetch_one(&self.pool),
            )
            .await?
            .map_err(map_query_error)?
        };
        let relation_kind = controlled(
            deadline,
            &cancellation,
            sqlx::query(TABLE_KIND_SQL)
                .bind(&schema)
                .bind(&table.name)
                .fetch_optional(&self.pool),
        )
        .await?
        .map_err(map_query_error)?
        .ok_or_else(|| DriverError::Unsupported("PostgreSQL relation does not exist".to_owned()))?
        .try_get::<String, _>("relation_kind")
        .map_err(map_query_error)?;
        let kind = match relation_kind.as_str() {
            "r" | "p" | "f" => TableKind::Table,
            "v" | "m" => TableKind::View,
            _ => {
                return Err(DriverError::Unsupported(
                    "PostgreSQL object is not a table or view".to_owned(),
                ));
            }
        };
        let column_rows = controlled(
            deadline,
            &cancellation,
            sqlx::query(TABLE_COLUMNS_SQL)
                .bind(&schema)
                .bind(&table.name)
                .fetch_all(&self.pool),
        )
        .await?
        .map_err(map_query_error)?;
        let columns = column_rows
            .iter()
            .map(postgres_table_column)
            .collect::<Result<Vec<_>, _>>()?;
        if columns.is_empty() {
            return Err(DriverError::Unsupported(
                "PostgreSQL relation has no visible columns".to_owned(),
            ));
        }
        let unique_keys = if kind == TableKind::Table {
            controlled(
                deadline,
                &cancellation,
                sqlx::query(TABLE_UNIQUE_KEYS_SQL)
                    .bind(&schema)
                    .bind(&table.name)
                    .fetch_all(&self.pool),
            )
            .await?
            .map_err(map_query_error)?
            .iter()
            .map(postgres_unique_key)
            .collect::<Result<Vec<_>, _>>()?
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
        let browse = build_browse_plan(&metadata, &request, RelationDialect::PostgreSql)?;
        let deadline = operation_deadline(request.timeout)?;
        let count = controlled(
            deadline,
            &cancellation,
            bind_postgres_query(&browse.count_sql, &browse.parameters)?
                .fetch_one(&self.pool),
        )
        .await?
        .map_err(map_query_error)?
        .try_get::<i64, _>(0)
        .map_err(map_query_error)?;
        let rows = controlled(
            deadline,
            &cancellation,
            bind_postgres_query(&browse.page_sql, &browse.parameters)?
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
                DriverError::Internal(
                    "PostgreSQL returned a negative table row count".to_owned(),
                )
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
        build_change_plan(&metadata, request, RelationDialect::PostgreSql)
    }

    async fn apply_table_changes(
        &self,
        plan: TableChangePlan,
        cancellation: CancellationToken,
    ) -> Result<TableApplyResult, DriverError> {
        validate_change_plan(&plan, RelationDialect::PostgreSql)?;
        let deadline = operation_deadline(plan.timeout)?;
        let request_id = plan.request_id;
        let summary = plan.summary;
        controlled(deadline, &cancellation, async {
            let mut transaction = self.pool.begin().await.map_err(map_query_error)?;
            for statement in &plan.statements {
                let result = bind_postgres_query(&statement.sql, &statement.parameters)?
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
    rows: Vec<PgRow>,
    parent: &ObjectPath,
    limit: usize,
    offset: usize,
) -> Result<ObjectPage, DriverError> {
    let has_next_page = rows.len() > limit;
    let mut items = rows
        .iter()
        .take(limit)
        .map(|row| database_object(row, parent))
        .collect::<Result<Vec<_>, _>>()?;
    items.shrink_to_fit();

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

fn database_object(row: &PgRow, parent: &ObjectPath) -> Result<DatabaseObject, DriverError> {
    let id = object_column::<String>(row, "object_id")?;
    let name = object_column::<String>(row, "object_name")?;
    let kind = object_kind(&object_column::<String>(row, "object_kind")?);
    let has_children = object_column::<bool>(row, "has_children")?;
    let properties = object_column::<serde_json::Value>(row, "properties")?;

    Ok(DatabaseObject {
        id,
        path: parent.child(name.clone()),
        name,
        kind,
        has_children,
        properties: json_properties(properties),
    })
}

fn object_column<T>(row: &PgRow, name: &str) -> Result<T, DriverError>
where
    for<'value> T: Decode<'value, Postgres> + Type<Postgres>,
{
    row.try_get::<T, _>(name).map_err(|_| {
        DriverError::Internal(format!(
            "PostgreSQL object discovery returned an invalid `{name}` column"
        ))
    })
}

fn object_kind(kind: &str) -> DatabaseObjectKind {
    match kind {
        "schema" => DatabaseObjectKind::Schema,
        "table" => DatabaseObjectKind::Table,
        "partitioned_table" => DatabaseObjectKind::PartitionedTable,
        "view" => DatabaseObjectKind::View,
        "materialized_view" => DatabaseObjectKind::MaterializedView,
        "foreign_table" => DatabaseObjectKind::ForeignTable,
        "sequence" => DatabaseObjectKind::Sequence,
        "column" => DatabaseObjectKind::Column,
        "index" => DatabaseObjectKind::Index,
        "constraint" => DatabaseObjectKind::Constraint,
        "trigger" => DatabaseObjectKind::Trigger,
        "routine" => DatabaseObjectKind::Routine,
        other => DatabaseObjectKind::Other(other.to_owned()),
    }
}

fn json_properties(value: serde_json::Value) -> BTreeMap<String, serde_json::Value> {
    match value {
        serde_json::Value::Object(properties) => properties.into_iter().collect(),
        _ => BTreeMap::new(),
    }
}

fn execution_plan_metadata(
    document: &serde_json::Value,
) -> BTreeMap<String, serde_json::Value> {
    let Some(summary) = document
        .as_array()
        .and_then(|plans| plans.first())
        .and_then(serde_json::Value::as_object)
    else {
        return BTreeMap::new();
    };
    ["Planning Time", "Execution Time"]
        .into_iter()
        .filter_map(|key| {
            summary
                .get(key)
                .cloned()
                .map(|value| (key.to_owned(), value))
        })
        .collect()
}

fn slow_query_statement(extension_schema: &str, order: SlowQueryOrder) -> String {
    let order_by = match order {
        SlowQueryOrder::MeanTime => {
            "mean_time_millis DESC NULLS LAST, total_time_millis DESC NULLS LAST"
        }
        SlowQueryOrder::TotalTime => {
            "total_time_millis DESC NULLS LAST, mean_time_millis DESC NULLS LAST"
        }
        SlowQueryOrder::Calls => {
            "calls DESC NULLS LAST, total_time_millis DESC NULLS LAST"
        }
    };
    let relation = format!(
        "{}.{}",
        quote_identifier(extension_schema),
        quote_identifier("pg_stat_statements")
    );

    format!(
        r#"
WITH statements AS (
    SELECT
        s.queryid::text AS fingerprint,
        d.datname AS database_name,
        r.rolname AS user_name,
        s.query,
        COALESCE((to_jsonb(s) ->> 'calls')::bigint, 0) AS calls,
        COALESCE(
            (to_jsonb(s) ->> 'total_exec_time')::double precision,
            (to_jsonb(s) ->> 'total_time')::double precision,
            0
        ) AS total_time_millis,
        COALESCE(
            (to_jsonb(s) ->> 'mean_exec_time')::double precision,
            (to_jsonb(s) ->> 'mean_time')::double precision,
            0
        ) AS mean_time_millis,
        COALESCE(
            (to_jsonb(s) ->> 'max_exec_time')::double precision,
            (to_jsonb(s) ->> 'max_time')::double precision
        ) AS max_time_millis,
        COALESCE((to_jsonb(s) ->> 'rows')::bigint, 0) AS result_rows,
        COALESCE((to_jsonb(s) ->> 'shared_blks_hit')::bigint, 0) AS shared_blks_hit,
        COALESCE((to_jsonb(s) ->> 'shared_blks_read')::bigint, 0) AS shared_blks_read,
        COALESCE((to_jsonb(s) ->> 'temp_blks_written')::bigint, 0) AS temp_blks_written
    FROM {relation} AS s
    LEFT JOIN pg_catalog.pg_database AS d ON d.oid = s.dbid
    LEFT JOIN pg_catalog.pg_roles AS r ON r.oid = s.userid
)
SELECT
    fingerprint,
    database_name,
    user_name,
    query,
    calls,
    total_time_millis,
    mean_time_millis,
    max_time_millis,
    result_rows,
    shared_blks_hit,
    shared_blks_read,
    temp_blks_written
FROM statements
WHERE mean_time_millis >= $1
ORDER BY {order_by}
LIMIT $2
"#
    )
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn slow_query_entry(row: &PgRow) -> Result<SlowQueryEntry, DriverError> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "shared_blks_hit".to_owned(),
        serde_json::Value::from(nonnegative_i64(slow_column(row, "shared_blks_hit")?)),
    );
    metadata.insert(
        "shared_blks_read".to_owned(),
        serde_json::Value::from(nonnegative_i64(slow_column(row, "shared_blks_read")?)),
    );
    metadata.insert(
        "temp_blks_written".to_owned(),
        serde_json::Value::from(nonnegative_i64(slow_column(row, "temp_blks_written")?)),
    );

    Ok(SlowQueryEntry {
        fingerprint: slow_column(row, "fingerprint")?,
        database: slow_column(row, "database_name")?,
        user: slow_column(row, "user_name")?,
        query: slow_column(row, "query")?,
        calls: nonnegative_i64(slow_column(row, "calls")?),
        total_time_millis: slow_column(row, "total_time_millis")?,
        mean_time_millis: slow_column(row, "mean_time_millis")?,
        max_time_millis: slow_column(row, "max_time_millis")?,
        rows: nonnegative_i64(slow_column(row, "result_rows")?),
        metadata,
    })
}

fn slow_column<T>(row: &PgRow, name: &str) -> Result<T, DriverError>
where
    for<'value> T: Decode<'value, Postgres> + Type<Postgres>,
{
    row.try_get::<T, _>(name).map_err(|_| {
        DriverError::Internal(format!(
            "pg_stat_statements returned an invalid `{name}` column"
        ))
    })
}

fn nonnegative_i64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

fn postgres_table_column(row: &PgRow) -> Result<TableColumn, DriverError> {
    let ordinal = row
        .try_get::<i32, _>("ordinal")
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
            DriverError::Internal("PostgreSQL returned an invalid column ordinal".to_owned())
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

fn postgres_unique_key(row: &PgRow) -> Result<UniqueKey, DriverError> {
    Ok(UniqueKey {
        name: row
            .try_get::<String, _>("key_name")
            .map_err(map_query_error)?,
        columns: row
            .try_get::<Vec<String>, _>("columns")
            .map_err(map_query_error)?,
        primary: row
            .try_get::<bool, _>("primary_key")
            .map_err(map_query_error)?,
    })
}

fn bind_postgres_query<'a>(
    sql: &'a str,
    parameters: &'a [StatementParameter],
) -> Result<Query<'a, Postgres, PgArguments>, DriverError> {
    let mut query = sqlx::query(AssertSqlSafe(sql).into_sql_str());
    for parameter in parameters {
        query = match &parameter.value {
            CellValue::Null
                if is_binary_type(&parameter.database_type, RelationDialect::PostgreSql) =>
            {
                query.bind(Option::<&[u8]>::None)
            }
            CellValue::Null => query.bind(Option::<&str>::None),
            CellValue::Text(value) => query.bind(value.as_str()),
            CellValue::Binary(value) => query.bind(value.as_slice()),
            CellValue::Default => {
                return Err(DriverError::Query(
                    "DEFAULT cannot be bound as a PostgreSQL parameter".to_owned(),
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
    fn from_columns(columns: &[PgColumn]) -> Self {
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

    fn push(&mut self, row: &PgRow) -> Result<(), DriverError> {
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
            .map(|values| {
                Arc::new(StringArray::from(mem::take(values))) as ArrayRef
            })
            .collect::<Vec<_>>();
        RecordBatch::try_new(self.schema, arrays).map_err(|_| {
            DriverError::Internal("could not build PostgreSQL result batch".to_owned())
        })
    }
}

fn decode_cell(row: &PgRow, index: usize) -> Result<Option<String>, DriverError> {
    let raw = row.try_get_raw(index).map_err(|_| {
        DriverError::Internal(format!("could not access PostgreSQL column {index}"))
    })?;
    if raw.is_null() {
        return Ok(None);
    }
    if raw.format() == PgValueFormat::Text {
        return raw.as_str().map(|value| Some(value.to_owned())).map_err(|_| {
            DriverError::Internal(format!("PostgreSQL column {index} is not valid UTF-8"))
        });
    }

    let type_name = row.column(index).type_info().name();
    let decoded = match type_name {
        "BOOL" => display_value::<bool>(row, index),
        "\"CHAR\"" => display_value::<i8>(row, index),
        "INT2" => display_value::<i16>(row, index),
        "INT4" => display_value::<i32>(row, index),
        "INT8" => display_value::<i64>(row, index),
        "FLOAT4" => display_value::<f32>(row, index),
        "FLOAT8" => display_value::<f64>(row, index),
        "NUMERIC" => display_value::<BigDecimal>(row, index),
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "UNKNOWN" | "CITEXT" => {
            display_value::<String>(row, index)
        }
        "DATE" => display_value::<NaiveDate>(row, index),
        "TIME" => display_value::<NaiveTime>(row, index),
        "TIMESTAMP" => display_value::<NaiveDateTime>(row, index),
        "TIMESTAMPTZ" => display_value::<DateTime<Utc>>(row, index),
        "UUID" => display_value::<Uuid>(row, index),
        "JSON" | "JSONB" => row
            .try_get::<serde_json::Value, _>(index)
            .and_then(|value| serde_json::to_string(&value).map_err(SqlxError::decode)),
        "BYTEA" => row
            .try_get::<Vec<u8>, _>(index)
            .map(|value| encode_hex(&value)),
        "OID" => row
            .try_get::<Oid, _>(index)
            .map(|value| value.0.to_string()),
        "INTERVAL" => row
            .try_get::<PgInterval, _>(index)
            .map(format_interval),
        _ => Ok(format!("<unsupported PostgreSQL type: {type_name}>")),
    }
    .map_err(|_| {
        DriverError::Internal(format!(
            "could not decode PostgreSQL column {index} as {type_name}"
        ))
    })?;

    Ok(Some(decoded))
}

fn decode_table_cell(row: &PgRow, index: usize) -> Result<CellValue, DriverError> {
    let raw = row.try_get_raw(index).map_err(|_| {
        DriverError::Internal(format!("could not access PostgreSQL column {index}"))
    })?;
    if raw.is_null() {
        return Ok(CellValue::Null);
    }
    if row.column(index).type_info().name() == "BYTEA" {
        return row
            .try_get::<Vec<u8>, _>(index)
            .map(CellValue::Binary)
            .map_err(|_| {
                DriverError::Internal(format!(
                    "could not decode PostgreSQL binary column {index}"
                ))
            });
    }
    decode_cell(row, index)?
        .map(CellValue::Text)
        .ok_or_else(|| DriverError::Internal("non-null PostgreSQL cell decoded as NULL".to_owned()))
}

fn display_value<T>(row: &PgRow, index: usize) -> Result<String, SqlxError>
where
    for<'value> T: Decode<'value, Postgres> + Type<Postgres>,
    T: Display,
{
    row.try_get::<T, _>(index).map(|value| value.to_string())
}

fn format_interval(value: PgInterval) -> String {
    format!(
        "{} months {} days {} microseconds",
        value.months, value.days, value.microseconds
    )
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2).saturating_add(2));
    encoded.push_str("\\x");
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn map_connect_error(error: SqlxError) -> DriverError {
    match error {
        SqlxError::Database(database_error)
            if matches!(database_error.code().as_deref(), Some("28P01" | "28000")) =>
        {
            DriverError::Authentication("PostgreSQL rejected the credentials".to_owned())
        }
        SqlxError::PoolTimedOut => DriverError::Timeout,
        SqlxError::Configuration(_) => {
            DriverError::Connection("PostgreSQL connection settings are invalid".to_owned())
        }
        _ => DriverError::Connection("could not establish the PostgreSQL connection".to_owned()),
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
            DriverError::Connection("PostgreSQL connection pool is closed".to_owned())
        }
        _ => DriverError::Internal("PostgreSQL query execution failed".to_owned()),
    }
}

fn map_slow_query_error(error: SqlxError) -> DriverError {
    match &error {
        SqlxError::Database(database_error) => match database_error.code().as_deref() {
            Some("42501") => DriverError::Permission(
                "reading pg_stat_statements requires PostgreSQL statistics privileges".to_owned(),
            ),
            Some("42P01" | "42704") => DriverError::Unsupported(
                "pg_stat_statements is not available in this database".to_owned(),
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
    use super::{
        decode_cursor, encode_hex, format_interval, object_kind, quote_identifier,
        slow_query_statement,
    };
    use dbc_core::{
        diagnostics::SlowQueryOrder,
        metadata::DatabaseObjectKind,
    };
    use sqlx::postgres::types::PgInterval;

    #[test]
    fn bytea_values_use_postgres_hex_format() {
        assert_eq!(encode_hex(&[0, 15, 16, 255]), "\\x000f10ff");
    }

    #[test]
    fn interval_values_remain_lossless() {
        let interval = PgInterval {
            months: 2,
            days: -3,
            microseconds: 45,
        };

        assert_eq!(
            format_interval(interval),
            "2 months -3 days 45 microseconds"
        );
    }

    #[test]
    fn object_cursors_are_numeric_and_bounded_by_the_request_validator() {
        assert_eq!(decode_cursor(None).expect("empty cursor should start at zero"), 0);
        assert_eq!(
            decode_cursor(Some("25")).expect("numeric cursor should parse"),
            25
        );
        assert!(decode_cursor(Some("next")).is_err());
    }

    #[test]
    fn postgres_object_kinds_have_portable_equivalents() {
        assert_eq!(object_kind("table"), DatabaseObjectKind::Table);
        assert_eq!(
            object_kind("extension_kind"),
            DatabaseObjectKind::Other("extension_kind".to_owned())
        );
    }

    #[test]
    fn extension_schema_is_quoted_before_building_statistics_sql() {
        assert_eq!(quote_identifier("odd\"schema"), "\"odd\"\"schema\"");
        let statement = slow_query_statement("odd\"schema", SlowQueryOrder::Calls);
        assert!(statement.contains("\"odd\"\"schema\".\"pg_stat_statements\""));
        assert!(statement.contains("ORDER BY calls DESC"));
    }
}
