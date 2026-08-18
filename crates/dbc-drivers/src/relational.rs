use dbc_core::{
    error::DriverError,
    table_data::{
        FilterOperator, PlannedStatement, SortDirection, StatementParameter, TableBrowseRequest,
        TableChangePlan, TableChangeRequest, TableChangeSummary, TableColumn, TableMetadata,
        TableRef, TableStatementKind,
    },
};
use dbc_core::result::CellValue;
use sqlparser::{
    ast::{GroupByExpr, SetExpr, Statement},
    dialect::{Dialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect},
    parser::Parser,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationDialect {
    PostgreSql,
    MySql,
    SQLite,
}

impl RelationDialect {
    fn parser_dialect(self) -> &'static dyn Dialect {
        static POSTGRES: PostgreSqlDialect = PostgreSqlDialect {};
        static MYSQL: MySqlDialect = MySqlDialect {};
        static SQLITE: SQLiteDialect = SQLiteDialect {};

        match self {
            Self::PostgreSql => &POSTGRES,
            Self::MySql => &MYSQL,
            Self::SQLite => &SQLITE,
        }
    }
}

pub(crate) struct BrowsePlan {
    pub count_sql: String,
    pub page_sql: String,
    pub parameters: Vec<StatementParameter>,
}

pub(crate) fn build_browse_plan(
    metadata: &TableMetadata,
    request: &TableBrowseRequest,
    dialect: RelationDialect,
) -> Result<BrowsePlan, DriverError> {
    request
        .validate(metadata)
        .map_err(|error| DriverError::Query(error.to_string()))?;
    if metadata.columns.is_empty() {
        return Err(DriverError::Unsupported(
            "relations without visible columns cannot be browsed".to_owned(),
        ));
    }

    let table = qualified_table(&metadata.table, dialect);
    let columns = metadata
        .columns
        .iter()
        .map(|column| {
            let identifier = quote_identifier(&column.name, dialect);
            if dialect == RelationDialect::PostgreSql
                && !is_binary_type(&column.database_type, dialect)
            {
                format!("{identifier}::text AS {identifier}")
            } else {
                identifier
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut parameters = Vec::new();
    let mut predicates = Vec::new();
    for filter in &request.filters {
        let column = metadata.column(&filter.column).ok_or_else(|| {
            DriverError::Query(format!("unknown table column: {}", filter.column))
        })?;
        predicates.push(filter_predicate(
            column,
            filter.operator,
            &filter.values,
            dialect,
            &mut parameters,
        )?);
    }
    if let Some(raw_where) = &request.raw_where {
        validate_raw_where(raw_where, dialect)?;
        predicates.push(format!("({})", raw_where.trim()));
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };

    let mut order_items = request
        .sort
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|sort| {
            let direction = match sort.direction {
                SortDirection::Ascending => "ASC",
                SortDirection::Descending => "DESC",
            };
            format!("{} {direction}", quote_identifier(&sort.column, dialect))
        })
        .collect::<Vec<_>>();
    if let Some(raw_order_by) = &request.raw_order_by {
        validate_raw_order_by(raw_order_by, dialect)?;
        order_items.push(raw_order_by.trim().to_owned());
    }
    let stable_columns = metadata.stable_key().map_or_else(
        || {
            metadata
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
        },
        |key| key.columns.iter().map(String::as_str).collect(),
    );
    for column in stable_columns {
        let quoted = quote_identifier(column, dialect);
        if !order_items.iter().any(|item| item.starts_with(&quoted)) {
            order_items.push(format!("{quoted} ASC"));
        }
    }
    let order_clause = format!(" ORDER BY {}", order_items.join(", "));
    let offset = request
        .page_index
        .checked_mul(u64::from(request.page_size))
        .ok_or_else(|| DriverError::Query("table page offset is too large".to_owned()))?;

    Ok(BrowsePlan {
        count_sql: format!("SELECT COUNT(*) FROM {table}{where_clause}"),
        page_sql: format!(
            "SELECT {columns} FROM {table}{where_clause}{order_clause} LIMIT {} OFFSET {offset}",
            request.page_size
        ),
        parameters,
    })
}

fn filter_predicate(
    column: &TableColumn,
    operator: FilterOperator,
    values: &[CellValue],
    dialect: RelationDialect,
    parameters: &mut Vec<StatementParameter>,
) -> Result<String, DriverError> {
    let identifier = quote_identifier(&column.name, dialect);
    let mut add_parameter = |value: &CellValue| {
        let placeholder = placeholder(column, value, dialect, parameters.len() + 1);
        parameters.push(StatementParameter {
            database_type: column.database_type.clone(),
            value: value.clone(),
        });
        placeholder
    };
    let predicate = match operator {
        FilterOperator::IsNull => format!("{identifier} IS NULL"),
        FilterOperator::IsNotNull => format!("{identifier} IS NOT NULL"),
        FilterOperator::Equals => {
            null_safe_comparison(&identifier, &add_parameter(&values[0]), false, dialect)
        }
        FilterOperator::NotEquals => {
            null_safe_comparison(&identifier, &add_parameter(&values[0]), true, dialect)
        }
        FilterOperator::GreaterThan => {
            format!("{identifier} > {}", add_parameter(&values[0]))
        }
        FilterOperator::GreaterThanOrEqual => {
            format!("{identifier} >= {}", add_parameter(&values[0]))
        }
        FilterOperator::LessThan => {
            format!("{identifier} < {}", add_parameter(&values[0]))
        }
        FilterOperator::LessThanOrEqual => {
            format!("{identifier} <= {}", add_parameter(&values[0]))
        }
        FilterOperator::Like => {
            format!("{identifier} LIKE {}", add_parameter(&values[0]))
        }
        FilterOperator::NotLike => {
            format!("{identifier} NOT LIKE {}", add_parameter(&values[0]))
        }
        FilterOperator::In | FilterOperator::NotIn => {
            let placeholders = values
                .iter()
                .map(&mut add_parameter)
                .collect::<Vec<_>>()
                .join(", ");
            let not = if operator == FilterOperator::NotIn {
                " NOT"
            } else {
                ""
            };
            format!("{identifier}{not} IN ({placeholders})")
        }
        FilterOperator::Between | FilterOperator::NotBetween => {
            let first = add_parameter(&values[0]);
            let second = add_parameter(&values[1]);
            let not = if operator == FilterOperator::NotBetween {
                " NOT"
            } else {
                ""
            };
            format!("{identifier}{not} BETWEEN {first} AND {second}")
        }
    };
    Ok(predicate)
}

pub(crate) fn build_change_plan(
    metadata: &TableMetadata,
    request: TableChangeRequest,
    dialect: RelationDialect,
) -> Result<TableChangePlan, DriverError> {
    request
        .validate(metadata)
        .map_err(|error| DriverError::Query(error.to_string()))?;
    let mut statements =
        Vec::with_capacity(request.deletes.len() + request.updates.len() + request.inserts.len());
    for delete in &request.deletes {
        statements.push(delete_statement(metadata, delete, dialect)?);
    }
    for update in &request.updates {
        statements.push(update_statement(metadata, update, dialect)?);
    }
    for insert in &request.inserts {
        statements.push(insert_statement(metadata, insert, dialect)?);
    }
    let summary = TableChangeSummary {
        inserted: usize_to_u64(request.inserts.len(), "insert count")?,
        updated: usize_to_u64(request.updates.len(), "update count")?,
        deleted: usize_to_u64(request.deletes.len(), "delete count")?,
    };
    Ok(TableChangePlan {
        request_id: request.id,
        table: request.table,
        statements,
        summary,
        timeout: request.timeout,
    })
}

fn delete_statement(
    metadata: &TableMetadata,
    delete: &dbc_core::table_data::TableDelete,
    dialect: RelationDialect,
) -> Result<PlannedStatement, DriverError> {
    let mut parameters = Vec::new();
    let predicates = optimistic_predicates(
        metadata,
        &delete.identity,
        &delete.original_values,
        dialect,
        &mut parameters,
    )?;
    Ok(PlannedStatement {
        kind: TableStatementKind::Delete,
        sql: format!(
            "DELETE FROM {} WHERE {}",
            qualified_table(&metadata.table, dialect),
            predicates.join(" AND ")
        ),
        parameters,
    })
}

fn update_statement(
    metadata: &TableMetadata,
    update: &dbc_core::table_data::TableUpdate,
    dialect: RelationDialect,
) -> Result<PlannedStatement, DriverError> {
    let mut parameters = Vec::new();
    let assignments = update
        .values
        .iter()
        .map(|(name, value)| {
            let column = required_column(metadata, name)?;
            let expression = placeholder(column, value, dialect, parameters.len() + 1);
            parameters.push(StatementParameter {
                database_type: column.database_type.clone(),
                value: value.clone(),
            });
            Ok(format!(
                "{} = {expression}",
                quote_identifier(name, dialect)
            ))
        })
        .collect::<Result<Vec<_>, DriverError>>()?;
    let predicates = optimistic_predicates(
        metadata,
        &update.identity,
        &update.original_values,
        dialect,
        &mut parameters,
    )?;
    Ok(PlannedStatement {
        kind: TableStatementKind::Update,
        sql: format!(
            "UPDATE {} SET {} WHERE {}",
            qualified_table(&metadata.table, dialect),
            assignments.join(", "),
            predicates.join(" AND ")
        ),
        parameters,
    })
}

fn insert_statement(
    metadata: &TableMetadata,
    insert: &dbc_core::table_data::TableInsert,
    dialect: RelationDialect,
) -> Result<PlannedStatement, DriverError> {
    let table = qualified_table(&metadata.table, dialect);
    if insert.values.is_empty() {
        let sql = match dialect {
            RelationDialect::MySql => format!("INSERT INTO {table} () VALUES ()"),
            RelationDialect::PostgreSql | RelationDialect::SQLite => {
                format!("INSERT INTO {table} DEFAULT VALUES")
            }
        };
        return Ok(PlannedStatement {
            kind: TableStatementKind::Insert,
            sql,
            parameters: Vec::new(),
        });
    }

    let mut parameters = Vec::new();
    let mut columns = Vec::with_capacity(insert.values.len());
    let mut values = Vec::with_capacity(insert.values.len());
    for (name, value) in &insert.values {
        let column = required_column(metadata, name)?;
        columns.push(quote_identifier(name, dialect));
        if matches!(value, CellValue::Default) {
            values.push("DEFAULT".to_owned());
        } else {
            values.push(placeholder(column, value, dialect, parameters.len() + 1));
            parameters.push(StatementParameter {
                database_type: column.database_type.clone(),
                value: value.clone(),
            });
        }
    }

    Ok(PlannedStatement {
        kind: TableStatementKind::Insert,
        sql: format!(
            "INSERT INTO {table} ({}) VALUES ({})",
            columns.join(", "),
            values.join(", ")
        ),
        parameters,
    })
}

fn optimistic_predicates(
    metadata: &TableMetadata,
    identity: &dbc_core::table_data::RowValues,
    originals: &dbc_core::table_data::RowValues,
    dialect: RelationDialect,
    parameters: &mut Vec<StatementParameter>,
) -> Result<Vec<String>, DriverError> {
    let key = metadata
        .stable_key()
        .ok_or_else(|| DriverError::Unsupported("table has no stable row identity".to_owned()))?;
    let mut predicates = Vec::with_capacity(identity.len() + originals.len());
    for name in &key.columns {
        let value = identity
            .get(name)
            .ok_or_else(|| DriverError::Query("row identity is incomplete".to_owned()))?;
        let column = required_column(metadata, name)?;
        let expression = placeholder(column, value, dialect, parameters.len() + 1);
        parameters.push(StatementParameter {
            database_type: column.database_type.clone(),
            value: value.clone(),
        });
        predicates.push(format!(
            "{} = {expression}",
            quote_identifier(name, dialect)
        ));
    }
    for (name, value) in originals {
        if identity.contains_key(name) {
            continue;
        }
        let column = required_column(metadata, name)?;
        let expression = placeholder(column, value, dialect, parameters.len() + 1);
        parameters.push(StatementParameter {
            database_type: column.database_type.clone(),
            value: value.clone(),
        });
        predicates.push(null_safe_comparison(
            &quote_identifier(name, dialect),
            &expression,
            false,
            dialect,
        ));
    }
    Ok(predicates)
}

fn required_column<'a>(
    metadata: &'a TableMetadata,
    name: &str,
) -> Result<&'a TableColumn, DriverError> {
    metadata
        .column(name)
        .ok_or_else(|| DriverError::Query(format!("unknown table column: {name}")))
}

fn placeholder(
    column: &TableColumn,
    value: &CellValue,
    dialect: RelationDialect,
    position: usize,
) -> String {
    match dialect {
        RelationDialect::PostgreSql if is_binary_type(&column.database_type, dialect) => {
            format!("${position}::bytea")
        }
        RelationDialect::PostgreSql => {
            format!("CAST(${position} AS text)::{}", column.database_type)
        }
        RelationDialect::MySql | RelationDialect::SQLite => {
            let _ = value;
            "?".to_owned()
        }
    }
}

fn null_safe_comparison(
    identifier: &str,
    expression: &str,
    negated: bool,
    dialect: RelationDialect,
) -> String {
    match (dialect, negated) {
        (RelationDialect::PostgreSql, false) => {
            format!("{identifier} IS NOT DISTINCT FROM {expression}")
        }
        (RelationDialect::PostgreSql, true) => {
            format!("{identifier} IS DISTINCT FROM {expression}")
        }
        (RelationDialect::MySql, false) => format!("{identifier} <=> {expression}"),
        (RelationDialect::MySql, true) => format!("NOT ({identifier} <=> {expression})"),
        (RelationDialect::SQLite, false) => {
            format!("{identifier} IS NOT DISTINCT FROM {expression}")
        }
        (RelationDialect::SQLite, true) => {
            format!("{identifier} IS DISTINCT FROM {expression}")
        }
    }
}

pub(crate) fn is_binary_type(database_type: &str, dialect: RelationDialect) -> bool {
    let normalized = database_type.to_ascii_lowercase();
    match dialect {
        RelationDialect::PostgreSql => normalized == "bytea",
        RelationDialect::MySql => {
            normalized.contains("binary")
                || normalized.contains("blob")
                || normalized.starts_with("bit")
                || normalized.starts_with("geometry")
        }
        RelationDialect::SQLite => normalized.contains("blob"),
    }
}

fn qualified_table(table: &TableRef, dialect: RelationDialect) -> String {
    table
        .qualifiers
        .iter()
        .chain(std::iter::once(&table.name))
        .map(|part| quote_identifier(part, dialect))
        .collect::<Vec<_>>()
        .join(".")
}

pub(crate) fn quote_identifier(identifier: &str, dialect: RelationDialect) -> String {
    let quote = match dialect {
        RelationDialect::MySql => '`',
        RelationDialect::PostgreSql | RelationDialect::SQLite => '"',
    };
    let escaped = identifier.replace(quote, &format!("{quote}{quote}"));
    format!("{quote}{escaped}{quote}")
}

fn validate_raw_where(fragment: &str, dialect: RelationDialect) -> Result<(), DriverError> {
    let sql = format!("SELECT 1 FROM __dbc_source WHERE {fragment}");
    let query = parse_wrapped_query(&sql, dialect, "WHERE")?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(invalid_raw_fragment("WHERE"));
    };
    if select.selection.is_none()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || has_grouping(&select.group_by)
        || select.having.is_some()
        || select.qualify.is_some()
    {
        return Err(invalid_raw_fragment("WHERE"));
    }
    Ok(())
}

fn validate_raw_order_by(fragment: &str, dialect: RelationDialect) -> Result<(), DriverError> {
    let sql = format!("SELECT 1 FROM __dbc_source ORDER BY {fragment}");
    let query = parse_wrapped_query(&sql, dialect, "ORDER BY")?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(invalid_raw_fragment("ORDER BY"));
    };
    if query.order_by.is_none()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || select.selection.is_some()
        || has_grouping(&select.group_by)
        || select.having.is_some()
        || select.qualify.is_some()
    {
        return Err(invalid_raw_fragment("ORDER BY"));
    }
    Ok(())
}

fn parse_wrapped_query(
    sql: &str,
    dialect: RelationDialect,
    fragment_name: &str,
) -> Result<Box<sqlparser::ast::Query>, DriverError> {
    let mut statements = Parser::parse_sql(dialect.parser_dialect(), sql)
        .map_err(|_| invalid_raw_fragment(fragment_name))?;
    if statements.len() != 1 {
        return Err(invalid_raw_fragment(fragment_name));
    }
    let statement = statements
        .pop()
        .ok_or_else(|| invalid_raw_fragment(fragment_name))?;
    let Statement::Query(query) = statement else {
        return Err(invalid_raw_fragment(fragment_name));
    };
    Ok(query)
}

fn has_grouping(group_by: &GroupByExpr) -> bool {
    match group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(expressions, modifiers) => {
            !expressions.is_empty() || !modifiers.is_empty()
        }
    }
}

fn invalid_raw_fragment(name: &str) -> DriverError {
    DriverError::Query(format!(
        "raw {name} must contain exactly one valid SQL fragment"
    ))
}

pub(crate) fn validate_change_plan(
    plan: &TableChangePlan,
    dialect: RelationDialect,
) -> Result<(), DriverError> {
    for statement in &plan.statements {
        let parsed =
            Parser::parse_sql(dialect.parser_dialect(), &statement.sql).map_err(|error| {
                DriverError::Query(format!(
                    "planned table change SQL is invalid: {error}; SQL: {}",
                    statement.sql
                ))
            })?;
        let valid = matches!(
            (statement.kind, parsed.as_slice()),
            (TableStatementKind::Insert, [Statement::Insert(_)])
                | (TableStatementKind::Update, [Statement::Update(_)])
                | (TableStatementKind::Delete, [Statement::Delete(_)])
        );
        if !valid {
            return Err(DriverError::Query(
                "planned table change does not match its statement kind".to_owned(),
            ));
        }
        if statement
            .parameters
            .iter()
            .any(|parameter| matches!(parameter.value, CellValue::Default))
        {
            return Err(DriverError::Query(
                "DEFAULT cannot be bound as a table-change parameter".to_owned(),
            ));
        }
    }
    Ok(())
}

fn usize_to_u64(value: usize, field: &str) -> Result<u64, DriverError> {
    u64::try_from(value).map_err(|_| DriverError::Query(format!("{field} is too large")))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use dbc_core::table_data::{
        FilterOperator, TableBrowseRequest, TableChangeRequest, TableColumn, TableFilter,
        TableInsert, TableKind, TableMetadata, TableRef, TableUpdate, UniqueKey,
    };
    use dbc_core::result::CellValue;
    use uuid::Uuid;

    use super::build_browse_plan;
    use super::{RelationDialect, build_change_plan};

    fn metadata() -> TableMetadata {
        TableMetadata {
            table: TableRef::new(["public"], "users"),
            kind: TableKind::Table,
            columns: vec![
                TableColumn {
                    name: "id".to_owned(),
                    database_type: "int8".to_owned(),
                    nullable: false,
                    ordinal: 1,
                    default_expression: None,
                    generated: false,
                    auto_increment: true,
                },
                TableColumn {
                    name: "name".to_owned(),
                    database_type: "text".to_owned(),
                    nullable: false,
                    ordinal: 2,
                    default_expression: None,
                    generated: false,
                    auto_increment: false,
                },
            ],
            unique_keys: vec![UniqueKey {
                name: "users_pkey".to_owned(),
                columns: vec!["id".to_owned()],
                primary: true,
            }],
        }
    }

    #[test]
    fn postgres_change_preview_is_parameterized_and_ordered() {
        let request = TableChangeRequest {
            id: Uuid::new_v4(),
            table: metadata().table,
            inserts: vec![TableInsert {
                values: BTreeMap::from([(
                    "name".to_owned(),
                    CellValue::Text("new user".to_owned()),
                )]),
            }],
            updates: vec![TableUpdate {
                identity: BTreeMap::from([("id".to_owned(), CellValue::Text("7".to_owned()))]),
                original_values: BTreeMap::from([(
                    "name".to_owned(),
                    CellValue::Text("old".to_owned()),
                )]),
                values: BTreeMap::from([(
                    "name".to_owned(),
                    CellValue::Text("changed".to_owned()),
                )]),
            }],
            deletes: Vec::new(),
            timeout: Duration::from_secs(5),
        };
        let plan = build_change_plan(&metadata(), request, RelationDialect::PostgreSql)
            .expect("change plan should build");

        assert_eq!(plan.statements.len(), 2);
        assert!(plan.statements[0].sql.contains("$1"));
        assert!(plan.statements[0].sql.contains("$2"));
        assert!(plan.statements[0].sql.contains("$3"));
        assert!(!plan.statements[0].sql.contains("changed"));
        assert_eq!(
            plan.statements[0].parameters[0].value,
            CellValue::Text("changed".to_owned())
        );
    }

    #[test]
    fn browse_filters_are_bound_and_raw_fragments_cannot_add_statements() {
        let request = TableBrowseRequest {
            id: Uuid::new_v4(),
            table: metadata().table,
            filters: vec![TableFilter {
                column: "name".to_owned(),
                operator: FilterOperator::Equals,
                values: vec![CellValue::Text("alice".to_owned())],
            }],
            sort: None,
            raw_where: None,
            raw_order_by: None,
            page_index: 2,
            page_size: 50,
            timeout: Duration::from_secs(5),
        };
        let plan = build_browse_plan(&metadata(), &request, RelationDialect::MySql)
            .expect("browse plan should build");
        assert!(plan.count_sql.contains("<=> ?"));
        assert!(plan.page_sql.contains("LIMIT 50 OFFSET 100"));
        assert!(!plan.page_sql.contains("alice"));
        assert_eq!(
            plan.parameters[0].value,
            CellValue::Text("alice".to_owned())
        );

        let mut injected = request;
        injected.raw_where = Some("1 = 1; DELETE FROM users".to_owned());
        assert!(build_browse_plan(&metadata(), &injected, RelationDialect::MySql).is_err());

        injected.raw_where = Some("name LIKE 'a%'".to_owned());
        injected.raw_order_by = Some("id DESC".to_owned());
        let raw = build_browse_plan(&metadata(), &injected, RelationDialect::MySql)
            .expect("valid raw fragments should be accepted");
        assert!(raw.page_sql.contains("(name LIKE 'a%')"));
        assert!(raw.page_sql.contains("ORDER BY id DESC"));
    }
}
