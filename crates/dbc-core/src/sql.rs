use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::Statement,
    dialect::{
        Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect,
    },
    parser::Parser,
};

/// Security classification used before policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementRisk {
    ReadOnly,
    SafeWrite,
    HighRiskWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Generic,
    PostgreSql,
    MySql,
    SQLite,
}

/// Classify SQL conservatively. Parse failures, multiple statements, and
/// statements not explicitly recognized are treated as high-risk writes.
#[must_use]
pub fn classify_sql(sql: &str) -> StatementRisk {
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return StatementRisk::HighRiskWrite;
    };

    if statements.len() != 1 {
        return StatementRisk::HighRiskWrite;
    }

    match &statements[0] {
        Statement::Query(_) | Statement::ExplainTable { .. } => StatementRisk::ReadOnly,
        Statement::Explain {
            analyze: false,
            statement,
            ..
        } if matches!(statement.as_ref(), Statement::Query(_)) => StatementRisk::ReadOnly,
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
            StatementRisk::SafeWrite
        }
        _ => StatementRisk::HighRiskWrite,
    }
}

/// Return whether input parses as exactly one SQL statement.
#[must_use]
pub fn is_single_statement(sql: &str) -> bool {
    is_single_statement_for(sql, SqlDialect::Generic)
}

/// Return whether input parses as exactly one statement in the selected SQL dialect.
#[must_use]
pub fn is_single_statement_for(sql: &str, dialect: SqlDialect) -> bool {
    let dialect: &dyn Dialect = match dialect {
        SqlDialect::Generic => &GenericDialect {},
        SqlDialect::PostgreSql => &PostgreSqlDialect {},
        SqlDialect::MySql => &MySqlDialect {},
        SqlDialect::SQLite => &SQLiteDialect {},
    };
    Parser::parse_sql(dialect, sql).is_ok_and(|statements| statements.len() == 1)
}
