use std::{collections::BTreeSet, time::Duration};

use dbc_core::{
    capability::{
        Capability, CapabilitySet, CrudCapabilities, ExplainCapabilities, QueryLanguage,
        SlowQueryCapabilities,
    },
    driver::SecretValue,
    query::{QueryRequest, QueryValidationError},
    sql::{
        SqlDialect, StatementRisk, classify_sql, is_single_statement,
        is_single_statement_for,
    },
};
use uuid::Uuid;

#[test]
fn capability_set_round_trips_without_losing_driver_features() {
    let capabilities = CapabilitySet::builder()
        .query_language(QueryLanguage::Sql)
        .enable(Capability::Crud(CrudCapabilities {
            create: true,
            update: true,
            delete: true,
            transactional: true,
        }))
        .enable(Capability::Explain(ExplainCapabilities {
            estimated: true,
            analyzed: true,
        }))
        .enable(Capability::SlowQueries(SlowQueryCapabilities {
            available: true,
            configurable: true,
        }))
        .build();

    let json = serde_json::to_string(&capabilities).expect("capabilities should serialize");
    let decoded: CapabilitySet =
        serde_json::from_str(&json).expect("capabilities should deserialize");

    assert_eq!(decoded, capabilities);
    assert_eq!(
        decoded.query_languages(),
        &BTreeSet::from([QueryLanguage::Sql])
    );
    assert!(decoded.supports_crud());
    assert!(decoded.supports_explain());
    assert!(decoded.supports_slow_queries());
}

#[test]
fn sql_classification_is_fail_closed() {
    assert_eq!(
        classify_sql("select * from users"),
        StatementRisk::ReadOnly
    );
    assert_eq!(
        classify_sql("update users set active = true where id = 1"),
        StatementRisk::SafeWrite
    );
    assert_eq!(
        classify_sql("drop table users"),
        StatementRisk::HighRiskWrite
    );
    assert_eq!(
        classify_sql("vacuum mystery dialect option"),
        StatementRisk::HighRiskWrite
    );
    assert_eq!(
        classify_sql("select 1; delete from users"),
        StatementRisk::HighRiskWrite
    );
    assert!(is_single_statement("select 1"));
    assert!(!is_single_statement("select 1; delete from users"));
    assert!(!is_single_statement("not valid sql !"));
}

#[test]
fn sql_statement_count_uses_database_dialects() {
    assert!(is_single_statement_for(
        "PRAGMA table_info('items')",
        SqlDialect::SQLite
    ));
    assert!(is_single_statement_for(
        "SHOW TABLES",
        SqlDialect::MySql
    ));
    assert!(!is_single_statement_for(
        "SELECT 1; SELECT 2",
        SqlDialect::PostgreSql
    ));
}

#[test]
fn query_request_rejects_empty_text_and_invalid_limits() {
    let empty = QueryRequest::new(
        Uuid::new_v4(),
        QueryLanguage::Sql,
        "  ",
        Duration::from_secs(30),
        100,
    );
    assert_eq!(empty.validate(), Err(QueryValidationError::EmptyQuery));

    let zero_timeout = QueryRequest::new(
        Uuid::new_v4(),
        QueryLanguage::Sql,
        "select 1",
        Duration::ZERO,
        100,
    );
    assert_eq!(
        zero_timeout.validate(),
        Err(QueryValidationError::ZeroTimeout)
    );

    let zero_rows = QueryRequest::new(
        Uuid::new_v4(),
        QueryLanguage::Sql,
        "select 1",
        Duration::from_secs(30),
        0,
    );
    assert_eq!(
        zero_rows.validate(),
        Err(QueryValidationError::ZeroRowLimit)
    );
}

#[test]
fn resolved_secrets_are_redacted_from_debug_output() {
    let secret = SecretValue::new("super-secret-password");

    assert_eq!(secret.expose(), "super-secret-password");
    assert_eq!(format!("{secret:?}"), "SecretValue([REDACTED])");
}
