use std::time::Duration;

use dbc_core::{
    query_editability::{QueryEditabilityReason, analyze_query_source, resolve_query_editability},
    sql::SqlDialect,
    table_data::{
        FilterOperator, TableBrowseRequest, TableColumn, TableFilter, TableKind, TableMetadata,
        TableRef, UniqueKey,
    },
};
use dbc_data::CellValue;
use uuid::Uuid;

fn users_metadata() -> TableMetadata {
    TableMetadata {
        table: TableRef::new(["public"], "users"),
        kind: TableKind::Table,
        columns: vec![
            TableColumn {
                name: "id".to_owned(),
                database_type: "INT8".to_owned(),
                nullable: false,
                ordinal: 1,
                default_expression: None,
                generated: false,
                auto_increment: true,
            },
            TableColumn {
                name: "email".to_owned(),
                database_type: "TEXT".to_owned(),
                nullable: false,
                ordinal: 2,
                default_expression: None,
                generated: false,
                auto_increment: false,
            },
            TableColumn {
                name: "nickname".to_owned(),
                database_type: "TEXT".to_owned(),
                nullable: true,
                ordinal: 3,
                default_expression: None,
                generated: false,
                auto_increment: false,
            },
        ],
        unique_keys: vec![
            UniqueKey {
                name: "users_email_key".to_owned(),
                columns: vec!["email".to_owned()],
                primary: false,
            },
            UniqueKey {
                name: "users_pkey".to_owned(),
                columns: vec!["id".to_owned()],
                primary: true,
            },
        ],
    }
}

#[test]
fn stable_row_identity_prefers_primary_and_rejects_nullable_unique_keys() {
    let metadata = users_metadata();

    let key = metadata
        .stable_key()
        .expect("primary key should provide stable identity");
    assert_eq!(key.name, "users_pkey");

    let mut without_primary = metadata.clone();
    without_primary.unique_keys.remove(1);
    assert_eq!(
        without_primary
            .stable_key()
            .expect("non-null unique key should be usable")
            .name,
        "users_email_key"
    );

    without_primary.unique_keys[0].columns = vec!["nickname".to_owned()];
    assert_eq!(without_primary.stable_key(), None);
}

#[test]
fn browse_request_validates_filter_arity_and_known_columns() {
    let metadata = users_metadata();
    let mut request = TableBrowseRequest {
        id: Uuid::new_v4(),
        table: metadata.table.clone(),
        filters: vec![TableFilter {
            column: "email".to_owned(),
            operator: FilterOperator::Equals,
            values: vec![CellValue::Text("alice@example.test".to_owned())],
        }],
        sort: None,
        raw_where: None,
        raw_order_by: None,
        page_index: 0,
        page_size: 200,
        timeout: Duration::from_secs(30),
    };

    assert_eq!(request.validate(&metadata), Ok(()));

    request.filters[0].values.clear();
    assert!(request.validate(&metadata).is_err());

    request.filters[0].values = vec![CellValue::Text("alice@example.test".to_owned())];
    request.filters[0].column = "missing".to_owned();
    assert!(request.validate(&metadata).is_err());
}

#[test]
fn single_table_projection_is_editable_only_when_the_key_is_returned() {
    let analysis = analyze_query_source(
        "SELECT u.id, u.email AS address, upper(u.email) AS normalized FROM public.users AS u",
        SqlDialect::PostgreSql,
    );
    assert_eq!(analysis.reason, None);

    let editable = resolve_query_editability(
        &analysis,
        &users_metadata(),
        &[
            "id".to_owned(),
            "address".to_owned(),
            "normalized".to_owned(),
        ],
    );
    assert!(editable.editable);
    assert_eq!(editable.columns[0].source_column.as_deref(), Some("id"));
    assert_eq!(editable.columns[1].source_column.as_deref(), Some("email"));
    assert!(editable.columns[0].editable);
    assert!(editable.columns[1].editable);
    assert!(!editable.columns[2].editable);
    assert!(!editable.allow_insert);
    assert!(editable.allow_delete);

    let without_key = resolve_query_editability(
        &analysis,
        &users_metadata(),
        &["address".to_owned(), "normalized".to_owned()],
    );
    assert!(!without_key.editable);
    assert_eq!(
        without_key.reason,
        Some(QueryEditabilityReason::PrimaryKeyNotReturned)
    );
}

#[test]
fn joins_and_ctes_have_explicit_read_only_reasons() {
    let joined = analyze_query_source(
        "SELECT u.id, r.name FROM users u JOIN roles r ON r.id = u.role_id",
        SqlDialect::PostgreSql,
    );
    assert_eq!(joined.reason, Some(QueryEditabilityReason::MultipleSources));

    let cte = analyze_query_source(
        "WITH active AS (SELECT * FROM users) SELECT * FROM active",
        SqlDialect::PostgreSql,
    );
    assert_eq!(cte.reason, Some(QueryEditabilityReason::Cte));
}

#[test]
fn aggregates_distinct_and_wildcard_have_conservative_editability() {
    let aggregate = analyze_query_source("SELECT count(*) FROM users", SqlDialect::PostgreSql);
    assert_eq!(aggregate.reason, Some(QueryEditabilityReason::Aggregation));

    let distinct = analyze_query_source("SELECT DISTINCT id FROM users", SqlDialect::PostgreSql);
    assert_eq!(distinct.reason, Some(QueryEditabilityReason::Distinct));

    let wildcard = analyze_query_source("SELECT * FROM public.users", SqlDialect::PostgreSql);
    let editable = resolve_query_editability(
        &wildcard,
        &users_metadata(),
        &["id".to_owned(), "email".to_owned(), "nickname".to_owned()],
    );
    assert!(editable.editable);
    assert!(editable.allow_insert);
    assert!(editable.allow_delete);
}
