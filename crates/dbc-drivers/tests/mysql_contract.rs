use std::{env, sync::Arc, time::Duration};

use arrow_array::StringArray;
use dbc_core::{
    capability::QueryLanguage,
    diagnostics::{ExplainMode, ExplainRequest, SlowQueryOrder, SlowQueryRequest},
    driver::{
        ConnectionProfile, DatabaseSession, DriverFactory, QueryEvent, SecretValue,
    },
    error::DriverError,
    metadata::{DatabaseObjectKind, ObjectListRequest, ObjectPath},
    query::QueryRequest,
};
use dbc_data::DataBatch;
use dbc_drivers::MySqlFactory;
use futures_util::TryStreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires DBC_TEST_MYSQL_URL and DBC_TEST_MYSQL_PASSWORD"]
async fn mysql_vertical_contract() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(endpoint) = env::var("DBC_TEST_MYSQL_URL") else {
        return Ok(());
    };
    let Ok(password) = env::var("DBC_TEST_MYSQL_PASSWORD") else {
        return Ok(());
    };
    let user = env::var("DBC_TEST_MYSQL_USER").unwrap_or_else(|_| "root".to_owned());
    let database =
        env::var("DBC_TEST_MYSQL_DATABASE").unwrap_or_else(|_| "dbc_contract".to_owned());
    let profile = ConnectionProfile {
        id: "mysql-contract".to_owned(),
        driver_id: "mysql".to_owned(),
        display_name: "MySQL contract".to_owned(),
        endpoint,
        database: Some(database.clone()),
        user: Some(user),
        secret_id: Some("test:mysql-contract".to_owned()),
    };
    let secret = SecretValue::new(password);
    let factory = MySqlFactory::new();
    let session = factory.connect(&profile, Some(&secret)).await?;

    execute_to_end(
        &session,
        r#"
CREATE TABLE IF NOT EXISTS dbc_contract_items (
    id INTEGER PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"#,
    )
    .await?;
    execute_to_end(&session, "TRUNCATE TABLE dbc_contract_items").await?;
    let insert_events = execute_to_end(
        &session,
        r#"
INSERT INTO dbc_contract_items (id, name, payload)
VALUES
    (1, 'alpha', JSON_OBJECT('enabled', TRUE)),
    (2, 'beta', JSON_OBJECT('enabled', FALSE))
"#,
    )
    .await?;
    assert!(
        insert_events
            .iter()
            .any(|event| matches!(event, QueryEvent::AffectedRows(2)))
    );

    let select_events = execute_to_end(
        &session,
        "SELECT id, name, payload, created_at FROM dbc_contract_items ORDER BY id",
    )
    .await?;
    let batch = select_events
        .iter()
        .find_map(|event| match event {
            QueryEvent::Rows(DataBatch::Tabular(batch)) => Some(batch),
            _ => None,
        })
        .expect("SELECT should return a tabular batch");
    assert_eq!(batch.num_rows(), 2);
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("MySQL text columns should use Arrow UTF-8");
    assert_eq!(names.value(0), "alpha");
    assert_eq!(names.value(1), "beta");

    let root = session
        .list_objects(object_request(None), CancellationToken::new())
        .await?;
    assert!(root.items.iter().any(|item| {
        item.name == database && item.kind == DatabaseObjectKind::Schema
    }));

    let schema = session
        .list_objects(
            object_request(Some(ObjectPath::new([database.as_str()]))),
            CancellationToken::new(),
        )
        .await?;
    assert!(schema.items.iter().any(|item| {
        item.name == "dbc_contract_items" && item.kind == DatabaseObjectKind::Table
    }));

    let relation = session
        .list_objects(
            object_request(Some(ObjectPath::new([
                database.as_str(),
                "dbc_contract_items",
            ]))),
            CancellationToken::new(),
        )
        .await?;
    assert!(relation.items.iter().any(|item| {
        item.name == "id" && item.kind == DatabaseObjectKind::Column
    }));
    assert!(
        relation
            .items
            .iter()
            .any(|item| item.kind == DatabaseObjectKind::Index)
    );

    let plan = session
        .explain(
            ExplainRequest {
                id: Uuid::new_v4(),
                text: "SELECT * FROM dbc_contract_items WHERE id = 1".to_owned(),
                mode: ExplainMode::Estimated,
                timeout: Duration::from_secs(5),
            },
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(plan.engine, "mysql");
    assert!(plan.document.is_object());

    let analyzed_write = session
        .explain(
            ExplainRequest {
                id: Uuid::new_v4(),
                text: "DELETE FROM dbc_contract_items".to_owned(),
                mode: ExplainMode::Analyze,
                timeout: Duration::from_secs(5),
            },
            CancellationToken::new(),
        )
        .await
        .expect_err("EXPLAIN ANALYZE must not execute writes");
    assert!(matches!(analyzed_write, DriverError::Permission(_)));

    let slow_queries = session
        .slow_queries(
            SlowQueryRequest {
                id: Uuid::new_v4(),
                limit: 20,
                minimum_mean_time_millis: Some(0.0),
                order: SlowQueryOrder::TotalTime,
                timeout: Duration::from_secs(5),
            },
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        slow_queries.source,
        "performance_schema.events_statements_summary_by_digest"
    );

    let cancellation = CancellationToken::new();
    let mut cancelled_stream = session
        .execute(query_request("SELECT SLEEP(10)"), cancellation.clone())
        .await?;
    cancellation.cancel();
    let cancelled = cancelled_stream
        .try_next()
        .await
        .expect_err("cancelled query should terminate with a typed error");
    assert!(matches!(cancelled, DriverError::Cancelled));

    session.close().await?;
    Ok(())
}

fn object_request(parent: Option<ObjectPath>) -> ObjectListRequest {
    ObjectListRequest {
        id: Uuid::new_v4(),
        parent,
        include_system: false,
        limit: 100,
        cursor: None,
        timeout: Duration::from_secs(5),
    }
}

fn query_request(text: &str) -> QueryRequest {
    QueryRequest::new(
        Uuid::new_v4(),
        QueryLanguage::Sql,
        text,
        Duration::from_secs(5),
        1_000,
    )
}

async fn execute_to_end(
    session: &Arc<dyn DatabaseSession>,
    text: &str,
) -> Result<Vec<QueryEvent>, DriverError> {
    session
        .execute(query_request(text), CancellationToken::new())
        .await?
        .try_collect()
        .await
}
