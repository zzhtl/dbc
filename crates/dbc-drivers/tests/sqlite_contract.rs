use std::{sync::Arc, time::Duration};

use arrow_array::StringArray;
use dbc_core::{
    capability::QueryLanguage,
    diagnostics::{ExplainMode, ExplainRequest},
    driver::{ConnectionProfile, DatabaseSession, DriverFactory, QueryEvent},
    error::DriverError,
    metadata::{DatabaseObjectKind, ObjectListRequest, ObjectPath},
    query::QueryRequest,
};
use dbc_data::{DataBatch, DataSchema};
use dbc_drivers::SqliteFactory;
use futures_util::TryStreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn sqlite_vertical_contract() -> Result<(), Box<dyn std::error::Error>> {
    let factory = SqliteFactory::new();
    let session = factory
        .connect(
            &ConnectionProfile {
                id: "sqlite-contract".to_owned(),
                driver_id: "sqlite".to_owned(),
                display_name: "SQLite contract".to_owned(),
                endpoint: "sqlite::memory:".to_owned(),
                database: None,
                user: None,
                secret_id: None,
            },
            None,
        )
        .await?;

    execute_to_end(
        &session,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, payload BLOB)",
    )
    .await?;
    let inserted = execute_to_end(
        &session,
        "INSERT INTO items (name, payload) VALUES ('alpha', X'00ff'), ('beta', NULL)",
    )
    .await?;
    assert!(inserted
        .iter()
        .any(|event| matches!(event, QueryEvent::AffectedRows(2))));

    let selected = execute_to_end(
        &session,
        "SELECT id, name, payload FROM items ORDER BY id",
    )
    .await?;
    let batch = selected
        .iter()
        .find_map(|event| match event {
            QueryEvent::Rows(DataBatch::Tabular(batch)) => Some(batch),
            _ => None,
        })
        .expect("SELECT should return rows");
    assert_eq!(batch.num_rows(), 2);
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("SQLite text should use Arrow UTF-8");
    assert_eq!(names.value(0), "alpha");

    let empty = execute_to_end(
        &session,
        "SELECT id, name FROM items WHERE 0",
    )
    .await?;
    assert!(empty.iter().any(|event| matches!(
        event,
        QueryEvent::Schema(DataSchema::Tabular(schema)) if schema.fields().len() == 2
    )));
    assert!(!empty
        .iter()
        .any(|event| matches!(event, QueryEvent::Rows(_))));

    let pragma = execute_to_end(&session, "PRAGMA table_info('items')").await?;
    assert!(pragma
        .iter()
        .any(|event| matches!(event, QueryEvent::Rows(DataBatch::Tabular(_)))));

    let returning = execute_to_end(
        &session,
        "INSERT INTO items (name, payload) VALUES ('gamma', NULL) RETURNING name",
    )
    .await?;
    assert!(returning
        .iter()
        .any(|event| matches!(event, QueryEvent::Rows(DataBatch::Tabular(_)))));

    let Err(multiple) = session
        .execute(
            query_request("SELECT 1; SELECT 2"),
            CancellationToken::new(),
        )
        .await
    else {
        panic!("SQLite should reject multiple statements");
    };
    assert!(matches!(multiple, DriverError::Query(_)));

    let root = session
        .list_objects(object_request(None), CancellationToken::new())
        .await?;
    assert!(root
        .items
        .iter()
        .any(|item| item.name == "items" && item.kind == DatabaseObjectKind::Table));

    let children = session
        .list_objects(
            object_request(Some(ObjectPath::new(["items"]))),
            CancellationToken::new(),
        )
        .await?;
    assert!(children
        .items
        .iter()
        .any(|item| item.name == "name" && item.kind == DatabaseObjectKind::Column));

    let plan = session
        .explain(
            ExplainRequest {
                id: Uuid::new_v4(),
                text: "SELECT * FROM items WHERE id = 1".to_owned(),
                mode: ExplainMode::Estimated,
                timeout: Duration::from_secs(2),
            },
            CancellationToken::new(),
        )
        .await?;
    assert!(plan.document.is_array());

    let analyzed = session
        .explain(
            ExplainRequest {
                id: Uuid::new_v4(),
                text: "SELECT 1".to_owned(),
                mode: ExplainMode::Analyze,
                timeout: Duration::from_secs(2),
            },
            CancellationToken::new(),
        )
        .await
        .expect_err("SQLite should expose estimated plans only");
    assert!(matches!(analyzed, DriverError::Unsupported(_)));

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
        timeout: Duration::from_secs(2),
    }
}

fn query_request(text: &str) -> QueryRequest {
    QueryRequest::new(
        Uuid::new_v4(),
        QueryLanguage::Sql,
        text,
        Duration::from_secs(2),
        100,
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
