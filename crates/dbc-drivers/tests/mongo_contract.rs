use std::{env, sync::Arc, time::Duration};

use dbc_core::{
    capability::QueryLanguage,
    diagnostics::{ExplainMode, ExplainRequest, SlowQueryOrder, SlowQueryRequest},
    driver::{ConnectionProfile, DatabaseSession, DriverFactory, QueryEvent},
    metadata::{DatabaseObjectKind, ObjectListRequest, ObjectPath},
    query::QueryRequest,
};
use dbc_data::DataBatch;
use dbc_drivers::MongoFactory;
use futures_util::TryStreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires DBC_TEST_MONGODB_URL"]
async fn mongodb_vertical_contract() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(endpoint) = env::var("DBC_TEST_MONGODB_URL") else {
        return Ok(());
    };
    let factory = MongoFactory::new();
    let session = factory
        .connect(
            &ConnectionProfile {
                id: "mongo-contract".to_owned(),
                driver_id: "mongodb".to_owned(),
                display_name: "MongoDB contract".to_owned(),
                endpoint,
                database: Some("dbc_contract".to_owned()),
                user: None,
                secret_id: None,
            },
            None,
        )
        .await?;

    execute_to_end(
        &session,
        r#"{"operation":"runCommand","command":{"drop":"items"}}"#,
    )
    .await
    .ok();
    execute_to_end(
        &session,
        r#"{"operation":"insertOne","collection":"items","document":{"id":1,"name":"alpha"}}"#,
    )
    .await?;
    execute_to_end(
        &session,
        r#"{"operation":"insertOne","collection":"items","document":{"id":2,"name":"beta"}}"#,
    )
    .await?;
    let updated = execute_to_end(
        &session,
        r#"{"operation":"updateMany","collection":"items","filter":{},"update":{"$set":{"active":true}}}"#,
    )
    .await?;
    assert!(updated
        .iter()
        .any(|event| matches!(event, QueryEvent::AffectedRows(2))));

    let found = execute_to_end(
        &session,
        r#"{"operation":"find","collection":"items","filter":{"active":true},"sort":{"id":1},"limit":10}"#,
    )
    .await?;
    let documents = found
        .iter()
        .find_map(|event| match event {
            QueryEvent::Rows(DataBatch::Documents(documents)) => Some(documents),
            _ => None,
        })
        .expect("find should return documents");
    assert_eq!(documents.len(), 2);
    assert_eq!(documents[0]["name"], "alpha");

    let databases = session
        .list_objects(object_request(None), CancellationToken::new())
        .await?;
    assert!(databases.items.iter().any(|item| {
        item.name == "dbc_contract" && item.kind == DatabaseObjectKind::Keyspace
    }));
    let collections = session
        .list_objects(
            object_request(Some(ObjectPath::new(["dbc_contract"]))),
            CancellationToken::new(),
        )
        .await?;
    assert!(collections.items.iter().any(|item| {
        item.name == "items" && item.kind == DatabaseObjectKind::Collection
    }));
    let indexes = session
        .list_objects(
            object_request(Some(ObjectPath::new(["dbc_contract", "items"]))),
            CancellationToken::new(),
        )
        .await?;
    assert!(indexes
        .items
        .iter()
        .any(|item| item.kind == DatabaseObjectKind::Index));

    let plan = session
        .explain(
            ExplainRequest {
                id: Uuid::new_v4(),
                text: r#"{"operation":"find","collection":"items","filter":{"id":1}}"#
                    .to_owned(),
                mode: ExplainMode::Analyze,
                timeout: Duration::from_secs(5),
            },
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(plan.engine, "mongodb");
    assert!(plan.document.get("queryPlanner").is_some());

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
    assert!(slow_queries.source.ends_with(".system.profile"));
    assert!(!slow_queries.entries.is_empty());

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
        QueryLanguage::MongoQuery,
        text,
        Duration::from_secs(5),
        100,
    )
}

async fn execute_to_end(
    session: &Arc<dyn DatabaseSession>,
    text: &str,
) -> Result<Vec<QueryEvent>, dbc_core::error::DriverError> {
    session
        .execute(query_request(text), CancellationToken::new())
        .await?
        .try_collect()
        .await
}
