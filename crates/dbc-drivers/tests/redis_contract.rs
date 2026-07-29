use std::{env, sync::Arc, time::Duration};

use dbc_core::{
    capability::QueryLanguage,
    diagnostics::{SlowQueryOrder, SlowQueryRequest},
    driver::{ConnectionProfile, DatabaseSession, DriverFactory, QueryEvent},
    error::DriverError,
    metadata::{DatabaseObjectKind, ObjectListRequest},
    query::QueryRequest,
};
use dbc_data::DataBatch;
use dbc_drivers::RedisFactory;
use futures_util::TryStreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires DBC_TEST_REDIS_URL"]
async fn redis_vertical_contract() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(endpoint) = env::var("DBC_TEST_REDIS_URL") else {
        return Ok(());
    };
    let factory = RedisFactory::new();
    let session = factory
        .connect(
            &ConnectionProfile {
                id: "redis-contract".to_owned(),
                driver_id: "redis".to_owned(),
                display_name: "Redis contract".to_owned(),
                endpoint,
                database: Some("0".to_owned()),
                user: None,
                secret_id: None,
            },
            None,
        )
        .await?;

    execute_to_end(&session, "FLUSHDB").await?;
    execute_to_end(&session, "SET greeting 'hello world'").await?;
    let fetched = execute_to_end(&session, "GET greeting").await?;
    let document = fetched
        .iter()
        .find_map(|event| match event {
            QueryEvent::Rows(DataBatch::Documents(documents)) => documents.first(),
            _ => None,
        })
        .expect("GET should return a document");
    assert_eq!(document, "hello world");

    let objects = session
        .list_objects(
            ObjectListRequest {
                id: Uuid::new_v4(),
                parent: None,
                include_system: false,
                limit: 100,
                cursor: None,
                timeout: Duration::from_secs(2),
            },
            CancellationToken::new(),
        )
        .await?;
    let greeting = objects
        .items
        .iter()
        .find(|item| item.name == "greeting")
        .expect("SCAN should find the inserted key");
    assert_eq!(greeting.kind, DatabaseObjectKind::Key);
    assert_eq!(greeting.properties["value_type"], "string");

    let keys_error = match session
        .execute(query_request("KEYS *"), CancellationToken::new())
        .await
    {
        Ok(_) => panic!("blocking KEYS should be rejected"),
        Err(error) => error,
    };
    assert!(matches!(keys_error, DriverError::Query(_)));

    let slow_queries = session
        .slow_queries(
            SlowQueryRequest {
                id: Uuid::new_v4(),
                limit: 20,
                minimum_mean_time_millis: Some(0.0),
                order: SlowQueryOrder::TotalTime,
                timeout: Duration::from_secs(2),
            },
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(slow_queries.source, "SLOWLOG");
    assert!(!slow_queries.entries.is_empty());

    session.close().await?;
    Ok(())
}

fn query_request(text: &str) -> QueryRequest {
    QueryRequest::new(
        Uuid::new_v4(),
        QueryLanguage::RedisCommand,
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
