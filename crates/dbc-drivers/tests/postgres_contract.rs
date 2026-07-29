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
use dbc_drivers::PostgresFactory;
use futures_util::TryStreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires DBC_TEST_POSTGRES_URL and DBC_TEST_POSTGRES_PASSWORD"]
async fn postgres_vertical_contract() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(endpoint) = env::var("DBC_TEST_POSTGRES_URL") else {
        return Ok(());
    };
    let Ok(password) = env::var("DBC_TEST_POSTGRES_PASSWORD") else {
        return Ok(());
    };
    let user =
        env::var("DBC_TEST_POSTGRES_USER").unwrap_or_else(|_| "postgres".to_owned());
    let profile = ConnectionProfile {
        id: "postgres-contract".to_owned(),
        driver_id: "postgresql".to_owned(),
        display_name: "PostgreSQL contract".to_owned(),
        endpoint,
        database: None,
        user: Some(user),
        secret_id: Some("test:postgres-contract".to_owned()),
    };
    let secret = SecretValue::new(password);
    let factory = PostgresFactory::new();
    let session = factory.connect(&profile, Some(&secret)).await?;

    execute_to_end(
        &session,
        r#"
CREATE TABLE IF NOT EXISTS public.dbc_contract_items (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"#,
    )
    .await?;
    execute_to_end(&session, "TRUNCATE public.dbc_contract_items").await?;
    let insert_events = execute_to_end(
        &session,
        r#"
INSERT INTO public.dbc_contract_items (id, name, payload)
VALUES
    (1, 'alpha', '{"enabled": true}'::jsonb),
    (2, 'beta', '{"enabled": false}'::jsonb)
"#,
    )
    .await?;
    assert!(insert_events.iter().any(
        |event| matches!(event, QueryEvent::AffectedRows(2))
    ));

    let select_events = execute_to_end(
        &session,
        "SELECT id, name, payload, created_at FROM public.dbc_contract_items ORDER BY id",
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
        .expect("PostgreSQL text columns should use Arrow UTF-8");
    assert_eq!(names.value(0), "alpha");
    assert_eq!(names.value(1), "beta");

    let root = session
        .list_objects(
            object_request(None),
            CancellationToken::new(),
        )
        .await?;
    assert!(root.items.iter().any(|item| {
        item.name == "public" && item.kind == DatabaseObjectKind::Schema
    }));

    let schema = session
        .list_objects(
            object_request(Some(ObjectPath::new(["public"]))),
            CancellationToken::new(),
        )
        .await?;
    assert!(schema.items.iter().any(|item| {
        item.name == "dbc_contract_items" && item.kind == DatabaseObjectKind::Table
    }));

    let relation = session
        .list_objects(
            object_request(Some(ObjectPath::new([
                "public",
                "dbc_contract_items",
            ]))),
            CancellationToken::new(),
        )
        .await?;
    assert!(relation.items.iter().any(|item| {
        item.name == "id" && item.kind == DatabaseObjectKind::Column
    }));
    assert!(relation.items.iter().any(|item| {
        item.kind == DatabaseObjectKind::Index
    }));

    let plan = session
        .explain(
            ExplainRequest {
                id: Uuid::new_v4(),
                text: "SELECT * FROM public.dbc_contract_items WHERE id = 1".to_owned(),
                mode: ExplainMode::Estimated,
                timeout: Duration::from_secs(5),
            },
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(plan.engine, "postgresql");
    assert!(plan.document.is_array());

    let analyzed_write = session
        .explain(
            ExplainRequest {
                id: Uuid::new_v4(),
                text: "DELETE FROM public.dbc_contract_items".to_owned(),
                mode: ExplainMode::Analyze,
                timeout: Duration::from_secs(5),
            },
            CancellationToken::new(),
        )
        .await
        .expect_err("EXPLAIN ANALYZE must not execute writes");
    assert!(matches!(analyzed_write, DriverError::Permission(_)));

    execute_to_end(
        &session,
        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
    )
    .await?;
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
    assert_eq!(slow_queries.source, "pg_stat_statements");
    assert!(!slow_queries.entries.is_empty());

    let cancellation = CancellationToken::new();
    let mut cancelled_stream = session
        .execute(
            query_request("SELECT pg_sleep(10)"),
            cancellation.clone(),
        )
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
