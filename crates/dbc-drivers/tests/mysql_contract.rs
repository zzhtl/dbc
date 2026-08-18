use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

use dbc_core::{
    capability::QueryLanguage,
    diagnostics::{ExplainMode, ExplainRequest, SlowQueryOrder, SlowQueryRequest},
    driver::{
        ConnectionProfile, DatabaseSession, DriverFactory, QueryEvent, SecretValue,
    },
    error::DriverError,
    metadata::{DatabaseObjectKind, ObjectListRequest, ObjectPath},
    query::QueryRequest,
    table_data::{
        SortDirection, TableBrowseRequest, TableChangeRequest, TableRef, TableSort, TableUpdate,
    },
};
use dbc_core::result::{CellValue, DataBatch};
use dbc_drivers::MySqlFactory;
use futures_util::TryStreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires DBC_TEST_MYSQL_URL and DBC_TEST_MYSQL_PASSWORD"]
async fn mysql_vertical_contract() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(endpoint) = env::var("DBC_TEST_MYSQL_URL") else {
        // Running with `--ignored` but no environment used to report a green
        // pass while testing nothing at all.
        eprintln!("skipped: DBC_TEST_MYSQL_URL is not set");
        return Ok(());
    };
    let Ok(password) = env::var("DBC_TEST_MYSQL_PASSWORD") else {
        // Running with `--ignored` but no environment used to report a green
        // pass while testing nothing at all.
        eprintln!("skipped: DBC_TEST_MYSQL_PASSWORD is not set");
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
    assert_eq!(batch.row_count(), 2);
    assert_eq!(batch.value(0, 1), Some("alpha"));
    assert_eq!(batch.value(1, 1), Some("beta"));

    let table = TableRef::new([database.as_str()], "dbc_contract_items");
    let metadata = session
        .table_metadata(table.clone(), CancellationToken::new())
        .await?;
    assert_eq!(
        metadata
            .stable_key()
            .expect("MySQL primary key should be stable")
            .columns,
        ["id"]
    );
    let page = session
        .browse_table(
            TableBrowseRequest {
                id: Uuid::new_v4(),
                table: table.clone(),
                filters: Vec::new(),
                sort: Some(vec![TableSort {
                    column: "id".to_owned(),
                    direction: SortDirection::Ascending,
                }]),
                raw_where: None,
                raw_order_by: None,
                page_index: 0,
                page_size: 1,
                timeout: Duration::from_secs(5),
            },
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(page.total_rows, 2);
    assert_eq!(page.rows.len(), 1);

    let update_request = TableChangeRequest {
        id: Uuid::new_v4(),
        table: table.clone(),
        inserts: Vec::new(),
        updates: vec![TableUpdate {
            identity: BTreeMap::from([(
                "id".to_owned(),
                CellValue::Text("1".to_owned()),
            )]),
            original_values: BTreeMap::from([(
                "name".to_owned(),
                CellValue::Text("alpha".to_owned()),
            )]),
            values: BTreeMap::from([(
                "name".to_owned(),
                CellValue::Text("alpha-edited".to_owned()),
            )]),
        }],
        deletes: Vec::new(),
        timeout: Duration::from_secs(5),
    };
    let update_plan = session
        .plan_table_changes(update_request.clone(), CancellationToken::new())
        .await?;
    assert!(update_plan.statements[0].sql.contains('?'));
    assert!(!update_plan.statements[0].sql.contains("alpha-edited"));
    session
        .apply_table_changes(update_plan, CancellationToken::new())
        .await?;
    let stale_plan = session
        .plan_table_changes(update_request, CancellationToken::new())
        .await?;
    assert!(matches!(
        session
            .apply_table_changes(stale_plan, CancellationToken::new())
            .await,
        Err(DriverError::Conflict(_))
    ));

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

    // The stream is lazy: cancelling before it is polled would never start the
    // statement at all, so drive it on a task and only cancel once the server
    // is provably running it.
    let cancellation = CancellationToken::new();
    let mut cancelled_stream = session
        .execute(query_request("SELECT SLEEP(10)"), cancellation.clone())
        .await?;
    // The driver yields `Schema` right after preparing, before the statement
    // runs, so the reader must consume past it for the query to actually start.
    let reader = tokio::spawn(async move {
        while cancelled_stream.try_next().await?.is_some() {}
        Ok::<(), DriverError>(())
    });
    assert!(
        wait_for_running_query(&session, "SLEEP(10)", true).await?,
        "the long query should be visible as running before it is cancelled"
    );

    cancellation.cancel();
    let cancelled = reader
        .await
        .expect("reader task should not panic")
        .expect_err("cancelled query should terminate with a typed error");
    assert!(matches!(cancelled, DriverError::Cancelled));

    // A client-side cancellation is not enough: `KILL QUERY` must actually
    // stop the statement, otherwise the server keeps working for nothing.
    assert!(
        wait_for_running_query(&session, "SLEEP(10)", false).await?,
        "KILL QUERY should have stopped the statement server-side"
    );

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

/// Poll the process list until a statement matching `needle` is (or is no
/// longer) running on another session.
async fn wait_for_running_query(
    session: &Arc<dyn DatabaseSession>,
    needle: &str,
    expect_running: bool,
) -> Result<bool, DriverError> {
    let sql = format!(
        "SELECT COUNT(*) AS running FROM information_schema.PROCESSLIST \
         WHERE ID <> CONNECTION_ID() AND INFO LIKE '%{needle}%' \
           AND INFO NOT LIKE '%PROCESSLIST%'"
    );
    poll_running_count(session, &sql, expect_running).await
}

/// Run `sql` until its single count column reaches the expected state.
async fn poll_running_count(
    session: &Arc<dyn DatabaseSession>,
    sql: &str,
    expect_running: bool,
) -> Result<bool, DriverError> {
    for _ in 0..50 {
        let events = execute_to_end(session, sql).await?;
        let running = events
            .iter()
            .find_map(|event| match event {
                QueryEvent::Rows(DataBatch::Tabular(batch)) => batch.value(0, 0),
                _ => None,
            })
            .unwrap_or("0")
            != "0";
        if running == expect_running {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(false)
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
