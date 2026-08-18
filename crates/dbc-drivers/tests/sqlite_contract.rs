use std::{collections::BTreeMap, sync::Arc, time::Duration};

use dbc_core::{
    capability::QueryLanguage,
    diagnostics::{ExplainMode, ExplainRequest},
    driver::{ConnectionProfile, DatabaseSession, DriverFactory, QueryEvent},
    error::DriverError,
    metadata::{DatabaseObjectKind, ObjectListRequest, ObjectPath},
    query::QueryRequest,
    table_data::{
        SortDirection, TableBrowseRequest, TableChangeRequest, TableDelete, TableInsert, TableRef,
        TableSort, TableUpdate,
    },
};
use dbc_core::result::{CellValue, DataBatch, DataSchema};
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
    assert_eq!(batch.row_count(), 2);
    assert_eq!(batch.value(0, 1), Some("alpha"));

    let empty = execute_to_end(
        &session,
        "SELECT id, name FROM items WHERE 0",
    )
    .await?;
    assert!(empty.iter().any(|event| matches!(
        event,
        QueryEvent::Schema(DataSchema::Tabular(schema)) if schema.len() == 2
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

#[tokio::test]
async fn sqlite_editable_table_data_is_typed_atomic_and_conflict_safe(
) -> Result<(), Box<dyn std::error::Error>> {
    let factory = SqliteFactory::new();
    let session = factory
        .connect(
            &ConnectionProfile {
                id: "sqlite-table-data".to_owned(),
                driver_id: "sqlite".to_owned(),
                display_name: "SQLite table data".to_owned(),
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
        "CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            payload BLOB,
            note TEXT
        )",
    )
    .await?;
    execute_to_end(
        &session,
        "INSERT INTO items (name, payload, note)
         VALUES ('alpha', X'00ff', NULL), ('beta', NULL, 'original')",
    )
    .await?;

    let table = TableRef::new(Vec::<String>::new(), "items");
    let metadata = session
        .table_metadata(table.clone(), CancellationToken::new())
        .await?;
    assert_eq!(
        metadata
            .stable_key()
            .expect("primary key should be stable")
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
                timeout: Duration::from_secs(2),
            },
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(page.total_rows, 2);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0][2], CellValue::Binary(vec![0, 255]));
    assert_eq!(page.rows[0][3], CellValue::Null);

    let request_id = Uuid::new_v4();
    let plan = session
        .plan_table_changes(
            TableChangeRequest {
                id: request_id,
                table: table.clone(),
                inserts: vec![TableInsert {
                    values: row_values([("name", CellValue::Text("gamma".to_owned()))]),
                }],
                updates: vec![TableUpdate {
                    identity: row_values([("id", CellValue::Text("1".to_owned()))]),
                    original_values: row_values([(
                        "name",
                        CellValue::Text("alpha".to_owned()),
                    )]),
                    values: row_values([(
                        "name",
                        CellValue::Text("alpha-updated".to_owned()),
                    )]),
                }],
                deletes: vec![TableDelete {
                    identity: row_values([("id", CellValue::Text("2".to_owned()))]),
                    original_values: row_values([(
                        "name",
                        CellValue::Text("beta".to_owned()),
                    )]),
                }],
                timeout: Duration::from_secs(2),
            },
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(plan.request_id, request_id);
    assert_eq!(plan.statements.len(), 3);
    assert!(plan
        .statements
        .iter()
        .all(|statement| !statement.sql.contains("alpha-updated")));
    assert!(plan
        .statements
        .iter()
        .any(|statement| statement.sql.contains('?')));

    let applied = session
        .apply_table_changes(plan, CancellationToken::new())
        .await?;
    assert_eq!(applied.summary.inserted, 1);
    assert_eq!(applied.summary.updated, 1);
    assert_eq!(applied.summary.deleted, 1);

    let stale_plan = session
        .plan_table_changes(
            TableChangeRequest {
                id: Uuid::new_v4(),
                table: table.clone(),
                inserts: Vec::new(),
                updates: vec![
                    TableUpdate {
                        identity: row_values([("id", CellValue::Text("3".to_owned()))]),
                        original_values: row_values([
                            ("name", CellValue::Text("gamma".to_owned())),
                            ("note", CellValue::Null),
                        ]),
                        values: row_values([(
                            "note",
                            CellValue::Text("must roll back".to_owned()),
                        )]),
                    },
                    TableUpdate {
                        identity: row_values([("id", CellValue::Text("1".to_owned()))]),
                        original_values: row_values([
                            ("name", CellValue::Text("alpha".to_owned())),
                            ("note", CellValue::Null),
                        ]),
                        values: row_values([(
                            "note",
                            CellValue::Text("stale".to_owned()),
                        )]),
                    },
                ],
                deletes: Vec::new(),
                timeout: Duration::from_secs(2),
            },
            CancellationToken::new(),
        )
        .await?;
    let conflict = session
        .apply_table_changes(stale_plan, CancellationToken::new())
        .await
        .expect_err("stale original values must conflict");
    assert!(matches!(conflict, DriverError::Conflict(_)));

    let verification = execute_to_end(
        &session,
        "SELECT note FROM items WHERE name = 'gamma'",
    )
    .await?;
    let batch = verification
        .iter()
        .find_map(|event| match event {
            QueryEvent::Rows(DataBatch::Tabular(batch)) => Some(batch),
            _ => None,
        })
        .expect("verification should return one row");
    assert_eq!(
        batch.value(0, 0),
        None,
        "the transaction must roll back"
    );

    session.close().await?;
    Ok(())
}

fn row_values<const N: usize>(values: [(&str, CellValue); N]) -> BTreeMap<String, CellValue> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
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
