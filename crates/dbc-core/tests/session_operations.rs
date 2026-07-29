use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use dbc_core::{
    diagnostics::{
        ExecutionPlan, ExplainMode, ExplainRequest, PlanFormat, SlowQueryEntry, SlowQueryOrder,
        SlowQueryPage, SlowQueryRequest,
    },
    driver::{DatabaseSession, QueryStream},
    error::DriverError,
    metadata::{
        DatabaseObject, DatabaseObjectKind, ObjectListRequest, ObjectPage, ObjectPath,
    },
    query::QueryRequest,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[test]
fn operation_payloads_round_trip_without_driver_specific_types() {
    let object_page = ObjectPage {
        items: vec![DatabaseObject {
            id: "pg:42".to_owned(),
            name: "users".to_owned(),
            path: ObjectPath::new(["public", "users"]),
            kind: DatabaseObjectKind::Table,
            has_children: true,
            properties: BTreeMap::from([("estimated_rows".to_owned(), json!(12))]),
        }],
        next_cursor: Some("next-page".to_owned()),
    };
    let object_json = serde_json::to_value(&object_page).expect("object page should serialize");
    let restored_page: ObjectPage =
        serde_json::from_value(object_json).expect("object page should deserialize");
    assert_eq!(restored_page, object_page);

    let plan = ExecutionPlan {
        engine: "postgresql".to_owned(),
        format: PlanFormat::Json,
        analyzed: false,
        document: json!([{"Plan": {"Node Type": "Seq Scan"}}]),
        metadata: BTreeMap::new(),
    };
    let plan_json = serde_json::to_value(&plan).expect("plan should serialize");
    let restored_plan: ExecutionPlan =
        serde_json::from_value(plan_json).expect("plan should deserialize");
    assert_eq!(restored_plan, plan);

    let slow_queries = SlowQueryPage {
        source: "pg_stat_statements".to_owned(),
        entries: vec![SlowQueryEntry {
            fingerprint: Some("123".to_owned()),
            database: Some("app".to_owned()),
            user: Some("reader".to_owned()),
            query: "SELECT 1".to_owned(),
            calls: 4,
            total_time_millis: 12.5,
            mean_time_millis: 3.125,
            max_time_millis: Some(4.0),
            rows: 4,
            metadata: BTreeMap::new(),
        }],
    };
    let slow_json = serde_json::to_value(&slow_queries).expect("slow queries should serialize");
    let restored_slow: SlowQueryPage =
        serde_json::from_value(slow_json).expect("slow queries should deserialize");
    assert_eq!(restored_slow, slow_queries);
}

#[test]
fn bounded_operation_requests_reject_invalid_limits() {
    let object_request = ObjectListRequest {
        id: Uuid::new_v4(),
        parent: None,
        include_system: false,
        limit: 0,
        cursor: None,
        timeout: Duration::from_secs(2),
    };
    assert!(object_request.validate().is_err());

    let explain_request = ExplainRequest {
        id: Uuid::new_v4(),
        text: " ".to_owned(),
        mode: ExplainMode::Estimated,
        timeout: Duration::from_secs(2),
    };
    assert!(explain_request.validate().is_err());

    let slow_request = SlowQueryRequest {
        id: Uuid::new_v4(),
        limit: 10_000,
        minimum_mean_time_millis: Some(1.0),
        order: SlowQueryOrder::MeanTime,
        timeout: Duration::from_secs(2),
    };
    assert!(slow_request.validate().is_err());
}

#[tokio::test]
async fn new_session_operations_degrade_to_unsupported_by_default() {
    let session: Arc<dyn DatabaseSession> = Arc::new(MinimalSession);
    let cancellation = CancellationToken::new();

    let object_error = session
        .list_objects(
            ObjectListRequest {
                id: Uuid::new_v4(),
                parent: None,
                include_system: false,
                limit: 100,
                cursor: None,
                timeout: Duration::from_secs(2),
            },
            cancellation.clone(),
        )
        .await
        .expect_err("minimal session should not expose object discovery");
    assert!(matches!(object_error, DriverError::Unsupported(_)));

    let explain_error = session
        .explain(
            ExplainRequest {
                id: Uuid::new_v4(),
                text: "SELECT 1".to_owned(),
                mode: ExplainMode::Estimated,
                timeout: Duration::from_secs(2),
            },
            cancellation.clone(),
        )
        .await
        .expect_err("minimal session should not expose execution plans");
    assert!(matches!(explain_error, DriverError::Unsupported(_)));

    let slow_error = session
        .slow_queries(
            SlowQueryRequest {
                id: Uuid::new_v4(),
                limit: 20,
                minimum_mean_time_millis: None,
                order: SlowQueryOrder::MeanTime,
                timeout: Duration::from_secs(2),
            },
            cancellation,
        )
        .await
        .expect_err("minimal session should not expose slow queries");
    assert!(matches!(slow_error, DriverError::Unsupported(_)));
}

#[derive(Debug)]
struct MinimalSession;

#[async_trait]
impl DatabaseSession for MinimalSession {
    async fn execute(
        &self,
        _request: QueryRequest,
        _cancellation: CancellationToken,
    ) -> Result<QueryStream, DriverError> {
        Err(DriverError::Unsupported("sql".to_owned()))
    }

    async fn close(&self) -> Result<(), DriverError> {
        Ok(())
    }
}
