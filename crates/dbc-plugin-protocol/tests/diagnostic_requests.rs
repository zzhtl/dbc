use dbc_plugin_protocol::{
    CURRENT_PROTOCOL_VERSION,
    generated::dbc::driver::v1::{
        ControlRequest, ExplainMode, ExplainRequest, FrameKind, ObjectListRequest,
        SlowQueryOrder, SlowQueryRequest, control_request,
    },
};
use prost::Message;

#[test]
fn protocol_minor_one_carries_object_and_diagnostic_operations() {
    assert_eq!(CURRENT_PROTOCOL_VERSION.major, 1);
    assert_eq!(CURRENT_PROTOCOL_VERSION.minor, 1);

    let requests = [
        ControlRequest {
            request: Some(control_request::Request::ListObjects(
                ObjectListRequest {
                    request_id: "objects-1".to_owned(),
                    connection_id: "connection-1".to_owned(),
                    parent_segments: vec!["public".to_owned()],
                    include_system: false,
                    limit: 100,
                    cursor: Some("100".to_owned()),
                    timeout_millis: 5_000,
                },
            )),
        },
        ControlRequest {
            request: Some(control_request::Request::Explain(ExplainRequest {
                request_id: "explain-1".to_owned(),
                connection_id: "connection-1".to_owned(),
                query: "SELECT 1".to_owned(),
                mode: ExplainMode::Analyze as i32,
                timeout_millis: 5_000,
            })),
        },
        ControlRequest {
            request: Some(control_request::Request::SlowQueries(
                SlowQueryRequest {
                    request_id: "slow-1".to_owned(),
                    connection_id: "connection-1".to_owned(),
                    limit: 50,
                    minimum_mean_time_millis: Some(10.0),
                    order: SlowQueryOrder::TotalTime as i32,
                    timeout_millis: 5_000,
                },
            )),
        },
    ];

    for request in requests {
        let encoded = request.encode_to_vec();
        let decoded =
            ControlRequest::decode(encoded.as_slice()).expect("request should decode");
        assert_eq!(decoded, request);
    }
}

#[test]
fn diagnostic_payloads_have_distinct_frame_kinds() {
    assert_ne!(FrameKind::ObjectPageJson, FrameKind::DocumentJson);
    assert_ne!(FrameKind::ExecutionPlanJson, FrameKind::ObjectPageJson);
    assert_ne!(FrameKind::SlowQueryPageJson, FrameKind::ExecutionPlanJson);
}
