use bytes::Bytes;
use dbc_plugin_protocol::{
    CodecLimits, Frame, FrameCodec, ProtocolVersion, generated::dbc::driver::v1::FrameKind,
};
use tokio::io::duplex;

#[test]
fn protocol_accepts_same_major_and_older_or_equal_minor() {
    let host = ProtocolVersion::new(1, 3);

    assert!(host.accepts(ProtocolVersion::new(1, 0)));
    assert!(host.accepts(ProtocolVersion::new(1, 3)));
    assert!(!host.accepts(ProtocolVersion::new(1, 4)));
    assert!(!host.accepts(ProtocolVersion::new(2, 0)));
}

#[tokio::test]
async fn frame_round_trip_preserves_header_and_binary_payload() {
    let (mut writer, mut reader) = duplex(4096);
    let codec = FrameCodec::new(CodecLimits::default());
    let frame = Frame::new(
        "query-1",
        FrameKind::ArrowIpc,
        7,
        true,
        Bytes::from_static(b"\0arrow\ndata"),
    );

    codec
        .write_frame(&mut writer, &frame)
        .await
        .expect("frame should encode");
    let decoded = codec
        .read_frame(&mut reader)
        .await
        .expect("frame should decode");

    assert_eq!(decoded, frame);
}

#[tokio::test]
async fn codec_rejects_payloads_larger_than_the_negotiated_limit() {
    let (mut writer, _) = duplex(4096);
    let codec = FrameCodec::new(CodecLimits {
        max_header_bytes: 1024,
        max_payload_bytes: 4,
    });
    let frame = Frame::new(
        "query-1",
        FrameKind::DocumentJson,
        0,
        false,
        Bytes::from_static(b"12345"),
    );

    let error = codec
        .write_frame(&mut writer, &frame)
        .await
        .expect_err("oversized payload should be rejected");
    assert_eq!(error.to_string(), "payload length 5 exceeds limit 4");
}
