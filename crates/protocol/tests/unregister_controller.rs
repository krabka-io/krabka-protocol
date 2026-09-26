//! Round-trip and byte-exact coverage for the KIP-1312 `UnregisterController`
//! RPC (api key 94).
//!
//! The vendored JVM oracle is Kafka 4.3.0, which predates api key 94, so the
//! differential sweep skips it. These tests pin the wire shape directly: every
//! valid version round-trips through the owned and borrowed codecs, and one
//! hand-encoded frame per direction fixes the exact bytes Kafka writes.

use bytes::BytesMut;
use krabka_protocol::{
    ApiKey, Decode, DecodeBorrow, Encode, UnknownTaggedFields,
    owned::{
        unregister_controller_request::{self, UnregisterControllerRequest},
        unregister_controller_response::{self, UnregisterControllerResponse},
    },
};

fn request(controller_id: i32) -> UnregisterControllerRequest {
    UnregisterControllerRequest {
        controller_id,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn response(
    throttle_time_ms: i32,
    error_code: i16,
    error_message: Option<&str>,
) -> UnregisterControllerResponse {
    UnregisterControllerResponse {
        throttle_time_ms,
        error_code,
        error_message: error_message.map(str::to_owned),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

/// Encode `msg` at `version`, check `encoded_len`, and return the bytes.
fn encode<T: Encode>(msg: &T, version: i16) -> Vec<u8> {
    let mut buf = BytesMut::new();
    msg.encode(&mut buf, version).unwrap();
    assert2::assert!(msg.encoded_len(version) == buf.len());
    buf.to_vec()
}

/// Decode `bytes` at `version` with the owned codec, requiring full consumption.
fn decode<'de, T: Decode<'de>>(bytes: &'de [u8], version: i16) -> T {
    let mut cur = bytes;
    let decoded = T::decode(&mut cur, version).unwrap();
    assert2::assert!(cur.is_empty());
    decoded
}

#[test]
fn api_key_94_is_unregister_controller() {
    assert2::assert!(ApiKey::from_i16(94) == Some(ApiKey::UnregisterController));
    assert2::assert!(unregister_controller_request::API_KEY == 94);
    assert2::assert!(unregister_controller_response::API_KEY == 94);
}

#[test]
fn version_range_matches_kafka() {
    let ranges = [
        (
            "request",
            unregister_controller_request::MIN_VERSION,
            unregister_controller_request::MAX_VERSION,
            unregister_controller_request::FLEXIBLE_MIN,
        ),
        (
            "response",
            unregister_controller_response::MIN_VERSION,
            unregister_controller_response::MAX_VERSION,
            unregister_controller_response::FLEXIBLE_MIN,
        ),
    ];
    for (_direction, min, max, flexible_min) in ranges {
        assert2::assert!((min, max, flexible_min) == (0, 0, 0));
    }
}

#[test]
fn request_round_trips_at_every_version() {
    let cases = [
        ("zero", request(0)),
        ("typical", request(3000)),
        ("negative", request(-1)),
        ("max", request(i32::MAX)),
    ];
    for version in
        unregister_controller_request::MIN_VERSION..=unregister_controller_request::MAX_VERSION
    {
        for (_case, msg) in &cases {
            let bytes = encode(msg, version);
            assert2::assert!(decode::<UnregisterControllerRequest>(&bytes, version) == *msg);

            let mut cur: &[u8] = &bytes;
            let borrowed =
                krabka_protocol::borrowed::unregister_controller_request::UnregisterControllerRequest::decode_borrow(
                    &mut cur, version,
                )
                .unwrap();
            assert2::assert!(cur.is_empty());
            assert2::assert!(borrowed.to_owned() == *msg);
            assert2::assert!(encode(&borrowed, version) == bytes);
        }
    }
}

#[test]
fn response_round_trips_at_every_version() {
    let cases = [
        ("success null message", response(0, 0, None)),
        ("success empty message", response(0, 0, Some(""))),
        ("throttled", response(250, 0, None)),
        (
            "error with message",
            response(
                0,
                41,
                Some("This is not the correct controller for this cluster."),
            ),
        ),
    ];
    for version in
        unregister_controller_response::MIN_VERSION..=unregister_controller_response::MAX_VERSION
    {
        for (_case, msg) in &cases {
            let bytes = encode(msg, version);
            assert2::assert!(decode::<UnregisterControllerResponse>(&bytes, version) == *msg);

            let mut cur: &[u8] = &bytes;
            let borrowed =
                krabka_protocol::borrowed::unregister_controller_response::UnregisterControllerResponse::decode_borrow(
                    &mut cur, version,
                )
                .unwrap();
            assert2::assert!(cur.is_empty());
            assert2::assert!(borrowed.to_owned() == *msg);
            assert2::assert!(encode(&borrowed, version) == bytes);
        }
    }
}

#[test]
fn request_v0_is_byte_exact() {
    // ControllerId int32 3000, then an empty tagged-field section.
    let wire = [0x00, 0x00, 0x0b, 0xb8, 0x00];
    let msg = request(3000);
    assert2::assert!(encode(&msg, 0) == wire);
    assert2::assert!(decode::<UnregisterControllerRequest>(&wire, 0) == msg);
}

#[test]
fn response_v0_is_byte_exact() {
    // ThrottleTimeMs int32 5, ErrorCode int16 41 (NOT_CONTROLLER), ErrorMessage
    // compact nullable string "no" (length + 1 = 3), then an empty tagged-field
    // section.
    let wire = [0x00, 0x00, 0x00, 0x05, 0x00, 0x29, 0x03, b'n', b'o', 0x00];
    let msg = response(5, 41, Some("no"));
    assert2::assert!(encode(&msg, 0) == wire);
    assert2::assert!(decode::<UnregisterControllerResponse>(&wire, 0) == msg);

    // A null ErrorMessage is the compact-string length 0.
    let null_wire = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    let null_msg = response(0, 0, None);
    assert2::assert!(encode(&null_msg, 0) == null_wire);
    assert2::assert!(decode::<UnregisterControllerResponse>(&null_wire, 0) == null_msg);
}

#[test]
fn unsupported_version_is_rejected() {
    let mut buf = BytesMut::new();
    assert2::assert!(request(1).encode(&mut buf, 1).is_err());
    assert2::assert!(response(0, 0, None).encode(&mut buf, 1).is_err());
}
