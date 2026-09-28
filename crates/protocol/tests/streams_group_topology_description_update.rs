//! Round-trip and byte-exact coverage for the KIP-1331
//! `StreamsGroupTopologyDescriptionUpdate` RPC (api key 93).
//!
//! The vendored JVM oracle is Kafka 4.3.0, which predates api key 93, so the
//! differential sweep skips it. These tests pin the wire shape directly: every
//! message round-trips through the owned and borrowed codecs, and one
//! hand-encoded frame per direction fixes the exact bytes Kafka writes.

use bytes::BytesMut;
use krabka_protocol::{
    ApiKey, Decode, DecodeBorrow, Encode, UnknownTaggedFields,
    owned::{
        common::streams_group_topology_description_update_request::{
            topology_description::TopologyDescription,
            topology_description_global_store::TopologyDescriptionGlobalStore,
            topology_description_node::TopologyDescriptionNode,
            topology_description_subtopology::TopologyDescriptionSubtopology,
        },
        streams_group_topology_description_update_request::{
            self, StreamsGroupTopologyDescriptionUpdateRequest,
        },
        streams_group_topology_description_update_response::{
            self, StreamsGroupTopologyDescriptionUpdateResponse,
        },
    },
};

/// Kafka's `STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED` error code (KIP-1331).
const STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED: i16 = 135;

fn node(name: &str, node_type: i8, source_topics: &[&str]) -> TopologyDescriptionNode {
    TopologyDescriptionNode {
        name: name.into(),
        node_type,
        source_topics: source_topics.iter().map(|t| (*t).into()).collect(),
        sink_topic: None,
        stores: vec![],
        successors: vec![],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn request(topology: TopologyDescription) -> StreamsGroupTopologyDescriptionUpdateRequest {
    StreamsGroupTopologyDescriptionUpdateRequest {
        group_id: "g".into(),
        member_id: "m".into(),
        topology_epoch: 7,
        topology_description: topology,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn one_source_topology() -> TopologyDescription {
    TopologyDescription {
        subtopologies: vec![TopologyDescriptionSubtopology {
            subtopology_id: "0".into(),
            nodes: vec![node("S", 1, &["t"])],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        global_stores: vec![],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn response(
    error_code: i16,
    error_message: Option<&str>,
) -> StreamsGroupTopologyDescriptionUpdateResponse {
    StreamsGroupTopologyDescriptionUpdateResponse {
        throttle_time_ms: 0,
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
fn api_key_93_is_streams_group_topology_description_update() {
    let keys = (
        ApiKey::from_i16(93),
        streams_group_topology_description_update_request::API_KEY,
        streams_group_topology_description_update_response::API_KEY,
    );
    assert2::assert!(keys == (Some(ApiKey::StreamsGroupTopologyDescriptionUpdate), 93, 93));
}

#[test]
fn version_range_matches_kafka() {
    let ranges = [
        (
            "request",
            streams_group_topology_description_update_request::MIN_VERSION,
            streams_group_topology_description_update_request::MAX_VERSION,
            streams_group_topology_description_update_request::LATEST_STABLE_VERSION,
            streams_group_topology_description_update_request::FLEXIBLE_MIN,
        ),
        (
            "response",
            streams_group_topology_description_update_response::MIN_VERSION,
            streams_group_topology_description_update_response::MAX_VERSION,
            0,
            streams_group_topology_description_update_response::FLEXIBLE_MIN,
        ),
    ];
    for (direction, min, max, latest_stable, flexible_min) in ranges {
        assert2::assert!(
            (min, max, latest_stable, flexible_min) == (0, 0, 0, 0),
            "{direction}"
        );
    }
}

#[test]
fn request_round_trips() {
    let sink = TopologyDescriptionNode {
        sink_topic: Some("out".into()),
        stores: vec!["store".into()],
        successors: vec!["K".into()],
        ..node("K", 3, &[])
    };
    let cases = [
        (
            "empty topology",
            request(TopologyDescription {
                subtopologies: vec![],
                global_stores: vec![],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }),
        ),
        ("one source node", request(one_source_topology())),
        (
            "sink node and global store",
            request(TopologyDescription {
                subtopologies: vec![TopologyDescriptionSubtopology {
                    subtopology_id: "1".into(),
                    nodes: vec![node("S", 1, &["a", "b"]), sink],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                global_stores: vec![TopologyDescriptionGlobalStore {
                    source: node("GS", 1, &["global"]),
                    processor: node("GP", 2, &[]),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }),
        ),
    ];
    for (case, msg) in &cases {
        let bytes = encode(msg, 0);
        assert2::assert!(
            decode::<StreamsGroupTopologyDescriptionUpdateRequest>(&bytes, 0) == *msg,
            "{case}"
        );

        let mut cur: &[u8] = &bytes;
        let borrowed = krabka_protocol::borrowed::streams_group_topology_description_update_request::StreamsGroupTopologyDescriptionUpdateRequest::decode_borrow(
            &mut cur, 0,
        )
        .unwrap();
        assert2::assert!(cur.is_empty(), "{case}");
        assert2::assert!(borrowed.to_owned() == *msg, "{case}");
        assert2::assert!(encode(&borrowed, 0) == bytes, "{case}");
    }
}

#[test]
fn request_v0_is_byte_exact() {
    let wire = [
        0x02, b'g', // GroupId "g"
        0x02, b'm', // MemberId "m"
        0x00, 0x00, 0x00, 0x07, // TopologyEpoch 7
        0x02, // Subtopologies: one entry
        0x02, b'0', // SubtopologyId "0"
        0x02, // Nodes: one entry
        0x02, b'S', // Name "S"
        0x01, // NodeType SOURCE
        0x02, 0x02, b't', // SourceTopics ["t"]
        0x00, // SinkTopic null
        0x01, // Stores []
        0x01, // Successors []
        0x00, // node tagged fields
        0x00, // subtopology tagged fields
        0x01, // GlobalStores []
        0x00, // topology tagged fields
        0x00, // request tagged fields
    ];
    let msg = request(one_source_topology());
    assert2::assert!(encode(&msg, 0) == wire);
    assert2::assert!(decode::<StreamsGroupTopologyDescriptionUpdateRequest>(&wire, 0) == msg);
}

#[test]
fn response_v0_is_byte_exact() {
    let cases: [(&str, StreamsGroupTopologyDescriptionUpdateResponse, &[u8]); 2] = [
        (
            "success with a null message",
            response(0, None),
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
        (
            "update failed with a message",
            response(STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED, Some("boom")),
            &[
                0x00, 0x00, 0x00, 0x00, 0x00, 0x87, 0x05, b'b', b'o', b'o', b'm', 0x00,
            ],
        ),
    ];
    for (case, msg, wire) in cases {
        assert2::assert!(encode(&msg, 0) == wire, "{case}");
        assert2::assert!(
            decode::<StreamsGroupTopologyDescriptionUpdateResponse>(wire, 0) == msg,
            "{case}"
        );
    }
}

#[test]
fn unsupported_version_is_rejected() {
    let mut buf = BytesMut::new();
    assert2::assert!(request(one_source_topology()).encode(&mut buf, 1).is_err());
    assert2::assert!(response(0, None).encode(&mut buf, 1).is_err());
}
