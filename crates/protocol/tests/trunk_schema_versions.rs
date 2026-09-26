//! Byte-exact coverage for the schema versions vendored from Kafka trunk ahead
//! of the 4.3.0 pin: `TxnOffsetCommit` v6 (KIP-1319 topic ids),
//! `StreamsGroupHeartbeat` v1 and `StreamsGroupDescribe` v1 (KIP-1331).
//!
//! The vendored JVM oracle is Kafka 4.3.0, which predates these versions, so
//! the differential sweep cannot check them. Each case here is a frame encoded
//! by hand from the trunk schema, and the owned codec must both write it and
//! read it back to the same message.

use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode, UnknownTaggedFields,
    owned::{
        streams_group_describe_request::StreamsGroupDescribeRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
        txn_offset_commit_request::{
            TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
        },
        txn_offset_commit_response::{
            TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};

const TOPIC_ID: [u8; 16] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
];

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

fn check_frame<T>(msg: &T, version: i16, wire: &[u8])
where
    T: Encode + for<'de> Decode<'de> + PartialEq + std::fmt::Debug,
{
    assert2::assert!(encode(msg, version) == wire);
    assert2::assert!(decode::<T>(wire, version) == *msg);
}

fn txn_offset_commit_request(name: &str, topic_id: [u8; 16]) -> TxnOffsetCommitRequest {
    TxnOffsetCommitRequest {
        transactional_id: "t".into(),
        group_id: "g".into(),
        producer_id: 1,
        producer_epoch: 2,
        generation_id_or_member_epoch: 3,
        member_id: "m".into(),
        group_instance_id: None,
        topics: vec![TxnOffsetCommitRequestTopic {
            name: name.into(),
            topic_id: Uuid(topic_id),
            partitions: vec![TxnOffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 5,
                committed_leader_epoch: -1,
                committed_metadata: Some(String::new()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn txn_offset_commit_response(name: &str, topic_id: [u8; 16]) -> TxnOffsetCommitResponse {
    TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics: vec![TxnOffsetCommitResponseTopic {
            name: name.into(),
            topic_id: Uuid(topic_id),
            partitions: vec![TxnOffsetCommitResponsePartition {
                partition_index: 0,
                error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

/// KIP-1319: v6 identifies a topic by id and drops the name; v5 still carries
/// the name and no id.
#[test]
fn txn_offset_commit_request_frames() {
    let header = [
        0x02, b't', // TransactionalId "t"
        0x02, b'g', // GroupId "g"
        0, 0, 0, 0, 0, 0, 0, 1, // ProducerId 1
        0, 2, // ProducerEpoch 2
        0, 0, 0, 3, // GenerationIdOrMemberEpoch 3
        0x02, b'm', // MemberId "m"
        0x00, // GroupInstanceId null
        0x02, // one topic
    ];
    let partition = [
        0x02, // one partition
        0, 0, 0, 0, // PartitionIndex 0
        0, 0, 0, 0, 0, 0, 0, 5, // CommittedOffset 5
        0xff, 0xff, 0xff, 0xff, // CommittedLeaderEpoch -1
        0x01, // CommittedMetadata ""
        0x00, // partition tagged fields
        0x00, // topic tagged fields
        0x00, // request tagged fields
    ];
    let v6 = [&header[..], &TOPIC_ID, &partition].concat();
    let v5 = [&header[..], &[0x07], b"orders", &partition].concat();

    for (version, msg, wire) in [
        (6, txn_offset_commit_request("", TOPIC_ID), v6),
        (5, txn_offset_commit_request("orders", [0; 16]), v5),
    ] {
        check_frame(&msg, version, &wire);
    }
}

#[test]
fn txn_offset_commit_response_frames() {
    let partition = [
        0x02, // one partition
        0, 0, 0, 0, // PartitionIndex 0
        0, 0,    // ErrorCode 0
        0x00, // partition tagged fields
        0x00, // topic tagged fields
        0x00, // response tagged fields
    ];
    let head = [0, 0, 0, 0, 0x02]; // ThrottleTimeMs 0, one topic
    let v6 = [&head[..], &TOPIC_ID, &partition].concat();
    let v5 = [&head[..], &[0x07], b"orders", &partition].concat();

    for (version, msg, wire) in [
        (6, txn_offset_commit_response("", TOPIC_ID), v6),
        (5, txn_offset_commit_response("orders", [0; 16]), v5),
    ] {
        check_frame(&msg, version, &wire);
    }
}

/// KIP-1331: v1 replaces the int32 `AcceptableRecoveryLagLegacy` with an int64
/// `AcceptableRecoveryLag` after `TaskOffsetIntervalMs`, and adds
/// `TopologyDescriptionRequired` after the task assignments.
#[test]
fn streams_group_heartbeat_response_frames() {
    let msg = |legacy_lag: i32, lag: i64, topology_required: bool| StreamsGroupHeartbeatResponse {
        member_id: "m".into(),
        member_epoch: 1,
        heartbeat_interval_ms: 2,
        acceptable_recovery_lag_legacy: legacy_lag,
        task_offset_interval_ms: 3,
        acceptable_recovery_lag: lag,
        status: None,
        topology_description_required: topology_required,
        endpoint_information_epoch: 4,
        ..StreamsGroupHeartbeatResponse::default()
    };
    let head = [
        0, 0, 0, 0, // ThrottleTimeMs 0
        0, 0,    // ErrorCode 0
        0x00, // ErrorMessage null
        0x02, b'm', // MemberId "m"
        0, 0, 0, 1, // MemberEpoch 1
        0, 0, 0, 2, // HeartbeatIntervalMs 2
    ];
    let v1 = [
        &head[..],
        &[0, 0, 0, 3],                   // TaskOffsetIntervalMs 3
        &[0, 0, 0, 0, 0, 0, 0x03, 0xe8], // AcceptableRecoveryLag 1000
        &[0x00, 0x00, 0x00, 0x00],       // Status, ActiveTasks, StandbyTasks, WarmupTasks null
        &[0x01],                         // TopologyDescriptionRequired true
        &[0, 0, 0, 4, 0x00, 0x00],       // EndpointInformationEpoch 4, no endpoints, tags
    ]
    .concat();
    let v0 = [
        &head[..],
        &[0, 0, 0x03, 0xe8],       // AcceptableRecoveryLagLegacy 1000
        &[0, 0, 0, 3],             // TaskOffsetIntervalMs 3
        &[0x00, 0x00, 0x00, 0x00], // Status, ActiveTasks, StandbyTasks, WarmupTasks null
        &[0, 0, 0, 4, 0x00, 0x00], // EndpointInformationEpoch 4, no endpoints, tags
    ]
    .concat();

    for (version, message, wire) in [(1, msg(0, 1000, true), v1), (0, msg(1000, -1, false), v0)] {
        check_frame(&message, version, &wire);
    }
}

/// KIP-1331: v1 appends `IncludeTopologyDescription`.
#[test]
fn streams_group_describe_request_frames() {
    let msg = |include_topology_description| StreamsGroupDescribeRequest {
        group_ids: vec!["g".into()],
        include_authorized_operations: true,
        include_topology_description,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    for (version, message, wire) in [
        (1, msg(true), vec![0x02, 0x02, b'g', 0x01, 0x01, 0x00]),
        (0, msg(false), vec![0x02, 0x02, b'g', 0x01, 0x00]),
    ] {
        check_frame(&message, version, &wire);
    }
}
