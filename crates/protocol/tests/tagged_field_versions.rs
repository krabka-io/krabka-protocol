//! KIP-482 tags belong to the schema versions that declare them.
//!
//! A message can be flexible from an earlier version than one of its tagged
//! fields. `FetchRequest` is flexible from v12, but `ReplicaState` carries
//! `"versions": "15+"`. A v12 to v14 request must therefore never carry tag 1,
//! whatever the field holds in memory. The same rule covers nested structs and
//! response messages.
//!
//! Each case sets one tagged field to a non-default value and compares the
//! encoding with the encoding of the same message with that field left at its
//! default. Below the field's first version the two must be byte-identical,
//! because the tag is out of range. From that version on they must differ. The
//! test also checks `encoded_len` against the buffer it produced, so the length
//! path and the encode path stay in step.
//!
//! The decoder applies the same rule. Kafka's generated `read` method throws
//! `Tag N is not valid for version V` when a known tag arrives at a flexible
//! version that its schema does not declare. `decode_rejects_out_of_version_tags`
//! sends each such tag as an unknown field, so the bytes carry the tag, and
//! expects that error.

use std::ops::RangeInclusive;

use assert2::assert;
use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Decode, DecodeBorrow, Encode, ProtocolError, UnknownTaggedField, UnknownTaggedFields,
    borrowed::{
        fetch_request::{
            FetchPartition as FetchPartitionBorrowed, FetchRequest as FetchRequestBorrowed,
            FetchTopic as FetchTopicBorrowed, ReplicaState as ReplicaStateBorrowed,
        },
        produce_response::ProduceResponse as ProduceResponseBorrowed,
    },
    kafka_3_6_2::owned::fetch_request::FetchRequest as FetchRequest362,
    owned::{
        broker_heartbeat_request::BrokerHeartbeatRequest,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic, ReplicaState},
        partition_record::PartitionRecord,
        produce_response::{NodeEndpoint, ProduceResponse},
        vote_response::VoteResponse,
    },
};

fn encode<T: Encode>(message: &T, version: i16) -> Vec<u8> {
    let mut buf = BytesMut::new();
    message.encode(&mut buf, version).unwrap();
    assert!(message.encoded_len(version) == buf.len());
    buf.to_vec()
}

/// One tagged field, the first version that declares it, and the flexible
/// range of the message that holds it.
struct Case<T> {
    name: &'static str,
    tag_min_version: i16,
    flexible_min: i16,
    max_version: i16,
    without_tag: fn() -> T,
    with_tag: fn() -> T,
}

fn check<T: Encode>(case: &Case<T>) {
    for version in case.flexible_min..=case.max_version {
        let plain = encode(&(case.without_tag)(), version);
        let tagged = encode(&(case.with_tag)(), version);
        if version < case.tag_min_version {
            assert!(
                plain == tagged,
                "{} must not write its tag at v{version}, below v{}",
                case.name,
                case.tag_min_version
            );
        } else {
            assert!(
                plain != tagged,
                "{} must write its tag at v{version}",
                case.name
            );
        }
    }
}

fn fetch_request(replica_state: ReplicaState) -> FetchRequest {
    FetchRequest {
        replica_state,
        ..FetchRequest::default()
    }
}

fn fetch_request_with_partition(partition: FetchPartition) -> FetchRequest {
    FetchRequest {
        topics: vec![FetchTopic {
            partitions: vec![partition],
            ..FetchTopic::default()
        }],
        ..FetchRequest::default()
    }
}

#[test]
fn fetch_request_replica_state_tag_starts_at_v15() {
    check(&Case {
        name: "FetchRequest.ReplicaState",
        tag_min_version: 15,
        flexible_min: 12,
        max_version: 18,
        without_tag: || fetch_request(ReplicaState::default()),
        with_tag: || {
            fetch_request(ReplicaState {
                replica_id: 7,
                replica_epoch: 11,
                ..ReplicaState::default()
            })
        },
    });
}

#[test]
fn fetch_partition_high_watermark_tag_starts_at_v18() {
    check(&Case {
        name: "FetchRequest.FetchPartition.HighWatermark",
        tag_min_version: 18,
        flexible_min: 12,
        max_version: 18,
        without_tag: || fetch_request_with_partition(FetchPartition::default()),
        with_tag: || {
            fetch_request_with_partition(FetchPartition {
                high_watermark: 42,
                ..FetchPartition::default()
            })
        },
    });
}

#[test]
fn produce_response_node_endpoints_tag_starts_at_v10() {
    check(&Case {
        name: "ProduceResponse.NodeEndpoints",
        tag_min_version: 10,
        flexible_min: 9,
        max_version: 13,
        without_tag: ProduceResponse::default,
        with_tag: || ProduceResponse {
            node_endpoints: vec![NodeEndpoint {
                node_id: 1,
                host: "broker-1".into(),
                port: 9092,
                ..NodeEndpoint::default()
            }],
            ..ProduceResponse::default()
        },
    });
}

/// The borrowed emitter carries the same gate. This is the zero-copy flavor of
/// `fetch_request_replica_state_tag_starts_at_v15`.
#[test]
fn borrowed_fetch_request_replica_state_tag_starts_at_v15() {
    check(&Case {
        name: "FetchRequest.ReplicaState (borrowed)",
        tag_min_version: 15,
        flexible_min: 12,
        max_version: 18,
        without_tag: FetchRequestBorrowed::default,
        with_tag: || FetchRequestBorrowed {
            replica_state: ReplicaStateBorrowed {
                replica_id: 7,
                replica_epoch: 11,
                ..ReplicaStateBorrowed::default()
            },
            ..FetchRequestBorrowed::default()
        },
    });
}

/// One known tag, sent at flexible versions where its schema does not declare
/// it.
struct OutOfVersion {
    name: &'static str,
    tag: u32,
    /// Flexible versions below the first version that declares the tag.
    rejected: RangeInclusive<i16>,
    /// The first version that declares the tag.
    accepted: i16,
    /// A valid encoding of the field value.
    payload: &'static [u8],
    /// Encodes the message at a version with `unknown` in the struct that
    /// declares the tag.
    wire: fn(UnknownTaggedFields, i16) -> Vec<u8>,
    /// Decodes the whole message at a version.
    decode: fn(&[u8], i16) -> Result<(), ProtocolError>,
}

fn unknown(tag: u32, payload: &'static [u8]) -> UnknownTaggedFields {
    UnknownTaggedFields(vec![UnknownTaggedField {
        tag,
        bytes: Bytes::from_static(payload),
    }])
}

fn decode_owned<T: for<'a> Decode<'a>>(bytes: &[u8], version: i16) -> Result<(), ProtocolError> {
    let mut cursor = bytes;
    T::decode(&mut cursor, version)?;
    assert!(cursor.is_empty());
    Ok(())
}

fn fetch_partition_wire(unknown_tagged_fields: UnknownTaggedFields, version: i16) -> Vec<u8> {
    encode(
        &fetch_request_with_partition(FetchPartition {
            unknown_tagged_fields,
            ..FetchPartition::default()
        }),
        version,
    )
}

const HIGH_WATERMARK: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 42];
const REPLICA_DIRECTORY_ID: &[u8] = &[7; 16];
const EMPTY_COMPACT_ARRAY: &[u8] = &[1];
/// `ReplicaState` at v15: replica id 1, replica epoch 2, no tagged fields.
const REPLICA_STATE: &[u8] = &[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0];

fn out_of_version_cases() -> Vec<OutOfVersion> {
    vec![
        OutOfVersion {
            name: "FetchRequest.FetchPartition.ReplicaDirectoryId",
            tag: 0,
            rejected: 12..=16,
            accepted: 17,
            payload: REPLICA_DIRECTORY_ID,
            wire: fetch_partition_wire,
            decode: decode_owned::<FetchRequest>,
        },
        OutOfVersion {
            name: "FetchRequest.FetchPartition.HighWatermark",
            tag: 1,
            rejected: 12..=17,
            accepted: 18,
            payload: HIGH_WATERMARK,
            wire: fetch_partition_wire,
            decode: decode_owned::<FetchRequest>,
        },
        OutOfVersion {
            name: "ProduceResponse.NodeEndpoints",
            tag: 0,
            rejected: 9..=9,
            accepted: 10,
            payload: EMPTY_COMPACT_ARRAY,
            wire: |unknown_tagged_fields, version| {
                encode(
                    &ProduceResponse {
                        unknown_tagged_fields,
                        ..ProduceResponse::default()
                    },
                    version,
                )
            },
            decode: decode_owned::<ProduceResponse>,
        },
        OutOfVersion {
            name: "VoteResponse.NodeEndpoints",
            tag: 0,
            rejected: 0..=0,
            accepted: 1,
            payload: EMPTY_COMPACT_ARRAY,
            wire: |unknown_tagged_fields, version| {
                encode(
                    &VoteResponse {
                        unknown_tagged_fields,
                        ..VoteResponse::default()
                    },
                    version,
                )
            },
            decode: decode_owned::<VoteResponse>,
        },
        OutOfVersion {
            name: "BrokerHeartbeatRequest.CordonedLogDirs",
            tag: 1,
            rejected: 0..=1,
            accepted: 2,
            payload: EMPTY_COMPACT_ARRAY,
            wire: |unknown_tagged_fields, version| {
                encode(
                    &BrokerHeartbeatRequest {
                        unknown_tagged_fields,
                        ..BrokerHeartbeatRequest::default()
                    },
                    version,
                )
            },
            decode: decode_owned::<BrokerHeartbeatRequest>,
        },
        OutOfVersion {
            name: "PartitionRecord.EligibleLeaderReplicas",
            tag: 1,
            rejected: 0..=1,
            accepted: 2,
            payload: EMPTY_COMPACT_ARRAY,
            wire: |unknown_tagged_fields, version| {
                encode(
                    &PartitionRecord {
                        unknown_tagged_fields,
                        ..PartitionRecord::default()
                    },
                    version,
                )
            },
            decode: decode_owned::<PartitionRecord>,
        },
        OutOfVersion {
            name: "kafka_3_6_2 FetchRequest.ReplicaState",
            tag: 1,
            rejected: 12..=14,
            accepted: 15,
            payload: REPLICA_STATE,
            wire: |unknown_tagged_fields, version| {
                encode(
                    &FetchRequest362 {
                        unknown_tagged_fields,
                        ..FetchRequest362::default()
                    },
                    version,
                )
            },
            decode: decode_owned::<FetchRequest362>,
        },
        OutOfVersion {
            name: "FetchRequest.FetchPartition.HighWatermark (borrowed)",
            tag: 1,
            rejected: 12..=17,
            accepted: 18,
            payload: HIGH_WATERMARK,
            wire: |unknown_tagged_fields, version| {
                encode(
                    &FetchRequestBorrowed {
                        topics: vec![FetchTopicBorrowed {
                            partitions: vec![FetchPartitionBorrowed {
                                unknown_tagged_fields,
                                ..FetchPartitionBorrowed::default()
                            }],
                            ..FetchTopicBorrowed::default()
                        }],
                        ..FetchRequestBorrowed::default()
                    },
                    version,
                )
            },
            decode: |bytes, version| {
                let mut cursor = bytes;
                FetchRequestBorrowed::decode_borrow(&mut cursor, version)?;
                assert!(cursor.is_empty());
                Ok(())
            },
        },
        OutOfVersion {
            name: "ProduceResponse.NodeEndpoints (borrowed)",
            tag: 0,
            rejected: 9..=9,
            accepted: 10,
            payload: EMPTY_COMPACT_ARRAY,
            wire: |unknown_tagged_fields, version| {
                encode(
                    &ProduceResponseBorrowed {
                        unknown_tagged_fields,
                        ..ProduceResponseBorrowed::default()
                    },
                    version,
                )
            },
            decode: |bytes, version| {
                let mut cursor = bytes;
                ProduceResponseBorrowed::decode_borrow(&mut cursor, version)?;
                assert!(cursor.is_empty());
                Ok(())
            },
        },
    ]
}

/// Each case decodes cleanly at the first version that declares its tag, so
/// the payload is valid. At every flexible version below that one, the same
/// tag fails with Kafka's error.
#[test]
fn decode_rejects_out_of_version_tags() {
    for case in out_of_version_cases() {
        let accepted = (case.wire)(unknown(case.tag, case.payload), case.accepted);
        assert!(
            let Ok(()) = (case.decode)(&accepted, case.accepted),
            "{} must decode tag {} at v{}",
            case.name,
            case.tag,
            case.accepted
        );

        for version in case.rejected.clone() {
            let bytes = (case.wire)(unknown(case.tag, case.payload), version);
            assert!(
                let Err(error) = (case.decode)(&bytes, version),
                "{} must reject tag {} at v{version}",
                case.name,
                case.tag
            );
            assert!(
                let ProtocolError::TagNotValidForVersion { tag, version: got } = &error,
                "{} at v{version}",
                case.name
            );
            assert!((*tag, *got) == (case.tag, version), "{}", case.name);
            assert!(
                error.to_string() == format!("Tag {} is not valid for version {version}", case.tag),
                "{}",
                case.name
            );
        }
    }
}
