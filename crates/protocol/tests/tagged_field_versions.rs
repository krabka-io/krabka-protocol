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

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{
    Encode,
    borrowed::fetch_request::{
        FetchRequest as FetchRequestBorrowed, ReplicaState as ReplicaStateBorrowed,
    },
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic, ReplicaState},
        produce_response::{NodeEndpoint, ProduceResponse},
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
