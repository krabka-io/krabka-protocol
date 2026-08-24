//! Wire round-trips for the krabka-private barrier control-plane messages.
//!
//! Each property holds three claims for a randomly built message: `encoded_len`
//! predicts the exact byte count, a decode consumes every byte, and the decoded
//! value equals the original.

use bytes::{Bytes, BytesMut};
use crabka_protocol::{
    Decode, Encode, UnknownTaggedField, UnknownTaggedFields,
    krabka::barrier::{
        AlterBarrierGroupResult, AlterBarrierGroupsRequest, AlterBarrierGroupsResponse,
        AlterableBarrierGroup, BarrierCut, BarrierCutPartition, BarrierCutTopic,
        BarrierMissingPartition, DescribeBarrierGroupsRequest, DescribeBarrierGroupsResponse,
        DescribedBarrierGroup, ListBarrierCutsRequest, ListBarrierCutsResponse,
        TriggerBarrierRequest, TriggerBarrierResponse, WritableBarrierPartition,
        WritableBarrierTopic, WriteBarrierMarkersRequest, WriteBarrierMarkersResponse,
        WrittenBarrierPartition, WrittenBarrierTopic,
    },
};
use proptest::{prelude::*, test_runner::TestCaseError};

/// The only version of every barrier message.
const V0: i16 = 0;

fn check_roundtrip<T>(value: &T) -> Result<(), TestCaseError>
where
    T: Encode + for<'de> Decode<'de> + PartialEq + std::fmt::Debug,
{
    let mut buf = BytesMut::new();
    value.encode(&mut buf, V0).unwrap();
    prop_assert_eq!(value.encoded_len(V0), buf.len());
    let mut cursor = &buf[..];
    let decoded = T::decode(&mut cursor, V0).unwrap();
    prop_assert!(cursor.is_empty());
    prop_assert_eq!(&decoded, value);
    Ok(())
}

fn arb_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9._-]{0,15}"
}

fn arb_names() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(arb_name(), 0..4)
}

fn arb_error_message() -> impl Strategy<Value = Option<String>> {
    prop::option::of("[ -~]{0,24}")
}

// Tags must be strictly ascending on the wire, so build them from a map.
fn arb_tagged_fields() -> impl Strategy<Value = UnknownTaggedFields> {
    prop::collection::btree_map(0u32..64, prop::collection::vec(any::<u8>(), 0..4), 0..3).prop_map(
        |entries| {
            UnknownTaggedFields(
                entries
                    .into_iter()
                    .map(|(tag, bytes)| UnknownTaggedField {
                        tag,
                        bytes: Bytes::from(bytes),
                    })
                    .collect(),
            )
        },
    )
}

fn arb_cut_partition() -> impl Strategy<Value = BarrierCutPartition> {
    (any::<i32>(), any::<i64>(), arb_tagged_fields()).prop_map(
        |(partition, offset, unknown_tagged_fields)| BarrierCutPartition {
            partition,
            offset,
            unknown_tagged_fields,
        },
    )
}

fn arb_cut_topics() -> impl Strategy<Value = Vec<BarrierCutTopic>> {
    let topic = (
        arb_name(),
        prop::collection::vec(arb_cut_partition(), 0..3),
        arb_tagged_fields(),
    )
        .prop_map(
            |(topic, partitions, unknown_tagged_fields)| BarrierCutTopic {
                topic,
                partitions,
                unknown_tagged_fields,
            },
        );
    prop::collection::vec(topic, 0..3)
}

fn arb_missing() -> impl Strategy<Value = Vec<BarrierMissingPartition>> {
    let missing = (arb_name(), any::<i32>(), arb_tagged_fields()).prop_map(
        |(topic, partition, unknown_tagged_fields)| BarrierMissingPartition {
            topic,
            partition,
            unknown_tagged_fields,
        },
    );
    prop::collection::vec(missing, 0..3)
}

fn arb_alter_request() -> impl Strategy<Value = AlterBarrierGroupsRequest> {
    let group = (
        arb_name(),
        arb_names(),
        any::<i64>(),
        any::<i32>(),
        any::<bool>(),
        arb_tagged_fields(),
    )
        .prop_map(
            |(group, topics, interval_ms, retained_cuts, delete, unknown_tagged_fields)| {
                AlterableBarrierGroup {
                    group,
                    topics,
                    interval_ms,
                    retained_cuts,
                    delete,
                    unknown_tagged_fields,
                }
            },
        );
    (prop::collection::vec(group, 0..4), arb_tagged_fields()).prop_map(
        |(groups, unknown_tagged_fields)| AlterBarrierGroupsRequest {
            groups,
            unknown_tagged_fields,
        },
    )
}

fn arb_alter_response() -> impl Strategy<Value = AlterBarrierGroupsResponse> {
    let result = (
        arb_name(),
        any::<i16>(),
        arb_error_message(),
        arb_tagged_fields(),
    )
        .prop_map(
            |(group, error_code, error_message, unknown_tagged_fields)| AlterBarrierGroupResult {
                group,
                error_code,
                error_message,
                unknown_tagged_fields,
            },
        );
    (
        any::<i32>(),
        prop::collection::vec(result, 0..4),
        arb_tagged_fields(),
    )
        .prop_map(|(throttle_time_ms, results, unknown_tagged_fields)| {
            AlterBarrierGroupsResponse {
                throttle_time_ms,
                results,
                unknown_tagged_fields,
            }
        })
}

fn arb_describe_request() -> impl Strategy<Value = DescribeBarrierGroupsRequest> {
    (arb_names(), arb_tagged_fields()).prop_map(|(groups, unknown_tagged_fields)| {
        DescribeBarrierGroupsRequest {
            groups,
            unknown_tagged_fields,
        }
    })
}

fn arb_describe_response() -> impl Strategy<Value = DescribeBarrierGroupsResponse> {
    let group = (
        arb_name(),
        any::<i16>(),
        arb_error_message(),
        arb_names(),
        any::<i64>(),
        any::<i32>(),
        any::<i64>(),
        any::<i32>(),
        arb_tagged_fields(),
    )
        .prop_map(
            |(
                group,
                error_code,
                error_message,
                topics,
                interval_ms,
                retained_cuts,
                last_epoch,
                coordinator_id,
                unknown_tagged_fields,
            )| DescribedBarrierGroup {
                group,
                error_code,
                error_message,
                topics,
                interval_ms,
                retained_cuts,
                last_epoch,
                coordinator_id,
                unknown_tagged_fields,
            },
        );
    (
        any::<i32>(),
        prop::collection::vec(group, 0..3),
        arb_tagged_fields(),
    )
        .prop_map(|(throttle_time_ms, groups, unknown_tagged_fields)| {
            DescribeBarrierGroupsResponse {
                throttle_time_ms,
                groups,
                unknown_tagged_fields,
            }
        })
}

fn arb_trigger_request() -> impl Strategy<Value = TriggerBarrierRequest> {
    (arb_name(), any::<i32>(), arb_tagged_fields()).prop_map(
        |(group, timeout_ms, unknown_tagged_fields)| TriggerBarrierRequest {
            group,
            timeout_ms,
            unknown_tagged_fields,
        },
    )
}

fn arb_trigger_response() -> impl Strategy<Value = TriggerBarrierResponse> {
    (
        any::<i32>(),
        any::<i16>(),
        arb_error_message(),
        any::<i64>(),
        any::<i8>(),
        arb_cut_topics(),
        arb_missing(),
        arb_tagged_fields(),
    )
        .prop_map(
            |(
                throttle_time_ms,
                error_code,
                error_message,
                epoch,
                status,
                topics,
                missing,
                unknown_tagged_fields,
            )| TriggerBarrierResponse {
                throttle_time_ms,
                error_code,
                error_message,
                epoch,
                status,
                topics,
                missing,
                unknown_tagged_fields,
            },
        )
}

fn arb_list_request() -> impl Strategy<Value = ListBarrierCutsRequest> {
    (arb_name(), any::<i64>(), any::<i32>(), arb_tagged_fields()).prop_map(
        |(group, from_epoch, max_results, unknown_tagged_fields)| ListBarrierCutsRequest {
            group,
            from_epoch,
            max_results,
            unknown_tagged_fields,
        },
    )
}

fn arb_list_response() -> impl Strategy<Value = ListBarrierCutsResponse> {
    let cut = (
        any::<i64>(),
        any::<i64>(),
        any::<i64>(),
        any::<i8>(),
        arb_cut_topics(),
        arb_missing(),
        arb_tagged_fields(),
    )
        .prop_map(
            |(
                epoch,
                triggered_at,
                completed_at,
                status,
                topics,
                missing,
                unknown_tagged_fields,
            )| BarrierCut {
                epoch,
                triggered_at,
                completed_at,
                status,
                topics,
                missing,
                unknown_tagged_fields,
            },
        );
    (
        any::<i32>(),
        any::<i16>(),
        arb_error_message(),
        prop::collection::vec(cut, 0..3),
        arb_tagged_fields(),
    )
        .prop_map(
            |(throttle_time_ms, error_code, error_message, cuts, unknown_tagged_fields)| {
                ListBarrierCutsResponse {
                    throttle_time_ms,
                    error_code,
                    error_message,
                    cuts,
                    unknown_tagged_fields,
                }
            },
        )
}

fn arb_writable_partition() -> impl Strategy<Value = WritableBarrierPartition> {
    (any::<i32>(), any::<i32>(), arb_tagged_fields()).prop_map(
        |(partition, expected_leader_epoch, unknown_tagged_fields)| WritableBarrierPartition {
            partition,
            expected_leader_epoch,
            unknown_tagged_fields,
        },
    )
}

fn arb_write_request() -> impl Strategy<Value = WriteBarrierMarkersRequest> {
    let topic = (
        arb_name(),
        prop::collection::vec(arb_writable_partition(), 0..4),
        arb_tagged_fields(),
    )
        .prop_map(
            |(topic, partitions, unknown_tagged_fields)| WritableBarrierTopic {
                topic,
                partitions,
                unknown_tagged_fields,
            },
        );
    (
        arb_name(),
        any::<i64>(),
        any::<i64>(),
        prop::collection::vec(topic, 0..3),
        arb_tagged_fields(),
    )
        .prop_map(
            |(group, epoch, triggered_at, topics, unknown_tagged_fields)| {
                WriteBarrierMarkersRequest {
                    group,
                    epoch,
                    triggered_at,
                    topics,
                    unknown_tagged_fields,
                }
            },
        )
}

fn arb_write_response() -> impl Strategy<Value = WriteBarrierMarkersResponse> {
    let partition = (
        any::<i32>(),
        any::<i16>(),
        any::<i64>(),
        arb_tagged_fields(),
    )
        .prop_map(|(partition, error_code, offset, unknown_tagged_fields)| {
            WrittenBarrierPartition {
                partition,
                error_code,
                offset,
                unknown_tagged_fields,
            }
        });
    let topic = (
        arb_name(),
        prop::collection::vec(partition, 0..3),
        arb_tagged_fields(),
    )
        .prop_map(
            |(topic, partitions, unknown_tagged_fields)| WrittenBarrierTopic {
                topic,
                partitions,
                unknown_tagged_fields,
            },
        );
    (prop::collection::vec(topic, 0..3), arb_tagged_fields()).prop_map(
        |(topics, unknown_tagged_fields)| WriteBarrierMarkersResponse {
            topics,
            unknown_tagged_fields,
        },
    )
}

proptest! {
    #[test]
    fn alter_barrier_groups_request_roundtrip(v in arb_alter_request()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn alter_barrier_groups_response_roundtrip(v in arb_alter_response()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn describe_barrier_groups_request_roundtrip(v in arb_describe_request()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn describe_barrier_groups_response_roundtrip(v in arb_describe_response()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn trigger_barrier_request_roundtrip(v in arb_trigger_request()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn trigger_barrier_response_roundtrip(v in arb_trigger_response()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn list_barrier_cuts_request_roundtrip(v in arb_list_request()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn list_barrier_cuts_response_roundtrip(v in arb_list_response()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn writable_barrier_partition_roundtrip(v in arb_writable_partition()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn write_barrier_markers_request_roundtrip(v in arb_write_request()) {
        check_roundtrip(&v)?;
    }

    #[test]
    fn write_barrier_markers_response_roundtrip(v in arb_write_response()) {
        check_roundtrip(&v)?;
    }
}
