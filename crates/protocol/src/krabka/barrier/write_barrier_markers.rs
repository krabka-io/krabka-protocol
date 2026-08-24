//! `WriteBarrierMarkers`, api key 1014.
//!
//! The coordinator sends this request to a broker that leads a partition of the
//! group. That broker appends one barrier marker to each named partition and
//! returns the offset that each marker took.
//!
//! This is inter-broker traffic. A client never sends it. The fan-out follows
//! the transaction-marker path, and it collects the returned offsets, which the
//! transaction path does not need.

use bytes::{Buf, BufMut};

use crate::{
    Decode, Encode, ProtocolError, ProtocolRequest, UnknownTaggedFields,
    krabka::barrier::common::{
        I16_LEN, I32_LEN, I64_LEN, array_len, check_version, decode_array, encode_array,
        read_unknown_tagged_fields, unknown_tagged_fields_len, write_unknown_tagged_fields,
    },
    primitives::{
        fixed::{get_i16, get_i32, get_i64, put_i16, put_i32, put_i64},
        string_bytes::{compact_string_len, get_compact_string_owned, put_compact_string},
    },
};

/// Api key of `WriteBarrierMarkers`.
pub const API_KEY: i16 = 1014;
/// Lowest version of `WriteBarrierMarkers` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `WriteBarrierMarkers` that this build speaks.
pub const MAX_VERSION: i16 = 0;
/// First version with flexible framing from KIP-482.
pub const FLEXIBLE_MIN: i16 = 0;

/// Reports whether `version` uses flexible framing from KIP-482.
///
/// The request header takes the same framing as the request body, so a caller
/// that writes the header needs this answer.
#[inline]
#[must_use]
pub fn is_flexible(version: i16) -> bool {
    version >= FLEXIBLE_MIN
}

/// One partition for the target broker to mark, and its expected leader epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableBarrierPartition {
    /// Partition index within the topic.
    pub partition: i32,
    /// Leader epoch that the coordinator read for this partition when it froze
    /// the target set. The target broker compares this epoch against its own
    /// leader epoch, and it rejects a mismatch with `FENCED_LEADER_EPOCH`. `-1`
    /// means the coordinator had no epoch for this partition, and the target
    /// broker does not fence on it.
    pub expected_leader_epoch: i32,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Default for WritableBarrierPartition {
    fn default() -> Self {
        Self {
            partition: 0,
            expected_leader_epoch: -1,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }
}

impl Encode for WritableBarrierPartition {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
        put_i32(buf, self.partition);
        put_i32(buf, self.expected_leader_epoch);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        I32_LEN + I32_LEN + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for WritableBarrierPartition {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            partition: get_i32(buf)?,
            expected_leader_epoch: get_i32(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// The partitions of one topic for the target broker to mark.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WritableBarrierTopic {
    /// Topic name.
    pub topic: String,
    /// One entry for each partition that the target broker leads.
    pub partitions: Vec<WritableBarrierPartition>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for WritableBarrierTopic {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        put_compact_string(buf, &self.topic);
        encode_array(buf, &self.partitions, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        compact_string_len(&self.topic)
            + array_len(&self.partitions, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for WritableBarrierTopic {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: get_compact_string_owned(buf)?,
            partitions: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// Asks a broker to append one barrier marker to each named partition.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WriteBarrierMarkersRequest {
    /// Name of the barrier group that the epoch belongs to.
    pub group: String,
    /// Epoch that the coordinator assigned to this cut. The marker carries it.
    pub epoch: i64,
    /// Milliseconds since the Unix epoch, at the moment the coordinator started
    /// the injection. The marker carries it.
    pub triggered_at: i64,
    /// Target partitions, grouped by topic.
    pub topics: Vec<WritableBarrierTopic>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for WriteBarrierMarkersRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_compact_string(buf, &self.group);
        put_i64(buf, self.epoch);
        put_i64(buf, self.triggered_at);
        encode_array(buf, &self.topics, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        compact_string_len(&self.group)
            + I64_LEN
            + I64_LEN
            + array_len(&self.topics, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for WriteBarrierMarkersRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            group: get_compact_string_owned(buf)?,
            epoch: get_i64(buf)?,
            triggered_at: get_i64(buf)?,
            topics: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for WriteBarrierMarkersRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = WriteBarrierMarkersResponse;
}

/// The outcome for one partition that the target broker tried to mark.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WrittenBarrierPartition {
    /// Partition index within the topic.
    pub partition: i32,
    /// Kafka error code. `0` means the append succeeded and `offset` holds the
    /// position of the marker.
    pub error_code: i16,
    /// Offset that the barrier marker took. The coordinator writes this offset
    /// into the cut for this partition.
    pub offset: i64,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for WrittenBarrierPartition {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
        put_i32(buf, self.partition);
        put_i16(buf, self.error_code);
        put_i64(buf, self.offset);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        I32_LEN + I16_LEN + I64_LEN + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for WrittenBarrierPartition {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            partition: get_i32(buf)?,
            error_code: get_i16(buf)?,
            offset: get_i64(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// The outcomes for the partitions of one topic.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WrittenBarrierTopic {
    /// Topic name.
    pub topic: String,
    /// One outcome for each partition that the request named for this topic.
    pub partitions: Vec<WrittenBarrierPartition>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for WrittenBarrierTopic {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        put_compact_string(buf, &self.topic);
        encode_array(buf, &self.partitions, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        compact_string_len(&self.topic)
            + array_len(&self.partitions, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for WrittenBarrierTopic {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: get_compact_string_owned(buf)?,
            partitions: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// The offset that each barrier marker took.
///
/// This response carries no throttle time. It is inter-broker traffic, and the
/// broker applies no client quota to it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WriteBarrierMarkersResponse {
    /// One entry for each topic that the request named, in request order.
    pub topics: Vec<WrittenBarrierTopic>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for WriteBarrierMarkersResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        encode_array(buf, &self.topics, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        array_len(&self.topics, version) + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for WriteBarrierMarkersResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            topics: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl WriteBarrierMarkersRequest {
    /// Builds a request with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            group: "orders".to_string(),
            epoch: 41,
            triggered_at: 1_724_500_000_000,
            topics: vec![
                WritableBarrierTopic {
                    topic: "orders".to_string(),
                    partitions: vec![
                        WritableBarrierPartition {
                            partition: 0,
                            expected_leader_epoch: 4,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                        WritableBarrierPartition {
                            partition: 3,
                            expected_leader_epoch: -1,
                            unknown_tagged_fields: super::test_support::sample_tagged_fields(),
                        },
                        WritableBarrierPartition {
                            partition: 7,
                            expected_leader_epoch: 17,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    ],
                    unknown_tagged_fields: super::test_support::sample_tagged_fields(),
                },
                WritableBarrierTopic {
                    topic: "payments".to_string(),
                    partitions: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: super::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl WriteBarrierMarkersResponse {
    /// Builds a response with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            topics: vec![WrittenBarrierTopic {
                topic: "orders".to_string(),
                partitions: vec![
                    WrittenBarrierPartition {
                        partition: 0,
                        error_code: 0,
                        offset: 9_412,
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    },
                    WrittenBarrierPartition {
                        partition: 3,
                        error_code: 6,
                        offset: -1,
                        unknown_tagged_fields: super::test_support::sample_tagged_fields(),
                    },
                ],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: super::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::krabka::barrier::test_support::{reject_truncated, roundtrip, unsupported_versions};

    #[test]
    fn default_roundtrips_all_versions() {
        for version in MIN_VERSION..=MAX_VERSION {
            roundtrip(&WriteBarrierMarkersRequest::default(), version);
            roundtrip(&WriteBarrierMarkersResponse::default(), version);
        }
    }

    #[test]
    fn populated_roundtrips_all_versions() {
        for version in MIN_VERSION..=MAX_VERSION {
            roundtrip(&WriteBarrierMarkersRequest::populated(), version);
            roundtrip(&WriteBarrierMarkersResponse::populated(), version);
        }
    }

    #[test]
    fn no_expected_leader_epoch_is_the_partition_default() {
        assert!(
            WritableBarrierPartition::default()
                == WritableBarrierPartition {
                    partition: 0,
                    expected_leader_epoch: -1,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
        );
    }

    #[test]
    fn truncated_input_is_rejected() {
        reject_truncated(&WriteBarrierMarkersRequest::populated(), MIN_VERSION);
        reject_truncated(&WriteBarrierMarkersResponse::populated(), MIN_VERSION);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let request = WriteBarrierMarkersRequest::populated();
            assert!(matches!(
                request.encode(&mut Vec::<u8>::new(), version),
                Err(ProtocolError::UnsupportedVersion {
                    api_key: API_KEY,
                    ..
                })
            ));
        }
    }
}
