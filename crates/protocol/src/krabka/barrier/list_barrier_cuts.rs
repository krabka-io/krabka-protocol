//! `ListBarrierCuts`, api key 1013.
//!
//! The request reads cuts that the coordinator published before. It is the RPC
//! path to the manifest. Any Kafka consumer can read the same cuts from the
//! `__barrier_state` topic, which is the portable path for a client in another
//! language.

use bytes::{Buf, BufMut};

use crate::{
    Decode, Encode, ProtocolError, ProtocolRequest, UnknownTaggedFields,
    krabka::barrier::common::{
        BarrierCutTopic, BarrierMissingPartition, I8_LEN, I16_LEN, I32_LEN, I64_LEN, array_len,
        check_version, decode_array, encode_array, read_unknown_tagged_fields,
        unknown_tagged_fields_len, write_unknown_tagged_fields,
    },
    primitives::{
        fixed::{get_i8, get_i16, get_i32, get_i64, put_i8, put_i16, put_i32, put_i64},
        string_bytes::{
            compact_nullable_string_len, compact_string_len, get_compact_nullable_string_owned,
            get_compact_string_owned, put_compact_nullable_string, put_compact_string,
        },
    },
};

/// Api key of `ListBarrierCuts`.
pub const API_KEY: i16 = 1013;
/// Lowest version of `ListBarrierCuts` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `ListBarrierCuts` that this build speaks.
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

/// Reads the published cuts of one barrier group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListBarrierCutsRequest {
    /// Name of the barrier group to read.
    pub group: String,
    /// Lowest epoch to return. The coordinator returns cuts in epoch order from
    /// this epoch upward, and it includes this epoch.
    pub from_epoch: i64,
    /// Largest number of cuts to return.
    pub max_results: i32,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for ListBarrierCutsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_compact_string(buf, &self.group);
        put_i64(buf, self.from_epoch);
        put_i32(buf, self.max_results);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_string_len(&self.group)
            + I64_LEN
            + I32_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for ListBarrierCutsRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            group: get_compact_string_owned(buf)?,
            from_epoch: get_i64(buf)?,
            max_results: get_i32(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for ListBarrierCutsRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = ListBarrierCutsResponse;
}

/// One published cut of a barrier group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BarrierCut {
    /// Epoch of this cut. The coordinator never reuses an epoch.
    pub epoch: i64,
    /// Milliseconds since the Unix epoch, at the moment the coordinator started
    /// the injection.
    pub triggered_at: i64,
    /// Milliseconds since the Unix epoch, at the moment the coordinator
    /// published the cut.
    pub completed_at: i64,
    /// [`CUT_STATUS_COMPLETE`](crate::krabka::barrier::CUT_STATUS_COMPLETE)
    /// when every partition took a marker, and
    /// [`CUT_STATUS_PARTIAL`](crate::krabka::barrier::CUT_STATUS_PARTIAL) when
    /// at least one partition did not.
    pub status: i8,
    /// Cut offsets, grouped by topic.
    pub topics: Vec<BarrierCutTopic>,
    /// Partitions of the frozen target set that took no marker. This array is
    /// empty for a complete cut.
    pub missing: Vec<BarrierMissingPartition>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for BarrierCut {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        put_i64(buf, self.epoch);
        put_i64(buf, self.triggered_at);
        put_i64(buf, self.completed_at);
        put_i8(buf, self.status);
        encode_array(buf, &self.topics, version)?;
        encode_array(buf, &self.missing, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        I64_LEN
            + I64_LEN
            + I64_LEN
            + I8_LEN
            + array_len(&self.topics, version)
            + array_len(&self.missing, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for BarrierCut {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            epoch: get_i64(buf)?,
            triggered_at: get_i64(buf)?,
            completed_at: get_i64(buf)?,
            status: get_i8(buf)?,
            topics: decode_array(buf, version)?,
            missing: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// The cuts that a [`ListBarrierCutsRequest`] asked for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListBarrierCutsResponse {
    /// Milliseconds that the broker held the request back for a quota.
    pub throttle_time_ms: i32,
    /// Kafka error code. `0` means `cuts` holds the answer.
    pub error_code: i16,
    /// Text that explains a non-zero `error_code`. `None` when the coordinator
    /// has nothing to add to the code.
    pub error_message: Option<String>,
    /// Cuts in epoch order, from the oldest epoch that the request asked for.
    pub cuts: Vec<BarrierCut>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for ListBarrierCutsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i32(buf, self.throttle_time_ms);
        put_i16(buf, self.error_code);
        put_compact_nullable_string(buf, self.error_message.as_deref());
        encode_array(buf, &self.cuts, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        I32_LEN
            + I16_LEN
            + compact_nullable_string_len(self.error_message.as_deref())
            + array_len(&self.cuts, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for ListBarrierCutsResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            throttle_time_ms: get_i32(buf)?,
            error_code: get_i16(buf)?,
            error_message: get_compact_nullable_string_owned(buf)?,
            cuts: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl ListBarrierCutsRequest {
    /// Builds a request with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            group: "orders".to_string(),
            from_epoch: 12,
            max_results: 100,
            unknown_tagged_fields: super::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl ListBarrierCutsResponse {
    /// Builds a response with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            throttle_time_ms: 9,
            error_code: 0,
            error_message: None,
            cuts: vec![
                BarrierCut {
                    epoch: 12,
                    triggered_at: 1_724_500_000_000,
                    completed_at: 1_724_500_000_120,
                    status: crate::krabka::barrier::CUT_STATUS_COMPLETE,
                    topics: super::test_support::sample_cut_topics(),
                    missing: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                BarrierCut {
                    epoch: 13,
                    triggered_at: 1_724_500_060_000,
                    completed_at: 1_724_500_090_000,
                    status: crate::krabka::barrier::CUT_STATUS_PARTIAL,
                    topics: super::test_support::sample_cut_topics(),
                    missing: super::test_support::sample_missing_partitions(),
                    unknown_tagged_fields: super::test_support::sample_tagged_fields(),
                },
            ],
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
            roundtrip(&ListBarrierCutsRequest::default(), version);
            roundtrip(&ListBarrierCutsResponse::default(), version);
        }
    }

    #[test]
    fn populated_roundtrips_all_versions() {
        for version in MIN_VERSION..=MAX_VERSION {
            roundtrip(&ListBarrierCutsRequest::populated(), version);
            roundtrip(&ListBarrierCutsResponse::populated(), version);
        }
    }

    #[test]
    fn truncated_input_is_rejected() {
        reject_truncated(&ListBarrierCutsResponse::populated(), MIN_VERSION);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let response = ListBarrierCutsResponse::populated();
            assert!(matches!(
                response.encode(&mut Vec::<u8>::new(), version),
                Err(ProtocolError::UnsupportedVersion {
                    api_key: API_KEY,
                    ..
                })
            ));
        }
    }
}
