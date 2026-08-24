//! `TriggerBarrier`, api key 1012.
//!
//! The request asks the coordinator to make a cut now. The coordinator picks the
//! next epoch, freezes the target set, and injects a marker into every partition
//! of the group. The response carries the offsets that those markers took.
//!
//! The coordinator consumes the epoch even when an injection misses a partition,
//! and it never reuses an epoch. The response then reports
//! [`CUT_STATUS_PARTIAL`](crate::krabka::barrier::CUT_STATUS_PARTIAL) and names
//! the partitions that got no marker.

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

/// Api key of `TriggerBarrier`.
pub const API_KEY: i16 = 1012;
/// Lowest version of `TriggerBarrier` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `TriggerBarrier` that this build speaks.
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

/// Asks the coordinator for a cut of one barrier group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TriggerBarrierRequest {
    /// Name of the barrier group to cut.
    pub group: String,
    /// Milliseconds that the coordinator waits for every partition to take a
    /// marker. After the deadline it publishes a partial cut.
    pub timeout_ms: i32,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for TriggerBarrierRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_compact_string(buf, &self.group);
        put_i32(buf, self.timeout_ms);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_string_len(&self.group)
            + I32_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for TriggerBarrierRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            group: get_compact_string_owned(buf)?,
            timeout_ms: get_i32(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for TriggerBarrierRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = TriggerBarrierResponse;
}

/// The cut that a [`TriggerBarrierRequest`] made.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TriggerBarrierResponse {
    /// Milliseconds that the broker held the request back for a quota.
    pub throttle_time_ms: i32,
    /// Kafka error code. `0` means the coordinator ran the injection, and the
    /// cut fields below hold its result.
    pub error_code: i16,
    /// Text that explains a non-zero `error_code`. `None` when the coordinator
    /// has nothing to add to the code.
    pub error_message: Option<String>,
    /// Epoch that the coordinator assigned to this cut. The coordinator never
    /// reuses an epoch, and a partial cut consumes one as a complete cut does.
    pub epoch: i64,
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

impl Encode for TriggerBarrierResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i32(buf, self.throttle_time_ms);
        put_i16(buf, self.error_code);
        put_compact_nullable_string(buf, self.error_message.as_deref());
        put_i64(buf, self.epoch);
        put_i8(buf, self.status);
        encode_array(buf, &self.topics, version)?;
        encode_array(buf, &self.missing, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        I32_LEN
            + I16_LEN
            + compact_nullable_string_len(self.error_message.as_deref())
            + I64_LEN
            + I8_LEN
            + array_len(&self.topics, version)
            + array_len(&self.missing, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for TriggerBarrierResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            throttle_time_ms: get_i32(buf)?,
            error_code: get_i16(buf)?,
            error_message: get_compact_nullable_string_owned(buf)?,
            epoch: get_i64(buf)?,
            status: get_i8(buf)?,
            topics: decode_array(buf, version)?,
            missing: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl TriggerBarrierRequest {
    /// Builds a request with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            group: "orders".to_string(),
            timeout_ms: 30_000,
            unknown_tagged_fields: super::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl TriggerBarrierResponse {
    /// Builds a partial-cut response with every field set, for the round-trip
    /// tests.
    fn populated() -> Self {
        Self {
            throttle_time_ms: 3,
            error_code: 0,
            error_message: Some("one partition had no leader".to_string()),
            epoch: 41,
            status: crate::krabka::barrier::CUT_STATUS_PARTIAL,
            topics: super::test_support::sample_cut_topics(),
            missing: super::test_support::sample_missing_partitions(),
            unknown_tagged_fields: super::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::krabka::barrier::{
        CUT_STATUS_COMPLETE, CUT_STATUS_PARTIAL,
        test_support::{roundtrip, sample_cut_topics, unsupported_versions},
    };

    #[test]
    fn default_roundtrips_all_versions() {
        for version in MIN_VERSION..=MAX_VERSION {
            roundtrip(&TriggerBarrierRequest::default(), version);
            roundtrip(&TriggerBarrierResponse::default(), version);
        }
    }

    #[test]
    fn populated_roundtrips_all_versions() {
        for version in MIN_VERSION..=MAX_VERSION {
            roundtrip(&TriggerBarrierRequest::populated(), version);
            roundtrip(&TriggerBarrierResponse::populated(), version);
        }
    }

    #[test]
    fn complete_cut_carries_no_missing_partitions() {
        let response = TriggerBarrierResponse {
            epoch: 7,
            status: CUT_STATUS_COMPLETE,
            topics: sample_cut_topics(),
            ..TriggerBarrierResponse::default()
        };
        roundtrip(&response, MIN_VERSION);
        assert!(response.missing.is_empty());
    }

    #[test]
    fn status_codes_are_distinct() {
        assert!(CUT_STATUS_COMPLETE != CUT_STATUS_PARTIAL);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let request = TriggerBarrierRequest::populated();
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
