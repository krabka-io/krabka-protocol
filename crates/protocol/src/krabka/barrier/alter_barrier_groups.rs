//! `AlterBarrierGroups`, api key 1010.
//!
//! One request creates a barrier group, updates one, or deletes one. The
//! coordinator applies each entry on its own and reports a per-group result.
//!
//! An edit to the topic set or to the partition count of a topic applies from
//! the next epoch. The coordinator freezes the target set before it appends the
//! first marker of an epoch.

use bytes::{Buf, BufMut};

use crate::{
    Decode, Encode, ProtocolError, ProtocolRequest, UnknownTaggedFields,
    krabka::common::{
        BOOL_LEN, I16_LEN, I32_LEN, I64_LEN, array_len, check_version, decode_array,
        decode_string_array, encode_array, encode_string_array, read_unknown_tagged_fields,
        string_array_len, unknown_tagged_fields_len, write_unknown_tagged_fields,
    },
    primitives::{
        fixed::{get_bool, get_i16, get_i32, get_i64, put_bool, put_i16, put_i32, put_i64},
        string_bytes::{
            compact_nullable_string_len, compact_string_len, get_compact_nullable_string_owned,
            get_compact_string_owned, put_compact_nullable_string, put_compact_string,
        },
    },
};

/// Api key of `AlterBarrierGroups`.
pub const API_KEY: i16 = 1010;
/// Lowest version of `AlterBarrierGroups` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `AlterBarrierGroups` that this build speaks.
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

/// One create, update, or delete of a barrier group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterableBarrierGroup {
    /// Name of the barrier group.
    pub group: String,
    /// Topics that the group covers. The coordinator injects a marker into
    /// every partition of every topic named here.
    pub topics: Vec<String>,
    /// Period between two automatic injections, in milliseconds. `-1` turns off
    /// periodic injection, and only a `TriggerBarrier` request then makes a cut.
    pub interval_ms: i64,
    /// Number of published cuts that the coordinator keeps for this group.
    pub retained_cuts: i32,
    /// `true` deletes the group. The coordinator then ignores `topics`,
    /// `interval_ms`, and `retained_cuts`.
    pub delete: bool,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Default for AlterableBarrierGroup {
    fn default() -> Self {
        Self {
            group: String::new(),
            topics: Vec::new(),
            interval_ms: -1,
            retained_cuts: 0,
            delete: false,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }
}

impl Encode for AlterableBarrierGroup {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
        put_compact_string(buf, &self.group);
        encode_string_array(buf, &self.topics);
        put_i64(buf, self.interval_ms);
        put_i32(buf, self.retained_cuts);
        put_bool(buf, self.delete);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_string_len(&self.group)
            + string_array_len(&self.topics)
            + I64_LEN
            + I32_LEN
            + BOOL_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for AlterableBarrierGroup {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            group: get_compact_string_owned(buf)?,
            topics: decode_string_array(buf)?,
            interval_ms: get_i64(buf)?,
            retained_cuts: get_i32(buf)?,
            delete: get_bool(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// Creates, updates, or deletes barrier groups.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AlterBarrierGroupsRequest {
    /// One entry for each group to create, update, or delete.
    pub groups: Vec<AlterableBarrierGroup>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for AlterBarrierGroupsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        encode_array(buf, &self.groups, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        array_len(&self.groups, version) + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for AlterBarrierGroupsRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            groups: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for AlterBarrierGroupsRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = AlterBarrierGroupsResponse;
}

/// Outcome of one entry of an [`AlterBarrierGroupsRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AlterBarrierGroupResult {
    /// Name of the barrier group that this result belongs to.
    pub group: String,
    /// Kafka error code. `0` means the coordinator applied the entry.
    pub error_code: i16,
    /// Text that explains a non-zero `error_code`. `None` when the coordinator
    /// has nothing to add to the code.
    pub error_message: Option<String>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for AlterBarrierGroupResult {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
        put_compact_string(buf, &self.group);
        put_i16(buf, self.error_code);
        put_compact_nullable_string(buf, self.error_message.as_deref());
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_string_len(&self.group)
            + I16_LEN
            + compact_nullable_string_len(self.error_message.as_deref())
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for AlterBarrierGroupResult {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            group: get_compact_string_owned(buf)?,
            error_code: get_i16(buf)?,
            error_message: get_compact_nullable_string_owned(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// Per-group outcome of an [`AlterBarrierGroupsRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AlterBarrierGroupsResponse {
    /// Milliseconds that the broker held the request back for a quota.
    pub throttle_time_ms: i32,
    /// One result for each entry of the request, in request order.
    pub results: Vec<AlterBarrierGroupResult>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for AlterBarrierGroupsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i32(buf, self.throttle_time_ms);
        encode_array(buf, &self.results, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        I32_LEN
            + array_len(&self.results, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for AlterBarrierGroupsResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            throttle_time_ms: get_i32(buf)?,
            results: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl AlterBarrierGroupsRequest {
    /// Builds a request with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            groups: vec![
                AlterableBarrierGroup {
                    group: "orders".to_string(),
                    topics: vec!["orders".to_string(), "payments".to_string()],
                    interval_ms: 60_000,
                    retained_cuts: 32,
                    delete: false,
                    unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
                },
                AlterableBarrierGroup {
                    group: "retired".to_string(),
                    topics: Vec::new(),
                    interval_ms: -1,
                    retained_cuts: 0,
                    delete: true,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl AlterBarrierGroupsResponse {
    /// Builds a response with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            throttle_time_ms: 17,
            results: vec![
                AlterBarrierGroupResult {
                    group: "orders".to_string(),
                    error_code: 0,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                AlterBarrierGroupResult {
                    group: "retired".to_string(),
                    error_code: 69,
                    error_message: Some("barrier group not found".to_string()),
                    unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
                },
            ],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::krabka::test_support::{roundtrip, unsupported_versions};

    #[test]
    fn default_roundtrips_all_versions() {
        for version in MIN_VERSION..=MAX_VERSION {
            roundtrip(&AlterBarrierGroupsRequest::default(), version);
            roundtrip(&AlterBarrierGroupsResponse::default(), version);
        }
    }

    #[test]
    fn populated_roundtrips_all_versions() {
        for version in MIN_VERSION..=MAX_VERSION {
            roundtrip(&AlterBarrierGroupsRequest::populated(), version);
            roundtrip(&AlterBarrierGroupsResponse::populated(), version);
        }
    }

    #[test]
    fn interval_of_minus_one_is_the_default() {
        assert!(AlterableBarrierGroup::default().interval_ms == -1);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let request = AlterBarrierGroupsRequest::populated();
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
