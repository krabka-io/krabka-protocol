//! `DescribeBarrierGroups`, api key 1011.
//!
//! The request reads the definition of one or more barrier groups. An empty
//! `groups` array asks for every group that the coordinator holds.

use bytes::{Buf, BufMut};

use crate::{
    Decode, Encode, ProtocolError, ProtocolRequest, UnknownTaggedFields,
    krabka::common::{
        I16_LEN, I32_LEN, I64_LEN, array_len, check_version, decode_array, decode_string_array,
        encode_array, encode_string_array, read_unknown_tagged_fields, string_array_len,
        unknown_tagged_fields_len, write_unknown_tagged_fields,
    },
    primitives::{
        fixed::{get_i16, get_i32, get_i64, put_i16, put_i32, put_i64},
        string_bytes::{
            compact_nullable_string_len, compact_string_len, get_compact_nullable_string_owned,
            get_compact_string_owned, put_compact_nullable_string, put_compact_string,
        },
    },
};

/// Api key of `DescribeBarrierGroups`.
pub const API_KEY: i16 = 1011;
/// Lowest version of `DescribeBarrierGroups` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `DescribeBarrierGroups` that this build speaks.
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

/// Reads the definition of one or more barrier groups.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DescribeBarrierGroupsRequest {
    /// Names of the groups to describe. An empty array asks for every group.
    pub groups: Vec<String>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for DescribeBarrierGroupsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        encode_string_array(buf, &self.groups);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        string_array_len(&self.groups) + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for DescribeBarrierGroupsRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            groups: decode_string_array(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for DescribeBarrierGroupsRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = DescribeBarrierGroupsResponse;
}

/// The definition of one barrier group, or the error that hid it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedBarrierGroup {
    /// Name of the barrier group.
    pub group: String,
    /// Kafka error code. `0` means the rest of the fields describe the group.
    pub error_code: i16,
    /// Text that explains a non-zero `error_code`. `None` when the coordinator
    /// has nothing to add to the code.
    pub error_message: Option<String>,
    /// Topics that the group covers.
    pub topics: Vec<String>,
    /// Period between two automatic injections, in milliseconds. `-1` means
    /// that periodic injection is off for this group.
    pub interval_ms: i64,
    /// Number of published cuts that the coordinator keeps for this group.
    pub retained_cuts: i32,
    /// Epoch of the last cut that the coordinator started for this group.
    /// `-1` means that the group has no cut yet.
    pub last_epoch: i64,
    /// Node id of the broker that coordinates this group. `-1` means that no
    /// broker owns the group right now.
    pub coordinator_id: i32,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Default for DescribedBarrierGroup {
    fn default() -> Self {
        Self {
            group: String::new(),
            error_code: 0,
            error_message: None,
            topics: Vec::new(),
            interval_ms: -1,
            retained_cuts: 0,
            last_epoch: -1,
            coordinator_id: -1,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }
}

impl Encode for DescribedBarrierGroup {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
        put_compact_string(buf, &self.group);
        put_i16(buf, self.error_code);
        put_compact_nullable_string(buf, self.error_message.as_deref());
        encode_string_array(buf, &self.topics);
        put_i64(buf, self.interval_ms);
        put_i32(buf, self.retained_cuts);
        put_i64(buf, self.last_epoch);
        put_i32(buf, self.coordinator_id);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_string_len(&self.group)
            + I16_LEN
            + compact_nullable_string_len(self.error_message.as_deref())
            + string_array_len(&self.topics)
            + I64_LEN
            + I32_LEN
            + I64_LEN
            + I32_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for DescribedBarrierGroup {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            group: get_compact_string_owned(buf)?,
            error_code: get_i16(buf)?,
            error_message: get_compact_nullable_string_owned(buf)?,
            topics: decode_string_array(buf)?,
            interval_ms: get_i64(buf)?,
            retained_cuts: get_i32(buf)?,
            last_epoch: get_i64(buf)?,
            coordinator_id: get_i32(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// The definitions that a [`DescribeBarrierGroupsRequest`] asked for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DescribeBarrierGroupsResponse {
    /// Milliseconds that the broker held the request back for a quota.
    pub throttle_time_ms: i32,
    /// One entry for each group that the request named, or for every group when
    /// the request named none.
    pub groups: Vec<DescribedBarrierGroup>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for DescribeBarrierGroupsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i32(buf, self.throttle_time_ms);
        encode_array(buf, &self.groups, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        I32_LEN
            + array_len(&self.groups, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for DescribeBarrierGroupsResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            throttle_time_ms: get_i32(buf)?,
            groups: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl DescribeBarrierGroupsRequest {
    /// Builds a request with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            groups: vec!["orders".to_string(), "audit".to_string()],
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl DescribeBarrierGroupsResponse {
    /// Builds a response with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            throttle_time_ms: 5,
            groups: vec![
                DescribedBarrierGroup {
                    group: "orders".to_string(),
                    error_code: 0,
                    error_message: None,
                    topics: vec!["orders".to_string(), "payments".to_string()],
                    interval_ms: 30_000,
                    retained_cuts: 64,
                    last_epoch: 12,
                    coordinator_id: 3,
                    unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
                },
                DescribedBarrierGroup {
                    group: "audit".to_string(),
                    error_code: 69,
                    error_message: Some("barrier group not found".to_string()),
                    ..DescribedBarrierGroup::default()
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
            roundtrip(&DescribeBarrierGroupsRequest::default(), version);
            roundtrip(&DescribeBarrierGroupsResponse::default(), version);
        }
    }

    #[test]
    fn populated_roundtrips_all_versions() {
        for version in MIN_VERSION..=MAX_VERSION {
            roundtrip(&DescribeBarrierGroupsRequest::populated(), version);
            roundtrip(&DescribeBarrierGroupsResponse::populated(), version);
        }
    }

    #[test]
    fn empty_group_list_asks_for_every_group() {
        let request = DescribeBarrierGroupsRequest::default();
        assert!(request.groups.is_empty());
        roundtrip(&request, MIN_VERSION);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let response = DescribeBarrierGroupsResponse::populated();
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
