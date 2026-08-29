//! `DescribeTopicFreezes`, api key 1016.
//!
//! The request reads the write-freeze registry. An empty `scope_filter` asks
//! for every entry.
//!
//! Each entry carries the `key_id` and the `signature` that
//! [`SetTopicFreeze`](super::set_topic_freeze) put in the metadata log. The
//! response gives them back on purpose. An operator tool re-verifies each
//! signature against the operator public keys on its own machine, so the
//! broker's word is not the only evidence of who set a freeze.

use bytes::{Buf, BufMut};

use super::{PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL};
use crate::{
    Decode, Encode, ProtocolError, ProtocolRequest, UnknownTaggedFields,
    krabka::common::{
        I8_LEN, I16_LEN, I32_LEN, I64_LEN, UUID_LEN, array_len, check_version, decode_array,
        encode_array, read_unknown_tagged_fields, unknown_tagged_fields_len,
        write_unknown_tagged_fields,
    },
    primitives::{
        fixed::{get_i8, get_i16, get_i32, get_i64, put_i8, put_i16, put_i32, put_i64},
        string_bytes::{
            compact_bytes_len, compact_nullable_string_len, compact_string_len,
            get_compact_bytes_owned, get_compact_nullable_string_owned, get_compact_string_owned,
            put_compact_bytes, put_compact_nullable_string, put_compact_string,
        },
        uuid::{Uuid, get_uuid, put_uuid},
    },
};

/// Api key of `DescribeTopicFreezes`.
pub const API_KEY: i16 = 1016;
/// Lowest version of `DescribeTopicFreezes` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `DescribeTopicFreezes` that this build speaks.
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

/// Reads the write-freeze registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicFreezesRequest {
    /// The scope to read. `None` asks for every entry. A value matches the
    /// `scope` of an entry exactly, and it is not itself a prefix match.
    pub scope_filter: Option<String>,
    /// [`PATTERN_TYPE_LITERAL`] or
    /// [`PATTERN_TYPE_PREFIXED`](super::PATTERN_TYPE_PREFIXED) reads entries of
    /// that pattern type alone. [`PATTERN_TYPE_ANY`] and `0` both read every
    /// pattern type.
    pub pattern_type_filter: i8,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Default for DescribeTopicFreezesRequest {
    fn default() -> Self {
        Self {
            scope_filter: None,
            pattern_type_filter: PATTERN_TYPE_ANY,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }
}

impl Encode for DescribeTopicFreezesRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_compact_nullable_string(buf, self.scope_filter.as_deref());
        put_i8(buf, self.pattern_type_filter);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_nullable_string_len(self.scope_filter.as_deref())
            + I8_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for DescribeTopicFreezesRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            scope_filter: get_compact_nullable_string_owned(buf)?,
            pattern_type_filter: get_i8(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for DescribeTopicFreezesRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = DescribeTopicFreezesResponse;
}

/// One live entry of the write-freeze registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopicFreeze {
    /// A literal topic name, or a topic-name prefix. `pattern_type` says which.
    pub scope: String,
    /// [`PATTERN_TYPE_LITERAL`] or
    /// [`PATTERN_TYPE_PREFIXED`](super::PATTERN_TYPE_PREFIXED).
    pub pattern_type: i8,
    /// Text that says why the operator set the freeze.
    pub reason: String,
    /// The principal that the broker authenticated when it took the request.
    pub set_by: String,
    /// Milliseconds since the Unix epoch, at the moment the operator signed the
    /// request that set this entry.
    pub set_at_ms: i64,
    /// The break-glass proposal that approved this entry. It is nil for a
    /// freeze, because a freeze needs no approval.
    pub proposal_id: Uuid,
    /// The operator key that made `signature`. It is empty for an entry that an
    /// unsigned request set.
    pub key_id: String,
    /// Detached Ed25519 signature over the canonical bytes of the freeze. It is
    /// empty for an entry that an unsigned request set. A tool re-verifies it
    /// locally, and an empty signature is an attestation and not a proof.
    pub signature: Vec<u8>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Default for DescribedTopicFreeze {
    fn default() -> Self {
        Self {
            scope: String::new(),
            pattern_type: PATTERN_TYPE_LITERAL,
            reason: String::new(),
            set_by: String::new(),
            set_at_ms: 0,
            proposal_id: Uuid::ZERO,
            key_id: String::new(),
            signature: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }
}

impl Encode for DescribedTopicFreeze {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
        put_compact_string(buf, &self.scope);
        put_i8(buf, self.pattern_type);
        put_compact_string(buf, &self.reason);
        put_compact_string(buf, &self.set_by);
        put_i64(buf, self.set_at_ms);
        put_uuid(buf, self.proposal_id);
        put_compact_string(buf, &self.key_id);
        put_compact_bytes(buf, &self.signature);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_string_len(&self.scope)
            + I8_LEN
            + compact_string_len(&self.reason)
            + compact_string_len(&self.set_by)
            + I64_LEN
            + UUID_LEN
            + compact_string_len(&self.key_id)
            + compact_bytes_len(&self.signature)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for DescribedTopicFreeze {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            scope: get_compact_string_owned(buf)?,
            pattern_type: get_i8(buf)?,
            reason: get_compact_string_owned(buf)?,
            set_by: get_compact_string_owned(buf)?,
            set_at_ms: get_i64(buf)?,
            proposal_id: get_uuid(buf)?,
            key_id: get_compact_string_owned(buf)?,
            signature: get_compact_bytes_owned(buf)?.to_vec(),
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// The registry entries that a [`DescribeTopicFreezesRequest`] asked for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DescribeTopicFreezesResponse {
    /// Milliseconds that the broker held the request back for a quota.
    pub throttle_time_ms: i32,
    /// Kafka error code. `0` means `freezes` holds the answer.
    pub error_code: i16,
    /// Text that explains a non-zero `error_code`. `None` when the broker has
    /// nothing to add to the code.
    pub error_message: Option<String>,
    /// One entry for each freeze that the filter matched.
    pub freezes: Vec<DescribedTopicFreeze>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for DescribeTopicFreezesResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i32(buf, self.throttle_time_ms);
        put_i16(buf, self.error_code);
        put_compact_nullable_string(buf, self.error_message.as_deref());
        encode_array(buf, &self.freezes, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        I32_LEN
            + I16_LEN
            + compact_nullable_string_len(self.error_message.as_deref())
            + array_len(&self.freezes, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for DescribeTopicFreezesResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            throttle_time_ms: get_i32(buf)?,
            error_code: get_i16(buf)?,
            error_message: get_compact_nullable_string_owned(buf)?,
            freezes: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl DescribeTopicFreezesRequest {
    /// Builds a request with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            scope_filter: Some("tenant-a.".to_string()),
            pattern_type_filter: super::PATTERN_TYPE_PREFIXED,
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl DescribeTopicFreezesResponse {
    /// Builds a response with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            throttle_time_ms: 3,
            error_code: 0,
            error_message: None,
            freezes: vec![
                DescribedTopicFreeze {
                    scope: "orders".to_string(),
                    pattern_type: PATTERN_TYPE_LITERAL,
                    reason: "DR cutover".to_string(),
                    set_by: "User:alice".to_string(),
                    set_at_ms: 1_770_000_000_000,
                    proposal_id: Uuid::ZERO,
                    key_id: "alice-yubi".to_string(),
                    signature: (0..64).collect(),
                    unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
                },
                DescribedTopicFreeze {
                    scope: "tenant-a.".to_string(),
                    pattern_type: super::PATTERN_TYPE_PREFIXED,
                    reason: "tenant offboarding".to_string(),
                    set_by: "User:bob".to_string(),
                    set_at_ms: 1_770_000_100_000,
                    ..DescribedTopicFreeze::default()
                },
            ],
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::krabka::{
        freeze::PATTERN_TYPE_PREFIXED,
        test_support::{reject_truncated, roundtrip_case, unsupported_versions},
    };

    #[test]
    fn requests_roundtrip() {
        let cases = [
            (
                "default asks for every entry",
                DescribeTopicFreezesRequest::default(),
            ),
            (
                "prefixed scope filter",
                DescribeTopicFreezesRequest::populated(),
            ),
            (
                "literal scope filter",
                DescribeTopicFreezesRequest {
                    scope_filter: Some("orders".to_string()),
                    pattern_type_filter: PATTERN_TYPE_LITERAL,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ),
            (
                "unknown pattern type reads every entry",
                DescribeTopicFreezesRequest {
                    scope_filter: None,
                    pattern_type_filter: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ),
        ];
        for (label, request) in &cases {
            for version in MIN_VERSION..=MAX_VERSION {
                roundtrip_case(label, request, version);
            }
        }
    }

    #[test]
    fn responses_roundtrip() {
        let cases = [
            ("default", DescribeTopicFreezesResponse::default()),
            (
                "signed and unsigned entries",
                DescribeTopicFreezesResponse::populated(),
            ),
            (
                "refusal with no entries",
                DescribeTopicFreezesResponse {
                    throttle_time_ms: 11,
                    error_code: 31,
                    error_message: Some("cluster authorization failed".to_string()),
                    freezes: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ),
        ];
        for (label, response) in &cases {
            for version in MIN_VERSION..=MAX_VERSION {
                roundtrip_case(label, response, version);
            }
        }
    }

    #[test]
    fn no_filter_is_the_request_default() {
        assert!(
            DescribeTopicFreezesRequest::default()
                == DescribeTopicFreezesRequest {
                    scope_filter: None,
                    pattern_type_filter: PATTERN_TYPE_ANY,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
        );
    }

    #[test]
    fn an_unsigned_entry_carries_an_empty_signature() {
        let response = DescribeTopicFreezesResponse::populated();
        let unsigned = &response.freezes[1];
        assert!(unsigned.key_id.is_empty());
        assert!(unsigned.signature.is_empty());
        assert!(unsigned.pattern_type == PATTERN_TYPE_PREFIXED);
    }

    #[test]
    fn truncated_input_is_rejected() {
        reject_truncated(&DescribeTopicFreezesRequest::populated(), MIN_VERSION);
        reject_truncated(&DescribeTopicFreezesResponse::populated(), MIN_VERSION);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let request = DescribeTopicFreezesRequest::populated();
            assert!(matches!(
                request.encode(&mut Vec::<u8>::new(), version),
                Err(ProtocolError::UnsupportedVersion {
                    api_key: API_KEY,
                    ..
                })
            ));
            let response = DescribeTopicFreezesResponse::populated();
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
