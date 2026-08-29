//! `SetTopicFreeze`, api key 1015.
//!
//! One request sets a write freeze on a scope, or it removes one. A scope is a
//! literal topic name or a topic-name prefix. The controller writes the outcome
//! to the metadata log, so the freeze holds after a restart.
//!
//! A freeze refuses every produce to a topic that the scope covers. A thaw
//! gives the topic back to its producers. The two directions are not
//! symmetrical. A freeze is the safe direction and one command sets it. A thaw
//! is the dangerous direction, so it names the break-glass proposal that
//! approved it in `proposal_id`.
//!
//! # Signature
//!
//! `key_id` and `signature` carry a detached Ed25519 signature that the
//! operator makes on their own machine. The private key never reaches a broker.
//! Both fields are empty for an unsigned request. The broker decides whether it
//! accepts an unsigned request, and it always refuses an unsigned thaw.

use bytes::{Buf, BufMut};

use super::PATTERN_TYPE_LITERAL;
use crate::{
    Decode, Encode, ProtocolError, ProtocolRequest, UnknownTaggedFields,
    krabka::common::{
        BOOL_LEN, I8_LEN, I16_LEN, I32_LEN, I64_LEN, UUID_LEN, check_version,
        read_unknown_tagged_fields, unknown_tagged_fields_len, write_unknown_tagged_fields,
    },
    primitives::{
        fixed::{
            get_bool, get_i8, get_i16, get_i32, get_i64, put_bool, put_i8, put_i16, put_i32,
            put_i64,
        },
        string_bytes::{
            compact_bytes_len, compact_nullable_string_len, compact_string_len,
            get_compact_bytes_owned, get_compact_nullable_string_owned, get_compact_string_owned,
            put_compact_bytes, put_compact_nullable_string, put_compact_string,
        },
        uuid::{Uuid, get_uuid, put_uuid},
    },
};

/// Api key of `SetTopicFreeze`.
pub const API_KEY: i16 = 1015;
/// Lowest version of `SetTopicFreeze` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `SetTopicFreeze` that this build speaks.
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

/// Sets a write freeze on one scope, or removes one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetTopicFreezeRequest {
    /// A literal topic name, or a topic-name prefix. `pattern_type` says which.
    pub scope: String,
    /// [`PATTERN_TYPE_LITERAL`] or
    /// [`PATTERN_TYPE_PREFIXED`](super::PATTERN_TYPE_PREFIXED).
    pub pattern_type: i8,
    /// `true` sets the freeze. `false` removes it, and that is the thaw.
    pub frozen: bool,
    /// Text that says why the operator set or removed the freeze. The broker
    /// keeps it in the metadata log and in the audit event.
    pub reason: String,
    /// The break-glass proposal that approved a thaw. It is nil for a freeze,
    /// because a freeze needs no approval.
    pub proposal_id: Uuid,
    /// Milliseconds since the Unix epoch, at the moment the operator signed the
    /// request. The broker refuses a value outside its skew window, and it
    /// refuses a value that is not newer than the entry that this one replaces.
    pub set_at_ms: i64,
    /// The operator key that made `signature`. It is empty for an unsigned
    /// request.
    pub key_id: String,
    /// Detached Ed25519 signature over the canonical bytes of the freeze. It is
    /// empty for an unsigned request.
    pub signature: Vec<u8>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Default for SetTopicFreezeRequest {
    fn default() -> Self {
        Self {
            scope: String::new(),
            pattern_type: PATTERN_TYPE_LITERAL,
            frozen: true,
            reason: String::new(),
            proposal_id: Uuid::ZERO,
            set_at_ms: 0,
            key_id: String::new(),
            signature: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }
}

impl Encode for SetTopicFreezeRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_compact_string(buf, &self.scope);
        put_i8(buf, self.pattern_type);
        put_bool(buf, self.frozen);
        put_compact_string(buf, &self.reason);
        put_uuid(buf, self.proposal_id);
        put_i64(buf, self.set_at_ms);
        put_compact_string(buf, &self.key_id);
        put_compact_bytes(buf, &self.signature);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_string_len(&self.scope)
            + I8_LEN
            + BOOL_LEN
            + compact_string_len(&self.reason)
            + UUID_LEN
            + I64_LEN
            + compact_string_len(&self.key_id)
            + compact_bytes_len(&self.signature)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for SetTopicFreezeRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            scope: get_compact_string_owned(buf)?,
            pattern_type: get_i8(buf)?,
            frozen: get_bool(buf)?,
            reason: get_compact_string_owned(buf)?,
            proposal_id: get_uuid(buf)?,
            set_at_ms: get_i64(buf)?,
            key_id: get_compact_string_owned(buf)?,
            signature: get_compact_bytes_owned(buf)?.to_vec(),
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for SetTopicFreezeRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = SetTopicFreezeResponse;
}

/// Outcome of a [`SetTopicFreezeRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SetTopicFreezeResponse {
    /// Milliseconds that the broker held the request back for a quota.
    pub throttle_time_ms: i32,
    /// Kafka error code. `0` means the controller applied the request.
    pub error_code: i16,
    /// Text that explains a non-zero `error_code`. `None` when the broker has
    /// nothing to add to the code.
    pub error_message: Option<String>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for SetTopicFreezeResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i32(buf, self.throttle_time_ms);
        put_i16(buf, self.error_code);
        put_compact_nullable_string(buf, self.error_message.as_deref());
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        I32_LEN
            + I16_LEN
            + compact_nullable_string_len(self.error_message.as_deref())
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for SetTopicFreezeResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            throttle_time_ms: get_i32(buf)?,
            error_code: get_i16(buf)?,
            error_message: get_compact_nullable_string_owned(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl SetTopicFreezeRequest {
    /// Builds a signed freeze with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            scope: "orders".to_string(),
            pattern_type: PATTERN_TYPE_LITERAL,
            frozen: true,
            reason: "DR cutover".to_string(),
            proposal_id: Uuid::ZERO,
            set_at_ms: 1_770_000_000_000,
            key_id: "alice-yubi".to_string(),
            signature: (0..64).collect(),
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl SetTopicFreezeResponse {
    /// Builds a response with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            throttle_time_ms: 21,
            error_code: 1011,
            error_message: Some("freeze scope names an internal topic".to_string()),
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::krabka::{
        freeze::{PATTERN_TYPE_ANY, PATTERN_TYPE_PREFIXED},
        test_support::{reject_truncated, roundtrip, roundtrip_case, unsupported_versions},
    };

    #[test]
    fn requests_roundtrip() {
        let cases = [
            ("default", SetTopicFreezeRequest::default()),
            ("signed literal freeze", SetTopicFreezeRequest::populated()),
            (
                "unsigned prefixed freeze",
                SetTopicFreezeRequest {
                    scope: "tenant-a.".to_string(),
                    pattern_type: PATTERN_TYPE_PREFIXED,
                    reason: "tenant offboarding".to_string(),
                    ..SetTopicFreezeRequest::default()
                },
            ),
            (
                "signed thaw naming a proposal",
                SetTopicFreezeRequest {
                    frozen: false,
                    proposal_id: Uuid([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
                    ..SetTopicFreezeRequest::populated()
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
            ("default", SetTopicFreezeResponse::default()),
            (
                "refusal with a message",
                SetTopicFreezeResponse::populated(),
            ),
            (
                "accepted with no message",
                SetTopicFreezeResponse {
                    throttle_time_ms: 7,
                    error_code: 0,
                    error_message: None,
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
    fn default_is_an_unsigned_literal_freeze() {
        assert!(
            SetTopicFreezeRequest::default()
                == SetTopicFreezeRequest {
                    scope: String::new(),
                    pattern_type: PATTERN_TYPE_LITERAL,
                    frozen: true,
                    reason: String::new(),
                    proposal_id: Uuid::ZERO,
                    set_at_ms: 0,
                    key_id: String::new(),
                    signature: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
        );
    }

    #[test]
    fn pattern_types_match_the_kafka_acl_values() {
        assert!(
            (
                PATTERN_TYPE_ANY,
                PATTERN_TYPE_LITERAL,
                PATTERN_TYPE_PREFIXED
            ) == (1, 3, 4)
        );
    }

    #[test]
    fn truncated_input_is_rejected() {
        reject_truncated(&SetTopicFreezeRequest::populated(), MIN_VERSION);
        reject_truncated(&SetTopicFreezeResponse::populated(), MIN_VERSION);
    }

    #[test]
    fn signature_survives_a_roundtrip() {
        let request = SetTopicFreezeRequest::populated();
        assert!(request.signature.len() == 64);
        roundtrip(&request, MIN_VERSION);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let request = SetTopicFreezeRequest::populated();
            assert!(matches!(
                request.encode(&mut Vec::<u8>::new(), version),
                Err(ProtocolError::UnsupportedVersion {
                    api_key: API_KEY,
                    ..
                })
            ));
            let response = SetTopicFreezeResponse::populated();
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
