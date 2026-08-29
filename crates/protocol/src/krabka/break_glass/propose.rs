//! `ProposeBreakGlass`, api key 1017.
//!
//! The request asks the controller to open a break-glass proposal. The
//! controller gives the proposal an id and an expiry, and it holds the proposal
//! in the metadata log until an operator consumes it, an operator withdraws it,
//! or it expires.
//!
//! A proposal names one action and one target. It carries no approval yet, so
//! the proposer sends an [`ApproveBreakGlass`](super::approve) request from a
//! second principal before the action can run.

use bytes::{Buf, BufMut};

use crate::{
    Decode, Encode, ProtocolError, ProtocolRequest, UnknownTaggedFields,
    krabka::common::{
        I8_LEN, I16_LEN, I32_LEN, I64_LEN, UUID_LEN, check_version, read_unknown_tagged_fields,
        unknown_tagged_fields_len, write_unknown_tagged_fields,
    },
    primitives::{
        fixed::{get_i8, get_i16, get_i32, get_i64, put_i8, put_i16, put_i32, put_i64},
        string_bytes::{
            compact_nullable_string_len, compact_string_len, get_compact_nullable_string_owned,
            get_compact_string_owned, put_compact_nullable_string, put_compact_string,
        },
        uuid::{Uuid, get_uuid, put_uuid},
    },
};

/// Api key of `ProposeBreakGlass`.
pub const API_KEY: i16 = 1017;
/// Lowest version of `ProposeBreakGlass` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `ProposeBreakGlass` that this build speaks.
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

/// Opens a break-glass proposal.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProposeBreakGlassRequest {
    /// The privileged action to gate. The value is the wire value of the
    /// broker's break-glass action, as [`super`] describes.
    pub action: i8,
    /// What the action applies to. The shape of the text follows the action: a
    /// topic name, a partition, a broker id, or a freeze scope.
    pub target: String,
    /// Text that says why the operator needs the action.
    pub reason: String,
    /// Lifetime of the proposal, in milliseconds. `0` asks for the lifetime
    /// that the broker holds in its configuration. The broker caps a longer
    /// value at that configured lifetime.
    pub ttl_ms: i64,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for ProposeBreakGlassRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i8(buf, self.action);
        put_compact_string(buf, &self.target);
        put_compact_string(buf, &self.reason);
        put_i64(buf, self.ttl_ms);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        I8_LEN
            + compact_string_len(&self.target)
            + compact_string_len(&self.reason)
            + I64_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for ProposeBreakGlassRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            action: get_i8(buf)?,
            target: get_compact_string_owned(buf)?,
            reason: get_compact_string_owned(buf)?,
            ttl_ms: get_i64(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for ProposeBreakGlassRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = ProposeBreakGlassResponse;
}

/// Outcome of a [`ProposeBreakGlassRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProposeBreakGlassResponse {
    /// Milliseconds that the broker held the request back for a quota.
    pub throttle_time_ms: i32,
    /// Kafka error code. `0` means the controller opened the proposal.
    pub error_code: i16,
    /// Text that explains a non-zero `error_code`. `None` when the broker has
    /// nothing to add to the code.
    pub error_message: Option<String>,
    /// Id of the new proposal. An approver names it, and so does the audit
    /// event of the action that consumes it. It is nil for a non-zero
    /// `error_code`.
    pub proposal_id: Uuid,
    /// Milliseconds since the Unix epoch, at the moment the proposal expires.
    /// It is `0` for a non-zero `error_code`.
    pub expires_at_ms: i64,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for ProposeBreakGlassResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i32(buf, self.throttle_time_ms);
        put_i16(buf, self.error_code);
        put_compact_nullable_string(buf, self.error_message.as_deref());
        put_uuid(buf, self.proposal_id);
        put_i64(buf, self.expires_at_ms);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        I32_LEN
            + I16_LEN
            + compact_nullable_string_len(self.error_message.as_deref())
            + UUID_LEN
            + I64_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for ProposeBreakGlassResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            throttle_time_ms: get_i32(buf)?,
            error_code: get_i16(buf)?,
            error_message: get_compact_nullable_string_owned(buf)?,
            proposal_id: get_uuid(buf)?,
            expires_at_ms: get_i64(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl ProposeBreakGlassRequest {
    /// Builds a request with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            action: 6,
            target: "doomed".to_string(),
            reason: "topic holds test data only".to_string(),
            ttl_ms: 1_800_000,
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl ProposeBreakGlassResponse {
    /// Builds a response with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            throttle_time_ms: 4,
            error_code: 0,
            error_message: None,
            proposal_id: Uuid([
                200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215,
            ]),
            expires_at_ms: 1_770_000_180_000,
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::krabka::test_support::{reject_truncated, roundtrip_case, unsupported_versions};

    #[test]
    fn requests_roundtrip() {
        let cases = [
            ("default", ProposeBreakGlassRequest::default()),
            ("delete a topic", ProposeBreakGlassRequest::populated()),
            (
                "configured lifetime and no reason",
                ProposeBreakGlassRequest {
                    action: 1,
                    target: "literal:orders".to_string(),
                    reason: String::new(),
                    ttl_ms: 0,
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
            ("default", ProposeBreakGlassResponse::default()),
            ("accepted proposal", ProposeBreakGlassResponse::populated()),
            (
                "refusal carries no proposal",
                ProposeBreakGlassResponse {
                    throttle_time_ms: 0,
                    error_code: 1008,
                    error_message: Some("principal is not an approver".to_string()),
                    proposal_id: Uuid::ZERO,
                    expires_at_ms: 0,
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
    fn a_default_request_asks_for_the_configured_lifetime() {
        assert!(ProposeBreakGlassRequest::default().ttl_ms == 0);
    }

    #[test]
    fn truncated_input_is_rejected() {
        reject_truncated(&ProposeBreakGlassRequest::populated(), MIN_VERSION);
        reject_truncated(&ProposeBreakGlassResponse::populated(), MIN_VERSION);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let request = ProposeBreakGlassRequest::populated();
            assert!(matches!(
                request.encode(&mut Vec::<u8>::new(), version),
                Err(ProtocolError::UnsupportedVersion {
                    api_key: API_KEY,
                    ..
                })
            ));
            let response = ProposeBreakGlassResponse::populated();
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
