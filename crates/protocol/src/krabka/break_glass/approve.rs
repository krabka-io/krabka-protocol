//! `ApproveBreakGlass`, api key 1018.
//!
//! One request adds an approval to a break-glass proposal, or it withdraws the
//! proposal. `withdraw` picks between the two, which follows
//! [`AlterableBarrierGroup`](crate::krabka::barrier::AlterableBarrierGroup) and
//! its `delete` flag. One api key then serves both directions of the same
//! standing authorization, and the krabka-private range keeps a key free.
//!
//! The broker checks the approver against the authenticated principal of the
//! connection. It refuses the proposer, a principal that is not in the approver
//! set, and a principal that already approved this proposal.
//!
//! # Signature
//!
//! `key_id` and `signature` carry a detached Ed25519 signature that the
//! operator makes on their own machine. The private key never reaches a broker.
//! Both fields are empty for an unsigned approval. The broker demands a
//! signature for the actions that its configuration names.

use bytes::{Buf, BufMut};

use crate::{
    Decode, Encode, ProtocolError, ProtocolRequest, UnknownTaggedFields,
    krabka::common::{
        BOOL_LEN, I16_LEN, I32_LEN, UUID_LEN, check_version, read_unknown_tagged_fields,
        unknown_tagged_fields_len, write_unknown_tagged_fields,
    },
    primitives::{
        fixed::{get_bool, get_i16, get_i32, put_bool, put_i16, put_i32},
        string_bytes::{
            compact_bytes_len, compact_nullable_string_len, compact_string_len,
            get_compact_bytes_owned, get_compact_nullable_string_owned, get_compact_string_owned,
            put_compact_bytes, put_compact_nullable_string, put_compact_string,
        },
        uuid::{Uuid, get_uuid, put_uuid},
    },
};

/// Api key of `ApproveBreakGlass`.
pub const API_KEY: i16 = 1018;
/// Lowest version of `ApproveBreakGlass` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `ApproveBreakGlass` that this build speaks.
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

/// Adds an approval to a break-glass proposal, or withdraws the proposal.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApproveBreakGlassRequest {
    /// The proposal to approve or to withdraw.
    pub proposal_id: Uuid,
    /// The operator key that made `signature`. It is empty for an unsigned
    /// approval.
    pub key_id: String,
    /// Detached Ed25519 signature over the canonical bytes of the proposal. It
    /// is empty for an unsigned approval.
    pub signature: Vec<u8>,
    /// `true` withdraws the proposal instead of approving it. The broker then
    /// ignores `key_id` and `signature`.
    pub withdraw: bool,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for ApproveBreakGlassRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_uuid(buf, self.proposal_id);
        put_compact_string(buf, &self.key_id);
        put_compact_bytes(buf, &self.signature);
        put_bool(buf, self.withdraw);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        UUID_LEN
            + compact_string_len(&self.key_id)
            + compact_bytes_len(&self.signature)
            + BOOL_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for ApproveBreakGlassRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            proposal_id: get_uuid(buf)?,
            key_id: get_compact_string_owned(buf)?,
            signature: get_compact_bytes_owned(buf)?.to_vec(),
            withdraw: get_bool(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for ApproveBreakGlassRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = ApproveBreakGlassResponse;
}

/// Outcome of an [`ApproveBreakGlassRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApproveBreakGlassResponse {
    /// Milliseconds that the broker held the request back for a quota.
    pub throttle_time_ms: i32,
    /// Kafka error code. `0` means the controller recorded the approval, or
    /// withdrew the proposal.
    pub error_code: i16,
    /// Text that explains a non-zero `error_code`. `None` when the broker has
    /// nothing to add to the code.
    pub error_message: Option<String>,
    /// Number of distinct principals that have approved the proposal after this
    /// request. A caller compares it with `approvals_required` to see whether
    /// the proposal can now authorize its action.
    pub approvals_held: i32,
    /// Number of distinct principals that the proposal needs. The broker holds
    /// this value in its configuration, and the minimum is two.
    pub approvals_required: i32,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for ApproveBreakGlassResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i32(buf, self.throttle_time_ms);
        put_i16(buf, self.error_code);
        put_compact_nullable_string(buf, self.error_message.as_deref());
        put_i32(buf, self.approvals_held);
        put_i32(buf, self.approvals_required);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        I32_LEN
            + I16_LEN
            + compact_nullable_string_len(self.error_message.as_deref())
            + I32_LEN
            + I32_LEN
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for ApproveBreakGlassResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            throttle_time_ms: get_i32(buf)?,
            error_code: get_i16(buf)?,
            error_message: get_compact_nullable_string_owned(buf)?,
            approvals_held: get_i32(buf)?,
            approvals_required: get_i32(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl ApproveBreakGlassRequest {
    /// Builds a signed approval with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            proposal_id: Uuid([
                200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215,
            ]),
            key_id: "bob-yubi".to_string(),
            signature: (0..64).collect(),
            withdraw: false,
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl ApproveBreakGlassResponse {
    /// Builds a response with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            throttle_time_ms: 9,
            error_code: 1007,
            error_message: Some("principal already approved this proposal".to_string()),
            approvals_held: 1,
            approvals_required: 2,
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
            ("default", ApproveBreakGlassRequest::default()),
            ("signed approval", ApproveBreakGlassRequest::populated()),
            (
                "unsigned approval",
                ApproveBreakGlassRequest {
                    key_id: String::new(),
                    signature: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                    ..ApproveBreakGlassRequest::populated()
                },
            ),
            (
                "withdraw",
                ApproveBreakGlassRequest {
                    key_id: String::new(),
                    signature: Vec::new(),
                    withdraw: true,
                    ..ApproveBreakGlassRequest::populated()
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
            ("default", ApproveBreakGlassResponse::default()),
            (
                "refusal with a message",
                ApproveBreakGlassResponse::populated(),
            ),
            (
                "second approval reaches the quorum",
                ApproveBreakGlassResponse {
                    throttle_time_ms: 0,
                    error_code: 0,
                    error_message: None,
                    approvals_held: 2,
                    approvals_required: 2,
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
    fn a_default_request_approves_and_does_not_withdraw() {
        assert!(!ApproveBreakGlassRequest::default().withdraw);
    }

    #[test]
    fn truncated_input_is_rejected() {
        reject_truncated(&ApproveBreakGlassRequest::populated(), MIN_VERSION);
        reject_truncated(&ApproveBreakGlassResponse::populated(), MIN_VERSION);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let request = ApproveBreakGlassRequest::populated();
            assert!(matches!(
                request.encode(&mut Vec::<u8>::new(), version),
                Err(ProtocolError::UnsupportedVersion {
                    api_key: API_KEY,
                    ..
                })
            ));
            let response = ApproveBreakGlassResponse::populated();
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
