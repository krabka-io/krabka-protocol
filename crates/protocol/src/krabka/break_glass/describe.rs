//! `DescribeBreakGlass`, api key 1019.
//!
//! The request reads break-glass proposals. A nil `proposal_id` asks for every
//! proposal that the controller holds, and `pending_only` drops the proposals
//! that an action consumed, that an operator withdrew, or that expired.
//!
//! Each approval carries the `key_id` and the `signature` that the approver
//! sent. The response gives them back on purpose, so an operator tool
//! re-verifies each approval against the operator public keys on its own
//! machine.

use bytes::{Buf, BufMut};

use crate::{
    Decode, Encode, ProtocolError, ProtocolRequest, UnknownTaggedFields,
    krabka::common::{
        BOOL_LEN, I8_LEN, I16_LEN, I32_LEN, I64_LEN, UUID_LEN, array_len, check_version,
        decode_array, encode_array, read_unknown_tagged_fields, unknown_tagged_fields_len,
        write_unknown_tagged_fields,
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

/// Api key of `DescribeBreakGlass`.
pub const API_KEY: i16 = 1019;
/// Lowest version of `DescribeBreakGlass` that this build speaks.
pub const MIN_VERSION: i16 = 0;
/// Highest version of `DescribeBreakGlass` that this build speaks.
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

/// Reads break-glass proposals.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DescribeBreakGlassRequest {
    /// `true` returns only the proposals that can still authorize an action.
    pub pending_only: bool,
    /// The proposal to read. A nil id asks for every proposal.
    pub proposal_id: Uuid,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for DescribeBreakGlassRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_bool(buf, self.pending_only);
        put_uuid(buf, self.proposal_id);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        BOOL_LEN + UUID_LEN + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for DescribeBreakGlassRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            pending_only: get_bool(buf)?,
            proposal_id: get_uuid(buf)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

impl ProtocolRequest for DescribeBreakGlassRequest {
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;

    type Response = DescribeBreakGlassResponse;
}

/// One approval of a break-glass proposal.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BreakGlassApproval {
    /// The principal that the broker authenticated when it took the approval.
    /// Every approval of one proposal names a different principal.
    pub principal: String,
    /// Milliseconds since the Unix epoch, at the moment the controller recorded
    /// the approval.
    pub approved_at_ms: i64,
    /// The operator key that made `signature`. It is empty for an unsigned
    /// approval.
    pub key_id: String,
    /// Detached Ed25519 signature over the canonical bytes of the proposal. It
    /// is empty for an unsigned approval.
    pub signature: Vec<u8>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for BreakGlassApproval {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
        put_compact_string(buf, &self.principal);
        put_i64(buf, self.approved_at_ms);
        put_compact_string(buf, &self.key_id);
        put_compact_bytes(buf, &self.signature);
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, _version: i16) -> usize {
        compact_string_len(&self.principal)
            + I64_LEN
            + compact_string_len(&self.key_id)
            + compact_bytes_len(&self.signature)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for BreakGlassApproval {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            principal: get_compact_string_owned(buf)?,
            approved_at_ms: get_i64(buf)?,
            key_id: get_compact_string_owned(buf)?,
            signature: get_compact_bytes_owned(buf)?.to_vec(),
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// One break-glass proposal, and the approvals that it holds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DescribedBreakGlassProposal {
    /// Id of the proposal.
    pub proposal_id: Uuid,
    /// The privileged action to gate. The value is the wire value of the
    /// broker's break-glass action, as [`super`] describes.
    pub action: i8,
    /// What the action applies to.
    pub target: String,
    /// The principal that opened the proposal. It cannot approve the proposal.
    pub proposer: String,
    /// Text that says why the operator needs the action.
    pub reason: String,
    /// Milliseconds since the Unix epoch, at the moment the controller opened
    /// the proposal.
    pub created_at_ms: i64,
    /// Milliseconds since the Unix epoch, at the moment the proposal expires.
    pub expires_at_ms: i64,
    /// Milliseconds since the Unix epoch, at the moment an action consumed the
    /// proposal. `0` means that no action consumed it. A proposal authorizes
    /// one action only.
    pub consumed_at_ms: i64,
    /// `true` means that an operator withdrew the proposal.
    pub withdrawn: bool,
    /// One entry for each approval, in the order that the controller recorded
    /// them.
    pub approvals: Vec<BreakGlassApproval>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for DescribedBreakGlassProposal {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        put_uuid(buf, self.proposal_id);
        put_i8(buf, self.action);
        put_compact_string(buf, &self.target);
        put_compact_string(buf, &self.proposer);
        put_compact_string(buf, &self.reason);
        put_i64(buf, self.created_at_ms);
        put_i64(buf, self.expires_at_ms);
        put_i64(buf, self.consumed_at_ms);
        put_bool(buf, self.withdrawn);
        encode_array(buf, &self.approvals, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        UUID_LEN
            + I8_LEN
            + compact_string_len(&self.target)
            + compact_string_len(&self.proposer)
            + compact_string_len(&self.reason)
            + I64_LEN
            + I64_LEN
            + I64_LEN
            + BOOL_LEN
            + array_len(&self.approvals, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for DescribedBreakGlassProposal {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        Ok(Self {
            proposal_id: get_uuid(buf)?,
            action: get_i8(buf)?,
            target: get_compact_string_owned(buf)?,
            proposer: get_compact_string_owned(buf)?,
            reason: get_compact_string_owned(buf)?,
            created_at_ms: get_i64(buf)?,
            expires_at_ms: get_i64(buf)?,
            consumed_at_ms: get_i64(buf)?,
            withdrawn: get_bool(buf)?,
            approvals: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

/// The proposals that a [`DescribeBreakGlassRequest`] asked for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DescribeBreakGlassResponse {
    /// Milliseconds that the broker held the request back for a quota.
    pub throttle_time_ms: i32,
    /// Kafka error code. `0` means `proposals` holds the answer.
    pub error_code: i16,
    /// Text that explains a non-zero `error_code`. `None` when the broker has
    /// nothing to add to the code.
    pub error_message: Option<String>,
    /// One entry for each proposal that the request matched.
    pub proposals: Vec<DescribedBreakGlassProposal>,
    /// Tagged fields that this build does not know.
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Encode for DescribeBreakGlassResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        put_i32(buf, self.throttle_time_ms);
        put_i16(buf, self.error_code);
        put_compact_nullable_string(buf, self.error_message.as_deref());
        encode_array(buf, &self.proposals, version)?;
        write_unknown_tagged_fields(buf, &self.unknown_tagged_fields);
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        I32_LEN
            + I16_LEN
            + compact_nullable_string_len(self.error_message.as_deref())
            + array_len(&self.proposals, version)
            + unknown_tagged_fields_len(&self.unknown_tagged_fields)
    }
}

impl Decode<'_> for DescribeBreakGlassResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        check_version(API_KEY, version, MIN_VERSION, MAX_VERSION)?;
        Ok(Self {
            throttle_time_ms: get_i32(buf)?,
            error_code: get_i16(buf)?,
            error_message: get_compact_nullable_string_owned(buf)?,
            proposals: decode_array(buf, version)?,
            unknown_tagged_fields: read_unknown_tagged_fields(buf)?,
        })
    }
}

#[cfg(test)]
impl DescribeBreakGlassRequest {
    /// Builds a request with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            pending_only: true,
            proposal_id: Uuid([
                200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215,
            ]),
            unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
        }
    }
}

#[cfg(test)]
impl DescribeBreakGlassResponse {
    /// Builds a response with every field set, for the round-trip tests.
    fn populated() -> Self {
        Self {
            throttle_time_ms: 6,
            error_code: 0,
            error_message: None,
            proposals: vec![
                DescribedBreakGlassProposal {
                    proposal_id: Uuid([
                        200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214,
                        215,
                    ]),
                    action: 6,
                    target: "doomed".to_string(),
                    proposer: "User:alice".to_string(),
                    reason: "topic holds test data only".to_string(),
                    created_at_ms: 1_770_000_000_000,
                    expires_at_ms: 1_770_000_180_000,
                    consumed_at_ms: 0,
                    withdrawn: false,
                    approvals: vec![
                        BreakGlassApproval {
                            principal: "User:bob".to_string(),
                            approved_at_ms: 1_770_000_060_000,
                            key_id: "bob-yubi".to_string(),
                            signature: (0..64).collect(),
                            unknown_tagged_fields:
                                crate::krabka::test_support::sample_tagged_fields(),
                        },
                        BreakGlassApproval {
                            principal: "User:carol".to_string(),
                            approved_at_ms: 1_770_000_090_000,
                            ..BreakGlassApproval::default()
                        },
                    ],
                    unknown_tagged_fields: crate::krabka::test_support::sample_tagged_fields(),
                },
                DescribedBreakGlassProposal {
                    proposal_id: Uuid([1; 16]),
                    action: 1,
                    target: "literal:orders".to_string(),
                    proposer: "User:dave".to_string(),
                    reason: "DR cutover finished".to_string(),
                    created_at_ms: 1_769_000_000_000,
                    expires_at_ms: 1_769_000_180_000,
                    consumed_at_ms: 1_769_000_120_000,
                    withdrawn: true,
                    approvals: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
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
    use crate::krabka::test_support::{reject_truncated, roundtrip_case, unsupported_versions};

    #[test]
    fn requests_roundtrip() {
        let cases = [
            (
                "default asks for every proposal",
                DescribeBreakGlassRequest::default(),
            ),
            (
                "one pending proposal",
                DescribeBreakGlassRequest::populated(),
            ),
            (
                "every pending proposal",
                DescribeBreakGlassRequest {
                    pending_only: true,
                    proposal_id: Uuid::ZERO,
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
            ("default", DescribeBreakGlassResponse::default()),
            ("two proposals", DescribeBreakGlassResponse::populated()),
            (
                "refusal with no proposals",
                DescribeBreakGlassResponse {
                    throttle_time_ms: 2,
                    error_code: 31,
                    error_message: Some("cluster authorization failed".to_string()),
                    proposals: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ),
            (
                "proposal with no approvals",
                DescribeBreakGlassResponse {
                    proposals: vec![DescribedBreakGlassProposal::default()],
                    ..DescribeBreakGlassResponse::default()
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
    fn a_nil_proposal_id_asks_for_every_proposal() {
        assert!(
            DescribeBreakGlassRequest::default()
                == DescribeBreakGlassRequest {
                    pending_only: false,
                    proposal_id: Uuid::ZERO,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
        );
    }

    #[test]
    fn an_unsigned_approval_carries_an_empty_signature() {
        let response = DescribeBreakGlassResponse::populated();
        let unsigned = &response.proposals[0].approvals[1];
        assert!(unsigned.key_id.is_empty());
        assert!(unsigned.signature.is_empty());
    }

    #[test]
    fn truncated_input_is_rejected() {
        reject_truncated(&DescribeBreakGlassRequest::populated(), MIN_VERSION);
        reject_truncated(&DescribeBreakGlassResponse::populated(), MIN_VERSION);
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in unsupported_versions(MIN_VERSION, MAX_VERSION) {
            let request = DescribeBreakGlassRequest::populated();
            assert!(matches!(
                request.encode(&mut Vec::<u8>::new(), version),
                Err(ProtocolError::UnsupportedVersion {
                    api_key: API_KEY,
                    ..
                })
            ));
            let response = DescribeBreakGlassResponse::populated();
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
