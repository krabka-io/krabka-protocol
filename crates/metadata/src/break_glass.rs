//! Break-glass proposal records, replicated through the raft quorum with
//! `MetadataRecord::V1BreakGlassProposal` and
//! `V1DeleteBreakGlassProposal`.
//!
//! A break-glass workflow needs two different people to agree before the
//! broker does a privileged operation. An approved proposal is a standing
//! authorization for a bounded window, and not a field on the request it
//! authorizes, so no Kafka wire message changes. An operator gets the
//! approval out of band, then runs the ordinary tool, and the broker reads
//! the proposal out of the metadata image.
//!
//! The records stay pure data. The approver set, the distinct-principal rule,
//! and the signature checks live in `krabka-broker`. The one rule this crate
//! enforces is in
//! [`MetadataImage::validate`](crate::MetadataImage::validate): an incoming
//! approval list must extend the stored one, so two concurrent approvals
//! cannot overwrite each other.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The privileged transitions that a break-glass proposal can authorize.
///
/// Each one can lose committed data, or can lift the protection that stops
/// another one from losing committed data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BreakGlassAction {
    /// Remove a write-freeze registry entry.
    ThawTopicFreeze,
    /// Elect a leader that is not in the ISR.
    UncleanElectLeaders,
    /// Recover a partition whose whole ISR is gone.
    UncleanRecovery,
    /// Drop the registration of a broker.
    UnregisterBroker,
    /// Cancel a partition reassignment that is in flight.
    CancelReassignment,
    /// Delete a topic.
    DeleteTopic,
    /// Delete records below an offset.
    DeleteRecords,
}

/// One approval on a proposal.
///
/// The broker checks that the principal is in the configured approver set,
/// that it is not the proposer, and that it is not already in the list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BreakGlassApproval {
    /// The approver, in `KafkaPrincipal` string form, for example
    /// `"User:bob"`.
    pub principal: String,
    /// Epoch milliseconds at which the approval was made.
    pub approved_at_ms: i64,
    /// The operator key that signed the approval. It is empty when the
    /// approval is unsigned.
    pub key_id: String,
    /// Detached Ed25519 signature over the canonical bytes of the proposal.
    /// It is empty when the approval is unsigned. This crate keeps the bytes
    /// opaque; `krabka-broker` builds and verifies them.
    pub signature: Vec<u8>,
}

/// One break-glass proposal, keyed by [`Self::proposal_id`].
///
/// The record is authoritative for that id: a later record with the same id
/// replaces the earlier one, which is how an approval and a consumption are
/// both written. `V1DeleteBreakGlassProposal` removes the entry, and the
/// expiry sweep emits it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BreakGlassProposalRecord {
    pub proposal_id: Uuid,
    pub action: BreakGlassAction,
    /// What the action applies to. The shape depends on the action: a
    /// `"<pattern>:<scope>"` pair, a `"<topic>-<partition>"` pair, a topic
    /// name, or a broker id.
    pub target: String,
    /// The principal that proposed the action, in `KafkaPrincipal` string
    /// form. It cannot also approve.
    pub proposer: String,
    /// Free text that says why the action is needed. It goes to the audit
    /// log.
    pub reason: String,
    pub created_at_ms: i64,
    /// Epoch milliseconds after which the proposal no longer authorizes
    /// anything. It bounds how long a stale approval stays usable.
    pub expires_at_ms: i64,
    /// The approvals collected so far, in the order they were made. A record
    /// can only append to this list. See
    /// [`MetadataImage::validate`](crate::MetadataImage::validate).
    pub approvals: Vec<BreakGlassApproval>,
    /// Epoch milliseconds at which the approval was spent on the transition
    /// it authorized. `0` means unconsumed. The gated handler stamps it and
    /// puts the stamped record in the same raft append as the transition, so
    /// the two commit together.
    pub consumed_at_ms: i64,
    /// `true` when the proposer withdrew the proposal. A withdrawn proposal
    /// authorizes nothing.
    pub withdrawn: bool,
}
