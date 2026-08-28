//! Topic write-freeze registry entries, replicated through the raft quorum
//! with `MetadataRecord::V1TopicFreeze`.
//!
//! A freeze is a cluster state in which a topic stays readable and stays
//! replicated, but the broker refuses every append that puts new
//! externally-authored data into its log. Incident response, migrations, and
//! disaster-recovery promotion all need that state, and an ACL edit cannot
//! express it.
//!
//! The record stays pure data. The produce-path gate and the signature checks
//! live in `krabka-broker`, and the resolution index lives on
//! [`MetadataImage::topic_freeze`](crate::MetadataImage::topic_freeze).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::acl::PatternType;

/// One entry in the write-freeze registry.
///
/// The entry is authoritative for its `(pattern_type, scope)` key: a later
/// record with the same key replaces the earlier one. [`Self::frozen`] is the
/// removal sentinel, in the same shape as `FeatureLevelRecord.level == 0`, so
/// a thaw stays attributed in the raft log rather than vanishing as a
/// tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicFreezeRecord {
    /// A literal topic name, or a topic-name prefix, as
    /// [`Self::pattern_type`] selects.
    pub scope: String,
    /// The vocabulary that `kafka-acls` already gives an operator:
    /// [`PatternType::Literal`] names one topic and
    /// [`PatternType::Prefixed`] names a namespace.
    pub pattern_type: PatternType,
    /// `true` adds or replaces the entry. `false` removes it, and is the
    /// thaw.
    pub frozen: bool,
    /// Free text that says why the freeze exists. It goes to the audit log
    /// and to the producer's error message.
    pub reason: String,
    /// The principal that set the entry, in `KafkaPrincipal` string form,
    /// for example `"User:alice"`.
    pub set_by: String,
    /// Epoch milliseconds at which the operator built the record. It is part
    /// of the signed bytes, so an old signed thaw cannot be replayed.
    pub set_at_ms: i64,
    /// The break-glass approval that authorized a thaw. It is
    /// [`Uuid::nil`] on a freeze, because a freeze needs no approval.
    pub proposal_id: Uuid,
    /// The operator key that signed the record. It is empty when the record
    /// is unsigned.
    pub key_id: String,
    /// Detached Ed25519 signature over the canonical bytes of the record,
    /// made on the machine of the operator. It is empty when the record is
    /// unsigned. This crate keeps the bytes opaque; `krabka-broker` builds
    /// and verifies them.
    pub signature: Vec<u8>,
}
