//! Versioned metadata records.
//!
//! wincode encodes a [`MetadataRecord`] as its variant index, then the fields
//! of the variant in declaration order. Nothing is length-prefixed, so a reader
//! cannot skip a variant it does not know: it fails to decode it. The variant
//! order and every field layout are part of the krabka 1.x on-disk contract. A
//! new variant goes at the end. The golden bytes in `wincode_contract.rs` pin
//! one value of every variant.

pub use krabka_ids::LeaderEpoch;
pub use krabka_voters::NodeId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRecord {
    pub name: String,
    pub topic_id: Uuid,
    pub partitions: i32,
    pub replication_factor: i16,
}

fn default_partition_epoch() -> i32 {
    -1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
    /// Per-partition leader epoch (KIP-320). Bumped on every leader change.
    /// Older on-disk metadata is not migrated. `#[serde(transparent)]` on
    /// [`LeaderEpoch`] keeps the on-disk bincode bytes identical to a bare
    /// `i32`.
    pub leader_epoch: LeaderEpoch,
    /// Replicas being added in an in-flight reassignment. Empty when no
    /// reassignment in flight. KIP-455.
    pub adding_replicas: Vec<NodeId>,
    /// Replicas being removed in an in-flight reassignment. Empty when
    /// no reassignment in flight. KIP-455.
    pub removing_replicas: Vec<NodeId>,
    /// KIP-858: the log-directory UUID that hosts each replica, parallel to
    /// [`Self::replicas`] in the same index order. The reserved ids are
    /// Kafka's `DirectoryId` sentinels (see [`crate::directory_id`]):
    /// [`MIGRATING`](crate::directory_id::MIGRATING), the nil UUID, is a
    /// replica placed before `3.7-IV2` or on a broker with no log-dir ids,
    /// and [`UNASSIGNED`](crate::directory_id::UNASSIGNED) is a replica the
    /// controller placed on a multi-dir broker that has not yet reported its
    /// `AssignReplicasToDirs`. An empty list reads as every replica
    /// `MIGRATING`, as Kafka's `PartitionRegistration` reads it. The
    /// controller matches this against the replica slot of a broker to map
    /// the failed-dir UUID of that broker to the partitions it must fail
    /// over.
    pub directories: Vec<Uuid>,
    /// KIP-631: per-partition state epoch. It increments on every state
    /// change, such as a leader election, an ISR change, or a reassignment.
    /// It is 0 on creation. The default of -1 matches the KIP-631 schema
    /// default, for compatibility with records written before this field
    /// existed.
    #[serde(default = "default_partition_epoch")]
    pub partition_epoch: i32,
}

/// KIP-858 directory-assignment delta. A broker reports which log-dir UUID
/// hosts its replica of `(topic, partition)`. Apply treats it as a DELTA: it
/// sets ONLY the slot of the reporting replica in
/// `PartitionRecord.directories` and never touches leader, isr, replicas,
/// adding, or removing. It therefore cannot clobber a concurrent reassignment
/// or ISR change. On the `KRaft` log it is encoded as Kafka's standard
/// `PartitionChangeRecord` with only the directories field set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionDirAssignmentRecord {
    pub topic: String,
    pub partition: i32,
    /// The reporting broker (must be a replica of the partition).
    pub replica: NodeId,
    /// The log-directory UUID hosting this broker's replica.
    pub directory: Uuid,
}

/// Diskless offset-sequencer delta: advance a partition's committed
/// next-offset by `count`.
///
/// Applied as a delta, never a full-record replace, so sequential advances on
/// the committed metadata log yield a gap-free, strictly-monotonic, unique
/// offset sequence. On the `KRaft` log it still uses a Krabka-private carrier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionOffsetAdvanceRecord {
    pub topic: String,
    pub partition: i32,
    /// Offsets consumed by the produce group. The producer's base offset is the
    /// committed next-offset before this increment applies.
    pub count: i64,
}

/// KIP-966 eligible-leader state for one partition. Empty vectors clear it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionElrRecord {
    pub topic: String,
    pub partition: i32,
    pub eligible_leader_replicas: Vec<NodeId>,
    pub last_known_elr: Vec<NodeId>,
}

/// KIP-704 state of an uncleanly elected leader.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i8)]
pub enum LeaderRecoveryState {
    #[default]
    Recovered = 0,
    Recovering = 1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRecoveryRecord {
    pub topic: String,
    pub partition: i32,
    pub state: LeaderRecoveryState,
}

/// A Kafka `PartitionChangeRecord` can carry assignment, ELR, and recovery
/// changes together. This record preserves that atomic update on replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionUpdateRecord {
    pub partition: PartitionRecord,
    pub eligible_leader_replicas: Option<Vec<NodeId>>,
    pub last_known_elr: Option<Vec<NodeId>>,
    pub recovery_state: Option<LeaderRecoveryState>,
}

/// A single named listener endpoint advertised by a broker. It is stored as a
/// list on [`BrokerRegistrationRecord::endpoints`], so KRaft-style metadata
/// can advertise per-listener `host:port` and protocol triples to clients on
/// `Metadata` v9+. A legacy single-listener broker leaves the list empty and
/// relies on the top-level `host` and `port` fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEndpoint {
    /// Listener name (e.g. `"PLAINTEXT"`, `"SSL"`, `"SASL_SSL"`).
    pub name: String,
    pub host: String,
    pub port: u16,
    pub protocol: krabka_security::ListenerProtocol,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRegistrationRecord {
    pub node_id: NodeId,
    /// KIP-903 broker epoch: the raft log offset at which this registration
    /// record committed. The controller leader assigns it at append time in
    /// `on_submit_change`. A freshly-built literal carries `0` until the
    /// leader stamps it. `AlterPartition` uses it to fence stale replicas
    /// from the ISR.
    pub broker_epoch: i64,
    /// KIP-631: UUID that identifies this specific process invocation of the
    /// broker. Generated once at first boot and persisted in
    /// `{log_dir}/incarnation_id`. A JVM controller uses it to detect
    /// broker restarts and fence stale replica memberships.
    #[serde(default)]
    pub incarnation_id: uuid::Uuid,
    /// Legacy single-listener host, used as inter-broker default and by
    /// pre-v9 `Metadata` responses. v9+ projects [`Self::endpoints`].
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
    /// Per-listener endpoints. Empty on records written before this
    /// field was added; populated from
    /// `BrokerConfig::effective_listeners()` for self-registration.
    pub endpoints: Vec<BrokerEndpoint>,
    /// KIP-858 stable IDs for the broker's online log directories.
    /// Empty at metadata versions before `3.7-IV2` and in legacy snapshots.
    #[serde(default)]
    pub log_dirs: Vec<uuid::Uuid>,
    /// KIP-631 fencing state. Kafka writes a new registration fenced (the
    /// `RegisterBrokerRecord` schema default) and unfences it with a
    /// [`BrokerRegistrationChangeRecord`] once the broker catches up.
    pub fenced: bool,
    /// KIP-841: the broker is in controlled shutdown. Only a
    /// [`BrokerRegistrationChangeRecord`] sets it; only a new registration
    /// clears it.
    pub in_controlled_shutdown: bool,
    /// KIP-1066 cordoned log directories. `None` until the broker first
    /// reports them in a heartbeat, as in Kafka's `BrokerRegistration`. Only
    /// carried from `metadata.version` `4.3-IV0`.
    pub cordoned_log_dirs: Option<Vec<uuid::Uuid>>,
    /// KIP-584 feature ranges advertised by this broker at registration.
    /// Empty only for legacy Krabka snapshots written before the ranges were
    /// retained in the image.
    #[serde(default)]
    pub features: std::collections::BTreeMap<String, (i16, i16)>,
}

/// KIP-919 controller registration persisted in the metadata log.
///
/// Static controller voters still register their current process incarnation,
/// listener endpoints, and supported feature ranges.  Keeping that state in
/// the image lets every controller replay the same registration and gives JVM
/// peers the `RegisterControllerRecord` they expect in a mixed quorum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerRegistrationRecord {
    pub node_id: NodeId,
    pub incarnation_id: Uuid,
    pub zk_migration_ready: bool,
    pub endpoints: Vec<BrokerEndpoint>,
    pub features: std::collections::BTreeMap<String, (i16, i16)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicRecord {
    pub name: String,
}

/// KIP-919 / `UnregisterBroker` (`api_key` 64), Kafka's
/// `UnregisterBrokerRecord` (metadata apiKey 1). Marks a broker as
/// permanently unregistered: the admin operator confirms that the broker is
/// gone for good and asks the cluster to drop its registration entry
/// from the metadata image. Later `Metadata` responses no longer
/// advertise the endpoints of the broker, and clients stop routing to it.
///
/// As in `ClusterControlManager.replay(UnregisterBrokerRecord)`, the record
/// removes the registration only when `broker_epoch` is the epoch of the
/// current registration. An apply against an unknown `node_id` or another
/// epoch does nothing, so a second apply is a no-op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnregisterBrokerRecord {
    pub node_id: NodeId,
    /// The epoch of the registration this record removes.
    pub broker_epoch: i64,
}

/// A change to the fencing state of a broker registration, as Kafka's
/// `BrokerRegistrationFencingChange` encodes it in
/// `BrokerRegistrationChangeRecord.Fenced`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FencingChange {
    /// `-1`: the broker has been unfenced.
    Unfence,
    /// `0`: no change.
    None,
    /// `1`: the broker has been fenced.
    Fence,
}

impl FencingChange {
    /// The `Fenced` byte Kafka writes for this change.
    #[must_use]
    pub fn wire_value(self) -> i8 {
        match self {
            Self::Unfence => -1,
            Self::None => 0,
            Self::Fence => 1,
        }
    }

    /// Reads a `Fenced` byte, or `None` for a value Kafka does not define.
    #[must_use]
    pub fn from_wire(value: i8) -> Option<Self> {
        match value {
            -1 => Some(Self::Unfence),
            0 => Some(Self::None),
            1 => Some(Self::Fence),
            _ => None,
        }
    }

    /// The fenced state after this change applies to `fenced`.
    #[must_use]
    pub fn apply(self, fenced: bool) -> bool {
        match self {
            Self::Unfence => false,
            Self::None => fenced,
            Self::Fence => true,
        }
    }
}

/// Kafka's `BrokerRegistrationChangeRecord` (metadata apiKey 17): a delta on
/// the registration of one broker.
///
/// As in `ClusterControlManager.replay(BrokerRegistrationChangeRecord)`, the
/// image applies it only when `broker_epoch` is the epoch of the current
/// registration; against an unknown broker or another epoch it does nothing.
/// `in_controlled_shutdown` true is Kafka's `IN_CONTROLLED_SHUTDOWN` (`1`) and
/// false is `NONE` (`0`); Kafka has no change that leaves controlled shutdown,
/// only a new registration does. An empty `log_dirs` and a `None`
/// `cordoned_log_dirs` mean no change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRegistrationChangeRecord {
    pub node_id: NodeId,
    pub broker_epoch: i64,
    pub fenced: FencingChange,
    pub in_controlled_shutdown: bool,
    /// KIP-858: the broker's online log directories after the change.
    pub log_dirs: Vec<Uuid>,
    /// KIP-1066: the broker's cordoned log directories after the change.
    pub cordoned_log_dirs: Option<Vec<Uuid>>,
}

impl BrokerRegistrationChangeRecord {
    /// A change that updates nothing, for `node_id` at `broker_epoch`. Set the
    /// fields to change on the result.
    #[must_use]
    pub fn no_change(node_id: NodeId, broker_epoch: i64) -> Self {
        Self {
            node_id,
            broker_epoch,
            fenced: FencingChange::None,
            in_controlled_shutdown: false,
            log_dirs: Vec::new(),
            cordoned_log_dirs: None,
        }
    }

    /// The registration after this change applies to `current`, as Kafka's
    /// `BrokerRegistration.cloneWith` builds it. The caller checks the epoch.
    #[must_use]
    pub fn applied_to(&self, current: &BrokerRegistrationRecord) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            fenced: self.fenced.apply(current.fenced),
            in_controlled_shutdown: current.in_controlled_shutdown || self.in_controlled_shutdown,
            log_dirs: if self.log_dirs.is_empty() {
                current.log_dirs.clone()
            } else {
                self.log_dirs.clone()
            },
            cordoned_log_dirs: self
                .cordoned_log_dirs
                .clone()
                .or_else(|| current.cordoned_log_dirs.clone()),
            ..current.clone()
        }
    }
}

/// KIP-1312 `UnregisterController` (`api_key` 94), Kafka trunk's
/// `UnregisterControllerRecord` (metadata apiKey 29). Removes the
/// registration of one controller. An apply against an unknown `node_id`
/// does nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnregisterControllerRecord {
    pub node_id: NodeId,
}

/// Mutable topic configuration overrides. Authoritative target state:
/// each `V1TopicConfig` record fully replaces the previous override map
/// for `topic`. An empty map clears all overrides. The `AlterConfigs`
/// handler merges before it submits the record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigRecord {
    pub topic: String,
    pub overrides: std::collections::BTreeMap<String, String>,
}

/// Per-broker configuration key/value pair. `Some(value)` = set; `None` = delete.
///
/// Kafka names the cluster-wide broker-default resource with an empty string.
/// [`DEFAULT_BROKER_CONFIG_NODE_ID`] represents that resource internally; the
/// `KRaft` translator maps it back to the empty resource name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigRecord {
    pub node_id: NodeId,
    pub config_name: String,
    /// `Some(value)` = set; `None` = delete.
    pub config_value: Option<String>,
}

/// Internal identity for Kafka's cluster-wide default broker-config resource.
/// Broker IDs are non-negative signed 32-bit integers on the Kafka wire, so
/// this value cannot collide with a real broker.
pub const DEFAULT_BROKER_CONFIG_NODE_ID: NodeId = NodeId(u64::MAX);

/// KIP-714 client-metrics subscription config. Authoritative target
/// state: each `V1ClientMetricsConfig` fully replaces the previous
/// override map for `name`, the subscription name. An empty map deletes
/// the subscription. The `IncrementalAlterConfigs` handler merges before it
/// submits the record, in the same pattern as [`TopicConfigRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetricsConfigRecord {
    pub name: String,
    pub configs: std::collections::BTreeMap<String, String>,
}

/// KIP-1071 dynamic configuration for one group resource. Each record is the
/// authoritative override map for `group_id`; an empty map clears the resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupConfigRecord {
    pub group_id: String,
    pub configs: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaEntity {
    pub entity_type: String,
    pub entity_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientQuotaRecord {
    /// Canonicalized entity tuple, sorted alphabetically by `entity_type`.
    pub entity: Vec<QuotaEntity>,
    pub config_key: String,
    pub config_value: Option<f64>,
}

/// Durable controller state for cluster-wide producer-ID block allocation.
/// `next_producer_id` is the first ID not covered by any committed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerIdsRecord {
    pub broker_id: NodeId,
    pub broker_epoch: i64,
    pub next_producer_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramCredentialRecord {
    pub user: String,
    pub mechanism: krabka_security::SaslMechanism,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteScramCredentialRecord {
    pub user: String,
    pub mechanism: krabka_security::SaslMechanism,
}

/// A single delegation token's authoritative state (KIP-48).
/// The record has replacement semantics: a new record with the same
/// `token_id` overwrites the prior one in the image. Both Create and Renew
/// use that. Removal goes through
/// [`DeleteDelegationTokenRecord`].
///
/// The fields are those of Kafka's `DelegationTokenRecord`. Like Kafka's, the
/// record carries no HMAC: the token's password is
/// [`krabka_security::compute_token_hmac`] of `token_id` under the cluster's
/// secret key, recomputed wherever it is needed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationTokenRecord {
    pub token_id: String,
    pub owner: krabka_security::KafkaPrincipal,
    /// The principal that created the token, which is the owner unless an
    /// administrator created it on the owner's behalf (KIP-373).
    pub requester: krabka_security::KafkaPrincipal,
    pub issue_timestamp_ms: i64,
    pub expiry_timestamp_ms: i64,
    /// Issue plus max-lifetime. A renewal cannot push `expiry_timestamp_ms`
    /// past this ceiling.
    pub max_timestamp_ms: i64,
    pub renewers: Vec<krabka_security::KafkaPrincipal>,
}

/// Tombstone record that removes a delegation token (KIP-48)
/// from the image. The `ExpireDelegationToken` handlers and the
/// background expiry sweep emit it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteDelegationTokenRecord {
    pub token_id: String,
}

/// KIP-853: finalizes the cluster-wide kraft.version feature level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KRaftVersionRecord {
    pub kraft_version: u16,
}

/// KIP-853: full snapshot of the controller voter set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub voters: crate::voters::VoterSet,
}

/// KIP-584 finalized feature level. `level` is the finalized
/// `max_version_level` for `name`. `level == 0` is the KIP-584 sentinel
/// for "delete this finalized feature": `MetadataImage::apply` removes the
/// entry and does not store a zero. Replacement semantics: a later record
/// with the same `name` overwrites the previous level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureLevelRecord {
    pub name: String,
    pub level: i16,
}

/// Snapshot-only carrier for the KIP-584 finalized-features epoch.
///
/// Apply normally derives the epoch, with one bump per applied
/// `V1FeatureLevel`, so it tracks the history of `UpdateFeatures` calls and
/// not the live feature count. That derivation cannot survive a snapshot. A
/// snapshot stores resulting *state*, so it emits at most one
/// `V1FeatureLevel` per live feature, which is fewer records than the
/// original apply history. A replay of those records alone reconstructs a
/// smaller epoch and diverges from a replica that replayed the full log.
///
/// [`MetadataImage::to_records`](crate::MetadataImage::to_records) therefore
/// emits this record last, and
/// [`MetadataImage::apply`](crate::MetadataImage::apply) SETS the epoch from
/// it verbatim and does not bump it. That pins the reconstructed epoch to the
/// original. Only `to_records` produces this record, and only snapshot replay
/// consumes it. Nothing submits it as a controller change, so it never appears
/// in the live Raft log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeaturesEpochRecord {
    pub epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MetadataRecord {
    V1Topic(TopicRecord),
    V1Partition(PartitionRecord),
    V1BrokerRegistration(BrokerRegistrationRecord),
    V1DeleteTopic(DeleteTopicRecord),
    V1TopicConfig(TopicConfigRecord),
    V1ScramCredential(ScramCredentialRecord),
    V1DeleteScramCredential(DeleteScramCredentialRecord),
    V1AccessControlEntry(crate::AclEntry),
    V1DeleteAccessControlEntry(crate::AclEntryFilter),
    V1BrokerConfig(BrokerConfigRecord),
    V1ClientQuota(ClientQuotaRecord),
    V1ProducerIds(ProducerIdsRecord),
    V1DelegationToken(DelegationTokenRecord),
    V1DeleteDelegationToken(DeleteDelegationTokenRecord),
    V1UnregisterBroker(UnregisterBrokerRecord),
    V1KRaftVersion(KRaftVersionRecord),
    V1Voters(VotersRecord),
    V1FeatureLevel(FeatureLevelRecord),
    V1ClientMetricsConfig(ClientMetricsConfigRecord),
    /// Snapshot-only: pins the finalized-features epoch on reconstruction.
    /// Nothing submits it through the controller. See [`FeaturesEpochRecord`].
    V1FeaturesEpoch(FeaturesEpochRecord),
    /// KIP-858 directory-assignment delta (see [`PartitionDirAssignmentRecord`]).
    /// Applied as a merge into one replica's `directories` slot; on the `KRaft`
    /// log it rides a Krabka-private carrier so it stays a delta end-to-end.
    V1PartitionDirAssignment(PartitionDirAssignmentRecord),
    /// Diskless offset-sequencer delta (see [`PartitionOffsetAdvanceRecord`]).
    /// Applied as an increment to the partition's committed next-offset.
    V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord),
    /// KIP-1071 dynamic GROUP resource configuration.
    V1GroupConfig(GroupConfigRecord),
    /// KIP-919 controller registration.
    V1ControllerRegistration(ControllerRegistrationRecord),
    /// One write-freeze registry entry (see
    /// [`TopicFreezeRecord`](crate::write_freeze::TopicFreezeRecord)).
    /// `frozen: false` removes the entry and is the thaw, so the registry
    /// needs no separate tombstone record.
    V1TopicFreeze(crate::write_freeze::TopicFreezeRecord),
    /// One break-glass proposal (see
    /// [`BreakGlassProposalRecord`](crate::break_glass::BreakGlassProposalRecord)).
    /// Replacement semantics on `proposal_id`: an approval and a consumption
    /// each write the whole record back.
    V1BreakGlassProposal(crate::break_glass::BreakGlassProposalRecord),
    /// Tombstone that removes a break-glass proposal by id. The expiry sweep
    /// emits it, in the same shape as `V1DeleteDelegationToken`.
    V1DeleteBreakGlassProposal(Uuid),
    /// KIP-966 per-partition eligible-leader state.
    V1PartitionElr(PartitionElrRecord),
    /// KIP-704 per-partition leader recovery state.
    V1PartitionRecovery(PartitionRecoveryRecord),
    /// Atomic standard-KRaft partition update decoded from a combined delta.
    V1PartitionUpdate(PartitionUpdateRecord),
    /// Kafka's `BrokerRegistrationChangeRecord`: a fencing, controlled-shutdown
    /// or log-dir delta on one broker registration, applied only at the
    /// registration's epoch.
    V1BrokerRegistrationChange(BrokerRegistrationChangeRecord),
    /// KIP-1312 controller unregistration.
    V1UnregisterController(UnregisterControllerRecord),
}

#[cfg(test)]
mod tests {

    use serde_wincode::SerdeCompat;
    use wincode::{Deserialize as _, Serialize as _};

    use super::*;

    fn round_trip(r: &MetadataRecord) -> MetadataRecord {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(r).unwrap();
        <SerdeCompat<MetadataRecord>>::deserialize(&bytes).unwrap()
    }

    #[test]
    fn feature_level_round_trip() {
        let r = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn group_config_round_trip() {
        let r = MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: "streams-app".into(),
            configs: std::collections::BTreeMap::from([(
                "streams.num.standby.replicas".into(),
                "1".into(),
            )]),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn controller_registration_round_trip() {
        let r = MetadataRecord::V1ControllerRegistration(ControllerRegistrationRecord {
            node_id: NodeId(3),
            incarnation_id: Uuid::from_u128(7),
            zk_migration_ready: false,
            endpoints: vec![BrokerEndpoint {
                name: "CONTROLLER".into(),
                host: "controller-3".into(),
                port: 9093,
                protocol: krabka_security::ListenerProtocol::Plaintext,
            }],
            features: std::collections::BTreeMap::from([("metadata.version".into(), (7, 25))]),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn producer_ids_round_trip() {
        let r = MetadataRecord::V1ProducerIds(ProducerIdsRecord {
            broker_id: NodeId(3),
            broker_epoch: 9,
            next_producer_id: 2_000,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn features_epoch_round_trip() {
        let r = MetadataRecord::V1FeaturesEpoch(FeaturesEpochRecord { epoch: 7 });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn topic_record_round_trip() {
        let r = MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 1,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn partition_record_round_trip() {
        let r = MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2)],
            leader_epoch: LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::from_u128(1), Uuid::from_u128(2), Uuid::nil()],
            partition_epoch: 0,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn partition_dir_assignment_round_trip() {
        let r = MetadataRecord::V1PartitionDirAssignment(PartitionDirAssignmentRecord {
            topic: "t".into(),
            partition: 2,
            replica: NodeId(3),
            directory: Uuid::from_u128(0xAB),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_registration_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: NodeId(7),
            broker_epoch: 0,
            incarnation_id: Uuid::from_u128(0xdeadbeef_cafe_babe_0123_456789abcdef),
            host: "192.168.1.10".into(),
            port: 9092,
            rack: Some("us-east-1a".into()),
            log_dirs: vec![],
            fenced: false,
            in_controlled_shutdown: false,
            cordoned_log_dirs: None,
            endpoints: vec![],
            features: std::collections::BTreeMap::new(),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_registration_with_endpoints_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: NodeId(1),
            broker_epoch: 0,
            incarnation_id: Uuid::from_u128(0xfeedface_0000_0000_0000_000000000001),
            host: "h".into(),
            port: 9092,
            rack: None,
            log_dirs: vec![],
            fenced: false,
            in_controlled_shutdown: false,
            cordoned_log_dirs: None,
            endpoints: vec![BrokerEndpoint {
                name: "EXTERNAL".into(),
                host: "ext.example.com".into(),
                port: 9092,
                protocol: krabka_security::ListenerProtocol::SaslSsl,
            }],
            features: std::collections::BTreeMap::new(),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_topic_round_trip() {
        let r = MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "doomed".into(),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn unregister_broker_round_trip() {
        let r = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
            node_id: NodeId(42),
            broker_epoch: 17,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_registration_change_and_unregister_controller_round_trip() {
        for r in [
            MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
                node_id: NodeId(4),
                broker_epoch: 12,
                fenced: FencingChange::Unfence,
                in_controlled_shutdown: true,
                log_dirs: vec![Uuid::from_u128(0xD1)],
                cordoned_log_dirs: Some(vec![]),
            }),
            MetadataRecord::V1UnregisterController(UnregisterControllerRecord {
                node_id: NodeId(3000),
            }),
        ] {
            assert2::assert!(round_trip(&r) == r);
        }
    }

    /// `FencingChange` is Kafka's `BrokerRegistrationFencingChange`: -1
    /// unfences, 0 leaves the state alone and 1 fences.
    #[test]
    fn fencing_change_matches_kafka() {
        for (change, wire, from_false, from_true) in [
            (FencingChange::Unfence, -1, false, false),
            (FencingChange::None, 0, false, true),
            (FencingChange::Fence, 1, true, true),
        ] {
            assert2::check!(
                (
                    change.wire_value(),
                    FencingChange::from_wire(wire),
                    change.apply(false),
                    change.apply(true),
                ) == (wire, Some(change), from_false, from_true)
            );
        }
        for undefined in [-2, 2, i8::MIN, i8::MAX] {
            assert2::check!(FencingChange::from_wire(undefined) == None);
        }
    }

    /// `applied_to` is Kafka's `BrokerRegistration.cloneWith`: each field of
    /// the change either leaves the registration's value or replaces it, and
    /// controlled shutdown can only turn on.
    #[test]
    fn registration_change_applies_like_clone_with() {
        let (d1, d2) = (Uuid::from_u128(0xD1), Uuid::from_u128(0xD2));
        let current = BrokerRegistrationRecord {
            node_id: NodeId(4),
            broker_epoch: 12,
            incarnation_id: Uuid::from_u128(4),
            host: "broker-4".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
            log_dirs: vec![d1],
            fenced: true,
            in_controlled_shutdown: false,
            cordoned_log_dirs: None,
            features: std::collections::BTreeMap::new(),
        };
        let base = BrokerRegistrationChangeRecord::no_change(NodeId(4), 12);
        let with = |edit: fn(&mut BrokerRegistrationRecord)| {
            let mut want = current.clone();
            edit(&mut want);
            want
        };
        for (case, change, want) in [
            ("no change", base.clone(), current.clone()),
            (
                "unfence",
                BrokerRegistrationChangeRecord {
                    fenced: FencingChange::Unfence,
                    ..base.clone()
                },
                with(|b| b.fenced = false),
            ),
            (
                "controlled shutdown",
                BrokerRegistrationChangeRecord {
                    in_controlled_shutdown: true,
                    ..base.clone()
                },
                with(|b| b.in_controlled_shutdown = true),
            ),
            (
                "new log dirs",
                BrokerRegistrationChangeRecord {
                    log_dirs: vec![d2],
                    ..base.clone()
                },
                with(|b| b.log_dirs = vec![Uuid::from_u128(0xD2)]),
            ),
            (
                "uncordon every dir",
                BrokerRegistrationChangeRecord {
                    cordoned_log_dirs: Some(vec![]),
                    ..base.clone()
                },
                with(|b| b.cordoned_log_dirs = Some(vec![])),
            ),
        ] {
            assert2::check!(change.applied_to(&current) == want, "{case}");
        }

        let shutting_down = BrokerRegistrationRecord {
            in_controlled_shutdown: true,
            ..current.clone()
        };
        assert2::check!(base.applied_to(&shutting_down) == shutting_down);
    }

    #[test]
    fn topic_config_record_round_trip() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        overrides.insert("segment.bytes".to_string(), "1048576".to_string());
        let r = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn scram_credential_round_trip() {
        let r = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: krabka_security::SaslMechanism::ScramSha512,
            salt: vec![1u8; 16],
            stored_key: vec![2u8; 64],
            server_key: vec![3u8; 64],
            iterations: 4096,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_scram_credential_round_trip() {
        let r = MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
            user: "alice".into(),
            mechanism: krabka_security::SaslMechanism::ScramSha512,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn v1_access_control_entry_round_trip() {
        let entry = crate::AclEntry {
            resource_type: crate::ResourceType::Topic,
            resource_name: "foo".into(),
            pattern_type: crate::PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: crate::AclOperation::Read,
            permission_type: crate::PermissionType::Allow,
        };
        let r = MetadataRecord::V1AccessControlEntry(entry);
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn v1_delete_access_control_entry_round_trip() {
        let filter = crate::AclEntryFilter {
            resource_type: Some(crate::ResourceType::Group),
            resource_name: Some("cg-foo".into()),
            pattern_type: Some(crate::PatternType::Literal),
            principal: None,
            host: None,
            operation: None,
            permission_type: None,
        };
        let r = MetadataRecord::V1DeleteAccessControlEntry(filter);
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_config_record_round_trip() {
        let r = MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(7),
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn client_quota_record_round_trip() {
        let r = MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app1".into()),
                },
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
            ],
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn delegation_token_record_round_trip() {
        let r = MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: "tok-abc".into(),
            owner: krabka_security::KafkaPrincipal {
                principal_type: "User".into(),
                name: "alice".into(),
            },
            requester: krabka_security::KafkaPrincipal {
                principal_type: "User".into(),
                name: "admin".into(),
            },
            issue_timestamp_ms: 1_700_000_000_000,
            expiry_timestamp_ms: 1_700_000_600_000,
            max_timestamp_ms: 1_700_604_800_000,
            renewers: vec![krabka_security::KafkaPrincipal {
                principal_type: "User".into(),
                name: "bob".into(),
            }],
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_delegation_token_record_round_trip() {
        let r = MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
            token_id: "tok-abc".into(),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn voters_record_round_trips() {
        let rec = MetadataRecord::V1Voters(VotersRecord {
            voters: crate::voters::VoterSet::from_voters([crate::voters::Voter {
                id: NodeId(7),
                directory_id: uuid::Uuid::from_u128(7),
                endpoints: vec![crate::voters::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "h".into(),
                    port: 1,
                }],
                kraft_version: crate::voters::KRaftVersionRange::default(),
            }]),
        });
        assert2::assert!(round_trip(&rec) == rec);
    }

    #[test]
    fn kraft_version_record_round_trips() {
        let rec = MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 });
        assert2::assert!(round_trip(&rec) == rec);
    }

    #[test]
    fn client_metrics_config_round_trip() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("interval.ms".to_string(), "60000".to_string());
        overrides.insert(
            "metrics".to_string(),
            "org.apache.kafka.consumer.".to_string(),
        );
        let r = MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: "sub-a".into(),
            configs: overrides,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn topic_freeze_round_trip() {
        let r = MetadataRecord::V1TopicFreeze(crate::write_freeze::TopicFreezeRecord {
            scope: "tenant-a.".into(),
            pattern_type: crate::PatternType::Prefixed,
            frozen: true,
            reason: "DR cutover".into(),
            set_by: "User:alice".into(),
            set_at_ms: 1_700_000_000_000,
            proposal_id: Uuid::from_u128(0xB1),
            key_id: "alice-yubi".into(),
            signature: vec![0xAB; 64],
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn break_glass_proposal_round_trip() {
        let r =
            MetadataRecord::V1BreakGlassProposal(crate::break_glass::BreakGlassProposalRecord {
                proposal_id: Uuid::from_u128(0xB1),
                action: crate::break_glass::BreakGlassAction::ThawTopicFreeze,
                target: "literal:orders".into(),
                proposer: "User:alice".into(),
                reason: "incident 4711 closed".into(),
                created_at_ms: 1_700_000_000_000,
                expires_at_ms: 1_700_000_600_000,
                approvals: vec![crate::break_glass::BreakGlassApproval {
                    principal: "User:bob".into(),
                    approved_at_ms: 1_700_000_060_000,
                    key_id: "bob-yubi".into(),
                    signature: vec![0xCD; 64],
                }],
                consumed_at_ms: 0,
                withdrawn: false,
            });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_break_glass_proposal_round_trip() {
        let r = MetadataRecord::V1DeleteBreakGlassProposal(Uuid::from_u128(0xB1));
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn partition_epoch_serde_default_is_minus_one() {
        // -1 is Kafka's "unknown epoch" sentinel; pin it (mutants flip it to
        // 0/1). This is the `#[serde(default)]` fallback for partition_epoch.
        assert2::assert!(default_partition_epoch() == -1);
    }
}
