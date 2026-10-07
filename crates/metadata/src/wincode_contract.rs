//! Golden bytes for the wincode encoding of [`MetadataRecord`].
//!
//! wincode is positional: it writes a variant as its declaration index and a
//! struct as its fields in order, with no names. serde has no attribute that
//! pins a variant index, so these fixtures are the guard. Each row encodes one
//! value of one variant and compares it with the bytes a krabka 1.x broker
//! writes to `bootstrap.records.bin`, the krabka-private `NoOpRecord` tags and
//! the `SubmitChange` RPC. A reordered, inserted or removed variant, or a
//! changed field layout, fails here.
//!
//! [`variant_index`] matches every variant without a wildcard, so a new
//! variant does not compile until it has an index and a row here.

use std::collections::BTreeMap;

use assert2::check;
use krabka_security::{KafkaPrincipal, ListenerProtocol, SaslMechanism};
use serde_wincode::SerdeCompat;
use uuid::Uuid;
use wincode::{Deserialize as _, Serialize as _};

use crate::{
    AclEntry, AclEntryFilter, AclOperation, BreakGlassAction, BreakGlassApproval,
    BreakGlassProposalRecord, BrokerConfigRecord, BrokerEndpoint, BrokerRegistrationChangeRecord,
    BrokerRegistrationRecord, ClientMetricsConfigRecord, ClientQuotaRecord,
    ControllerRegistrationRecord, DelegationTokenRecord, DeleteDelegationTokenRecord,
    DeleteScramCredentialRecord, DeleteTopicRecord, FeatureLevelRecord, FeaturesEpochRecord,
    FencingChange, GroupConfigRecord, KRaftVersionRange, KRaftVersionRecord, LeaderEpoch,
    LeaderRecoveryState, MetadataRecord, NodeId, PartitionDirAssignmentRecord, PartitionElrRecord,
    PartitionOffsetAdvanceRecord, PartitionRecord, PartitionRecoveryRecord, PartitionUpdateRecord,
    PatternType, PermissionType, ProducerIdsRecord, QuotaEntity, ResourceType,
    ScramCredentialRecord, TopicConfigRecord, TopicFreezeRecord, TopicRecord,
    UnregisterBrokerRecord, UnregisterControllerRecord, Voter, VoterEndpoint, VoterSet,
    VotersRecord,
};

/// The wincode variant index of `rec`. It is the declaration order of
/// [`MetadataRecord`], spelled out so that a new variant must be given one.
fn variant_index(rec: &MetadataRecord) -> u32 {
    match rec {
        MetadataRecord::V1Topic(_) => 0,
        MetadataRecord::V1Partition(_) => 1,
        MetadataRecord::V1BrokerRegistration(_) => 2,
        MetadataRecord::V1DeleteTopic(_) => 3,
        MetadataRecord::V1TopicConfig(_) => 4,
        MetadataRecord::V1ScramCredential(_) => 5,
        MetadataRecord::V1DeleteScramCredential(_) => 6,
        MetadataRecord::V1AccessControlEntry(_) => 7,
        MetadataRecord::V1DeleteAccessControlEntry(_) => 8,
        MetadataRecord::V1BrokerConfig(_) => 9,
        MetadataRecord::V1ClientQuota(_) => 10,
        MetadataRecord::V1ProducerIds(_) => 11,
        MetadataRecord::V1DelegationToken(_) => 12,
        MetadataRecord::V1DeleteDelegationToken(_) => 13,
        MetadataRecord::V1UnregisterBroker(_) => 14,
        MetadataRecord::V1KRaftVersion(_) => 15,
        MetadataRecord::V1Voters(_) => 16,
        MetadataRecord::V1FeatureLevel(_) => 17,
        MetadataRecord::V1ClientMetricsConfig(_) => 18,
        MetadataRecord::V1FeaturesEpoch(_) => 19,
        MetadataRecord::V1PartitionDirAssignment(_) => 20,
        MetadataRecord::V1PartitionOffsetAdvance(_) => 21,
        MetadataRecord::V1GroupConfig(_) => 22,
        MetadataRecord::V1ControllerRegistration(_) => 23,
        MetadataRecord::V1TopicFreeze(_) => 24,
        MetadataRecord::V1BreakGlassProposal(_) => 25,
        MetadataRecord::V1DeleteBreakGlassProposal(_) => 26,
        MetadataRecord::V1PartitionElr(_) => 27,
        MetadataRecord::V1PartitionRecovery(_) => 28,
        MetadataRecord::V1PartitionUpdate(_) => 29,
        MetadataRecord::V1BrokerRegistrationChange(_) => 30,
        MetadataRecord::V1UnregisterController(_) => 31,
    }
}

fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn principal(name: &str) -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type: "User".into(),
        name: name.into(),
    }
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint {
        name: "PLAINTEXT".into(),
        host: "h".into(),
        port: 9092,
        protocol: ListenerProtocol::SaslSsl,
    }
}

fn configs() -> BTreeMap<String, String> {
    BTreeMap::from([("k".to_owned(), "v".to_owned())])
}

fn features() -> BTreeMap<String, (i16, i16)> {
    BTreeMap::from([("metadata.version".to_owned(), (1, 30))])
}

fn partition() -> PartitionRecord {
    PartitionRecord {
        topic: "t".into(),
        partition: 2,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2)],
        isr: vec![NodeId(1)],
        leader_epoch: LeaderEpoch(5),
        adding_replicas: vec![NodeId(2)],
        removing_replicas: vec![NodeId(3)],
        directories: vec![id(0xD1)],
        partition_epoch: 6,
    }
}

/// One value of every variant, in variant order, and its wincode bytes as hex.
/// The bytes are the krabka 1.x contract: never edit one to make a change pass.
fn rows() -> Vec<(MetadataRecord, &'static str)> {
    let mut rows = cluster_rows();
    rows.extend(raft_and_feature_rows());
    rows.extend(partition_and_kfc_rows());
    rows
}

/// Variants 0 to 10.
fn cluster_rows() -> Vec<(MetadataRecord, &'static str)> {
    vec![
        (
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: id(0x7),
                partitions: 3,
                replication_factor: 2,
            }),
            "00000000010000000000000074100000000000000000000000000000000000000000000007030000000200",
        ),
        (
            MetadataRecord::V1Partition(partition()),
            "010000000100000000000000740200000001000000000000000200000000000000010000000000000002000000000000000100000000000000010000000000000005000000010000000000000002000000000000000100000000000000030000000000000001000000000000001000000000000000000000000000000000000000000000d106000000",
        ),
        (
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: NodeId(1),
                broker_epoch: 10,
                incarnation_id: id(0x1C),
                host: "h".into(),
                port: 9092,
                rack: Some("r".into()),
                endpoints: vec![endpoint()],
                log_dirs: vec![id(0xD1)],
                fenced: true,
                in_controlled_shutdown: false,
                cordoned_log_dirs: Some(vec![id(0xD2)]),
                features: features(),
            }),
            "0200000001000000000000000a0000000000000010000000000000000000000000000000000000000000001c01000000000000006884230101000000000000007201000000000000000900000000000000504c41494e5445585401000000000000006884230300000001000000000000001000000000000000000000000000000000000000000000d101000101000000000000001000000000000000000000000000000000000000000000d2010000000000000010000000000000006d657461646174612e76657273696f6e01001e00",
        ),
        (
            MetadataRecord::V1DeleteTopic(DeleteTopicRecord { name: "t".into() }),
            "03000000010000000000000074",
        ),
        (
            MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides: configs(),
            }),
            "04000000010000000000000074010000000000000001000000000000006b010000000000000076",
        ),
        (
            MetadataRecord::V1ScramCredential(ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
                salt: vec![0x5A; 2],
                stored_key: vec![0x5B; 2],
                server_key: vec![0x5C; 2],
                iterations: 4096,
            }),
            "050000000500000000000000616c6963650200000002000000000000005a5a02000000000000005b5b02000000000000005c5c00100000",
        ),
        (
            MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha256,
            }),
            "060000000500000000000000616c69636501000000",
        ),
        (
            MetadataRecord::V1AccessControlEntry(AclEntry {
                resource_type: ResourceType::Group,
                resource_name: "g".into(),
                pattern_type: PatternType::Prefixed,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: AclOperation::Write,
                permission_type: PermissionType::Deny,
            }),
            "0700000001000000010000000000000067010000000a00000000000000557365723a616c69636501000000000000002a0200000001000000",
        ),
        (
            MetadataRecord::V1DeleteAccessControlEntry(AclEntryFilter {
                resource_type: Some(ResourceType::Topic),
                resource_name: None,
                pattern_type: Some(PatternType::Literal),
                principal: Some("User:bob".into()),
                host: None,
                operation: Some(AclOperation::Read),
                permission_type: Some(PermissionType::Allow),
            }),
            "080000000100000000000100000000010800000000000000557365723a626f620001010000000100000000",
        ),
        (
            MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(1),
                config_name: "k".into(),
                config_value: Some("v".into()),
            }),
            "09000000010000000000000001000000000000006b01010000000000000076",
        ),
        (
            MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: vec![
                    QuotaEntity {
                        entity_type: "user".into(),
                        entity_name: Some("alice".into()),
                    },
                    QuotaEntity {
                        entity_type: "client-id".into(),
                        entity_name: None,
                    },
                ],
                config_key: "producer_byte_rate".into(),
                config_value: Some(1024.5),
            }),
            "0a0000000200000000000000040000000000000075736572010500000000000000616c6963650900000000000000636c69656e742d696400120000000000000070726f64756365725f627974655f72617465010000000000029040",
        ),
    ]
}

/// Variants 11 to 20.
fn raft_and_feature_rows() -> Vec<(MetadataRecord, &'static str)> {
    vec![
        (
            MetadataRecord::V1ProducerIds(ProducerIdsRecord {
                broker_id: NodeId(1),
                broker_epoch: 10,
                next_producer_id: 1000,
            }),
            "0b00000001000000000000000a00000000000000e803000000000000",
        ),
        (
            MetadataRecord::V1DelegationToken(DelegationTokenRecord {
                token_id: "tok".into(),
                owner: principal("alice"),
                requester: principal("bob"),
                issue_timestamp_ms: 1,
                expiry_timestamp_ms: 2,
                max_timestamp_ms: 3,
                renewers: vec![principal("carol")],
            }),
            "0c0000000300000000000000746f6b0400000000000000557365720500000000000000616c6963650400000000000000557365720300000000000000626f62010000000000000002000000000000000300000000000000010000000000000004000000000000005573657205000000000000006361726f6c",
        ),
        (
            MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
                token_id: "tok".into(),
            }),
            "0d0000000300000000000000746f6b",
        ),
        (
            MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
                node_id: NodeId(1),
                broker_epoch: 10,
            }),
            "0e00000001000000000000000a00000000000000",
        ),
        (
            MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 }),
            "0f0000000100",
        ),
        (
            MetadataRecord::V1Voters(VotersRecord {
                voters: VoterSet::from_voters([Voter {
                    id: NodeId(1),
                    directory_id: id(0xD1),
                    endpoints: vec![VoterEndpoint {
                        name: "CONTROLLER".into(),
                        host: "h".into(),
                        port: 9093,
                    }],
                    kraft_version: KRaftVersionRange { min: 0, max: 1 },
                }]),
            }),
            "100000000100000000000000010000000000000001000000000000001000000000000000000000000000000000000000000000d101000000000000000a00000000000000434f4e54524f4c4c4552010000000000000068852300000100",
        ),
        (
            MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: "metadata.version".into(),
                level: 30,
            }),
            "1100000010000000000000006d657461646174612e76657273696f6e1e00",
        ),
        (
            MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
                name: "m".into(),
                configs: configs(),
            }),
            "1200000001000000000000006d010000000000000001000000000000006b010000000000000076",
        ),
        (
            MetadataRecord::V1FeaturesEpoch(FeaturesEpochRecord { epoch: 7 }),
            "130000000700000000000000",
        ),
        (
            MetadataRecord::V1PartitionDirAssignment(PartitionDirAssignmentRecord {
                topic: "t".into(),
                partition: 2,
                replica: NodeId(1),
                directory: id(0xD1),
            }),
            "140000000100000000000000740200000001000000000000001000000000000000000000000000000000000000000000d1",
        ),
    ]
}

/// Variants 21 to 31.
fn partition_and_kfc_rows() -> Vec<(MetadataRecord, &'static str)> {
    vec![
        (
            MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
                topic: "t".into(),
                partition: 2,
                count: 9,
            }),
            "15000000010000000000000074020000000900000000000000",
        ),
        (
            MetadataRecord::V1GroupConfig(GroupConfigRecord {
                group_id: "g".into(),
                configs: configs(),
            }),
            "16000000010000000000000067010000000000000001000000000000006b010000000000000076",
        ),
        (
            MetadataRecord::V1ControllerRegistration(ControllerRegistrationRecord {
                node_id: NodeId(3000),
                incarnation_id: id(0x1C),
                zk_migration_ready: false,
                endpoints: vec![endpoint()],
                features: features(),
            }),
            "17000000b80b00000000000010000000000000000000000000000000000000000000001c0001000000000000000900000000000000504c41494e54455854010000000000000068842303000000010000000000000010000000000000006d657461646174612e76657273696f6e01001e00",
        ),
        (
            MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
                scope: "tenant-a.".into(),
                pattern_type: PatternType::Prefixed,
                frozen: true,
                reason: "dr".into(),
                set_by: "User:alice".into(),
                set_at_ms: 1_700_000_000_000,
                proposal_id: id(0xB1),
                key_id: "k".into(),
                signature: vec![0xAB; 2],
            }),
            "18000000090000000000000074656e616e742d612e0100000001020000000000000064720a00000000000000557365723a616c6963650068e5cf8b0100001000000000000000000000000000000000000000000000b101000000000000006b0200000000000000abab",
        ),
        (
            MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
                proposal_id: id(0xB1),
                action: BreakGlassAction::UncleanElectLeaders,
                target: "literal:orders".into(),
                proposer: "User:alice".into(),
                reason: "r".into(),
                created_at_ms: 1,
                expires_at_ms: 2,
                approvals: vec![BreakGlassApproval {
                    principal: "User:bob".into(),
                    approved_at_ms: 3,
                    key_id: "k".into(),
                    signature: vec![0xCD; 2],
                }],
                consumed_at_ms: 4,
                withdrawn: true,
            }),
            "190000001000000000000000000000000000000000000000000000b1010000000e000000000000006c69746572616c3a6f72646572730a00000000000000557365723a616c6963650100000000000000720100000000000000020000000000000001000000000000000800000000000000557365723a626f62030000000000000001000000000000006b0200000000000000cdcd040000000000000001",
        ),
        (
            MetadataRecord::V1DeleteBreakGlassProposal(id(0xB1)),
            "1a0000001000000000000000000000000000000000000000000000b1",
        ),
        (
            MetadataRecord::V1PartitionElr(PartitionElrRecord {
                topic: "t".into(),
                partition: 2,
                eligible_leader_replicas: vec![NodeId(2)],
                last_known_elr: vec![NodeId(3)],
            }),
            "1b000000010000000000000074020000000100000000000000020000000000000001000000000000000300000000000000",
        ),
        (
            MetadataRecord::V1PartitionRecovery(PartitionRecoveryRecord {
                topic: "t".into(),
                partition: 2,
                state: LeaderRecoveryState::Recovering,
            }),
            "1c0000000100000000000000740200000001000000",
        ),
        (
            MetadataRecord::V1PartitionUpdate(PartitionUpdateRecord {
                partition: partition(),
                eligible_leader_replicas: Some(vec![NodeId(2)]),
                last_known_elr: None,
                recovery_state: Some(LeaderRecoveryState::Recovered),
            }),
            "1d0000000100000000000000740200000001000000000000000200000000000000010000000000000002000000000000000100000000000000010000000000000005000000010000000000000002000000000000000100000000000000030000000000000001000000000000001000000000000000000000000000000000000000000000d1060000000101000000000000000200000000000000000100000000",
        ),
        (
            MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
                node_id: NodeId(1),
                broker_epoch: 10,
                fenced: FencingChange::Unfence,
                in_controlled_shutdown: true,
                log_dirs: vec![id(0xD1)],
                cordoned_log_dirs: None,
            }),
            "1e00000001000000000000000a00000000000000000000000101000000000000001000000000000000000000000000000000000000000000d100",
        ),
        (
            MetadataRecord::V1UnregisterController(UnregisterControllerRecord {
                node_id: NodeId(3000),
            }),
            "1f000000b80b000000000000",
        ),
    ]
}

/// Lower-case hex of `bytes`, so a fixture mismatch reads as a diff.
pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|b| [DIGITS[usize::from(b >> 4)], DIGITS[usize::from(b & 0xF)]])
        .map(char::from)
        .collect()
}

/// The bytes of a hex fixture.
pub(crate) fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// The table holds exactly one row per variant, in variant order.
#[test]
fn the_table_covers_every_variant_in_order() {
    let indices: Vec<u32> = rows().iter().map(|(rec, _)| variant_index(rec)).collect();
    let want: Vec<u32> = (0..32).collect();
    check!(indices == want);
}

#[test]
fn every_variant_encodes_to_its_golden_bytes() {
    for (rec, golden) in rows() {
        let index = variant_index(&rec);
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(&rec).unwrap();
        check!(hex(&bytes) == golden, "variant {index}");
    }
}

#[test]
fn every_golden_decodes_to_its_value() {
    for (rec, golden) in rows() {
        let index = variant_index(&rec);
        let decoded = <SerdeCompat<MetadataRecord>>::deserialize(&unhex(golden));
        check!(decoded.ok() == Some(rec), "variant {index}");
    }
}

/// A variant index past the last variant, as a later 1.x broker might write
/// before the operator finalizes its feature level, is a decode error.
#[test]
fn an_unknown_variant_index_does_not_decode() {
    let decoded = <SerdeCompat<MetadataRecord>>::deserialize(&32u32.to_le_bytes());
    check!(decoded.is_err());
}
