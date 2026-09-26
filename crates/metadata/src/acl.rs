//! Wire-stable ACL types replicated through the raft quorum
//! with `MetadataRecord::V1AccessControlEntry` and
//! `V1DeleteAccessControlEntry`. They mirror the shape that Kafka exposes on
//! the `CreateAcls`, `DeleteAcls`, and `DescribeAcls` wire messages, but they
//! stay pure data. The authorizer in `krabka-broker` evaluates them.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    /// KIP-48 delegation tokens. The resource name is the `KafkaPrincipal`
    /// string form of the owner, for example `"User:alice"`. The pattern types
    /// `LITERAL` and `PREFIXED` apply through the existing matcher. Only the
    /// `Describe` operation is externally grantable, because `Create`,
    /// `Renew`, and `Expire` are implicit on ownership.
    DelegationToken,
    /// KIP-373 users. The resource name is a user principal name, without the
    /// `User:` prefix. `CreateTokens` and `DescribeTokens` on a `User`
    /// resource let one principal create or describe delegation tokens owned
    /// by another. Kafka wire discriminant 7.
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PatternType {
    Literal,
    Prefixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PermissionType {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AclOperation {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
    /// KIP-939: permission to take part in two-phase commit (2PC) on a
    /// `TransactionalId`. An `InitProducerId` that carries `enable2Pc=true`
    /// needs it in addition to `Write`. Kafka wire discriminant 15.
    TwoPhaseCommit,
    /// KIP-373: permission to create a delegation token on behalf of the
    /// `User` resource. Kafka wire discriminant 13.
    CreateTokens,
    /// KIP-373: permission to describe the delegation tokens the `User`
    /// resource owns. Kafka wire discriminant 14.
    DescribeTokens,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclEntry {
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub pattern_type: PatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission_type: PermissionType,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclEntryFilter {
    pub resource_type: Option<ResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: Option<PatternType>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<AclOperation>,
    pub permission_type: Option<PermissionType>,
}

impl AclEntryFilter {
    /// Returns true if every populated axis matches `entry`. `None` axes
    /// match anything.
    #[must_use]
    pub fn matches(&self, entry: &AclEntry) -> bool {
        self.resource_type
            .is_none_or(|rt| rt == entry.resource_type)
            && self
                .resource_name
                .as_ref()
                .is_none_or(|rn| rn == &entry.resource_name)
            && self.pattern_type.is_none_or(|pt| pt == entry.pattern_type)
            && self
                .principal
                .as_ref()
                .is_none_or(|p| p == &entry.principal)
            && self.host.as_ref().is_none_or(|h| h == &entry.host)
            && self.operation.is_none_or(|op| op == entry.operation)
            && self
                .permission_type
                .is_none_or(|pt| pt == entry.permission_type)
    }
}

#[cfg(test)]
mod tests {

    use serde_wincode::SerdeCompat;
    use wincode::{Deserialize as _, Serialize as _};

    use super::*;

    fn rt<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug,
    {
        let bytes = <SerdeCompat<T>>::serialize(value).unwrap();
        <SerdeCompat<T>>::deserialize(&bytes).unwrap()
    }

    #[test]
    fn acl_entry_round_trip() {
        let entry = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "foo".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        assert2::assert!(rt(&entry) == entry);
    }

    /// The metadata log stores ACL enums by variant index, so a variant
    /// inserted anywhere but the end would make every stored entry after it
    /// decode as its neighbour. Pin the encoding of every variant.
    #[test]
    fn acl_enum_log_encoding_is_stable() {
        let index = |i: u32| i.to_le_bytes().to_vec();
        let resource_types = [
            (ResourceType::Topic, 0),
            (ResourceType::Group, 1),
            (ResourceType::Cluster, 2),
            (ResourceType::TransactionalId, 3),
            (ResourceType::DelegationToken, 4),
            (ResourceType::User, 5),
        ];
        for (variant, want) in resource_types {
            let bytes = <SerdeCompat<ResourceType>>::serialize(&variant).unwrap();
            assert2::assert!((variant, bytes) == (variant, index(want)));
        }
        let operations = [
            (AclOperation::All, 0),
            (AclOperation::Read, 1),
            (AclOperation::Write, 2),
            (AclOperation::Create, 3),
            (AclOperation::Delete, 4),
            (AclOperation::Alter, 5),
            (AclOperation::Describe, 6),
            (AclOperation::ClusterAction, 7),
            (AclOperation::DescribeConfigs, 8),
            (AclOperation::AlterConfigs, 9),
            (AclOperation::IdempotentWrite, 10),
            (AclOperation::TwoPhaseCommit, 11),
            (AclOperation::CreateTokens, 12),
            (AclOperation::DescribeTokens, 13),
        ];
        for (variant, want) in operations {
            let bytes = <SerdeCompat<AclOperation>>::serialize(&variant).unwrap();
            assert2::assert!((variant, bytes) == (variant, index(want)));
        }
        for (variant, want) in [(PatternType::Literal, 0), (PatternType::Prefixed, 1)] {
            let bytes = <SerdeCompat<PatternType>>::serialize(&variant).unwrap();
            assert2::assert!((variant, bytes) == (variant, index(want)));
        }
        for (variant, want) in [(PermissionType::Allow, 0), (PermissionType::Deny, 1)] {
            let bytes = <SerdeCompat<PermissionType>>::serialize(&variant).unwrap();
            assert2::assert!((variant, bytes) == (variant, index(want)));
        }
    }

    /// KIP-373's `User` resource and token operations survive the metadata
    /// log's serde encoding like every other variant.
    #[test]
    fn kip_373_entries_round_trip() {
        for operation in [AclOperation::CreateTokens, AclOperation::DescribeTokens] {
            let entry = AclEntry {
                resource_type: ResourceType::User,
                resource_name: "alice".into(),
                pattern_type: PatternType::Literal,
                principal: "User:bob".into(),
                host: "*".into(),
                operation,
                permission_type: PermissionType::Allow,
            };
            assert2::assert!(rt(&entry) == entry);
        }
    }

    #[test]
    fn acl_entry_filter_round_trip() {
        let filter = AclEntryFilter {
            resource_type: Some(ResourceType::Group),
            resource_name: None,
            pattern_type: Some(PatternType::Prefixed),
            principal: Some("User:bob".into()),
            host: None,
            operation: Some(AclOperation::All),
            permission_type: None,
        };
        assert2::assert!(rt(&filter) == filter);
    }

    #[test]
    fn filter_with_all_none_matches_anything() {
        let f = AclEntryFilter::default();
        let entry = AclEntry {
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster".into(),
            pattern_type: PatternType::Literal,
            principal: "User:admin".into(),
            host: "*".into(),
            operation: AclOperation::All,
            permission_type: PermissionType::Allow,
        };
        assert2::assert!(f.matches(&entry));
    }

    #[test]
    fn filter_with_specific_axis_filters_correctly() {
        let f = AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            ..AclEntryFilter::default()
        };
        let topic_entry = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "foo".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        let group_entry = AclEntry {
            resource_type: ResourceType::Group,
            ..topic_entry.clone()
        };
        assert2::assert!((f.matches(&topic_entry), f.matches(&group_entry)) == (true, false));
    }
}
