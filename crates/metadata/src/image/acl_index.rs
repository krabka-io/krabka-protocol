//! The in-memory ACL index of a [`MetadataImage`](super::MetadataImage).
//!
//! The authorizer asks for the ACLs that match one resource on every
//! authorized request, so a lookup allocates nothing and does not scan every
//! prefixed ACL of the resource type:
//!
//! - Literal ACLs sit in a map keyed by resource type and then by resource
//!   name, looked up by `&str`. The literal `*`, Kafka's `WILDCARD_RESOURCE`,
//!   has a bucket of its own beside that map.
//! - Prefixed ACLs sit in a map keyed by prefix, beside the set of distinct
//!   prefix lengths. A lookup probes the map once per distinct length that
//!   fits the resource name, with that many leading bytes of the name.
//!
//! The index is never serialized: a snapshot writes the image's ACLs as
//! `V1AccessControlEntry` records.

use std::collections::{BTreeMap, HashMap};

use crate::acl::{AclEntry, AclEntryFilter, PatternType, ResourceType};

/// Kafka's `WILDCARD_RESOURCE`: a literal ACL with this name matches every
/// resource of its type.
const WILDCARD_RESOURCE: &str = "*";

/// Every ACL of an image, indexed for [`AclIndex::matching`].
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct AclIndex {
    literal: HashMap<ResourceType, LiteralAcls>,
    prefixed: HashMap<ResourceType, PrefixedAcls>,
}

/// The literal ACLs of one resource type.
#[derive(Debug, Clone, Default, PartialEq)]
struct LiteralAcls {
    /// Entries by resource name, the wildcard name excepted.
    by_name: HashMap<String, Vec<AclEntry>>,
    /// Entries whose resource name is [`WILDCARD_RESOURCE`].
    wildcard: Vec<AclEntry>,
}

/// The prefixed ACLs of one resource type.
#[derive(Debug, Clone, Default, PartialEq)]
struct PrefixedAcls {
    by_prefix: HashMap<String, Vec<AclEntry>>,
    /// Each distinct prefix length in bytes, with the number of prefixes in
    /// `by_prefix` that have it.
    lengths: BTreeMap<usize, usize>,
}

impl AclIndex {
    /// Adds `entry`, replacing an identical entry so that each ACL is held
    /// once.
    pub(super) fn insert(&mut self, entry: &AclEntry) {
        let bucket = match entry.pattern_type {
            PatternType::Literal => self
                .literal
                .entry(entry.resource_type)
                .or_default()
                .bucket(&entry.resource_name),
            PatternType::Prefixed => self
                .prefixed
                .entry(entry.resource_type)
                .or_default()
                .bucket(&entry.resource_name),
        };
        // Last-write-wins on full-tuple equality.
        bucket.retain(|e| e != entry);
        bucket.push(entry.clone());
    }

    /// Removes every entry that `filter` matches.
    pub(super) fn remove_matching(&mut self, filter: &AclEntryFilter) {
        let keep = |e: &AclEntry| !filter.matches(e);
        self.literal.retain(|_, acls| {
            acls.retain(keep);
            !acls.is_empty()
        });
        self.prefixed.retain(|_, acls| {
            acls.retain(keep);
            !acls.is_empty()
        });
    }

    /// Every entry that could match resource `name` of type `resource_type`:
    /// the literal entries for `name`, the literal wildcard entries, and the
    /// prefixed entries whose prefix `name` starts with.
    pub(super) fn matching<'a>(
        &'a self,
        resource_type: ResourceType,
        name: &'a str,
    ) -> impl Iterator<Item = &'a AclEntry> + 'a {
        let literal = self.literal.get(&resource_type);
        // The wildcard name is never a key of `by_name`, so a lookup of `*`
        // itself yields the wildcard entries once.
        let named = literal
            .and_then(|acls| acls.by_name.get(name))
            .into_iter()
            .flatten();
        let wildcard = literal.into_iter().flat_map(|acls| &acls.wildcard);
        let prefixed = self
            .prefixed
            .get(&resource_type)
            .into_iter()
            .flat_map(move |acls| acls.matching(name));
        named.chain(wildcard).chain(prefixed)
    }

    /// Every entry, literal and prefixed, across all resource types.
    pub(super) fn iter(&self) -> impl Iterator<Item = &AclEntry> {
        let literal = self
            .literal
            .values()
            .flat_map(|acls| acls.by_name.values().flatten().chain(&acls.wildcard));
        let prefixed = self
            .prefixed
            .values()
            .flat_map(|acls| acls.by_prefix.values().flatten());
        literal.chain(prefixed)
    }
}

impl LiteralAcls {
    fn bucket(&mut self, name: &str) -> &mut Vec<AclEntry> {
        if name == WILDCARD_RESOURCE {
            &mut self.wildcard
        } else {
            self.by_name.entry(name.to_owned()).or_default()
        }
    }

    fn retain(&mut self, keep: impl Fn(&AclEntry) -> bool) {
        self.by_name.retain(|_, bucket| {
            bucket.retain(&keep);
            !bucket.is_empty()
        });
        self.wildcard.retain(keep);
    }

    fn is_empty(&self) -> bool {
        self.by_name.is_empty() && self.wildcard.is_empty()
    }
}

impl PrefixedAcls {
    fn bucket(&mut self, prefix: &str) -> &mut Vec<AclEntry> {
        let Self { by_prefix, lengths } = self;
        by_prefix.entry(prefix.to_owned()).or_insert_with(|| {
            *lengths.entry(prefix.len()).or_default() += 1;
            Vec::new()
        })
    }

    fn retain(&mut self, keep: impl Fn(&AclEntry) -> bool) {
        let Self { by_prefix, lengths } = self;
        by_prefix.retain(|prefix, bucket| {
            bucket.retain(&keep);
            if bucket.is_empty() {
                forget_length(lengths, prefix.len());
            }
            !bucket.is_empty()
        });
    }

    fn is_empty(&self) -> bool {
        self.by_prefix.is_empty()
    }

    /// The entries whose prefix `name` starts with, shortest prefix first.
    ///
    /// A prefix that `name` starts with is `name`'s first `len` bytes for
    /// some stored prefix length `len`, so one map probe per distinct length
    /// finds them all. A length that splits a character of `name` cannot
    /// belong to a matching prefix, because a stored prefix is valid UTF-8.
    fn matching<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a AclEntry> + 'a {
        self.lengths
            .range(..=name.len())
            .filter_map(move |(&len, _)| name.get(..len))
            .filter_map(|prefix| self.by_prefix.get(prefix))
            .flatten()
    }
}

/// Drops one prefix of `len` bytes from the length counts.
fn forget_length(lengths: &mut BTreeMap<usize, usize>, len: usize) {
    if let Some(count) = lengths.get_mut(&len) {
        *count -= 1;
        if *count == 0 {
            lengths.remove(&len);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::slice;

    use proptest::{collection, option, prelude::*};

    use super::*;
    use crate::acl::{AclOperation, PermissionType};

    fn entry(resource_type: ResourceType, pattern_type: PatternType, name: &str) -> AclEntry {
        AclEntry {
            resource_type,
            resource_name: name.to_owned(),
            pattern_type,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }
    }

    /// The linear scan the index replaces: what Kafka's authorizer matches.
    fn reference_matches<'a>(
        entries: &'a [AclEntry],
        resource_type: ResourceType,
        name: &str,
    ) -> Vec<&'a AclEntry> {
        entries
            .iter()
            .filter(|e| e.resource_type == resource_type)
            .filter(|e| match e.pattern_type {
                PatternType::Literal => {
                    e.resource_name == name || e.resource_name == WILDCARD_RESOURCE
                }
                PatternType::Prefixed => name.starts_with(&e.resource_name),
            })
            .collect()
    }

    /// `entries` in a canonical order, for comparing match sets.
    fn sorted(mut entries: Vec<&AclEntry>) -> Vec<&AclEntry> {
        entries.sort_by_cached_key(|e| format!("{e:?}"));
        entries
    }

    fn index_of(entries: &[AclEntry]) -> AclIndex {
        let mut index = AclIndex::default();
        for e in entries {
            index.insert(e);
        }
        index
    }

    #[test]
    fn matching_finds_literal_wildcard_and_every_matching_prefix() {
        let topic = ResourceType::Topic;
        let entries = vec![
            entry(topic, PatternType::Literal, "team-a.orders"),
            entry(topic, PatternType::Literal, "team-a.other"),
            entry(topic, PatternType::Literal, WILDCARD_RESOURCE),
            entry(topic, PatternType::Prefixed, ""),
            entry(topic, PatternType::Prefixed, "team-"),
            entry(topic, PatternType::Prefixed, "team-a."),
            entry(topic, PatternType::Prefixed, "team-b."),
            entry(topic, PatternType::Prefixed, "team-a.orders.long"),
            entry(ResourceType::Group, PatternType::Literal, "team-a.orders"),
            entry(ResourceType::Group, PatternType::Prefixed, "team-"),
        ];
        let index = index_of(&entries);
        for name in [
            "team-a.orders",
            "team-a.other",
            "team-b.x",
            "team-",
            "team",
            "",
            WILDCARD_RESOURCE,
            "unrelated",
        ] {
            assert2::assert!(
                sorted(index.matching(topic, name).collect())
                    == sorted(reference_matches(&entries, topic, name)),
                "{name}"
            );
        }
    }

    /// The wildcard entries match once, also when the requested name is the
    /// wildcard itself.
    #[test]
    fn wildcard_lookup_yields_each_wildcard_entry_once() {
        let wildcard = entry(ResourceType::Topic, PatternType::Literal, WILDCARD_RESOURCE);
        let index = index_of(slice::from_ref(&wildcard));
        for name in [WILDCARD_RESOURCE, "orders"] {
            assert2::assert!(
                index
                    .matching(ResourceType::Topic, name)
                    .collect::<Vec<_>>()
                    == vec![&wildcard]
            );
        }
    }

    /// A stored prefix length that splits a multi-byte character of the name
    /// is skipped, not a panic, and the whole-character prefixes still match.
    #[test]
    fn prefix_lengths_inside_a_character_are_skipped() {
        let topic = ResourceType::Topic;
        let entries = vec![
            entry(topic, PatternType::Prefixed, "ab"),
            entry(topic, PatternType::Prefixed, "é"),
            entry(topic, PatternType::Prefixed, "éa"),
        ];
        let index = index_of(&entries);
        for name in ["é", "éab", "aé", "abc"] {
            assert2::assert!(
                sorted(index.matching(topic, name).collect())
                    == sorted(reference_matches(&entries, topic, name)),
                "{name}"
            );
        }
    }

    /// Removing every entry of a prefix also drops its length, so the index
    /// of an emptied image equals a fresh one.
    #[test]
    fn removing_everything_leaves_an_empty_index() {
        let entries = vec![
            entry(ResourceType::Topic, PatternType::Literal, "orders"),
            entry(ResourceType::Topic, PatternType::Literal, WILDCARD_RESOURCE),
            entry(ResourceType::Topic, PatternType::Prefixed, "team-"),
            entry(ResourceType::Group, PatternType::Prefixed, "g"),
        ];
        let mut index = index_of(&entries);
        index.remove_matching(&AclEntryFilter {
            pattern_type: Some(PatternType::Prefixed),
            resource_type: Some(ResourceType::Topic),
            ..AclEntryFilter::default()
        });
        assert2::assert!(
            index == index_of(&[entries[0].clone(), entries[1].clone(), entries[3].clone()])
        );

        index.remove_matching(&AclEntryFilter::default());
        assert2::assert!(index == AclIndex::default());
    }

    fn arb_entry() -> impl Strategy<Value = AclEntry> {
        let resource_type = prop_oneof![Just(ResourceType::Topic), Just(ResourceType::Group)];
        let pattern_type = prop_oneof![Just(PatternType::Literal), Just(PatternType::Prefixed)];
        let name = prop_oneof![Just(WILDCARD_RESOURCE.to_owned()), "[ab*é]{0,4}"];
        let permission = prop_oneof![Just(PermissionType::Allow), Just(PermissionType::Deny)];
        (resource_type, pattern_type, name, permission).prop_map(
            |(resource_type, pattern_type, name, permission_type)| AclEntry {
                permission_type,
                ..entry(resource_type, pattern_type, &name)
            },
        )
    }

    proptest! {
        /// After any inserts and filtered removals, a lookup returns exactly
        /// the entries the linear reference scan matches.
        #[test]
        fn matching_agrees_with_the_linear_scan(
            inserted in collection::vec(arb_entry(), 0..24),
            removed in option::of(arb_entry()),
            names in collection::vec("[ab*é]{0,5}", 1..8),
        ) {
            let mut index = index_of(&inserted);
            let mut expected: Vec<AclEntry> = Vec::new();
            for e in &inserted {
                expected.retain(|kept| kept != e);
                expected.push(e.clone());
            }
            if let Some(removed) = removed {
                let filter = AclEntryFilter {
                    resource_type: Some(removed.resource_type),
                    resource_name: Some(removed.resource_name.clone()),
                    pattern_type: Some(removed.pattern_type),
                    ..AclEntryFilter::default()
                };
                index.remove_matching(&filter);
                expected.retain(|e| !filter.matches(e));
            }
            for name in &names {
                for resource_type in [ResourceType::Topic, ResourceType::Group] {
                    assert2::assert!(
                        sorted(index.matching(resource_type, name).collect())
                            == sorted(reference_matches(&expected, resource_type, name))
                    );
                }
            }
            assert2::assert!(sorted(index.iter().collect()) == sorted(expected.iter().collect()));
        }
    }
}
