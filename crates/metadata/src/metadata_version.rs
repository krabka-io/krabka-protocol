//! KIP-778 `metadata.version` feature-level model. The canonical
//! string<->integer-level table, mirrored byte-for-byte from upstream
//! Kafka's `MetadataVersion` enum over the range Krabka advertises
//! (`[METADATA_VERSION_MIN, METADATA_VERSION_MAX]`). JVM clients call
//! `MetadataVersion.fromFeatureLevel(N)` and throw on any level their
//! enum does not know, so the levels and `X.Y-IVn` names here MUST match
//! upstream exactly. Verify against `MetadataVersion.java` in Kafka's
//! `server-common` module on trunk before editing: levels are never reused, so
//! the table must carry every level up to [`METADATA_VERSION_MAX`], reserved
//! ones included.

/// The `metadata.version` feature name (KIP-584 / KIP-778).
pub const METADATA_VERSION_FEATURE: &str = "metadata.version";

/// Krabka registration-only marker for KIP-1155 downgrade support. The KIP is
/// still under discussion and has not assigned its promised capability
/// `metadata.version` level, so this must not extend the canonical
/// metadata-version range or appear in `ApiVersions`. It is carried only in
/// broker/controller registration feature maps; pre-KIP JVM nodes omit it.
pub const METADATA_DOWNGRADE_CAPABILITY_FEATURE: &str = "krabka.metadata.downgrade";
/// The only supported level of [`METADATA_DOWNGRADE_CAPABILITY_FEATURE`].
pub const METADATA_DOWNGRADE_CAPABILITY_LEVEL: i16 = 1;

/// The `share.version` feature name (KIP-932). Gates share-group membership.
pub const SHARE_VERSION_FEATURE: &str = "share.version";
/// KIP-853 Raft protocol and dynamic-membership feature.
pub const KRAFT_VERSION_FEATURE: &str = "kraft.version";
/// Minimum supported `share.version` level: `0` (feature disabled).
pub const SHARE_VERSION_MIN: i16 = 0;
/// Maximum supported `share.version` level: `1` (KIP-932 GA).
pub const SHARE_VERSION_MAX: i16 = 1;

/// The `eligible.leader.replicas.version` feature name (KIP-966). Gates the
/// controller's maintenance of eligible leader replicas.
pub const ELR_VERSION_FEATURE: &str = "eligible.leader.replicas.version";
/// Minimum supported `eligible.leader.replicas.version` level: `0`, `ELRV_0`,
/// the level at which the controller keeps no ELR.
pub const ELR_VERSION_MIN: i16 = 0;
/// Maximum supported `eligible.leader.replicas.version` level: `1`, `ELRV_1`,
/// which turns KIP-966 ELR maintenance on.
pub const ELR_VERSION_MAX: i16 = 1;

/// The `streams.version` feature name (KIP-1071). Gates the broker-side
/// Streams rebalance protocol (`StreamsGroupHeartbeat` / `StreamsGroupDescribe`).
pub const STREAMS_VERSION_FEATURE: &str = "streams.version";
/// Minimum supported `streams.version` level: `0` (feature disabled).
pub const STREAMS_VERSION_MIN: i16 = 0;
/// Maximum supported `streams.version` level: `1` (KIP-1071 early access).
pub const STREAMS_VERSION_MAX: i16 = 1;

/// Minimum supported level: `3.3-IV3` (`KRaft` GA), the floor that real Kafka
/// 4.0 supports.
pub const METADATA_VERSION_MIN: i16 = 7;
/// Maximum supported level: `4.4-IV2` (KIP-1312 controller unregistration).
pub const METADATA_VERSION_MAX: i16 = 33;

/// Level at which `KRaft` gained SCRAM credentials (`3.5-IV2`).
pub const SCRAM_MIN_LEVEL: i16 = 11;
/// Level at which `KRaft` gained delegation tokens (`3.6-IV2`).
pub const DELEGATION_TOKEN_MIN_LEVEL: i16 = 14;
/// Lowest level that supports controller registrations (`3.7-IV0`). Online
/// downgrades cannot cross this boundary because the active controller needs
/// those registrations to verify every quorum member supports the target.
pub const ONLINE_DOWNGRADE_MIN_LEVEL: i16 = 15;
/// Level at which partition directory assignments became part of the `KRaft`
/// metadata records (`3.7-IV2`, KIP-858).
pub const DIRECTORY_ASSIGNMENT_MIN_LEVEL: i16 = 17;
/// Level at which partition records gained KIP-966 eligible-leader fields
/// (`4.0-IV1`). It selects the record version Kafka readers expect, and it is
/// what [`crate::feature::ElrVersionFeature`] depends on at level 1.
pub const ELR_MIN_LEVEL: i16 = 23;
/// Level at which `ELRV_1` becomes the bootstrap default (`4.1-IV0`,
/// `EligibleLeaderReplicasVersion.ELRV_1`'s bootstrap metadata version).
pub const ELR_DEFAULT_METADATA_LEVEL: i16 = 26;
/// Level at which `share.version` 1 becomes the bootstrap default (`4.2-IV0`,
/// `ShareVersion.SV_1`'s bootstrap metadata version, KIP-932 GA).
pub const SHARE_VERSION_DEFAULT_METADATA_LEVEL: i16 = 28;
/// Level at which `streams.version` 1 becomes the bootstrap default (`4.2-IV1`,
/// `StreamsVersion.SV_1`'s bootstrap metadata version, KIP-1071 GA).
pub const STREAMS_VERSION_DEFAULT_METADATA_LEVEL: i16 = 29;
/// Level at which `RegisterBrokerRecord` and `BrokerRegistrationChangeRecord`
/// carry KIP-1066 cordoned log directories (`4.3-IV0`).
pub const CORDONED_LOG_DIRS_MIN_LEVEL: i16 = 30;
/// Level at which ACL host patterns may be CIDR blocks (`4.4-IV1`, KIP-1276).
pub const CIDR_ACL_MIN_LEVEL: i16 = 32;
/// Level at which a controller may be unregistered (`4.4-IV2`, KIP-1312):
/// Kafka's `MetadataVersion.isControllerUnregistrationSupported`.
pub const CONTROLLER_UNREGISTRATION_MIN_LEVEL: i16 = 33;

/// One `metadata.version` level: its integer feature level, canonical
/// `X.Y-IVn` name, short `X.Y` form, and Kafka's `didMetadataChange` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataVersion {
    level: i16,
    ivn: &'static str,
    short: &'static str,
    did_metadata_change: bool,
}

impl MetadataVersion {
    #[must_use]
    pub fn feature_level(self) -> i16 {
        self.level
    }
    #[must_use]
    pub fn ivn(self) -> &'static str {
        self.ivn
    }
    #[must_use]
    pub fn short(self) -> &'static str {
        self.short
    }
    /// Kafka's `MetadataVersion.didMetadataChange`: whether this level changed
    /// the metadata record format, so a downgrade across it can lose data.
    #[must_use]
    pub fn did_metadata_change(self) -> bool {
        self.did_metadata_change
    }
}

const TABLE: &[MetadataVersion] = &[
    MetadataVersion {
        level: 7,
        ivn: "3.3-IV3",
        short: "3.3",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 8,
        ivn: "3.4-IV0",
        short: "3.4",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 9,
        ivn: "3.5-IV0",
        short: "3.5",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 10,
        ivn: "3.5-IV1",
        short: "3.5",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 11,
        ivn: "3.5-IV2",
        short: "3.5",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 12,
        ivn: "3.6-IV0",
        short: "3.6",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 13,
        ivn: "3.6-IV1",
        short: "3.6",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 14,
        ivn: "3.6-IV2",
        short: "3.6",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 15,
        ivn: "3.7-IV0",
        short: "3.7",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 16,
        ivn: "3.7-IV1",
        short: "3.7",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 17,
        ivn: "3.7-IV2",
        short: "3.7",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 18,
        ivn: "3.7-IV3",
        short: "3.7",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 19,
        ivn: "3.7-IV4",
        short: "3.7",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 20,
        ivn: "3.8-IV0",
        short: "3.8",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 21,
        ivn: "3.9-IV0",
        short: "3.9",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 22,
        ivn: "4.0-IV0",
        short: "4.0",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 23,
        ivn: "4.0-IV1",
        short: "4.0",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 24,
        ivn: "4.0-IV2",
        short: "4.0",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 25,
        ivn: "4.0-IV3",
        short: "4.0",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 26,
        ivn: "4.1-IV0",
        short: "4.1",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 27,
        ivn: "4.1-IV1",
        short: "4.1",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 28,
        ivn: "4.2-IV0",
        short: "4.2",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 29,
        ivn: "4.2-IV1",
        short: "4.2",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 30,
        ivn: "4.3-IV0",
        short: "4.3",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 31,
        ivn: "4.4-IV0",
        short: "4.4",
        did_metadata_change: false,
    },
    MetadataVersion {
        level: 32,
        ivn: "4.4-IV1",
        short: "4.4",
        did_metadata_change: true,
    },
    MetadataVersion {
        level: 33,
        ivn: "4.4-IV2",
        short: "4.4",
        did_metadata_change: true,
    },
];

/// Look up a level by integer feature level. `None` if outside the
/// supported table.
#[must_use]
pub fn from_feature_level(level: i16) -> Option<MetadataVersion> {
    TABLE.iter().copied().find(|m| m.level == level)
}

/// Resolve a version string to a level. The function accepts both the exact
/// `X.Y-IVn` form and the short `X.Y` form. The short form resolves to the
/// highest level within that minor, which matches
/// `MetadataVersion.fromVersionString`.
#[must_use]
pub fn from_version_string(s: &str) -> Option<MetadataVersion> {
    let s = s.trim();
    if s.contains('-') {
        return TABLE.iter().copied().find(|m| m.ivn == s);
    }
    TABLE
        .iter()
        .copied()
        .filter(|m| m.short == s)
        .max_by_key(|m| m.level)
}

/// True if `level` is within `[METADATA_VERSION_MIN, METADATA_VERSION_MAX]`.
#[must_use]
pub fn is_supported_level(level: i16) -> bool {
    (METADATA_VERSION_MIN..=METADATA_VERSION_MAX).contains(&level)
}

/// Kafka's `didMetadataChange` flag for `level`, or `None` for a level outside
/// the supported table. Levels up to `4.3-IV0` (30) follow Kafka 4.3.1, and
/// `4.4-IV0` (31) to `4.4-IV2` (33) follow Kafka trunk.
#[must_use]
pub fn did_metadata_change(level: i16) -> Option<bool> {
    from_feature_level(level).map(MetadataVersion::did_metadata_change)
}

/// Kafka's `MetadataVersion.checkIfMetadataChanged`: whether moving between
/// `from` and `to`, in either direction, crosses a level that changed the
/// metadata record format. Equal levels never do.
///
/// Kafka walks down from the higher level and answers false only when it
/// reaches the lower level without passing a changing level. The lower level
/// itself does not count, because a cluster already at it has its records. A
/// level outside the supported table counts as changing, so a range that
/// leaves the table answers true, as Kafka does when it runs out of
/// predecessors.
#[must_use]
pub fn metadata_changed_between(from: i16, to: i16) -> bool {
    let (low, high) = if from <= to { (from, to) } else { (to, from) };
    ((low + 1)..=high).any(|level| did_metadata_change(level).unwrap_or(true))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Kafka's `didMetadataChange` flag per level: `MetadataVersion.java` at
    /// 4.3.1 for 7-30, at trunk for 31 (`4.4-IV0`) to 33 (`4.4-IV2`).
    const KAFKA_DID_METADATA_CHANGE: [(i16, bool); 27] = [
        (7, true),
        (8, true),
        (9, false),
        (10, false),
        (11, true),
        (12, false),
        (13, true),
        (14, true),
        (15, true),
        (16, false),
        (17, true),
        (18, false),
        (19, false),
        (20, false),
        (21, false),
        (22, false),
        (23, true),
        (24, false),
        (25, false),
        (26, false),
        (27, false),
        (28, false),
        (29, false),
        (30, true),
        (31, false),
        (32, true),
        (33, true),
    ];

    #[test]
    fn did_metadata_change_matches_kafka() {
        let table: Vec<(i16, bool)> = (METADATA_VERSION_MIN..=METADATA_VERSION_MAX)
            .map(|level| (level, did_metadata_change(level).expect("supported level")))
            .collect();
        check!(table == KAFKA_DID_METADATA_CHANGE);
        check!(
            (did_metadata_change(6), did_metadata_change(34)) == (None, None),
            "levels outside the table"
        );
    }

    /// Cases worked through Kafka's `checkIfMetadataChangedOrdered` by hand:
    /// walk down from the higher level while the current level did not change
    /// metadata and is not the lower level; the answer is whether the walk
    /// stopped before reaching the lower level.
    #[test]
    fn metadata_changed_between_matches_kafka() {
        for (case, from, to, want) in [
            ("same level", 25, 25, false),
            ("same changing level", 30, 30, false),
            ("4.0-IV3 to 4.2-IV1 changes nothing", 25, 29, false),
            ("4.2-IV1 down to 4.0-IV3 changes nothing", 29, 25, false),
            ("up into 4.3-IV0 cordoned log dirs", 29, 30, true),
            ("down out of 4.3-IV0", 30, 29, true),
            ("4.3-IV0 to 4.4-IV0 changes nothing", 30, 31, false),
            ("4.4-IV0 down to 4.3-IV0 changes nothing", 31, 30, false),
            ("down out of 4.4-IV1 CIDR ACLs", 32, 31, true),
            ("down across ELR records at 4.0-IV1", 25, 22, true),
            ("down to 4.0-IV1 itself", 25, 23, false),
            ("3.5-IV0 to 3.5-IV1", 9, 10, false),
            ("3.5-IV1 up to SCRAM at 3.5-IV2", 10, 11, true),
            ("3.7-IV2 to 3.7-IV4", 17, 19, false),
            ("3.7-IV0 to 3.7-IV1", 15, 16, false),
            ("3.7-IV1 up to JBOD at 3.7-IV2", 16, 17, true),
            ("minimum to 3.4-IV0", 7, 8, true),
            (
                "down out of 4.4-IV2 controller unregistration",
                33,
                32,
                true,
            ),
            ("whole table", 7, 33, true),
            ("below the table", 6, 7, true),
            ("above the table", 33, 34, true),
        ] {
            check!(metadata_changed_between(from, to) == want, "{case}");
        }
    }

    #[test]
    fn min_max_levels() {
        check!(
            (
                METADATA_VERSION_MIN,
                METADATA_VERSION_MAX,
                TABLE.first().unwrap().level,
                TABLE.last().unwrap().level,
            ) == (7, 33, METADATA_VERSION_MIN, METADATA_VERSION_MAX)
        );
    }

    /// Kafka never reuses or skips a level, so the table must be every level
    /// from the minimum to the maximum, in order.
    #[test]
    fn table_is_contiguous_and_ordered() {
        let levels: Vec<i16> = TABLE.iter().map(|m| m.level).collect();
        let want: Vec<i16> = (METADATA_VERSION_MIN..=METADATA_VERSION_MAX).collect();
        check!(levels == want);
    }

    #[test]
    fn share_version_feature_levels() {
        check!(
            (SHARE_VERSION_FEATURE, SHARE_VERSION_MIN, SHARE_VERSION_MAX)
                == ("share.version", 0, 1)
        );
    }

    #[test]
    fn elr_version_feature_levels() {
        check!(
            (ELR_VERSION_FEATURE, ELR_VERSION_MIN, ELR_VERSION_MAX)
                == ("eligible.leader.replicas.version", 0, 1)
        );
    }

    #[test]
    fn streams_version_feature_levels() {
        check!(
            (
                STREAMS_VERSION_FEATURE,
                STREAMS_VERSION_MIN,
                STREAMS_VERSION_MAX
            ) == ("streams.version", 0, 1)
        );
    }

    #[test]
    fn from_feature_level_known_and_unknown() {
        for (level, want) in [
            (
                7,
                Some(MetadataVersion {
                    level: 7,
                    ivn: "3.3-IV3",
                    short: "3.3",
                    did_metadata_change: true,
                }),
            ),
            (
                25,
                Some(MetadataVersion {
                    level: 25,
                    ivn: "4.0-IV3",
                    short: "4.0",
                    did_metadata_change: false,
                }),
            ),
            (
                30,
                Some(MetadataVersion {
                    level: 30,
                    ivn: "4.3-IV0",
                    short: "4.3",
                    did_metadata_change: true,
                }),
            ),
            (
                32,
                Some(MetadataVersion {
                    level: 32,
                    ivn: "4.4-IV1",
                    short: "4.4",
                    did_metadata_change: true,
                }),
            ),
            (
                33,
                Some(MetadataVersion {
                    level: 33,
                    ivn: "4.4-IV2",
                    short: "4.4",
                    did_metadata_change: true,
                }),
            ),
            (6, None),
            (34, None),
        ] {
            assert2::assert!(from_feature_level(level) == want);
        }
    }

    /// The table is compared as whole structs elsewhere, which checks the
    /// fields but never calls the accessors that read them back. `short()`
    /// answering a constant is what the release-version parsing keys off.
    #[test]
    fn accessors_return_the_table_entry() {
        let v = from_feature_level(25).expect("level 25 is in the table");
        assert2::check!((v.feature_level(), v.ivn(), v.short()) == (25, "4.0-IV3", "4.0"));

        let earliest = from_feature_level(7).expect("level 7 is in the table");
        assert2::check!((earliest.ivn(), earliest.short()) == ("3.3-IV3", "3.3"));
    }

    #[test]
    fn from_version_string_exact_ivn() {
        for (_case, s, want) in [
            ("known 3.5 IV", "3.5-IV2", Some(11)),
            ("known 4.0 IV", "4.0-IV3", Some(25)),
            ("known 4.3 IV", "4.3-IV0", Some(30)),
            ("known 4.4 IV", "4.4-IV1", Some(32)),
            ("latest 4.4 IV", "4.4-IV2", Some(33)),
            ("reserved 4.4 IV", "4.4-IV3", None),
            ("unknown IV", "3.5-IV9", None),
        ] {
            assert2::assert!(
                from_version_string(s).map(super::MetadataVersion::feature_level) == want
            );
        }
    }

    #[test]
    fn from_version_string_short_picks_highest_in_minor() {
        for (_case, s, want) in [
            ("known 3.7 minor", "3.7", Some(19)),
            ("known 4.0 minor", "4.0", Some(25)),
            ("known 4.1 minor", "4.1", Some(27)),
            ("known 4.2 minor", "4.2", Some(29)),
            ("known 4.3 minor", "4.3", Some(30)),
            ("unsupported minor", "2.8", None),
        ] {
            assert2::assert!(
                from_version_string(s).map(super::MetadataVersion::feature_level) == want
            );
        }
    }

    #[test]
    fn in_supported_range_predicate() {
        for (_case, level, want) in [
            ("minimum", 7, true),
            ("maximum", 33, true),
            ("below minimum", 6, false),
            ("above maximum", 34, false),
        ] {
            assert2::assert!(is_supported_level(level) == want);
        }
    }

    #[test]
    fn gate_level_constants() {
        for (case, level, expected_ivn) in [
            ("SCRAM gate", SCRAM_MIN_LEVEL, "3.5-IV2"),
            (
                "delegation-token gate",
                DELEGATION_TOKEN_MIN_LEVEL,
                "3.6-IV2",
            ),
            (
                "controller registration gate",
                ONLINE_DOWNGRADE_MIN_LEVEL,
                "3.7-IV0",
            ),
            (
                "directory assignment gate",
                DIRECTORY_ASSIGNMENT_MIN_LEVEL,
                "3.7-IV2",
            ),
            ("ELR record gate", ELR_MIN_LEVEL, "4.0-IV1"),
            ("ELRV_1 bootstrap", ELR_DEFAULT_METADATA_LEVEL, "4.1-IV0"),
            (
                "SV_1 bootstrap",
                SHARE_VERSION_DEFAULT_METADATA_LEVEL,
                "4.2-IV0",
            ),
            (
                "streams SV_1 bootstrap",
                STREAMS_VERSION_DEFAULT_METADATA_LEVEL,
                "4.2-IV1",
            ),
            (
                "cordoned log dirs gate",
                CORDONED_LOG_DIRS_MIN_LEVEL,
                "4.3-IV0",
            ),
            ("CIDR ACL gate", CIDR_ACL_MIN_LEVEL, "4.4-IV1"),
            (
                "controller unregistration gate",
                CONTROLLER_UNREGISTRATION_MIN_LEVEL,
                "4.4-IV2",
            ),
        ] {
            check!(
                from_feature_level(level).unwrap().ivn() == expected_ivn,
                "case {case}"
            );
        }
    }
}
