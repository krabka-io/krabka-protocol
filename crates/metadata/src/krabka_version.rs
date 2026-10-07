//! The krabka-owned `krabka.version` feature (KIP-584).
//!
//! From krabka-broker 1.0.0 on, `krabka.version` gates every krabka-only
//! on-disk and inter-node format change, as `metadata.version` does for
//! Kafka's. `metadata.version` mirrors Kafka's `MetadataVersion` table level
//! for level, so krabka cannot add a level there. A broker keeps writing and
//! sending the old format until the operator finalizes the `krabka.version`
//! level that introduces the new one.
//!
//! # Levels
//!
//! - **Level 0** means exactly the krabka-broker 1.0.0 formats. An image that
//!   has no finalized `krabka.version` reads as level 0, as Kafka treats an
//!   absent feature (see [`KrabkaVersion::finalized`]).
//! - **Level 1** also means exactly the 1.0.0 formats. It carries no format
//!   change, and it never will: a 1.0.0 node advertises support for it, so a
//!   later release cannot give it a meaning that 1.0.0 nodes do not know. The
//!   first krabka-only format change takes level 2.
//!
//! # Why the maximum is 1, not 0
//!
//! Kafka does not advertise a feature whose highest supported level is 0.
//! `BrokerFeatures.defaultSupportedFeatures` and
//! `QuorumFeatures.defaultSupportedFeatureMap` both add a feature to the
//! supported map only when `maxVersion > 0`, and the controller treats a
//! feature that a node does not advertise as supported at `[0, 0]`. A feature
//! with a maximum of 0 therefore cannot be finalized above 0, and
//! `kafka-features describe`, which lists `supportedFeatures`, would show it
//! only because krabka breaks that rule. With a maximum of 1, `krabka.version`
//! appears in `kafka-features describe`, and
//! `kafka-features upgrade --feature krabka.version=1` finalizes it.
//! `kafka-features upgrade --release-version` iterates Kafka's own
//! `Feature.PRODUCTION_FEATURES` only, so it never touches `krabka.version`.
//!
//! A fresh cluster bootstraps at [`KRABKA_VERSION_MAX`], the latest production
//! level, as Kafka bootstraps its production features. The level does not
//! follow the bootstrap `metadata.version`.
//!
//! # Dependencies
//!
//! `krabka.version` declares no KIP-1022 dependency on `metadata.version`. A
//! future level that needs a Kafka record shape adds one for that level only.
//!
//! # Private-RPC version negotiation
//!
//! [`private_rpc_version`] is the negotiation mechanism for the krabka-private
//! controller RPCs, the same one Kafka uses for inter-broker protocol versions
//! through `metadata.version`. The controller finalizes a level only once every
//! registered node advertises support for it. The finalized level therefore
//! tells a sender which request versions every peer serves, and a sender never
//! sends a version above the one the table gives for that level.

use crate::MetadataImage;

/// KIP-584 feature name.
pub const KRABKA_VERSION_FEATURE: &str = "krabka.version";

/// Minimum supported level: the krabka-broker 1.0.0 formats.
pub const KRABKA_VERSION_MIN: i16 = 0;

/// Maximum supported level, and the latest production level. Level 1 means
/// the same formats as level 0. See the module docs for why it is 1.
pub const KRABKA_VERSION_MAX: i16 = 1;

/// The finalized `krabka.version` level of a metadata image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KrabkaVersion(i16);

impl KrabkaVersion {
    /// Wrap a raw level.
    #[must_use]
    pub const fn new(level: i16) -> Self {
        Self(level)
    }

    /// The finalized `krabka.version` of `image`. An image that has not
    /// finalized the feature reads as level 0, as Kafka treats an absent
    /// feature.
    #[must_use]
    pub fn finalized(image: &MetadataImage) -> Self {
        Self(
            image
                .finalized_feature(KRABKA_VERSION_FEATURE)
                .unwrap_or(KRABKA_VERSION_MIN),
        )
    }

    /// The raw level.
    #[must_use]
    pub const fn level(self) -> i16 {
        self.0
    }

    /// The highest request version of `rpc` that a node may send at this
    /// level. See [`private_rpc_version`].
    #[must_use]
    pub const fn max_request_version(self, rpc: PrivateRpc) -> Option<i16> {
        rpc.max_request_version(self.0)
    }
}

/// A krabka-private controller RPC. Its api key sits at 1000 or above, so a
/// later Kafka assignment cannot collide with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PrivateRpc {
    /// `SubmitChange`, api key 1003: a node forwards a metadata change to the
    /// controller leader.
    SubmitChange,
    /// `MetadataFetch`, api key 1004: an observer fetches the metadata log.
    MetadataFetch,
    /// `DelegationTokenMutation`, api key 1005: a guarded forward of a
    /// delegation-token change.
    DelegationTokenMutation,
}

impl PrivateRpc {
    /// Every private RPC, in api-key order.
    pub const ALL: [Self; 3] = [
        Self::SubmitChange,
        Self::MetadataFetch,
        Self::DelegationTokenMutation,
    ];

    /// The api key of this RPC.
    #[must_use]
    pub const fn api_key(self) -> i16 {
        match self {
            Self::SubmitChange => 1003,
            Self::MetadataFetch => 1004,
            Self::DelegationTokenMutation => 1005,
        }
    }

    /// The private RPC with `api_key`, or `None` for any other key.
    #[must_use]
    pub const fn from_api_key(api_key: i16) -> Option<Self> {
        match api_key {
            1003 => Some(Self::SubmitChange),
            1004 => Some(Self::MetadataFetch),
            1005 => Some(Self::DelegationTokenMutation),
            _ => None,
        }
    }

    /// The highest request version of this RPC that a node may send when the
    /// finalized `krabka.version` is `krabka_version`. `None` for a level
    /// outside `[KRABKA_VERSION_MIN, KRABKA_VERSION_MAX]`: no node of this
    /// build can see such a level finalized, because the controller finalizes
    /// only a level that every node supports.
    #[must_use]
    pub const fn max_request_version(self, krabka_version: i16) -> Option<i16> {
        if krabka_version < KRABKA_VERSION_MIN || krabka_version > KRABKA_VERSION_MAX {
            return None;
        }
        // Levels 0 and 1 are both the 1.0.0 formats, so every RPC is v0. A
        // level that raises an RPC's version adds an arm here.
        Some(match self {
            Self::SubmitChange | Self::MetadataFetch | Self::DelegationTokenMutation => 0,
        })
    }
}

/// The highest request version of the private controller RPC `api_key` that a
/// node may send when the finalized `krabka.version` is `krabka_version`.
///
/// Returns `None` for an api key that is not a private controller RPC, and for
/// a level this build does not support. At levels 0 and 1 every RPC is v0.
#[must_use]
pub const fn private_rpc_version(api_key: i16, krabka_version: i16) -> Option<i16> {
    match PrivateRpc::from_api_key(api_key) {
        Some(rpc) => rpc.max_request_version(krabka_version),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::{FeatureLevelRecord, MetadataRecord};

    fn image_at(level: Option<i16>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        if let Some(level) = level {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: KRABKA_VERSION_FEATURE.into(),
                level,
            }));
        }
        image
    }

    #[test]
    fn constants_are_pinned() {
        check!(
            (
                KRABKA_VERSION_FEATURE,
                KRABKA_VERSION_MIN,
                KRABKA_VERSION_MAX
            ) == ("krabka.version", 0, 1)
        );
    }

    #[test]
    fn finalized_reads_the_image() {
        for (case, finalized, want) in [
            ("absent reads as 0", None, KrabkaVersion::new(0)),
            // A level-0 record deletes the entry, so it reads as absent.
            ("explicit 0", Some(0), KrabkaVersion::new(0)),
            (
                "maximum",
                Some(KRABKA_VERSION_MAX),
                KrabkaVersion::new(KRABKA_VERSION_MAX),
            ),
        ] {
            check!(
                KrabkaVersion::finalized(&image_at(finalized)) == want,
                "{case}"
            );
        }
    }

    #[test]
    fn finalized_ignores_other_features() {
        let mut image = image_at(None);
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "transaction.version".into(),
            level: 2,
        }));
        check!(KrabkaVersion::finalized(&image).level() == 0);
    }

    /// One row per private RPC. The match has no wildcard arm, so a new
    /// `PrivateRpc` variant does not compile until it has a row here.
    #[test]
    fn private_rpc_version_table() {
        for rpc in PrivateRpc::ALL {
            // (api key, [(krabka.version, highest request version)])
            let (api_key, rows): (i16, [(i16, Option<i16>); 4]) = match rpc {
                PrivateRpc::SubmitChange => {
                    (1003, [(-1, None), (0, Some(0)), (1, Some(0)), (2, None)])
                }
                PrivateRpc::MetadataFetch => {
                    (1004, [(-1, None), (0, Some(0)), (1, Some(0)), (2, None)])
                }
                PrivateRpc::DelegationTokenMutation => {
                    (1005, [(-1, None), (0, Some(0)), (1, Some(0)), (2, None)])
                }
            };
            check!(rpc.api_key() == api_key);
            check!(PrivateRpc::from_api_key(api_key) == Some(rpc));
            for (level, want) in rows {
                check!(
                    private_rpc_version(api_key, level) == want,
                    "{rpc:?} at {level}"
                );
                check!(
                    KrabkaVersion::new(level).max_request_version(rpc) == want,
                    "{rpc:?} at {level}"
                );
            }
        }
    }

    #[test]
    fn private_rpc_version_refuses_other_api_keys() {
        for api_key in [0, 18, 1000, 1001, 1002, 1006, -1] {
            check!(
                private_rpc_version(api_key, 0).is_none(),
                "api key {api_key}"
            );
        }
    }
}
