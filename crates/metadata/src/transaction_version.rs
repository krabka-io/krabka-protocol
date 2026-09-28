//! KIP-890 `transaction.version` feature-level constants, matching Kafka's
//! `TransactionVersion`, which defines `TV_0` to `TV_2` only. 0 means classic
//! (KIP-98) non-flexible txn-state records, 1 means flexible (tagged)
//! txn-state records, and 2 means an epoch bump on completion plus
//! server-side `AddPartitionsToTxn` verification. Both `TV_1` and `TV_2`
//! bootstrap at 4.0-IV2.
//!
//! KIP-939 two-phase commit is not a transaction version. Kafka gates it on
//! the broker config `transaction.two.phase.commit.enable` and the
//! `TWO_PHASE_COMMIT` ACL, and asks for no finalized level beyond `TV_2`.

pub const TRANSACTION_VERSION_FEATURE: &str = "transaction.version";
pub const TRANSACTION_VERSION_MIN: i16 = 0;
/// `TransactionVersion.LATEST_PRODUCTION`, `TV_2`, on Kafka 4.3.1 and trunk.
pub const TRANSACTION_VERSION_MAX: i16 = 2;

/// metadata.version at or above which transaction.version becomes a bootstrap
/// default. Both `TV_1` and `TV_2` bootstrap at 4.0-IV2 (level 24), so the
/// per-release default jumps 0 -> 2 at level 24. This is a bootstrap-default
/// input only, NOT a hard `UpdateFeatures` dependency.
pub const TV1_METADATA_LEVEL: i16 = 24; // 4.0-IV2
pub const TV2_METADATA_LEVEL: i16 = 24; // 4.0-IV2
