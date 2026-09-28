//! KIP-858 reserved log-directory ids, transcribed from Kafka's
//! `org.apache.kafka.common.DirectoryId`.
//!
//! A partition's `directories` list holds one id per replica. Besides the real
//! ids a broker generates for its log directories, Kafka reserves the first
//! 100 UUIDs (most significant bits 0, least significant bits below 100) and
//! gives three of them a meaning.

use uuid::Uuid;

/// `DirectoryId.MIGRATING`, `Uuid(0, 0)`: the replica already lives in some
/// log directory, but nobody recorded which. Kafka reads a partition record
/// with no directories (every record before `3.7-IV2`) as all `MIGRATING`,
/// and its controller places a new replica on a broker that registered no
/// log-dir ids, or on any broker below `3.7-IV2`, as `MIGRATING`.
pub const MIGRATING: Uuid = Uuid::from_u64_pair(0, 0);

/// `DirectoryId.UNASSIGNED`, `Uuid(0, 1)`: a new replica that no log directory
/// hosts yet. Kafka's controller places a replica on a broker with more than
/// one log directory, or on a broker it has no registration for, as
/// `UNASSIGNED`, and the broker's `AssignReplicasToDirs` later names the
/// directory.
pub const UNASSIGNED: Uuid = Uuid::from_u64_pair(0, 1);

/// `DirectoryId.LOST`, `Uuid(0, 2)`: the replica's directory is offline and
/// its id is unknown.
pub const LOST: Uuid = Uuid::from_u64_pair(0, 2);

/// `DirectoryId.reserved`: true for the first 100 UUIDs, which no broker
/// generates as a log-directory id.
#[must_use]
pub fn reserved(dir: Uuid) -> bool {
    let (most, least) = dir.as_u64_pair();
    most == 0 && least < 100
}

/// `DirectoryId.isOnline`: whether a replica assigned to `dir` counts as on
/// an online directory of a broker whose online directories are `online_dirs`.
///
/// `UNASSIGNED` and `MIGRATING` are online and `LOST` is offline. An empty
/// `online_dirs` also counts as online: Kafka reaches it only for a broker
/// that registered before `3.7-IV2` carried log dirs, and it assumes a broker
/// halts once every log directory is down.
#[must_use]
pub fn is_online(dir: Uuid, online_dirs: &[Uuid]) -> bool {
    if dir == UNASSIGNED || dir == MIGRATING {
        return true;
    }
    if dir == LOST {
        return false;
    }
    online_dirs.is_empty() || online_dirs.contains(&dir)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// The sentinels are Kafka's `new Uuid(0L, 0L)`, `new Uuid(0L, 1L)` and
    /// `new Uuid(0L, 2L)`, whose big-endian bytes end in 0, 1 and 2.
    #[test]
    fn sentinels_match_kafka() {
        check!(
            [MIGRATING, UNASSIGNED, LOST] == [Uuid::nil(), Uuid::from_u128(1), Uuid::from_u128(2)]
        );
    }

    #[test]
    fn reserved_is_the_first_hundred_uuids() {
        for (case, dir, want) in [
            ("MIGRATING", MIGRATING, true),
            ("UNASSIGNED", UNASSIGNED, true),
            ("LOST", LOST, true),
            ("last reserved", Uuid::from_u128(99), true),
            ("first unreserved", Uuid::from_u128(100), false),
            ("high bits set", Uuid::from_u64_pair(1, 0), false),
        ] {
            check!(reserved(dir) == want, "{case}");
        }
    }

    #[test]
    fn is_online_matches_kafka() {
        let (a, b) = (Uuid::from_u128(0xA00), Uuid::from_u128(0xB00));
        for (case, dir, online, want) in [
            ("UNASSIGNED", UNASSIGNED, &[a][..], true),
            ("MIGRATING", MIGRATING, &[a][..], true),
            ("LOST", LOST, &[a][..], false),
            ("LOST with no dirs", LOST, &[][..], false),
            ("listed dir", a, &[a, b][..], true),
            ("unlisted dir", b, &[a][..], false),
            ("no dirs registered", b, &[][..], true),
        ] {
            check!(is_online(dir, online) == want, "{case}");
        }
    }
}
