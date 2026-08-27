//! The error type that every fallible hybrid logical clock operation returns.

use thiserror::Error;

use crate::stamp::HLC_ENCODED_LEN;

/// Why a hybrid logical clock operation failed.
///
/// No variant holds the input bytes that failed. A peer controls the value of
/// the `krabka.hlc` header, and a message that keeps those bytes would put
/// hostile input into the log field that shows the error. The variants carry
/// lengths, counts, and time deltas only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum HlcError {
    /// The header value is not exactly [`HLC_ENCODED_LEN`] bytes.
    #[error("hlc header value must be {HLC_ENCODED_LEN} bytes, got {0}")]
    Length(usize),

    /// The node id is wider than the 4 bytes the layout gives it.
    ///
    /// `NodeId` holds a `u64`, and Kafka carries the same value as a 4-byte
    /// wire field. A node id above `u32::MAX` has no place in the layout, so
    /// the encode fails instead of a silent truncation.
    #[error("node id {0} does not fit the 4-byte node-id field")]
    NodeIdTooLarge(u64),

    /// The peer's wall time is further ahead of the local physical clock than
    /// the configured maximum offset, so the clock refused the stamp.
    #[error(
        "peer wall time is {ahead_micros} microseconds ahead of the local physical clock, \
         over the {max_micros} microsecond maximum offset"
    )]
    PeerTooFarAhead {
        /// How far the peer's wall time is ahead of the local physical clock.
        ahead_micros: i64,
        /// The configured maximum offset.
        max_micros: i64,
    },

    /// The logical counter reached `u32::MAX` inside one microsecond.
    ///
    /// The counter separates events that share a wall-time microsecond. It
    /// never wraps, because a wrap would put a later event before an earlier
    /// one.
    #[error("hlc counter reached u32::MAX inside one microsecond")]
    CounterOverflow,
}
