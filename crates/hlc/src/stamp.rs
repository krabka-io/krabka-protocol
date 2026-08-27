//! The hybrid logical clock stamp and its fixed-width byte layout.

use crabka_ids::NodeId;
use derive_more::{Display, From, Into};

use crate::{error::HlcError, headers::HLC_HEADER};

/// The byte length of an encoded [`Hlc`].
///
/// The layout is fixed-width, so this length is the same for every stamp: 8
/// bytes of wall time, 4 bytes of counter, and 4 bytes of node id.
pub const HLC_ENCODED_LEN: usize = 16;

/// A wall-clock instant, in microseconds since the Unix epoch.
///
/// This is a coordinate on the time line and not an extent of time, so it is a
/// newtype here and not a [`crabka_units::Time`]. See the code style guide's
/// "Dimensioned Values" section for the rule.
///
/// The unit is the microsecond. A Kafka record timestamp counts milliseconds,
/// and one millisecond is too coarse to order the records inside one batch. A
/// microsecond gives the logical counter far fewer events to separate.
///
/// The value is signed, and a value below zero names an instant before the
/// Unix epoch. The clock accepts such a value and does not treat it as an
/// error, because a host whose clock is unset reports one.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into,
)]
pub struct WallMicros(pub i64);

impl WallMicros {
    /// The Unix epoch, `1970-01-01T00:00:00Z`.
    pub const EPOCH: Self = WallMicros(0);

    /// The inner `i64`. Use it for arithmetic against other time counts.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// One hybrid logical clock stamp: a wall time, a logical counter, and the
/// node that minted it.
///
/// The three fields give a total order over every event in the cluster.
/// [`Ord`] compares the wall time first, then the counter, then the node id.
/// The wall time keeps the order close to real time. The counter separates
/// events that share a microsecond. The node id breaks the remaining ties, so
/// two nodes that stamp the same wall time and the same counter still get one
/// order, and every node computes that same order.
///
/// The type is `Copy`, and it is 16 bytes wide in memory and 16 bytes wide on
/// the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display)]
#[display("{wall}.{counter}@{node}")]
pub struct Hlc {
    /// The wall time this stamp claims.
    pub wall: WallMicros,

    /// The logical counter that separates events inside one microsecond.
    pub counter: u32,

    /// The node that minted the stamp.
    pub node: NodeId,
}

impl Hlc {
    /// A stamp built from its three parts.
    #[must_use]
    pub const fn new(wall: WallMicros, counter: u32, node: NodeId) -> Self {
        Self {
            wall,
            counter,
            node,
        }
    }

    /// The stamp as the exact bytes of the `krabka.hlc` header value.
    ///
    /// The layout is big-endian and fixed-width: 8 bytes of wall time as an
    /// `i64`, 4 bytes of counter as a `u32`, and 4 bytes of node id as a
    /// `u32`. There is no varint framing and no length prefix. Java and Go
    /// clients read this header by hand, and a plain big-endian layout is what
    /// `DataInputStream.readLong` and `binary.BigEndian.Uint32` already give
    /// them.
    ///
    /// # Errors
    ///
    /// Returns [`HlcError::NodeIdTooLarge`] when the node id is above
    /// `u32::MAX`, which is wider than the 4-byte field the layout gives it.
    pub fn encode(self) -> Result<[u8; HLC_ENCODED_LEN], HlcError> {
        let node = u32::try_from(self.node.get())
            .map_err(|_| HlcError::NodeIdTooLarge(self.node.get()))?;
        let wall = self.wall.get().to_be_bytes();
        let counter = self.counter.to_be_bytes();
        let node = node.to_be_bytes();
        Ok([
            wall[0], wall[1], wall[2], wall[3], wall[4], wall[5], wall[6], wall[7], counter[0],
            counter[1], counter[2], counter[3], node[0], node[1], node[2], node[3],
        ])
    }

    /// The stamp that `bytes` carries, with full validation.
    ///
    /// This function reads a peer-supplied header value, so it validates the
    /// length before it reads a field. It never panics on malformed input, and
    /// it never allocates. Every 16-byte input is a valid stamp, because each
    /// of the three fields covers the whole range of its type.
    ///
    /// Use [`extract_from_headers`](crate::extract_from_headers) on an ingress
    /// path, where a bad value must not fail the record that carried it. Use
    /// this function where the caller wants to know why a value failed.
    ///
    /// # Errors
    ///
    /// Returns [`HlcError::Length`] when `bytes` is not exactly
    /// [`HLC_ENCODED_LEN`] bytes long.
    pub fn decode(bytes: &[u8]) -> Result<Self, HlcError> {
        let Ok(value) = <&[u8; HLC_ENCODED_LEN]>::try_from(bytes) else {
            return Err(HlcError::Length(bytes.len()));
        };
        // A slice pattern over a fixed-width array. It splits the value into
        // its three fields with no indexing, no `copy_from_slice`, and no
        // conversion that can fail, so no path here can panic.
        let [wall @ .., c0, c1, c2, c3, n0, n1, n2, n3] = *value;
        Ok(Self {
            wall: WallMicros(i64::from_be_bytes(wall)),
            counter: u32::from_be_bytes([c0, c1, c2, c3]),
            node: NodeId(u64::from(u32::from_be_bytes([n0, n1, n2, n3]))),
        })
    }

    /// The stamp as a Kafka record header: the [`HLC_HEADER`] key and the
    /// encoded value.
    ///
    /// A producer attaches the pair to the record it writes. A record that
    /// carries no stamp gets no header, so an unstamped record costs zero
    /// bytes.
    ///
    /// # Errors
    ///
    /// Returns [`HlcError::NodeIdTooLarge`] when the node id is above
    /// `u32::MAX`. See [`Hlc::encode`].
    pub fn header(self) -> Result<(&'static str, [u8; HLC_ENCODED_LEN]), HlcError> {
        Ok((HLC_HEADER, self.encode()?))
    }
}
