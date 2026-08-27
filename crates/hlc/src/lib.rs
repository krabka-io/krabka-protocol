//! Hybrid logical clock stamps that ride on Kafka record headers.
//!
//! A hybrid logical clock (HLC) gives every event in the cluster one timestamp
//! that is close to real time and that still respects cause and effect. The
//! stamp holds a wall time, a logical counter, and the node that minted it, so
//! it sorts totally and every node computes the same order. Kulkarni et al.,
//! *Logical Physical Clocks and Consistent Snapshots in Globally Distributed
//! Databases* (OPODIS 2014), define the algorithm this crate implements.
//!
//! # Carriage
//!
//! A stamp travels on one Kafka record header, [`HLC_HEADER`]. A producer
//! calls [`HlcClock::now`] and attaches [`Hlc::header`]. A consumer calls
//! [`extract_from_headers`] and feeds the result to [`HlcClock::observe`].
//!
//! Like `crabka-trace-context`, this crate does not depend on
//! `crabka-protocol`. It stays type-erased over `(&str, impl AsRef<[u8]>)`
//! header pairs, so any producer or consumer header type works. The caller
//! converts to and from its own `Header` at the edge.
//!
//! Four rules govern that boundary:
//!
//! 1. The crate never stores a peer's bytes. It parses them into an [`Hlc`],
//!    and it renders the header again from the parsed value.
//! 2. A failure to read a stamp is silent at the carrier layer.
//!    [`extract_from_headers`] gives `None` for an absent, short, or invalid
//!    value, because a consumer that cannot read the stamp must still be able
//!    to apply the record. [`Hlc::decode`] returns a [`Result`] for a caller
//!    that wants the reason.
//! 3. A record with no stamp carries no header, so it costs zero bytes.
//! 4. No [`HlcError`] variant holds input bytes, so hostile input cannot reach
//!    a log line.
//!
//! # Byte layout
//!
//! The header value is 16 bytes, big-endian and fixed-width: 8 bytes of wall
//! time as an `i64`, 4 bytes of counter as a `u32`, and 4 bytes of node id as
//! a `u32`. There is no varint framing. Java and Go clients read this header
//! by hand, and a plain big-endian layout is what `DataInputStream.readLong`
//! and `binary.BigEndian.Uint32` already give them. A varint would make every
//! such client write a decoder first.
//!
//! # Self-fence
//!
//! [`HlcClock`] refuses a stamp whose wall time is more than its configured
//! maximum offset ahead of the local physical clock, and it holds
//! [`DEFAULT_MAX_OFFSET`] by default. A hybrid logical clock adopts the
//! highest wall time it sees, so one host with a broken clock would drag every
//! node that reads its records into the future, and logical time never comes
//! back. The fence keeps the damage inside the broken host. A stamp behind the
//! local clock is always safe, and the clock always accepts it.
//!
//! # Clock-confidence signal
//!
//! [`HlcClock::observation`] returns what the clock has learned about skew:
//! the current drift of the logical clock from the physical clock, how many
//! records pulled the clock forward, how many stamps the fence refused, and
//! the largest peer-ahead delta. A record whose stamp pulls the local clock
//! forward by 40 milliseconds is direct evidence of 40 milliseconds of skew
//! between this node and the node that wrote the record, measured on the data
//! path with no probe and no extra round trip. The snapshot is a plain struct:
//! this crate has no metrics dependency and it must not gain one.
//!
//! # Example
//!
//! ```
//! use crabka_hlc::{HlcClock, ManualClock, WallMicros, extract_from_headers};
//! use crabka_ids::NodeId;
//!
//! let physical = ManualClock::new(WallMicros(1_700_000_000_000_000));
//!
//! // The producer stamps a record it writes.
//! let producer = HlcClock::with_clock(NodeId(1), &physical);
//! let (key, value) = producer.now()?.header()?;
//!
//! // The consumer reads the stamp back and merges it into its own clock.
//! let consumer = HlcClock::with_clock(NodeId(2), &physical);
//! if let Some(stamp) = extract_from_headers([(key, value.as_slice())]) {
//!     consumer.observe(stamp)?;
//! }
//! # Ok::<(), crabka_hlc::HlcError>(())
//! ```

#![forbid(unsafe_code)]

mod clock;
mod error;
mod headers;
mod stamp;

pub use self::{
    clock::{
        ClockObservation, DEFAULT_MAX_OFFSET, HlcClock, ManualClock, PhysicalClock, SystemClock,
    },
    error::HlcError,
    headers::{HLC_HEADER, extract_from_headers},
    stamp::{HLC_ENCODED_LEN, Hlc, WallMicros},
};
