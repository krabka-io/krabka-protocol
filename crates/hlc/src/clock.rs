//! The clock state machine, its physical-clock seam, and its observations.

use core::sync::atomic::{AtomicI64, Ordering};
use std::{
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::SystemTime,
};

use crabka_ids::NodeId;
use crabka_units::{Time, convert::TimeExt as _, millis};

use crate::{
    error::HlcError,
    stamp::{Hlc, WallMicros},
};

/// The maximum offset an [`HlcClock`] accepts from a peer by default.
///
/// 500 milliseconds is wider than the offset a healthy NTP or PTP fleet holds,
/// and it is far below the offset a broken clock reports.
pub const DEFAULT_MAX_OFFSET: Time = millis(500);

/// The source of physical time for an [`HlcClock`].
///
/// The clock reads this trait once per event. A test supplies a
/// [`ManualClock`], so the whole state machine runs without a real clock and
/// without a sleep. Production supplies a [`SystemClock`].
///
/// An implementor returns the current wall-clock time in microseconds since
/// the Unix epoch. The value should never go backwards, but the clock does not
/// depend on that: the logical time it keeps never goes backwards, whatever
/// the physical clock does.
pub trait PhysicalClock {
    /// The current wall-clock time, in microseconds since the Unix epoch.
    fn now_micros(&self) -> i64;
}

impl<C: PhysicalClock + ?Sized> PhysicalClock for &C {
    fn now_micros(&self) -> i64 {
        (**self).now_micros()
    }
}

impl<C: PhysicalClock + ?Sized> PhysicalClock for Arc<C> {
    fn now_micros(&self) -> i64 {
        (**self).now_micros()
    }
}

/// The host's wall clock, read through [`SystemTime`].
///
/// This is the physical clock a broker uses.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl PhysicalClock for SystemClock {
    // `SystemTime` is not monotonic and it can sit before the Unix epoch, so
    // both directions get an answer and neither one panics. A time the `i64`
    // range cannot hold saturates, which keeps the stamp ordered against every
    // other stamp.
    fn now_micros(&self) -> i64 {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(after) => i64::try_from(after.as_micros()).unwrap_or(i64::MAX),
            Err(before) => {
                i64::try_from(before.duration().as_micros()).map_or(i64::MIN, i64::saturating_neg)
            }
        }
    }
}

/// A physical clock a test moves by hand.
///
/// The clock holds one instant and returns it until the test changes it. Every
/// method takes `&self`, so a test can hold the clock in an [`Arc`] and still
/// move it while an [`HlcClock`] reads it.
#[derive(Debug)]
pub struct ManualClock {
    micros: AtomicI64,
}

impl ManualClock {
    /// A clock stopped at `now`.
    #[must_use]
    pub fn new(now: WallMicros) -> Self {
        Self {
            micros: AtomicI64::new(now.get()),
        }
    }

    /// Moves the clock to `now`. The new instant can be before the old one,
    /// which is how a test models a clock that jumps backwards.
    pub fn set(&self, now: WallMicros) {
        self.micros.store(now.get(), Ordering::SeqCst);
    }

    /// Moves the clock forward by `by`. A negative extent moves it backwards.
    pub fn advance(&self, by: Time) {
        let micros = by.micros_i64();
        let _ = self
            .micros
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.saturating_add(micros))
            });
    }
}

impl PhysicalClock for ManualClock {
    fn now_micros(&self) -> i64 {
        self.micros.load(Ordering::SeqCst)
    }
}

/// What an [`HlcClock`] has learned about clock skew in the cluster.
///
/// Every field is evidence measured on the data path. A record whose stamp
/// pulls the local clock forward by 40 milliseconds is direct evidence of 40
/// milliseconds of skew between this node and the node that wrote the record.
/// No probe and no extra round trip produce that number. The record already
/// carried it.
///
/// This is a plain snapshot. The crate has no metrics dependency, and it must
/// not gain one. An observability agent reads the snapshot and publishes it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockObservation {
    /// How far the logical clock leads the physical clock right now.
    ///
    /// This is `l - pt`: the logical wall time minus the physical wall time.
    /// A positive value means that a peer, or this node's own history, holds
    /// the logical clock ahead of what the hardware reports, and that lead is
    /// the skew this node carries. A negative value is normal, and it only
    /// means that the physical clock moved on since the last event.
    pub drift: Time,

    /// How many received stamps moved the local clock forward because the
    /// peer's wall time was ahead of both the local logical clock and the
    /// local physical clock.
    pub peer_advances: u64,

    /// How many received stamps the self-fence refused.
    ///
    /// A number above zero names a peer whose clock is broken, or a maximum
    /// offset set below the fleet's real skew.
    pub refusals: u64,

    /// The largest positive delta between a peer's wall time and the local
    /// physical clock, over every stamp this clock received.
    ///
    /// Refused stamps count here too, so the value shows the worst skew the
    /// node saw and not only the worst skew it absorbed. The value stays at
    /// zero while no peer is ahead.
    pub max_peer_ahead: Time,
}

/// The mutable part of an [`HlcClock`].
#[derive(Debug)]
struct State {
    wall: i64,
    counter: u32,
    peer_advances: u64,
    refusals: u64,
    max_peer_ahead_micros: i64,
}

/// A hybrid logical clock for one node.
///
/// The clock holds a wall time and a logical counter, and it follows the
/// algorithm in Kulkarni et al., *Logical Physical Clocks and Consistent
/// Snapshots in Globally Distributed Databases* (OPODIS 2014). [`HlcClock::now`]
/// is the paper's send event and [`HlcClock::observe`] is its receive event.
/// The pair keeps two guarantees. The stamps one node mints strictly increase.
/// A stamp that a node received is always below every stamp that the node
/// mints after it, so the order the stamps give agrees with cause and effect.
///
/// Every method takes `&self`. The state sits behind a [`Mutex`], and the
/// critical section only compares and assigns integers, so a producer task, a
/// consumer task, and an observability agent can share one clock.
///
/// The physical clock is a type parameter, so a test drives the whole state
/// machine with a [`ManualClock`] and no sleep.
#[derive(Debug)]
pub struct HlcClock<C = SystemClock> {
    node: NodeId,
    physical: C,
    max_offset_micros: i64,
    state: Mutex<State>,
}

impl HlcClock<SystemClock> {
    /// A clock for `node` that reads the host's wall clock and fences at
    /// [`DEFAULT_MAX_OFFSET`].
    #[must_use]
    pub fn new(node: NodeId) -> Self {
        Self::with_max_offset(node, SystemClock, DEFAULT_MAX_OFFSET)
    }
}

impl<C: PhysicalClock> HlcClock<C> {
    /// A clock for `node` that reads `physical` and fences at
    /// [`DEFAULT_MAX_OFFSET`].
    #[must_use]
    pub fn with_clock(node: NodeId, physical: C) -> Self {
        Self::with_max_offset(node, physical, DEFAULT_MAX_OFFSET)
    }

    /// A clock for `node` that reads `physical` and fences at `max_offset`.
    ///
    /// The clock starts at the current reading of `physical`, with the counter
    /// at zero. A negative `max_offset` has the same effect as zero: the clock
    /// then refuses every stamp whose wall time is above its physical clock.
    #[must_use]
    pub fn with_max_offset(node: NodeId, physical: C, max_offset: Time) -> Self {
        let wall = physical.now_micros();
        Self {
            node,
            physical,
            max_offset_micros: max_offset.micros_i64().max(0),
            state: Mutex::new(State {
                wall,
                counter: 0,
                peer_advances: 0,
                refusals: 0,
                max_peer_ahead_micros: 0,
            }),
        }
    }

    /// The node this clock stamps for.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }

    /// The maximum offset the self-fence allows.
    #[must_use]
    pub fn max_offset(&self) -> Time {
        Time::from_micros(self.max_offset_micros)
    }

    /// The next stamp for a local event, such as a record this node produces.
    ///
    /// This is the paper's send event. The new wall time is the later of the
    /// current logical wall time and the physical clock. The counter goes up
    /// by one when the wall time did not move, and back to zero when it did.
    /// Two calls never return the same stamp, whatever the physical clock
    /// does.
    ///
    /// # Errors
    ///
    /// Returns [`HlcError::CounterOverflow`] when the counter reaches
    /// `u32::MAX` inside one microsecond. That needs more than four billion
    /// stamps in one microsecond, and the clock reports it rather than let the
    /// counter wrap.
    pub fn now(&self) -> Result<Hlc, HlcError> {
        let physical = self.physical.now_micros();
        let mut state = self.state();
        let wall = state.wall.max(physical);
        let counter = if wall == state.wall {
            next_counter(state.counter)?
        } else {
            0
        };
        state.wall = wall;
        state.counter = counter;
        Ok(Hlc::new(WallMicros(wall), counter, self.node))
    }

    /// Merges a peer's stamp into this clock, and returns the stamp for the
    /// receive event.
    ///
    /// This is the paper's receive event. The new wall time is the latest of
    /// the local logical wall time, the peer's wall time, and the physical
    /// clock. The counter then follows the source of that wall time: it is one
    /// above the larger of the two counters when the local clock and the peer
    /// agree, one above the counter of whichever side supplied the wall time
    /// when only one side did, and zero when the physical clock overtook both.
    ///
    /// The returned stamp is always above `peer`, so the record this node
    /// writes next sorts after the record it read.
    ///
    /// The clock also fences itself. It refuses a stamp whose wall time is more
    /// than [`HlcClock::max_offset`] ahead of the local physical clock. It
    /// returns [`HlcError::PeerTooFarAhead`], and it does not move. A hybrid
    /// logical clock adopts the highest wall time it sees, so one host with a
    /// clock set to the year 2098 would drag every node that reads its records
    /// into 2098, and then every node that reads those nodes' records. Nothing
    /// brings the fleet back, because logical time never goes backwards. The
    /// fence keeps the damage inside the broken host.
    ///
    /// A stamp behind the local physical clock is always safe, and the clock
    /// always accepts it. The fence is one-sided for that reason.
    ///
    /// # Errors
    ///
    /// Returns [`HlcError::PeerTooFarAhead`] when the self-fence refuses the
    /// stamp, and [`HlcError::CounterOverflow`] when the merged counter
    /// reaches `u32::MAX` inside one microsecond.
    pub fn observe(&self, peer: Hlc) -> Result<Hlc, HlcError> {
        let physical = self.physical.now_micros();
        let peer_wall = peer.wall.get();
        // Saturating, because a peer that claims `i64::MAX` while the local
        // clock sits below zero would overflow the plain subtraction. The
        // saturated value is still far over any sane offset, so the fence
        // below refuses it.
        let ahead_micros = peer_wall.saturating_sub(physical);

        let mut state = self.state();
        state.max_peer_ahead_micros = state.max_peer_ahead_micros.max(ahead_micros);
        if ahead_micros > self.max_offset_micros {
            state.refusals = state.refusals.saturating_add(1);
            drop(state);
            tracing::debug!(
                node = self.node.get(),
                peer_node = peer.node.get(),
                ahead_micros,
                max_micros = self.max_offset_micros,
                "refused a hybrid logical clock stamp from a peer that is too far ahead"
            );
            return Err(HlcError::PeerTooFarAhead {
                ahead_micros,
                max_micros: self.max_offset_micros,
            });
        }

        let local_wall = state.wall;
        let wall = local_wall.max(peer_wall).max(physical);
        let counter = if wall == local_wall && wall == peer_wall {
            next_counter(state.counter.max(peer.counter))?
        } else if wall == local_wall {
            next_counter(state.counter)?
        } else if wall == peer_wall {
            next_counter(peer.counter)?
        } else {
            0
        };

        if peer_wall > local_wall && peer_wall > physical {
            state.peer_advances = state.peer_advances.saturating_add(1);
        }
        state.wall = wall;
        state.counter = counter;
        Ok(Hlc::new(WallMicros(wall), counter, self.node))
    }

    /// What this clock has learned about clock skew.
    ///
    /// The snapshot is consistent: it reads the physical clock and the whole
    /// state together.
    #[must_use]
    pub fn observation(&self) -> ClockObservation {
        let physical = self.physical.now_micros();
        let state = self.state();
        ClockObservation {
            drift: Time::from_micros(state.wall.saturating_sub(physical)),
            peer_advances: state.peer_advances,
            refusals: state.refusals,
            max_peer_ahead: Time::from_micros(state.max_peer_ahead_micros),
        }
    }

    // The critical section compares and assigns integers only, so it cannot
    // panic and it cannot leave the state half-written. A poisoned lock still
    // holds a valid state, so recovery keeps the clock free of panics.
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The next value of a logical counter.
fn next_counter(counter: u32) -> Result<u32, HlcError> {
    counter.checked_add(1).ok_or(HlcError::CounterOverflow)
}
