//! The clock state machine: local events, receive events, and the self-fence.

use std::sync::Arc;

use assert2::check;
use crabka_hlc::{
    DEFAULT_MAX_OFFSET, Hlc, HlcClock, HlcError, ManualClock, PhysicalClock as _, WallMicros,
};
use crabka_ids::NodeId;
use crabka_units::{Time, convert::TimeExt as _, micros, millis};

const LOCAL: NodeId = NodeId(1);
const PEER: NodeId = NodeId(9);
// A wall time far from zero, so a field that defaults to zero is visible.
const BASE: i64 = 1_700_000_000_000_000;

type TestClock = HlcClock<Arc<ManualClock>>;

// A clock whose logical time is `(wall, counter)` and whose physical clock
// reads `physical`.
//
// The counter is built by repeated local events at a stalled physical clock,
// which is the only way the state machine reaches a counter above zero.
fn clock_at(wall: i64, counter: u32, physical: i64) -> (TestClock, Arc<ManualClock>) {
    let manual = Arc::new(ManualClock::new(WallMicros(wall)));
    let clock = HlcClock::with_clock(LOCAL, Arc::clone(&manual));
    for _ in 0..counter {
        clock.now().unwrap();
    }
    manual.set(WallMicros(physical));
    (clock, manual)
}

#[test]
fn a_stalled_physical_clock_increments_the_counter() {
    let manual = Arc::new(ManualClock::new(WallMicros(BASE)));
    let clock = HlcClock::with_clock(LOCAL, Arc::clone(&manual));

    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE), 1, LOCAL)));
    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE), 2, LOCAL)));
    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE), 3, LOCAL)));
}

#[test]
fn a_physical_clock_that_advances_resets_the_counter() {
    let manual = Arc::new(ManualClock::new(WallMicros(BASE)));
    let clock = HlcClock::with_clock(LOCAL, Arc::clone(&manual));

    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE), 1, LOCAL)));
    manual.advance(millis(1));
    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE + 1_000), 0, LOCAL)));
    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE + 1_000), 1, LOCAL)));
}

#[test]
fn a_physical_clock_that_jumps_backwards_never_moves_logical_time_back() {
    let manual = Arc::new(ManualClock::new(WallMicros(BASE)));
    let clock = HlcClock::with_clock(LOCAL, Arc::clone(&manual));

    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE), 1, LOCAL)));
    manual.set(WallMicros(BASE - 5_000_000));
    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE), 2, LOCAL)));
    check!(manual.now_micros() == BASE - 5_000_000);
}

#[test]
fn the_receive_rule_picks_the_counter_from_the_side_that_supplied_the_wall_time() {
    // One row per branch of the three-way counter rule in Kulkarni et al.
    // Every row holds the local logical time, the peer stamp, the physical
    // clock, and the whole stamp the receive event returns.
    let cases: [(&str, i64, u32, Hlc, i64, Hlc); 6] = [
        (
            "local and peer share the wall time, so the counters merge",
            100,
            3,
            Hlc::new(WallMicros(100), 7, PEER),
            50,
            Hlc::new(WallMicros(100), 8, LOCAL),
        ),
        (
            "the local clock leads, so its own counter advances",
            100,
            3,
            Hlc::new(WallMicros(80), 7, PEER),
            50,
            Hlc::new(WallMicros(100), 4, LOCAL),
        ),
        (
            "the peer leads, so the peer's counter advances",
            100,
            3,
            Hlc::new(WallMicros(120), 7, PEER),
            50,
            Hlc::new(WallMicros(120), 8, LOCAL),
        ),
        (
            "the physical clock overtakes both, so the counter resets",
            100,
            3,
            Hlc::new(WallMicros(110), 7, PEER),
            200,
            Hlc::new(WallMicros(200), 0, LOCAL),
        ),
        (
            "the physical clock overtakes an agreed wall time",
            100,
            3,
            Hlc::new(WallMicros(100), 7, PEER),
            300,
            Hlc::new(WallMicros(300), 0, LOCAL),
        ),
        (
            "a peer level with the physical clock but behind the local clock",
            100,
            3,
            Hlc::new(WallMicros(60), 99, PEER),
            60,
            Hlc::new(WallMicros(100), 4, LOCAL),
        ),
    ];

    for (name, wall, counter, peer, physical, expected) in cases {
        let (clock, _manual) = clock_at(wall, counter, physical);
        check!(clock.observe(peer) == Ok(expected), "{name}");
    }
}

#[test]
fn a_received_stamp_always_sorts_below_the_stamp_the_receive_returns() {
    let cases: [(&str, i64, u32, Hlc, i64); 4] = [
        ("peer ahead", 100, 3, Hlc::new(WallMicros(120), 7, PEER), 50),
        ("peer behind", 100, 3, Hlc::new(WallMicros(20), 7, PEER), 50),
        ("peer level", 100, 3, Hlc::new(WallMicros(100), 7, PEER), 50),
        (
            "peer level with a higher counter",
            100,
            3,
            Hlc::new(WallMicros(100), 40, PEER),
            50,
        ),
    ];

    for (name, wall, counter, peer, physical) in cases {
        let (clock, _manual) = clock_at(wall, counter, physical);
        let merged = clock.observe(peer).unwrap();
        check!(peer < merged, "{name}");
        check!(clock.now().unwrap() > merged, "{name}");
    }
}

#[test]
fn a_stamp_behind_the_local_clock_is_always_accepted() {
    let (clock, _manual) = clock_at(BASE, 0, BASE);
    let old = Hlc::new(WallMicros(BASE - 86_400_000_000), 0, PEER);

    check!(clock.observe(old) == Ok(Hlc::new(WallMicros(BASE), 1, LOCAL)));
    check!(clock.observation().refusals == 0);
    check!(clock.observation().peer_advances == 0);
}

#[test]
fn the_self_fence_refuses_a_peer_too_far_ahead_and_does_not_move() {
    let (clock, _manual) = clock_at(BASE, 0, BASE);
    let far_future = Hlc::new(WallMicros(BASE + 600_000), 4, PEER);

    check!(
        clock.observe(far_future)
            == Err(HlcError::PeerTooFarAhead {
                ahead_micros: 600_000,
                max_micros: 500_000,
            })
    );
    // The local clock did not absorb the peer's wall time.
    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE), 1, LOCAL)));
    check!(clock.observation().drift.micros_i64() == 0);
}

#[test]
fn the_fence_admits_a_peer_exactly_at_the_maximum_offset() {
    let cases: [(&str, i64, bool); 4] = [
        ("one microsecond inside the fence", 499_999, true),
        ("exactly at the fence", 500_000, true),
        ("one microsecond outside the fence", 500_001, false),
        ("a clock set to the wrong year", 86_400_000_000, false),
    ];

    for (name, ahead, accepted) in cases {
        let (clock, _manual) = clock_at(BASE, 0, BASE);
        let peer = Hlc::new(WallMicros(BASE + ahead), 0, PEER);
        let expected = if accepted {
            Ok(Hlc::new(WallMicros(BASE + ahead), 1, LOCAL))
        } else {
            Err(HlcError::PeerTooFarAhead {
                ahead_micros: ahead,
                max_micros: 500_000,
            })
        };
        check!(clock.observe(peer) == expected, "{name}");
    }
}

#[test]
fn a_configured_maximum_offset_replaces_the_default() {
    let manual = Arc::new(ManualClock::new(WallMicros(BASE)));
    let clock = HlcClock::with_max_offset(LOCAL, Arc::clone(&manual), millis(10));
    check!(clock.max_offset().millis_i64() == 10);
    check!(clock.node() == LOCAL);

    let inside = Hlc::new(WallMicros(BASE + 10_000), 0, PEER);
    let outside = Hlc::new(WallMicros(BASE + 10_001), 0, PEER);
    check!(clock.observe(inside).is_ok());
    check!(
        clock.observe(outside)
            == Err(HlcError::PeerTooFarAhead {
                ahead_micros: 10_001,
                max_micros: 10_000,
            })
    );
}

#[test]
fn a_negative_maximum_offset_refuses_every_peer_above_the_physical_clock() {
    let manual = Arc::new(ManualClock::new(WallMicros(BASE)));
    let clock = HlcClock::with_max_offset(LOCAL, Arc::clone(&manual), Time::from_millis(-5));
    check!(clock.max_offset().micros_i64() == 0);

    let ahead = Hlc::new(WallMicros(BASE + 1), 0, PEER);
    check!(
        clock.observe(ahead)
            == Err(HlcError::PeerTooFarAhead {
                ahead_micros: 1,
                max_micros: 0,
            })
    );
    check!(clock.observe(Hlc::new(WallMicros(BASE), 0, PEER)).is_ok());
}

#[test]
fn the_default_maximum_offset_is_five_hundred_milliseconds() {
    check!(DEFAULT_MAX_OFFSET.millis_i64() == 500);
    check!(HlcClock::new(LOCAL).max_offset() == DEFAULT_MAX_OFFSET);
}

#[test]
fn the_observation_reports_drift_advances_and_refusals() {
    let manual = Arc::new(ManualClock::new(WallMicros(BASE)));
    let clock = HlcClock::with_clock(LOCAL, Arc::clone(&manual));
    clock.now().unwrap();

    let fresh = clock.observation();
    check!(fresh.drift.micros_i64() == 0);
    check!(fresh.peer_advances == 0);
    check!(fresh.refusals == 0);
    check!(fresh.max_peer_ahead.micros_i64() == 0);

    // A record from a node 40 milliseconds ahead pulls this clock forward, and
    // the pull is the measurement: 40 milliseconds of skew on the data path.
    clock
        .observe(Hlc::new(WallMicros(BASE + 40_000), 0, PEER))
        .unwrap();
    let pulled = clock.observation();
    check!(pulled.drift == millis(40));
    check!(pulled.peer_advances == 1);
    check!(pulled.refusals == 0);
    check!(pulled.max_peer_ahead == millis(40));

    // A record from a node with a broken clock is refused, and it is counted.
    clock
        .observe(Hlc::new(WallMicros(BASE + 600_000), 0, PEER))
        .unwrap_err();
    let fenced = clock.observation();
    check!(fenced.drift == millis(40));
    check!(fenced.peer_advances == 1);
    check!(fenced.refusals == 1);
    check!(fenced.max_peer_ahead == micros(600_000));

    // The physical clock catches up, so the drift closes.
    manual.advance(millis(40));
    check!(clock.observation().drift.micros_i64() == 0);
}

#[test]
fn a_peer_behind_the_physical_clock_does_not_count_as_an_advance() {
    let (clock, _manual) = clock_at(BASE, 0, BASE + 1_000);
    clock
        .observe(Hlc::new(WallMicros(BASE + 500), 0, PEER))
        .unwrap();

    let observation = clock.observation();
    check!(observation.peer_advances == 0);
    check!(observation.refusals == 0);
    // The delta is still recorded, because the peer is ahead of nothing.
    check!(observation.max_peer_ahead.micros_i64() == 0);
}

#[test]
fn two_nodes_that_agree_on_wall_and_counter_still_have_one_order() {
    let earlier = Hlc::new(WallMicros(4), 9, NodeId(9));
    let node_one = Hlc::new(WallMicros(5), 2, NodeId(1));
    let node_two = Hlc::new(WallMicros(5), 2, NodeId(2));
    let later_counter = Hlc::new(WallMicros(5), 3, NodeId(1));

    check!(node_one < node_two);
    check!(node_two < later_counter);

    let mut stamps = vec![node_two, later_counter, earlier, node_one];
    stamps.sort();
    check!(stamps == vec![earlier, node_one, node_two, later_counter]);
}

#[test]
fn a_counter_at_its_maximum_is_reported_and_never_wraps() {
    let (clock, _manual) = clock_at(BASE, 0, BASE);
    let saturated = Hlc::new(WallMicros(BASE), u32::MAX, PEER);

    check!(clock.observe(saturated) == Err(HlcError::CounterOverflow));
    // The clock refused the merge, so it still stamps from where it was.
    check!(clock.now() == Ok(Hlc::new(WallMicros(BASE), 1, LOCAL)));
}

#[test]
fn a_borrowed_and_a_shared_physical_clock_both_drive_the_state_machine() {
    let manual = ManualClock::new(WallMicros(BASE));
    let borrowed = HlcClock::with_clock(LOCAL, &manual);
    check!(borrowed.now() == Ok(Hlc::new(WallMicros(BASE), 1, LOCAL)));
    manual.advance(micros(1));
    check!(borrowed.now() == Ok(Hlc::new(WallMicros(BASE + 1), 0, LOCAL)));

    let shared = Arc::new(ManualClock::new(WallMicros(BASE)));
    let arc_driven = HlcClock::with_clock(LOCAL, Arc::clone(&shared));
    check!(arc_driven.now() == Ok(Hlc::new(WallMicros(BASE), 1, LOCAL)));
    shared.set(WallMicros(BASE + 2));
    check!(arc_driven.now() == Ok(Hlc::new(WallMicros(BASE + 2), 0, LOCAL)));
}
