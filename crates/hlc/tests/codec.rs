//! The fixed-width byte layout of a stamp, and the validation of a decode.

use assert2::check;
use krabka_hlc::{HLC_ENCODED_LEN, Hlc, HlcError, WallMicros};
use krabka_ids::NodeId;

// A stamp whose three fields hold distinct ascending bytes, so a field that
// moves, or a field written in the wrong byte order, changes the output.
fn distinct_stamp() -> Hlc {
    Hlc::new(
        WallMicros(0x0102_0304_0506_0708),
        0x090a_0b0c,
        NodeId(0x0d0e_0f10),
    )
}

#[test]
fn encode_writes_wall_then_counter_then_node_in_big_endian() {
    let bytes = distinct_stamp().encode().unwrap();
    check!(
        bytes
            == [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, 0x10,
            ]
    );
    check!(bytes.len() == HLC_ENCODED_LEN);
}

#[test]
fn encode_writes_a_negative_wall_time_as_a_twos_complement_i64() {
    let stamp = Hlc::new(WallMicros(-1), 0, NodeId(0));
    let bytes = stamp.encode().unwrap();
    check!(
        bytes
            == [
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0
            ]
    );
    check!(Hlc::decode(&bytes) == Ok(stamp));
}

#[test]
fn decode_reads_back_every_field_of_an_encoded_stamp() {
    let stamp = distinct_stamp();
    let bytes = stamp.encode().unwrap();
    check!(Hlc::decode(&bytes) == Ok(stamp));
}

#[test]
fn decode_rejects_every_length_other_than_sixteen() {
    let cases: [(&str, usize); 6] = [
        ("empty", 0),
        ("one byte", 1),
        ("one byte short", 15),
        ("one byte long", 17),
        ("two stamps", 32),
        ("a full record header", 64),
    ];

    for (name, length) in cases {
        let bytes = vec![0x5a; length];
        check!(
            Hlc::decode(&bytes) == Err(HlcError::Length(length)),
            "{name}"
        );
    }
}

#[test]
fn decode_accepts_the_full_range_of_every_field() {
    let cases: [(&str, Hlc); 4] = [
        (
            "all fields at zero",
            Hlc::new(WallMicros::EPOCH, 0, NodeId(0)),
        ),
        (
            "wall time at i64::MIN",
            Hlc::new(WallMicros(i64::MIN), 0, NodeId(0)),
        ),
        (
            "wall time at i64::MAX",
            Hlc::new(WallMicros(i64::MAX), u32::MAX, NodeId(0)),
        ),
        (
            "node id at the widest the field holds",
            Hlc::new(WallMicros(1), 2, NodeId(u64::from(u32::MAX))),
        ),
    ];

    for (name, stamp) in cases {
        let bytes = stamp.encode().unwrap();
        check!(Hlc::decode(&bytes) == Ok(stamp), "{name}");
    }
}

#[test]
fn encode_refuses_a_node_id_wider_than_the_four_byte_field() {
    let too_wide = u64::from(u32::MAX) + 1;
    let stamp = Hlc::new(WallMicros(1), 0, NodeId(too_wide));
    check!(stamp.encode() == Err(HlcError::NodeIdTooLarge(too_wide)));
    check!(stamp.header() == Err(HlcError::NodeIdTooLarge(too_wide)));
}

#[test]
fn a_peer_value_is_re_rendered_from_the_parsed_stamp() {
    // The crate keeps no peer bytes. A decode followed by an encode returns
    // the same 16 bytes, which is what lets a consumer forward a stamp it read
    // without a copy of the input.
    let peer_bytes: [u8; HLC_ENCODED_LEN] = [
        0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x2a, 0xff, 0xff, 0xff,
        0xfe,
    ];
    let stamp = Hlc::decode(&peer_bytes).unwrap();
    check!(stamp == Hlc::new(WallMicros(i64::MAX), 42, NodeId(0xffff_fffe)));
    check!(stamp.encode() == Ok(peer_bytes));
}

#[test]
fn display_names_the_three_fields() {
    check!(distinct_stamp().to_string() == "72623859790382856.151653132@219025168");
}

#[test]
fn the_wall_time_newtype_converts_to_and_from_its_inner_count() {
    check!(WallMicros::EPOCH.get() == 0);
    check!(WallMicros::from(-42).get() == -42);
    let raw: i64 = WallMicros(7).into();
    check!(raw == 7);
    check!(WallMicros(1) < WallMicros(2));
    check!(WallMicros(9).to_string() == "9");
}

#[test]
fn every_error_message_names_its_cause_and_holds_no_input_bytes() {
    check!(HlcError::Length(3).to_string() == "hlc header value must be 16 bytes, got 3");
    check!(
        HlcError::NodeIdTooLarge(4_294_967_296).to_string()
            == "node id 4294967296 does not fit the 4-byte node-id field"
    );
    check!(
        HlcError::PeerTooFarAhead {
            ahead_micros: 600_000,
            max_micros: 500_000,
        }
        .to_string()
            == "peer wall time is 600000 microseconds ahead of the local physical clock, \
                over the 500000 microsecond maximum offset"
    );
    check!(
        HlcError::CounterOverflow.to_string()
            == "hlc counter reached u32::MAX inside one microsecond"
    );
}
