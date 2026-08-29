//! Carriage of a stamp on Kafka record headers.

use assert2::check;
use krabka_hlc::{HLC_HEADER, Hlc, WallMicros, extract_from_headers};
use krabka_ids::NodeId;

// A named case plus the record headers it feeds to `extract_from_headers`.
type HeaderCase<'a> = (&'a str, Vec<(&'a str, Vec<u8>)>);

fn stamp() -> Hlc {
    Hlc::new(WallMicros(1_700_000_000_000_000), 3, NodeId(7))
}

#[test]
fn header_pairs_the_krabka_key_with_the_encoded_value() {
    let (key, value) = stamp().header().unwrap();
    check!(key == "krabka.hlc");
    check!(key == HLC_HEADER);
    check!(value == stamp().encode().unwrap());
}

#[test]
fn a_stamped_record_round_trips_through_its_headers() {
    let (key, value) = stamp().header().unwrap();
    let headers = [("other", b"x".as_slice()), (key, value.as_slice())];
    check!(extract_from_headers(headers) == Some(stamp()));
}

#[test]
fn extraction_is_silent_for_anything_it_cannot_read() {
    let good = stamp().encode().unwrap();
    let short = good[..15].to_vec();
    let mut long = good.to_vec();
    long.push(0);

    let cases: [HeaderCase<'_>; 7] = [
        ("no headers at all", vec![]),
        (
            "an unstamped record",
            vec![("content-type", b"application/json".to_vec())],
        ),
        ("an empty value", vec![(HLC_HEADER, Vec::new())]),
        ("a short value", vec![(HLC_HEADER, short)]),
        ("a long value", vec![(HLC_HEADER, long)]),
        (
            "a key of the wrong case",
            vec![("KRABKA.HLC", good.to_vec())],
        ),
        (
            "a key with a different namespace",
            vec![("kafka.hlc", good.to_vec())],
        ),
    ];

    for (name, headers) in cases {
        let borrowed: Vec<(&str, &[u8])> = headers
            .iter()
            .map(|(key, value)| (*key, value.as_slice()))
            .collect();
        check!(extract_from_headers(borrowed).is_none(), "{name}");
    }
}

#[test]
fn the_last_header_with_the_key_wins() {
    // Kafka lets a record carry one key more than once, and its own
    // `Headers.lastHeader` reads the last one.
    let first = Hlc::new(WallMicros(10), 0, NodeId(1));
    let last = Hlc::new(WallMicros(20), 1, NodeId(2));
    let first_bytes = first.encode().unwrap();
    let last_bytes = last.encode().unwrap();
    let headers = [
        (HLC_HEADER, first_bytes.as_slice()),
        (HLC_HEADER, last_bytes.as_slice()),
    ];
    check!(extract_from_headers(headers) == Some(last));
}

#[test]
fn an_unreadable_last_value_hides_a_readable_earlier_one() {
    // The reader takes the last value and validates it. It does not search
    // backwards for a value that happens to parse, because the value the
    // producer wrote last is the one it meant.
    let good = stamp().encode().unwrap();
    let headers = [
        (HLC_HEADER, good.as_slice()),
        (HLC_HEADER, b"too short".as_slice()),
    ];
    check!(extract_from_headers(headers).is_none());
}
