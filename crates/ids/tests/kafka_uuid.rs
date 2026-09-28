//! Every expected string, value, and error message below is the output of
//! `org.apache.kafka.common.Uuid` from the `kafka-clients-4.3.1.jar` in the
//! `apache/kafka:4.3.1` image, run on its Temurin 21 JVM.

use std::collections::HashSet;

use krabka_ids::{KafkaUuid, KafkaUuidError};
use uuid::Uuid;

#[test]
fn display_matches_kafka_to_string() {
    let cases = [
        (0, 0, "AAAAAAAAAAAAAAAAAAAAAA"),
        (0, 1, "AAAAAAAAAAAAAAAAAAAAAQ"),
        (0, 2, "AAAAAAAAAAAAAAAAAAAAAg"),
        (-1, -1, "_____________________w"),
        (i64::MIN, 0, "gAAAAAAAAAAAAAAAAAAAAA"),
        (34, 98, "AAAAAAAAACIAAAAAAAAAYg"),
        (1, 0, "AAAAAAAAAAEAAAAAAAAAAA"),
        (
            0x123e_4567_e89b_12d3,
            0xa456_4266_1417_4000_u64.cast_signed(),
            "Ej5FZ-ibEtOkVkJmFBdAAA",
        ),
        (
            0x517c_94f6_2e40_467e,
            0x96d3_438d_dad6_9e58_u64.cast_signed(),
            "UXyU9i5ARn6W00ON2taeWA",
        ),
    ];
    for (msb, lsb, expected) in cases {
        let id = KafkaUuid::from_bits(msb, lsb);
        assert2::assert!(id.to_string() == expected);
        assert2::assert!((id.most_significant_bits(), id.least_significant_bits()) == (msb, lsb));
    }
}

#[test]
fn from_str_accepts_what_kafka_accepts() {
    let cases = [
        ("AAAAAAAAAAAAAAAAAAAAAA", 0, 0, "AAAAAAAAAAAAAAAAAAAAAA"),
        ("AAAAAAAAAAAAAAAAAAAAAQ", 0, 1, "AAAAAAAAAAAAAAAAAAAAAQ"),
        (
            "UXyU9i5ARn6W00ON2taeWA",
            0x517c_94f6_2e40_467e,
            0x96d3_438d_dad6_9e58_u64.cast_signed(),
            "UXyU9i5ARn6W00ON2taeWA",
        ),
        (
            "U-hCdTzaSPyFB3swwc7Qdw",
            0x53e8_4275_3cda_48fc,
            0x8507_7b30_c1ce_d077_u64.cast_signed(),
            "U-hCdTzaSPyFB3swwc7Qdw",
        ),
        ("_____________________w", -1, -1, "_____________________w"),
        ("_____________________A", -1, -4, "_____________________A"),
        // Kafka ignores the four unused bits of the last character.
        ("_____________________x", -1, -1, "_____________________w"),
        ("AAAAAAAAAAAAAAAAAAAAAB", 0, 0, "AAAAAAAAAAAAAAAAAAAAAA"),
        // And it accepts canonical padding.
        ("AAAAAAAAAAAAAAAAAAAAAA==", 0, 0, "AAAAAAAAAAAAAAAAAAAAAA"),
    ];
    for (input, msb, lsb, rendered) in cases {
        let id: KafkaUuid = input.parse().unwrap();
        assert2::assert!(id == KafkaUuid::from_bits(msb, lsb), "{input}");
        assert2::assert!(id.to_string() == rendered);
    }
}

#[test]
fn from_str_rejects_what_kafka_rejects_with_its_message() {
    let a22 = "A".repeat(22);
    let a21 = "A".repeat(21);
    let a20 = "A".repeat(20);
    let cases = [
        (
            "A".repeat(25),
            "Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAAA` is too long to be decoded as a \
             base64 UUID",
        ),
        (
            "A".repeat(43),
            "Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAAA` is too long to be decoded as a \
             base64 UUID",
        ),
        (
            "123e4567-e89b-12d3-a456-426614174000".to_owned(),
            "Input string with prefix `123e4567-e89b-12d3-a456-` is too long to be decoded as a \
             base64 UUID",
        ),
        (
            format!("{}==", "A".repeat(23)),
            "Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAA=` is too long to be decoded as a \
             base64 UUID",
        ),
        (
            format!("{}\u{1f600}", "A".repeat(23)),
            "Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAA?` is too long to be decoded as a \
             base64 UUID",
        ),
        (
            "A".repeat(23),
            "Input string `AAAAAAAAAAAAAAAAAAAAAAA` decoded as 17 bytes, which is not equal to \
             the expected 16 bytes of a base64-encoded UUID",
        ),
        (
            "A".repeat(24),
            "Input string `AAAAAAAAAAAAAAAAAAAAAAAA` decoded as 18 bytes, which is not equal to \
             the expected 16 bytes of a base64-encoded UUID",
        ),
        (
            format!("{}=", "A".repeat(23)),
            "Input string `AAAAAAAAAAAAAAAAAAAAAAA=` decoded as 17 bytes, which is not equal to \
             the expected 16 bytes of a base64-encoded UUID",
        ),
        (
            String::new(),
            "Input string `` decoded as 0 bytes, which is not equal to the expected 16 bytes of a \
             base64-encoded UUID",
        ),
        (
            "AAAAAA".to_owned(),
            "Input string `AAAAAA` decoded as 4 bytes, which is not equal to the expected 16 \
             bytes of a base64-encoded UUID",
        ),
        (
            "AA==".to_owned(),
            "Input string `AA==` decoded as 1 bytes, which is not equal to the expected 16 bytes \
             of a base64-encoded UUID",
        ),
        (
            "AAA=".to_owned(),
            "Input string `AAA=` decoded as 2 bytes, which is not equal to the expected 16 bytes \
             of a base64-encoded UUID",
        ),
        (
            "A".to_owned(),
            "Input byte[] should at least have 2 bytes for base64 bytes",
        ),
        (
            "\u{1f600}".to_owned(),
            "Input byte[] should at least have 2 bytes for base64 bytes",
        ),
        (a21.clone(), "Last unit does not have enough valid bits"),
        (
            format!("{a21}="),
            "Last unit does not have enough valid bits",
        ),
        (
            format!("{a21}=="),
            "Last unit does not have enough valid bits",
        ),
        (
            "A===".to_owned(),
            "Last unit does not have enough valid bits",
        ),
        (format!("{a20}+A"), "Illegal base64 character 2b"),
        (format!("{a20}/A"), "Illegal base64 character 2f"),
        (format!("{a20} A"), "Illegal base64 character 20"),
        (format!("{a21}\u{0}"), "Illegal base64 character 0"),
        (format!("{a21}\u{7f}"), "Illegal base64 character 7f"),
        (format!("{a21}\u{e9}"), "Illegal base64 character -17"),
        (format!("{a22}\u{e9}"), "Illegal base64 character -17"),
        (format!("{a21}\u{20ac}"), "Illegal base64 character 3f"),
        (format!("{a21}\u{100}"), "Illegal base64 character 3f"),
        ("AA\u{1f600}".to_owned(), "Illegal base64 character 3f"),
        (
            format!("{a22}="),
            "Input byte array has wrong 4-byte ending unit",
        ),
        (
            format!("{a22}=A"),
            "Input byte array has wrong 4-byte ending unit",
        ),
        (
            format!("{a20}=A"),
            "Input byte array has wrong 4-byte ending unit",
        ),
        (
            format!("{a20}=="),
            "Input byte array has wrong 4-byte ending unit",
        ),
        (
            format!("{a20}=\u{e9}"),
            "Input byte array has wrong 4-byte ending unit",
        ),
        (
            format!("={a21}"),
            "Input byte array has wrong 4-byte ending unit",
        ),
        (
            format!("AAAA={}", "A".repeat(17)),
            "Input byte array has wrong 4-byte ending unit",
        ),
        (
            "AAA=AAAA".to_owned(),
            "Input byte array has incorrect ending byte at 4",
        ),
        (
            "AA==AAAA".to_owned(),
            "Input byte array has incorrect ending byte at 4",
        ),
    ];
    for (input, message) in cases {
        let error = input.parse::<KafkaUuid>().unwrap_err();
        assert2::assert!(error.to_string() == message, "{input:?}");
    }
}

#[test]
fn from_str_errors_carry_structured_fields() {
    let cases = [
        (
            "A".repeat(30),
            KafkaUuidError::TooLong {
                prefix: "A".repeat(24),
            },
        ),
        (
            "AAAAAA".to_owned(),
            KafkaUuidError::WrongLength {
                input: "AAAAAA".to_owned(),
                decoded_len: 4,
            },
        ),
        ("A".to_owned(), KafkaUuidError::TooShort),
        (
            format!("{}+", "A".repeat(21)),
            KafkaUuidError::IllegalCharacter { byte: b'+' },
        ),
        (
            "AAA=AAAA".to_owned(),
            KafkaUuidError::IncorrectEndingByte { position: 4 },
        ),
    ];
    for (input, expected) in cases {
        assert2::assert!(input.parse::<KafkaUuid>() == Err(expected));
    }
}

#[test]
fn every_rendered_id_parses_back_to_itself() {
    let mut ids = vec![
        KafkaUuid::ZERO,
        KafkaUuid::ONE,
        KafkaUuid::DIRECTORY_LOST,
        KafkaUuid::from_bits(-1, -1),
        KafkaUuid::from_bits(i64::MIN, i64::MAX),
    ];
    ids.extend((0..200).map(|_| KafkaUuid::random()));
    for id in ids {
        let rendered = id.to_string();
        assert2::assert!(rendered.len() == 22);
        assert2::assert!(rendered.parse::<KafkaUuid>() == Ok(id));
        assert2::assert!(KafkaUuid::parse_kafka_or_hyphenated(&rendered) == Ok(id));
        assert2::assert!(
            KafkaUuid::parse_kafka_or_hyphenated(&id.get().hyphenated().to_string()) == Ok(id)
        );
    }
}

#[test]
fn parse_kafka_or_hyphenated_accepts_both_forms() {
    let expected = KafkaUuid::from_bits(
        0x123e_4567_e89b_12d3,
        0xa456_4266_1417_4000_u64.cast_signed(),
    );
    let accepted = [
        "Ej5FZ-ibEtOkVkJmFBdAAA",
        "123e4567-e89b-12d3-a456-426614174000",
        "123E4567-E89B-12D3-A456-426614174000",
    ];
    for input in accepted {
        assert2::assert!(KafkaUuid::parse_kafka_or_hyphenated(input) == Ok(expected));
    }
}

#[test]
fn parse_kafka_or_hyphenated_rejects_bad_input() {
    let hyphenated_but_not_hex = "123e4567-e89b-12d3-a456-42661417400g";
    let error = KafkaUuid::parse_kafka_or_hyphenated(hyphenated_but_not_hex).unwrap_err();
    assert2::assert!(matches!(
        &error,
        KafkaUuidError::Hyphenated { input, .. } if input == hyphenated_but_not_hex
    ));

    // Other forms that `uuid` accepts are not the hyphenated form.
    let rejected = [
        (
            "123e4567e89b12d3a456426614174000",
            "Input string with prefix `123e4567e89b12d3a4564266` is too long to be decoded as a \
             base64 UUID",
        ),
        (
            "{123e4567-e89b-12d3-a456-426614174000}",
            "Input string with prefix `{123e4567-e89b-12d3-a456` is too long to be decoded as a \
             base64 UUID",
        ),
        (
            "urn:uuid:123e4567-e89b-12d3-a456-426614174000",
            "Input string with prefix `urn:uuid:123e4567-e89b-1` is too long to be decoded as a \
             base64 UUID",
        ),
        (
            "A",
            "Input byte[] should at least have 2 bytes for base64 bytes",
        ),
    ];
    for (input, message) in rejected {
        let error = KafkaUuid::parse_kafka_or_hyphenated(input).unwrap_err();
        assert2::assert!(error.to_string() == message, "{input}");
    }
}

#[test]
fn reserved_ids_match_kafka() {
    let constants = [
        (KafkaUuid::ZERO, "AAAAAAAAAAAAAAAAAAAAAA"),
        (KafkaUuid::ONE, "AAAAAAAAAAAAAAAAAAAAAQ"),
        (KafkaUuid::METADATA_TOPIC_ID, "AAAAAAAAAAAAAAAAAAAAAQ"),
        (KafkaUuid::DIRECTORY_MIGRATING, "AAAAAAAAAAAAAAAAAAAAAA"),
        (KafkaUuid::DIRECTORY_UNASSIGNED, "AAAAAAAAAAAAAAAAAAAAAQ"),
        (KafkaUuid::DIRECTORY_LOST, "AAAAAAAAAAAAAAAAAAAAAg"),
        (KafkaUuid::default(), "AAAAAAAAAAAAAAAAAAAAAA"),
    ];
    for (id, rendered) in constants {
        assert2::assert!(id.to_string() == rendered);
    }
    assert2::assert!(KafkaUuid::RESERVED == [KafkaUuid::ZERO, KafkaUuid::ONE]);

    // (id, Uuid.RESERVED.contains, DirectoryId.reserved)
    let cases = [
        (KafkaUuid::ZERO, true, true),
        (KafkaUuid::ONE, true, true),
        (KafkaUuid::DIRECTORY_LOST, false, true),
        (KafkaUuid::from_bits(0, 99), false, true),
        (KafkaUuid::from_bits(0, 100), false, false),
        (KafkaUuid::from_bits(0, -1), false, true),
        (KafkaUuid::from_bits(1, 0), false, false),
        (KafkaUuid::from_bits(1, 1), false, false),
        (KafkaUuid::from_bits(-1, 0), false, false),
    ];
    for (id, reserved, reserved_directory_id) in cases {
        assert2::assert!(
            (id.is_reserved(), id.is_reserved_directory_id()) == (reserved, reserved_directory_id),
            "{id}"
        );
    }
}

#[test]
fn random_ids_obey_kafka_generation_rules() {
    let mut seen = HashSet::new();
    for _ in 0..1000 {
        let id = KafkaUuid::random();
        assert2::assert!(!id.is_reserved());
        assert2::assert!(!id.to_string().starts_with('-'));
        assert2::assert!(id.get().get_version_num() == 4);
        seen.insert(id);

        let directory_id = KafkaUuid::random_directory_id();
        assert2::assert!(!directory_id.is_reserved_directory_id());
        assert2::assert!(!directory_id.to_string().starts_with('-'));
    }
    assert2::assert!(seen.len() == 1000);
}

#[test]
fn ordering_is_kafka_compare_to_over_signed_halves() {
    // UuidTest.testCompareUuids, and a pair where signed and unsigned order
    // disagree.
    let ascending = [
        KafkaUuid::from_bits(i64::MIN, 0),
        KafkaUuid::from_bits(-1, -1),
        KafkaUuid::from_bits(0, -1),
        KafkaUuid::from_bits(0, 0),
        KafkaUuid::from_bits(0, 1),
        KafkaUuid::from_bits(1, 0),
        KafkaUuid::from_bits(i64::MAX, 0),
    ];
    let mut sorted = ascending;
    sorted.reverse();
    sorted.sort();
    assert2::assert!(sorted == ascending);
    assert2::assert!(
        KafkaUuid::from_bits(0, 0)
            .cmp(&KafkaUuid::from_bits(0, 0))
            .is_eq()
    );
}

#[test]
fn serde_uses_the_kafka_string() {
    let id = KafkaUuid::from_bits(
        0x517c_94f6_2e40_467e,
        0x96d3_438d_dad6_9e58_u64.cast_signed(),
    );
    assert2::assert!(serde_json::to_string(&id).unwrap() == r#""UXyU9i5ARn6W00ON2taeWA""#);
    assert2::assert!(
        serde_json::from_str::<KafkaUuid>(r#""UXyU9i5ARn6W00ON2taeWA""#).unwrap() == id
    );

    let rejected = [
        (
            r#""123e4567-e89b-12d3-a456-426614174000""#,
            "Input string with prefix `123e4567-e89b-12d3-a456-` is too long to be decoded as a \
             base64 UUID",
        ),
        (
            r#""AAAAAA""#,
            "Input string `AAAAAA` decoded as 4 bytes, which is not equal to the expected 16 \
             bytes of a base64-encoded UUID",
        ),
    ];
    for (json, message) in rejected {
        let error = serde_json::from_str::<KafkaUuid>(json).unwrap_err();
        assert2::assert!(error.to_string().starts_with(message), "{json}");
    }
}

#[test]
fn conversions_to_and_from_uuid() {
    let raw = Uuid::from_u128(0x0123_4567_89ab_cdef_fedc_ba98_7654_3210);
    let id = KafkaUuid::from(raw);
    assert2::assert!(id == KafkaUuid(raw));
    assert2::assert!(id.get() == raw);
    assert2::assert!(Uuid::from(id) == raw);
    assert2::assert!(format!("{:>24}", KafkaUuid::ONE) == "  AAAAAAAAAAAAAAAAAAAAAQ");
}
