//! Truncated and malformed payloads, through the public `decompress`.
//!
//! Every codec must answer a short or corrupt payload with
//! `CompressionError::InvalidData`, never with a panic or a partial result.
//! The payloads are built from a real compressed frame, then cut or
//! corrupted, so each case reaches the check it names.
#![cfg(test)]

use assert2::{assert, check};
use krabka_compression::{CompressionError, CompressionType, compress, decompress};
use krabka_units::mebibytes;

/// A payload that compresses well and spans two snappy chunks and two LZ4
/// blocks.
fn payload() -> Vec<u8> {
    b"krabka short input "
        .iter()
        .copied()
        .cycle()
        .take(160 * 1024)
        .collect()
}

/// The Xerial header that every Kafka snappy payload opens with.
const XERIAL_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
];

/// Decompress `data` with a cap well above the payload, and return the
/// `InvalidData` message, failing the test on any other outcome.
fn invalid(ct: CompressionType, data: &[u8]) -> String {
    match decompress(ct, data, mebibytes(4)) {
        Err(CompressionError::InvalidData(message)) => message,
        other => panic!("{ct:?} accepted a malformed payload: {other:?}"),
    }
}

#[test]
fn every_codec_rejects_an_empty_payload() {
    for ct in [
        CompressionType::Gzip,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Zstd,
    ] {
        let message = invalid(ct, &[]);
        check!(!message.is_empty(), "{ct:?}");
    }
}

#[test]
fn every_codec_rejects_each_truncation_of_a_real_frame() {
    let data = payload();
    for ct in [
        CompressionType::Gzip,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Zstd,
    ] {
        let full = compress(ct, &data).unwrap();
        assert!(
            decompress(ct, &full, mebibytes(4)).unwrap() == data,
            "{ct:?}"
        );
        // Snappy's Xerial framing ends cleanly at every chunk boundary, so a
        // cut there is a shorter valid payload. Cut one byte short of a
        // boundary instead, and at points inside the framing.
        for cut in [1, 3, 7, 15, 17, 19, full.len() / 2, full.len() - 1] {
            let message = invalid(ct, &full[..cut]);
            check!(!message.is_empty(), "{ct:?} cut at {cut}");
        }
    }
}

#[test]
fn snappy_names_the_part_of_the_frame_that_is_short() {
    let full = compress(CompressionType::Snappy, &payload()).unwrap();
    let chunk_len = u32::from_be_bytes(full[16..20].try_into().unwrap()) as usize;
    for (name, data, want) in [
        (
            "shorter than the Xerial header",
            full[..15].to_vec(),
            "too short for xerial header",
        ),
        (
            "a chunk length cut short",
            full[..18].to_vec(),
            "chunk header truncated",
        ),
        (
            "a chunk body cut short",
            full[..20 + chunk_len - 1].to_vec(),
            "chunk body truncated",
        ),
    ] {
        let message = invalid(CompressionType::Snappy, &data);
        check!(message.contains(want), "{name}: {message}");
    }
}

#[test]
fn snappy_rejects_a_bad_magic_and_a_corrupt_chunk() {
    let mut bad_magic = compress(CompressionType::Snappy, &payload())
        .unwrap()
        .to_vec();
    bad_magic[1] = b's';
    check!(invalid(CompressionType::Snappy, &bad_magic).contains("xerial magic"));

    // A chunk whose snappy varint claims more output than the block encodes.
    let mut corrupt = XERIAL_HEADER.to_vec();
    corrupt.extend_from_slice(&3u32.to_be_bytes());
    corrupt.extend_from_slice(&[0x40, 0x00, 0x61]);
    check!(invalid(CompressionType::Snappy, &corrupt).contains("snappy decode"));
}

#[test]
fn lz4_rejects_a_corrupted_block() {
    let mut corrupt = compress(CompressionType::Lz4, &payload()).unwrap().to_vec();
    let middle = corrupt.len() / 2;
    corrupt[middle] ^= 0xff;
    corrupt[middle + 1] ^= 0xff;
    check!(!invalid(CompressionType::Lz4, &corrupt).is_empty());
}

#[test]
fn zstd_rejects_a_frame_with_a_bad_magic_number() {
    // Kafka writes zstd frames without a content checksum, so a flipped byte
    // in the body can decode to different bytes. The magic number is checked.
    let mut corrupt = compress(CompressionType::Zstd, &payload())
        .unwrap()
        .to_vec();
    corrupt[0] ^= 0xff;
    check!(!invalid(CompressionType::Zstd, &corrupt).is_empty());
}
