//! Owned batch decoding must agree across contiguous and fragmented input.

use assert2::assert;
use bytes::{Buf, Bytes, BytesMut};
use krabka_compression::{CompressionType, RecordDecompressionPolicy};
use krabka_protocol::records::{Attributes, Record, RecordBatch, RecordHeader, RecordsError};
use krabka_units::{bytes, fraction};

const CODECS: [CompressionType; 5] = [
    CompressionType::None,
    CompressionType::Gzip,
    CompressionType::Lz4,
    CompressionType::Snappy,
    CompressionType::Zstd,
];

fn fixture(codec: CompressionType) -> RecordBatch {
    let mut batch = RecordBatch {
        attributes: Attributes::default(),
        base_offset: 17,
        partition_leader_epoch: 3,
        last_offset_delta: 1,
        base_timestamp: 1000,
        max_timestamp: 1007,
        producer_id: 42,
        producer_epoch: 2,
        base_sequence: 7,
        records: vec![
            Record {
                attributes: 1,
                timestamp_delta: -7,
                key: Some(Bytes::from_static(b"key")),
                value: Some(Bytes::from(vec![0xAB; 8192])),
                headers: vec![RecordHeader {
                    key: "header".into(),
                    value: Some(Bytes::from_static(b"value")),
                }],
                ..Default::default()
            },
            Record {
                timestamp_delta: 7,
                offset_delta: 1,
                ..Default::default()
            },
        ],
    };
    batch.attributes = batch.attributes.with_compression(codec);
    batch
}

fn encode(batch: &RecordBatch) -> Vec<u8> {
    let mut out = BytesMut::new();
    batch.encode(&mut out).unwrap();
    out.to_vec()
}

fn outcome(result: Result<RecordBatch, RecordsError>) -> Result<RecordBatch, String> {
    result.map_err(|error| format!("{error:?}"))
}

fn compare_buffers(wire: &[u8], policy: RecordDecompressionPolicy) {
    let mut contiguous = wire;
    let expected = outcome(RecordBatch::decode_with_policy(&mut contiguous, policy));
    for split in [
        0,
        1,
        60,
        61,
        62,
        wire.len() / 2,
        wire.len().saturating_sub(1),
        wire.len(),
    ] {
        let split = split.min(wire.len());
        let mut fragmented = wire[..split].chain(&wire[split..]);
        let actual = outcome(RecordBatch::decode_with_policy(&mut fragmented, policy));
        assert!((actual, fragmented.remaining()) == (expected.clone(), contiguous.len()));
    }
}

#[test]
fn buffers_preserve_whole_batches_and_stop_before_the_next_batch() {
    for codec in CODECS {
        let batch = fixture(codec);
        let wire = encode(&batch);
        let mut sequence = wire.clone();
        sequence.extend_from_slice(&encode(&fixture(CompressionType::None)));
        let mut cur = sequence.as_slice();
        assert!(RecordBatch::decode(&mut cur).unwrap() == batch);
        assert!(cur == &sequence[wire.len()..]);
        compare_buffers(&sequence, RecordDecompressionPolicy::default());
    }
}

#[test]
fn buffer_shapes_preserve_crc_errors_limits_and_cursor_consumption() {
    let limited = RecordDecompressionPolicy::new(fraction(1.0), bytes(1), bytes(32)).unwrap();
    for codec in CODECS {
        let wire = encode(&fixture(codec));
        compare_buffers(&wire, limited);
        for cut in 0..wire.len() {
            compare_buffers(&wire[..cut], RecordDecompressionPolicy::default());
        }
        for pos in 0..wire.len() {
            let mut corrupt = wire.clone();
            corrupt[pos] ^= 0x80;
            compare_buffers(&corrupt, RecordDecompressionPolicy::default());
        }
        // With a valid CRC, malformed compressed data reaches decompression.
        // A negative count reaches record parsing after decompression succeeds.
        for body_corrupt in [false, true] {
            let mut corrupt = wire.clone();
            if body_corrupt {
                corrupt[61..].fill(0xFF);
            } else {
                corrupt[57..61].copy_from_slice(&(-1_i32).to_be_bytes());
            }
            let crc = crc32c::crc32c(&corrupt[21..]);
            corrupt[17..21].copy_from_slice(&crc.to_be_bytes());
            let mut cur = corrupt.as_slice();
            assert!(RecordBatch::decode(&mut cur).is_err());
            assert!(cur.is_empty());
            compare_buffers(&corrupt, RecordDecompressionPolicy::default());
        }
    }
}
