//! Owned wire encoding agrees with the generic encoder and decodes completely.

use bytes::{Bytes, BytesMut};
use krabka_compression::CompressionType;
use krabka_protocol::records::{Attributes, Record, RecordBatch, RecordHeader};

#[test]
fn owned_wire_preserves_batch_fields_and_bytes() {
    for codec in [
        CompressionType::None,
        CompressionType::Gzip,
        CompressionType::Lz4,
        CompressionType::Snappy,
        CompressionType::Zstd,
    ] {
        for value_len in [0, 63, 64, 8_191, 8_192, 128 * 1024] {
            let mut batch = RecordBatch {
                base_offset: 42,
                partition_leader_epoch: 3,
                attributes: Attributes::default().with_compression(codec),
                last_offset_delta: 1,
                base_timestamp: 1_700_000_000,
                max_timestamp: 1_700_000_001,
                producer_id: 5,
                producer_epoch: 2,
                base_sequence: 7,
                records: vec![
                    Record {
                        attributes: 1,
                        timestamp_delta: -9,
                        key: Some(Bytes::from_static(b"key")),
                        value: Some(Bytes::from(vec![0xAB; value_len])),
                        headers: vec![
                            RecordHeader {
                                key: "header".into(),
                                value: Some(Bytes::from_static(b"value")),
                            },
                            RecordHeader {
                                key: String::new(),
                                value: None,
                            },
                        ],
                        ..Record::default()
                    },
                    Record {
                        timestamp_delta: 1,
                        offset_delta: 1,
                        ..Record::default()
                    },
                ],
            };
            for empty in [false, true] {
                if empty {
                    batch.records.clear();
                }
                let mut expected = BytesMut::new();
                batch.encode(&mut expected).unwrap();
                let wire = batch.encode_to_bytes().unwrap();
                assert2::assert!(wire.as_ref() == expected.as_ref());
                let mut cursor = wire.as_ref();
                let decoded = RecordBatch::decode(&mut cursor).unwrap();
                assert2::assert!((decoded, cursor.is_empty()) == (batch.clone(), true));
            }
        }
    }
}
