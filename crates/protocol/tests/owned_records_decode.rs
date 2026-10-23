//! Owned generated Fetch codecs preserve framing and lenient record decoding.

use assert2::assert;
use bytes::{Buf, Bytes, BytesMut};
use krabka_compression::CompressionType;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Attributes, Record, RecordBatch, RecordsPayload},
};

#[test]
fn generated_fetch_decodes_owned_records_from_fragmented_buffers() {
    for version in [4, 12] {
        for compression in [CompressionType::None, CompressionType::Gzip] {
            let batch = RecordBatch {
                attributes: Attributes::default().with_compression(compression),
                records: vec![Record {
                    value: Some(Bytes::from(vec![42; 8192])),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            };
            let encoded = batch.encode_to_bytes().unwrap();
            let mut truncated = encoded.to_vec();
            truncated.extend_from_slice(&encoded[..encoded.len() - 1]);
            for raw in [
                None,
                Some(Bytes::from_static(b"legacy")),
                Some(encoded),
                Some(Bytes::from(truncated)),
            ] {
                let mut expected = FetchResponse {
                    responses: vec![FetchableTopicResponse {
                        topic: "topic".into(),
                        partitions: vec![PartitionData {
                            records: raw.clone().map(RecordsPayload::Raw),
                            ..PartitionData::default()
                        }],
                        ..FetchableTopicResponse::default()
                    }],
                    ..FetchResponse::default()
                };
                let mut wire = BytesMut::new();
                expected.encode(&mut wire, version).unwrap();
                expected.responses[0].partitions[0].records = raw
                    .map(RecordsPayload::from_fetch_bytes)
                    .transpose()
                    .unwrap();
                let wire = wire.freeze();
                for split in [1, wire.len() / 2, wire.len() - 1] {
                    let mut cursor = wire.slice(..split).chain(wire.slice(split..));
                    let actual = FetchResponse::decode(&mut cursor, version).unwrap();
                    assert!(actual == expected);
                    assert!(cursor.remaining() == 0);
                }
            }
        }
    }
}

#[test]
fn generated_produce_rejects_incomplete_or_corrupt_records() {
    let batch = RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"payload")),
            ..Record::default()
        }],
        ..RecordBatch::default()
    };
    let raw = batch.encode_to_bytes().unwrap();
    let mut corrupt = raw.to_vec();
    corrupt[61] ^= 0xff;
    for version in [3, 9] {
        for raw in [raw.slice(..raw.len() - 1), Bytes::from(corrupt.clone())] {
            let request = ProduceRequest {
                topic_data: vec![TopicProduceData {
                    name: "topic".into(),
                    partition_data: vec![PartitionProduceData {
                        records: Some(RecordsPayload::Raw(raw)),
                        ..PartitionProduceData::default()
                    }],
                    ..TopicProduceData::default()
                }],
                ..ProduceRequest::default()
            };
            let mut wire = BytesMut::new();
            request.encode(&mut wire, version).unwrap();
            assert!(ProduceRequest::decode(&mut wire.freeze(), version).is_err());
        }
    }
}
