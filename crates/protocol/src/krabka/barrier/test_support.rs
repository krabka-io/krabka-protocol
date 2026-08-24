//! Shared checks and sample values for the barrier codec tests.

use assert2::assert;
use bytes::{Bytes, BytesMut};

use crate::{
    Decode, Encode, UnknownTaggedField, UnknownTaggedFields,
    krabka::barrier::common::{BarrierCutPartition, BarrierCutTopic, BarrierMissingPartition},
};

/// Encodes `message`, checks the predicted length, decodes it back, and encodes
/// the decoded value again.
///
/// The check holds four properties at once. `encoded_len` predicts the exact
/// byte count. A decode consumes every byte that the encode wrote. A decode
/// returns a value equal to the original. A second encode gives the same bytes.
pub(crate) fn roundtrip<T>(message: &T, version: i16)
where
    T: Encode + for<'de> Decode<'de> + PartialEq + std::fmt::Debug,
{
    let mut buf = BytesMut::new();
    message.encode(&mut buf, version).unwrap();
    assert!(message.encoded_len(version) == buf.len());

    let bytes = buf.freeze();
    let mut cursor = &bytes[..];
    let decoded = T::decode(&mut cursor, version).unwrap();
    assert!(cursor.is_empty());
    assert!(&decoded == message);

    let mut reencoded = BytesMut::new();
    decoded.encode(&mut reencoded, version).unwrap();
    assert!(&reencoded[..] == &bytes[..]);
}

/// Checks that a decode of any short prefix of `message` fails.
///
/// Every field of a barrier message takes at least one byte, so a prefix can
/// never hold a whole message.
pub(crate) fn reject_truncated<T>(message: &T, version: i16)
where
    T: Encode + for<'de> Decode<'de> + std::fmt::Debug,
{
    let mut buf = BytesMut::new();
    message.encode(&mut buf, version).unwrap();
    let bytes = buf.freeze();
    for end in 0..bytes.len() {
        let mut cursor = &bytes[..end];
        assert!(T::decode(&mut cursor, version).is_err());
    }
}

/// Versions that sit outside the range `min..=max`.
pub(crate) fn unsupported_versions(min: i16, max: i16) -> Vec<i16> {
    vec![min - 1, max + 1]
}

/// Two unknown tagged fields, in the ascending tag order that a decode needs.
pub(crate) fn sample_tagged_fields() -> UnknownTaggedFields {
    UnknownTaggedFields(vec![
        UnknownTaggedField {
            tag: 7,
            bytes: Bytes::from_static(&[1, 2, 3]),
        },
        UnknownTaggedField {
            tag: 11,
            bytes: Bytes::from_static(&[4]),
        },
    ])
}

/// A cut over two topics, one of them with two partitions.
pub(crate) fn sample_cut_topics() -> Vec<BarrierCutTopic> {
    vec![
        BarrierCutTopic {
            topic: "orders".to_string(),
            partitions: vec![
                BarrierCutPartition {
                    partition: 0,
                    offset: 1_024,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                BarrierCutPartition {
                    partition: 1,
                    offset: 2_048,
                    unknown_tagged_fields: sample_tagged_fields(),
                },
            ],
            unknown_tagged_fields: sample_tagged_fields(),
        },
        BarrierCutTopic {
            topic: "payments".to_string(),
            partitions: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
    ]
}

/// One partition that took no marker.
pub(crate) fn sample_missing_partitions() -> Vec<BarrierMissingPartition> {
    vec![BarrierMissingPartition {
        topic: "payments".to_string(),
        partition: 4,
        unknown_tagged_fields: sample_tagged_fields(),
    }]
}
