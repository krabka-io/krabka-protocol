//! Shared checks and sample values for the krabka-private codec tests.

use assert2::{assert, check};
use bytes::{Bytes, BytesMut};

use crate::{Decode, Encode, UnknownTaggedField, UnknownTaggedFields};

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
    roundtrip_case(std::any::type_name::<T>(), message, version);
}

/// [`roundtrip`] for one row of a table-driven test.
///
/// Each check names `label`, so a table of many rows still says which row
/// failed.
pub(crate) fn roundtrip_case<T>(label: &str, message: &T, version: i16)
where
    T: Encode + for<'de> Decode<'de> + PartialEq + std::fmt::Debug,
{
    let mut buf = BytesMut::new();
    message.encode(&mut buf, version).unwrap();
    check!(message.encoded_len(version) == buf.len(), "{label}");

    let bytes = buf.freeze();
    let mut cursor = &bytes[..];
    let decoded = T::decode(&mut cursor, version).unwrap();
    check!(cursor.is_empty(), "{label}");
    check!(&decoded == message, "{label}");

    let mut reencoded = BytesMut::new();
    decoded.encode(&mut reencoded, version).unwrap();
    check!(&reencoded[..] == &bytes[..], "{label}");
}

/// Checks that a decode of any short prefix of `message` fails.
///
/// Every field of a krabka-private message takes at least one byte, so a prefix
/// can never hold a whole message. A decode of a prefix returns an error, and
/// it does not panic.
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
