//! The `KRaft` metadata record-value envelope (`MetadataRecordSerde` /
//! `ApiMessageAndVersion`): a record value is
//! `frameVersion (uvarint, 1) + apiKey (uvarint) + apiVersion (uvarint) + body`.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::primitives::varint::{get_uvarint, put_uvarint, uvarint_len};

/// The only `KRaft` metadata frame version, which is part of the krabka 1.x
/// on-disk contract. Kafka's `AbstractApiMessageSerde.DEFAULT_FRAME_VERSION`
/// is 1, and the `__cluster_metadata` record values of
/// mirror.gcr.io/apache/kafka:4.0.0 start with `0x01`.
pub const FRAME_VERSION: u32 = 1;

/// Decoded envelope header, which is everything before the message body.
///
/// It has no frame version: [`decode_value_header`] accepts only
/// [`FRAME_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueHeader {
    pub api_key: u32,
    pub api_version: u32,
}

/// Error decoding a metadata record value envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    #[error("truncated metadata record value envelope")]
    Truncated,
    /// The envelope declares a frame version other than [`FRAME_VERSION`].
    /// The text matches the `MetadataParseException` of Kafka's
    /// `AbstractApiMessageSerde.read`.
    #[error(
        "Could not deserialize metadata record due to unknown frame version {0}(only frame version 1 is supported)"
    )]
    UnknownFrameVersion(u32),
}

/// Encodes a record value: the envelope header plus the already-encoded `body`
/// bytes.
#[must_use]
pub fn encode_value(api_key: u32, api_version: u32, body: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(
        uvarint_len(FRAME_VERSION) + uvarint_len(api_key) + uvarint_len(api_version) + body.len(),
    );
    put_uvarint(&mut out, FRAME_VERSION);
    put_uvarint(&mut out, api_key);
    put_uvarint(&mut out, api_version);
    out.put_slice(body);
    out.freeze()
}

/// Decodes the envelope header and leaves `buf` positioned at the message body.
///
/// # Errors
/// Returns [`EnvelopeError::Truncated`] if any varint cannot be read, and
/// [`EnvelopeError::UnknownFrameVersion`] for a frame version other than
/// [`FRAME_VERSION`], as Kafka's `AbstractApiMessageSerde.read` refuses one.
pub fn decode_value_header<B: Buf>(buf: &mut B) -> Result<ValueHeader, EnvelopeError> {
    let frame_version = get_uvarint(buf).map_err(|_| EnvelopeError::Truncated)?;
    if frame_version != FRAME_VERSION {
        return Err(EnvelopeError::UnknownFrameVersion(frame_version));
    }
    let api_key = get_uvarint(buf).map_err(|_| EnvelopeError::Truncated)?;
    let api_version = get_uvarint(buf).map_err(|_| EnvelopeError::Truncated)?;
    Ok(ValueHeader {
        api_key,
        api_version,
    })
}

#[cfg(test)]
mod tests {

    use super::*;

    /// `FeatureLevelRecord` apiKey 12, apiVersion 0, with body `01 02`, as
    /// Kafka frames it. These bytes are the 1.x on-disk contract.
    const GOLDEN_VALUE: [u8; 5] = [0x01, 0x0C, 0x00, 0x01, 0x02];

    #[test]
    fn encode_matches_the_golden_bytes() {
        assert2::assert!(encode_value(12, 0, &[0x01, 0x02])[..] == GOLDEN_VALUE);
    }

    #[test]
    fn golden_bytes_decode_to_the_header_and_body() {
        let mut cur: &[u8] = &GOLDEN_VALUE;
        let hdr = decode_value_header(&mut cur);
        assert2::assert!(
            hdr == Ok(ValueHeader {
                api_key: 12,
                api_version: 0,
            })
        );
        assert2::assert!(cur == [0x01, 0x02]);
    }

    #[test]
    fn rejects_a_bad_envelope() {
        for (label, value, want) in [
            ("empty", vec![], EnvelopeError::Truncated),
            ("frame version only", vec![0x01], EnvelopeError::Truncated),
            (
                "frame version 0",
                vec![0x00, 0x0C, 0x00],
                EnvelopeError::UnknownFrameVersion(0),
            ),
            (
                "frame version 2",
                vec![0x02, 0x0C, 0x00],
                EnvelopeError::UnknownFrameVersion(2),
            ),
            (
                "frame version 300 (two-byte varint)",
                vec![0xAC, 0x02, 0x0C, 0x00],
                EnvelopeError::UnknownFrameVersion(300),
            ),
        ] {
            let mut cur: &[u8] = &value;
            assert2::assert!(decode_value_header(&mut cur) == Err(want), "{label}");
        }
    }

    #[test]
    fn unknown_frame_version_reads_as_kafkas_message() {
        assert2::assert!(
            EnvelopeError::UnknownFrameVersion(2).to_string()
                == "Could not deserialize metadata record due to unknown frame version 2(only frame version 1 is supported)"
        );
    }
}
