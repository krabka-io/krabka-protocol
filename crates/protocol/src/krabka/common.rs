//! Codec helpers that the krabka-private messages share.
//!
//! Every message under [`crate::krabka`] is flexible from version 0, so each
//! one writes compact strings, compact arrays, and a tagged-fields trailer.
//! These helpers hold that framing in one place, and each message module reads
//! its fields with them.

use bytes::{Buf, BufMut};

use crate::{
    Decode, Encode, ProtocolError, UnknownTaggedFields,
    primitives::{
        array::{array_len_prefix_len, get_array_len, put_array_len},
        string_bytes::{compact_string_len, get_compact_string_owned, put_compact_string},
    },
    tagged_fields::{WriteTaggedFields, read_tagged_fields, tagged_fields_len},
};

/// Every krabka-private message is flexible from version 0, so each
/// array-length prefix takes the compact form from KIP-482.
pub(crate) const FLEXIBLE: bool = true;

/// Encoded width of a `bool` field.
pub(crate) const BOOL_LEN: usize = 1;
/// Encoded width of an `i8` field.
pub(crate) const I8_LEN: usize = 1;
/// Encoded width of an `i16` field.
pub(crate) const I16_LEN: usize = 2;
/// Encoded width of an `i32` field.
pub(crate) const I32_LEN: usize = 4;
/// Encoded width of an `i64` field.
pub(crate) const I64_LEN: usize = 8;
/// Encoded width of a [`Uuid`](crate::primitives::uuid::Uuid) field.
pub(crate) const UUID_LEN: usize = 16;

/// Rejects a version outside the range that a message type supports.
///
/// `min` and `max` come from the `MIN_VERSION` and `MAX_VERSION` constants of
/// the message module that calls this function.
pub(crate) fn check_version(
    api_key: i16,
    version: i16,
    min: i16,
    max: i16,
) -> Result<(), ProtocolError> {
    if (min..=max).contains(&version) {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion { api_key, version })
    }
}

/// Writes a compact array of nested structs, and each struct after it.
pub(crate) fn encode_array<T: Encode, B: BufMut>(
    buf: &mut B,
    items: &[T],
    version: i16,
) -> Result<(), ProtocolError> {
    put_array_len(buf, items.len(), FLEXIBLE);
    for item in items {
        item.encode(buf, version)?;
    }
    Ok(())
}

/// Reads a compact array of nested structs.
pub(crate) fn decode_array<T, B>(buf: &mut B, version: i16) -> Result<Vec<T>, ProtocolError>
where
    T: for<'de> Decode<'de>,
    B: Buf,
{
    let count = get_array_len(buf, FLEXIBLE)?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(T::decode(buf, version)?);
    }
    Ok(items)
}

/// Byte count that [`encode_array`] writes.
pub(crate) fn array_len<T: Encode>(items: &[T], version: i16) -> usize {
    let body: usize = items.iter().map(|item| item.encoded_len(version)).sum();
    array_len_prefix_len(items.len(), FLEXIBLE) + body
}

/// Writes a compact array of compact strings.
pub(crate) fn encode_string_array<B: BufMut>(buf: &mut B, items: &[String]) {
    put_array_len(buf, items.len(), FLEXIBLE);
    for item in items {
        put_compact_string(buf, item);
    }
}

/// Reads a compact array of compact strings.
pub(crate) fn decode_string_array<B: Buf>(buf: &mut B) -> Result<Vec<String>, ProtocolError> {
    let count = get_array_len(buf, FLEXIBLE)?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(get_compact_string_owned(buf)?);
    }
    Ok(items)
}

/// Byte count that [`encode_string_array`] writes.
pub(crate) fn string_array_len(items: &[String]) -> usize {
    let body: usize = items.iter().map(|item| compact_string_len(item)).sum();
    array_len_prefix_len(items.len(), FLEXIBLE) + body
}

/// Writes the tagged-fields trailer. A krabka-private message declares no known
/// tag, so the trailer holds only the tags that this build did not recognize.
pub(crate) fn write_unknown_tagged_fields<B: BufMut>(buf: &mut B, unknown: &UnknownTaggedFields) {
    WriteTaggedFields::new().write(buf, unknown);
}

/// Reads the tagged-fields trailer and keeps every tag verbatim.
pub(crate) fn read_unknown_tagged_fields<B: Buf>(
    buf: &mut B,
) -> Result<UnknownTaggedFields, ProtocolError> {
    read_tagged_fields(buf, |_tag, _payload| Ok(false))
}

/// Byte count that [`write_unknown_tagged_fields`] writes.
pub(crate) fn unknown_tagged_fields_len(unknown: &UnknownTaggedFields) -> usize {
    tagged_fields_len(&[], unknown)
}
