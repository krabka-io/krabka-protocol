use bytes::{Buf, BufMut, Bytes};

use crate::{
    ProtocolError,
    primitives::{
        fixed::{get_i16, get_i32, put_i16, put_i32},
        varint::{compact_length, get_uvarint, put_uvarint, usize_uvarint_len, uvarint_len},
    },
};

// ---- STRING (non-flexible) ----
// Wire: INT16 length (>=0), then `length` bytes UTF-8. -1 = null.

/// # Errors
/// Returns [`ProtocolError::LengthOverflow`] when the length does not fit its
/// prefix.
pub fn put_string<B: BufMut>(buf: &mut B, s: &str) -> Result<(), ProtocolError> {
    let len = i16::try_from(s.len()).map_err(|_| ProtocolError::LengthOverflow {
        what: "STRING",
        len: s.len(),
    })?;
    put_i16(buf, len);
    buf.put_slice(s.as_bytes());
    Ok(())
}

/// # Errors
/// Returns [`ProtocolError::LengthOverflow`] when the length does not fit its
/// prefix.
pub fn put_nullable_string<B: BufMut>(buf: &mut B, s: Option<&str>) -> Result<(), ProtocolError> {
    match s {
        None => {
            put_i16(buf, -1);
            Ok(())
        }
        Some(s) => put_string(buf, s),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_string_owned<B: Buf>(buf: &mut B) -> Result<String, ProtocolError> {
    get_nullable_string_owned(buf)?
        .ok_or(ProtocolError::InvalidValue("non-nullable STRING was null"))
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_nullable_string_owned<B: Buf>(buf: &mut B) -> Result<Option<String>, ProtocolError> {
    // A negative length is null.
    let Ok(n) = usize::try_from(get_i16(buf)?) else {
        return Ok(None);
    };
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    let s = String::from_utf8(v).map_err(|e| ProtocolError::InvalidUtf8(e.utf8_error()))?;
    Ok(Some(s))
}

#[must_use]
pub fn string_len(s: &str) -> usize {
    2 + s.len()
}
#[must_use]
pub fn nullable_string_len(s: Option<&str>) -> usize {
    2 + s.map_or(0, str::len)
}

// ---- COMPACT_STRING (flexible) ----
// Wire: UVARINT length+1 (0 = null), then `length` UTF-8 bytes.

/// # Errors
/// Returns [`ProtocolError::LengthOverflow`] when the length does not fit its
/// prefix.
pub fn put_compact_string<B: BufMut>(buf: &mut B, s: &str) -> Result<(), ProtocolError> {
    put_uvarint(buf, compact_length(s.len(), "COMPACT_STRING")?);
    buf.put_slice(s.as_bytes());
    Ok(())
}

/// # Errors
/// Returns [`ProtocolError::LengthOverflow`] when the length does not fit its
/// prefix.
pub fn put_compact_nullable_string<B: BufMut>(
    buf: &mut B,
    s: Option<&str>,
) -> Result<(), ProtocolError> {
    match s {
        None => {
            put_uvarint(buf, 0);
            Ok(())
        }
        Some(s) => put_compact_string(buf, s),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_compact_string_owned<B: Buf>(buf: &mut B) -> Result<String, ProtocolError> {
    get_compact_nullable_string_owned(buf)?.ok_or(ProtocolError::InvalidValue(
        "non-nullable COMPACT_STRING was null",
    ))
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_compact_nullable_string_owned<B: Buf>(
    buf: &mut B,
) -> Result<Option<String>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Ok(None);
    }
    let n = (raw - 1) as usize;
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    let s = String::from_utf8(v).map_err(|e| ProtocolError::InvalidUtf8(e.utf8_error()))?;
    Ok(Some(s))
}

#[must_use]
pub fn compact_string_len(s: &str) -> usize {
    usize_uvarint_len(s.len().saturating_add(1)).saturating_add(s.len())
}
#[must_use]
pub fn compact_nullable_string_len(s: Option<&str>) -> usize {
    s.map_or_else(|| uvarint_len(0), compact_string_len)
}

// ---- BYTES / COMPACT_BYTES ----
// BYTES: INT32 length, `length` bytes. -1 = null.
// COMPACT_BYTES: UVARINT length+1 (0=null), `length` bytes.

/// # Errors
/// Returns [`ProtocolError::LengthOverflow`] when the length does not fit its
/// prefix.
pub fn put_bytes<B: BufMut>(buf: &mut B, b: &[u8]) -> Result<(), ProtocolError> {
    let len = i32::try_from(b.len()).map_err(|_| ProtocolError::LengthOverflow {
        what: "BYTES",
        len: b.len(),
    })?;
    put_i32(buf, len);
    buf.put_slice(b);
    Ok(())
}

/// # Errors
/// Returns [`ProtocolError::LengthOverflow`] when the length does not fit its
/// prefix.
pub fn put_nullable_bytes<B: BufMut>(buf: &mut B, b: Option<&[u8]>) -> Result<(), ProtocolError> {
    match b {
        None => {
            put_i32(buf, -1);
            Ok(())
        }
        Some(b) => put_bytes(buf, b),
    }
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_bytes_owned<B: Buf>(buf: &mut B) -> Result<Bytes, ProtocolError> {
    get_nullable_bytes_owned(buf)?.ok_or(ProtocolError::InvalidValue("non-nullable BYTES was null"))
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_nullable_bytes_owned<B: Buf>(buf: &mut B) -> Result<Option<Bytes>, ProtocolError> {
    // A negative length is null.
    let Ok(n) = usize::try_from(get_i32(buf)?) else {
        return Ok(None);
    };
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    Ok(Some(Bytes::from(v)))
}

#[must_use]
pub fn bytes_len(b: &[u8]) -> usize {
    4 + b.len()
}
#[must_use]
pub fn nullable_bytes_len(b: Option<&[u8]>) -> usize {
    4 + b.map_or(0, <[u8]>::len)
}

/// # Errors
/// Returns [`ProtocolError::LengthOverflow`] when the length does not fit its
/// prefix.
pub fn put_compact_bytes<B: BufMut>(buf: &mut B, b: &[u8]) -> Result<(), ProtocolError> {
    put_uvarint(buf, compact_length(b.len(), "COMPACT_BYTES")?);
    buf.put_slice(b);
    Ok(())
}

/// # Errors
/// Returns [`ProtocolError::LengthOverflow`] when the length does not fit its
/// prefix.
pub fn put_compact_nullable_bytes<B: BufMut>(
    buf: &mut B,
    b: Option<&[u8]>,
) -> Result<(), ProtocolError> {
    match b {
        None => {
            put_uvarint(buf, 0);
            Ok(())
        }
        Some(b) => put_compact_bytes(buf, b),
    }
}

#[must_use]
pub fn compact_bytes_len(b: &[u8]) -> usize {
    usize_uvarint_len(b.len().saturating_add(1)).saturating_add(b.len())
}

/// Like `compact_bytes_len`, but takes the byte count directly rather than a slice.
///
/// Use this when you know the content size and do not need to materialise the
/// buffer.
#[must_use]
pub fn compact_bytes_len_from_size(n: usize) -> usize {
    usize_uvarint_len(n.saturating_add(1)).saturating_add(n)
}
#[must_use]
pub fn compact_nullable_bytes_len(b: Option<&[u8]>) -> usize {
    b.map_or_else(|| uvarint_len(0), compact_bytes_len)
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_compact_bytes_owned<B: Buf>(buf: &mut B) -> Result<Bytes, ProtocolError> {
    get_compact_nullable_bytes_owned(buf)?.ok_or(ProtocolError::InvalidValue(
        "non-nullable COMPACT_BYTES was null",
    ))
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
pub fn get_compact_nullable_bytes_owned<B: Buf>(
    buf: &mut B,
) -> Result<Option<Bytes>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Ok(None);
    }
    let n = (raw - 1) as usize;
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    Ok(Some(Bytes::from(v)))
}

#[cfg(test)]
mod tests {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn string_roundtrip() {
        let mut buf = BytesMut::new();
        put_string(&mut buf, "kafka").unwrap();
        // INT16(5) + bytes
        assert2::assert!(&buf[..] == &[0x00, 0x05, b'k', b'a', b'f', b'k', b'a']);
        let mut cur = &buf[..];
        assert2::assert!(get_string_owned(&mut cur).unwrap() == "kafka");
    }

    #[test]
    fn nullable_string_null() {
        let mut buf = BytesMut::new();
        put_nullable_string(&mut buf, None).unwrap();
        assert2::assert!(&buf[..] == &[0xFF, 0xFF]);
        let mut cur = &buf[..];
        assert2::assert!(get_nullable_string_owned(&mut cur).unwrap() == None);
    }

    #[test]
    fn compact_string_roundtrip() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "kafka").unwrap();
        // UVARINT(6) + bytes
        assert2::assert!(&buf[..] == &[0x06, b'k', b'a', b'f', b'k', b'a']);
        let mut cur = &buf[..];
        assert2::assert!(get_compact_string_owned(&mut cur).unwrap() == "kafka");
    }

    #[test]
    fn compact_nullable_string_null() {
        let mut buf = BytesMut::new();
        put_compact_nullable_string(&mut buf, None).unwrap();
        assert2::assert!(&buf[..] == &[0x00]);
        let mut cur = &buf[..];
        assert2::assert!(get_compact_nullable_string_owned(&mut cur).unwrap() == None);
    }

    #[test]
    fn empty_compact_string() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "").unwrap();
        assert2::assert!(&buf[..] == &[0x01]); // length = 1 means "0 bytes"
        let mut cur = &buf[..];
        assert2::assert!(get_compact_string_owned(&mut cur).unwrap() == "");
    }

    #[test]
    fn bytes_roundtrip() {
        let mut buf = BytesMut::new();
        put_bytes(&mut buf, &[1, 2, 3]).unwrap();
        let mut cur = &buf[..];
        let out = get_bytes_owned(&mut cur).unwrap();
        assert2::assert!(out.as_ref() == &[1, 2, 3]);
    }

    #[test]
    fn invalid_utf8_is_rejected() {
        // INT16(2) + invalid UTF-8 byte sequence
        let bytes = [0x00, 0x02, 0xC3, 0x28];
        let mut cur = &bytes[..];
        assert2::assert!(matches!(
            get_string_owned(&mut cur),
            Err(ProtocolError::InvalidUtf8(_))
        ));
    }
}
