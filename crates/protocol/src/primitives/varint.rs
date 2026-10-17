use bytes::{Buf, BufMut};

use crate::ProtocolError;

const MAX_VARINT_BYTES: usize = 5; // 32-bit
const MAX_VARLONG_BYTES: usize = 10; // 64-bit

pub fn put_uvarint<B: BufMut>(buf: &mut B, mut v: u32) {
    if v < 0x80 {
        buf.put_u8(v.to_le_bytes()[0]);
        return;
    }
    let mut encoded = [0_u8; MAX_VARINT_BYTES];
    let mut len = 0;
    while (v & !0x7F) != 0 {
        encoded[len] = (v & 0x7F).to_le_bytes()[0] | 0x80;
        v >>= 7;
        len += 1;
    }
    encoded[len] = v.to_le_bytes()[0];
    buf.put_slice(&encoded[..=len]);
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
#[inline]
pub fn get_uvarint<B: Buf>(buf: &mut B) -> Result<u32, ProtocolError> {
    let mut result: u32 = 0;
    let mut shift = 0;
    for _ in 0..MAX_VARINT_BYTES {
        if buf.remaining() == 0 {
            return Err(ProtocolError::UnexpectedEof { needed: 1 });
        }
        let b = buf.get_u8();
        result |= u32::from(b & 0x7F) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(ProtocolError::VarintTooLong {
        max: MAX_VARINT_BYTES,
    })
}

/// [`get_varint`] on a byte slice, with no `Buf` in between.
///
/// The record parser reads every varint of every record of every batch the
/// produce path validates, and through the generic decoder each byte was a
/// `Buf` call with its own bounds check. The one- and two-byte forms, which
/// cover every length and delta below 8 Ki, are read inline; longer ones go
/// through [`get_long_varint`]. It agrees with [`get_varint`] on every input,
/// errors included.
///
/// # Errors
/// [`ProtocolError::UnexpectedEof`] when the slice ends inside the varint, and
/// [`ProtocolError::VarintTooLong`] when it runs past five bytes.
#[inline]
pub(crate) fn get_varint_from_slice(buf: &mut &[u8]) -> Result<i32, ProtocolError> {
    let bytes: &[u8] = buf;
    let zigzag = match *bytes {
        [first, ref rest @ ..] if first < 0x80 => {
            *buf = rest;
            u32::from(first)
        }
        [first, second, ref rest @ ..] if second < 0x80 => {
            *buf = rest;
            u32::from(first & 0x7F) | (u32::from(second) << 7)
        }
        _ => {
            let (value, rest) = get_long_varint(bytes)?;
            *buf = rest;
            return Ok(value);
        }
    };
    Ok((zigzag >> 1).cast_signed() ^ -(zigzag & 1).cast_signed())
}

/// [`get_varint_from_slice`] for a varint of three bytes or more, or one the
/// slice cuts short.
#[cold]
///
/// It takes the slice by value and returns what is left, so a caller's cursor
/// never has its address taken and stays in registers on the fast path.
fn get_long_varint(mut bytes: &[u8]) -> Result<(i32, &[u8]), ProtocolError> {
    let value = get_varint(&mut bytes)?;
    Ok((value, bytes))
}

/// [`get_varlong`] on a byte slice; see [`get_varint_from_slice`].
///
/// # Errors
/// [`ProtocolError::UnexpectedEof`] when the slice ends inside the varlong,
/// and [`ProtocolError::VarintTooLong`] when it runs past ten bytes.
#[inline]
pub(crate) fn get_varlong_from_slice(buf: &mut &[u8]) -> Result<i64, ProtocolError> {
    let bytes: &[u8] = buf;
    let zigzag = match *bytes {
        [first, ref rest @ ..] if first < 0x80 => {
            *buf = rest;
            u64::from(first)
        }
        [first, second, ref rest @ ..] if second < 0x80 => {
            *buf = rest;
            u64::from(first & 0x7F) | (u64::from(second) << 7)
        }
        _ => {
            let (value, rest) = get_long_varlong(bytes)?;
            *buf = rest;
            return Ok(value);
        }
    };
    Ok((zigzag >> 1).cast_signed() ^ -(zigzag & 1).cast_signed())
}

/// [`get_varlong_from_slice`] for a varlong of three bytes or more, or one
/// the slice cuts short.
#[cold]
///
/// It takes the slice by value and returns what is left, so a caller's cursor
/// never has its address taken and stays in registers on the fast path.
fn get_long_varlong(mut bytes: &[u8]) -> Result<(i64, &[u8]), ProtocolError> {
    let value = get_varlong(&mut bytes)?;
    Ok((value, bytes))
}

#[must_use]
pub fn uvarint_len(v: u32) -> usize {
    if v < 0x80 {
        return 1;
    }
    let bits = usize::from((u32::BITS - v.leading_zeros()).to_le_bytes()[0]);
    bits.div_ceil(7)
}

pub fn put_varint<B: BufMut>(buf: &mut B, v: i32) {
    let zz = ((v << 1) ^ (v >> 31)).cast_unsigned();
    put_uvarint(buf, zz);
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
#[inline]
pub fn get_varint<B: Buf>(buf: &mut B) -> Result<i32, ProtocolError> {
    let zz = get_uvarint(buf)?;
    Ok((zz >> 1).cast_signed() ^ -(zz & 1).cast_signed())
}

#[must_use]
pub fn varint_len(v: i32) -> usize {
    let zz = ((v << 1) ^ (v >> 31)).cast_unsigned();
    uvarint_len(zz)
}

pub fn put_uvarlong<B: BufMut>(buf: &mut B, mut v: u64) {
    if v < 0x80 {
        buf.put_u8(v.to_le_bytes()[0]);
        return;
    }
    let mut encoded = [0_u8; MAX_VARLONG_BYTES];
    let mut len = 0;
    while (v & !0x7F) != 0 {
        encoded[len] = (v & 0x7F).to_le_bytes()[0] | 0x80;
        v >>= 7;
        len += 1;
    }
    encoded[len] = v.to_le_bytes()[0];
    buf.put_slice(&encoded[..=len]);
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
#[inline]
pub fn get_uvarlong<B: Buf>(buf: &mut B) -> Result<u64, ProtocolError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for _ in 0..MAX_VARLONG_BYTES {
        if buf.remaining() == 0 {
            return Err(ProtocolError::UnexpectedEof { needed: 1 });
        }
        let b = buf.get_u8();
        result |= u64::from(b & 0x7F) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(ProtocolError::VarintTooLong {
        max: MAX_VARLONG_BYTES,
    })
}

pub fn put_varlong<B: BufMut>(buf: &mut B, v: i64) {
    let zz = ((v << 1) ^ (v >> 63)).cast_unsigned();
    put_uvarlong(buf, zz);
}

/// # Errors
/// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
#[inline]
pub fn get_varlong<B: Buf>(buf: &mut B) -> Result<i64, ProtocolError> {
    let zz = get_uvarlong(buf)?;
    Ok((zz >> 1).cast_signed() ^ -(zz & 1).cast_signed())
}

#[must_use]
pub fn uvarlong_len(v: u64) -> usize {
    if v < 0x80 {
        return 1;
    }
    let bits = usize::from((u64::BITS - v.leading_zeros()).to_le_bytes()[0]);
    bits.div_ceil(7)
}

#[must_use]
pub fn varlong_len(v: i64) -> usize {
    let zz = ((v << 1) ^ (v >> 63)).cast_unsigned();
    uvarlong_len(zz)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn uvarint_known_vectors() {
        // (value, expected bytes) — pulled from KIP-482 / protobuf reference.
        let cases: &[(u32, &[u8])] = &[
            (0, &[0x00]),
            (1, &[0x01]),
            (127, &[0x7F]),
            (128, &[0x80, 0x01]),
            (16_383, &[0xFF, 0x7F]),
            (16_384, &[0x80, 0x80, 0x01]),
            (u32::MAX, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
        ];
        for (v, expected) in cases {
            let mut buf = BytesMut::new();
            put_uvarint(&mut buf, *v);
            assert2::assert!(&buf[..] == *expected);
            for split in 0..=expected.len() {
                let mut left = vec![0; split];
                let mut right = vec![0; expected.len() - split];
                put_uvarint(&mut BufMut::chain_mut(&mut left[..], &mut right[..]), *v);
                left.extend_from_slice(&right);
                assert2::assert!(left.as_slice() == *expected);
            }
            let mut cur = *expected;
            check!(get_uvarint(&mut cur).unwrap() == *v);
            check!(cur.is_empty());
            check!(uvarint_len(*v) == expected.len());
        }
    }

    #[test]
    fn uvarlong_known_vectors_across_buffer_boundaries() {
        let cases: &[(u64, &[u8])] = &[
            (0, &[0]),
            (127, &[0x7F]),
            (128, &[0x80, 1]),
            (16_383, &[0xFF, 0x7F]),
            (16_384, &[0x80, 0x80, 1]),
            (
                u64::MAX,
                &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 1],
            ),
        ];
        for (value, expected) in cases {
            for split in 0..=expected.len() {
                let mut left = vec![0; split];
                let mut right = vec![0; expected.len() - split];
                put_uvarlong(
                    &mut BufMut::chain_mut(&mut left[..], &mut right[..]),
                    *value,
                );
                left.extend_from_slice(&right);
                assert2::assert!(left.as_slice() == *expected);
            }
            let mut cursor = *expected;
            assert2::assert!(
                (get_uvarlong(&mut cursor).unwrap(), cursor.is_empty()) == (*value, true)
            );
            assert2::assert!(uvarlong_len(*value) == expected.len());
        }
    }

    #[test]
    fn varint_zigzag_sample() {
        // (value, expected bytes) — protobuf zig-zag examples.
        let cases: &[(i32, &[u8])] = &[
            (0, &[0x00]),
            (-1, &[0x01]),
            (1, &[0x02]),
            (-2, &[0x03]),
            (2, &[0x04]),
            (i32::MAX, &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]),
            (i32::MIN, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
        ];
        for (v, expected) in cases {
            let mut buf = BytesMut::new();
            put_varint(&mut buf, *v);
            assert2::assert!(&buf[..] == *expected);
            let mut cur = *expected;
            assert2::assert!(get_varint(&mut cur).unwrap() == *v);
            assert2::assert!(varint_len(*v) == expected.len());
        }
    }

    #[test]
    fn uvarint_rejects_overlong() {
        let too_long = [0x80u8, 0x80, 0x80, 0x80, 0x80, 0x01];
        let mut cur = &too_long[..];
        assert2::assert!(matches!(
            get_uvarint(&mut cur),
            Err(ProtocolError::VarintTooLong { .. })
        ));
    }

    #[test]
    fn uvarint_eof() {
        let truncated = [0x80u8];
        let mut cur = &truncated[..];
        assert2::assert!(matches!(
            get_uvarint(&mut cur),
            Err(ProtocolError::UnexpectedEof { .. })
        ));
    }

    #[test]
    fn uvarlong_len_known_values() {
        // (value, expected byte length on wire)
        let cases: &[(u64, usize)] = &[
            (0, 1),
            (1, 1),
            (127, 1),
            (128, 2),
            (u64::from(u32::MAX), 5),
            (u64::MAX, 10),
        ];
        for (v, expected) in cases {
            assert2::assert!(uvarlong_len(*v) == *expected);
        }
    }

    #[test]
    fn varlong_len_known_values() {
        // Zigzag: 0->0, -1->1, 1->2, i64::MIN has max bits
        let cases: &[(i64, usize)] = &[
            (0, 1),
            (-1, 1),
            (1, 1),
            (63, 1),
            (64, 2),
            (-64, 1),
            (-65, 2),
            (i64::MAX, 10),
            (i64::MIN, 10),
        ];
        for (v, expected) in cases {
            assert2::assert!(varlong_len(*v) == *expected);
        }
    }

    proptest::proptest! {
        /// The slice decoders and the `Buf` decoders read the same value, or
        /// fail with the same error, from any bytes, and leave the same
        /// bytes unread when they succeed.
        #[test]
        fn slice_decoders_agree_with_the_buf_decoders(
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..12),
        ) {
            let mut generic: &[u8] = &bytes;
            let mut slice: &[u8] = &bytes;
            let want = get_varint(&mut generic);
            let got = get_varint_from_slice(&mut slice);
            proptest::prop_assert_eq!(format!("{got:?}"), format!("{want:?}"));
            if want.is_ok() {
                proptest::prop_assert_eq!(slice, generic);
            }

            let mut generic: &[u8] = &bytes;
            let mut slice: &[u8] = &bytes;
            let want = get_varlong(&mut generic);
            let got = get_varlong_from_slice(&mut slice);
            proptest::prop_assert_eq!(format!("{got:?}"), format!("{want:?}"));
            if want.is_ok() {
                proptest::prop_assert_eq!(slice, generic);
            }
        }
    }
}
