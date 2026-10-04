//! Gzip (RFC-1952), through the configured `flate2` backend.

use std::{
    cell::RefCell,
    io::{Read, Write},
};

use bytes::Bytes;
use flate2::{Compression as GzipLevel, Crc, read::GzDecoder, write::DeflateEncoder};

use crate::CompressionError;

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    compress_at(data, GzipLevel::default())
}

/// Compress at `level`, which the caller has checked. -1 is the zlib default
/// level, as Java's `Deflater.DEFAULT_COMPRESSION`.
pub fn compress_with_level(data: &[u8], level: i32) -> Result<Bytes, CompressionError> {
    let level = u32::try_from(level).map_or_else(|_| GzipLevel::default(), GzipLevel::new);
    compress_at(data, level)
}

fn compress_at(data: &[u8], level: GzipLevel) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        let mut out = Vec::with_capacity(20);
        out.extend_from_slice(&frame_header(level));
        // An empty final fixed-Huffman block, then zero CRC32 and input size.
        out.extend_from_slice(&[3, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        return Ok(Bytes::from(out));
    }
    // One encoder per thread, replaced when its level changes. Its window and
    // staging buffer have fixed sizes; completed output is taken each time.
    thread_local! {
        static ENCODER: RefCell<Option<(GzipLevel, DeflateEncoder<Vec<u8>>)>> =
            const { RefCell::new(None) };
    }
    ENCODER
        .try_with(|cached| {
            let mut cached = cached.borrow_mut();
            let result = compress_reused(data, level, &mut cached);
            if result.is_err() {
                // Do not reuse a stream that failed before its end mark.
                cached.take();
            }
            result
        })
        // A different thread-local destructor may encode after this cache drops.
        .unwrap_or_else(|_| compress_reused(data, level, &mut None))
}

fn compress_reused(
    data: &[u8],
    level: GzipLevel,
    cached: &mut Option<(GzipLevel, DeflateEncoder<Vec<u8>>)>,
) -> Result<Bytes, CompressionError> {
    // Allow for framing and stored-block overhead. Vec can grow if needed.
    let capacity = data
        .len()
        .saturating_add(data.len().div_ceil(16 * 1024).saturating_mul(5))
        .saturating_add(20);
    let out = Vec::with_capacity(capacity);
    if let Some((cached_level, encoder)) = cached.as_mut()
        && *cached_level == level
    {
        encoder.reset(out)?;
    } else {
        // Release the previous workspace before allocating a different level.
        cached.take();
        *cached = Some((level, DeflateEncoder::new(out, level)));
    }
    let encoder = &mut cached.as_mut().expect("the encoder was initialized").1;
    encoder.get_mut().extend_from_slice(&frame_header(level));
    encoder.write_all(data)?;
    encoder.try_finish()?;
    // Leave the cached encoder finished with an empty writer. Reset it only
    // when the next frame starts, so drop does not write an empty stream.
    let mut out = std::mem::take(encoder.get_mut());
    let mut crc = Crc::new();
    crc.update(data);
    out.extend_from_slice(&crc.sum().to_le_bytes());
    out.extend_from_slice(&crc.amount().to_le_bytes());
    Ok(Bytes::from(out))
}

fn frame_header(level: GzipLevel) -> [u8; 10] {
    // GzBuilder defaults: no optional fields, zero mtime, unknown OS. XFL
    // distinguishes the best and fastest levels.
    let xfl = if level.level() >= 9 {
        2
    } else if level.level() <= 1 {
        4
    } else {
        0
    };
    [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, xfl, 255]
}

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty gzip payload".into()));
    }
    let initial_capacity = data.len().saturating_mul(2).min(max_output);
    // ISIZE is untrusted and wraps at 4 GiB. Use it only as a bounded hint;
    // the decoder still verifies the complete stream, CRC and actual size.
    let hint = data
        .last_chunk::<4>()
        .filter(|_| {
            data.len() >= 18
                && data.starts_with(&[0x1f, 0x8b, 8])
                && u32::try_from(max_output).is_ok()
        })
        .and_then(|size| usize::try_from(u32::from_le_bytes(*size)).ok())
        .filter(|&size| size <= max_output);
    if let Some(size) = hint {
        // Shrinking a reservation cannot exceed the old allocation. Grow a
        // small reservation only up to 128 KiB; larger output can grow as usual.
        let mut capacity = if size <= initial_capacity {
            size
        } else {
            initial_capacity.max(size.min(128 * 1024))
        };
        // Mid-sized exact allocations can repeatedly trim glibc's heap along
        // with the decoder workspace. A bounded 128 KiB reservation avoids it.
        if size > 32 * 1024 {
            capacity = capacity.max((128 * 1024).min(max_output));
        }
        if capacity == initial_capacity {
            return decode_capped(data, max_output, initial_capacity, None).map(Bytes::from);
        }
        if let Ok(mut out) = decode_capped(data, max_output, capacity, Some(size)) {
            if out.len() < size {
                // A trailing member can supply the hint. The first member is
                // already validated; discard spare capacity without decoding it again.
                let mut trimmed = Vec::new();
                if trimmed.try_reserve_exact(out.len()).is_ok() {
                    trimmed.extend_from_slice(&out);
                    out = trimmed;
                }
            }
            return Ok(Bytes::from(out));
        }
        // Preserve the original reader's error ordering after dropping the
        // failed attempt, including corruption competing with an output cap.
        return decode_capped(data, max_output, initial_capacity, None).map(Bytes::from);
    }
    decode_capped(data, max_output, initial_capacity, None).map(Bytes::from)
}

fn decode_capped(
    data: &[u8],
    max_output: usize,
    capacity: usize,
    expected_size: Option<usize>,
) -> Result<Vec<u8>, CompressionError> {
    // Allocate output before temporary decoder storage. The opposite order
    // can make glibc trim and regrow the heap for each 64–128 KiB result.
    let mut out = if expected_size.is_some() {
        let mut out = Vec::new();
        out.try_reserve_exact(capacity)
            .map_err(|_| CompressionError::Io(std::io::ErrorKind::OutOfMemory.into()))?;
        out
    } else {
        Vec::with_capacity(capacity)
    };
    // Read at most `max_output + 1` bytes: the extra byte lets us detect that
    // the real output exceeds the cap without ever materializing it.
    let mut limited = GzDecoder::new(data).take((max_output as u64).saturating_add(1));
    if let Some(expected) = expected_size {
        while out.len() < expected {
            if out.len() == out.capacity() {
                // Verify EOF before growing. Retain one probed byte if the
                // stream continues, then grow geometrically up to ISIZE.
                let mut probe = [0];
                let read = loop {
                    match limited.read(&mut probe) {
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        result => break result,
                    }
                }
                .map_err(|error| CompressionError::InvalidData(format!("gzip decode: {error}")))?;
                if read == 0 {
                    break;
                }
                let next = out.capacity().saturating_mul(2).max(1).min(expected);
                out.try_reserve_exact(next - out.len())
                    .map_err(|_| CompressionError::Io(std::io::ErrorKind::OutOfMemory.into()))?;
                out.push(probe[0]);
            }
            let chunk = (expected - out.len()).min(out.capacity() - out.len());
            let before = out.len();
            limited
                .by_ref()
                .take(chunk as u64)
                .read_to_end(&mut out)
                .map_err(|error| CompressionError::InvalidData(format!("gzip decode: {error}")))?;
            if out.len() - before < chunk {
                break;
            }
        }
    }
    limited
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::InvalidData(format!("gzip decode: {e}")))?;
    if out.len() > max_output {
        return Err(CompressionError::TooLarge { limit: max_output });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {

    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";
    const BIG_CAP: usize = 256 * 1024 * 1024;

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        assert2::assert!(z.len() < HELLO.len() + 32);
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == HELLO);
    }

    #[test]
    fn decompress_empty_rejected() {
        assert2::assert!(matches!(
            decompress(b"", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_garbage_rejected() {
        assert2::assert!(matches!(
            decompress(b"this is not gzip", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn compress_empty_produces_valid_frame() {
        let z = compress(b"").unwrap();
        assert2::assert!(!z.is_empty());
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == b"");
    }

    #[test]
    fn decompression_bomb_rejected() {
        // 64 MiB of zeros compresses tiny but expands hugely.
        let bomb = vec![0u8; 64 * 1024 * 1024];
        let z = compress(&bomb).unwrap();
        assert2::assert!(matches!(
            decompress(&z, 1024),
            Err(CompressionError::TooLarge { limit: 1024 })
        ));
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == bomb.as_slice());
    }

    #[test]
    fn decompress_at_exact_cap_succeeds() {
        let z = compress(HELLO).unwrap();
        // Output of exactly `max_output` bytes is allowed (cap check is
        // `len > max_output`, not `>=`).
        let back = decompress(&z, HELLO.len()).unwrap();
        assert2::assert!(back.as_ref() == HELLO);
        // One byte under the exact size is rejected.
        assert2::assert!(matches!(
            decompress(&z, HELLO.len() - 1),
            Err(CompressionError::TooLarge { limit }) if limit == HELLO.len() - 1
        ));
    }
}
