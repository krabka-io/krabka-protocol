//! Zstd through the `zstd` crate, which wraps libzstd.

use std::cell::RefCell;

use bytes::Bytes;

use crate::CompressionError;

/// Match Kafka's default zstd level.
const DEFAULT_LEVEL: i32 = 3;

// Reuse only the default-level context for small batches. Large inputs and
// expensive levels must not leave their workspaces on every worker thread.
const REUSE_MAX_INPUT: usize = 128 * 1024;

thread_local! {
    static COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> = const { RefCell::new(None) };
}

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    compress_with_level(data, DEFAULT_LEVEL)
}

/// Compress at `level`, which the caller has checked.
pub fn compress_with_level(data: &[u8], level: i32) -> Result<Bytes, CompressionError> {
    let out = if level == DEFAULT_LEVEL && data.len() <= REUSE_MAX_INPUT {
        COMPRESSOR.with(|cell| {
            let mut cached = cell.borrow_mut();
            if cached.is_none() {
                *cached = Some(zstd::bulk::Compressor::new(DEFAULT_LEVEL)?);
            }
            cached
                .as_mut()
                .expect("compressor initialized")
                .compress(data)
        })?
    } else {
        zstd::bulk::compress(data, level)?
    };
    Ok(Bytes::from(out))
}

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty zstd payload".into()));
    }
    let decoder = zstd::stream::Decoder::new(data)
        .map_err(|e| CompressionError::InvalidData(format!("zstd open: {e}")))?;
    // Read at most `max_output + 1` bytes so we can detect overflow without
    // materializing the oversized output.
    let mut limited = std::io::Read::take(decoder, (max_output as u64).saturating_add(1));
    let mut out = Vec::with_capacity(data.len().saturating_mul(2).min(max_output));
    std::io::Read::read_to_end(&mut limited, &mut out)
        .map_err(|e| CompressionError::InvalidData(format!("zstd decode: {e}")))?;
    if out.len() > max_output {
        return Err(CompressionError::TooLarge { limit: max_output });
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {

    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";
    const BIG_CAP: usize = 256 * 1024 * 1024;

    #[test]
    fn cached_workspace_stays_bounded() {
        std::thread::spawn(|| {
            compress(&vec![0xAB; REUSE_MAX_INPUT]).unwrap();
            let retained =
                COMPRESSOR.with(|cell| cell.borrow_mut().as_mut().unwrap().context_mut().sizeof());
            assert2::assert!(retained <= 2 * 1024 * 1024);
            for (size, level) in [(1024 * 1024, 3), (REUSE_MAX_INPUT, 9), (1, 3)] {
                compress_with_level(&vec![0xCD; size], level).unwrap();
            }
            let after =
                COMPRESSOR.with(|cell| cell.borrow_mut().as_mut().unwrap().context_mut().sizeof());
            assert2::assert!(after <= retained);
            println!("retained default zstd workspace: {retained} bytes");
        })
        .join()
        .unwrap();
    }

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
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
            decompress(b"this is not zstd", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn larger_payload_roundtrips() {
        let big = vec![0xABu8; 128 * 1024];
        let z = compress(&big).unwrap();
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == big.as_slice());
    }

    #[test]
    fn decompression_bomb_rejected() {
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
