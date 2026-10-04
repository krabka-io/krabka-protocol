//! Zstd through the `zstd` crate, which wraps libzstd.

use std::{
    cell::RefCell,
    io::{self, BufRead, Read},
};

use bytes::Bytes;

use crate::CompressionError;

/// Match Kafka's default zstd level.
const DEFAULT_LEVEL: i32 = 3;

// Reuse only the default-level context for small batches. Large inputs and
// expensive levels must not leave their workspaces on every worker thread.
const REUSE_MAX_INPUT: usize = 128 * 1024;

// Direct decode bounds its output allocation even when the frame header lies.
// Larger frames and streams retain the incremental output cap check.
const MAX_DIRECT_OUTPUT: usize = 128 * 1024;

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
    // The streaming path owns errors: malformed frames can differ in when
    // direct and incremental decoders report a size mismatch or output cap.
    if let Ok(Some(size)) = zstd::zstd_safe::get_frame_content_size(data)
        && let Ok(size) = usize::try_from(size)
        && size <= max_output.min(MAX_DIRECT_OUTPUT)
        // Single-segment frames use the content size as their window size.
        // Other frames retain the streaming decoder's window limit.
        && data.get(4).is_some_and(|descriptor| descriptor & 0x20 != 0)
        && zstd::zstd_safe::find_frame_compressed_size(data) == Ok(data.len())
        && let Ok(out) = zstd::bulk::decompress(data, size)
    {
        return Ok(Bytes::from(out));
    }
    let decoder = zstd::stream::Decoder::with_buffer(ChunkedSlice {
        data,
        chunk_remaining: 0,
    })
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

// Expose the same input boundaries as Decoder::new's staging buffer without
// allocating or copying the slice. Boundaries affect checksum/cap error order.
struct ChunkedSlice<'a> {
    data: &'a [u8],
    chunk_remaining: usize,
}

impl BufRead for ChunkedSlice<'_> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.chunk_remaining == 0 {
            self.chunk_remaining = self.data.len().min(zstd::zstd_safe::DCtx::in_size());
        }
        Ok(&self.data[..self.chunk_remaining])
    }

    fn consume(&mut self, amount: usize) {
        let amount = amount.min(self.chunk_remaining);
        self.data = &self.data[amount..];
        self.chunk_remaining -= amount;
    }
}

impl Read for ChunkedSlice<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let input = self.fill_buf()?;
        let len = input.len().min(output.len());
        output[..len].copy_from_slice(&input[..len]);
        self.consume(len);
        Ok(len)
    }
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

    #[test]
    fn concatenated_and_skippable_frames_preserve_output_caps() {
        use std::io::Write as _;

        let first = vec![0xAB; 64 * 1024];
        let second = vec![0xCD; 128 * 1024 + 1];
        let expected = [first.as_slice(), second.as_slice()].concat();
        for known_size in [false, true] {
            let first_frame = if known_size {
                compress(&first).unwrap().to_vec()
            } else {
                let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
                encoder.include_contentsize(false).unwrap();
                encoder.write_all(&first).unwrap();
                encoder.finish().unwrap()
            };
            for skip_first in [false, true] {
                let mut wire = Vec::new();
                if skip_first {
                    // A valid skippable frame containing four opaque bytes.
                    wire.extend_from_slice(&0x184D_2A50_u32.to_le_bytes());
                    wire.extend_from_slice(&4_u32.to_le_bytes());
                    wire.extend_from_slice(b"skip");
                }
                wire.extend_from_slice(&first_frame);
                wire.extend_from_slice(&compress(&second).unwrap());
                assert2::assert!(decompress(&wire, expected.len()).unwrap().as_ref() == expected);
                assert2::assert!(matches!(
                    decompress(&wire, expected.len() - 1),
                    Err(CompressionError::TooLarge { limit }) if limit == expected.len() - 1
                ));
            }
        }
    }

    #[test]
    fn corrupt_declared_size_is_still_checked_by_decoder() {
        // A single-segment frame advertising an impossible 64-bit size.
        let mut wire = vec![0x28, 0xB5, 0x2F, 0xFD, 0xE0];
        wire.extend_from_slice(&u64::MAX.to_le_bytes());
        wire.extend_from_slice(&[1, 0, 0]);
        assert2::assert!(matches!(
            decompress(&wire, BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn single_frames_check_checksum_truncation_and_output_limit() {
        use std::io::Write as _;

        for size in [0, 1, 1024, 65_536, MAX_DIRECT_OUTPUT, MAX_DIRECT_OUTPUT + 1] {
            let input = vec![0xAB; size];
            let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
            encoder
                .set_pledged_src_size(Some(u64::try_from(size).unwrap()))
                .unwrap();
            encoder.include_checksum(true).unwrap();
            encoder.write_all(&input).unwrap();
            let wire = encoder.finish().unwrap();
            assert2::assert!(decompress(&wire, size).unwrap().as_ref() == input);
            if size != 0 {
                assert2::assert!(matches!(
                    decompress(&wire, size - 1),
                    Err(CompressionError::TooLarge { limit }) if limit == size - 1
                ));
            }
            assert2::assert!(matches!(
                decompress(&wire[..wire.len() - 1], BIG_CAP),
                Err(CompressionError::InvalidData(_))
            ));
            let mut corrupt = wire;
            *corrupt.last_mut().unwrap() ^= 1;
            assert2::assert!(matches!(
                decompress(&corrupt, BIG_CAP),
                Err(CompressionError::InvalidData(_))
            ));
        }
    }

    #[test]
    fn multi_segment_frames_keep_streaming_window_limit() {
        let input = vec![0xAB; 1024];
        let original = compress(&input).unwrap();
        for window in [0x80, 0x88, 0x90] {
            let mut wire = original.to_vec();
            // Keep the content size and blocks, replacing the implicit
            // single-segment window with a separate window descriptor.
            wire[4] &= !0x20;
            wire.insert(5, window);
            if window <= 0x88 {
                assert2::assert!(decompress(&wire, BIG_CAP).unwrap().as_ref() == input);
            } else {
                assert2::assert!(matches!(
                    decompress(&wire, BIG_CAP),
                    Err(CompressionError::InvalidData(_))
                ));
            }
        }
    }

    #[test]
    fn malformed_frames_match_streaming_results() {
        use std::io::{Read as _, Write as _};

        fn streamed(data: &[u8], cap: usize) -> Result<Vec<u8>, CompressionError> {
            let decoder = zstd::stream::Decoder::new(data)
                .map_err(|e| CompressionError::InvalidData(e.to_string()))?;
            let mut out = Vec::with_capacity(data.len().saturating_mul(2).min(cap));
            decoder
                .take(u64::try_from(cap).unwrap() + 1)
                .read_to_end(&mut out)
                .map_err(|e| CompressionError::InvalidData(e.to_string()))?;
            if out.len() > cap {
                Err(CompressionError::TooLarge { limit: cap })
            } else {
                Ok(out)
            }
        }

        for size in [0, 1, 1024] {
            let input = (0..size)
                .map(|i| u8::try_from(i % 251).unwrap())
                .collect::<Vec<_>>();
            let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
            encoder
                .set_pledged_src_size(Some(u64::try_from(size).unwrap()))
                .unwrap();
            encoder.include_checksum(true).unwrap();
            encoder.write_all(&input).unwrap();
            let wire = encoder.finish().unwrap();
            for position in 0..wire.len() {
                for xor in [1, 128, 255] {
                    let mut mutant = wire.clone();
                    mutant[position] ^= xor;
                    for cap in [0, 1, 1023, 65_536] {
                        match (streamed(&mutant, cap), decompress(&mutant, cap)) {
                            (Ok(expected), Ok(actual)) => {
                                assert2::assert!(actual.as_ref() == expected);
                            }
                            (
                                Err(CompressionError::TooLarge { limit: expected }),
                                Err(CompressionError::TooLarge { limit: actual }),
                            ) => assert2::assert!(actual == expected),
                            (
                                Err(CompressionError::InvalidData(_)),
                                Err(CompressionError::InvalidData(_)),
                            ) => {}
                            (expected, actual) => panic!(
                                "size={size}, position={position}, xor={xor}, cap={cap}: {expected:?} != {actual:?}"
                            ),
                        }
                    }
                }
            }
        }
    }
}
