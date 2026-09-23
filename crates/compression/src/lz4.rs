//! LZ4 frame format (LZ4F), independent blocks.
//!
//! Kafka writes LZ4 in the frame format, with magic `0x04 22 4D 18`, and makes
//! these choices: 64 KiB block size, independent blocks, no block checksum, and
//! no content-size in the header. We match those defaults, so our bytes agree
//! with the output of `KafkaLZ4BlockOutputStream` for differential testing.
//!
//! `KafkaLZ4BlockOutputStream` (`Lz4BlockOutputStream.java`) picks the block
//! compressor by level: the fast compressor at the default level (9), and LZ4
//! HC (`LZ4Factory.highCompressor(level)`) at every other level in `1..=17`.
//! `compress_with_level` matches that split. `lz4rip` has only the fast
//! compressor, so the HC path builds the same independent-block frame by hand,
//! using `lzzzz`'s binding to the reference `liblz4` HC compressor for the
//! block bytes. The frame header and end mark are identical either way: they
//! depend only on [`frame_info`], not on which compressor filled the blocks.

use std::{
    io::{Read, Write},
    sync::LazyLock,
};

use bytes::Bytes;
use lz4rip::frame::{BlockMode, BlockSize, FrameDecoder, FrameEncoder, FrameInfo};

use crate::CompressionError;

/// Kafka's default lz4 level (`CompressionType.LZ4.defaultLevel()`). Only this
/// level uses the fast compressor; every other level in `1..=17` uses HC.
const DEFAULT_LEVEL: i32 = 9;

/// Kafka's independent-block max size (`Lz4BlockOutputStream` uses the LZ4F
/// default of 64 KiB), matched here so HC blocks line up with the fast path.
const BLOCK_SIZE: usize = 64 * 1024;

/// The high bit of a block's 4-byte little-endian size that marks it as
/// stored uncompressed, per the LZ4 frame format.
const BLOCK_UNCOMPRESSED_BIT: u32 = 0x8000_0000;

fn frame_info() -> FrameInfo {
    FrameInfo::new()
        .block_size(BlockSize::Max64KB)
        .block_mode(BlockMode::Independent)
        .block_checksums(false)
        .content_checksum(false)
}

/// The frame header bytes for [`frame_info`]: magic, FLG, BD and the header
/// checksum. Fixed by `frame_info`'s settings (no content size, no
/// dictionary), so we derive it once from `lz4rip`'s own encoder run over
/// empty input, rather than re-deriving the header checksum by hand, and reuse
/// it for every HC frame.
fn frame_header() -> &'static [u8] {
    static HEADER: LazyLock<Vec<u8>> = LazyLock::new(|| {
        let encoder = FrameEncoder::with_frame_info(frame_info(), Vec::new());
        let out = encoder
            .finish()
            .expect("an empty lz4 frame always finishes");
        // `out` is the header followed by the 4-byte end mark (there is no
        // content checksum): strip the end mark to leave just the header.
        out[..out.len() - 4].to_vec()
    });
    &HEADER
}

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let mut encoder = FrameEncoder::with_frame_info(frame_info(), Vec::with_capacity(data.len()));
    encoder.write_all(data)?;
    let out = encoder
        .finish()
        .map_err(|e| CompressionError::InvalidData(format!("lz4 finish: {e}")))?;
    Ok(Bytes::from(out))
}

/// Compress at `level`, which the caller has checked against Kafka's `1..=17`
/// range. Level 9 (Kafka's default) gives the same bytes as [`compress`].
/// Every other level uses LZ4 HC, in the same independent-64-KiB-block frame.
pub fn compress_with_level(data: &[u8], level: i32) -> Result<Bytes, CompressionError> {
    if level == DEFAULT_LEVEL {
        return compress(data);
    }
    let mut out = Vec::with_capacity(data.len());
    out.extend_from_slice(frame_header());
    // `[T]::chunks` yields no chunks for empty input, matching `compress`,
    // which never writes a block for an empty frame either.
    for block in data.chunks(BLOCK_SIZE) {
        write_hc_block(&mut out, block, level)?;
    }
    out.extend_from_slice(&0u32.to_le_bytes()); // end mark
    Ok(Bytes::from(out))
}

/// Append one LZ4F block (4-byte size, then the block bytes) compressed with
/// HC at `level`. Falls back to storing the block uncompressed, exactly as
/// the LZ4 frame format allows, when HC does not shrink it.
fn write_hc_block(out: &mut Vec<u8>, block: &[u8], level: i32) -> Result<(), CompressionError> {
    let mut buf = vec![0u8; lzzzz::lz4::max_compressed_size(block.len())];
    let comp_len = lzzzz::lz4_hc::compress(block, &mut buf, level)
        .map_err(|e| CompressionError::InvalidData(format!("lz4 hc compress: {e}")))?;
    // `block` is at most `BLOCK_SIZE` (64 KiB), so both lengths always fit in
    // a `u32`.
    if comp_len < block.len() {
        let size = u32::try_from(comp_len).expect("a 64 KiB block's compressed length fits u32");
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&buf[..comp_len]);
    } else {
        let size = u32::try_from(block.len()).expect("a 64 KiB block's length fits u32")
            | BLOCK_UNCOMPRESSED_BIT;
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(block);
    }
    Ok(())
}

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty lz4 payload".into()));
    }
    let decoder = FrameDecoder::new(data);
    // Read at most `max_output + 1` bytes so we can detect overflow without
    // materializing the oversized output.
    let mut limited = decoder.take((max_output as u64).saturating_add(1));
    let mut out = Vec::with_capacity(data.len().saturating_mul(2).min(max_output));
    limited
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::InvalidData(format!("lz4 decode: {e}")))?;
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
            decompress(b"this is not lz4", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn larger_payload_roundtrips() {
        let big = vec![0xABu8; 128 * 1024]; // 128 KiB -> multiple 64 KiB blocks
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
    fn frame_uses_64kib_independent_blocks() {
        // Compress a payload larger than 64 KiB so the block-size choice is
        // observable in the frame header: our explicit `Max64KB` must stay
        // 64 KiB rather than grow to an auto-selected larger block. This pins
        // the `frame_info()` settings (a `Default::default()` FrameInfo would
        // auto-pick a 256 KiB block for a payload this size).
        let big = vec![0xCDu8; 128 * 1024];
        let z = compress(&big).unwrap();
        // LZ4 frame layout: [magic:4][FLG][BD]...
        let flg = z[4];
        let bd = z[5];
        // BD bits 4..6 encode the block max size; value 4 == 64 KiB.
        assert2::assert!(&z[0..4] == &[0x04, 0x22, 0x4D, 0x18][..]);
        assert2::assert!(bd >> 4 & 0x7 == 4);
        assert2::assert!(flg >> 5 & 1 == 1);
        assert2::assert!(flg >> 4 & 1 == 0);
        assert2::assert!(flg >> 2 & 1 == 0);
    }

    /// A payload compressible enough that HC's larger search window finds
    /// matches the fast compressor misses, over more than one 64 KiB block.
    fn compressible_payload() -> Vec<u8> {
        let unit = b"the quick brown fox jumps over the lazy dog, again and again; ";
        unit.iter().copied().cycle().take(3 * BLOCK_SIZE).collect()
    }

    /// Table-driven over Kafka's lz4 levels: the default level (9) gives the
    /// same bytes as the fast compressor, every other tested level uses HC and
    /// beats the fast compressor's size on a compressible payload, and every
    /// level decompresses back to the original payload.
    #[test]
    fn compress_with_level_uses_hc_except_at_the_default() {
        let payload = compressible_payload();
        let fast = compress(&payload).unwrap();

        for (level, expect_fast_bytes) in [(9, true), (1, false), (17, false)] {
            let out = compress_with_level(&payload, level).unwrap();

            assert2::assert!(
                (out.as_ref() == fast.as_ref()) == expect_fast_bytes,
                "level {level}"
            );
            if !expect_fast_bytes {
                assert2::assert!(out.len() < fast.len(), "level {level}");
            }

            let back = decompress(&out, BIG_CAP).unwrap();
            assert2::assert!(back.as_ref() == payload.as_slice(), "level {level}");
        }
    }

    #[test]
    fn compress_with_level_hc_frame_uses_the_same_header_as_the_fast_frame() {
        // The HC path builds its own frame by hand; its header must still be
        // byte-identical to `lz4rip`'s, since that is what makes the frame
        // settings (independent 64 KiB blocks, no checksums, no content size)
        // match Kafka's regardless of which compressor filled the blocks.
        let payload = compressible_payload();
        let fast = compress(&payload).unwrap();
        let hc = compress_with_level(&payload, 1).unwrap();
        assert2::assert!(fast[..frame_header().len()] == hc[..frame_header().len()]);
    }
}
