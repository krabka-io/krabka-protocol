//! Check buffer optimizations against separately assembled codec streams.

use assert2::assert;
use krabka_compression::{
    CompressionError, CompressionType, compress, compress_with_level, decompress,
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

fn payload(size: usize, random: bool) -> Vec<u8> {
    let mut state = 0xDEAD_BEEF_CAFE_BABE_u64;
    (0..size)
        .map(|i| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            if random {
                state.to_be_bytes()[0]
            } else {
                i.to_le_bytes()[0]
            }
        })
        .collect()
}

#[test]
fn block_boundaries_and_output_limits() {
    for codec in [
        CompressionType::Lz4,
        CompressionType::Snappy,
        CompressionType::Zstd,
    ] {
        for size in [
            0, 1, 32_767, 32_768, 32_769, 65_535, 65_536, 65_537, 131_072, 131_073, 1_048_576,
        ] {
            for random in [false, true] {
                let input = payload(size, random);
                let wire = compress(codec, &input).unwrap();
                let decoded = decompress(
                    codec,
                    &wire,
                    ByteSize::from_bytes(u64::try_from(size).unwrap()),
                )
                .unwrap();
                assert!(decoded.as_ref() == input);
                if size != 0 {
                    assert!(matches!(
                        decompress(
                            codec,
                            &wire,
                            ByteSize::from_bytes(u64::try_from(size - 1).unwrap())
                        ),
                        Err(CompressionError::TooLarge { .. })
                    ));
                }
            }
        }
    }
}

#[test]
fn lz4_matches_streaming_encoder() {
    use std::io::Write as _;

    use lz4rip::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
    for size in [0, 1, 65_535, 65_536, 65_537, 1_048_576] {
        for random in [false, true] {
            let input = payload(size, random);
            let info = FrameInfo::new()
                .block_size(BlockSize::Max64KB)
                .block_mode(BlockMode::Independent)
                .block_checksums(false)
                .content_checksum(false);
            let mut encoder = FrameEncoder::with_frame_info(info, Vec::new());
            encoder.write_all(&input).unwrap();
            let reference = encoder.finish().unwrap();
            let wire = compress(CompressionType::Lz4, &input).unwrap();
            assert!(wire.as_ref() == reference);
        }
    }
}

#[test]
fn snappy_matches_separate_chunk_buffers() {
    for size in [0, 1, 32_767, 32_768, 32_769, 65_536, 1_048_576] {
        for random in [false, true] {
            let input = payload(size, random);
            let mut reference = vec![
                0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0, 0, 0, 0, 1, 0, 0, 0, 1,
            ];
            let mut encoder = snap::raw::Encoder::new();
            for chunk in input.chunks(32_768) {
                let block = encoder.compress_vec(chunk).unwrap();
                reference.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes());
                reference.extend_from_slice(&block);
            }
            let wire = compress(CompressionType::Snappy, &input).unwrap();
            assert!(wire.as_ref() == reference);
            let decoded = decompress(
                CompressionType::Snappy,
                &reference,
                ByteSize::from_bytes(u64::try_from(size).unwrap()),
            )
            .unwrap();
            assert!(decoded.as_ref() == input);
        }
    }
}

#[test]
fn zstd_reuse_preserves_fresh_context_output() {
    // Alternating sizes/levels and independent threads must not leak a previous
    // batch's parameters or dictionary into the next frame.
    for _ in 0..2 {
        std::thread::spawn(|| {
            for size in [65_536, 1, 131_072, 0, 131_073, 1024, 1_048_576, 65_536] {
                for level in [3, 1, 9, 3, -3, 3] {
                    let input = payload(size, false);
                    let reference = zstd::bulk::compress(&input, level).unwrap();
                    let wire = compress_with_level(CompressionType::Zstd, &input, level).unwrap();
                    assert!(wire.as_ref() == reference);
                }
            }
        })
        .join()
        .unwrap();
    }
}
