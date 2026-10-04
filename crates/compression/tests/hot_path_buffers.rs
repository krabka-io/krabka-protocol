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
        CompressionType::Gzip,
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
    for size in (0..=13).chain([
        96, 1024, 1025, 65_534, 65_535, 65_536, 65_537, 65_548, 1_048_576,
    ]) {
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
fn lz4_reuse_preserves_independent_frames() {
    let threads: Vec<_> = (0..2_u8)
        .map(|seed| {
            std::thread::spawn(move || {
                for size in [65_536, 65_535, 65_534, 131_071, 12, 65_536, 1_048_576] {
                    for random in [false, true] {
                        let mut input = payload(size, random);
                        for byte in &mut input {
                            *byte ^= seed;
                        }
                        let mut reference = vec![0x04, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x82];
                        for block in input.chunks(65_536) {
                            let mut compressed =
                                vec![0; lz4rip::block::get_maximum_output_size(block.len())];
                            let len = lz4rip::block::compress_into(block, &mut compressed).unwrap();
                            if len < block.len() {
                                reference
                                    .extend_from_slice(&u32::try_from(len).unwrap().to_le_bytes());
                                reference.extend_from_slice(&compressed[..len]);
                            } else {
                                let len = u32::try_from(block.len()).unwrap() | 0x8000_0000;
                                reference.extend_from_slice(&len.to_le_bytes());
                                reference.extend_from_slice(block);
                            }
                        }
                        reference.extend_from_slice(&[0; 4]);
                        let wire = compress(CompressionType::Lz4, &input).unwrap();
                        assert!(wire.as_ref() == reference);
                    }
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn lz4_hc_matches_separate_block_buffers() {
    const BLOCK: usize = 65_536;
    for size in (0..=15).chain([
        96,
        1024,
        BLOCK - 1,
        BLOCK,
        BLOCK + 1,
        BLOCK + 12,
        BLOCK + 13,
        2 * BLOCK,
        2 * BLOCK + 1,
    ]) {
        for input in [payload(size, false), payload(size, true), vec![7; size]] {
            for level in 1..=17 {
                if level == 9 {
                    assert!(
                        compress_with_level(CompressionType::Lz4, &input, level).unwrap()
                            == compress(CompressionType::Lz4, &input).unwrap()
                    );
                    continue;
                }
                // Independently assemble the old frame with separate compressed
                // block buffers, including blocks too short to shrink.
                let mut reference = vec![0x04, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x82];
                for block in input.chunks(BLOCK) {
                    let mut compressed = vec![0; lzzzz::lz4::max_compressed_size(block.len())];
                    let len =
                        lzzzz::lz4_hc::compress(block, &mut compressed, level.min(12)).unwrap();
                    if len < block.len() {
                        reference.extend_from_slice(&u32::try_from(len).unwrap().to_le_bytes());
                        reference.extend_from_slice(&compressed[..len]);
                    } else {
                        let len = u32::try_from(block.len()).unwrap() | 0x8000_0000;
                        reference.extend_from_slice(&len.to_le_bytes());
                        reference.extend_from_slice(block);
                    }
                }
                reference.extend_from_slice(&[0; 4]);
                let wire = compress_with_level(CompressionType::Lz4, &input, level).unwrap();
                assert!(wire.as_ref() == reference, "size={size}, level={level}");
                let limit = ByteSize::from_bytes(u64::try_from(size).unwrap());
                assert!(
                    decompress(CompressionType::Lz4, &wire, limit)
                        .unwrap()
                        .as_ref()
                        == input
                );
                if size != 0 {
                    let too_small = ByteSize::from_bytes(u64::try_from(size - 1).unwrap());
                    assert!(matches!(
                        decompress(CompressionType::Lz4, &wire, too_small),
                        Err(CompressionError::TooLarge { .. })
                    ));
                }
            }
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

#[test]
fn gzip_reuse_matches_streaming_encoder() {
    use std::io::Write as _;

    use flate2::{Compression, write::GzEncoder};
    for size in [
        0, 1, 10, 13, 1024, 32_767, 32_768, 32_769, 65_535, 65_536, 65_537, 131_071, 131_072,
        131_073, 1_048_576,
    ] {
        for random in [false, true] {
            let input = payload(size, random);
            for level in std::iter::once(-1).chain(1..=9) {
                let compression =
                    u32::try_from(level).map_or_else(|_| Compression::default(), Compression::new);
                let mut reference = GzEncoder::new(Vec::new(), compression);
                reference.write_all(&input).unwrap();
                let reference = reference.finish().unwrap();
                for _ in 0..2 {
                    let wire = compress_with_level(CompressionType::Gzip, &input, level).unwrap();
                    assert!(
                        wire.as_ref() == reference,
                        "{size} bytes, random={random}, level={level}"
                    );
                }
            }
        }
    }
}

#[test]
fn gzip_reuse_preserves_independent_frames() {
    use std::io::Write as _;

    use flate2::{Compression, write::GzEncoder};
    let threads: Vec<_> = (0..2)
        .map(|seed| {
            std::thread::spawn(move || {
                for level in [6, 6, 1, 1, 9, -1, 6] {
                    for size in [65_536, 1, 131_073, 131_072, 0, 32_768, 13] {
                        let mut input = payload(size, seed != 0);
                        for byte in &mut input {
                            *byte ^= seed;
                        }
                        let compression = u32::try_from(level)
                            .map_or_else(|_| Compression::default(), Compression::new);
                        let mut reference = GzEncoder::new(Vec::new(), compression);
                        reference.write_all(&input).unwrap();
                        let reference = reference.finish().unwrap();
                        let wire =
                            compress_with_level(CompressionType::Gzip, &input, level).unwrap();
                        assert!(wire.as_ref() == reference);
                        let back = decompress(
                            CompressionType::Gzip,
                            &wire,
                            ByteSize::from_bytes(u64::try_from(size).unwrap()),
                        )
                        .unwrap();
                        assert!(back.as_ref() == input);
                    }
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn gzip_encoding_during_thread_local_drop() {
    struct EncodeOnDrop;
    impl Drop for EncodeOnDrop {
        fn drop(&mut self) {
            use std::io::Write as _;

            use flate2::{Compression, write::GzEncoder};
            let input = b"encode after the later thread-local cache has dropped";
            let mut reference = GzEncoder::new(Vec::new(), Compression::default());
            reference.write_all(input).unwrap();
            let reference = reference.finish().unwrap();
            let wire = compress(CompressionType::Gzip, input).unwrap();
            assert!(wire.as_ref() == reference);
        }
    }
    thread_local! {
        static FIRST: EncodeOnDrop = const { EncodeOnDrop };
    }
    std::thread::spawn(|| {
        // Rust drops these in reverse initialization order. FIRST must encode
        // after the cache initialized by this nonempty compression is gone.
        FIRST.with(|_| {});
        compress(CompressionType::Gzip, b"initialize the encoder cache").unwrap();
    })
    .join()
    .unwrap();
}
