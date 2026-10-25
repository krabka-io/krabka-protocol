#![cfg(test)]

mod support;
use std::iter;

use krabka_compression::{CompressionType, compress, compress_with_level, decompress};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use proptest::{collection, prelude::*};
use support::oracle::{self, Oracle};

fn arb_payload() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        collection::vec(any::<u8>(), 0..=8 * 1024),
        collection::vec(0u8..=0u8, 0..=8 * 1024),
    ]
}

macro_rules! diff_test {
    ($name:ident, $codec_str:literal, $ct:expr) => {
        #[test]
        #[ignore = "requires JVM oracle"]
        fn $name() {
            // proptest! requires an `Fn` closure; wrap the guard in RefCell
            // so we can call `&mut Oracle` methods via interior mutability.
            let o = std::cell::RefCell::new(oracle::shared());
            proptest!(|(data in arb_payload())| {
                // Rust compresses; JVM decompresses; bytes match input.
                let rust_z = compress($ct, &data).unwrap();
                let jvm_back = o.borrow_mut().decompress($codec_str, &rust_z);
                prop_assert_eq!(jvm_back, data.clone());

                // JVM compresses; Rust decompresses; bytes match input.
                let jvm_z = o.borrow_mut().compress($codec_str, &data);
                let rust_back = decompress($ct, &jvm_z, ByteSize::from_bytes(u64::MAX)).unwrap();
                prop_assert_eq!(rust_back.as_ref(), data.as_slice());
            });
        }
    };
}

diff_test!(gzip_differential, "gzip", CompressionType::Gzip);
diff_test!(snappy_differential, "snappy", CompressionType::Snappy);
diff_test!(lz4_differential, "lz4", CompressionType::Lz4);
diff_test!(zstd_differential, "zstd", CompressionType::Zstd);

#[test]
#[ignore = "requires JVM oracle"]
fn lz4_hc_levels_and_block_boundaries_differential() {
    check_lz4_hc_levels_and_block_boundaries(&mut oracle::shared());
}

/// The body of [`lz4_hc_levels_and_block_boundaries_differential`], against the locked shared oracle.
fn check_lz4_hc_levels_and_block_boundaries(oracle: &mut Oracle) {
    for size in [0, 1, 12, 13, 65_536, 65_537, 65_548, 1_048_576] {
        for random in [false, true] {
            let mut state = 0xDEAD_BEEF_CAFE_BABE_u64;
            let input: Vec<u8> = iter::repeat_with(|| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                if random { state.to_be_bytes()[0] } else { 7 }
            })
            .take(size)
            .collect();
            for level in [1, 12, 17] {
                let encoded = compress_with_level(CompressionType::Lz4, &input, level).unwrap();
                assert2::assert!(oracle.decompress("lz4", &encoded) == input);
            }
        }
    }
}

#[test]
#[ignore = "requires JVM oracle"]
fn chunk_boundaries_differential() {
    check_chunk_boundaries(&mut oracle::shared());
}

/// The body of [`chunk_boundaries_differential`], against the locked shared oracle.
fn check_chunk_boundaries(oracle: &mut Oracle) {
    for (name, codec) in [
        ("lz4", CompressionType::Lz4),
        ("snappy", CompressionType::Snappy),
        ("zstd", CompressionType::Zstd),
    ] {
        for size in [
            0, 32_767, 32_768, 32_769, 65_535, 65_536, 65_537, 131_073, 1_048_576,
        ] {
            for random in [false, true] {
                let mut state = 0xDEAD_BEEF_CAFE_BABE_u64;
                let input: Vec<u8> = iter::repeat_with(|| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    if random { state.to_be_bytes()[0] } else { 0xAB }
                })
                .take(size)
                .collect();
                let encoded = compress(codec, &input).unwrap();
                assert2::assert!(oracle.decompress(name, &encoded) == input);
                let encoded = oracle.compress(name, &input);
                let decoded = decompress(
                    codec,
                    &encoded,
                    ByteSize::from_bytes(u64::try_from(size).unwrap()),
                )
                .unwrap();
                assert2::assert!(decoded.as_ref() == input);
            }
        }
    }
}

#[test]
#[ignore = "requires JVM oracle"]
fn gzip_levels_and_frame_sizes_differential() {
    check_gzip_levels_and_frame_sizes(&mut oracle::shared());
}

/// The body of [`gzip_levels_and_frame_sizes_differential`], against the locked shared oracle.
fn check_gzip_levels_and_frame_sizes(oracle: &mut Oracle) {
    for size in [0_usize, 1, 1024, 131_071, 131_072, 131_073] {
        for random in [false, true] {
            let mut state = 0xDEAD_BEEF_CAFE_BABE_u64;
            let input: Vec<u8> = (0..size)
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
                .collect();
            for level in [1, 6, 9, -1] {
                for _ in 0..2 {
                    let wire = compress_with_level(CompressionType::Gzip, &input, level).unwrap();
                    let back = oracle.decompress("gzip", &wire);
                    assert2::assert!(back == input);
                }
            }
        }
    }
}
