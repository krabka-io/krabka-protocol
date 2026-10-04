use std::io::Write as _;

use assert2::assert;
use bytes::Bytes;
use krabka_compression::{CompressionError, CompressionType, decompress};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
const MAX_DIRECT_OUTPUT: usize = 128 * 1024;
fn original_decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
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

#[test]
fn zstd_input_boundaries_preserve_complete_results_and_errors() {
    for size in [
        0_usize, 1, 1024, 131_071, 131_072, 131_073, 131_074, 131_075, 131_076, 262_144, 1_048_576,
    ] {
        for random in [false, true] {
            let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
            let input: Vec<u8> = (0..size)
                .map(|i| {
                    if random {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        u8::try_from(seed & 255).unwrap()
                    } else {
                        u8::try_from(i % 251).unwrap()
                    }
                })
                .collect();
            for known_size in [false, true] {
                for checksum in [false, true] {
                    let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
                    encoder.include_checksum(checksum).unwrap();
                    if known_size {
                        encoder
                            .set_pledged_src_size(Some(u64::try_from(size).unwrap()))
                            .unwrap();
                    }
                    encoder.write_all(&input).unwrap();
                    let wire = encoder.finish().unwrap();
                    let mut variants = vec![wire.clone(), Vec::new()];
                    for index in [
                        0,
                        4,
                        wire.len() / 2,
                        wire.len() - 1,
                        131_074,
                        131_075,
                        131_076,
                    ] {
                        if index < wire.len() {
                            let mut changed = wire.clone();
                            changed[index] ^= 1;
                            variants.push(changed);
                        }
                    }
                    for len in [4, wire.len() / 2, wire.len() - 1] {
                        variants.push(wire[..len.min(wire.len())].to_vec());
                    }
                    let mut trailing = wire.clone();
                    trailing.extend_from_slice(&[0; 32]);
                    variants.push(trailing);
                    let mut concat = wire.clone();
                    concat.extend_from_slice(&zstd::bulk::compress(b"another frame", 3).unwrap());
                    variants.push(concat);
                    let mut skipped = 0x184D_2A50_u32.to_le_bytes().to_vec();
                    skipped.extend_from_slice(&4_u32.to_le_bytes());
                    skipped.extend_from_slice(b"skip");
                    skipped.extend_from_slice(&wire);
                    variants.push(skipped);
                    let mut caps = vec![
                        0,
                        1,
                        size.saturating_sub(1),
                        size,
                        size + 1,
                        131_071,
                        131_072,
                        131_073,
                        2_097_152,
                    ];
                    caps.sort_unstable();
                    caps.dedup();
                    for variant in variants {
                        for &cap in &caps {
                            let expected = original_decompress(&variant, cap)
                                .map(|b| b.to_vec())
                                .map_err(|e| e.to_string());
                            let actual = decompress(
                                CompressionType::Zstd,
                                &variant,
                                ByteSize::from_bytes(u64::try_from(cap).unwrap()),
                            )
                            .map(|b| b.to_vec())
                            .map_err(|e| e.to_string());
                            assert!(
                                actual == expected,
                                "size={size}, random={random}, known={known_size}, checksum={checksum}, cap={cap}"
                            );
                        }
                    }
                }
            }
        }
    }
}
