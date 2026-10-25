//! CRC-32C (Castagnoli), using the runtime-dispatched `crc-fast` backend.
//!
//! Kafka v2 record batches use this CRC over everything after the `crc` field of
//! the header. [`records`](super) re-exports both functions so that every
//! krabka crate takes the checksum through one entry point.

/// Returns the CRC-32C of `data`.
///
/// This is the checksum a v2 record batch stores, taken over the bytes from
/// [`CRC_COVERAGE_START`](super::CRC_COVERAGE_START) to the end of the batch.
/// Prefer one call over a contiguous region to [`crc32c_append`]: it is a
/// single pass with no digest set-up.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    crc_fast::crc32_iscsi(data)
}

/// Continues a CRC-32C over `data` from `seed`, the finished CRC-32C of the
/// bytes before it.
///
/// `crc32c_append(crc32c(a), b)` equals the CRC-32C of `a` followed by `b`.
/// Use it only when the two parts are not contiguous in memory: each call
/// builds a digest from `seed`.
#[must_use]
pub fn crc32c_append(seed: u32, data: &[u8]) -> u32 {
    // `seed` is finalized; the digest starts with the unfinalized state.
    let mut digest =
        crc_fast::Digest::new_with_init_state(crc_fast::CrcAlgorithm::Crc32Iscsi, u64::from(!seed));
    digest.update(data);
    // A finalized CRC-32 occupies the low 32 bits of the digest's `u64`.
    let [b0, b1, b2, b3, ..] = digest.finalize().to_le_bytes();
    u32::from_le_bytes([b0, b1, b2, b3])
}

#[cfg(test)]
mod tests {
    use proptest::{collection, prelude::*};

    use super::*;

    /// Standard CRC-32C reference vectors.
    /// "123456789" -> 0xE3069283, from RFC 3720 for iSCSI.
    const VECTORS: &[(&[u8], u32)] = &[
        (b"", 0x0000_0000),
        (b"a", 0xC1D0_4330),
        (b"123456789", 0xE306_9283),
        (b"The quick brown fox jumps over the lazy dog", 0x2262_0404),
    ];

    #[test]
    fn known_vectors() {
        for (input, expected) in VECTORS {
            let got = crc32c(input);
            assert2::assert!(got == *expected);
        }
    }

    proptest! {
        #[test]
        fn matches_reference_for_unaligned_and_seeded_chunks(
            data in collection::vec(any::<u8>(), 0..131_072),
            seed in any::<u32>(),
            split in any::<usize>(),
            prefix in 0..64_usize,
        ) {
            let bytes = &data[prefix.min(data.len())..];
            let split = split % (bytes.len() + 1);
            assert2::assert!(crc32c(bytes) == crc32c::crc32c(bytes));
            assert2::assert!(crc32c_append(seed, bytes) == crc32c::crc32c_append(seed, bytes));
            assert2::assert!(
                crc32c_append(crc32c_append(seed, &bytes[..split]), &bytes[split..])
                    == crc32c::crc32c_append(seed, bytes)
            );
        }
    }
}
