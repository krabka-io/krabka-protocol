//! `CodSpeed` microbenchmarks for `krabka-compression`.
//!
//! For each supported codec, we measure compress and decompress over three
//! payload sizes and three payload shapes. The sizes are 1 KiB, 64 KiB, and
//! 1 MiB. The shapes are:
//!
//! - **alternating runs**: mildly compressible, the original fixture.
//! - **random**: incompressible. This shape exercises the codec's worst case.
//! - **text-like**: repeated English-like bytes. This shape exercises typical
//!   user data.
//!
//! We also measure the round trip, compress then decompress, for each codec.
//! codspeed thus tracks net work that is directly comparable across codecs.

use std::{env, process};

use bytes::Bytes;
use criterion::{Criterion, abs_file, black_box, criterion_main};
use krabka_compression::{CompressionError, CompressionType, compress, decompress};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

// ---------------------------------------------------------------------------
// Payload generators — deterministic so codspeed sees the same bytes every run.
// ---------------------------------------------------------------------------

fn payload_alternating(size: usize) -> Bytes {
    let mut v = Vec::with_capacity(size);
    for i in 0..size {
        v.push(if (i / 32) % 2 == 0 { 0xAB } else { 0xCD });
    }
    Bytes::from(v)
}

fn payload_random(size: usize) -> Bytes {
    // Deterministic LCG so the bytes don't compress at all.
    let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
    let mut v = Vec::with_capacity(size);
    for _ in 0..size {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        v.push(state.to_be_bytes()[0]);
    }
    Bytes::from(v)
}

fn payload_text(size: usize) -> Bytes {
    // Repeated lorem-ipsum-like bytes. Highly compressible.
    const FRAGMENT: &[u8] =
        b"The quick brown fox jumps over the lazy dog. Lorem ipsum dolor sit amet. ";
    let mut v = Vec::with_capacity(size);
    while v.len() < size {
        let take = (size - v.len()).min(FRAGMENT.len());
        v.extend(FRAGMENT.iter().take(take));
    }
    Bytes::from(v)
}

// ---------------------------------------------------------------------------
// Bench drivers
// ---------------------------------------------------------------------------

/// The `max_output` cap for every decompression: no cap.
fn no_cap() -> ByteSize {
    ByteSize::from_bytes(u64::MAX)
}

/// The payload sizes every shape is measured at: 1 KiB, 64 KiB and 1 MiB.
const SIZES: [usize; 3] = [1024, 64 * 1024, 1024 * 1024];

/// Benchmarks one codec over one payload shape.
///
/// The timed closures return the codec's `Result`, so a failure cannot hide
/// in the measurement: each payload is compressed and decompressed once up
/// front, and an error there stops the run.
fn bench_one_shape(
    c: &mut Criterion,
    codec_name: &str,
    shape_name: &str,
    ct: CompressionType,
    payload: fn(usize) -> Bytes,
) -> Result<(), CompressionError> {
    let cases = SIZES
        .iter()
        .map(|&size| {
            let data = payload(size);
            let compressed = compress(ct, &data)?;
            decompress(ct, &compressed, no_cap())?;
            Ok((size, data, compressed))
        })
        .collect::<Result<Vec<_>, CompressionError>>()?;
    measure_shape(c, &format!("{codec_name}/{shape_name}"), ct, &cases);
    Ok(())
}

/// Measures compress, decompress and the round trip over each of `cases`:
/// a payload size, the payload, and its compressed form.
fn measure_shape(
    c: &mut Criterion,
    group_name: &str,
    ct: CompressionType,
    cases: &[(usize, Bytes, Bytes)],
) {
    let mut group = c.benchmark_group(group_name);
    for (size, data, compressed) in cases {
        group.bench_function(format!("compress_{size}"), |b| {
            b.iter(|| compress(ct, black_box(data)));
        });
        group.bench_function(format!("decompress_{size}"), |b| {
            b.iter(|| decompress(ct, black_box(compressed), no_cap()));
        });
        group.bench_function(format!("roundtrip_{size}"), |b| {
            b.iter(|| {
                let c = compress(ct, black_box(data))?;
                decompress(ct, &c, no_cap())
            });
        });
    }
    group.finish();
}

fn bench_codec(c: &mut Criterion, name: &str, ct: CompressionType) -> Result<(), CompressionError> {
    bench_one_shape(c, name, "alt", ct, payload_alternating)?;
    bench_one_shape(c, name, "rand", ct, payload_random)?;
    bench_one_shape(c, name, "text", ct, payload_text)
}

fn bench_gzip(c: &mut Criterion) -> Result<(), CompressionError> {
    bench_codec(c, "gzip", CompressionType::Gzip)
}
fn bench_snappy(c: &mut Criterion) -> Result<(), CompressionError> {
    bench_codec(c, "snappy", CompressionType::Snappy)
}
fn bench_lz4(c: &mut Criterion) -> Result<(), CompressionError> {
    bench_codec(c, "lz4", CompressionType::Lz4)
}
fn bench_zstd(c: &mut Criterion) -> Result<(), CompressionError> {
    bench_codec(c, "zstd", CompressionType::Zstd)
}

fn bench_none(c: &mut Criterion) -> Result<(), CompressionError> {
    // CompressionType::None is the pass-through path; track its overhead too.
    let payloads = SIZES
        .iter()
        .map(|&size| {
            let data = payload_alternating(size);
            compress(CompressionType::None, &data)?;
            decompress(CompressionType::None, &data, no_cap())?;
            Ok((size, data))
        })
        .collect::<Result<Vec<_>, CompressionError>>()?;
    measure_none(c, &payloads);
    Ok(())
}

/// Measures the pass-through path over each of `payloads`: a payload size
/// and the payload.
fn measure_none(c: &mut Criterion, payloads: &[(usize, Bytes)]) {
    let mut group = c.benchmark_group("none");
    for (size, data) in payloads {
        group.bench_function(format!("compress_{size}"), |b| {
            b.iter(|| compress(CompressionType::None, black_box(data)));
        });
        group.bench_function(format!("decompress_{size}"), |b| {
            b.iter(|| decompress(CompressionType::None, black_box(data), no_cap()));
        });
    }
    group.finish();
}

/// A benchmark target, under the name `criterion_group!` would give it.
type Target = (
    &'static str,
    fn(&mut Criterion) -> Result<(), CompressionError>,
);

/// Every benchmark target, in run order.
const TARGETS: [Target; 5] = [
    ("bench_none", bench_none),
    ("bench_gzip", bench_gzip),
    ("bench_snappy", bench_snappy),
    ("bench_lz4", bench_lz4),
    ("bench_zstd", bench_zstd),
];

/// Runs every target in [`TARGETS`], stopping at the first one that fails.
fn run_targets(criterion: &mut Criterion) -> Result<(), (&'static str, CompressionError)> {
    for (name, target) in TARGETS {
        if env::var("CODSPEED_ENV").is_ok() {
            criterion.set_current_file(abs_file!());
            criterion.set_macro_group(format!("benches::{name}"));
        }
        target(criterion).map_err(|error| (name, error))?;
    }
    Ok(())
}

/// The benchmark group, written out by hand where `criterion_group!` would
/// generate it, so that a failing target reports its error and stops the run.
fn benches() {
    let result = run_targets(&mut Criterion::default().configure_from_args());
    if let Err((name, error)) = result {
        eprintln!("{name}: {error}");
        process::exit(1);
    }
}

criterion_main!(benches);
