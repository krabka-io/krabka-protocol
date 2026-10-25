//! `CodSpeed` microbenchmarks for `krabka-protocol::records`.
//!
//! Covers the v2 `RecordBatch` codec path end to end: encode, owned decode,
//! borrowed zero-copy decode with iteration, and `encoded_len`. Each axis
//! varies across compression codecs and a small grid of batch sizes, so a
//! regression in any one shape shows up clearly.

use std::process;

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkGroup, Criterion, black_box, criterion_main, measurement::WallTime};
use krabka_compression::CompressionType;
use krabka_protocol::{
    DecodeBorrow, ProtocolError,
    records::{
        Record, RecordBatch, RecordBatchBorrowed, RecordHeader, RecordsError,
        count_records_in_v2_batches, validate_one_v2_batch,
    },
};

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn make_header(key: &str) -> RecordHeader {
    RecordHeader {
        key: key.to_owned(),
        value: Some(Bytes::from_static(b"header-value")),
    }
}

fn make_record(offset_delta: i32, payload_size: usize) -> Record {
    Record {
        attributes: 0,
        timestamp_delta: i64::from(offset_delta) * 1000,
        offset_delta,
        key: Some(Bytes::from_static(b"benchmark-key")),
        value: Some(Bytes::from(vec![0xABu8; payload_size])),
        headers: vec![make_header("tracing-id")],
    }
}

fn make_ramp_record(offset_delta: i32, payload_size: usize) -> Record {
    let mut value = vec![0u8; payload_size];
    for (i, byte) in value.iter_mut().enumerate() {
        *byte = i.to_le_bytes()[0];
    }
    Record {
        attributes: 0,
        timestamp_delta: i64::from(offset_delta) * 1000,
        offset_delta,
        key: Some(Bytes::from_static(b"benchmark-key")),
        value: Some(Bytes::from(value)),
        headers: vec![make_header("tracing-id")],
    }
}

fn make_batch(n: u32, payload_size: usize, codec: CompressionType) -> RecordBatch {
    let records: Vec<Record> = (0..n)
        .map(|i| make_record(i.cast_signed(), payload_size))
        .collect();
    let mut b = RecordBatch {
        base_offset: 100,
        partition_leader_epoch: 0,
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_000 + i64::from(n) * 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        last_offset_delta: (i32::try_from(n.saturating_sub(1))).unwrap_or(0),
        records,
        ..RecordBatch::default()
    };
    b.attributes = b.attributes.with_compression(codec);
    b
}

fn make_ramp_batch(n: u32, payload_size: usize, codec: CompressionType) -> RecordBatch {
    let records: Vec<Record> = (0..n)
        .map(|i| make_ramp_record(i.cast_signed(), payload_size))
        .collect();
    let mut b = RecordBatch {
        base_offset: 100,
        partition_leader_epoch: 0,
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_000 + i64::from(n) * 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        last_offset_delta: (i32::try_from(n.saturating_sub(1))).unwrap_or(0),
        records,
        ..RecordBatch::default()
    };
    b.attributes = b.attributes.with_compression(codec);
    b
}

fn encode_batch(b: &RecordBatch) -> Result<Vec<u8>, RecordsError> {
    let mut buf = BytesMut::with_capacity(b.encoded_batch_len());
    b.encode_batch(&mut buf)?;
    Ok(buf.to_vec())
}

/// Runs `benches` in a new benchmark group named `name`.
///
/// The bench run ends with an error when the group's setup fails.
fn with_group(
    c: &mut Criterion,
    name: &str,
    benches: impl FnOnce(&mut BenchmarkGroup<'_, WallTime>) -> Result<(), ProtocolError>,
) {
    let mut group = c.benchmark_group(name);
    if let Err(e) = benches(&mut group) {
        eprintln!("{name}: setup failed: {e}");
        process::exit(1);
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// RecordBatch encode benchmarks — codecs × batch size
// ---------------------------------------------------------------------------

fn bench_encode_by_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/encode");

    for (label, codec) in [
        ("none", CompressionType::None),
        ("gzip", CompressionType::Gzip),
        ("snappy", CompressionType::Snappy),
        ("lz4", CompressionType::Lz4),
        ("zstd", CompressionType::Zstd),
    ] {
        let batch = make_batch(10_u32, 128, codec);
        let mut buf = BytesMut::with_capacity(batch.encoded_batch_len() * 2);
        group.bench_function(label, |b| {
            b.iter(|| {
                buf.clear();
                black_box(&batch).encode_batch(&mut buf)
            });
        });
    }

    group.finish();
}

fn bench_encode_by_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/encode_by_size_none");

    for &n in &[1u32, 10, 100, 1000] {
        let batch = make_batch(n, 64, CompressionType::None);
        let mut buf = BytesMut::with_capacity(batch.encoded_batch_len() * 2);
        group.bench_function(format!("{n}_records"), |b| {
            b.iter(|| {
                buf.clear();
                black_box(&batch).encode_batch(&mut buf)
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// RecordBatch decode benchmarks (owned)
// ---------------------------------------------------------------------------

fn bench_decode_owned_by_codec(c: &mut Criterion) {
    with_group(
        c,
        "record_batch/decode_owned",
        try_bench_decode_owned_by_codec,
    );
}

fn try_bench_decode_owned_by_codec(
    group: &mut BenchmarkGroup<'_, WallTime>,
) -> Result<(), ProtocolError> {
    for (label, codec) in [
        ("none", CompressionType::None),
        ("gzip", CompressionType::Gzip),
        ("snappy", CompressionType::Snappy),
        ("lz4", CompressionType::Lz4),
        ("zstd", CompressionType::Zstd),
    ] {
        let encoded = encode_batch(&make_batch(10_u32, 128, codec))?;
        group.bench_function(label, |b| {
            b.iter(|| {
                let mut cur: &[u8] = black_box(&encoded);
                RecordBatch::decode_batch(&mut cur)
            });
        });
    }

    Ok(())
}

fn bench_decode_owned_by_size(c: &mut Criterion) {
    with_group(
        c,
        "record_batch/decode_owned_by_size_none",
        try_bench_decode_owned_by_size,
    );
}

fn try_bench_decode_owned_by_size(
    group: &mut BenchmarkGroup<'_, WallTime>,
) -> Result<(), ProtocolError> {
    for &n in &[1u32, 10, 100, 1000] {
        let encoded = encode_batch(&make_batch(n, 64, CompressionType::None))?;
        group.bench_function(format!("{n}_records"), |b| {
            b.iter(|| {
                let mut cur: &[u8] = black_box(&encoded);
                RecordBatch::decode_batch(&mut cur)
            });
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// RecordBatch decode benchmarks (borrowed, zero-copy)
// ---------------------------------------------------------------------------

fn bench_decode_borrowed(c: &mut Criterion) {
    with_group(c, "record_batch/decode_borrowed", try_bench_decode_borrowed);
}

fn try_bench_decode_borrowed(
    group: &mut BenchmarkGroup<'_, WallTime>,
) -> Result<(), ProtocolError> {
    for &n in &[1u32, 10, 100, 1000] {
        let encoded = encode_batch(&make_batch(n, 64, CompressionType::None))?;
        group.bench_function(format!("{n}_records"), |b| {
            b.iter(|| {
                let mut cur: &[u8] = black_box(&encoded);
                RecordBatchBorrowed::decode_borrow(&mut cur, 0)
            });
        });
    }

    Ok(())
}

fn bench_validate_one_v2_batch(c: &mut Criterion) {
    with_group(
        c,
        "record_batch/validate_one_v2_batch",
        try_bench_validate_one_v2_batch,
    );
}

fn try_bench_validate_one_v2_batch(
    group: &mut BenchmarkGroup<'_, WallTime>,
) -> Result<(), ProtocolError> {
    for &(label, records, payload) in &[
        ("1rec_empty", 1, 0),
        ("1rec_64B", 1, 64),
        ("1rec_1KiB", 1, 1024),
        ("1rec_100KiB", 1u32, 100 * 1024usize),
        ("1rec_512KiB", 1, 512 * 1024),
        ("10rec_10KiB", 10, 10 * 1024),
        ("100rec_1KiB", 100, 1024),
    ] {
        let encoded = encode_batch(&make_batch(records, payload, CompressionType::None))?;
        group.bench_function(label, |b| {
            b.iter(|| {
                validate_one_v2_batch(black_box(&encoded)).map(|validated| validated.total_len)
            });
        });
    }

    let encoded = encode_batch(&make_ramp_batch(1, 100 * 1024, CompressionType::Lz4))?;
    group.bench_function("1rec_100KiB_lz4_ramp", |b| {
        b.iter(|| validate_one_v2_batch(black_box(&encoded)).map(|validated| validated.total_len));
    });

    Ok(())
}

fn bench_count_records_in_v2_batches(c: &mut Criterion) {
    with_group(
        c,
        "record_batch/count_records_in_v2_batches",
        try_bench_count_records_in_v2_batches,
    );
}

fn try_bench_count_records_in_v2_batches(
    group: &mut BenchmarkGroup<'_, WallTime>,
) -> Result<(), ProtocolError> {
    for &(label, records, payload) in &[
        ("1rec_100KiB", 1u32, 100 * 1024usize),
        ("1rec_512KiB", 1, 512 * 1024),
        ("10rec_10KiB", 10, 10 * 1024),
        ("100rec_1KiB", 100, 1024),
    ] {
        let encoded = encode_batch(&make_batch(records, payload, CompressionType::None))?;
        group.bench_function(label, |b| {
            b.iter(|| black_box(count_records_in_v2_batches(black_box(&encoded))));
        });
    }

    let encoded = encode_batch(&make_ramp_batch(1, 100 * 1024, CompressionType::Lz4))?;
    group.bench_function("1rec_100KiB_lz4_ramp", |b| {
        b.iter(|| black_box(count_records_in_v2_batches(black_box(&encoded))));
    });

    Ok(())
}

fn bench_borrowed_iter(c: &mut Criterion) {
    with_group(c, "record_batch/borrowed_iter", try_bench_borrowed_iter);
}

fn try_bench_borrowed_iter(group: &mut BenchmarkGroup<'_, WallTime>) -> Result<(), ProtocolError> {
    for &n in &[10u32, 100, 1000] {
        let encoded = encode_batch(&make_batch(n, 64, CompressionType::None))?;
        let mut cur: &[u8] = &encoded;
        let batch = RecordBatchBorrowed::decode_borrow(&mut cur, 0)?;
        group.bench_function(format!("{n}_records"), |b| {
            b.iter(|| -> Result<usize, RecordsError> {
                let mut count = 0usize;
                for r in black_box(&batch) {
                    count += r?.value.map_or(0, <[u8]>::len);
                }
                Ok(count)
            });
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// encoded_len benchmark (no allocation)
// ---------------------------------------------------------------------------

fn bench_encoded_len(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/encoded_len");

    for &n in &[1u32, 10, 100, 1000] {
        let batch = make_batch(n, 64, CompressionType::None);
        group.bench_function(format!("none_{n}rec"), |b| {
            b.iter(|| black_box(&batch).encoded_batch_len());
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Runs every benchmark in this file.
///
/// Spelled out rather than through `criterion_group!`, so the `Criterion`
/// drops as soon as the last benchmark is done.
fn benches() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_encode_by_codec(&mut criterion);
    bench_encode_by_size(&mut criterion);
    bench_decode_owned_by_codec(&mut criterion);
    bench_decode_owned_by_size(&mut criterion);
    bench_decode_borrowed(&mut criterion);
    bench_validate_one_v2_batch(&mut criterion);
    bench_count_records_in_v2_batches(&mut criterion);
    bench_borrowed_iter(&mut criterion);
    bench_encoded_len(&mut criterion);
    drop(criterion);
}
criterion_main!(benches);
