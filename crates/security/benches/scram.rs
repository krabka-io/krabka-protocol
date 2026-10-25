//! `CodSpeed` microbenchmarks for `krabka-security`.
//!
//! These benchmarks cover the security primitives that every authenticated
//! connection hits: SASL/PLAIN verification with a constant-time compare, SCRAM
//! password hashing with PBKDF2, and `derive_keys_from_salted`, the broker-side
//! key derivation from KIP-554.

use std::{collections::HashMap, env};

use criterion::{Criterion, abs_file, black_box, criterion_main};
use krabka_security::{SaslMechanism, scram, verify_plain};

// ---------------------------------------------------------------------------
// PBKDF2 — the dominant cost in a SCRAM login.
// ---------------------------------------------------------------------------

fn bench_scram_pbkdf2(c: &mut Criterion) {
    let mut group = c.benchmark_group("scram/pbkdf2");
    let password = b"correct horse battery staple";
    let salt = vec![0x77u8; 16];

    // SCRAM-SHA-256 across iteration counts. Kafka defaults to 4096; admins
    // often set 8192 or higher. We track the cost across that range.
    for &iters in &[4096u32, 8192, 16384] {
        group.bench_function(format!("sha256_{iters}"), |b| {
            b.iter(|| {
                scram::hash_scram_password_with_salt(
                    black_box(password),
                    SaslMechanism::ScramSha256,
                    iters,
                    salt.clone(),
                )
            });
        });
        group.bench_function(format!("sha512_{iters}"), |b| {
            b.iter(|| {
                scram::hash_scram_password_with_salt(
                    black_box(password),
                    SaslMechanism::ScramSha512,
                    iters,
                    salt.clone(),
                )
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// derive_keys_from_salted — the broker's per-credential transform under
// KIP-554. Called on every AlterUserScramCredentials request.
// ---------------------------------------------------------------------------

fn bench_derive_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("scram/derive_keys");

    let sha256_input = vec![0x42u8; 32];
    let sha512_input = vec![0x42u8; 64];

    group.bench_function("sha256", |b| {
        b.iter(|| {
            scram::derive_keys_from_salted(SaslMechanism::ScramSha256, black_box(&sha256_input))
        });
    });

    group.bench_function("sha512", |b| {
        b.iter(|| {
            scram::derive_keys_from_salted(SaslMechanism::ScramSha512, black_box(&sha512_input))
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// SASL/PLAIN verification — the constant-time happy path and the
// unknown-user / wrong-password paths.
// ---------------------------------------------------------------------------

fn bench_verify_plain(c: &mut Criterion) {
    let mut group = c.benchmark_group("scram/verify_plain");

    let mut creds: HashMap<String, String> = HashMap::new();
    for i in 0..16 {
        creds.insert(format!("user-{i:03}"), format!("password-{i:03}"));
    }
    creds.insert("alice".to_owned(), "hunter2".to_owned());

    group.bench_function("ok", |b| {
        b.iter(|| verify_plain(black_box(&creds), black_box("alice"), black_box(b"hunter2")));
    });
    group.bench_function("wrong_password", |b| {
        b.iter(|| verify_plain(black_box(&creds), black_box("alice"), black_box(b"hunter3")));
    });
    group.bench_function("unknown_user", |b| {
        b.iter(|| verify_plain(black_box(&creds), black_box("nobody"), black_box(b"x")));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// scram_hash_len — trivial; included as a smoke benchmark to detect a
// regression that would imply the dispatch became non-constant-cost.
// ---------------------------------------------------------------------------

fn bench_scram_hash_len(c: &mut Criterion) {
    let mut group = c.benchmark_group("scram/hash_len");
    group.bench_function("sha256", |b| {
        b.iter(|| scram::scram_hash_len(black_box(SaslMechanism::ScramSha256)));
    });
    group.bench_function("sha512", |b| {
        b.iter(|| scram::scram_hash_len(black_box(SaslMechanism::ScramSha512)));
    });
    group.finish();
}

/// A benchmark target, under the name `criterion_group!` would give it.
type Target = (&'static str, fn(&mut Criterion));

/// Every benchmark target, in run order.
const TARGETS: [Target; 4] = [
    ("bench_scram_pbkdf2", bench_scram_pbkdf2),
    ("bench_derive_keys", bench_derive_keys),
    ("bench_verify_plain", bench_verify_plain),
    ("bench_scram_hash_len", bench_scram_hash_len),
];

/// Runs every target in [`TARGETS`].
fn run_targets(criterion: &mut Criterion) {
    for (name, target) in TARGETS {
        if env::var("CODSPEED_ENV").is_ok() {
            criterion.set_current_file(abs_file!());
            criterion.set_macro_group(format!("benches::{name}"));
        }
        target(criterion);
    }
}

/// The benchmark group, written out by hand where `criterion_group!` would
/// generate it, so that the `Criterion` it configures drops as soon as the
/// targets have run.
fn benches() {
    run_targets(&mut Criterion::default().configure_from_args());
}

criterion_main!(benches);
