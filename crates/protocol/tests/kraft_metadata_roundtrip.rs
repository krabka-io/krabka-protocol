//! Byte-identity test for real `KRaft` metadata logs.
//!
//! This test decodes every record and batch in real
//! `mirror.gcr.io/apache/kafka:4.0.0` metadata logs and bootstrap.checkpoint. It
//! decodes them through the generated record types and the `KraftMetadataRecord`
//! envelope, re-encodes them, and asserts that the bytes are unchanged.

use std::{env, fs, path::Path};

use bytes::BytesMut;
use krabka_protocol::records::{RecordBatch, metadata::record::KraftMetadataRecord};

/// Walk every batch in `log`.
///
/// This function asserts that each batch re-encodes byte-identically. It also
/// asserts that every non-control record value round-trips through
/// `KraftMetadataRecord`.
fn assert_log_roundtrips(log: &[u8]) -> datatest_stable::Result<()> {
    let mut rest = log;
    while !rest.is_empty() {
        let mut cur: &[u8] = rest;
        let batch = RecordBatch::decode_batch(&mut cur)?;
        let consumed = rest.len() - cur.len();
        let batch_bytes = rest
            .get(..consumed)
            .ok_or("batch decode consumed more bytes than the log holds")?;

        // The whole batch must re-encode byte-identically (validates the record
        // batch layer + that record values were read verbatim).
        let mut re = BytesMut::new();
        batch.encode_batch(&mut re)?;
        assert2::assert!(re.as_ref() == batch_bytes);

        // Control batches (LeaderChange / Snapshot*) are not value-enveloped;
        // their byte-identity is covered by the whole-batch assertion above.
        if !batch.attributes.is_control_batch() {
            for rec in &batch.records {
                if let Some(value) = &rec.value {
                    let (decoded, version) = KraftMetadataRecord::decode_value(value)?;
                    let reencoded = decoded.encode_value(version)?;
                    assert2::assert!(reencoded.as_ref() == value.as_ref());
                }
            }
        }
        rest = cur;
    }
    Ok(())
}

fn metadata_log_roundtrips(path: &Path) -> datatest_stable::Result<()> {
    let bytes = fs::read(path)?;
    assert_log_roundtrips(&bytes)
}

/// Resolve a crate-relative fixture directory.
///
/// Cargo runs an integration test with the crate directory as the working
/// directory, so a bare `tests/...` resolves as written. Bazel runs it from an
/// execution root instead and stages the same files under
/// `$TEST_SRCDIR/$TEST_WORKSPACE`, with `FIXTURE_ROOT` naming the package inside
/// that tree. Absent those variables this is the Cargo path unchanged.
fn fixture_root(relative: &str) -> String {
    let (Ok(srcdir), Ok(workspace), Ok(package)) = (
        env::var("TEST_SRCDIR"),
        env::var("TEST_WORKSPACE"),
        env::var("FIXTURE_ROOT"),
    ) else {
        return relative.to_owned();
    };
    format!("{srcdir}/{workspace}/{package}/{relative}")
}

datatest_stable::harness! {
    { test = metadata_log_roundtrips, root = fixture_root("tests/fixtures"), pattern = r"^[^/]+\.bin$" },
}
