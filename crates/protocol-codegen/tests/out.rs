//! The output ledger's write and prune, on scratch directories.
//!
//! These tests touch only a private temporary directory and never call
//! rustfmt, so they run under Cargo and Bazel alike.
#![cfg(test)]
mod scratch;

use std::path::PathBuf;

use assert2::assert;
use krabka_protocol_codegen::{emit::common::banner, out::Written};
use scratch::Scratch;

/// Write a file that opens with the generated-file banner.
fn generated(scratch: &Scratch, rel: &str) -> PathBuf {
    scratch.at(rel, &format!("{}pub const X: i16 = 0;\n", banner("test")))
}

#[test]
fn prune_removes_a_generated_file_the_run_did_not_write() {
    let scratch = Scratch::new("out-stale");
    let stale = generated(&scratch, "Removed.owned.rs");
    let fresh = scratch.path().join("Kept.owned.rs");

    let mut written = Written::default();
    written
        .write(fresh.clone(), &format!("{}\n", banner("test")))
        .unwrap();
    written.prune(scratch.path(), false).unwrap();

    assert!(!stale.exists());
    assert!(fresh.exists());
    assert!(written.contains(&fresh));
}

#[test]
fn prune_keeps_a_hand_written_neighbour() {
    let scratch = Scratch::new("out-handwritten");
    let hand = scratch.at("hand_written.rs", "//! Hand-written.\n");

    Written::default().prune(scratch.path(), false).unwrap();

    assert!(hand.exists());
}

#[test]
fn a_flat_prune_leaves_subdirectories_alone() {
    let scratch = Scratch::new("out-flat");
    let nested = generated(&scratch, "kafka_3_6_2/Removed.owned.rs");

    Written::default().prune(scratch.path(), false).unwrap();

    assert!(nested.exists());
}

#[test]
fn a_recursive_prune_drops_the_directory_it_empties() {
    let scratch = Scratch::new("out-recursive");
    let nested = generated(&scratch, "common/owned/gone/key_value.owned.rs");
    let kept = scratch.at("common/owned/kept/hand.rs", "//! Hand-written.\n");

    Written::default().prune(scratch.path(), true).unwrap();

    assert!(!nested.exists());
    assert!(!scratch.path().join("common/owned/gone").exists());
    assert!(kept.exists());
}

#[test]
fn prune_of_a_missing_directory_does_nothing() {
    let scratch = Scratch::new("out-missing");
    let missing = scratch.path().join("missing");

    Written::default().prune(&missing, true).unwrap();

    assert!(!missing.exists());
}

#[test]
fn write_into_a_missing_directory_fails_and_records_nothing() {
    let scratch = Scratch::new("out-unwritable");
    let path = scratch.path().join("missing/File.rs");

    let mut written = Written::default();
    assert!(written.write(path.clone(), "x").is_err());
    assert!(!written.contains(&path));
}
