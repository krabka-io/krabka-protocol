//! Whole generator runs over a small schema set in a scratch directory.
//!
//! The runs format through `fmt::prettyprint` alone, so they need no rustfmt binary
//! and no checkout, and run under Cargo and Bazel alike.
#![cfg(test)]
mod scratch;

use std::{
    fs,
    path::{Path, PathBuf},
};

use assert2::{assert, check};
use krabka_protocol_codegen::{
    driver::{self, RunError},
    emit::common::banner,
    fmt,
};
use scratch::Scratch;

const REQUEST: &str = r#"// A request with a common struct and a tagged field.
{
  "apiKey": 1000, "type": "request", "name": "WidgetRequest",
  "validVersions": "0-1", "flexibleVersions": "1+",
  "fields": [
    { "name": "Parts", "type": "[]Part", "versions": "0+" },
    { "name": "Note", "type": "string", "versions": "1+", "taggedVersions": "1+",
      "tag": 0, "nullableVersions": "1+", "default": "null" }
  ],
  "commonStructs": [
    { "name": "Part", "versions": "0+", "fields": [
      { "name": "Id", "type": "int32", "versions": "0+" }
    ]}
  ]
}"#;

const RESPONSE: &str = r#"{
  "apiKey": 1000, "type": "response", "name": "WidgetResponse",
  "validVersions": "0-1", "flexibleVersions": "1+",
  "fields": [ { "name": "ErrorCode", "type": "int16", "versions": "0+" } ]
}"#;

/// A schema with no valid version, which the generator skips.
const REMOVED: &str = r#"{
  "apiKey": 1001, "type": "request", "name": "RemovedRequest",
  "validVersions": "none", "flexibleVersions": "none", "fields": []
}"#;

/// The formatter for these runs: the pretty-printing pass, without rustfmt.
#[expect(clippy::unnecessary_wraps, reason = "a `Formatter` returns a `Result`")]
fn pretty(src: &str) -> Result<String, fmt::FmtError> {
    Ok(fmt::prettyprint(src))
}

/// A schema directory in `scratch`, at `crates/protocol/schemas`.
fn schemas(scratch: &Scratch) -> PathBuf {
    scratch.at(
        "crates/protocol/schemas/VERSION",
        "kafka: test\nsha: abc123\n",
    );
    scratch.at("crates/protocol/schemas/WidgetRequest.json", REQUEST);
    scratch.at("crates/protocol/schemas/WidgetResponse.json", RESPONSE);
    scratch.at("crates/protocol/schemas/RemovedRequest.json", REMOVED);
    scratch.path().join("crates/protocol/schemas")
}

/// The paths below `root` of every file in it, sorted.
fn files(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let rel = path.strip_prefix(root).unwrap();
                out.push(rel.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    out.sort();
    out
}

#[test]
fn a_run_writes_the_generated_tree_and_its_wrappers() {
    let scratch = Scratch::new("driver-run");
    let schemas = schemas(&scratch);
    let protocol = scratch.path().join("crates/protocol");
    let out = protocol.join("generated");
    // A stale file an earlier schema set generated, and a hand-written one.
    scratch.at(
        "crates/protocol/generated/GoneRequest.owned.rs",
        &banner("old"),
    );
    scratch.at(
        "crates/protocol/src/owned/gone/hand.rs",
        "//! Hand-written.\n",
    );

    let count = driver::run(&schemas, &out, None, pretty).unwrap();

    check!(
        files(&protocol.join("generated"))
            == [
                "WidgetRequest.borrowed.rs",
                "WidgetRequest.owned.rs",
                "WidgetResponse.borrowed.rs",
                "WidgetResponse.owned.rs",
                "api_key.rs",
                "common/borrowed/widget_request/part.borrowed.rs",
                "common/owned/widget_request/part.owned.rs",
                "differential_table.rs",
            ]
    );
    check!(
        files(&protocol.join("src"))
            == [
                "borrowed/common/mod.rs",
                "borrowed/common/widget_request/mod.rs",
                "borrowed/common/widget_request/part.rs",
                "borrowed/mod.rs",
                "borrowed/widget_request.rs",
                "borrowed/widget_response.rs",
                "owned/common/mod.rs",
                "owned/common/widget_request/mod.rs",
                "owned/common/widget_request/part.rs",
                "owned/gone/hand.rs",
                "owned/mod.rs",
                "owned/widget_request.rs",
                "owned/widget_response.rs",
            ]
    );
    check!(count == 20);

    let owned = fs::read_to_string(out.join("WidgetRequest.owned.rs")).unwrap();
    check!(owned.starts_with(&banner("abc123")));
    let wrapper =
        fs::read_to_string(protocol.join("src/owned/common/widget_request/part.rs")).unwrap();
    check!(wrapper.contains("\"/generated/common/owned/widget_request/part.owned.rs\""));
    // `mod.rs` is written as token text, without formatting.
    let mod_rs: String = fs::read_to_string(protocol.join("src/owned/mod.rs"))
        .unwrap()
        .split_whitespace()
        .collect();
    check!(mod_rs.contains("pubmodcommon;"));
    check!(mod_rs.contains("pubmodwidget_request;"));
    check!(!mod_rs.contains("removed_request"));
}

#[test]
fn a_namespaced_run_writes_below_its_namespace() {
    let scratch = Scratch::new("driver-ns");
    let schemas = schemas(&scratch);
    let protocol = scratch.path().join("crates/protocol");
    let out = protocol.join("generated/kafka_3_6_2");

    driver::run(&schemas, &out, Some("kafka_3_6_2"), pretty).unwrap();

    let generated = files(&out);
    check!(generated.contains(&"WidgetRequest.owned.rs".to_owned()));
    check!(!generated.contains(&"api_key.rs".to_owned()));
    check!(!generated.contains(&"differential_table.rs".to_owned()));
    let src = protocol.join("src/kafka_3_6_2");
    check!(
        fs::read_to_string(src.join("mod.rs")).unwrap() == "pub mod borrowed;\npub mod owned;\n"
    );
    let wrapper = fs::read_to_string(src.join("owned/common/widget_request/part.rs")).unwrap();
    check!(
        wrapper.contains("\"/generated/kafka_3_6_2/common/owned/widget_request/part.owned.rs\"")
    );
    let borrowed = fs::read_to_string(out.join("WidgetRequest.borrowed.rs")).unwrap();
    check!(borrowed.contains("crate::kafka_3_6_2::owned"));
}

#[test]
fn a_schema_directory_without_a_sha_is_rejected() {
    let scratch = Scratch::new("driver-sha");
    let schemas = schemas(&scratch);
    fs::write(schemas.join("VERSION"), "kafka: test\n").unwrap();
    let out = scratch.path().join("crates/protocol/generated");

    let result = driver::run(&schemas, &out, None, pretty);

    assert!(let Err(RunError::MissingSha) = result);
    check!(!out.exists());
}

#[test]
fn a_formatter_error_stops_the_run() {
    let scratch = Scratch::new("driver-fmt");
    let schemas = schemas(&scratch);
    let out = scratch.path().join("crates/protocol/generated");

    let result = driver::run(&schemas, &out, None, |_| {
        Err(fmt::FmtError::Rejected {
            status: "1".to_owned(),
            stderr: "no".to_owned(),
        })
    });

    assert!(let Err(RunError::Write(_)) = result);
}
