//! Loading a schema directory, on scratch directories.
#![cfg(test)]
mod scratch;

use assert2::assert;
use krabka_protocol_codegen::ir::{self, FlexibleVersions, IrError, VersionRange};
use scratch::Scratch;

/// A schema in the vendored style: `//` comments, a `//` inside a string, and
/// an escaped quote before a trailing comment.
const COMMENTED: &str = r#"// Licensed to the Apache Software Foundation.
{
  "apiKey": 1000, // trailing comment
  "type": "request",
  "name": "BRequest",
  "about": "see http://kafka.apache.org and a \"quoted\" word", // tail
  "validVersions": "0-1",
  "flexibleVersions": "none",
  "fields": []
}
"#;

const FLEXIBLE: &str = r#"{
  "type": "response", "name": "AResponse",
  "validVersions": "0-3", "flexibleVersions": "2+", "fields": []
}"#;

#[test]
fn load_dir_reads_every_json_schema_in_name_order() {
    let scratch = Scratch::new("ir-load");
    scratch.at("B.json", COMMENTED);
    scratch.at("A.json", FLEXIBLE);
    scratch.at("README.md", "not a schema");

    let specs = ir::load_dir(scratch.path()).unwrap();

    let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    assert!(names == ["AResponse", "BRequest"]);
    let flexible = &specs[0];
    assert!(matches!(
        flexible.flexible_versions,
        FlexibleVersions::Range(VersionRange {
            min: 2,
            max: i16::MAX
        })
    ));
    assert!(!flexible.flexible_versions.is_flexible(1));
    assert!(flexible.flexible_versions.is_flexible(2));
    let legacy = &specs[1];
    assert!(matches!(legacy.flexible_versions, FlexibleVersions::None));
    assert!(!legacy.flexible_versions.is_flexible(0));
    assert!(legacy.api_key == Some(1000));
}

#[test]
fn load_dir_names_the_file_it_cannot_parse() {
    let scratch = Scratch::new("ir-bad");
    let bad = scratch.at("Bad.json", r#"{ "name": "Bad", "validVersions": "x+" }"#);

    let err = ir::load_dir(scratch.path());

    assert!(let Err(IrError::Parse { file, .. }) = err);
    assert!(file == bad.display().to_string());
}

#[test]
fn load_dir_of_a_missing_directory_is_an_io_error() {
    let scratch = Scratch::new("ir-missing");
    let err = ir::load_dir(&scratch.path().join("missing"));
    assert!(let Err(IrError::Io(_)) = err);
}
