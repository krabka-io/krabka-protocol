//! Every emitter, on schemas built in memory that hold the field shapes the
//! vendored Kafka schemas use rarely or not at all.
//!
//! The snapshot and parity suites cover the real schemas, but they read them
//! through `CARGO_MANIFEST_DIR`, so they run only under Cargo. These tests read
//! nothing from disk and need no rustfmt, so they run under Bazel as well.
//! Each one checks that the emitter succeeds and that its output parses as
//! Rust, then checks the code that the shape is meant to produce.
#![cfg(test)]
// The kitchen-sink `json!` literal nests deeper than the default limit.
#![recursion_limit = "256"]
use assert2::{assert, check};
use krabka_protocol_codegen::{
    emit::{
        api_key_enum_quote, borrowed_quote, default_json, differential_table, mod_rs,
        owned::EmitError,
        owned_quote, protocol_request, tagged_fixture,
        wrappers::{self, Flavor},
    },
    ir::MessageSpec,
    resolve::{self, ResolveError},
};
use serde_json::{Value, json};

const SHA: &str = "testsha";

fn spec(value: Value) -> MessageSpec {
    serde_json::from_value(value).unwrap()
}

/// A flexible request with a field of every shape: nullable in only some of
/// its versions, nullable in none, arrays of every element kind, nested,
/// common and fallible structs, and tagged fields with every kind of default.
fn kitchen_sink() -> MessageSpec {
    spec(json!({
        "name": "KitchenSinkRequest", "type": "request", "apiKey": 1001,
        "validVersions": "0-5", "flexibleVersions": "2+",
        "fields": [
            {"name": "Name", "type": "string", "versions": "0+", "nullableVersions": "3+"},
            {"name": "Data", "type": "bytes", "versions": "0+", "nullableVersions": "0-3"},
            {"name": "Never", "type": "string", "versions": "0+", "nullableVersions": "none"},
            {"name": "Only", "type": "int32", "versions": "3"},
            {"name": "Middle", "type": "int32", "versions": "1-3"},
            {"name": "Until", "type": "int32", "versions": "0-3"},
            {"name": "Ratio", "type": "float64", "versions": "0+"},
            {"name": "Id", "type": "uuid", "versions": "0+"},
            {"name": "Ids", "type": "[]int32", "versions": "0+", "nullableVersions": "1+"},
            {"name": "Blobs", "type": "[]bytes", "versions": "0+"},
            {"name": "NullBlobs", "type": "[]bytes", "versions": "0+", "nullableVersions": "0+"},
            {"name": "SplitBlobs", "type": "[]bytes", "versions": "0+", "nullableVersions": "4+"},
            {"name": "Strs", "type": "[]string", "versions": "0+", "nullableVersions": "0+"},
            {"name": "Names", "type": "[]string", "versions": "0+"},
            {"name": "TopRecs", "type": "records", "versions": "0+"},
            {"name": "RecArr", "type": "[]records", "versions": "0+"},
            {"name": "NullRecArr", "type": "[]records", "versions": "0+",
             "nullableVersions": "0+"},
            {"name": "Plain", "type": "[]Plain", "versions": "0+", "fields": [
                {"name": "P", "type": "int16", "versions": "0+"},
                {"name": "Pn", "type": "string", "versions": "0+", "nullableVersions": "2+"}
            ]},
            {"name": "NullPlain", "type": "[]NullPlain", "versions": "0+",
             "nullableVersions": "0+", "fields": [
                {"name": "Q", "type": "int16", "versions": "0+"}
            ]},
            {"name": "Child", "type": "Child", "versions": "0+", "nullableVersions": "4+",
             "fields": [
                {"name": "Recs", "type": "records", "versions": "0+"},
                {"name": "MaybeRecs", "type": "records", "versions": "0+",
                 "nullableVersions": "1+"}
            ]},
            {"name": "Kids", "type": "[]Kid", "versions": "0+", "nullableVersions": "0+",
             "fields": [
                {"name": "Recs", "type": "records", "versions": "0+", "nullableVersions": "0+"}
            ]},
            {"name": "Bunch", "type": "[]Bunch", "versions": "0+", "fields": [
                {"name": "Recs", "type": "records", "versions": "0+"}
            ]},
            {"name": "Shared", "type": "Shared", "versions": "0+"},
            {"name": "Leaves", "type": "[]Leaf", "versions": "0+"},
            {"name": "BoolTrue", "type": "bool", "versions": "2+", "taggedVersions": "2+",
             "tag": 0, "default": true},
            {"name": "BoolTrueStr", "type": "bool", "versions": "2+", "taggedVersions": "2+",
             "tag": 1, "default": "true"},
            {"name": "BoolFalse", "type": "bool", "versions": "2+", "taggedVersions": "2+",
             "tag": 2, "default": false},
            {"name": "BoolFalseStr", "type": "bool", "versions": "2+", "taggedVersions": "2+",
             "tag": 3, "default": "false"},
            {"name": "BoolPlain", "type": "bool", "versions": "2+", "taggedVersions": "2+",
             "tag": 4},
            {"name": "TagInt", "type": "int32", "versions": "3+", "taggedVersions": "3+",
             "tag": 5, "default": 7},
            {"name": "TagStr", "type": "string", "versions": "2+", "taggedVersions": "2+",
             "tag": 6, "default": "abc"},
            {"name": "TagEmpty", "type": "string", "versions": "2+", "taggedVersions": "2+",
             "tag": 7, "default": ""},
            {"name": "TagUuid", "type": "uuid", "versions": "2+", "taggedVersions": "2+",
             "tag": 8},
            {"name": "TagFloat", "type": "float64", "versions": "2+", "taggedVersions": "2+",
             "tag": 9, "default": 1.5},
            {"name": "TagBytes", "type": "bytes", "versions": "2-4", "taggedVersions": "2-4",
             "tag": 10, "nullableVersions": "2+"},
            {"name": "TagStruct", "type": "TagStruct", "versions": "2+", "taggedVersions": "2+",
             "tag": 11, "fields": [
                {"name": "Inner", "type": "int8", "versions": "0+"}
            ]},
            {"name": "TagSplit", "type": "string", "versions": "2+", "taggedVersions": "2+",
             "tag": 12, "nullableVersions": "4+"}
        ],
        "commonStructs": [
            {"name": "Shared", "versions": "0+", "fields": [
                {"name": "Leaf", "type": "Leaf", "versions": "0+"},
                {"name": "MaybeLeaf", "type": "Leaf", "versions": "0+",
                 "nullableVersions": "0+"},
                {"name": "SharedTag", "type": "int32", "versions": "3+",
                 "taggedVersions": "3+", "tag": 0}
            ]},
            {"name": "Leaf", "versions": "0+", "fields": [
                {"name": "L", "type": "uint16", "versions": "0+"},
                {"name": "Recs", "type": "records", "versions": "0+"}
            ]}
        ]
    }))
}

/// A response with no flexible version, so its common structs are never
/// flexible.
fn legacy() -> MessageSpec {
    spec(json!({
        "name": "LegacyResponse", "type": "response", "apiKey": 1001,
        "validVersions": "0-1", "flexibleVersions": "none",
        "fields": [
            {"name": "Common", "type": "Common", "versions": "0+"}
        ],
        "commonStructs": [
            {"name": "Common", "versions": "0+", "fields": [
                {"name": "C", "type": "int32", "versions": "0+"},
                {"name": "S", "type": "string", "versions": "0+"}
            ]}
        ]
    }))
}

/// A message flexible at every version, from `i16::MIN`.
fn always_flexible() -> MessageSpec {
    spec(json!({
        "name": "AlwaysFlexibleRequest", "type": "request", "apiKey": 1002,
        "validVersions": "0-1", "flexibleVersions": "-32768+",
        "fields": [
            {"name": "Common", "type": "Common", "versions": "0+"},
            {"name": "Everywhere", "type": "int32", "versions": "-32768+"}
        ],
        "commonStructs": [
            {"name": "Common", "versions": "0+", "fields": [
                {"name": "C", "type": "int32", "versions": "0+"},
                {"name": "T", "type": "int32", "versions": "0+", "taggedVersions": "0+",
                 "tag": 0}
            ]}
        ]
    }))
}

/// A message with no fields at all.
fn empty() -> MessageSpec {
    spec(json!({
        "name": "EmptyResponse", "type": "response", "apiKey": 1002,
        "validVersions": "0", "flexibleVersions": "0+", "fields": []
    }))
}

fn all_specs() -> Vec<MessageSpec> {
    vec![kitchen_sink(), legacy(), always_flexible(), empty()]
}

/// Parse `src` as a Rust file, failing the test with `what` on an error.
fn parses(what: &str, src: &str) {
    let parsed = syn::parse_file(src);
    check!(parsed.is_ok(), "{what} is not valid Rust: {src}");
}

/// `src` with every run of whitespace removed, so a check does not depend on
/// how the emitter spaced its tokens.
fn squeezed(src: &str) -> String {
    src.split_whitespace().collect()
}

#[test]
fn owned_and_borrowed_emitters_produce_valid_rust_for_every_shape() {
    for spec in all_specs() {
        let owned = owned_quote::emit(&spec, SHA).unwrap();
        parses(&spec.name, &owned.primary);
        for (stem, body) in &owned.commons {
            parses(stem, body);
        }
        for namespace in [None, Some("kafka_3_6_2")] {
            let borrowed = borrowed_quote::emit(&spec, SHA, namespace).unwrap();
            parses(&spec.name, &borrowed.primary);
            for (stem, body) in &borrowed.commons {
                parses(stem, body);
            }
        }
    }
}

#[test]
fn nullable_splits_switch_codec_per_version() {
    let owned = squeezed(&owned_quote::emit(&kitchen_sink(), SHA).unwrap().primary);
    // `Name` is nullable from v3, `Data` up to v3, `Never` in no version.
    check!(owned.contains("version>=3"));
    check!(owned.contains("version<=3"));
    check!(owned.contains("version==i16::MAX"));
    check!(owned.contains("version==i16::MIN"));
    let borrowed = squeezed(
        &borrowed_quote::emit(&kitchen_sink(), SHA, None)
            .unwrap()
            .primary,
    );
    check!(borrowed.contains("version==i16::MAX"));
    check!(borrowed.contains("version==i16::MIN"));
}

#[test]
fn records_below_a_struct_make_its_to_owned_fallible() {
    let borrowed = borrowed_quote::emit(&kitchen_sink(), SHA, None).unwrap();
    let primary = squeezed(&borrowed.primary);
    check!(primary.contains(".collect::<Result<_,_>>()"));
    check!(primary.contains(".transpose()?"));
    check!(primary.contains("RecordsPayloadBorrowed::to_owned"));
    check!(primary.contains("Bytes::copy_from_slice"));
    // A common struct converts to its own owned type, and a nullable
    // reference to a sibling common struct converts through the sibling.
    assert!(let Some((_, shared)) = borrowed
            .commons
            .iter()
            .find(|(stem, _)| stem.ends_with("shared"))
    );
    let shared = squeezed(shared);
    check!(shared.contains("crate::owned::common::kitchen_sink_request::shared::Shared"));
    check!(shared.contains(".map(super::leaf::Leaf::to_owned).transpose()?"));
    let namespaced = borrowed_quote::emit(&kitchen_sink(), SHA, Some("kafka_3_6_2")).unwrap();
    check!(squeezed(&namespaced.primary).contains("crate::kafka_3_6_2::owned"));
}

#[test]
fn tagged_bool_defaults_set_the_encode_condition() {
    for out in [
        owned_quote::emit(&kitchen_sink(), SHA).unwrap().primary,
        borrowed_quote::emit(&kitchen_sink(), SHA, None)
            .unwrap()
            .primary,
    ] {
        let out = squeezed(&out);
        check!(out.contains("!self.bool_true"));
        check!(out.contains("!self.bool_true_str"));
        check!(out.contains("ifself.bool_false{"));
        check!(out.contains("ifself.bool_false_str{"));
        // A tag that starts above the flexible threshold is version-gated.
        check!(out.contains("version>=3&&"));
    }
}

#[test]
fn common_structs_take_the_flexible_test_of_their_message() {
    let legacy = owned_quote::emit(&legacy(), SHA).unwrap();
    check!(
        legacy
            .commons
            .iter()
            .any(|(_, body)| squeezed(body).contains("letflex=version==i16::MAX;"))
    );
    let always = owned_quote::emit(&always_flexible(), SHA).unwrap();
    assert!(let Some((_, common)) = always.commons.first());
    check!(squeezed(common).contains("letflex=true;"));
    let borrowed = borrowed_quote::emit(&always_flexible(), SHA, None).unwrap();
    assert!(let Some((_, common)) = borrowed.commons.first());
    check!(squeezed(common).contains("letflex=true;"));
}

#[test]
fn default_json_covers_split_nullability_and_string_defaults() {
    let out = default_json::emit_default_json(&kitchen_sink()).unwrap();
    parses("default json", &out);
    let out = squeezed(&out);
    check!(out.contains("::serde_json::Value::Array(vec![])"));
    check!(out.contains("::serde_json::Value::String(String::new())"));
    check!(out.contains(r#"::serde_json::Value::String("abc".to_owned())"#));
    check!(out.contains("version==3"));
    check!(out.contains("(1..=3).contains(&version)"));
    check!(out.contains("version<=3"));

    let empty = default_json::emit_default_json(&empty()).unwrap();
    parses("empty default json", &empty);
    check!(squeezed(&empty).contains("_version"));
    check!(!squeezed(&empty).contains("letmut"));
}

#[test]
fn tagged_fixture_fills_every_tag() {
    let spec = kitchen_sink();
    let res_map = resolve::resolve_message(&spec).unwrap();
    let out = tagged_fixture::emit_tagged_fixture(&spec, &res_map).unwrap();
    parses("tagged fixture", &out);
    check!(squeezed(&out).contains("Uuid([1u8;16])"));

    let always = always_flexible();
    let res_map = resolve::resolve_message(&always).unwrap();
    let out = tagged_fixture::emit_tagged_fixture(&always, &res_map).unwrap();
    parses("always-flexible tagged fixture", &out);
    // Every tag holds at every version, so the fixture never reads it.
    check!(squeezed(&out).contains("_version"));

    let res_map = resolve::resolve_message(&empty()).unwrap();
    check!(tagged_fixture::emit_tagged_fixture(&empty(), &res_map).unwrap() == "");
}

#[test]
fn tagged_fixture_rejects_a_tag_type_with_no_non_default_value() {
    let spec = spec(json!({
        "name": "OddRequest", "type": "request", "apiKey": 1003,
        "validVersions": "0", "flexibleVersions": "0+",
        "fields": [
            {"name": "Odd", "type": "uint64", "versions": "0+", "taggedVersions": "0+",
             "tag": 0}
        ]
    }));
    let res_map = resolve::resolve_message(&spec).unwrap();
    assert!(let Err(EmitError::Unsupported(message)) = tagged_fixture::emit_tagged_fixture(&spec, &res_map)
    );
    check!(message.contains("uint64"));
}

#[test]
fn an_unknown_struct_type_fails_to_resolve() {
    let spec = spec(json!({
        "name": "BrokenRequest", "type": "request", "apiKey": 1004,
        "validVersions": "0", "flexibleVersions": "0+",
        "fields": [{"name": "Gone", "type": "Missing", "versions": "0+"}]
    }));
    assert!(let Err(ResolveError::Unknown { message, type_name }) = resolve::resolve_message(&spec)
    );
    check!(message == "BrokenRequest");
    check!(type_name == "Missing");
    check!(owned_quote::emit(&spec, SHA).is_err());
    check!(borrowed_quote::emit(&spec, SHA, None).is_err());
}

#[test]
fn wrappers_pick_the_to_owned_shape_and_namespace() {
    for spec in all_specs() {
        for flavor in [Flavor::Owned, Flavor::Borrowed] {
            for namespace in [None, Some("kafka_3_6_2")] {
                let out = wrappers::emit(&spec, flavor, SHA, namespace).unwrap();
                parses(&spec.name, &out);
                let path = format!(".{}.rs", flavor.dir());
                check!(out.contains(&path));
            }
        }
    }
    let fallible = wrappers::emit(&kitchen_sink(), Flavor::Borrowed, SHA, None).unwrap();
    check!(squeezed(&fallible).contains("decoded.to_owned().unwrap()"));
    let infallible = wrappers::emit(&legacy(), Flavor::Borrowed, SHA, None).unwrap();
    check!(!squeezed(&infallible).contains("decoded.to_owned().unwrap()"));
    let namespaced = wrappers::emit(&legacy(), Flavor::Owned, SHA, Some("kafka_3_6_2")).unwrap();
    check!(namespaced.contains("/generated/kafka_3_6_2/LegacyResponse.owned.rs"));
}

#[test]
fn registry_emitters_list_every_message() {
    let specs = all_specs();
    let api_keys = api_key_enum_quote::emit(&specs, SHA).unwrap();
    parses("api key enum", &api_keys);
    let api_keys = squeezed(&api_keys);
    check!(api_keys.contains("Self::KitchenSink,"));
    check!(api_keys.contains("1001=>Some(Self::KitchenSink)"));

    let refs: Vec<&MessageSpec> = specs.iter().collect();
    for flavor in [Flavor::Owned, Flavor::Borrowed] {
        for has_common in [false, true] {
            let out = mod_rs::emit(&refs, flavor, SHA, has_common).unwrap();
            parses("mod.rs", &out);
            let out = squeezed(&out);
            check!(out.contains("pubmodcommon;") == has_common);
            check!(out.contains("pubmodkitchen_sink_request;"));
        }
    }

    let mut with_data = specs;
    with_data.push(spec(json!({
        "name": "SnapshotData", "type": "data",
        "validVersions": "0", "flexibleVersions": "0+", "fields": []
    })));
    let table = differential_table::emit(&with_data, SHA).unwrap();
    parses("differential table", &table);
    check!(!table.contains("SnapshotData"));
    check!(table.contains("KitchenSinkRequest"));
}

#[test]
fn protocol_request_binds_a_request_to_its_response() {
    for spec in all_specs() {
        if let Some(out) = protocol_request::emit_protocol_request(&spec).unwrap() {
            parses(&spec.name, &out);
        }
    }
}
