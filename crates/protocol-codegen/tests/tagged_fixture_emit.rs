//! The tagged-field fixture emitter, on schemas built in memory.
//!
//! The JVM differential sweep is the real check of `tagged_fixture()` and
//! `tagged_fixture_json(version)`. It needs a JDK, so it runs only in the
//! `jvm differential` CI job. These tests run anywhere, under Cargo and Bazel.
//! They pin the emitted code for one message that uses every scalar type, a
//! nested tag, a common struct and each shape of version check.

use assert2::assert;
use krabka_protocol_codegen::{
    emit::{differential_table, owned_quote, tagged_fixture, wrappers},
    ir::MessageSpec,
    resolve,
    validate::{ValidateError, validate},
};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};

fn spec(value: serde_json::Value) -> MessageSpec {
    serde_json::from_value(value).unwrap()
}

/// Every tagged scalar type, a tag inside an array element, a tagged array of
/// a common struct, and fields whose versions start or end inside the message
/// range.
fn sample() -> MessageSpec {
    spec(serde_json::json!({
        "name": "SampleResponse", "type": "response", "apiKey": 1000,
        "validVersions": "0-3", "flexibleVersions": "1+",
        "fields": [
            {"name": "ThrottleTimeMs", "type": "int32", "versions": "0+"},
            {"name": "ErrorCode", "type": "int16", "versions": "1-2"},
            {"name": "Topics", "type": "[]Topic", "versions": "0+", "fields": [
                {"name": "Name", "type": "string", "versions": "0+"},
                {"name": "Leader", "type": "Leader", "versions": "2+",
                 "taggedVersions": "2+", "tag": 0, "fields": [
                    {"name": "LeaderId", "type": "int32", "versions": "2+"},
                    {"name": "LeaderEpoch", "type": "int32", "versions": "3+", "default": "-1"}
                ]}
            ]},
            {"name": "Endpoints", "type": "[]Endpoint", "versions": "1+",
             "taggedVersions": "1+", "tag": 1},
            {"name": "Epoch", "type": "int64", "versions": "1+",
             "taggedVersions": "1+", "tag": 2, "default": "1"},
            {"name": "Ready", "type": "bool", "versions": "1+",
             "taggedVersions": "1+", "tag": 3, "default": "true"},
            {"name": "ClusterId", "type": "string", "versions": "1+",
             "taggedVersions": "1+", "tag": 4, "nullableVersions": "1+", "default": "null"},
            {"name": "Token", "type": "bytes", "versions": "1+", "taggedVersions": "1+", "tag": 5},
            {"name": "DirectoryId", "type": "uuid", "versions": "2+", "taggedVersions": "2+", "tag": 6},
            {"name": "Ratio", "type": "float64", "versions": "1+", "taggedVersions": "1+", "tag": 7},
            {"name": "Replicas", "type": "[]int32", "versions": "1+", "taggedVersions": "1+", "tag": 8},
            {"name": "Port", "type": "uint16", "versions": "1+", "taggedVersions": "1+", "tag": 9},
            {"name": "Level", "type": "int8", "versions": "1+", "taggedVersions": "1+", "tag": 10},
            {"name": "Weight", "type": "uint32", "versions": "1+", "taggedVersions": "1+", "tag": 11}
        ],
        "commonStructs": [{"name": "Endpoint", "versions": "1+", "fields": [
            {"name": "Host", "type": "string", "versions": "1+"},
            {"name": "Rack", "type": "string", "versions": "1-2",
             "nullableVersions": "1+", "default": "x"}
        ]}]
    }))
}

fn untagged() -> MessageSpec {
    spec(serde_json::json!({
        "name": "PlainRequest", "type": "request", "apiKey": 1001,
        "validVersions": "0-1", "flexibleVersions": "1+",
        "fields": [{"name": "Name", "type": "string", "versions": "0+"}]
    }))
}

fn tokens(source: &str) -> String {
    source.parse::<TokenStream>().unwrap().to_string()
}

/// The items of a generated file whose names satisfy `wanted`, as token text.
fn items(source: &str, wanted: impl Fn(&str) -> bool) -> Vec<String> {
    let file: syn::File = syn::parse_str(source).unwrap();
    file.items
        .into_iter()
        .filter(|item| item_name(item).is_some_and(|name| wanted(&name)))
        .map(|item| item.to_token_stream().to_string())
        .collect()
}

fn item_name(item: &syn::Item) -> Option<String> {
    match item {
        syn::Item::Fn(f) => Some(f.sig.ident.to_string()),
        syn::Item::Const(c) => Some(c.ident.to_string()),
        syn::Item::Struct(s) => Some(s.ident.to_string()),
        _ => None,
    }
}

#[test]
fn fixture_sets_every_tag_and_json_follows_the_schema_versions() {
    let spec = sample();
    let res_map = resolve::resolve_message(&spec).unwrap();
    let emitted = tagged_fixture::emit_tagged_fixture(&spec, &res_map);

    let expected = quote! {
        #[doc = " A message with a non-default value in every tagged field, at every depth, for"]
        #[doc = " the JVM oracle differential sweep. The value is the same at every version."]
        #[must_use]
        pub fn tagged_fixture() -> SampleResponse {
            SampleResponse {
                topics: vec![Topic {
                    leader: Leader { leader_id: 1i32, leader_epoch: 1i32, ..Leader::default() },
                    ..Topic::default()
                }],
                endpoints: vec![super::common::sample_response::endpoint::Endpoint {
                    host: "x".to_string(),
                    rack: Some("y".to_string()),
                    ..super::common::sample_response::endpoint::Endpoint::default()
                }],
                epoch: 2i64,
                ready: false,
                cluster_id: Some("x".to_string()),
                token: ::bytes::Bytes::from_static(b"x"),
                directory_id: crate::primitives::uuid::Uuid([1u8; 16]),
                ratio: 1.5f64,
                replicas: vec![1i32],
                port: 1u16,
                level: 1i8,
                weight: 1u32,
                ..SampleResponse::default()
            }
        }

        #[doc = " The JSON form of `tagged_fixture()` that Kafka's JSON converter reads at"]
        #[doc = " `version`. It holds only the fields that the schema declares at that version."]
        #[must_use]
        pub fn tagged_fixture_json(version: i16) -> ::serde_json::Value {
            let mut m = ::serde_json::Map::new();
            m.insert("throttleTimeMs".to_string(), ::serde_json::json!(0));
            if (1..=2).contains(&version) {
                m.insert("errorCode".to_string(), ::serde_json::json!(0));
            }
            m.insert("topics".to_string(), ::serde_json::Value::Array(vec![{
                let mut m = ::serde_json::Map::new();
                m.insert("name".to_string(), ::serde_json::Value::String(String::new()));
                if version >= 2 {
                    m.insert("leader".to_string(), {
                        let mut m = ::serde_json::Map::new();
                        m.insert("leaderId".to_string(), ::serde_json::json!(1));
                        if version == 3 {
                            m.insert("leaderEpoch".to_string(), ::serde_json::json!(1));
                        }
                        ::serde_json::Value::Object(m)
                    });
                }
                ::serde_json::Value::Object(m)
            }]));
            if version >= 1 {
                m.insert("endpoints".to_string(), ::serde_json::Value::Array(vec![{
                    let mut m = ::serde_json::Map::new();
                    m.insert("host".to_string(), ::serde_json::Value::String("x".to_string()));
                    if version <= 2 {
                        m.insert("rack".to_string(), ::serde_json::Value::String("y".to_string()));
                    }
                    ::serde_json::Value::Object(m)
                }]));
            }
            if version >= 1 { m.insert("epoch".to_string(), ::serde_json::json!(2)); }
            if version >= 1 { m.insert("ready".to_string(), ::serde_json::Value::Bool(false)); }
            if version >= 1 {
                m.insert("clusterId".to_string(), ::serde_json::Value::String("x".to_string()));
            }
            if version >= 1 {
                m.insert("token".to_string(), ::serde_json::Value::String("eA==".to_string()));
            }
            if version >= 2 {
                m.insert(
                    "directoryId".to_string(),
                    ::serde_json::Value::String("AQEBAQEBAQEBAQEBAQEBAQ".to_string())
                );
            }
            if version >= 1 { m.insert("ratio".to_string(), ::serde_json::json!(1.5)); }
            if version >= 1 {
                m.insert("replicas".to_string(), ::serde_json::Value::Array(vec![::serde_json::json!(1)]));
            }
            if version >= 1 { m.insert("port".to_string(), ::serde_json::json!(1)); }
            if version >= 1 { m.insert("level".to_string(), ::serde_json::json!(1)); }
            if version >= 1 { m.insert("weight".to_string(), ::serde_json::json!(1)); }
            ::serde_json::Value::Object(m)
        }
    };

    assert!(tokens(&emitted) == expected.to_string());
}

#[test]
fn only_a_message_with_a_tag_gets_a_fixture() {
    let cases = [(sample(), true), (untagged(), false)];
    for (spec, has_tag) in cases {
        let res_map = resolve::resolve_message(&spec).unwrap();
        let fixture = tagged_fixture::emit_tagged_fixture(&spec, &res_map);
        let owned = owned_quote::emit(&spec, "sha").unwrap().primary;

        // The owned module carries exactly the fixture functions the emitter
        // writes, and nothing when the message has no tag.
        let fixture_fns = |source: &str| items(source, |name| name.starts_with("tagged_fixture"));
        let expected_fns = if has_tag {
            fixture_fns(&fixture)
        } else {
            Vec::new()
        };
        assert!(
            (tagged_fixture::has_tagged_field(&spec), fixture_fns(&owned))
                == (has_tag, expected_fns),
            "{}",
            spec.name
        );
    }
}

#[test]
fn owned_wrapper_round_trips_the_fixture_only_with_a_tag() {
    let tagged_test = quote! {
        #[test]
        fn tagged_fixture_roundtrips_all_versions() {
            for v in MIN_VERSION..=MAX_VERSION {
                roundtrip(&tagged_fixture(), v);
                assert!(tagged_fixture_json(v).is_object());
            }
            let encode = |msg: &SampleResponse| {
                let mut buf = BytesMut::new();
                msg.encode(&mut buf, MAX_VERSION).unwrap();
                buf
            };
            assert!(encode(&tagged_fixture()) != encode(&SampleResponse::default()));
        }
    }
    .to_string();

    let cases = [(sample(), vec![tagged_test]), (untagged(), Vec::new())];
    for (spec, expected) in cases {
        let wrapper = wrappers::emit(&spec, wrappers::Flavor::Owned, "sha", None);
        let file: syn::File = syn::parse_str(&wrapper).unwrap();
        let tests = file
            .items
            .iter()
            .find_map(|item| match item {
                syn::Item::Mod(m) if m.ident == "tests" => m.content.as_ref(),
                _ => None,
            })
            .unwrap();
        let tagged: Vec<String> = tests
            .1
            .iter()
            .filter(|item| {
                item_name(item).as_deref() == Some("tagged_fixture_roundtrips_all_versions")
            })
            .map(|item| item.to_token_stream().to_string())
            .collect();
        assert!(tagged == expected, "{}", spec.name);
    }
}

#[test]
fn differential_table_lists_every_version_of_each_tagged_message() {
    let table = differential_table::emit(&[sample(), untagged()], "sha");

    let expected = [
        quote! {
            pub const TAGGED_CASES: &[TaggedCase] = &[
                TaggedCase { name: "SampleResponse", version: 0 },
                TaggedCase { name: "SampleResponse", version: 1 },
                TaggedCase { name: "SampleResponse", version: 2 },
                TaggedCase { name: "SampleResponse", version: 3 },
            ];
        },
        quote! {
            fn encode_tagged_fixture_0(name: &str, version: i16) -> Option<(Vec<u8>, Vec<u8>)> {
                use krabka_protocol::DecodeBorrow;
                Some(match name {
                    "SampleResponse" => {
                        let owned = krabka_protocol::owned::sample_response::tagged_fixture();
                        let max = krabka_protocol::owned::sample_response::MAX_VERSION;
                        let full = encode_with(&owned, max);
                        let mut cur = full.as_slice();
                        let borrowed =
                            krabka_protocol::borrowed::sample_response::SampleResponse::decode_borrow(&mut cur, max)
                                .unwrap();
                        assert2::assert!(cur.is_empty());
                        (encode_with(&owned, version), encode_with(&borrowed, version))
                    }
                    _ => return None,
                })
            }
        },
        quote! {
            /// # Panics
            ///
            /// Panics when `name` does not identify a message with a tagged field.
            #[must_use]
            pub fn tagged_fixture_json_for(name: &str, version: i16) -> ::serde_json::Value {
                match name {
                    "SampleResponse" => krabka_protocol::owned::sample_response::tagged_fixture_json(version),
                    _ => panic!("unknown message in tagged_fixture_json_for: {name}"),
                }
            }
        },
    ]
    .map(|item| item.to_string());

    let names = [
        "TAGGED_CASES",
        "encode_tagged_fixture_0",
        "tagged_fixture_json_for",
    ];
    let got = items(&table, |name| names.contains(&name));
    assert!(got == expected);
}

/// Kafka's `FieldSpec` rejects both ranges. The borrowed half of the
/// differential sweep relies on it: every tag must be in range at the highest
/// version.
#[test]
fn tagged_versions_follow_kafka_rules() {
    let message = |versions: &str, tagged_versions: &str| {
        spec(serde_json::json!({
            "name": "TaggedRequest", "type": "request", "apiKey": 0,
            "validVersions": "0-5", "flexibleVersions": "0+",
            "fields": [{
                "name": "Value", "type": "int32", "versions": versions,
                "taggedVersions": tagged_versions, "tag": 0
            }]
        }))
    };
    let rejected = |message: &'static str| {
        Err(ValidateError::Unsupported {
            message,
            context: "TaggedRequest.Value".to_string(),
        })
    };
    let cases = [
        ("0+", "3+", Ok(())),
        ("2+", "2-4", rejected("taggedVersions is not open-ended")),
        (
            "2+",
            "1+",
            rejected("taggedVersions is not a subset of versions"),
        ),
        (
            "2-4",
            "2+",
            rejected("taggedVersions is not a subset of versions"),
        ),
    ];
    for (versions, tagged_versions, expected) in cases {
        assert!(
            validate(&[message(versions, tagged_versions)]) == expected,
            "versions {versions}, taggedVersions {tagged_versions}"
        );
    }
}
