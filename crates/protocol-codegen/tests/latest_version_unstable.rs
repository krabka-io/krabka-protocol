//! `latestVersionUnstable`, on schemas built in memory.
//!
//! The curated snapshots cover the vendored `InitProducerIdRequest`. Those
//! tests read the schema directory and shell out to rustfmt, so they run only
//! under Cargo. These tests run anywhere, under Cargo and Bazel.

use assert2::assert;
use krabka_protocol_codegen::{
    emit::{borrowed_quote, owned_quote},
    ir::MessageSpec,
    validate::{ValidateError, validate},
};

fn spec(message_type: &str, valid_versions: &str, unstable: bool) -> MessageSpec {
    let suffix = match message_type {
        "request" => "Request",
        "response" => "Response",
        "header" => "Header",
        _ => "Data",
    };
    serde_json::from_value(serde_json::json!({
        "name": format!("Sample{suffix}"),
        "type": message_type,
        "apiKey": 1000,
        "validVersions": valid_versions,
        "flexibleVersions": "2+",
        "latestVersionUnstable": unstable,
        "fields": [
            {"name": "TransactionalId", "type": "string", "versions": "0+",
             "nullableVersions": "0+"}
        ]
    }))
    .unwrap()
}

#[test]
fn missing_key_reads_as_stable() {
    let spec: MessageSpec = serde_json::from_value(serde_json::json!({
        "name": "SampleRequest",
        "type": "request",
        "apiKey": 1000,
        "validVersions": "0-6",
    }))
    .unwrap();
    assert!(!spec.latest_version_unstable);
    assert!(spec.latest_stable_version() == 6);
}

#[test]
fn latest_stable_version() {
    for (valid_versions, unstable, want) in [
        ("0-6", true, 5),
        ("0-6", false, 6),
        ("3", true, 2),
        ("3", false, 3),
    ] {
        let got = spec("request", valid_versions, unstable).latest_stable_version();
        assert!(
            got == want,
            "validVersions {valid_versions}, unstable {unstable}"
        );
    }
}

fn unsupported(message: &'static str, context: &str) -> Result<(), ValidateError> {
    Err(ValidateError::Unsupported {
        message,
        context: context.to_owned(),
    })
}

#[test]
fn validator_rules() {
    const NOT_REQUEST: &str = "latestVersionUnstable on a message that is not a request";
    let cases = [
        ("request", "0-6", true, Ok(())),
        ("request", "0-6", false, Ok(())),
        ("request", "0+", false, Ok(())),
        // Kafka accepts a request whose only version is unstable. The API then
        // has no enabled version.
        ("request", "0", true, Ok(())),
        ("request", "none", false, Ok(())),
        (
            "request",
            "none",
            true,
            unsupported(
                "latestVersionUnstable with empty validVersions",
                "SampleRequest",
            ),
        ),
        (
            "request",
            "0+",
            true,
            unsupported(
                "latestVersionUnstable with open-ended validVersions",
                "SampleRequest",
            ),
        ),
        (
            "response",
            "0-6",
            true,
            unsupported(NOT_REQUEST, "SampleResponse"),
        ),
        (
            "header",
            "0-2",
            true,
            unsupported(NOT_REQUEST, "SampleHeader"),
        ),
        ("data", "0", true, unsupported(NOT_REQUEST, "SampleData")),
        ("response", "0-6", false, Ok(())),
    ];
    for (message_type, valid_versions, unstable, want) in cases {
        let got = validate(&[spec(message_type, valid_versions, unstable)]);
        assert!(
            got == want,
            "type {message_type}, validVersions {valid_versions}, unstable {unstable}"
        );
    }
}

/// The `LATEST_STABLE_VERSION` items in one emitted file.
#[derive(Debug, PartialEq, Eq)]
struct LatestStable {
    /// The value of the module-level `pub const LATEST_STABLE_VERSION`.
    module_const: Option<i16>,
    /// The right-hand side of `const LATEST_STABLE_VERSION` in the
    /// `impl crate::ProtocolRequest` block.
    trait_const: Option<String>,
}

fn latest_stable(source: &str) -> LatestStable {
    let file = syn::parse_file(source).unwrap();
    let mut found = LatestStable {
        module_const: None,
        trait_const: None,
    };
    for item in &file.items {
        match item {
            syn::Item::Const(item) if item.ident == "LATEST_STABLE_VERSION" => {
                let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(value),
                    ..
                }) = item.expr.as_ref()
                else {
                    panic!("LATEST_STABLE_VERSION is not an integer literal");
                };
                found.module_const = Some(value.base10_parse().unwrap());
            }
            syn::Item::Impl(item)
                if item.trait_.as_ref().is_some_and(|(path, _)| {
                    path.segments.last().unwrap().ident == "ProtocolRequest"
                }) =>
            {
                for impl_item in &item.items {
                    if let syn::ImplItem::Const(c) = impl_item
                        && c.ident == "LATEST_STABLE_VERSION"
                    {
                        let expr = &c.expr;
                        found.trait_const = Some(quote::quote!(#expr).to_string());
                    }
                }
            }
            _ => {}
        }
    }
    found
}

#[test]
fn emitted_constants() {
    let cases = [
        (
            spec("request", "0-6", true),
            LatestStable {
                module_const: Some(5),
                trait_const: Some("LATEST_STABLE_VERSION".to_owned()),
            },
            LatestStable {
                module_const: Some(5),
                trait_const: None,
            },
        ),
        (
            spec("request", "0-6", false),
            LatestStable {
                module_const: Some(6),
                trait_const: Some("LATEST_STABLE_VERSION".to_owned()),
            },
            LatestStable {
                module_const: Some(6),
                trait_const: None,
            },
        ),
        (
            spec("response", "0-6", false),
            LatestStable {
                module_const: None,
                trait_const: None,
            },
            LatestStable {
                module_const: None,
                trait_const: None,
            },
        ),
    ];
    for (spec, want_owned, want_borrowed) in cases {
        let owned = owned_quote::emit(&spec, "test").unwrap();
        let borrowed = borrowed_quote::emit(&spec, "test", None).unwrap();
        assert!(latest_stable(&owned.primary) == want_owned, "{}", spec.name);
        assert!(
            latest_stable(&borrowed.primary) == want_borrowed,
            "{}",
            spec.name
        );
    }
}
