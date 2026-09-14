//! Parameterised differential sweeps against the JVM oracle.
//!
//! `every_pair_byte_equal` covers every active `(api_key, version)` pair in the
//! generated `CASES` table. It encodes the Rust default fixture, sends the
//! equivalent JSON to the oracle, and asserts byte equality.
//!
//! A default fixture cannot find a KIP-482 tag written outside its declared
//! versions, because the encoders skip a tag that holds its default value.
//! `every_tagged_field_byte_equal` closes that gap. It covers every message in
//! `TAGGED_CASES` at every valid version, with a non-default value in every
//! tagged field at every depth. That includes the versions where a tag is out
//! of range and must not appear on the wire. It checks the owned and the
//! borrowed encoders.
//!
//! Each sweep collects all failures and reports them at the end, so a single
//! run reveals every divergence and not only the first one.

mod support;
use serde_json::json;
use support::oracle;

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/differential_table.rs"
));

#[test]
#[ignore = "requires JVM oracle"]
fn every_pair_byte_equal() {
    let mut o = oracle::shared();
    let mut failures: Vec<String> = Vec::new();
    for case in CASES {
        if !oracle_supports(case.name, case.version) {
            continue;
        }
        let rust_bytes = encode_default(case.name, case.version);
        let jval = default_json_for(case.name, case.version);

        let req = match case.kind {
            Kind::Request => json!({
                "op": "encode",
                "apiKey": case.api_key,
                "messageName": case.name,
                "version": case.version,
                "isRequest": true,
                "value": jval,
            }),
            Kind::Response => json!({
                "op": "encode",
                "apiKey": case.api_key,
                "messageName": case.name,
                "version": case.version,
                "isRequest": false,
                "value": jval,
            }),
            Kind::RequestHeader => json!({
                "op": "header_encode",
                "kind": "request",
                "version": case.version,
                "value": jval,
            }),
            Kind::ResponseHeader => json!({
                "op": "header_encode",
                "kind": "response",
                "version": case.version,
                "value": jval,
            }),
        };

        let result = o.try_call(&req);
        match result {
            Err(e) => {
                failures.push(format!(
                    "{}[{}] v{}: ORACLE_ERROR: {}",
                    case.name,
                    kind_str(case.kind),
                    case.version,
                    e,
                ));
            }
            Ok(resp) => {
                let jvm_bytes = hex::decode(resp["hex"].as_str().unwrap()).unwrap();
                if rust_bytes != jvm_bytes {
                    failures.push(mismatch(
                        &format!("{}[{}] v{}", case.name, kind_str(case.kind), case.version),
                        &rust_bytes,
                        &jvm_bytes,
                    ));
                }
            }
        }
    }
    assert2::assert!(failures == Vec::<String>::new());
}

#[test]
#[ignore = "requires JVM oracle"]
fn every_tagged_field_byte_equal() {
    let mut o = oracle::shared();
    let mut failures: Vec<String> = Vec::new();
    for case in TAGGED_CASES {
        if !oracle_supports(case.name, case.version) {
            continue;
        }
        let label = format!("{}[tagged] v{}", case.name, case.version);
        let (owned, borrowed) = encode_tagged_fixture(case.name, case.version);
        let req = json!({
            "op": "encode",
            "messageName": case.name,
            "version": case.version,
            "isRequest": case.name.ends_with("Request"),
            "value": tagged_fixture_json_for(case.name, case.version),
        });
        match o.try_call(&req) {
            Err(e) => failures.push(format!("{label}: ORACLE_ERROR: {e}")),
            Ok(resp) => {
                let jvm_bytes = hex::decode(resp["hex"].as_str().unwrap()).unwrap();
                for (flavor, rust_bytes) in [("owned", &owned), ("borrowed", &borrowed)] {
                    if *rust_bytes != jvm_bytes {
                        failures.push(mismatch(
                            &format!("{label} {flavor}"),
                            rust_bytes,
                            &jvm_bytes,
                        ));
                    }
                }
            }
        }
    }
    assert2::assert!(failures == Vec::<String>::new());
}

fn mismatch(label: &str, rust_bytes: &[u8], jvm_bytes: &[u8]) -> String {
    format!(
        "{label}: rust={} ({} bytes), jvm={} ({} bytes), first_diff_at={}",
        hex::encode(rust_bytes),
        rust_bytes.len(),
        hex::encode(jvm_bytes),
        jvm_bytes.len(),
        first_diff(rust_bytes, jvm_bytes),
    )
}

fn oracle_supports(name: &str, version: i16) -> bool {
    // The pinned Kafka 4.3.0 client predates KIP-1242, so it has no v5 of
    // ApiVersions. It silently encodes a v5 request as v4, and it cannot size a
    // v5 response that holds supported features. The Rust v5 codecs remain
    // covered by their owned roundtrip tests.
    !(matches!(name, "ApiVersionsRequest" | "ApiVersionsResponse") && version == 5)
}

#[test]
fn kafka_430_oracle_excludes_kip1242_api_versions() {
    let cases = [
        ("ApiVersionsRequest", 4, true),
        ("ApiVersionsRequest", 5, false),
        ("ApiVersionsResponse", 4, true),
        ("ApiVersionsResponse", 5, false),
        ("FetchRequest", 5, true),
    ];
    for (name, version, supported) in cases {
        assert2::assert!(
            oracle_supports(name, version) == supported,
            "{name} v{version}"
        );
    }
}

fn kind_str(k: Kind) -> &'static str {
    match k {
        Kind::Request => "req",
        Kind::Response => "resp",
        Kind::RequestHeader => "rhdr",
        Kind::ResponseHeader => "shdr",
    }
}

fn first_diff(a: &[u8], b: &[u8]) -> usize {
    let min = a.len().min(b.len());
    for i in 0..min {
        if a[i] != b[i] {
            return i;
        }
    }
    min
}
