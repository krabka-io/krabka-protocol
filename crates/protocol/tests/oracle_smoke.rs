#![cfg(test)]

mod support;
use serde_json::json;
use support::oracle;

#[test]
#[ignore = "requires JVM oracle built; see CONTRIBUTING"]
fn encode_apiversions_v0_empty() {
    let bytes = oracle::shared().encode(18, 0, true, &json!({}));
    assert2::assert!(bytes.is_empty());
}
