//! The version bounds that `ProtocolRequest` gives a client.
//!
//! `MAX_VERSION` is the range a codec encodes and decodes. `LATEST_STABLE_VERSION`
//! is the highest version a client can send. They differ only when the schema
//! sets `latestVersionUnstable`, as Kafka's `ApiKeys.latestVersion(false)` does.

use assert2::assert;
use krabka_protocol::{
    ProtocolRequest, kafka_3_6_2,
    krabka::break_glass::ProposeBreakGlassRequest,
    owned::{
        api_versions_request::ApiVersionsRequest, end_txn_request::EndTxnRequest,
        init_producer_id_request::InitProducerIdRequest,
    },
};

#[derive(Debug, PartialEq, Eq)]
struct VersionBounds {
    api_key: i16,
    min: i16,
    max: i16,
    latest_stable: i16,
}

fn bounds<R: ProtocolRequest>() -> VersionBounds {
    VersionBounds {
        api_key: R::API_KEY,
        min: R::MIN_VERSION,
        max: R::MAX_VERSION,
        latest_stable: R::LATEST_STABLE_VERSION,
    }
}

#[test]
fn latest_stable_version_excludes_only_an_unstable_latest_version() {
    let cases = [
        // `"latestVersionUnstable": true` with `"validVersions": "0-6"`. v6 is
        // the KIP-939 version.
        (
            "InitProducerIdRequest",
            bounds::<InitProducerIdRequest>(),
            VersionBounds {
                api_key: 22,
                min: 0,
                max: 6,
                latest_stable: 5,
            },
        ),
        // `"latestVersionUnstable": false`, set explicitly.
        (
            "EndTxnRequest",
            bounds::<EndTxnRequest>(),
            VersionBounds {
                api_key: 26,
                min: 0,
                max: 5,
                latest_stable: 5,
            },
        ),
        // No `latestVersionUnstable` key.
        (
            "ApiVersionsRequest",
            bounds::<ApiVersionsRequest>(),
            VersionBounds {
                api_key: 18,
                min: 0,
                max: 5,
                latest_stable: 5,
            },
        ),
        (
            "kafka_3_6_2 ProduceRequest",
            bounds::<kafka_3_6_2::owned::produce_request::ProduceRequest>(),
            VersionBounds {
                api_key: 0,
                min: 0,
                max: 9,
                latest_stable: 9,
            },
        ),
        // A hand-written krabka RPC.
        (
            "ProposeBreakGlassRequest",
            bounds::<ProposeBreakGlassRequest>(),
            VersionBounds {
                api_key: 1017,
                min: 0,
                max: 0,
                latest_stable: 0,
            },
        ),
    ];
    for (name, got, want) in cases {
        assert!(got == want, "{name}");
    }
}
