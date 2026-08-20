//! Byte-identity test for captured KIP-595 RPC frames.
//!
//! Each frame is a header and a body from a real
//! `mirror.gcr.io/apache/kafka:4.0.0` 3-node controller quorum. This test
//! decodes each frame through the generated types and re-encodes it. It then
//! asserts that the bytes are unchanged.

use std::path::Path;

use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        begin_quorum_epoch_request::BeginQuorumEpochRequest,
        begin_quorum_epoch_response::BeginQuorumEpochResponse,
        describe_quorum_request::DescribeQuorumRequest,
        describe_quorum_response::DescribeQuorumResponse,
        end_quorum_epoch_request::EndQuorumEpochRequest,
        end_quorum_epoch_response::EndQuorumEpochResponse, fetch_request::FetchRequest,
        fetch_response::FetchResponse, request_header::RequestHeader,
        response_header::ResponseHeader, vote_request::VoteRequest, vote_response::VoteResponse,
    },
};

/// Header version for a flexible message: `RequestHeader` v2, `ResponseHeader` v1.
const FLEX_REQ_HDR: i16 = 2;
const FLEX_RESP_HDR: i16 = 1;

fn roundtrip_request<T>(frame: &[u8], api_version: i16)
where
    T: for<'de> Decode<'de> + Encode,
{
    let mut cur: &[u8] = frame;
    let hdr = RequestHeader::decode(&mut cur, FLEX_REQ_HDR).expect("request header decodes");
    let body = T::decode(&mut cur, api_version).expect("request body decodes");
    assert2::assert!(cur.is_empty());
    let mut out = BytesMut::new();
    hdr.encode(&mut out, FLEX_REQ_HDR)
        .expect("header re-encodes");
    body.encode(&mut out, api_version).expect("body re-encodes");
    assert2::assert!(out.as_ref() == frame);
}

fn roundtrip_response<T>(frame: &[u8], api_version: i16)
where
    T: for<'de> Decode<'de> + Encode,
{
    let mut cur: &[u8] = frame;
    let hdr = ResponseHeader::decode(&mut cur, FLEX_RESP_HDR).expect("response header decodes");
    let body = T::decode(&mut cur, api_version).expect("response body decodes");
    assert2::assert!(cur.is_empty());
    let mut out = BytesMut::new();
    hdr.encode(&mut out, FLEX_RESP_HDR)
        .expect("header re-encodes");
    body.encode(&mut out, api_version).expect("body re-encodes");
    assert2::assert!(out.as_ref() == frame);
}

fn rpc_frame_roundtrips(path: &Path) -> datatest_stable::Result<()> {
    let frame = std::fs::read(path)?;
    match path.file_name().and_then(|name| name.to_str()) {
        Some("vote_request.bin") => roundtrip_request::<VoteRequest>(&frame, 2),
        Some("vote_response.bin") => roundtrip_response::<VoteResponse>(&frame, 2),
        Some("begin_quorum_epoch_request.bin") => {
            roundtrip_request::<BeginQuorumEpochRequest>(&frame, 1);
        }
        Some("begin_quorum_epoch_response.bin") => {
            roundtrip_response::<BeginQuorumEpochResponse>(&frame, 1);
        }
        Some("end_quorum_epoch_request.bin") => {
            roundtrip_request::<EndQuorumEpochRequest>(&frame, 1);
        }
        Some("end_quorum_epoch_response.bin") => {
            roundtrip_response::<EndQuorumEpochResponse>(&frame, 1);
        }
        Some("describe_quorum_request.bin") => {
            roundtrip_request::<DescribeQuorumRequest>(&frame, 2);
        }
        Some("describe_quorum_response.bin") => {
            roundtrip_response::<DescribeQuorumResponse>(&frame, 2);
        }
        Some("fetch_request.bin") => roundtrip_request::<FetchRequest>(&frame, 17),
        Some("fetch_response.bin") => roundtrip_response::<FetchResponse>(&frame, 17),
        other => panic!("unexpected RPC fixture {other:?}"),
    }
    Ok(())
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
        std::env::var("TEST_SRCDIR"),
        std::env::var("TEST_WORKSPACE"),
        std::env::var("FIXTURE_ROOT"),
    ) else {
        return relative.to_owned();
    };
    format!("{srcdir}/{workspace}/{package}/{relative}")
}

datatest_stable::harness! {
    { test = rpc_frame_roundtrips, root = fixture_root("tests/fixtures/rpc"), pattern = r".*\.bin$" },
}
