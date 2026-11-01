# Changelog

All notable changes to `krabka-protocol` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0] — 2026-11-01

**Breaking Rust API.** Handle the `Result` that the wire encoders and the
records-carrying borrowed `to_owned()` now return, call the owned
`RecordBatch` codec methods by their new names, and pass
`patch_base_offset_and_leader_epoch` a header array.

### Added

- `records::crc32c` and `records::crc32c_append`, the CRC-32C helpers that
  the record codecs use, are now public. A downstream crate takes a batch
  checksum through the same runtime-dispatched `crc-fast` entry point
  ([#79](https://github.com/krabka-io/krabka-protocol/pull/79)).
  `crc32c_append` no longer has a panic path.
- `ValidatedBatch::validate_record_summaries_with` visits a new
  `records::RecordSummary` per record: the record without its headers, plus
  its header count and the body length that its length prefix declares
  (Kafka's `sizeOfBodyInBytes`). The walk checks every header, the UTF-8 of
  its key included, but collects none, so it allocates nothing per record
  ([#79](https://github.com/krabka-io/krabka-protocol/pull/79)).
- `RecordsPayload::encode_length_prefixed` writes a records payload with its
  `BYTES` or `COMPACT_BYTES` length prefix
  ([#76](https://github.com/krabka-io/krabka-protocol/pull/76)).
- `ProtocolError::LengthOverflow`, for a length that its wire-format length
  prefix cannot hold
  ([#82](https://github.com/krabka-io/krabka-protocol/pull/82)).

### Changed

- **Breaking.** An encoder that panicked on a length its prefix cannot hold
  now returns `ProtocolError::LengthOverflow`
  ([#82](https://github.com/krabka-io/krabka-protocol/pull/82)). The
  `put_*` string and bytes encoders in `primitives::string_bytes`,
  `primitives::array::put_array_len` and `put_nullable_array_len` return
  `Result<(), ProtocolError>`, and so does `WriteTaggedFields::write`.
  `tagged_fields::encode_to_bytes`,
  `records::metadata::control::encode_control_batch` and
  `records::metadata::checkpoint::build_bootstrap_checkpoint` return
  `Result<Bytes, ProtocolError>`. The generated `Encode` implementations
  return the error instead of panicking.
- **Breaking.** The borrowed `to_owned()` of a message that carries records
  returns `Result<_, RecordsError>`, and no longer panics when its records
  do not convert ([#82](https://github.com/krabka-io/krabka-protocol/pull/82)).
  This covers `FetchResponse`, `FetchSnapshotResponse`, `ProduceRequest`
  and `ShareFetchResponse`, their `kafka_3_6_2` counterparts, and each of
  their nested types that holds records.
- **Breaking.** The inherent `decode`, `encode` and `encoded_len` of the
  owned `RecordBatch` are now `decode_batch`, `encode_batch` and
  `encoded_batch_len`, so they no longer shadow the `Decode` and `Encode`
  trait methods ([#82](https://github.com/krabka-io/krabka-protocol/pull/82)).
- **Breaking.** `patch_base_offset_and_leader_epoch` takes
  `&mut [u8; HEADER_LEN]` in place of a slice that it asserted was long
  enough ([#82](https://github.com/krabka-io/krabka-protocol/pull/82)).
- The owned codecs copy less. A decoder keeps the framed `Bytes` of a
  records field ([#74](https://github.com/krabka-io/krabka-protocol/pull/74)),
  and an encoder writes an uncompressed records field directly into the
  frame through `encode_length_prefixed`. Only a compressed v2 batch, whose
  size is not known before compression, still goes through a temporary
  buffer ([#76](https://github.com/krabka-io/krabka-protocol/pull/76)).
- `ValidatedBatch::validate_records` uses the summary walk, so it no longer
  allocates per record, and a record-field error label is formatted only
  when the error occurs. The owned decoder and encoder take the CRC of a
  contiguous batch in one pass. The bytes written, the errors and their
  order are unchanged
  ([#79](https://github.com/krabka-io/krabka-protocol/pull/79)).

## [0.7.0] — 2026-10-20

- Re-released with the 0.7 workspace; library behavior is unchanged.

## [0.6.0] — 2026-10-08

### Changed

- **Breaking.** `decode_value_header` accepts only KIP-631 frame version 1,
  `FRAME_VERSION`, as Kafka's `MetadataRecordSerde` does. Any other value is
  `EnvelopeError::UnknownFrameVersion`, and `decode_value` reports it as a
  schema mismatch with Kafka's message. `ValueHeader` no longer carries
  `frame_version`, and `EnvelopeError` is now `Clone`, `Copy` and `Eq`.

## [0.5.1] — 2026-10-06

- First release on crates.io under the `krabka-*` name. Earlier versions were
  published as `crabka-*`; those names are being retired.

## [0.4.0] — 2026-08-12


### <!-- 0 -->🚀 Features


- Expose runtime configuration policy ([#904](https://github.com/robot-head/crabka/pull/904)) (**breaking**)


### <!-- 1 -->🐛 Bug Fixes


- Start removing clippy suppressions ([#784](https://github.com/robot-head/crabka/pull/784))


### <!-- 3 -->📚 Documentation


- Rewrite all prose to ASD-STE100 Simplified Technical English ([#982](https://github.com/robot-head/crabka/pull/982))


## [0.3.9] — 2026-07-07


### <!-- 6 -->🧪 Testing


- Harden broker mutant coverage ([#713](https://github.com/robot-head/crabka/pull/713))


## [0.3.8] — 2026-06-23


### <!-- 4 -->⚡ Performance


- Profiling-driven optimizations (codec decode, broker group-commit, streams changelog) ([#630](https://github.com/robot-head/crabka/pull/630))


## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- KIP-534 log-compaction retention + fix control-batch dedup data-loss ([#528](https://github.com/robot-head/crabka/pull/528))

- Scaffold rules_rust client build + lean client_minimal facade ([#570](https://github.com/robot-head/crabka/pull/570))


### <!-- 1 -->🐛 Bug Fixes


- Make the client scaffold actually build end-to-end ([#573](https://github.com/robot-head/crabka/pull/573))


## [0.3.6] — 2026-06-13


## [0.3.5] — 2026-06-12


### <!-- 1 -->🐛 Bug Fixes


- Format generated code with prettyplease before rustfmt ([#495](https://github.com/robot-head/crabka/pull/495))


### <!-- 2 -->🚜 Refactor


- Emit all generated code via quote! instead of writeln! ([#493](https://github.com/robot-head/crabka/pull/493))


## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- Add crate skeleton

- Add ProtocolError

- Add Encode/Decode traits

- Add fixed-width integer primitives

- Add varint/varlong/uvarint primitives

- Add string/bytes/compact primitives

- Add UUID primitive

- Add KIP-482 tagged-fields support

- Vendor Kafka 4.2.0 message schemas

- Hand-write owned::ApiVersionsRequest

- Hand-write owned::ApiVersionsResponse

- Wire generated ApiVersionsRequest into protocol crate

- Borrowed flavor for ApiVersionsRequest

- Borrowed-flavor codegen for ApiVersionsRequest

- IR-walking owned emitter (primitives only)

- Owned emitter supports primitive arrays

- IR-walking borrowed emitter

- Turn on generation for Metadata request/response

- Turn on generation for Produce request/response

- Turn on generation for OffsetCommit request/response

- Turn on generation for Request/Response headers

- CommonStructs emission + DescribeGroups support

- Central ApiKey enum

- KIP-966 offset-aware unclean leader recovery ([#294](https://github.com/robot-head/crabka/pull/294))


### <!-- 1 -->🐛 Bug Fixes


- Struct Default honors schema-level non-null defaults

- Use ..Default::default() in ApiVersionsResponse literal

- Honor nullableVersions upper bound ([#283](https://github.com/robot-head/crabka/pull/283))


### <!-- 10 -->💼 Other


- Fail build on schema/generated drift

- Introduce RecordsPayload as the wire-field type for `records` ([#214](https://github.com/robot-head/crabka/pull/214))


### <!-- 2 -->🚜 Refactor


- Share is_default helper via krabka-protocol


### <!-- 3 -->📚 Documentation


- Crate-level rustdoc and CONTRIBUTING


### <!-- 6 -->🧪 Testing


- JVM oracle subprocess wrapper

- Byte-equality differential vs JVM oracle for ApiVersions

- Proptest round-trip for ApiVersions request/response

- Corpus replay harness with ApiVersions sample

- Pick oracle wrapper by host OS, not file existence

- Spawn oracle via `sh` to avoid ENOEXEC


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Rust + clippy + fmt matrix and JVM differential job

- Clean up clippy pedantic warnings

- Expand microbenchmark coverage across crates ([#138](https://github.com/robot-head/crabka/pull/138))


## [0.1.0] — 2026-05-11

### Added

- Wire protocol codec for Apache Kafka 4.2.0.
- Owned + borrowed flavors for every active Kafka 4.2 message (189
  message types across 604 supported `(api_key, version)` pairs).
- Typed `RecordBatch` v2 decoder/encoder with `zerocopy` header
  reinterpretation and `krabka-compression` integration.
- Central `ApiKey` enum listing every Kafka 4.2 API.
- Differential testing against `kafka-clients` 4.2.0 for every active
  `(api_key, version)` pair — all byte-equal.

### Supported Kafka versions

- Wire protocol: 4.2.0.

### MSRV

- Rust 1.95.0.
