# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Breaking.** The krabka-owned feature `krabka.version` (new module
  `krabka_version`) gates every krabka-only on-disk and inter-node format
  change from krabka-broker 1.0.0 on, as `metadata.version` does for Kafka's.
  Its supported range is `[0, 1]`: level 0, which an absent feature reads as,
  and level 1 both mean the 1.0.0 formats, and the first format change takes
  level 2. It is registered in `feature_registry`, so `supported_feature_ranges`
  advertises it and `bootstrap_feature_records` now seeds `krabka.version=1`
  for every release. `KrabkaVersion::finalized` reads the level from a
  `MetadataImage`, and `private_rpc_version` (with `PrivateRpc`) gives the
  highest request version of the private controller RPCs `SubmitChange`
  (1003), `MetadataFetch` (1004) and `DelegationTokenMutation` (1005) that a
  node may send at a finalized level. At levels 0 and 1 every RPC is v0.

### Changed

- **Breaking.** The body of each krabka-private record in a `NoOpRecord`
  tagged field (tags 1001 and 1003 to 1006) now starts with a big-endian
  `i16` version, `PRIVATE_RECORD_VERSION` (0). The reader refuses any other
  version, a tag the table does not assign (1002 included), and a record
  that arrives under a tag other than its own. `TranslateError` gains
  `UnknownPrivateTag`, `UnknownPrivateRecordVersion` and `PrivateTagMismatch`
  for these, so a caller can tell them from `NoCounterpart`. A record written
  before this change is refused, so a metadata log from 0.x must be formatted
  again.
- The wincode layout of every `MetadataRecord` variant is pinned by a
  golden-bytes test. It is part of the on-disk contract from krabka-broker
  1.0.0 on.

## [0.5.1] — 2026-10-06

- First release on crates.io under the `krabka-*` name. Earlier versions were
  published as `crabka-*`; those names are being retired.

## [0.3.8] — 2026-06-23


### <!-- 1 -->🐛 Bug Fixes


- Faster broker failover — retry backoff + startupProbe ([#583](https://github.com/robot-head/crabka/pull/583))


### <!-- 2 -->🚜 Refactor


- Migrate whole-function cargo-mutants exclusions to #[mutants::skip] ([#615](https://github.com/robot-head/crabka/pull/615))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Emit Cloud Logging-friendly structured JSON across services ([#508](https://github.com/robot-head/crabka/pull/508))

- Interactive in-browser WASM consensus simulator ([#562](https://github.com/robot-head/crabka/pull/562))

- KIP-939 2PC participation — coordinator semantics + stateright model ([#560](https://github.com/robot-head/crabka/pull/560))

- Scaffold rules_rust client build + lean client_minimal facade ([#570](https://github.com/robot-head/crabka/pull/570))


### <!-- 3 -->📚 Documentation


- Add exhaustive KIP matrix; fix UnregisterBroker KIP-185→919 mislabel ([#549](https://github.com/robot-head/crabka/pull/549))


### <!-- 6 -->🧪 Testing


- Stateright consensus model + deterministic-sync test infra (Phase 1) ([#511](https://github.com/robot-head/crabka/pull/511))

- Kill cargo-mutants survivors in metadata (and raft) ([#555](https://github.com/robot-head/crabka/pull/555))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12

## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- Migrate from bincode to wincode + serde-wincode ([#58](https://github.com/robot-head/crabka/pull/58))

- UnregisterBroker admin API (KIP-185) ([#259](https://github.com/robot-head/crabka/pull/259))

- KIP-584 UpdateFeatures write path (api_key 57) ([#281](https://github.com/robot-head/crabka/pull/281))

- KIP-853 dynamic KRaft quorum reconfiguration ([#290](https://github.com/robot-head/crabka/pull/290))

- Vertical role separation — process.roles + true observer metadata fetch ([#292](https://github.com/robot-head/crabka/pull/292))

- KRaft metadata snapshots (KIP-630) ([#287](https://github.com/robot-head/crabka/pull/287))

- Broker runtime metadata.version enforcement ([#307](https://github.com/robot-head/crabka/pull/307))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#52](https://github.com/robot-head/crabka/pull/52))

- Release v0.1.0 ([#59](https://github.com/robot-head/crabka/pull/59))

- Expand microbenchmark coverage across crates ([#138](https://github.com/robot-head/crabka/pull/138))

## [0.1.0] — 2026-05-13


### <!-- 0 -->🚀 Features


- Migrate from bincode to wincode + serde-wincode ([#58](https://github.com/robot-head/crabka/pull/58))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#52](https://github.com/robot-head/crabka/pull/52))

