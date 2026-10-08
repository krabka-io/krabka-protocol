# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0] — 2026-10-08

- No changes. Released with the rest of the workspace for the breaking
  `krabka-protocol` change.

## [0.5.1] — 2026-10-06

- First release on crates.io under the `krabka-*` name. Earlier versions were
  published as `crabka-*`; those names are being retired.


### Added

- Hybrid logical clock stamps carried on the `krabka.hlc` Kafka record header, with a 16-byte fixed-width big-endian layout.
- `HlcClock`, the clock state machine from Kulkarni et al. (OPODIS 2014), over an injectable `PhysicalClock`.
- A self-fence that refuses a peer stamp more than a configured maximum offset ahead of the local physical clock.
- `HlcClock::observation`, a snapshot of the clock skew the node measured on the data path.
