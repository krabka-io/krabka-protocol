# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0] — 2026-10-08

- First release on crates.io under the `krabka-*` name. 0.5.1 could not
  publish because its `sspi` dependency came from git.

### Changed

- GSSAPI (Kerberos) now depends on `krabka-sspi` 0.23.0 from crates.io instead
  of a git fork of `sspi`. `krabka-sspi` is Devolutions' `sspi` 0.23.0 with the
  MIT Kerberos interoperability fixes (devolutions/sspi-rs#738) and the #764
  fix, and its library name is still `sspi`. The published crate therefore
  builds and has those fixes, so a consumer no longer needs a
  `[patch.crates-io]` entry for `sspi`. `krabka-sspi` is temporary: once an
  upstream `sspi` release has the fixes, this crate goes back to it.

## [0.5.1] — 2026-10-06

- First release on crates.io under the `krabka-*` name. Earlier versions were
  published as `crabka-*`; those names are being retired.

## [0.4.0] — 2026-08-12


### <!-- 0 -->🚀 Features


- Add Gres PostgreSQL runtime ([#795](https://github.com/robot-head/crabka/pull/795))

- Expose runtime configuration policy ([#904](https://github.com/robot-head/crabka/pull/904)) (**breaking**)


### <!-- 1 -->🐛 Bug Fixes


- Start removing clippy suppressions ([#784](https://github.com/robot-head/crabka/pull/784))


### <!-- 3 -->📚 Documentation


- Rewrite all prose to ASD-STE100 Simplified Technical English ([#982](https://github.com/robot-head/crabka/pull/982))

## [0.3.9] — 2026-07-07


### <!-- 0 -->🚀 Features


- Cross-service demo traces + codebase-wide instrumentation ([#706](https://github.com/robot-head/crabka/pull/706))


### <!-- 6 -->🧪 Testing


- Harden broker mutant coverage ([#713](https://github.com/robot-head/crabka/pull/713))

## [0.3.8] — 2026-06-23


### <!-- 2 -->🚜 Refactor


- Migrate whole-function cargo-mutants exclusions to #[mutants::skip] ([#615](https://github.com/robot-head/crabka/pull/615))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Scaffold rules_rust client build + lean client_minimal facade ([#570](https://github.com/robot-head/crabka/pull/570))


### <!-- 1 -->🐛 Bug Fixes


- Make the client scaffold actually build end-to-end ([#573](https://github.com/robot-head/crabka/pull/573))


### <!-- 6 -->🧪 Testing


- Kill accessor + OAUTHBEARER claim-logic survivors ([#563](https://github.com/robot-head/crabka/pull/563))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12

## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- SASL/GSSAPI (Kerberos) authentication ([#295](https://github.com/robot-head/crabka/pull/295))


### <!-- 1 -->🐛 Bug Fixes


- Unblock pbkdf2/sha2/hmac 0.13 upgrade ([#87](https://github.com/robot-head/crabka/pull/87))


### <!-- 10 -->💼 Other


- Crates/security — build_client_config_from_pem helper


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Expand microbenchmark coverage across crates ([#138](https://github.com/robot-head/crabka/pull/138))


### <!-- 8 -->🛡️ Security


- Workspace dependency refresh (rcgen 0.14, reqwest 0.13, kube 3.1, schemars 1.2) ([#155](https://github.com/robot-head/crabka/pull/155))

