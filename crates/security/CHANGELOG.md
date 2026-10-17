# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `UnsecuredJwsValidator::default()` allows no clock skew, as Kafka's
  `unsecuredValidatorAllowableClockSkewMs` defaults to 0
  ([krabka-broker#1430](https://github.com/krabka-io/krabka-broker/issues/1430)).
  It used to allow 30 s. `SignedJwsValidator::new` keeps Kafka's 30 s
  `sasl.oauthbearer.clock.skew.seconds`.
- `ScramServerExchange` now reads the SCRAM client-final message with the
  grammar of Kafka 4.3.1's `ScramMessages.ClientFinalMessage`
  ([#65](https://github.com/krabka-io/krabka-protocol/issues/65)):
  `c=<base64>,r=<nonce>`, any `,<alpha>=<value>` extensions, then
  `,p=<base64>`. It rejects attributes in another order, an attribute after
  the proof, a repeated `c=`, a nonce outside Kafka's printable characters,
  base64 outside Kafka's grammar, and trailing data. It accepts a proof whose
  last base64 character sets unused bits, as Java's decoder does. The
  `AuthMessage` still signs the client-final message as the client sent it.
  A client-first authzid is now compared with the username as the client
  escaped it, and the last of a repeated SCRAM extension wins, as in Kafka.
- `parse_client_initial_response` now matches the SASL/OAUTHBEARER client
  initial response with the grammar of Kafka 4.3.1's
  `OAuthBearerClientInitialResponse`
  ([krabka-broker#1390](https://github.com/krabka-io/krabka-broker/issues/1390)).
  It rejects the `y` channel-binding flag, an authzid without `a=` or with a
  raw `=` or a non-ASCII character, a message without the final `\x01`,
  trailing data, malformed extension keys and values, and a token outside
  Kafka's token characters. It accepts the `Bearer` scheme in any letter case
  and an authzid that contains `\x01`, and it no longer keeps the extra spaces
  before the token in the token. The authzid stays verbatim: Kafka does not
  decode `=2C` or `=3D` either.
- `UnsecuredJwsValidator::validate` now checks a token the way Kafka 4.3.1's
  `OAuthBearerUnsecuredValidatorCallbackHandler` does
  ([#66](https://github.com/krabka-io/krabka-protocol/issues/66)). It rejects
  an `exp` that is not after `iat`, an `iat` that is not a number, a blank
  principal, and a numeric scope claim. The new `scope_claim_name` and
  `required_scope` fields are Kafka's `unsecuredValidatorScopeClaimName` and
  `unsecuredValidatorRequiredScope`, and `parse_required_scope` reads the
  option. A token without a required scope item fails with the new
  `AuthError::InsufficientScope`. The new `oauth_error_json` renders Kafka's
  server challenge for any validation failure, such as
  `{"status":"insufficient_scope", "scope":"[a, b]"}`, and replaces
  `invalid_token_json`, which is removed.

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

