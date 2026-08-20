# krabka-protocol

The Apache Kafka wire layer that [krabka](https://github.com/krabka-io) is built
on: request and response codecs for every Kafka API, the KRaft metadata record
types, SASL/TLS authentication, and the small domain crates the rest of the
stack shares.

Everything here is below the client and the broker in the dependency graph.
Nothing in this repository depends on either.

## Crates

| Crate | What it is |
| --- | --- |
| `crabka-protocol` | Kafka request/response codecs, generated from the upstream JSON schemas. |
| `crabka-metadata` | KRaft metadata records and the in-memory metadata image. |
| `crabka-security` | SASL (SCRAM, OAUTHBEARER, GSSAPI, PLAIN) and TLS configuration. |
| `crabka-compression` | gzip, snappy, lz4 and zstd record-batch codecs. |
| `crabka-voters` | KRaft voter-set model. |
| `crabka-ids` | Topic, partition, broker and producer identifiers. |
| `crabka-units` | Typed quantities (bytes, durations, rates) used across the stack. |
| `crabka-trace-context` | W3C trace-context and sqlcommenter propagation. |
| `crabka-kafka-tap` | Test-only TCP tap that records Kafka frames for corpus capture. |

## Build

Bazel is the build and test path. Cargo stays the dependency source of truth:
[`rules_rs`](https://github.com/hermeticbuild/rules_rs) reads the same
`Cargo.toml` and `Cargo.lock` that Cargo does, so there is no second dependency
set to keep in sync.

```
bazel test //...
```

`cargo` still works for everything Bazel does not cover:

```
cargo nextest run --workspace
```

## Mutation testing

Mutation sweeps run through
[`rules_rs_mutants`](https://github.com/robot-head/rules_rs_mutants), which lets
`cargo-mutants` enumerate mutants and lets Bazel build and test them. Each crate
has a `<crate>_mutants` target:

```
bazel test //crates/metadata:metadata_mutants
```

They are tagged `manual`, so `bazel test //...` skips them and a nightly job runs
the full sweep. Two things to know about the results:

* Only `#[cfg(test)]` unit tests take part. Mutants that the `tests/*.rs` suites
  would kill are reported as survivors here, so scores read lower than the
  monorepo's `cargo mutants` numbers for the same code.
* The `cargo-mutants` version is pinned by `tools/mutants/Cargo.lock`, not by
  whatever is on `PATH`.

## What does not run under Bazel

Some suites are tagged `manual` and run only under Cargo. Each carries a comment
at its `crate_tests` call saying why; the reasons are:

* **JVM differential suites** (`differential_*`, `oracle_smoke`,
  `capture_corpus`) drive the Gradle oracle in `tools/oracle`, which stayed in
  the monorepo.
* **`corpus_replay`, `kraft_rpc_roundtrip`, `kraft_metadata_roundtrip`** use
  `datatest-stable`, whose harness walks a fixture directory and keeps only
  entries whose `file_type().is_file()`. Every file Bazel stages in a runfiles
  tree is a symlink, so that filter discards all of them and the harness reports
  an empty run. Fixing it needs a change in `datatest-stable`.
* **`corpus_coverage`** resolves `CARGO_MANIFEST_DIR` at run time, which
  `rules_rust` rejects because it embeds an absolute build path in the binary.
* **`gssapi_provider`** needs the KDC that `crates/security/tests/fixtures/kdc`
  brings up under Docker. It is `#[ignore]`d under Cargo for the same reason.

## Publishing

These crates are published to crates.io from
[`robot-head/crabka`](https://github.com/robot-head/crabka), which is still the
release home for the `crabka-*` names. This repository has no release
automation; consumers pin it by git revision.
