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

### Everything CI does, locally

The [Aspect CLI](https://github.com/aspect-build/aspect-cli) narrows each task to
what a change actually touched. Every one has a plain-Bazel equivalent, so the
CLI is a convenience rather than a requirement:

| | Aspect CLI | Plain Bazel |
| --- | --- | --- |
| Build | `aspect build //...` | `bazel build //...` |
| Test | `aspect test //...` | `bazel test //...` |
| Lint | `aspect lint` | `bazel build --config=lint //...` |
| Format | `aspect format` | `bazel run //tools/format` |
| Coverage | `aspect test --coverage` | `bazel coverage //crates/...` |
| Docs | — | `bazel build //crates/protocol:protocol_doc` |

Formatting and linting are Bazel targets rather than a separate `cargo fmt` /
`cargo clippy` pass, so they see exactly the files and crates the build sees. A
file in no target cannot drift unnoticed, and clippy resolves the same features
the build resolves.

Two details worth knowing:

* **rustfmt runs on a pinned nightly.** `rustfmt.toml` uses
  `format_code_in_doc_comments`, `group_imports` and `imports_granularity`, all
  still nightly-gated; stable rustfmt warns and silently skips them. The nightly
  is pinned in `MODULE.bazel`, so formatting is reproducible rather than a
  function of whichever nightly is installed.
* **`rustfmt.toml` states its edition.** `cargo fmt` passes `--edition` from
  `Cargo.toml`; rustfmt invoked directly defaults to 2015 and sorts `use` lists
  differently. Stating it makes formatting a property of the repository rather
  than of how rustfmt was launched.

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

Nothing, apart from one recording tool. `bazel test //...` and
`cargo test --workspace` run the same 2220 tests and the same 11 rustdoc
examples.

`capture_corpus` is tagged `manual`: it records new fixtures through the JVM
oracle rather than reading them, so it is a tool rather than a test.

Suites needing Docker, the JVM oracle or an MIT KDC are `#[ignore]`d
individually, so they build and skip under both build systems, the same way.

Getting there took two things worth knowing about:

* **`datatest-stable` is patched under Bazel.** Its fixture walk keeps only
  entries whose `file_type().is_file()`, and `walkdir` does not follow symlinks
  by default — so for the symlinks Bazel stages in a runfiles tree the predicate
  is false, the walk finds nothing, and the harness reports an *empty run rather
  than a failure*. That silently skipped 620 fixtures.
  `bazel/patches/datatest-stable-follow-links.patch` adds `follow_links(true)`,
  applied through `crate.annotation`; Cargo keeps using the released crate.
* **Fixture paths resolve against the working directory.** Cargo runs an
  integration test from the crate directory; Bazel runs it from an execution
  root and stages data under `$TEST_SRCDIR/$TEST_WORKSPACE`. The suites that read
  fixtures take a `FIXTURE_ROOT` prefix, set in their `crate_tests` call, and
  fall back to the bare relative path when it is unset.

Neither of these is Bazel-specific pedantry: a test that resolves
`CARGO_MANIFEST_DIR` at run time, or reads a path relative to the working
directory, only works when it is launched the way Cargo happens to launch it.

## Publishing

These crates are published to crates.io from
[`robot-head/crabka`](https://github.com/robot-head/crabka), which is still the
release home for the `crabka-*` names. This repository has no release
automation; consumers pin it by git revision.
