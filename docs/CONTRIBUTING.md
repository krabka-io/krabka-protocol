# Contributing to krabka-protocol

## Code and documentation style

Code and docs follow the [style guides](style_guides/README.md) in
`docs/style_guides/`. Read the [code style
guide](style_guides/code_style_guide.md) before your first change.

## Build, test, lint and format

Bazel is the build and test path. The [README](../README.md) lists every task
and its plain-Bazel equivalent.

```
bazel test //...
```

## The generated wire codecs

`crates/protocol` does not hand-write its Kafka request and response codecs.
`crates/protocol-codegen` emits them from the Apache Kafka JSON schemas that
`crates/protocol/schemas` vendors, and the result is committed under
`crates/protocol/generated`.

Two things follow from that. Do not edit a file under
`crates/protocol/generated`, because the next regeneration overwrites it. Edit
the emitter or the schema instead.

### Regenerate after you edit a schema

```
./tools/regenerate.sh
git diff crates/protocol/generated crates/protocol/src
```

The script needs two toolchains. `rust-toolchain.toml` pins the compiler.
rustfmt is a separate pin, because `rustfmt.toml` uses three nightly-gated
options that stable rustfmt skips without an error. Set
`KRABKA_RUSTFMT_TOOLCHAIN` to the nightly that `MODULE.bazel` pins for
`//tools/format`:

```
rustup toolchain install nightly-2026-08-14 --profile minimal --component rustfmt
KRABKA_RUSTFMT_TOOLCHAIN=nightly-2026-08-14 ./tools/regenerate.sh
```

The `codegen drift` CI job runs the same script and fails when the working tree
changes. So `crates/protocol/generated` cannot go out of sync with
`crates/protocol/schemas`.

### Bump the upstream Kafka version

1. `./tools/sync-schemas.sh <new-kafka-tag>`
2. `./tools/regenerate.sh`
3. Commit `crates/protocol/schemas/VERSION` and the regenerated files together.

`sync-schemas.sh` replaces only the top-level schema set. The pinned legacy
namespaces under `crates/protocol/schemas/versions/` keep their own `VERSION`
and their own upstream tag, so bump each one on its own.

The JVM differential-test oracle is not in this repository. It is in
[krabka-broker](https://github.com/krabka-io/krabka-broker), which holds
`tools/oracle` and its `kafka-clients` dependency. A Kafka version bump is two
changes, one in each repository. Give both the same Kafka tag. Do the schema
half here first. Then follow the oracle procedure in that repository's
`docs/CONTRIBUTING.md`.
