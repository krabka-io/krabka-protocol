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

The schemas and the JVM oracle move together. Give both the same Kafka tag, in
one pull request.

1. `./tools/sync-schemas.sh <new-kafka-tag>`
2. Review `git status crates/protocol/schemas`. Restore the schemas that the
   script deleted or overwrote. See the note below.
3. `./tools/regenerate.sh`
4. Set `kafkaVersion` in `tools/oracle/build.gradle.kts` to the same tag.
5. Build the oracle and run the differential suites:

   ```
   (cd tools/oracle && ./gradlew installDist)
   cargo test --no-fail-fast -p krabka-protocol -p krabka-compression \
       --test 'differential*' --test oracle_smoke -- --ignored
   ```

6. Commit `crates/protocol/schemas/VERSION`, the regenerated files and
   `tools/oracle/build.gradle.kts` together.

`sync-schemas.sh` replaces only the top-level schema set. The pinned legacy
namespaces under `crates/protocol/schemas/versions/` keep their own `VERSION`
and their own upstream tag, so bump each one on its own.

`sync-schemas.sh` copies only `clients/src/main/resources/common/message`. The
top-level set also holds the metadata records from Kafka's `metadata` module
and the remote log metadata records from its `storage` module, and some
schemas carry local edits. The script deletes or overwrites them.

The `jvm differential` CI job runs step 5 on every pull request. A mismatch
there after a bump is a real difference between the generated codecs and the
new `kafka-clients` release.
