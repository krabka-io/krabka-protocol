# krabka-oracle

A JVM differential-test oracle. It answers Kafka wire questions with Apache
Kafka's own `kafka-clients` code.

Part of [Krabka](../../README.md), a Rust implementation of Apache
Kafka-compatible infrastructure and clients.

## Overview

The oracle is a small Java program. It reads one JSON request for each line of
standard input, and it writes one JSON response for each line of standard
output. Each request asks Kafka's generated message classes to encode or to
decode a value. The Rust side then compares its own bytes against the answer.

A hand-written expectation can be wrong about Kafka. The oracle calls the same
classes that a real Kafka client or broker calls.

The program supports these operations:

| Operation | What it does |
| :--- | :--- |
| `encode`, `decode` | A message, through Kafka's generated `*JsonConverter` classes. |
| `header_encode`, `header_decode` | A `RequestHeader` or a `ResponseHeader`. |
| `record_batch_encode`, `record_batch_decode` | A v2 record batch, through `MemoryRecords`. |
| `compress`, `decompress` | One buffer, with the `gzip`, `snappy`, `lz4`, or `zstd` codec. |

An `encode` or `decode` request names its message with `apiKey` and
`isRequest`, or with `messageName`. A `messageName` lookup also finds the
metadata records in `kafka-metadata` (for example `PartitionRecord`) and the
remote log metadata records in `kafka-storage`.

## Kafka version

The Kafka version in [`build.gradle.kts`](build.gradle.kts) must equal the
`ref` in [`crates/protocol/schemas/VERSION`](../../crates/protocol/schemas/VERSION).
The oracle checks the codecs that are generated from those schemas, so it uses
the Kafka release that the schemas came from.
[`docs/CONTRIBUTING.md`](../../docs/CONTRIBUTING.md) changes both in one bump.

## Build

You need a JDK 17. Set `JAVA_HOME` if the default JDK is a different release.
You do not need a system Gradle, because the wrapper is in this directory.

```bash
(cd tools/oracle && ./gradlew installDist)
```

Gradle installs the program under `tools/oracle/build/install/krabka-oracle/`.
The start scripts are `bin/krabka-oracle` and `bin/krabka-oracle.bat`. Gradle
writes both scripts on every platform. The jars are in `lib/`.

Gradle writes `build/` and `.gradle/` in this directory. They are build output,
and `.gitignore` excludes them.

## Who runs it

The differential suites start the program as a child process, and they keep it
alive for the whole suite:

- `crates/protocol/tests/differential_*.rs` and `oracle_smoke.rs`
- `crates/compression/tests/differential.rs`

Each of these tests is `#[ignore]`d, because it needs a JDK. The `jvm
differential` job in [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
builds the oracle and runs them. To run them locally:

```bash
(cd tools/oracle && ./gradlew installDist)
cargo test --no-fail-fast -p krabka-protocol -p krabka-compression \
    --test 'differential*' --test oracle_smoke -- --ignored
```

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
