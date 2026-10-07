# krabka-protocol — project-specific guidance

## Compatibility

**From krabka-broker 1.0.0 on, krabka is backwards compatible on disk.** Any
1.x broker reads every artifact that an earlier 1.x broker wrote, and a rolling
upgrade from 1.x to 1.y works. Data written before 1.0.0 gets no promise.
krabka-broker's
[`docs/persisted_formats.md`](https://github.com/krabka-io/krabka-broker/blob/main/docs/persisted_formats.md)
lists every persisted format, states the contract, and records the known gaps.

In this repository the contract covers the code that defines bytes a broker
persists, or sends to a node of another version during a rolling upgrade:

- `krabka-metadata`: `MetadataRecord` and every type it contains, the
  `NoOpRecord` private tags, and the KIP-631 translation in `kraft_translate.rs`
- `krabka-protocol`: the record-batch, control-record, metadata-envelope and
  checkpoint codecs under `crates/protocol/src/records/`
- every type in `krabka-ids`, `krabka-voters` and `krabka-security` that a
  `MetadataRecord` contains

For all of them:

- Never reorder or remove a `MetadataRecord` variant, and never insert one
  before an existing variant. Add a new variant at the end only. wincode
  encodes a variant by its index.
- Never change the fields of a type that a persisted `MetadataRecord` contains.
  wincode is positional and carries no field names, so `#[serde(default)]` does
  not help it. To change a record's shape, add a new variant at the end (for
  example `V2Topic`) and keep the old variant readable.
- Never reuse a `NoOpRecord` private tag. 1001 and 1003 to 1006 are assigned,
  and 1002 is burned. A new private record takes a new tag.
- Give a new or changed format a version marker, and a reader for every earlier
  1.x version of it.
- Gate new writer behavior on a feature level, so a broker keeps writing the old
  format until the operator finalizes the level that introduces the new one, as
  in Kafka's KIP-584 and KIP-778. Never add a `metadata.version` level that
  Kafka's `MetadataVersion` does not have.
- Add a golden-bytes fixture test for each persisted format you add or change.
  The test decodes bytes that an earlier release wrote and compares the decoded
  value. It does not compare source text.
- Where they keep 1.x data readable, `#[serde(default)]` on a JSON field, a kept
  `V1` variant beside its `V2`, and a reader for an older version are required,
  not forbidden.

During development, deleting local raft logs and data directories is still fine
for a format that no release has shipped.

Everything else that Kafka compatibility does not govern keeps the greenfield
rule: in-memory types, internal APIs, and the Rust API. For those, do not write
backwards-compatibility shims:

- No feature flags that gate new non-persisted behavior behind a default-off
  switch
- No deprecated-but-kept API surfaces

When a non-persisted schema, enum, or interface changes, change it. The Rust API
is not under a stability promise.

**Kafka compatibility is the constraint that matters.** Always keep:

- Apache Kafka wire-protocol byte exactness for request and response shapes,
  field order, error codes, and version negotiation
- KIP semantics for the feature that you implement
- Behavior that the JVM admin tools rely on, such as `kafka-topics`,
  `kafka-acls`, `kafka-leader-election`, and `kafka-reassign-partitions`

When in doubt, match Kafka. If Kafka's behavior is undocumented or
version-dependent, check the behavior of the latest released cp-kafka image. Do
not rely on the wiki.

## Build

Bazel is the build and test path; Cargo is the dependency source of truth.
`rules_rs` reads the same `Cargo.toml` / `Cargo.lock` Cargo does.

```
bazel test //...          # everything CI gates on
cargo nextest run --workspace
```

Per-crate BUILD files stay small on purpose: `//bazel:defs.bzl` reads crate
name, edition, feature set and dependency labels out of the `@crates` repo that
`crate.from_cargo` generates, so a manifest change does not need a matching
BUILD edit. Add a new workspace member by writing its `Cargo.toml` and a
four-line `BUILD.bazel` that calls `crate_library` and `crate_tests`.

Suites that cannot run hermetically are tagged `manual` at their `crate_tests`
call, with a comment saying why. Add to that list rather than deleting a test.


## Code & Documentation Style

Follow the style guides in [`docs/style_guides/`](docs/style_guides/README.md):
[code](docs/style_guides/code_style_guide.md),
[rustdoc](docs/style_guides/rustdoc_style_guide.md),
[README](docs/style_guides/readme_style_guide.md),
[design docs](docs/style_guides/design_doc_style_guide.md), and
[coverage reports](docs/style_guides/coverage_report_style_guide.md). Examples
are the pinned stable toolchain, `cargo +nightly fmt`, forbidden `unsafe`, and
`clippy::pedantic`.

Do not make style-only sweeps across untouched files. Bring a file into line
with the guides only when you already edit it. Keep the tidy-up proportionate to
the change.

### Assertions and Clippy

- Never add `#[allow(clippy::...)]` or any equivalent Clippy suppression. Fix
  every Clippy warning in the code, regardless of the effort required.
- Never use Rust's plain `assert!`, `assert_eq!`, or `assert_ne!` macros. Use
  the `assert2` crate's `assert!` macro instead. Use it also for equality and
  inequality comparisons.

Clippy is a Cargo-side gate. `bazel build` applies `-Funsafe_code` (the one
`[workspace.lints]` entry whose guarantee must not lapse under a second build
system) but does not run Clippy, so run `cargo clippy --workspace --all-targets
-- -D warnings` before you push.

## Execution

When you execute an implementation plan, always use **subagent-driven
development in parallel batches** where the per-task file sets do not overlap.
Dispatch all tasks in a batch concurrently, in one message with multiple Agent
calls. Then wait for the batch to complete, review it, and move to the next.

A "conflict" between parallel implementers occurs only when both edit the same
file. When in doubt, list the file set that each task touches before you decide.

**Never discard working-tree state while parallel implementers run.**
`git checkout -- <path>`, `git restore`, `git stash`, and `git clean` all
destroy *every* uncommitted change in the files they touch, not only yours. To
undo your own edit, reverse it directly.

Tests must exercise behavior, not source text. Do not read source files in tests
and assert against their contents. `include_str!` and `fs::read_to_string` are
examples of such reads. If a behavior is hard to test, add a narrow helper or
seam. Then test that behavior directly.

When you check generated protocol records or other structured values in tests,
compare the whole expected struct. This is better than long chains of
field-by-field assertions. Use table-driven or parameterized tests for repeated
scenarios that differ only by inputs, protocol version, or expected request
shape.

## Releases

A `vX.Y.Z` tag on `main` starts `.github/workflows/publish.yml`, which publishes
every member crate without `publish = false` to crates.io. A new crate that is
not a library for other repositories sets `publish = false`. Every `krabka-*`
dependency carries a `version` as well as its `path`, because `cargo publish`
needs one. A git dependency that is not on crates.io may appear only as a
dev-dependency, and without a `version`, so that cargo drops it from the
published manifest.

[`docs/releasing.md`](docs/releasing.md) is the procedure, including the
token bootstrap for a new crate name and the switch to trusted publishing. It
also covers `.github/workflows/retire-crabka.yml`, the one-time retirement of
the `crabka-*` crates that the project published under its old name.
