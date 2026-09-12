#!/usr/bin/env bash
set -euo pipefail

# Regenerate the krabka-protocol wire codecs from the vendored Apache Kafka
# schemas in crates/protocol/schemas.
#
# Run this after you edit a schema, and after tools/sync-schemas.sh vendors a
# new upstream tag. The output is committed, so the CI `codegen drift` job runs
# this same script and fails when the working tree changes.

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

# rustfmt.toml sets `format_code_in_doc_comments`, `group_imports` and
# `imports_granularity`. All three are still nightly-gated: stable rustfmt
# warns and silently skips them, which would emit the generated imports in a
# different shape from the committed tree. MODULE.bazel pins the same nightly
# for //tools/format, so both formatters agree.
NIGHTLY="${KRABKA_RUSTFMT_TOOLCHAIN:-nightly}"

cargo run -p krabka-protocol-codegen -- \
    crates/protocol/schemas \
    crates/protocol/generated

cargo run -p krabka-protocol-codegen -- \
    --namespace kafka_3_6_2 \
    crates/protocol/schemas/versions/kafka_3_6_2 \
    crates/protocol/generated/kafka_3_6_2

# Auto-fix the machine-fixable clippy/rustc lints on the freshly emitted code
# (semicolons, range-contains, redundant closures, deref, unused, …) so the
# generated tree is idiomatic rather than blanket-#![allow]'d. clippy --fix edits
# the include!'d generated bodies in place; only the genuinely unfixable lints
# (intentional casts, must_use, always-true version comparisons) stay allowed in
# the wrapper header. Run twice — a fix can expose a follow-on lint.
cargo clippy --fix --allow-dirty --allow-staged -p krabka-protocol --all-targets >/dev/null
cargo clippy --fix --allow-dirty --allow-staged -p krabka-protocol --all-targets >/dev/null

# The codegen binary rustfmts each generated message file (they are include!'d,
# so cargo fmt never reaches them); clippy --fix may leave its edits unformatted.
# Re-run rustfmt on the generated bodies, then cargo fmt for the real module files.
find crates/protocol/generated -name '*.rs' -print0 |
    xargs -0 rustfmt "+${NIGHTLY}" --edition 2024
cargo "+${NIGHTLY}" fmt -p krabka-protocol

echo "Regenerated. Review the diff with: git diff crates/protocol/generated crates/protocol/src"
