#!/usr/bin/env bash
set -euo pipefail

# Usage: tools/sync-schemas.sh <git-ref>
# Vendors Apache Kafka's wire-protocol JSON schemas at the given ref
# into crates/protocol/schemas/.
#
# Run tools/regenerate.sh afterwards to re-emit the codecs.

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

REF="${1:?usage: sync-schemas.sh <git-ref>}"
REPO="https://github.com/apache/kafka.git"
DEST="crates/protocol/schemas"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "Cloning apache/kafka at $REF into $TMP..."
git clone --depth 1 --branch "$REF" "$REPO" "$TMP/kafka" 2>/dev/null || {
  git clone "$REPO" "$TMP/kafka"
  (cd "$TMP/kafka" && git checkout "$REF")
}

SRC="$TMP/kafka/clients/src/main/resources/common/message"
test -d "$SRC" || { echo "schema dir not found under upstream"; exit 1; }

# Replace only the top-level schema set. `$DEST/versions/<ns>` holds the
# separately pinned legacy namespaces (kafka_3_6_2), each with its own VERSION
# and its own upstream tag, so removing the whole directory would delete them.
mkdir -p "$DEST"
rm -f "$DEST"/*.json "$DEST/VERSION"
cp "$SRC"/*.json "$DEST"/

SHA=$(cd "$TMP/kafka" && git rev-parse HEAD)
cat > "$DEST/VERSION" <<VERSION_EOF
ref: $REF
sha: $SHA
synced_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)
VERSION_EOF

echo "Vendored $(ls "$DEST"/*.json | wc -l) schemas at $SHA"
