#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

MIX_VERSION=$(grep -E '@version "' mix.exs | head -1 | sed -E 's/.*@version "([^"]+)".*/\1/')
CHANGELOG_VERSION=$(grep -E '^## \[[0-9]' CHANGELOG.md | head -1 | sed -E 's/^## \[([^]]+)\].*/\1/')
SOURCE_REF_LINE=$(grep -E 'source_ref:' mix.exs | head -1)
if echo "$SOURCE_REF_LINE" | grep -q '@version'; then
  SOURCE_REF="$MIX_VERSION"
else
  SOURCE_REF=$(echo "$SOURCE_REF_LINE" | sed -E 's/.*source_ref: "v([^"]+)".*/\1/')
fi

echo "mix.exs version : $MIX_VERSION"
echo "CHANGELOG latest: $CHANGELOG_VERSION"
echo "docs source_ref : $SOURCE_REF"

FAIL=0
if [ "$MIX_VERSION" != "$CHANGELOG_VERSION" ]; then
  echo "MISMATCH: mix.exs ($MIX_VERSION) != CHANGELOG ($CHANGELOG_VERSION)" >&2
  FAIL=1
fi
if [ "$MIX_VERSION" != "$SOURCE_REF" ]; then
  echo "MISMATCH: mix.exs ($MIX_VERSION) != source_ref ($SOURCE_REF)" >&2
  FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
echo "release check: OK"