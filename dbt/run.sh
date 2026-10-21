#!/usr/bin/env bash
# Build the gold layer against a pinned Iceberg snapshot.
set -euo pipefail

cd "$(dirname "$0")"

# The streaming jobs commit Iceberg snapshots while we build.  The catalog can
# retire the metadata file we pinned between the resolve and the scan, so the
# build is retried once against a freshly resolved snapshot.
attempt=1
max_attempts=2

while [ "${attempt}" -le "${max_attempts}" ]; do
  echo "resolving Iceberg metadata through ${ICEBERG_REST_URI:-http://iceberg-rest:8181} (attempt ${attempt}) ..."
  VARS="$(python resolve_iceberg.py)"
  echo "vars: ${VARS}"

  if dbt build --profiles-dir . --vars "${VARS}" --target dev; then
    exit 0
  fi

  if [ "${attempt}" -ge "${max_attempts}" ]; then
    echo "dbt build failed; see the log above" >&2
    exit 1
  fi
  echo "dbt build failed; re-resolving the Iceberg snapshot and retrying ..." >&2
  attempt=$((attempt + 1))
  sleep 5
done
