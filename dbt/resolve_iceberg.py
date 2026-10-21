#!/usr/bin/env python3
"""
Resolve the current Iceberg metadata.json for each table through the REST
catalog and emit a dbt --vars JSON document.

DuckDB cannot query a REST catalog directly, so dbt must be handed a concrete
metadata file.  Keeping this resolver out of dbt means every model stays plain
SQL and every run is pinned to one snapshot.
"""
from __future__ import annotations

import json
import os
import sys
import time

import requests

REST = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181").rstrip("/")
TABLES = os.environ.get(
    "ICEBERG_TABLES",
    "recon.match_results",
).split(",")
ATTEMPTS = int(os.environ.get("ICEBERG_RESOLVE_ATTEMPTS", "90"))
DELAY = float(os.environ.get("ICEBERG_RESOLVE_DELAY", "2"))


def resolve(table: str) -> str:
    namespace, name = table.split(".", 1)
    url = f"{REST}/v1/namespaces/{namespace}/tables/{name}"
    last = None
    for attempt in range(ATTEMPTS):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 404:
                last = f"{table} does not exist yet"
            else:
                response.raise_for_status()
                payload = response.json()
                location = payload.get("metadata-location") or payload.get("metadata_location")
                if location:
                    return location
                last = f"{table} response has no metadata-location"
        except Exception as exc:  # noqa: BLE001 - startup retry loop
            last = f"{table}: {exc}"
        print(f"waiting for Iceberg table {table} ({attempt + 1}/{ATTEMPTS}): {last}", file=sys.stderr)
        time.sleep(DELAY)
    raise RuntimeError(f"could not resolve {table}: {last}")


def main() -> int:
    vars_doc = {table.replace(".", "__"): resolve(table) for table in TABLES}
    print(json.dumps(vars_doc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
