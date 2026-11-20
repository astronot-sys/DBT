"""Query the published lake with DuckDB (the demo's 'look, it landed' step).

  python query_lake.py                      # row counts per published table
  python query_lake.py "select * from exp_customers limit 5"
  python query_lake.py --run-id 20260101T000000Z-abc123 "select count(*) from exp_orders"
"""
from __future__ import annotations

import argparse
from typing import Any

import duckdb
import pyarrow.parquet as pq

from common import Settings
from publish_lake import lake_fs, read_json


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sql", nargs="?", help="SQL to run; published tables are available by name")
    parser.add_argument("--run-id", help="Read a specific run instead of the latest")
    args = parser.parse_args()

    fs, base = lake_fs(Settings.from_env())
    manifest_path = (
        f"{base}/_manifests/run_id={args.run_id}/manifest.json"
        if args.run_id
        else f"{base}/_manifests/latest.json"
    )
    manifest: dict[str, Any] = read_json(fs, manifest_path)
    print(f"Lake run {manifest['run_id']} published at {manifest['published_at']}")

    con = duckdb.connect()
    for entry in manifest["tables"]:
        con.register(entry["table"], pq.read_table(f"{base}/{entry['path']}", filesystem=fs))

    sql = args.sql or " union all ".join(
        f"select '{e['table']}' as table_name, count(*) as row_count from {e['table']}"
        for e in manifest["tables"]
    )
    print(con.execute(sql).fetchdf().to_string(index=False))


if __name__ == "__main__":
    main()
