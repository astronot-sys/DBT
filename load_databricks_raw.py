"""Copy the Postgres `raw` tables to Databricks (<catalog>.wwi_raw) as Delta tables.

Phase 2: dbt builds staging and marts directly on Databricks, so the raw data has to
be there. extract_load.py still does the SQL Server copy (type mapping, row-count
reconciliation) into Postgres `raw`; this script ships those tables on:

  Postgres raw --(Parquet)--> Volume <catalog>.wwi_raw.landing --> Delta <catalog>.wwi_raw.<table>

Column types come from Postgres, so empty columns and decimals keep their exact type.
Row counts are reconciled per table.

  python load_databricks_raw.py                                  # every table in the load report
  python load_databricks_raw.py --tables sales_orders,sales_invoices
"""
from __future__ import annotations

import argparse
import io
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa

from common import CATALOG_PATH, Settings
from extract_load import EXCLUDED_TABLES, RAW_SCHEMA, ReconcileError
from publish_bricks import Databricks

TARGET_SCHEMA = "wwi_raw"  # matches the raw source schema in models/staging/_sources.yml

PG_TO_ARROW: dict[str, pa.DataType] = {
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "boolean": pa.bool_(),
    "real": pa.float32(),
    "double precision": pa.float64(),
    "text": pa.string(),
    "date": pa.date32(),
    "timestamp without time zone": pa.timestamp("us"),
    "timestamp with time zone": pa.timestamp("us", tz="UTC"),
}


def arrow_schema(conn: sa.Connection, table: str) -> pa.Schema:
    rows = conn.execute(
        sa.text(
            "select column_name, data_type, numeric_precision, numeric_scale "
            "from information_schema.columns where table_schema = :s and table_name = :t "
            "order by ordinal_position"
        ),
        {"s": RAW_SCHEMA, "t": table},
    ).all()
    fields = []
    for name, data_type, precision, scale in rows:
        if data_type == "numeric":
            typ = pa.decimal128(int(precision), int(scale)) if precision is not None else pa.decimal128(38, 10)
        elif data_type in PG_TO_ARROW:
            typ = PG_TO_ARROW[data_type]
        else:
            raise TypeError(f"raw.{table}.{name}: no Parquet mapping for Postgres type '{data_type}'")
        fields.append(pa.field(name, typ))
    return pa.schema(fields)


def copy_table(settings: Settings, db: Databricks, table: str, expected_rows: int) -> dict[str, Any]:
    engine = settings.pg_engine()
    with engine.connect() as conn:
        schema = arrow_schema(conn, table)
        frame = pd.read_sql(
            sa.text(f'select * from "{RAW_SCHEMA}"."{table}"'),
            conn,
            coerce_float=False,
            dtype_backend="numpy_nullable",
        )
    data = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(data, buffer, compression="snappy")
    buffer.seek(0)

    folder = f"/Volumes/{db.catalog}/{TARGET_SCHEMA}/landing/{table}"
    db.client.files.upload(f"{folder}/part-0000.parquet", buffer, overwrite=True)
    full_name = f"{db.catalog}.{TARGET_SCHEMA}.{table}"
    db.sql(
        f"CREATE OR REPLACE TABLE {full_name} "
        f"AS SELECT * EXCEPT (_rescued_data) FROM read_files('{folder}/', format => 'parquet')"
    )
    rows = int(db.scalar(f"SELECT count(*) FROM {full_name}"))
    print(f"  raw.{table} -> {full_name}: {rows} rows")
    return {"table": table, "expected_rows": expected_rows, "rows": rows}


def run(settings: Settings, tables: list[str] | None = None, workers: int = 4) -> list[dict[str, Any]]:
    catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    entries = [
        t
        for t in catalog["tables"]
        if t["source_table"].lower() not in EXCLUDED_TABLES
        and (tables is None or t["raw_table"] in tables)
    ]
    if not entries:
        raise RuntimeError("No tables to copy. Run extract_load.py first, or check --tables.")

    db = Databricks.from_env()
    db.sql(f"CREATE SCHEMA IF NOT EXISTS {db.catalog}.{TARGET_SCHEMA}")
    db.sql(f"CREATE VOLUME IF NOT EXISTS {db.catalog}.{TARGET_SCHEMA}.landing")

    print(f"Copying {len(entries)} table(s) from Postgres raw to Databricks {db.catalog}.{TARGET_SCHEMA}")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(lambda t: copy_table(settings, db, t["raw_table"], t["raw_rows"]), entries))

    mismatches = [r for r in results if r["rows"] != r["expected_rows"]]
    if mismatches:
        detail = ", ".join(f"{r['table']} (raw {r['expected_rows']} vs Databricks {r['rows']})" for r in mismatches)
        raise ReconcileError(f"Row count mismatch: {detail}")
    print(f"Reconciled: row counts match for all {len(results)} table(s) in Databricks")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tables", help="Comma-separated raw table names (default: all)")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    tables = [t.strip() for t in args.tables.split(",")] if args.tables else None
    run(Settings.from_env(), tables=tables, workers=args.workers)


if __name__ == "__main__":
    main()
