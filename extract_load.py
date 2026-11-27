"""Copy source tables into the Postgres `raw` schema and reconcile row counts.

Run on its own:  python extract_load.py [--tables a,b] [--max-tables N]
"""
from __future__ import annotations

import argparse
import json
import re
import warnings
from collections.abc import Callable
from typing import Any

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import types as sat

from common import CATALOG_PATH, REPORT_DIR, Settings, new_run_id, snake, utc_now

RAW_SCHEMA = "raw"
# Lower-cased source table names that are never copied. ColdRoomTemperatures_Archive
# (WideWorldImporters) is 3.65M rows of sensor history that no model uses; it made up
# most of the load time.
EXCLUDED_TABLES = {"sysdiagrams", "coldroomtemperatures_archive"}

# Source type name -> Postgres-friendly SQLAlchemy type. Anything not listed
# (binary, xml, geography, hierarchyid, rowversion, ...) is skipped on purpose.
TYPE_MAP: dict[str, Callable[[], sat.TypeEngine]] = {
    "TINYINT": sat.SmallInteger,
    "SMALLINT": sat.SmallInteger,
    "INT": sat.Integer,
    "INTEGER": sat.Integer,
    "BIGINT": sat.BigInteger,
    "BIT": sat.Boolean,
    "BOOLEAN": sat.Boolean,
    "DECIMAL": sat.Numeric,
    "NUMERIC": sat.Numeric,
    "MONEY": sat.Numeric,
    "SMALLMONEY": sat.Numeric,
    "FLOAT": sat.Float,
    "REAL": sat.Float,
    "DATE": sat.Date,
    "DATETIME": sat.DateTime,
    "DATETIME2": sat.DateTime,
    "SMALLDATETIME": sat.DateTime,
    "DATETIMEOFFSET": lambda: sat.DateTime(timezone=True),
    "TIME": sat.Time,
    "CHAR": sat.Text,
    "VARCHAR": sat.Text,
    "NCHAR": sat.Text,
    "NVARCHAR": sat.Text,
    "TEXT": sat.Text,
    "NTEXT": sat.Text,
    "UNIQUEIDENTIFIER": sat.Text,
}


class ReconcileError(RuntimeError):
    """Raised when source and raw row counts differ."""


def target_type(type_name: str, source_type: object) -> sat.TypeEngine:
    """Build the Postgres column type, preserving precision and scale for exact numerics."""
    if type_name == "MONEY":
        return sat.Numeric(19, 4)
    if type_name == "SMALLMONEY":
        return sat.Numeric(10, 4)
    if type_name in {"DECIMAL", "NUMERIC"}:
        precision = getattr(source_type, "precision", None)
        scale = getattr(source_type, "scale", None)
        if precision is not None and scale is not None:
            return sat.Numeric(int(precision), int(scale))
    return TYPE_MAP[type_name]()


def base_type_name(col_type: object) -> str:
    return re.split(r"[\s(]", str(col_type).upper().strip())[0]


def _target_names(columns: list[str]) -> dict[str, str]:
    used: set[str] = set()
    mapping: dict[str, str] = {}
    for col in columns:
        candidate = snake(col)
        suffix = 2
        while candidate in used:
            candidate = f"{snake(col)}_{suffix}"
            suffix += 1
        used.add(candidate)
        mapping[col] = candidate
    return mapping


def load_table(
    src: sa.Engine,
    pg: sa.Engine,
    schema: str,
    table_name: str,
    raw_name: str,
    chunk_rows: int,
) -> dict[str, Any] | None:
    insp = sa.inspect(src)
    md = sa.MetaData()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", sa.exc.SAWarning)
        tbl = sa.Table(table_name, md, schema=schema, autoload_with=src)

    keep: list[sa.Column[Any]] = []
    skipped: list[str] = []
    for col in tbl.columns:
        type_name = base_type_name(col.type)
        if type_name in TYPE_MAP:
            keep.append(col)
        else:
            skipped.append(f"{col.name}:{type_name}")
    if not keep:
        print(f"  {schema}.{table_name}: skipped (no loadable columns)")
        return None

    rename = _target_names([c.name for c in keep])
    dtype: dict[str, sat.TypeEngine] = {}
    to_str: set[str] = set()
    columns_meta: list[dict[str, str]] = []
    for col in keep:
        type_name = base_type_name(col.type)
        target = rename[col.name]
        pg_type = target_type(type_name, col.type)
        dtype[target] = pg_type
        if type_name == "UNIQUEIDENTIFIER":
            to_str.add(target)
        columns_meta.append(
            {"name": target, "kind": "text" if isinstance(pg_type, sat.Text) else "other"}
        )

    pk_cols = insp.get_pk_constraint(table_name, schema=schema).get("constrained_columns") or []
    pk = [rename[c] for c in pk_cols if c in rename]
    if len(pk) != len(pk_cols):
        pk = []

    stmt = sa.select(*keep)
    total = 0
    first = True
    with src.connect() as sconn, pg.begin() as pconn:
        for chunk in pd.read_sql(stmt, sconn, chunksize=chunk_rows):
            chunk = chunk.rename(columns=rename)
            for col_name in to_str:
                chunk[col_name] = chunk[col_name].map(lambda v: None if v is None else str(v))
            chunk.to_sql(
                raw_name,
                pconn,
                schema=RAW_SCHEMA,
                if_exists="replace" if first else "append",
                index=False,
                dtype=dtype,
                chunksize=5000,
            )
            first = False
            total += len(chunk)
        if first:  # source table is empty: still create the target table
            pd.DataFrame(columns=list(dtype)).to_sql(
                raw_name, pconn, schema=RAW_SCHEMA, if_exists="replace", index=False, dtype=dtype
            )
        source_rows = sconn.execute(sa.select(sa.func.count()).select_from(tbl)).scalar_one()
        raw_rows = pconn.execute(
            sa.text(f'select count(*) from "{RAW_SCHEMA}"."{raw_name}"')
        ).scalar_one()

    print(
        f"  {schema}.{table_name} -> {RAW_SCHEMA}.{raw_name}: "
        f"{raw_rows} rows, {len(keep)} columns"
        + (f", skipped {len(skipped)} unsupported" if skipped else "")
    )
    return {
        "source_schema": schema,
        "source_table": table_name,
        "raw_table": raw_name,
        "source_rows": int(source_rows),
        "raw_rows": int(raw_rows),
        "columns": columns_meta,
        "primary_key": pk,
        "skipped_columns": skipped,
    }


def run(
    settings: Settings,
    tables: list[str] | None = None,
    max_tables: int | None = None,
    chunk_rows: int = 20000,
) -> dict[str, Any]:
    src = settings.source_engine()
    pg = settings.pg_engine()
    with pg.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))

    insp = sa.inspect(src)
    wanted = {t.lower() for t in tables} if tables else None
    entries: list[tuple[str, str, str]] = []
    for schema in settings.source_schemas:
        for table_name in sorted(insp.get_table_names(schema=schema)):
            if table_name.lower() in EXCLUDED_TABLES:
                continue
            if wanted is not None and table_name.lower() not in wanted:
                continue
            if len(settings.source_schemas) == 1:
                raw_name = snake(table_name)
            else:
                raw_name = snake(f"{schema}_{table_name}")
            entries.append((schema, table_name, raw_name))
    if max_tables:
        entries = entries[:max_tables]
    if not entries:
        raise RuntimeError("No source tables found. Check MSSQL_DB, SOURCE_SCHEMAS and --tables.")

    print(f"Loading {len(entries)} table(s) into Postgres schema '{RAW_SCHEMA}'")
    loaded: list[dict[str, Any]] = []
    for schema, table_name, raw_name in entries:
        result = load_table(src, pg, schema, table_name, raw_name, chunk_rows)
        if result is not None:
            loaded.append(result)

    report_tables = loaded
    if tables and CATALOG_PATH.exists():
        # Partial load (--tables): keep the other tables' entries so gen_dbt_stubs.py
        # and the dbt sources still see the whole database.
        previous = json.loads(CATALOG_PATH.read_text(encoding="utf-8")).get("tables", [])
        reloaded = {t["raw_table"] for t in loaded}
        report_tables = [t for t in previous if t["raw_table"] not in reloaded] + loaded
        report_tables.sort(key=lambda t: t["raw_table"])

    catalog = {
        "load_id": new_run_id(),
        "loaded_at": utc_now().isoformat(),
        "tables": report_tables,
    }
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    CATALOG_PATH.write_text(json.dumps(catalog, indent=2), encoding="utf-8")

    mismatches = [t for t in loaded if t["source_rows"] != t["raw_rows"]]
    if mismatches:
        detail = ", ".join(
            f"{t['source_table']} (source {t['source_rows']} vs raw {t['raw_rows']})"
            for t in mismatches
        )
        raise ReconcileError(f"Row count mismatch: {detail}")
    print(f"Reconciled: row counts match for all {len(loaded)} table(s)")
    return catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tables", help="Comma-separated source table names to load")
    parser.add_argument("--max-tables", type=int, help="Load at most N tables")
    parser.add_argument("--chunk-rows", type=int, default=20000)
    args = parser.parse_args()
    tables = [t.strip() for t in args.tables.split(",")] if args.tables else None
    run(Settings.from_env(), tables=tables, max_tables=args.max_tables, chunk_rows=args.chunk_rows)


if __name__ == "__main__":
    main()
