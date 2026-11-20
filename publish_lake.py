"""Publish the dbt `export` schema from Postgres to the data lake as Parquet.

Layout (relative to LAKE_URI):
  curated/<table>/run_id=<run_id>/part-0000.parquet
  _manifests/run_id=<run_id>/manifest.json   (written last: it is the commit marker)
  _manifests/latest.json                     (points at the newest committed run)

LAKE_URI is a local folder by default (./lake). For any S3-compatible store use
s3://bucket/prefix together with S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import sqlalchemy as sa

from common import Settings, ensure_ops, utc_now

EXPORT_SCHEMA = "export"


def lake_fs(settings: Settings) -> tuple[pafs.FileSystem, str]:
    """Return (filesystem, base path) for the configured lake."""
    uri = settings.lake_uri
    if uri.startswith("s3://"):
        base = uri[len("s3://") :].rstrip("/")
        kwargs: dict[str, Any] = {"region": settings.s3_region}
        if settings.s3_endpoint:
            scheme, _, host = settings.s3_endpoint.partition("://")
            kwargs.update(endpoint_override=host, scheme=scheme)
        if settings.s3_access_key and settings.s3_secret_key:
            kwargs.update(access_key=settings.s3_access_key, secret_key=settings.s3_secret_key)
        return pafs.S3FileSystem(**kwargs), base
    folder = Path(uri).expanduser().resolve()
    folder.mkdir(parents=True, exist_ok=True)
    return pafs.LocalFileSystem(), folder.as_posix()


def write_bytes(fs: pafs.FileSystem, path: str, data: bytes) -> None:
    fs.create_dir(path.rsplit("/", 1)[0], recursive=True)
    with fs.open_output_stream(path) as out:
        out.write(data)


def read_json(fs: pafs.FileSystem, path: str) -> dict[str, Any]:
    with fs.open_input_stream(path) as src:
        return json.loads(src.read())


def export_tables(engine: sa.Engine) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "select table_name from information_schema.tables "
                "where table_schema = :s and table_type = 'BASE TABLE' order by table_name"
            ),
            {"s": EXPORT_SCHEMA},
        ).all()
    return [r[0] for r in rows]


def decimal_types(conn: sa.Connection, table: str) -> dict[str, pa.DataType]:
    """Map numeric columns to decimal128(precision, scale) taken from Postgres metadata.

    Unconstrained NUMERIC (no declared precision) falls back to decimal128(38, 10).
    """
    rows = conn.execute(
        sa.text(
            "select column_name, numeric_precision, numeric_scale from information_schema.columns "
            "where table_schema = :s and table_name = :t and data_type = 'numeric'"
        ),
        {"s": EXPORT_SCHEMA, "t": table},
    ).all()
    return {r[0]: pa.decimal128(int(r[1]), int(r[2])) if r[1] is not None else pa.decimal128(38, 10) for r in rows}


def to_arrow(frame: pd.DataFrame, decimals: dict[str, pa.DataType]) -> pa.Table:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    for col, typ in decimals.items():
        idx = table.schema.get_field_index(col)
        table = table.set_column(idx, col, table.column(idx).cast(typ))
    return table


def publish(settings: Settings, run_id: str, dbt_invocation_id: str | None) -> dict[str, Any]:
    engine = settings.pg_engine()
    ensure_ops(engine)
    fs, base = lake_fs(settings)
    names = export_tables(engine)
    if not names:
        raise RuntimeError(f"No tables found in Postgres schema '{EXPORT_SCHEMA}'. Run dbt build first.")

    published_at = utc_now()
    entries: list[dict[str, Any]] = []
    for name in names:
        with engine.connect() as conn:
            frame = pd.read_sql(sa.text(f'select * from "{EXPORT_SCHEMA}"."{name}"'), conn, coerce_float=False)
            decimals = decimal_types(conn, name)
        table = to_arrow(frame, decimals)
        rel_path = f"curated/{name}/run_id={run_id}/part-0000.parquet"
        fs.create_dir(f"{base}/{rel_path.rsplit('/', 1)[0]}", recursive=True)
        pq.write_table(table, f"{base}/{rel_path}", filesystem=fs, compression="snappy")
        entries.append(
            {
                "table": name,
                "rows": table.num_rows,
                "path": rel_path,
                "columns": [{"name": f.name, "type": str(f.type)} for f in table.schema],
            }
        )
        print(f"  published {name}: {table.num_rows} rows -> {rel_path}")

    manifest = {
        "run_id": run_id,
        "dbt_invocation_id": dbt_invocation_id,
        "published_at": published_at.isoformat(),
        "tables": entries,
    }
    payload = json.dumps(manifest, indent=2).encode("utf-8")
    # Manifest last: a run only counts as published once its manifest exists.
    write_bytes(fs, f"{base}/_manifests/run_id={run_id}/manifest.json", payload)
    write_bytes(fs, f"{base}/_manifests/latest.json", payload)

    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "insert into ops.publish_log (run_id, table_name, row_count, lake_path, published_at) "
                "values (:run_id, :table_name, :row_count, :lake_path, :published_at) "
                "on conflict (run_id, table_name) do update set "
                "row_count = excluded.row_count, lake_path = excluded.lake_path, "
                "published_at = excluded.published_at"
            ),
            [
                {
                    "run_id": run_id,
                    "table_name": e["table"],
                    "row_count": e["rows"],
                    "lake_path": e["path"],
                    "published_at": published_at,
                }
                for e in entries
            ],
        )
    print(f"Published {len(entries)} table(s) to {settings.lake_uri} (run {run_id})")
    return manifest


if __name__ == "__main__":
    from common import new_run_id

    publish(Settings.from_env(), new_run_id(), None)
