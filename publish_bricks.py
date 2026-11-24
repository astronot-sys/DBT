"""Publish the latest committed lake run to Databricks (Unity Catalog Delta tables).

  python publish_bricks.py                 # latest run in the lake
  python publish_bricks.py --run-id <id>   # a specific run

main.py calls publish() automatically after a successful local publish.
"""
from __future__ import annotations

import argparse
import io
import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from common import Settings
from publish_lake import lake_fs, read_json


def run_sql(w: WorkspaceClient, warehouse_id: str, statement: str) -> None:
    resp = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        error = resp.status.error.message if resp.status.error else resp.status.state
        raise RuntimeError(f"Databricks SQL failed: {error}\n{statement}")


def publish(run_id: str | None = None) -> None:
    settings = Settings.from_env()  # also loads .env, including the DATABRICKS_* keys
    fs, base = lake_fs(settings)
    manifest_path = (
        f"{base}/_manifests/run_id={run_id}/manifest.json" if run_id else f"{base}/_manifests/latest.json"
    )
    manifest = read_json(fs, manifest_path)
    run_id = manifest["run_id"]

    w = WorkspaceClient()  # reads DATABRICKS_HOST and DATABRICKS_TOKEN
    warehouse = os.environ["DATABRICKS_WAREHOUSE_ID"]
    catalog = os.environ.get("DATABRICKS_CATALOG", "workspace")
    schema = os.environ.get("DATABRICKS_SCHEMA", "wwi_curated")
    volume = os.environ.get("DATABRICKS_VOLUME", "landing")
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

    run_sql(w, warehouse, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    run_sql(w, warehouse, f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

    for table in manifest["tables"]:
        name, rel_path = table["table"], table["path"]
        with fs.open_input_stream(f"{base}/{rel_path}") as src:
            w.files.upload(f"{volume_path}/{rel_path}", io.BytesIO(src.read()), overwrite=True)

        folder = f"{volume_path}/{rel_path.rsplit('/', 1)[0]}/"
        run_sql(w, warehouse, f"""
            CREATE OR REPLACE TABLE {catalog}.{schema}.{name}
            COMMENT 'Published by DBT_pipeline run {run_id}'
            TBLPROPERTIES ('pipeline.run_id' = '{run_id}',
                           'pipeline.dbt_invocation_id' = '{manifest.get("dbt_invocation_id") or ""}')
            AS SELECT * EXCEPT (_rescued_data) FROM read_files('{folder}', format => 'parquet')
        """)
        print(f"  databricks: {catalog}.{schema}.{name} <- {table['rows']} rows")

    print(f"Published run {run_id} to Databricks {catalog}.{schema}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id")
    publish(parser.parse_args().run_id)