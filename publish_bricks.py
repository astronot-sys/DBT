"""Publish tested tables to Databricks (Unity Catalog Delta tables in <catalog>.wwi_curated).

Two ways in, depending on where dbt ran:

  Postgres target  - publish():  upload a lake run's Parquet files and create tables from them.
  Databricks target - promote(): dbt built and tested the marts in <catalog>.wwi_marts;
                     copy them to wwi_curated only after the tests passed.

Consumers (notebooks, dashboards, AI assistants) read wwi_curated, so a run that fails
its dbt tests never reaches them.

  python publish_bricks.py                 # latest run in the local lake
  python publish_bricks.py --run-id <id>   # a specific lake run

main.py calls these automatically after the dbt tests pass.
"""
from __future__ import annotations

import argparse
import io
import os
import time
from dataclasses import dataclass
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementResponse, StatementState

from common import Settings
from publish_lake import lake_fs, read_json

MARTS_SCHEMA = "wwi_marts"  # where dbt builds the marts on Databricks (macros/generate_schema_name.sql)


@dataclass
class Databricks:
    """Connection and naming settings, read from .env (DATABRICKS_*)."""

    client: WorkspaceClient
    warehouse_id: str
    catalog: str
    curated_schema: str
    volume: str

    @classmethod
    def from_env(cls) -> Databricks:
        Settings.from_env()  # loads .env into the environment
        return cls(
            client=WorkspaceClient(),  # reads DATABRICKS_HOST and DATABRICKS_TOKEN
            warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
            catalog=os.environ.get("DATABRICKS_CATALOG", "workspace"),
            curated_schema=os.environ.get("DATABRICKS_SCHEMA", "wwi_curated"),
            volume=os.environ.get("DATABRICKS_VOLUME", "landing"),
        )

    def sql(self, statement: str) -> StatementResponse:
        """Run one SQL statement on the warehouse and wait for it; raise on failure."""
        resp = self.client.statement_execution.execute_statement(
            statement=statement, warehouse_id=self.warehouse_id, wait_timeout="50s"
        )
        while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(2)
            resp = self.client.statement_execution.get_statement(resp.statement_id)
        if resp.status.state != StatementState.SUCCEEDED:
            error = resp.status.error.message if resp.status.error else resp.status.state
            raise RuntimeError(f"Databricks SQL failed: {error}\n{statement}")
        return resp

    def scalar(self, statement: str) -> Any:
        resp = self.sql(statement)
        return resp.result.data_array[0][0] if resp.result and resp.result.data_array else None


def _properties(run_id: str, invocation_id: str | None) -> str:
    return (
        f"COMMENT 'Published by DBT_pipeline run {run_id}' "
        f"TBLPROPERTIES ('pipeline.run_id' = '{run_id}', "
        f"'pipeline.dbt_invocation_id' = '{invocation_id or ''}')"
    )


def publish(run_id: str | None = None) -> None:
    """Postgres target: create wwi_curated tables from a lake run's Parquet files."""
    db = Databricks.from_env()
    fs, base = lake_fs(Settings.from_env())
    manifest_path = (
        f"{base}/_manifests/run_id={run_id}/manifest.json" if run_id else f"{base}/_manifests/latest.json"
    )
    manifest = read_json(fs, manifest_path)
    run_id = manifest["run_id"]
    schema = f"{db.catalog}.{db.curated_schema}"
    volume_path = f"/Volumes/{db.catalog}/{db.curated_schema}/{db.volume}"

    db.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    db.sql(f"CREATE VOLUME IF NOT EXISTS {schema}.{db.volume}")

    for table in manifest["tables"]:
        name, rel_path = table["table"], table["path"]
        with fs.open_input_stream(f"{base}/{rel_path}") as src:
            db.client.files.upload(f"{volume_path}/{rel_path}", io.BytesIO(src.read()), overwrite=True)

        folder = f"{volume_path}/{rel_path.rsplit('/', 1)[0]}/"
        db.sql(
            f"CREATE OR REPLACE TABLE {schema}.{name} {_properties(run_id, manifest.get('dbt_invocation_id'))} "
            f"AS SELECT * EXCEPT (_rescued_data) FROM read_files('{folder}', format => 'parquet')"
        )
        print(f"  databricks: {schema}.{name} <- {table['rows']} rows")

    print(f"Published run {run_id} to Databricks {schema}")


def promote(run_id: str, tables: list[str], invocation_id: str | None) -> list[dict[str, Any]]:
    """Databricks target: copy dbt-built, tested marts from wwi_marts to wwi_curated."""
    if not tables:
        raise RuntimeError(f"No models were built in {MARTS_SCHEMA}; nothing to promote.")
    db = Databricks.from_env()
    source = f"{db.catalog}.{MARTS_SCHEMA}"
    target = f"{db.catalog}.{db.curated_schema}"
    db.sql(f"CREATE SCHEMA IF NOT EXISTS {target}")

    promoted: list[dict[str, Any]] = []
    for name in tables:
        db.sql(
            f"CREATE OR REPLACE TABLE {target}.{name} {_properties(run_id, invocation_id)} "
            f"AS SELECT * FROM {source}.{name}"
        )
        rows = int(db.scalar(f"SELECT count(*) FROM {target}.{name}"))
        promoted.append({"table": name, "rows": rows})
        print(f"  databricks: {source}.{name} -> {target}.{name} ({rows} rows)")
    print(f"Promoted {len(promoted)} table(s) to {target} (run {run_id})")
    return promoted


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id")
    publish(parser.parse_args().run_id)
