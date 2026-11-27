"""Orchestrate the pipeline: load -> dbt build + tests -> (gate) -> publish.

Two targets:

  --target postgres (default)
      SQL Server -> Postgres raw -> dbt on Postgres -> local lake (Parquet)
      -> Databricks wwi_curated (when DATABRICKS_HOST is set)

  --target databricks (Phase 2)
      SQL Server -> Postgres raw -> Databricks wwi_raw -> dbt on Databricks
      (wwi_staging, wwi_marts) -> promote tested marts to wwi_curated

Examples:
  python main.py                              # Postgres target, full load
  python main.py --skip-load                  # reuse data already loaded
  python main.py --skip-databricks            # Postgres target, local lake only
  python main.py --target databricks          # Phase 2: dbt runs on Databricks
  python main.py --target databricks --skip-load --inject-fault   # show the gate

Only models dbt built and tested in this run are published.
Exit codes: 0 published, 2 blocked by failing dbt tests, 1 any other failure.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import sqlalchemy as sa

import extract_load
from common import CATALOG_PATH, DBT_DIR, ROOT, Settings, ensure_ops, new_run_id, utc_now
from publish_lake import EXPORT_SCHEMA, built_models, publish

RUN_RESULTS = DBT_DIR / "target" / "run_results.json"
DBT_TARGETS = {"postgres": "dev", "databricks": "databricks"}  # pipeline target -> profiles.yml output


def dbt_executable() -> str:
    scripts = Path(sys.executable).parent
    for name in ("dbt.exe", "dbt"):
        candidate = scripts / name
        if candidate.exists():
            return str(candidate)
    found = shutil.which("dbt")
    if found:
        return found
    raise RuntimeError("dbt executable not found. Run: pip install -r requirements.txt")


def run_dbt(settings: Settings, target: str) -> int:
    RUN_RESULTS.unlink(missing_ok=True)  # never read results left over from an earlier run
    cmd = [
        dbt_executable(), "build",
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
        "--target", DBT_TARGETS[target],
    ]
    print("$ " + " ".join(cmd))
    return subprocess.run(cmd, cwd=ROOT, env={**os.environ, **settings.dbt_env()}).returncode


def read_dbt_summary() -> dict[str, Any]:
    if not RUN_RESULTS.exists():
        return {"invocation_id": None, "counts": {}, "failures": []}
    data = json.loads(RUN_RESULTS.read_text(encoding="utf-8"))
    counts: dict[str, int] = {}
    failures: list[str] = []
    for result in data.get("results", []):
        status = result.get("status", "unknown")
        counts[status] = counts.get(status, 0) + 1
        if status in {"fail", "error"}:
            failures.append(result.get("unique_id", "unknown"))
    return {
        "invocation_id": data.get("metadata", {}).get("invocation_id"),
        "counts": counts,
        "failures": failures,
    }


def inject_fault(engine: sa.Engine, target: str) -> str:
    """Duplicate one row of a table with a single-column primary key, in the target's raw schema."""
    catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    for table in catalog["tables"]:
        if len(table["primary_key"]) == 1 and table["raw_rows"] > 0:
            name = table["raw_table"]
            if target == "databricks":
                from publish_bricks import Databricks
                from load_databricks_raw import TARGET_SCHEMA

                db = Databricks.from_env()
                full = f"{db.catalog}.{TARGET_SCHEMA}.{name}"
                db.sql(f"INSERT INTO {full} SELECT * FROM {full} LIMIT 1")
                return full
            with engine.begin() as conn:
                conn.execute(
                    sa.text(f'insert into raw."{name}" select * from raw."{name}" limit 1')
                )
            return f"raw.{name}"
    raise RuntimeError("No table with a single-column primary key and rows to break.")


def record_run(
    engine: sa.Engine,
    run_id: str,
    status: str,
    *,
    dbt_rc: int | None = None,
    summary: dict[str, Any] | None = None,
    message: str | None = None,
    finished: bool = False,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "insert into ops.pipeline_runs (run_id, started_at, status) "
                "values (:run_id, :now, :status) "
                "on conflict (run_id) do update set status = excluded.status"
            ),
            {"run_id": run_id, "now": utc_now(), "status": status},
        )
        if finished:
            conn.execute(
                sa.text(
                    "update ops.pipeline_runs set finished_at = :now, status = :status, "
                    "dbt_return_code = :rc, dbt_summary = cast(:summary as jsonb), message = :msg "
                    "where run_id = :run_id"
                ),
                {
                    "now": utc_now(),
                    "status": status,
                    "rc": dbt_rc,
                    "summary": json.dumps(summary or {}),
                    "msg": message,
                    "run_id": run_id,
                },
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--inject-fault", action="store_true", help="Break one source row to show the gate")
    parser.add_argument("--skip-load", action="store_true", help="Skip the source -> Postgres copy")
    parser.add_argument("--tables", help="Comma-separated source tables to load")
    parser.add_argument("--max-tables", type=int)
    parser.add_argument("--skip-databricks", action="store_true", help="Postgres target: do not publish to Databricks")
    parser.add_argument(
        "--target",
        choices=sorted(DBT_TARGETS),
        default="postgres",
        help="Where dbt builds the models (default: postgres)",
    )
    args = parser.parse_args()
    on_databricks = args.target == "databricks"

    settings = Settings.from_env()
    engine = settings.pg_engine()
    ensure_ops(engine)
    run_id = new_run_id()
    record_run(engine, run_id, "running")
    print(f"=== Pipeline run {run_id} (target: {args.target}) ===")

    try:
        if args.skip_load:
            print("[1/3] Load skipped")
        else:
            print("[1/3] Copy source -> Postgres raw")
            tables = [t.strip() for t in args.tables.split(",")] if args.tables else None
            extract_load.run(settings, tables=tables, max_tables=args.max_tables)
            if on_databricks:
                import load_databricks_raw  # needs databricks-sdk only for this target

                print("      Copy Postgres raw -> Databricks wwi_raw")
                raw_names = None
                if tables:
                    report = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))["tables"]
                    wanted = {t.lower() for t in tables}
                    raw_names = [t["raw_table"] for t in report if t["source_table"].lower() in wanted]
                load_databricks_raw.run(settings, tables=raw_names)

        if args.inject_fault:
            broken = inject_fault(engine, args.target)
            print(f"      !! Fault injected: duplicated one row in {broken}")

        print(f"[2/3] Transform and test with dbt (target: {DBT_TARGETS[args.target]})")
        rc = run_dbt(settings, args.target)
        summary = read_dbt_summary()
        print(f"      dbt result counts: {summary['counts']}")

        if rc != 0:
            msg = f"Publish blocked: dbt build failed ({', '.join(summary['failures']) or 'see dbt output'})"
            record_run(engine, run_id, "blocked", dbt_rc=rc, summary=summary, message=msg, finished=True)
            print(f"[3/3] {msg}")
            return 2

        if on_databricks:
            import publish_bricks

            print("[3/3] Promote tested marts to Databricks wwi_curated")
            promoted = publish_bricks.promote(
                run_id, built_models(publish_bricks.MARTS_SCHEMA), summary["invocation_id"]
            )
            message = f"Promoted {len(promoted)} table(s) to Databricks"
        else:
            print("[3/3] Publish to the data lake")
            manifest = publish(settings, run_id, summary["invocation_id"], tables=built_models(EXPORT_SCHEMA))
            message = f"Published {len(manifest['tables'])} table(s)"

            if args.skip_databricks:
                print("      Databricks publish skipped (--skip-databricks)")
            elif os.environ.get("DATABRICKS_HOST"):
                print("      Publish to Databricks")
                import publish_bricks  # imported here so databricks-sdk is only needed when used

                publish_bricks.publish(run_id)
                message += " + Databricks"
            else:
                print("      Databricks publish skipped (DATABRICKS_HOST not set)")

        record_run(
            engine,
            run_id,
            "published",
            dbt_rc=rc,
            summary=summary,
            message=message,
            finished=True,
        )
        print("=== Done: published ===")
        return 0
    except Exception as exc:  # noqa: BLE001 - record any failure before re-raising
        record_run(engine, run_id, "failed", message=str(exc)[:500], finished=True)
        print(f"=== Failed: {exc} ===", file=sys.stderr)
        raise


if __name__ == "__main__":
    sys.exit(main())
