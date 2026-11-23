# SQL Server -> Postgres -> dbt -> Data Lake (MVP demo)

ELT demo. The `.bak` database restored in SQL Server is the source. Postgres is the transformation engine. Curated tables are published as Parquet to a data lake (a local folder by default, any S3-compatible store optionally).

```
SQL Server --(copy)--> Postgres raw --(dbt)--> staging --> export --(gate)--> Lake (Parquet + manifest)
                                         |                                        |
                                    dbt tests                               DuckDB query demo
                                         +---------- ops.pipeline_runs / ops.publish_log (audit)
```

Publishing is gated: the lake only receives data if `dbt build` (models + tests) passes.

## 1. Prerequisites

- Python 3.10+ and Docker (for Postgres)
- Your SQL Server reachable from this machine, with a **SQL login** (user + password). Windows authentication is not supported by this demo. Enable a login in SSMS or the VS Code MSSQL extension, and make sure TCP/IP is enabled on port 1433.

## 2. Setup

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d               # Postgres 16 on localhost:5432
cp .env.example .env               # Windows: copy .env.example .env
```

Edit `.env`: set `MSSQL_*` to your instance and database, and `SOURCE_SCHEMAS` (default `dbo`).

To check SQL Server, MySQL, and/or PostgreSQL, set the corresponding `MSSQL_*`, `MYSQL_*`, and `PG_*` values in `.env`, then run:

```bash
python scripts/test_db_connections.py
```

No SQL Server handy? Dry-run first with a SQLite stand-in:

```bash
python scripts/make_demo_source.py
# in .env set:  SOURCE_URL=sqlite:///demo_source.db   and   SOURCE_SCHEMAS=main
```

## 3. Run it

```bash
python extract_load.py --max-tables 5     # 1. copy tables into Postgres `raw`, reconcile row counts
python gen_dbt_stubs.py --export <t1,t2>  # 2. generate dbt sources/staging/export models + tests
python run_pipeline.py                    # 3. load -> dbt build -> publish (gated)
python query_lake.py                      # 4. query what landed in the lake
python query_lake.py "select count(*) from <export_table>"
```

Start with a handful of tables (`--tables a,b` or `--max-tables N`). Use the raw table names from `reports/load_report.json` (lower-cased, snake_case) for `--export`.

Exit codes of `run_pipeline.py`: `0` published, `2` blocked by failing dbt tests, `1` other failure.

## 4. Demo script (5-7 minutes)

1. **Source**: show the restored database in the MSSQL extension.
2. **Load**: `python extract_load.py` and point at the row-count reconciliation line.
3. **Transform**: `python gen_dbt_stubs.py`, open one `stg_` and one `exp_` model. Explain staging (clean) vs export (curated).
4. **Publish**: `python run_pipeline.py`. Show the lake folder: `curated/<table>/run_id=<id>/part-0000.parquet` plus `_manifests/latest.json`.
5. **Query the lake**: `python query_lake.py "select ..."` (DuckDB reading Parquet directly).
6. **Break it**: `python run_pipeline.py --inject-fault`. A source row is duplicated, the dbt `unique` test fails, publishing is blocked, `latest.json` still points at the previous good run. Re-run without the flag to recover.
7. **Audit**: `select * from ops.pipeline_runs order by started_at desc;` in Postgres.

## 5. Publishing to an S3-compatible lake

Set in `.env`: `LAKE_URI=s3://bucket/prefix`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`. The S3 path is implemented but **not tested** in this build. MinIO's community edition is archived and its Docker images were withdrawn, so it is not included. Use a local folder for the demo and say "this folder is the lake; in production it is object storage".

## 6. Behavior and limits you should know

- Full reload each run (tables are replaced). No incremental loads, no CDC, no deletes handling.
- Column types are mapped explicitly. Unsupported types (binary, xml, geography, hierarchyid, rowversion) are skipped and listed in `reports/load_report.json`. `NUMERIC/DECIMAL/MONEY` keep precision and scale through to Parquet decimals.
- `unique` / `not_null` tests are generated only for single-column primary keys. Composite keys and tables without a primary key get no tests until you add them.
- Generated stub models only trim text and convert empty strings to NULL. Replace the export SQL with real business logic (copy the model elsewhere before hand-editing: re-running `gen_dbt_stubs.py` overwrites generated files).
- A lake publish is a set of Parquet files plus a manifest written last. Readers must go through `_manifests/latest.json` (as `query_lake.py` does). This is not a table format (no Iceberg/Delta).
- Tested end to end against Postgres 16 with a SQLite source. **Not yet tested against a real SQL Server / `pymssql`.** If `pymssql` fails to install on your Python version, set `SOURCE_URL` to an `mssql+pyodbc://...` URL and `pip install pyodbc` (requires the Microsoft ODBC driver).
