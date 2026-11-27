# WideWorldImporters: SQL Server → dbt → Databricks (with a semantic layer for AI)

ELT pipeline over the WideWorldImporters sample database. Data is copied from SQL Server,
transformed and tested with dbt, and published to Databricks Unity Catalog only when
every test passes. A dbt semantic layer defines the business metrics, so AI assistants
(through the dbt MCP server) and BI tools all get the same numbers.

```
                         ┌─ target postgres ──── dbt on Postgres ── local lake (Parquet) ─┐
SQL Server ─▶ Postgres raw                                                                 ├─(gate)─▶ Databricks wwi_curated
                         └─ target databricks ─▶ Databricks wwi_raw ── dbt on Databricks ──┘
                                                                  (wwi_staging, wwi_marts)
      every run is logged in Postgres ops.pipeline_runs
```

**The gate:** consumers read `wwi_curated` (and the local lake's `latest.json`). They are only
updated after `dbt build` (models + tests) succeeds, and only with the models built in that run.

## Contents

| Layer | What | Where |
|---|---|---|
| Raw | 47 source tables, row counts reconciled | Postgres `raw`, Databricks `wwi_raw` |
| Staging | One cleaned model per source table (generated) | `models/staging/`, schema `staging` / `wwi_staging` |
| Marts | `dim_customer`, `dim_stock_item`, `dim_date`, `fct_sales`, `fct_order_fulfillment` | `models/marts/`, schema `marts` / `wwi_marts` |
| Semantic layer | 18 metrics (revenue, gross margin, on-time delivery, fill rate...) on 4 semantic models | `models/marts/_semantic.yml` |
| Published | Tested marts for consumers | Databricks `wwi_curated`, local `lake/` |

## 1. Setup

Prerequisites: Python 3.10+, SQL Server reachable with a SQL login (the `sql_local` Docker
container with WideWorldImporters restored), a Postgres database, and optionally a Databricks
workspace with a SQL warehouse and a personal access token.

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then fill in MSSQL_*, PG_*, DATABRICKS_*
```

If SQL Server does not start after a reboot: `docker start sql_local` (and
`docker update --restart unless-stopped sql_local` once).

## 2. Run the pipeline

```bash
# Postgres target (dbt on Postgres; publishes to the local lake and Databricks wwi_curated)
python main.py                       # full load from SQL Server (~3 min)
python main.py --skip-load           # reuse the data already loaded
python main.py --skip-databricks     # local lake only

# Databricks target (Phase 2: dbt runs on Databricks; promotes tested marts to wwi_curated)
python main.py --target databricks
python main.py --target databricks --skip-load

# Show the gate: duplicate a row, a unique test fails, nothing is published (exit code 2)
python main.py --target databricks --skip-load --inject-fault
python load_databricks_raw.py --tables application_cities   # repair the broken table
```

Exit codes: `0` published, `2` blocked by failing dbt tests, `1` other failure.

Individual steps, if needed:

| Script | Does |
|---|---|
| `extract_load.py` | SQL Server → Postgres `raw`; writes `reports/load_report.json` (partial `--tables` loads are merged in) |
| `gen_dbt_stubs.py --export sales_orders` | Regenerates `models/staging` from the load report (run after the source schema changes) |
| `load_databricks_raw.py` | Postgres `raw` → Databricks `wwi_raw` |
| `publish_lake.py` / `publish_bricks.py` | Publish steps; normally called by `main.py` after the tests pass |
| `query_lake.py "select ..."` | Query the local lake with DuckDB |
| `scripts/test_db_connections.py` | Check the SQL Server, MySQL and Postgres connections in `.env` |

## 3. Developing a model

```bash
set -a && . ./.env && set +a
cd dbt_project
dbt build -s fct_order_fulfillment --profiles-dir .                    # Postgres
dbt build -s fct_order_fulfillment --profiles-dir . --target databricks
```

Then run `python main.py --skip-load` (or `--target databricks --skip-load`) to publish
through the gate. Hand-written models go in `models/marts/`; `models/staging/` is regenerated
by `gen_dbt_stubs.py`. Use the cross-database macros in `macros/json.sql` and dbt's
`dbt.datediff` / `dbt.date_trunc` so models run on both Postgres and Databricks.

## 4. Semantic layer (metrics)

`models/marts/_semantic.yml` defines the metrics once:

| Area | Metrics |
|---|---|
| Sales | `revenue`, `revenue_incl_tax`, `gross_profit`, `gross_margin_pct`, `units_sold`, `invoice_count`, `average_invoice_value` |
| Fulfillment | `order_count`, `delivered_order_count`, `on_time_delivery_rate`, `backorder_rate`, `avg_days_to_deliver`, `fill_rate` (+ helpers) |

They can be sliced by `metric_time` (day to year) and by customer and product attributes,
e.g. `customer__sales_territory`, `customer__customer_category`, `stock_item__supplier_name`.

Query them locally with MetricFlow (install in a separate virtualenv:
`pip install "dbt-metricflow[dbt-postgres]"`):

```bash
cd dbt_project && export DBT_PROFILES_DIR=$PWD
mf query --metrics revenue,gross_margin_pct --group-by metric_time__year
mf query --metrics on_time_delivery_rate --group-by customer__sales_territory
```

Reference values (verified against the source): revenue 2013 = 45,707,188, 2014 = 49,929,487,
2015 = 53,991,490, 2016 (Jan–May) = 22,633,176; on-time delivery 93.65%; fill rate 96.13%.

## 5. dbt MCP server (local)

`.mcp.json` registers the open-source dbt MCP server (`scripts/dbt_mcp.sh`) for AI clients
started in this repository (Claude Code, VS Code, Cursor). Credentials are read from `.env`.

Only read-only tools are switched on (`DBT_MCP_ENABLE_TOOLS` in `scripts/dbt_mcp.sh`):
`list`, `parse`, `compile`, `get_lineage_dev`, `get_node_details_dev`. Anything else, such as
`build` or `show`, is rejected. `DBT_TARGET` in `.env` picks Postgres (`dev`) or `databricks`.

Metric querying through MCP (`list_metrics`, `query_metrics`...) needs dbt Platform; see below.

## 6. dbt Platform (remote MCP server, metric queries)

These steps need your browser and a dbt Platform account; menu names may differ slightly.

1. **Account**: sign up at getdbt.com and create a project.
2. **Connection**: Databricks. Host = `DATABRICKS_HOST` without `https://`; HTTP path =
   `/sql/1.0/warehouses/<DATABRICKS_WAREHOUSE_ID>`; catalog = `workspace`.
3. **Repository**: connect GitHub and pick this repository. Set **Project subdirectory** to
   `dbt_project`.
4. **Development credentials**: a Databricks token; schema `wwi_default`. The project's
   `generate_schema_name` macro builds into `wwi_staging` / `wwi_marts` on Databricks.
5. **Production environment** + a **job** running `dbt build` (daily, or after
   `python main.py --target databricks` has refreshed `wwi_raw`).
6. **Semantic Layer**: in the project settings, configure it for the production environment
   and create a **read-only service token** (Semantic Layer + Metadata only).
7. **Remote MCP**: connect the AI client (Gemini Enterprise, Claude...) to the dbt remote MCP
   server with that service token and the production environment ID. Enable only the
   discovery and semantic-layer tools for the POC.

Raw data still arrives through this repository (`main.py --target databricks` or
`load_databricks_raw.py`); dbt Platform takes over the transformation and testing.

## 7. Behaviour and limits

- Full reload each run; no incremental loads, CDC or delete handling.
- Unsupported source types (binary, xml, geography, hierarchyid) are skipped and listed in
  `reports/load_report.json`. `Warehouse.ColdRoomTemperatures_Archive` (3.65M rows, unused)
  is excluded in `extract_load.py`.
- Generated `unique` / `not_null` tests exist only for single-column primary keys; the
  `*_Archive` tables have none.
- Each publish replaces the `wwi_curated` tables; Delta history keeps earlier versions
  (`DESCRIBE HISTORY workspace.wwi_curated.fct_sales`). Old lake runs stay in `lake/` until deleted.
- On Databricks, dbt rebuilds `wwi_marts` before the tests run; consumers must read
  `wwi_curated`, which only changes after the tests pass.
