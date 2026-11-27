#!/usr/bin/env bash
# Start the local dbt MCP server (open-source dbt-mcp) for this project.
#
# Used by .mcp.json, so AI clients (Claude Code, VS Code, Cursor...) can ask about
# the dbt project: models, lineage, compiled SQL. Credentials stay in .env; this
# script loads them so dbt can connect, and nothing secret goes into .mcp.json.
#
# Tool allowlist (governance): only read-only tools are switched on. dbt-mcp registers
# ONLY the tools named in DBT_MCP_ENABLE_TOOLS. To let the assistant run tests or
# builds, add e.g. "test" or "build" deliberately; "show" runs SQL against the warehouse.
#
# Target: DBT_TARGET in .env picks the profiles.yml output (dev = Postgres, databricks).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

export DBT_PROJECT_DIR="$ROOT/dbt_project"
export DBT_PROFILES_DIR="$ROOT/dbt_project"
export DBT_PATH="$ROOT/venv/bin/dbt"
export DBT_MCP_ENABLE_TOOLS="${DBT_MCP_ENABLE_TOOLS:-list,parse,compile,get_lineage_dev,get_node_details_dev}"
export DO_NOT_TRACK=1

exec uvx --from "dbt-mcp==2.10.3" dbt-mcp
