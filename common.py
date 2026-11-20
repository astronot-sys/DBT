"""Shared settings, paths and helpers for the demo pipeline."""
from __future__ import annotations

import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import sqlalchemy as sa
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from sqlalchemy.engine import URL

ROOT = Path(__file__).resolve().parent
DBT_DIR = ROOT / "dbt_project"
REPORT_DIR = ROOT / "reports"
CATALOG_PATH = REPORT_DIR / "load_report.json"

OPS_DDL: tuple[str, ...] = (
    "CREATE SCHEMA IF NOT EXISTS ops",
    """
    CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
        run_id text PRIMARY KEY,
        started_at timestamptz NOT NULL,
        finished_at timestamptz,
        status text NOT NULL,
        dbt_return_code integer,
        dbt_summary jsonb,
        message text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.publish_log (
        run_id text NOT NULL,
        table_name text NOT NULL,
        row_count bigint NOT NULL,
        lake_path text NOT NULL,
        published_at timestamptz NOT NULL,
        PRIMARY KEY (run_id, table_name)
    )
    """,
)


class Settings(BaseModel):
    """All configuration, read from environment variables or a .env file."""

    source_url: str | None = None
    mssql_host: str = "localhost"
    mssql_port: int = 1433
    mssql_user: str = "sa"
    mssql_password: str = ""
    mssql_db: str = ""
    source_schemas: list[str] = Field(default_factory=lambda: ["dbo"])
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_user: str = "demo"
    pg_password: str = "demo"
    pg_db: str = "demo"
    lake_uri: str = "./lake"
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "us-east-1"

    @classmethod
    def from_env(cls) -> Settings:
        load_dotenv(ROOT / ".env")
        data: dict[str, object] = {}
        for field_name in cls.model_fields:
            raw = os.environ.get(field_name.upper())
            if raw is None or raw == "":
                continue
            if field_name == "source_schemas":
                data[field_name] = [p.strip() for p in raw.split(",") if p.strip()]
            else:
                data[field_name] = raw
        return cls(**data)

    def source_engine(self) -> sa.Engine:
        if self.source_url:
            return sa.create_engine(self.source_url)
        if not self.mssql_db:
            raise ValueError("Set MSSQL_DB (or SOURCE_URL) in .env")
        url = URL.create(
            "mssql+pymssql",
            username=self.mssql_user,
            password=self.mssql_password,
            host=self.mssql_host,
            port=self.mssql_port,
            database=self.mssql_db,
        )
        return sa.create_engine(url)

    def pg_url(self) -> URL:
        query: dict[str, str] = {}
        host: str | None = self.pg_host
        if self.pg_host.startswith("/"):  # unix socket directory
            query["host"] = self.pg_host
            host = None
        return URL.create(
            "postgresql+psycopg2",
            username=self.pg_user,
            password=self.pg_password,
            host=host,
            port=self.pg_port,
            database=self.pg_db,
            query=query,
        )

    def pg_engine(self) -> sa.Engine:
        return sa.create_engine(self.pg_url(), pool_pre_ping=True)

    def dbt_env(self) -> dict[str, str]:
        return {
            "PG_HOST": self.pg_host,
            "PG_PORT": str(self.pg_port),
            "PG_USER": self.pg_user,
            "PG_PASSWORD": self.pg_password,
            "PG_DB": self.pg_db,
        }


def ensure_ops(engine: sa.Engine) -> None:
    with engine.begin() as conn:
        for ddl in OPS_DDL:
            conn.execute(sa.text(ddl))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_run_id() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def snake(name: str) -> str:
    """Normalise an identifier: CamelCase and spaces become snake_case."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    cleaned = re.sub(r"\W+", "_", spaced).strip("_").lower()
    return cleaned or "col"
