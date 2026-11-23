"""Check connectivity to the configured SQL Server and MySQL instances."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy.engine import URL

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def check(label: str, driver: str, settings: dict[str, str], required: tuple[str, ...]) -> bool | None:
    missing = [key for key in required if not settings.get(key)]
    if missing:
        print(f"{label}: SKIPPED (set {', '.join(missing)} in DBT_pipeline/.env)")
        return None

    try:
        url = URL.create(
            driver,
            username=settings["user"],
            password=settings["password"],
            host=settings["host"],
            port=int(settings["port"]),
            database=settings["database"],
        )
        timeout_option = "connect_timeout" if driver == "mysql+pymysql" else "timeout"
        engine = sa.create_engine(url, connect_args={timeout_option: 5})
        try:
            with engine.connect() as connection:
                connection.execute(sa.text("SELECT 1"))
        finally:
            engine.dispose()
        print(f"{label}: OK (connected to {settings['host']}:{settings['port']}/{settings['database']})")
        return True
    except Exception as exc:
        # Driver errors can include credentials; redact the configured password.
        detail = str(exc).replace(settings["password"], "[redacted]") if settings["password"] else str(exc)
        print(f"{label}: FAILED ({type(exc).__name__}: {detail})")
        return False


def main() -> int:
    mssql = {
        "host": os.getenv("MSSQL_HOST", "localhost"),
        "port": os.getenv("MSSQL_PORT", "1433"),
        "user": os.getenv("MSSQL_USER", ""),
        "password": os.getenv("MSSQL_PASSWORD", ""),
        "database": os.getenv("MSSQL_DB", ""),
    }
    mysql = {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": os.getenv("MYSQL_PORT", "3306"),
        "user": os.getenv("MYSQL_USER", ""),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DB", ""),
    }

    mssql_ok = check(
        "Microsoft SQL Server", "mssql+pymssql", mssql,
        ("user", "password", "database"),
    )
    mysql_ok = check(
        "MySQL", "mysql+pymysql", mysql,
        ("user", "password", "database"),
    )
    results = [result for result in (mssql_ok, mysql_ok) if result is not None]
    if not results:
        print("\nNo connection was configured. Set MSSQL_* and/or MYSQL_* values in .env.")
        return 1
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
