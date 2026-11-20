"""Create a small SQLite database to dry-run the pipeline without SQL Server.

  python scripts/make_demo_source.py
  # then in .env:  SOURCE_URL=sqlite:///demo_source.db   SOURCE_SCHEMAS=main
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import sqlalchemy as sa

OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("demo_source.db")


def main() -> None:
    OUT.unlink(missing_ok=True)
    engine = sa.create_engine(f"sqlite:///{OUT}")
    md = sa.MetaData()
    customers = sa.Table(
        "Customers",
        md,
        sa.Column("CustomerID", sa.Integer, primary_key=True),
        sa.Column("FullName", sa.String(80)),
        sa.Column("Email", sa.String(120)),
        sa.Column("CreatedAt", sa.DateTime),
    )
    orders = sa.Table(
        "Orders",
        md,
        sa.Column("OrderID", sa.Integer, primary_key=True),
        sa.Column("CustomerID", sa.Integer),
        sa.Column("Amount", sa.Numeric(10, 2)),
        sa.Column("Status", sa.String(20)),
        sa.Column("OrderedAt", sa.DateTime),
    )
    md.create_all(engine)
    base = datetime(2026, 1, 1, 9, 0, 0)
    with engine.begin() as conn:
        conn.execute(
            customers.insert(),
            [
                {
                    "CustomerID": i,
                    "FullName": f"  Customer {i}  " if i % 3 else "",
                    "Email": f"customer{i}@example.com ",
                    "CreatedAt": base + timedelta(days=i),
                }
                for i in range(1, 41)
            ],
        )
        conn.execute(
            orders.insert(),
            [
                {
                    "OrderID": i,
                    "CustomerID": (i % 40) + 1,
                    "Amount": Decimal(f"{(i * 7) % 500}.{i % 100:02d}"),
                    "Status": " shipped " if i % 2 else "pending",
                    "OrderedAt": base + timedelta(hours=i * 5),
                }
                for i in range(1, 201)
            ],
        )
    print(f"Created {OUT} with Customers (40 rows) and Orders (200 rows)")


if __name__ == "__main__":
    main()
