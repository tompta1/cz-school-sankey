#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

import psycopg
from psycopg import sql


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Vacuum ETL tables so repeated refreshes reuse storage pages")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    with psycopg.connect(args.database_url, autocommit=True) as conn:
        tables = conn.execute(
            """
            select schemaname, tablename
            from pg_tables
            where schemaname in ('raw', 'core')
            order by pg_total_relation_size(format('%I.%I', schemaname, tablename)::regclass) desc
            """
        ).fetchall()

        print("### Neon ETL Vacuum")
        print()
        print(f"Vacuuming {len(tables)} raw/core tables to make dead-row pages reusable.")
        for schema_name, table_name in tables:
            conn.execute(
                sql.SQL("vacuum (analyze) {}.{}").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                )
            )
        print("Vacuum completed.")


if __name__ == "__main__":
    main()
