#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = ROOT / "db" / "schema.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply db/schema.sql to a Postgres database")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--schema-file",
        type=Path,
        default=DEFAULT_SCHEMA,
        help="SQL file to execute",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    sql = args.schema_file.read_text(encoding="utf-8")
    with psycopg.connect(args.database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, prepare=False)
    print(f"Applied {args.schema_file.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
