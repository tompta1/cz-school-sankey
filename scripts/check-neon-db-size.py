#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

import psycopg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Neon database size before an ETL run."
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--max-size-mb",
        type=float,
        default=450.0,
        help="Fail when the current database size exceeds this threshold in MB. Use 0 to disable.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    with psycopg.connect(args.database_url) as conn:
        row = conn.execute(
            """
            select
              current_database(),
              pg_database_size(current_database()) as size_bytes
            """
        ).fetchone()

    database_name = str(row[0])
    size_bytes = int(row[1])
    size_mb = size_bytes / (1024 * 1024)
    max_size_mb = float(args.max_size_mb)

    print("### Neon Storage Guard")
    print()
    print(f"- Database: `{database_name}`")
    print(f"- Current size: `{size_mb:.1f} MB`")
    if max_size_mb > 0:
        headroom_mb = max_size_mb - size_mb
        print(f"- Guard threshold: `{max_size_mb:.1f} MB`")
        print(f"- Headroom before guard fails: `{headroom_mb:.1f} MB`")
    else:
        print("- Guard threshold: disabled")

    if max_size_mb > 0 and size_mb > max_size_mb:
        raise SystemExit(
            f"Database size guard failed: {size_mb:.1f} MB is above the configured threshold of {max_size_mb:.1f} MB."
        )


if __name__ == "__main__":
    main()
