#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from collections import defaultdict

import psycopg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prune old Neon dataset releases while keeping the latest snapshot per dataset/year bucket"
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--keep-year",
        action="append",
        type=int,
        required=True,
        help="Reporting year to retain. May be passed multiple times.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the prune. Without this flag, prints a dry-run summary only.",
    )
    return parser.parse_args()


def fetch_referencing_tables(conn: psycopg.Connection) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        select c.table_schema, c.table_name
        from information_schema.columns c
        join information_schema.tables t
          on t.table_schema = c.table_schema
         and t.table_name = c.table_name
        where c.column_name = 'dataset_release_id'
          and c.table_schema in ('raw', 'core')
          and t.table_type = 'BASE TABLE'
        order by c.table_schema, c.table_name
        """
    ).fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def fetch_prune_candidates(conn: psycopg.Connection, keep_years: set[int]) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        with ranked as (
          select
            dataset_release_id,
            domain_code,
            dataset_code,
            reporting_year,
            snapshot_label,
            row_count,
            row_number() over (
              partition by domain_code, dataset_code, reporting_year
              order by snapshot_label desc, dataset_release_id desc
            ) as snapshot_rank
          from meta.dataset_release
        )
        select
          dataset_release_id,
          domain_code,
          dataset_code,
          reporting_year,
          snapshot_label,
          row_count,
          snapshot_rank
        from ranked
        order by domain_code, dataset_code, reporting_year nulls first, snapshot_label desc, dataset_release_id desc
        """
    ).fetchall()

    candidates: list[dict[str, object]] = []
    for row in rows:
        release_id = int(row[0])
        reporting_year = row[3]
        snapshot_rank = int(row[6])
        drop = False
        reason = ""

        if reporting_year is None:
            if snapshot_rank > 1:
                drop = True
                reason = "superseded-null-year"
        elif int(reporting_year) not in keep_years:
            drop = True
            reason = "outside-retention-window"
        elif snapshot_rank > 1:
            drop = True
            reason = "superseded-same-year"

        if drop:
            candidates.append(
                {
                    "dataset_release_id": release_id,
                    "domain_code": str(row[1]),
                    "dataset_code": str(row[2]),
                    "reporting_year": reporting_year,
                    "snapshot_label": str(row[4]),
                    "row_count": int(row[5] or 0),
                    "reason": reason,
                }
            )
    return candidates


def count_rows_for_release(
    conn: psycopg.Connection, tables: list[tuple[str, str]], dataset_release_id: int
) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = {}
    for schema_name, table_name in tables:
        sql = f"select count(*) from {schema_name}.{table_name} where dataset_release_id = %s"
        count = int(conn.execute(sql, (dataset_release_id,)).fetchone()[0])
        if count > 0:
            counts[(schema_name, table_name)] = count
    return counts


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    keep_years = set(args.keep_year)
    with psycopg.connect(args.database_url) as conn:
        referencing_tables = fetch_referencing_tables(conn)
        candidates = fetch_prune_candidates(conn, keep_years)

        row_counts_by_release: dict[int, dict[tuple[str, str], int]] = {}
        aggregate_counts: dict[tuple[str, str], int] = defaultdict(int)
        for candidate in candidates:
            counts = count_rows_for_release(conn, referencing_tables, int(candidate["dataset_release_id"]))
            row_counts_by_release[int(candidate["dataset_release_id"])] = counts
            for table_key, count in counts.items():
                aggregate_counts[table_key] += count

        print("## Neon Dataset Release Prune")
        print()
        print(f"Retained reporting years: {', '.join(str(year) for year in sorted(keep_years))}")
        print(f"Dry run: {'no' if args.execute else 'yes'}")
        print()
        print(f"Candidate releases: {len(candidates)}")
        print()
        print("| Release ID | Domain | Dataset | Year | Snapshot | Rows | Reason |")
        print("|---:|---|---|---:|---|---:|---|")
        if not candidates:
            print("| none | | | | | | |")
        for candidate in candidates:
            year_text = "n/a" if candidate["reporting_year"] is None else str(candidate["reporting_year"])
            print(
                f"| {candidate['dataset_release_id']} | {candidate['domain_code']} | {candidate['dataset_code']} | "
                f"{year_text} | {candidate['snapshot_label']} | {candidate['row_count']} | {candidate['reason']} |"
            )

        print()
        print("### Referencing Rows")
        print()
        print("| Table | Rows to delete |")
        print("|---|---:|")
        if not aggregate_counts:
            print("| none | 0 |")
        else:
            for (schema_name, table_name), count in sorted(aggregate_counts.items()):
                print(f"| {schema_name}.{table_name} | {count} |")

        if not args.execute or not candidates:
            return

        for schema_name, table_name in referencing_tables:
            release_ids = [
                candidate["dataset_release_id"]
                for candidate in candidates
                if row_counts_by_release[int(candidate["dataset_release_id"])].get((schema_name, table_name))
            ]
            if not release_ids:
                continue
            sql = f"delete from {schema_name}.{table_name} where dataset_release_id = any(%s)"
            conn.execute(sql, (release_ids,))

        release_ids = [candidate["dataset_release_id"] for candidate in candidates]
        conn.execute("delete from meta.dataset_release where dataset_release_id = any(%s)", (release_ids,))
        conn.commit()

        print()
        print(f"Deleted {len(release_ids)} dataset releases.")


if __name__ == "__main__":
    main()
