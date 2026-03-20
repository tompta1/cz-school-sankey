#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw"

SOURCES = {
    "msmt": ("MŠMT", "https://www.msmt.cz"),
    "dotaceeu": ("DotaceEU", "https://www.dotaceeu.cz"),
    "monitor": ("MONITOR", "https://monitor.statnipokladna.gov.cz"),
}

DATASETS = [
    {
        "table": "raw.school_entities",
        "dataset_code": "school_entities",
        "source_code": "msmt",
        "filename": "school_entities.csv",
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "institution_id",
            "institution_name",
            "ico",
            "founder_id",
            "founder_name",
            "founder_type",
            "municipality",
            "region",
            "capacity",
            "payload",
        ],
        "build_rows": lambda release_id, year, rows, path: [
            (
                release_id,
                year,
                row["institution_id"],
                row["institution_name"],
                row.get("ico") or None,
                row.get("founder_id") or None,
                row.get("founder_name") or None,
                row.get("founder_type") or None,
                row.get("municipality") or None,
                row.get("region") or None,
                int(row["capacity"]) if row.get("capacity", "").isdigit() else None,
                Jsonb({"local_path": str(path.relative_to(ROOT))}),
            )
            for row in rows
        ],
    },
    {
        "table": "raw.school_allocations",
        "dataset_code": "school_allocations",
        "source_code": "msmt",
        "filename": "msmt_allocations.csv",
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "institution_id",
            "ico",
            "pedagogical_amount",
            "nonpedagogical_amount",
            "oniv_amount",
            "other_amount",
            "operations_amount",
            "investment_amount",
            "bucket_basis",
            "bucket_certainty",
            "payload",
        ],
        "build_rows": lambda release_id, year, rows, path: [
            (
                release_id,
                year,
                row.get("institution_id") or None,
                row.get("ico") or None,
                int(row.get("pedagogical_amount") or 0),
                int(row.get("nonpedagogical_amount") or 0),
                int(row.get("oniv_amount") or 0),
                int(row.get("other_amount") or 0),
                int(row.get("operations_amount") or 0),
                int(row.get("investment_amount") or 0),
                row.get("bucket_basis") or None,
                row.get("bucket_certainty") or None,
                Jsonb({"local_path": str(path.relative_to(ROOT))}),
            )
            for row in rows
        ],
    },
    {
        "table": "raw.school_eu_projects",
        "dataset_code": "school_eu_projects",
        "source_code": "dotaceeu",
        "filename": "eu_projects.csv",
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "institution_id",
            "ico",
            "programme",
            "project_name",
            "amount_czk",
            "basis",
            "certainty",
            "payload",
        ],
        "build_rows": lambda release_id, year, rows, path: [
            (
                release_id,
                year,
                row.get("institution_id") or None,
                row.get("ico") or None,
                row["programme"],
                row["project_name"],
                int(row.get("amount") or 0),
                row.get("basis") or None,
                row.get("certainty") or None,
                Jsonb({"local_path": str(path.relative_to(ROOT))}),
            )
            for row in rows
        ],
    },
    {
        "table": "raw.school_founder_support",
        "dataset_code": "school_founder_support",
        "source_code": "monitor",
        "filename": "founder_support.csv",
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "institution_id",
            "ico",
            "amount_czk",
            "basis",
            "certainty",
            "note",
            "payload",
        ],
        "build_rows": lambda release_id, year, rows, path: [
            (
                release_id,
                year,
                row.get("institution_id") or None,
                row.get("ico") or None,
                int(row.get("amount") or 0),
                row.get("basis") or None,
                row.get("certainty") or None,
                row.get("note") or None,
                Jsonb({"local_path": str(path.relative_to(ROOT))}),
            )
            for row in rows
        ],
    },
    {
        "table": "raw.school_state_budget",
        "dataset_code": "school_state_budget",
        "source_code": "monitor",
        "filename": "state_budget.csv",
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "node_id",
            "node_name",
            "node_category",
            "flow_type",
            "amount_czk",
            "basis",
            "certainty",
            "source_url",
            "payload",
        ],
        "build_rows": lambda release_id, year, rows, path: [
            (
                release_id,
                year,
                row["node_id"],
                row["node_name"],
                row.get("node_category") or None,
                row["flow_type"],
                int(row.get("amount_czk") or 0),
                row.get("basis") or None,
                row.get("certainty") or None,
                row.get("source_url") or None,
                Jsonb({"local_path": str(path.relative_to(ROOT))}),
            )
            for row in rows
        ],
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load current school CSV outputs into raw Postgres tables")
    parser.add_argument("--year", type=int, required=True, help="Budget year under etl/data/raw/<year>")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    return parser.parse_args()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def upsert_source_system(conn: psycopg.Connection, code: str, name: str, base_url: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into meta.source_system (code, name, base_url)
            values (%s, %s, %s)
            on conflict (code) do update
              set name = excluded.name,
                  base_url = excluded.base_url
            returning source_system_id
            """,
            (code, name, base_url),
        )
        return int(cur.fetchone()[0])


def upsert_dataset_release(
    conn: psycopg.Connection,
    *,
    source_system_id: int,
    year: int,
    dataset_code: str,
    source_url: str,
    local_path: Path,
    row_count: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into meta.dataset_release (
              source_system_id,
              domain_code,
              dataset_code,
              reporting_year,
              period_code,
              snapshot_label,
              source_url,
              local_path,
              row_count,
              metadata,
              status
            )
            values (%s, 'school', %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'staged')
            on conflict (domain_code, dataset_code, snapshot_label) do update
              set row_count = excluded.row_count,
                  local_path = excluded.local_path,
                  source_url = excluded.source_url,
                  metadata = excluded.metadata,
                  status = excluded.status
            returning dataset_release_id
            """,
            (
                source_system_id,
                dataset_code,
                year,
                str(year),
                f"{year}-local",
                source_url,
                str(local_path.relative_to(ROOT)),
                row_count,
                '{"loader":"etl/load_school_raw.py"}',
            ),
        )
        return int(cur.fetchone()[0])


def load_dataset(conn: psycopg.Connection, *, year: int, config: dict[str, object]) -> int:
    path = RAW_ROOT / str(year) / str(config["filename"])
    if not path.exists():
        return 0

    rows = read_csv_rows(path)
    source_code = str(config["source_code"])
    source_name, source_url = SOURCES[source_code]
    source_system_id = upsert_source_system(conn, source_code, source_name, source_url)
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        year=year,
        dataset_code=str(config["dataset_code"]),
        source_url=source_url,
        local_path=path,
        row_count=len(rows),
    )

    with conn.cursor() as cur:
        cur.execute(f"delete from {config['table']} where dataset_release_id = %s", (dataset_release_id,))
        insert_sql = (
            f"insert into {config['table']} ({', '.join(config['columns'])}) "
            f"values ({', '.join(['%s'] * len(config['columns']))})"
        )
        built_rows = config["build_rows"](dataset_release_id, year, rows, path)
        cur.executemany(insert_sql, built_rows)
    return len(rows)


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    with psycopg.connect(args.database_url, autocommit=False) as conn:
        total_rows = 0
        for dataset in DATASETS:
            count = load_dataset(conn, year=args.year, config=dataset)
            total_rows += count
            print(f"{dataset['dataset_code']}: {count} rows")
        conn.commit()
    print(f"Loaded {total_rows} raw school rows for {args.year}")


if __name__ == "__main__":
    main()
