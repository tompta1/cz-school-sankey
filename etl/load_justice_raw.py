#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "justice"

SOURCES = {
    "justice_budget_docs": ("Ministerstvo spravedlnosti ČR", "https://msp.gov.cz"),
    "justice_activity_docs": ("Ministerstvo spravedlnosti / VS ČR", "https://msp.gov.cz"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load downloaded justice files into raw Postgres tables")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=["justice_budget_aggregates", "justice_activity_aggregates"],
        help="Restrict loading to one or more dataset codes.",
    )
    return parser.parse_args()


def parse_decimal(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    text = value.strip()
    if not text:
        return Decimal("0")
    return Decimal(text)


def latest_data_path(dataset_code: str) -> Path | None:
    dataset_dir = RAW_ROOT / dataset_code
    if not dataset_dir.exists():
        return None
    candidates = [
        path
        for path in dataset_dir.iterdir()
        if path.is_file() and not path.name.endswith(".download.json")
    ]
    if not candidates:
        return None
    return sorted(candidates)[-1]


def snapshot_label_for(path: Path) -> str:
    return path.name.split("__", 1)[0]


def sidecar_for(path: Path) -> dict:
    sidecar_path = Path(str(path) + ".download.json")
    if not sidecar_path.exists():
        return {}
    return json.loads(sidecar_path.read_text(encoding="utf-8"))


def read_rows(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        yield from csv.DictReader(handle)


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
    dataset_code: str,
    snapshot_label: str,
    source_url: str | None,
    local_path: Path,
    metadata: dict,
    content_sha256: str | None,
    reporting_year: int | None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into meta.dataset_release (
              source_system_id,
              domain_code,
              dataset_code,
              reporting_year,
              snapshot_label,
              source_url,
              local_path,
              content_sha256,
              row_count,
              metadata,
              status
            )
            values (%s, 'justice', %s, %s, %s, %s, %s, %s, 0, %s, 'staged')
            on conflict (domain_code, dataset_code, snapshot_label) do update
              set source_url = excluded.source_url,
                  local_path = excluded.local_path,
                  content_sha256 = excluded.content_sha256,
                  reporting_year = excluded.reporting_year,
                  metadata = excluded.metadata,
                  status = excluded.status
            returning dataset_release_id
            """,
            (
                source_system_id,
                dataset_code,
                reporting_year,
                snapshot_label,
                source_url,
                str(local_path.relative_to(ROOT)),
                content_sha256,
                Jsonb(metadata),
            ),
        )
        return int(cur.fetchone()[0])


def finalize_dataset_release(conn: psycopg.Connection, *, dataset_release_id: int, row_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update meta.dataset_release
            set row_count = %s,
                status = 'staged'
            where dataset_release_id = %s
            """,
            (row_count, dataset_release_id),
        )


def load_justice_budget_aggregates(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "justice_budget_docs", *SOURCES["justice_budget_docs"])
    reporting_years = sidecar.get("years") or []
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="justice_budget_aggregates",
        snapshot_label=snapshot_label,
        source_url=(sidecar.get("sources") or [{}])[0].get("source_url"),
        local_path=path,
        metadata=sidecar,
        content_sha256=None,
        reporting_year=max(reporting_years) if reporting_years else None,
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.justice_budget_aggregate where dataset_release_id = %s", (dataset_release_id,))
        inserted = 0
        for row in read_rows(path):
            cur.execute(
                """
                insert into raw.justice_budget_aggregate (
                  dataset_release_id,
                  reporting_year,
                  basis,
                  chapter_code,
                  chapter_name,
                  metric_group,
                  metric_code,
                  metric_name,
                  amount_czk,
                  source_url,
                  payload
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dataset_release_id,
                    int(row["reporting_year"]),
                    row["basis"],
                    row["chapter_code"],
                    row["chapter_name"],
                    row["metric_group"],
                    row["metric_code"],
                    row["metric_name"],
                    parse_decimal(row["amount_czk"]),
                    row.get("source_url"),
                    Jsonb(row),
                ),
            )
            inserted += 1

    finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=inserted)
    return dataset_release_id, inserted


def load_justice_activity_aggregates(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "justice_activity_docs", *SOURCES["justice_activity_docs"])
    reporting_years = sidecar.get("years") or []
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="justice_activity_aggregates",
        snapshot_label=snapshot_label,
        source_url=(sidecar.get("sources") or [{}])[0].get("source_url"),
        local_path=path,
        metadata=sidecar,
        content_sha256=(sidecar.get("sources") or [{}])[0].get("sha256"),
        reporting_year=max(reporting_years) if reporting_years else None,
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.justice_activity_aggregate where dataset_release_id = %s", (dataset_release_id,))
        inserted = 0
        for row in read_rows(path):
            cur.execute(
                """
                insert into raw.justice_activity_aggregate (
                  dataset_release_id,
                  reporting_year,
                  activity_domain,
                  metric_code,
                  metric_name,
                  count_value,
                  source_url,
                  payload
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dataset_release_id,
                    int(row["reporting_year"]),
                    row["activity_domain"],
                    row["metric_code"],
                    row["metric_name"],
                    parse_decimal(row["count_value"]),
                    row.get("source_url"),
                    Jsonb(row),
                ),
            )
            inserted += 1

    finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=inserted)
    return dataset_release_id, inserted


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    requested = args.dataset or ["justice_budget_aggregates", "justice_activity_aggregates"]
    loaders = {
        "justice_budget_aggregates": load_justice_budget_aggregates,
        "justice_activity_aggregates": load_justice_activity_aggregates,
    }

    with psycopg.connect(args.database_url) as conn:
        conn.autocommit = False
        for dataset_code in requested:
            data_path = latest_data_path(dataset_code)
            if data_path is None:
                raise SystemExit(f"No downloaded file found for {dataset_code}")
            dataset_release_id, inserted = loaders[dataset_code](conn, data_path)
            conn.commit()
            print(f"{dataset_code}: dataset_release_id={dataset_release_id} rows={inserted}")


if __name__ == "__main__":
    main()
