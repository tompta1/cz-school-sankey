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
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "social"

SOURCES = {
    "mf_chapter_results": ("Ministerstvo financí ČR", "https://mf.gov.cz"),
    "social_stats": ("MPSV / ČSSZ", "https://data.mpsv.cz"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load downloaded social files into raw Postgres tables")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=["social_mpsv_aggregates", "social_recipient_metrics"],
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
            values (%s, 'social', %s, %s, %s, %s, %s, %s, 0, %s, 'staged')
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


def load_social_mpsv_aggregates(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "mf_chapter_results", *SOURCES["mf_chapter_results"])
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="social_mpsv_aggregates",
        snapshot_label=snapshot_label,
        source_url=sidecar.get("source_url"),
        local_path=path,
        metadata=sidecar,
        content_sha256=sidecar.get("sha256"),
        reporting_year=sidecar.get("reporting_year"),
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.social_mpsv_aggregate where dataset_release_id = %s", (dataset_release_id,))
        inserted = 0
        for row in read_rows(path):
            cur.execute(
                """
                insert into raw.social_mpsv_aggregate (
                  dataset_release_id,
                  reporting_year,
                  chapter_code,
                  chapter_name,
                  metric_group,
                  metric_code,
                  metric_name,
                  amount_czk,
                  source_url,
                  payload
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dataset_release_id,
                    int(row["reporting_year"]),
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


def load_social_recipient_metrics(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "social_stats", *SOURCES["social_stats"])
    reporting_years = sidecar.get("years") or []
    reporting_year = max(reporting_years) if reporting_years else None
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="social_recipient_metrics",
        snapshot_label=snapshot_label,
        source_url=(sidecar.get("sources") or [None])[0],
        local_path=path,
        metadata=sidecar,
        content_sha256=sidecar.get("sha256"),
        reporting_year=reporting_year,
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.social_recipient_metric where dataset_release_id = %s", (dataset_release_id,))
        inserted = 0
        for row in read_rows(path):
            cur.execute(
                """
                insert into raw.social_recipient_metric (
                  dataset_release_id,
                  reporting_year,
                  metric_code,
                  metric_name,
                  denominator_kind,
                  recipient_count,
                  source_url,
                  payload
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dataset_release_id,
                    int(row["reporting_year"]),
                    row["metric_code"],
                    row["metric_name"],
                    row["denominator_kind"],
                    parse_decimal(row["recipient_count"]),
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

    datasets = args.dataset or ["social_mpsv_aggregates", "social_recipient_metrics"]
    loaders = []
    for dataset_code in datasets:
        data_path = latest_data_path(dataset_code)
        if data_path is None:
            raise SystemExit(f"No downloaded file found for dataset {dataset_code}")
        if dataset_code == "social_mpsv_aggregates":
            loaders.append((dataset_code, data_path, load_social_mpsv_aggregates))
        if dataset_code == "social_recipient_metrics":
            loaders.append((dataset_code, data_path, load_social_recipient_metrics))

    with psycopg.connect(args.database_url, autocommit=False) as conn:
        for dataset_code, path, loader in loaders:
            dataset_release_id, row_count = loader(conn, path)
            conn.commit()
            print(f"{dataset_code}: loaded {row_count} rows into dataset_release_id={dataset_release_id}")


if __name__ == "__main__":
    main()
