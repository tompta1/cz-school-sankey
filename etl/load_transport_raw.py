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
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "transport"

SOURCES = {
    "transport_monitor_entities": ("Monitor MF", "https://monitor.statnipokladna.gov.cz"),
    "transport_sfdi_projects": ("SFDI", "https://kz.sfdi.cz"),
    "transport_activity_metrics": ("SYDOS / eDalnice / CzechToll", "https://www.sydos.cz"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load downloaded transport files into raw Postgres tables")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=["transport_budget_entities", "transport_sfdi_projects", "transport_activity_metrics"],
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
            values (%s, 'transport', %s, %s, %s, %s, %s, %s, 0, %s, 'staged')
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


def load_transport_budget_entities(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "transport_monitor_entities", *SOURCES["transport_monitor_entities"])
    reporting_years = sidecar.get("years") or []
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="transport_budget_entities",
        snapshot_label=snapshot_label,
        source_url=sidecar.get("source_url"),
        local_path=path,
        metadata=sidecar,
        content_sha256=None,
        reporting_year=max(reporting_years) if reporting_years else None,
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.transport_budget_entity where dataset_release_id = %s", (dataset_release_id,))
        inserted = 0
        for row in read_rows(path):
            cur.execute(
                """
                insert into raw.transport_budget_entity (
                  dataset_release_id,
                  reporting_year,
                  period_code,
                  entity_ico,
                  entity_name,
                  entity_kind,
                  expenses_czk,
                  costs_czk,
                  revenues_czk,
                  result_czk,
                  source_url,
                  payload
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dataset_release_id,
                    int(row["reporting_year"]),
                    row["period_code"],
                    row["entity_ico"],
                    row["entity_name"],
                    row["entity_kind"],
                    parse_decimal(row["expenses_czk"]),
                    parse_decimal(row["costs_czk"]),
                    parse_decimal(row["revenues_czk"]),
                    parse_decimal(row["result_czk"]),
                    row.get("source_url"),
                    Jsonb(row),
                ),
            )
            inserted += 1

    finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=inserted)
    return dataset_release_id, inserted


def load_transport_sfdi_projects(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "transport_sfdi_projects", *SOURCES["transport_sfdi_projects"])
    reporting_years = sidecar.get("years") or []
    first_source = (sidecar.get("sources") or [{}])[0]
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="transport_sfdi_projects",
        snapshot_label=snapshot_label,
        source_url=first_source.get("source_url"),
        local_path=path,
        metadata=sidecar,
        content_sha256=first_source.get("sha256"),
        reporting_year=max(reporting_years) if reporting_years else None,
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.transport_sfdi_project where dataset_release_id = %s", (dataset_release_id,))
        inserted = 0
        for row in read_rows(path):
            cur.execute(
                """
                insert into raw.transport_sfdi_project (
                  dataset_release_id,
                  reporting_year,
                  action_id,
                  budget_area_code,
                  action_type_code,
                  financing_code,
                  status_code,
                  project_name,
                  total_cost_czk,
                  adjusted_budget_czk,
                  paid_czk,
                  sfdi_paid_czk,
                  eu_paid_czk,
                  region_code,
                  investor_name,
                  investor_ico,
                  investor_address,
                  start_period,
                  end_period,
                  source_url,
                  payload
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dataset_release_id,
                    int(row["reporting_year"]),
                    row["action_id"],
                    row["budget_area_code"],
                    row["action_type_code"],
                    row["financing_code"],
                    row["status_code"],
                    row["project_name"],
                    parse_decimal(row["total_cost_czk"]),
                    parse_decimal(row["adjusted_budget_czk"]),
                    parse_decimal(row["paid_czk"]),
                    parse_decimal(row["sfdi_paid_czk"]),
                    parse_decimal(row["eu_paid_czk"]),
                    row["region_code"],
                    row["investor_name"],
                    row["investor_ico"],
                    row["investor_address"],
                    row["start_period"],
                    row["end_period"],
                    row.get("source_url"),
                    Jsonb(row),
                ),
            )
            inserted += 1

    finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=inserted)
    return dataset_release_id, inserted


def load_transport_activity_metrics(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "transport_activity_metrics", *SOURCES["transport_activity_metrics"])
    reporting_years = sidecar.get("years") or []
    source_url = ((sidecar.get("sources") or [None])[0]) or None
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="transport_activity_metrics",
        snapshot_label=snapshot_label,
        source_url=source_url,
        local_path=path,
        metadata=sidecar,
        content_sha256=None,
        reporting_year=max(reporting_years) if reporting_years else None,
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.transport_activity_metric where dataset_release_id = %s", (dataset_release_id,))
        inserted = 0
        for row in read_rows(path):
            cur.execute(
                """
                insert into raw.transport_activity_metric (
                  dataset_release_id,
                  reporting_year,
                  activity_domain,
                  metric_code,
                  metric_name,
                  count_value,
                  reference_amount_czk,
                  source_url,
                  payload
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dataset_release_id,
                    int(row["reporting_year"]),
                    row["activity_domain"],
                    row["metric_code"],
                    row["metric_name"],
                    parse_decimal(row["count_value"]),
                    parse_decimal(row["reference_amount_czk"]),
                    row.get("source_url"),
                    Jsonb(row),
                ),
            )
            inserted += 1

    finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=inserted)
    return dataset_release_id, inserted


LOADERS = {
    "transport_budget_entities": load_transport_budget_entities,
    "transport_sfdi_projects": load_transport_sfdi_projects,
    "transport_activity_metrics": load_transport_activity_metrics,
}


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL must be provided via --database-url or environment")

    dataset_codes = args.dataset or list(LOADERS.keys())

    with psycopg.connect(args.database_url) as conn:
        with conn.transaction():
            for dataset_code in dataset_codes:
                path = latest_data_path(dataset_code)
                if path is None:
                    print(f"Skipping {dataset_code}: no downloaded file found")
                    continue
                dataset_release_id, inserted = LOADERS[dataset_code](conn, path)
                print(f"{dataset_code}: dataset_release_id={dataset_release_id} rows={inserted} path={path}")


if __name__ == "__main__":
    main()
