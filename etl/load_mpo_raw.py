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
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "mpo"

SOURCES = {
    "mpo_budget_entities": ("Ministerstvo průmyslu a obchodu", "https://monitor.statnipokladna.gov.cz"),
    "mpo_optak_operations": ("DotaceEU / OP TAK", "https://www.dotaceeu.cz"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load downloaded MPO files into raw Postgres tables")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=["mpo_budget_entities", "mpo_optak_operations"],
        help="Restrict loading to one or more dataset codes.",
    )
    return parser.parse_args()


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
    csv_candidates = [path for path in candidates if path.suffix.lower() == ".csv"]
    if csv_candidates:
        return sorted(csv_candidates)[-1]
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


def parse_decimal(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    text = value.strip()
    if not text:
        return Decimal("0")
    return Decimal(text)


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
            values (%s, 'mpo', %s, %s, %s, %s, %s, %s, 0, %s, 'staged')
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


def load_budget_entities(conn: psycopg.Connection, dataset_release_id: int, path: Path) -> int:
    rows = []
    for row in read_rows(path):
        rows.append(
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
                row.get("source_url") or None,
                Jsonb(row),
            )
        )

    with conn.cursor() as cur:
        cur.execute("delete from raw.mpo_budget_entity where dataset_release_id = %s", (dataset_release_id,))
        cur.executemany(
            """
            insert into raw.mpo_budget_entity (
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
            rows,
        )
    return len(rows)


def load_optak_operations(conn: psycopg.Connection, dataset_release_id: int, path: Path) -> int:
    with conn.cursor() as cur:
        cur.execute("delete from raw.mpo_optak_operation_yearly where dataset_release_id = %s", (dataset_release_id,))
        cur.execute(
            """
            create temporary table mpo_optak_operation_stage (
              reporting_year integer,
              region_code text,
              region_name text,
              recipient_key text,
              recipient_name text,
              recipient_ico text,
              project_id text,
              project_name text,
              priority_name text,
              specific_objective_name text,
              intervention_name text,
              allocated_total_czk numeric(18,2),
              union_support_czk numeric(18,2),
              national_public_czk numeric(18,2),
              charged_total_czk numeric(18,2),
              source_url text
            ) on commit drop
            """
        )
        with cur.copy(
            """
            copy mpo_optak_operation_stage (
              reporting_year,
              region_code,
              region_name,
              recipient_key,
              recipient_name,
              recipient_ico,
              project_id,
              project_name,
              priority_name,
              specific_objective_name,
              intervention_name,
              allocated_total_czk,
              union_support_czk,
              national_public_czk,
              charged_total_czk,
              source_url
            )
            from stdin with (format csv, header true)
            """
        ) as copy:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                while chunk := handle.read(1_048_576):
                    copy.write(chunk)

        cur.execute(
            """
            insert into raw.mpo_optak_operation_yearly (
              dataset_release_id,
              reporting_year,
              region_code,
              region_name,
              recipient_key,
              recipient_name,
              recipient_ico,
              project_id,
              project_name,
              priority_name,
              specific_objective_name,
              intervention_name,
              allocated_total_czk,
              union_support_czk,
              national_public_czk,
              charged_total_czk,
              source_url,
              payload
            )
            select
              %s,
              reporting_year,
              nullif(region_code, ''),
              nullif(region_name, ''),
              recipient_key,
              recipient_name,
              nullif(recipient_ico, ''),
              project_id,
              project_name,
              nullif(priority_name, ''),
              nullif(specific_objective_name, ''),
              nullif(intervention_name, ''),
              allocated_total_czk,
              union_support_czk,
              national_public_czk,
              charged_total_czk,
              nullif(source_url, ''),
              jsonb_build_object(
                'priority_name', priority_name,
                'specific_objective_name', specific_objective_name,
                'intervention_name', intervention_name
              )
            from mpo_optak_operation_stage
            """,
            (dataset_release_id,),
        )
        row_count = cur.rowcount

    return row_count if row_count is not None else 0


def load_dataset(conn: psycopg.Connection, dataset_code: str, data_path: Path) -> int:
    sidecar = sidecar_for(data_path)
    name, base_url = SOURCES[dataset_code]
    source_system_id = upsert_source_system(conn, dataset_code, name, base_url)
    release_years = sidecar.get("years") or []
    reporting_year = int(release_years[-1]) if release_years else None
    source_url = None
    if sidecar.get("sources"):
        source_url = sidecar["sources"][0].get("source_url")
    elif sidecar.get("source_url"):
        source_url = sidecar.get("source_url")
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code=dataset_code,
        snapshot_label=snapshot_label_for(data_path),
        source_url=source_url,
        local_path=data_path,
        metadata=sidecar or {},
        content_sha256=sidecar.get("content_sha256"),
        reporting_year=reporting_year,
    )

    if dataset_code == "mpo_budget_entities":
        row_count = load_budget_entities(conn, dataset_release_id, data_path)
    elif dataset_code == "mpo_optak_operations":
        row_count = load_optak_operations(conn, dataset_release_id, data_path)
    else:
        raise ValueError(f"Unsupported dataset: {dataset_code}")

    finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=row_count)
    return row_count


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    selected = args.dataset or list(SOURCES.keys())
    with psycopg.connect(args.database_url) as conn:
        for dataset_code in selected:
            data_path = latest_data_path(dataset_code)
            if data_path is None:
                print(f"Skipping {dataset_code}: no local data found")
                continue
            row_count = load_dataset(conn, dataset_code, data_path)
            conn.commit()
            print(f"Loaded {dataset_code}: {row_count} rows from {data_path}")


if __name__ == "__main__":
    main()
