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
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "environment"

SOURCES = {
    "environment_budget_entities": ("Monitor MF", "https://monitor.statnipokladna.gov.cz"),
    "environment_sfzp_supports": ("SFŽP otevřená data", "https://otevrenadata.sfzp.cz/"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load downloaded MŽP files into raw Postgres tables")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=["environment_budget_entities", "environment_sfzp_supports"],
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
            values (%s, 'environment', %s, %s, %s, %s, %s, %s, 0, %s, 'staged')
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
        cur.execute("delete from raw.environment_budget_entity where dataset_release_id = %s", (dataset_release_id,))
        cur.executemany(
            """
            insert into raw.environment_budget_entity (
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


def load_sfzp_supports(conn: psycopg.Connection, dataset_release_id: int, path: Path) -> int:
    with conn.cursor() as cur:
        cur.execute("delete from raw.environment_sfzp_support_yearly where dataset_release_id = %s", (dataset_release_id,))
        cur.execute(
            """
            create temporary table environment_sfzp_support_stage (
              reporting_year integer,
              program_code text,
              program_name text,
              recipient_key text,
              recipient_name text,
              recipient_ico text,
              municipality text,
              support_czk numeric(18,2),
              paid_czk numeric(18,2),
              project_count integer,
              source_url text
            ) on commit drop
            """
        )
        with cur.copy(
            """
            copy environment_sfzp_support_stage (
              reporting_year,
              program_code,
              program_name,
              recipient_key,
              recipient_name,
              recipient_ico,
              municipality,
              support_czk,
              paid_czk,
              project_count,
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
            insert into raw.environment_sfzp_support_yearly (
              dataset_release_id,
              reporting_year,
              program_code,
              program_name,
              recipient_key,
              recipient_name,
              recipient_ico,
              municipality,
              support_czk,
              paid_czk,
              project_count,
              source_url,
              payload
            )
            select
              %s,
              reporting_year,
              program_code,
              program_name,
              recipient_key,
              recipient_name,
              nullif(recipient_ico, ''),
              nullif(municipality, ''),
              support_czk,
              paid_czk,
              project_count,
              nullif(source_url, ''),
              '{}'::jsonb
            from environment_sfzp_support_stage
            """,
            (dataset_release_id,),
        )
        row_count = cur.rowcount
    return row_count


LOADERS = {
    "environment_budget_entities": load_budget_entities,
    "environment_sfzp_supports": load_sfzp_supports,
}


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing DATABASE_URL / --database-url")

    dataset_codes = args.dataset or list(SOURCES.keys())

    with psycopg.connect(args.database_url) as conn:
        for dataset_code in dataset_codes:
            path = latest_data_path(dataset_code)
            if path is None:
                print(f"Skip {dataset_code}: no local file")
                continue

            source_name, base_url = SOURCES[dataset_code]
            sidecar = sidecar_for(path)
            reporting_year = None
            years = sidecar.get("years")
            if isinstance(years, list) and len(years) == 1:
                reporting_year = int(years[0])

            source_system_id = upsert_source_system(conn, dataset_code, source_name, base_url)
            dataset_release_id = upsert_dataset_release(
                conn,
                source_system_id=source_system_id,
                dataset_code=dataset_code,
                snapshot_label=snapshot_label_for(path),
                source_url=sidecar.get("source_url"),
                local_path=path,
                metadata=sidecar,
                content_sha256=sidecar.get("content_sha256"),
                reporting_year=reporting_year,
            )

            row_count = LOADERS[dataset_code](conn, dataset_release_id, path)
            finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=row_count)
            conn.commit()
            print(f"Loaded {dataset_code}: {row_count} rows from {path.name}")


if __name__ == "__main__":
    main()
