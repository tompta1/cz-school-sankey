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
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "agriculture"

SOURCES = {
    "agriculture_budget_entities": ("Monitor MF", "https://monitor.statnipokladna.gov.cz"),
    "agriculture_szif_payments": ("SZIF", "https://szif.gov.cz/cs/seznam-prijemcu-dotaci"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load downloaded agriculture files into raw Postgres tables")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=["agriculture_budget_entities", "agriculture_szif_payments"],
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
            values (%s, 'agriculture', %s, %s, %s, %s, %s, %s, 0, %s, 'staged')
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


def load_agriculture_budget_entities(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "agriculture_budget_entities", *SOURCES["agriculture_budget_entities"])
    reporting_years = sidecar.get("years") or []
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="agriculture_budget_entities",
        snapshot_label=snapshot_label,
        source_url=sidecar.get("source_url"),
        local_path=path,
        metadata=sidecar,
        content_sha256=None,
        reporting_year=max(reporting_years) if reporting_years else None,
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.agriculture_budget_entity where dataset_release_id = %s", (dataset_release_id,))
        inserted = 0
        for row in read_rows(path):
            cur.execute(
                """
                insert into raw.agriculture_budget_entity (
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


def load_agriculture_szif_payments(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "agriculture_szif_payments", *SOURCES["agriculture_szif_payments"])
    reporting_years = sidecar.get("years") or []
    first_source = (sidecar.get("sources") or [{}])[0]
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="agriculture_szif_payments",
        snapshot_label=snapshot_label,
        source_url=first_source.get("source_url"),
        local_path=path,
        metadata=sidecar,
        content_sha256=first_source.get("sha256"),
        reporting_year=max(reporting_years) if reporting_years else None,
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.agriculture_szif_recipient_yearly where dataset_release_id = %s", (dataset_release_id,))
        aggregates: dict[tuple[int, str, str], dict[str, object]] = {}

        for row in read_rows(path):
            reporting_year = int(row["reporting_year"])
            funding_source_code = row["funding_source_code"]
            recipient_key = row["recipient_key"]
            aggregate_key = (reporting_year, funding_source_code, recipient_key)

            aggregate = aggregates.setdefault(
                aggregate_key,
                {
                    "reporting_year": reporting_year,
                    "funding_source_code": funding_source_code,
                    "funding_source_name": row["funding_source_name"],
                    "recipient_name": row["recipient_name"],
                    "recipient_ico": row["recipient_ico"] or None,
                    "recipient_key": recipient_key,
                    "municipality": row["municipality"] or None,
                    "district": row["district"] or None,
                    "eu_source_czk": Decimal("0"),
                    "cz_source_czk": Decimal("0"),
                    "amount_czk": Decimal("0"),
                    "payment_count": 0,
                    "source_url": row.get("source_url"),
                },
            )
            aggregate["eu_source_czk"] += parse_decimal(row["eu_source_czk"])
            aggregate["cz_source_czk"] += parse_decimal(row["cz_source_czk"])
            aggregate["amount_czk"] += parse_decimal(row["amount_czk"])
            aggregate["payment_count"] += 1

        inserted = 0
        copy_sql = """
            copy raw.agriculture_szif_recipient_yearly (
              dataset_release_id,
              reporting_year,
              funding_source_code,
              funding_source_name,
              recipient_name,
              recipient_ico,
              recipient_key,
              municipality,
              district,
              eu_source_czk,
              cz_source_czk,
              amount_czk,
              payment_count,
              source_url,
              payload
            ) from stdin
        """
        with cur.copy(copy_sql) as copy:
            for aggregate_key in sorted(aggregates):
                row = aggregates[aggregate_key]
                copy.write_row(
                    (
                        dataset_release_id,
                        row["reporting_year"],
                        row["funding_source_code"],
                        row["funding_source_name"],
                        row["recipient_name"],
                        row["recipient_ico"],
                        row["recipient_key"],
                        row["municipality"],
                        row["district"],
                        row["eu_source_czk"],
                        row["cz_source_czk"],
                        row["amount_czk"],
                        row["payment_count"],
                        row["source_url"],
                        Jsonb({}),
                    )
                )
                inserted += 1

    finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=inserted)
    return dataset_release_id, inserted


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    selected = set(args.dataset or ["agriculture_budget_entities", "agriculture_szif_payments"])

    with psycopg.connect(args.database_url) as conn:
        if "agriculture_budget_entities" in selected:
            path = latest_data_path("agriculture_budget_entities")
            if not path:
                raise SystemExit("No downloaded agriculture budget entity file found")
            dataset_release_id, inserted = load_agriculture_budget_entities(conn, path)
            print(f"Loaded agriculture_budget_entities release={dataset_release_id} rows={inserted}")

        if "agriculture_szif_payments" in selected:
            path = latest_data_path("agriculture_szif_payments")
            if not path:
                raise SystemExit("No downloaded agriculture SZIF payment file found")
            dataset_release_id, inserted = load_agriculture_szif_payments(conn, path)
            print(f"Loaded agriculture_szif_payments release={dataset_release_id} rows={inserted}")

        conn.commit()


if __name__ == "__main__":
    main()
