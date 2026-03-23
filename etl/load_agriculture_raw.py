#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import unicodedata
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "agriculture"

SOURCES = {
    "agriculture_budget_entities": ("Monitor MF", "https://monitor.statnipokladna.gov.cz"),
    "agriculture_szif_payments": ("SZIF", "https://szif.gov.cz/cs/seznam-prijemcu-dotaci"),
    "agriculture_lpis_user_area": ("MZe pLPIS", "https://mze.gov.cz/public/app/wms/plpis_wfs.fcgi"),
}

AREA_MEASURE_CODES = {
    "EU:I.1",
    "EU:I.2",
    "EU:I.3",
    "EU:I.4",
    "EU:I.5",
    "EU:I.6",
    "EU:II.2",
    "EU:II.4",
    "EU:II.6",
    "EU:II.7",
    "EU:V.1",
    "EU:V.2",
    "EU:V.3",
    "EU:VI.15",
    "EU:VI.16",
    "EU:VI.18",
}

LIVESTOCK_MEASURE_CODES = {
    "EU:III.2",
    "EU:VI.19",
}

INVESTMENT_MEASURE_CODES = {
    "EU:V.4",
    "EU:V.5",
    "EU:VI.1",
    "EU:VI.4",
    "EU:VI.6",
    "EU:VI.11",
    "EU:VI.12",
    "EU:VI.13",
    "EU:VI.21",
    "EU:VI.24",
}

LIVESTOCK_NAME_PATTERNS = (
    "chovu",
    "zvířat",
    "včelařství",
    "dojnic",
    "prasat",
    "drůbeže",
    "býků",
    "genetického potenciálu",
)

INVESTMENT_NAME_PATTERNS = (
    "investice",
    "rozvoj zemědělských podniků",
    "lesnických technologií",
    "spolupráce",
    "leader",
    "rekonstrukce",
    "výstavby",
    "zahájení činnosti",
)

AREA_NAME_PATTERNS = (
    "ekologické zemědělství",
    "agroenvironmentálně-klimatické opatření",
    "natura 2000",
    "platba na plochu",
    "mladé zemědělce",
)

FAMILY_LABELS = {
    "AREA": "Plošne a krajinne podpory",
    "LIVESTOCK": "Zivocisna vyroba a welfare",
    "INVESTMENT": "Investice a rozvoj",
    "OTHER": "Ostatni podpory",
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
        choices=["agriculture_budget_entities", "agriculture_szif_payments", "agriculture_lpis_user_area"],
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


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.replace("\xa0", " ").split())


def normalize_match_key(value: str | None) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def classify_measure_family(funding_source_code: str, measure_code: str | None, measure_name: str | None) -> tuple[str, str]:
    code_key = f"{funding_source_code}:{(measure_code or '').strip()}"
    name = normalize_text(measure_name)
    lowered_name = name.lower()

    if code_key in AREA_MEASURE_CODES or any(pattern in lowered_name for pattern in AREA_NAME_PATTERNS):
        return "AREA", FAMILY_LABELS["AREA"]
    if code_key in LIVESTOCK_MEASURE_CODES or any(pattern in lowered_name for pattern in LIVESTOCK_NAME_PATTERNS):
        return "LIVESTOCK", FAMILY_LABELS["LIVESTOCK"]
    if code_key in INVESTMENT_MEASURE_CODES or any(pattern in lowered_name for pattern in INVESTMENT_NAME_PATTERNS):
        return "INVESTMENT", FAMILY_LABELS["INVESTMENT"]
    return "OTHER", FAMILY_LABELS["OTHER"]


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
        cur.execute("delete from raw.agriculture_szif_family_recipient_yearly where dataset_release_id = %s", (dataset_release_id,))
        aggregates: dict[tuple[int, str, str], dict[str, object]] = {}
        family_aggregates: dict[tuple[int, str, str, str], dict[str, object]] = {}

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

            family_code, family_name = classify_measure_family(
                funding_source_code,
                row.get("measure_code"),
                row.get("measure_name"),
            )
            family_key = (reporting_year, funding_source_code, family_code, recipient_key)
            family_aggregate = family_aggregates.setdefault(
                family_key,
                {
                    "reporting_year": reporting_year,
                    "funding_source_code": funding_source_code,
                    "funding_source_name": row["funding_source_name"],
                    "family_code": family_code,
                    "family_name": family_name,
                    "recipient_name": row["recipient_name"],
                    "recipient_name_normalized": normalize_match_key(row["recipient_name"]),
                    "recipient_ico": row["recipient_ico"] or None,
                    "recipient_key": recipient_key,
                    "municipality": row["municipality"] or None,
                    "district": row["district"] or None,
                    "amount_czk": Decimal("0"),
                    "payment_count": 0,
                    "source_url": row.get("source_url"),
                },
            )
            family_aggregate["amount_czk"] += parse_decimal(row["amount_czk"])
            family_aggregate["payment_count"] += 1

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

        family_copy_sql = """
            copy raw.agriculture_szif_family_recipient_yearly (
              dataset_release_id,
              reporting_year,
              funding_source_code,
              funding_source_name,
              family_code,
              family_name,
              recipient_name,
              recipient_name_normalized,
              recipient_ico,
              recipient_key,
              municipality,
              district,
              amount_czk,
              payment_count,
              source_url,
              payload
            ) from stdin
        """
        with cur.copy(family_copy_sql) as copy:
            for aggregate_key in sorted(family_aggregates):
                row = family_aggregates[aggregate_key]
                copy.write_row(
                    (
                        dataset_release_id,
                        row["reporting_year"],
                        row["funding_source_code"],
                        row["funding_source_name"],
                        row["family_code"],
                        row["family_name"],
                        row["recipient_name"],
                        row["recipient_name_normalized"],
                        row["recipient_ico"],
                        row["recipient_key"],
                        row["municipality"],
                        row["district"],
                        row["amount_czk"],
                        row["payment_count"],
                        row["source_url"],
                        Jsonb({}),
                    )
                )

    finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=inserted)
    return dataset_release_id, inserted


def load_agriculture_lpis_user_area(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    snapshot_label = snapshot_label_for(path)
    sidecar = sidecar_for(path)
    source_system_id = upsert_source_system(conn, "agriculture_lpis_user_area", *SOURCES["agriculture_lpis_user_area"])
    reporting_years = sidecar.get("reporting_years") or []
    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code="agriculture_lpis_user_area",
        snapshot_label=snapshot_label,
        source_url=sidecar.get("source_url"),
        local_path=path,
        metadata=sidecar,
        content_sha256=None,
        reporting_year=max(reporting_years) if reporting_years else None,
    )

    with conn.cursor() as cur:
        cur.execute("delete from raw.agriculture_lpis_user_area_yearly where dataset_release_id = %s", (dataset_release_id,))
        inserted = 0
        copy_sql = """
            copy raw.agriculture_lpis_user_area_yearly (
              dataset_release_id,
              reporting_year,
              user_name,
              user_name_normalized,
              lpis_user_ji,
              area_ha,
              block_count,
              source_url,
              payload
            ) from stdin
        """
        with cur.copy(copy_sql) as copy:
            for row in read_rows(path):
                copy.write_row(
                    (
                        dataset_release_id,
                        int(row["reporting_year"]),
                        row["user_name"],
                        normalize_match_key(row["user_name"]),
                        row.get("lpis_user_ji") or None,
                        parse_decimal(row["area_ha"]),
                        int(row["block_count"]),
                        row.get("source_url"),
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

    selected = set(args.dataset or ["agriculture_budget_entities", "agriculture_szif_payments", "agriculture_lpis_user_area"])

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

        if "agriculture_lpis_user_area" in selected:
            path = latest_data_path("agriculture_lpis_user_area")
            if not path:
                raise SystemExit("No downloaded agriculture LPIS user area file found")
            dataset_release_id, inserted = load_agriculture_lpis_user_area(conn, path)
            print(f"Loaded agriculture_lpis_user_area release={dataset_release_id} rows={inserted}")

        conn.commit()


if __name__ == "__main__":
    main()
