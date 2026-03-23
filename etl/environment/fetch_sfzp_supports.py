#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from _common import RAW_ROOT, USER_AGENT, fetch_bytes, sha256_bytes, timestamp_label, write_sidecar

DATASET_CODE = "environment_sfzp_supports"
SOURCE_URL = "https://otevrenadata.sfzp.cz/data/sfzp_aktivni_IS.csv"
OUTPUT_FILE_NAME = "sfzp-supports-normalized.csv"


@dataclass(frozen=True)
class SupportAggregate:
    reporting_year: int
    program_code: str
    program_name: str
    recipient_key: str
    recipient_name: str
    recipient_ico: str
    municipality: str
    support_czk: Decimal
    paid_czk: Decimal
    project_count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and normalize SFŽP open support registry")
    parser.add_argument("--year", action="append", type=int, required=True, help="Signature year to keep. Can be used multiple times.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.replace("\xa0", " ").split())


def normalize_match_key(value: str) -> str:
    text = unicodedata.normalize("NFKD", normalize_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def normalize_ico(value: str | None) -> str:
    digits = "".join(ch for ch in (value or "") if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(8) if len(digits) <= 8 else digits


def parse_decimal(value: str | None) -> Decimal:
    text = normalize_text(value)
    if not text:
        return Decimal("0")
    try:
        return Decimal(text.replace(",", "."))
    except InvalidOperation:
        return Decimal("0")


def parse_signature_year(value: str | None) -> int | None:
    text = normalize_text(value)
    if len(text) < 4 or not text[:4].isdigit():
        return None
    return int(text[:4])


def derive_program_family(program_label: str) -> tuple[str, str]:
    normalized = normalize_text(program_label)
    lowered = normalized.lower()

    if lowered.startswith("nzu") or lowered.startswith("nzú"):
        return "NZU", "Nova zelena usporam"
    if lowered.startswith("npzp") or lowered.startswith("npžp"):
        return "NPZP", "Narodni program Zivotni prostredi"
    if lowered.startswith("opzp") or lowered.startswith("opžp"):
        return "OPZP", "Operacni program Zivotni prostredi"
    if "kotlik" in lowered:
        return "KOTLIKY", "Kotlikove dotace a pujcky"
    if "moderniz" in lowered or "res+" in lowered or "heat" in lowered or "energov" in lowered or "transgov" in lowered:
        return "MODERNIZATION", "Modernizacni fond a navazne programy"
    return "OTHER", "Ostatni podpory SFZP"


def recipient_key(recipient_ico: str, recipient_name: str, municipality: str) -> str:
    if recipient_ico:
        return recipient_ico
    return f"JI:{normalize_match_key(recipient_name)}|{normalize_match_key(municipality)}"


def read_source_rows(payload: bytes):
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig", "ignore")))
    yield from reader


def aggregate_rows(payload: bytes, years: set[int]) -> list[SupportAggregate]:
    aggregates: dict[tuple[int, str, str], SupportAggregate] = {}

    for raw_row in read_source_rows(payload):
        reporting_year = parse_signature_year(raw_row.get("Datum podpisu rozhodnutí"))
        if reporting_year is None or reporting_year not in years:
            continue

        recipient_name = normalize_text(raw_row.get("Žadatel"))
        recipient_ico = normalize_ico(raw_row.get("IČO"))
        municipality = normalize_text(raw_row.get("Obec"))
        support_czk = parse_decimal(raw_row.get("Podpora"))
        paid_czk = parse_decimal(raw_row.get("Vyplaceno"))
        purpose = normalize_text(raw_row.get("Účel (Výzva – Číslo žádosti)"))
        program_label = purpose.split(" - ", 1)[0].strip()
        if not recipient_name or not program_label:
            continue

        program_code, program_name = derive_program_family(program_label)
        key = (reporting_year, program_code, recipient_key(recipient_ico, recipient_name, municipality))
        current = aggregates.get(key)

        if current is None:
            aggregates[key] = SupportAggregate(
                reporting_year=reporting_year,
                program_code=program_code,
                program_name=program_name,
                recipient_key=key[2],
                recipient_name=recipient_name,
                recipient_ico=recipient_ico,
                municipality=municipality,
                support_czk=support_czk,
                paid_czk=paid_czk,
                project_count=1,
            )
            continue

        aggregates[key] = SupportAggregate(
            reporting_year=current.reporting_year,
            program_code=current.program_code,
            program_name=current.program_name,
            recipient_key=current.recipient_key,
            recipient_name=current.recipient_name,
            recipient_ico=current.recipient_ico,
            municipality=current.municipality,
            support_czk=current.support_czk + support_czk,
            paid_czk=current.paid_czk + paid_czk,
            project_count=current.project_count + 1,
        )

    return sorted(
        aggregates.values(),
        key=lambda row: (row.reporting_year, row.program_code, -row.support_czk, row.recipient_name),
    )


def write_snapshot(
    *,
    out_dir: Path,
    snapshot: str,
    rows: list[SupportAggregate],
    years: list[int],
    payload: bytes,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "program_code",
        "program_name",
        "recipient_key",
        "recipient_name",
        "recipient_ico",
        "municipality",
        "support_czk",
        "paid_czk",
        "project_count",
        "source_url",
    ]

    with data_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "reporting_year": row.reporting_year,
                    "program_code": row.program_code,
                    "program_name": row.program_name,
                    "recipient_key": row.recipient_key,
                    "recipient_name": row.recipient_name,
                    "recipient_ico": row.recipient_ico,
                    "municipality": row.municipality,
                    "support_czk": f"{row.support_czk:.2f}",
                    "paid_czk": f"{row.paid_czk:.2f}",
                    "project_count": row.project_count,
                    "source_url": SOURCE_URL,
                }
            )

    write_sidecar(
        data_path,
        {
            "dataset_code": DATASET_CODE,
            "source_url": SOURCE_URL,
            "downloaded_at": datetime.now(UTC).isoformat(),
            "years": years,
            "row_count": len(rows),
            "content_sha256": sha256_bytes(payload),
            "generator": "etl/environment/fetch_sfzp_supports.py",
            "user_agent": USER_AGENT,
            "year_basis": "signature_year",
            "amount_basis": "support_czk",
            "note": "Aktivní registr SFŽP. Atlas používá rok podpisu rozhodnutí jako roční osu a částku podpory jako programový share pro alokaci skutečných výdajů SFŽP.",
        },
    )
    return data_path


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    snapshot = timestamp_label(args.snapshot)
    payload = fetch_bytes(SOURCE_URL)
    rows = aggregate_rows(payload, set(years))
    data_path = write_snapshot(
        out_dir=args.out_dir,
        snapshot=snapshot,
        rows=rows,
        years=years,
        payload=payload,
    )

    print(f"Wrote {data_path}")
    print(f"Years: {', '.join(str(year) for year in years)}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
