#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from _common import RAW_ROOT, USER_AGENT, fetch_bytes, sha256_bytes, timestamp_label, write_sidecar

DATASET_CODE = "agriculture_szif_payments"
OUTPUT_FILE_NAME = "szif-payments-normalized.csv"

SZIF_EU_PAGE_URL = "https://szif.gov.cz/cs/seznam-prijemcu-dotaci"
SZIF_NATIONAL_PAGE_URL = "https://szif.gov.cz/cs/seznam-prijemcu-nd"

FUNDING_SOURCE_LABELS = {
    "EU": "Dotace z fondů EU",
    "NATIONAL": "Narodni zemedelske dotace",
}

ICO_RE = re.compile(r"(\d{8})$")


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self._href = dict(attrs).get("href")
            self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._href is not None:
            text = " ".join("".join(self._text_parts).split())
            self.links.append((text, self._href))
            self._href = None
            self._text_parts = []


@dataclass(frozen=True)
class SourceSpec:
    reporting_year: int
    funding_source_code: str
    source_url: str
    source_label: str
    sha256: str
    size_bytes: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and normalize published SZIF subsidy recipient data")
    parser.add_argument("--year", action="append", type=int, required=True, help="Fiscal/reporting year to fetch. Can be used multiple times.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def parse_decimal(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    text = value.strip().replace("\xa0", " ")
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def normalize_ico(value: str | None) -> str:
    if not value:
        return ""
    digits = "".join(character for character in value if character.isdigit())
    if not digits:
        return ""
    if len(digits) <= 8:
        return digits.zfill(8)
    return digits


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.replace("\xa0", " ").split())


def recipient_key(recipient_ico: str, recipient_name: str, municipality: str, district: str) -> str:
    if recipient_ico:
        return recipient_ico
    return f"JI:{recipient_name}|{municipality}|{district}"


def eu_download_label(year: int) -> str:
    if year >= 2023:
        return f"Seznam příjemců dotací z Fondů EU {year} (CZK)"
    return f"Seznam příjemců dotací z Fondů EU {year}"


def national_download_label(year: int) -> str:
    return f"Seznam příjemců dotací z národních zdrojů {year}"


def discover_download_url(page_url: str, expected_label: str) -> str:
    html = urlopen(Request(page_url, headers={"User-Agent": USER_AGENT})).read().decode("utf-8", "ignore")
    parser = LinkParser()
    parser.feed(html)
    for text, href in parser.links:
        if text == expected_label:
            return urljoin(page_url, href)
    raise RuntimeError(f"Could not find link {expected_label!r} on {page_url}")


def parse_parent_subject(parent_subject: str) -> tuple[str, str]:
    normalized = normalize_text(parent_subject)
    if not normalized:
        return "", ""
    match = ICO_RE.search(normalized)
    if not match:
        return normalized, ""
    parent_ico = match.group(1)
    parent_name = normalized[: match.start()].strip()
    return parent_name, parent_ico


def build_payment_hash(parts: list[str]) -> str:
    digest = hashlib.sha256()
    digest.update("|".join(parts).encode("utf-8"))
    return digest.hexdigest()


def is_eu_detail_row(row: dict[str, str]) -> bool:
    return any(
        normalize_text(row.get(column))
        for column in (
            "Fond",
            "Kód opatření/typ intervence",
            "Název opatření",
            "Datum nabytí právní moci rozhodnutí",
        )
    )


def normalize_eu_rows(reporting_year: int, source_url: str, payload: bytes) -> list[dict[str, object]]:
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig", "ignore")))
    rows: list[dict[str, object]] = []

    for raw_row in reader:
        if not is_eu_detail_row(raw_row):
            continue

        recipient_name = normalize_text(raw_row.get("Název příjemce (právnická osoba)")) or normalize_text(
            raw_row.get("Příjmení a jméno příjemce")
        )
        recipient_ico = normalize_ico(raw_row.get("IČ Příjemce"))
        municipality = normalize_text(raw_row.get("Obec"))
        district = normalize_text(raw_row.get("Okres (NUTS 4)"))
        parent_name, parent_ico = parse_parent_subject(raw_row.get("Název mateřského subjektu a IČ", ""))
        fund_type = normalize_text(raw_row.get("Fond"))
        measure_code = normalize_text(raw_row.get("Kód opatření/typ intervence"))
        measure_name = normalize_text(raw_row.get("Název opatření"))
        legal_effective_date = normalize_text(raw_row.get("Datum nabytí právní moci rozhodnutí"))

        ezzf_amount = parse_decimal(raw_row.get("Částka podle operací v rámci EZZF"))
        ezfrv_amount = parse_decimal(raw_row.get("Částka podle operací v rámci EZFRV"))
        ezfrv_cofinancing = parse_decimal(raw_row.get("Částka podle operace v rámci spolufinancování EZFRV"))
        ezzf_cofinancing = parse_decimal(raw_row.get("Částka podle operace v rámci spolufinancování EZZF"))
        eu_source_czk = ezzf_amount + ezfrv_amount
        cz_source_czk = ezfrv_cofinancing + ezzf_cofinancing
        amount_czk = eu_source_czk + cz_source_czk
        if amount_czk == 0:
            continue

        key = recipient_key(recipient_ico, recipient_name, municipality, district)
        row_hash = build_payment_hash(
            [
                str(reporting_year),
                "EU",
                key,
                fund_type,
                measure_code,
                measure_name,
                legal_effective_date,
                f"{amount_czk:.2f}",
            ]
        )

        rows.append(
            {
                "reporting_year": reporting_year,
                "funding_source_code": "EU",
                "funding_source_name": FUNDING_SOURCE_LABELS["EU"],
                "recipient_name": recipient_name,
                "recipient_ico": recipient_ico,
                "recipient_key": key,
                "parent_subject_name": parent_name,
                "parent_subject_ico": parent_ico,
                "municipality": municipality,
                "district": district,
                "fund_type": fund_type,
                "measure_code": measure_code,
                "measure_name": measure_name,
                "legal_effective_date": legal_effective_date,
                "eu_source_czk": f"{eu_source_czk:.2f}",
                "cz_source_czk": f"{cz_source_czk:.2f}",
                "amount_czk": f"{amount_czk:.2f}",
                "source_url": source_url,
                "payment_row_hash": row_hash,
            }
        )

    return rows


def normalize_national_rows(reporting_year: int, source_url: str, payload: bytes) -> list[dict[str, object]]:
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig", "ignore")))
    rows: list[dict[str, object]] = []

    for raw_row in reader:
        recipient_name = normalize_text(raw_row.get("Jméno/Název"))
        recipient_ico = normalize_ico(raw_row.get("IČ"))
        municipality = normalize_text(raw_row.get("Obec"))
        district = normalize_text(raw_row.get("Okres"))
        fund_type = normalize_text(raw_row.get("Typ fondu"))
        measure_name = normalize_text(raw_row.get("Opatření"))
        legal_effective_date = normalize_text(raw_row.get("Datum nabytí právní moci"))
        eu_source_czk = parse_decimal(raw_row.get("Zdroje EU"))
        cz_source_czk = parse_decimal(raw_row.get("Zdroje ČR"))
        amount_czk = parse_decimal(raw_row.get("Celkem CZK"))
        if amount_czk == 0:
            continue

        key = recipient_key(recipient_ico, recipient_name, municipality, district)
        row_hash = build_payment_hash(
            [
                str(reporting_year),
                "NATIONAL",
                key,
                fund_type,
                measure_name,
                legal_effective_date,
                f"{amount_czk:.2f}",
            ]
        )

        rows.append(
            {
                "reporting_year": reporting_year,
                "funding_source_code": "NATIONAL",
                "funding_source_name": FUNDING_SOURCE_LABELS["NATIONAL"],
                "recipient_name": recipient_name,
                "recipient_ico": recipient_ico,
                "recipient_key": key,
                "parent_subject_name": "",
                "parent_subject_ico": "",
                "municipality": municipality,
                "district": district,
                "fund_type": fund_type,
                "measure_code": "",
                "measure_name": measure_name,
                "legal_effective_date": legal_effective_date,
                "eu_source_czk": f"{eu_source_czk:.2f}",
                "cz_source_czk": f"{cz_source_czk:.2f}",
                "amount_czk": f"{amount_czk:.2f}",
                "source_url": source_url,
                "payment_row_hash": row_hash,
            }
        )

    return rows


def fetch_source_specs(years: list[int]) -> tuple[list[SourceSpec], list[str]]:
    specs: list[SourceSpec] = []
    failures: list[str] = []

    for year in years:
        for funding_source_code, page_url, expected_label in (
            ("EU", SZIF_EU_PAGE_URL, eu_download_label(year)),
            ("NATIONAL", SZIF_NATIONAL_PAGE_URL, national_download_label(year)),
        ):
            try:
                source_url = discover_download_url(page_url, expected_label)
                payload = fetch_bytes(source_url)
                specs.append(
                    SourceSpec(
                        reporting_year=year,
                        funding_source_code=funding_source_code,
                        source_url=source_url,
                        source_label=expected_label,
                        sha256=sha256_bytes(payload),
                        size_bytes=len(payload),
                    )
                )
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{funding_source_code}:{year}:{exc}")

    return specs, failures


def normalize_sources(specs: list[SourceSpec]) -> list[dict[str, object]]:
    normalized_rows: list[dict[str, object]] = []
    for spec in specs:
        payload = fetch_bytes(spec.source_url)
        if spec.funding_source_code == "EU":
            normalized_rows.extend(normalize_eu_rows(spec.reporting_year, spec.source_url, payload))
        else:
            normalized_rows.extend(normalize_national_rows(spec.reporting_year, spec.source_url, payload))
    return normalized_rows


def write_snapshot(
    *,
    out_dir: Path,
    snapshot: str,
    rows: list[dict[str, object]],
    specs: list[SourceSpec],
    failures: list[str],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "funding_source_code",
        "funding_source_name",
        "recipient_name",
        "recipient_ico",
        "recipient_key",
        "parent_subject_name",
        "parent_subject_ico",
        "municipality",
        "district",
        "fund_type",
        "measure_code",
        "measure_name",
        "legal_effective_date",
        "eu_source_czk",
        "cz_source_czk",
        "amount_czk",
        "source_url",
        "payment_row_hash",
    ]

    with data_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    write_sidecar(
        data_path,
        {
            "dataset_code": DATASET_CODE,
            "downloaded_at": datetime.now(UTC).isoformat(),
            "row_count": len(rows),
            "years": sorted({spec.reporting_year for spec in specs}),
            "sources": [
                {
                    "year": spec.reporting_year,
                    "funding_source_code": spec.funding_source_code,
                    "source_label": spec.source_label,
                    "source_url": spec.source_url,
                    "sha256": spec.sha256,
                    "size_bytes": spec.size_bytes,
                }
                for spec in specs
            ],
            "failure_count": len(failures),
            "failures": failures[:100],
            "generator": "etl/agriculture/fetch_szif_payments.py",
            "user_agent": USER_AGENT,
        },
    )
    return data_path


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    snapshot = timestamp_label(args.snapshot)

    specs, failures = fetch_source_specs(years)
    rows = normalize_sources(specs)
    rows.sort(
        key=lambda row: (
            int(row["reporting_year"]),
            str(row["funding_source_code"]),
            -Decimal(str(row["amount_czk"])),
            str(row["recipient_name"]),
        )
    )

    data_path = write_snapshot(
        out_dir=args.out_dir,
        snapshot=snapshot,
        rows=rows,
        specs=specs,
        failures=failures,
    )

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")
    if failures:
        print(f"Failures: {len(failures)}")


if __name__ == "__main__":
    main()
