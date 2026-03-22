#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import UTC, datetime
from pathlib import Path

from pypdf import PdfReader

from _common import RAW_ROOT, USER_AGENT, fetch_bytes, sha256_bytes, timestamp_label

DATASET_CODE = "justice_budget_aggregates"
OUTPUT_FILE_NAME = "justice-budget-aggregates.csv"
CHAPTER_CODE = "336"
CHAPTER_NAME = "Ministerstvo spravedlnosti"

SOURCE_SPECS = {
    2024: {
        "url": "https://msp.gov.cz/documents/d/msp/zaverecny-ucet-kapitoly-za-rok-2024-pdf",
        "basis": "realized",
    },
    2025: {
        "url": "https://msp.gov.cz/documents/d/msp/zavazne-ukazatele-2025-pdf",
        "basis": "budgeted",
    },
}

DECIMAL_RE = re.compile(r"\d[\d ]*,\d+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch justice budget aggregates from official MSp PDFs")
    parser.add_argument("--year", type=int, action="append", choices=sorted(SOURCE_SPECS), help="Year to fetch")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def normalize_space(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def parse_amount_czk(token: str, multiplier: int) -> int:
    normalized = token.replace(" ", "").replace(",", ".")
    return int(round(float(normalized) * multiplier))


def load_pdf_texts(pdf_bytes: bytes) -> list[str]:
    pdf_path = Path("/tmp/justice_budget_source.pdf")
    pdf_path.write_bytes(pdf_bytes)
    reader = PdfReader(str(pdf_path))
    pages: list[str] = []
    for page in reader.pages:
        raw_text = page.extract_text() or ""
        pages.append("\n".join(normalize_space(line) for line in raw_text.splitlines()))
    return pages


def find_line(pages: list[str], prefix: str) -> str:
    for page in pages:
        for line in page.splitlines():
            normalized = normalize_space(line)
            if normalized.startswith(prefix):
                return normalized
    raise RuntimeError(f"Could not find line starting with {prefix!r}")


def find_line_containing(pages: list[str], needle: str) -> str:
    for page in pages:
        for line in page.splitlines():
            normalized = normalize_space(line)
            if needle in normalized:
                return normalized
    raise RuntimeError(f"Could not find line containing {needle!r}")


def line_amount(line: str, index: int, multiplier: int) -> int:
    numbers = DECIMAL_RE.findall(line)
    if len(numbers) <= index:
        raise RuntimeError(f"Unexpected numeric layout: {line}")
    return parse_amount_czk(numbers[index], multiplier)


def build_rows_2024(pages: list[str], source_url: str) -> list[dict[str, object]]:
    summary_pages = pages[209:210] if len(pages) > 209 else pages
    group3 = line_amount(find_line_containing(summary_pages, "3 Sl užby pro fyzi cké"), 4, 1000)
    group4 = line_amount(find_line_containing(summary_pages, "4 Soci á l ní věci"), 4, 1000)
    group5 = line_amount(find_line_containing(summary_pages, "5 Bezpečnos t s tá tu"), 4, 1000)
    group6 = line_amount(find_line_containing(summary_pages, "6 Vš eobecná veřejná"), 4, 1000)
    courts = line_amount(find_line_containing(summary_pages, "542 Soudni ctví"), 4, 1000)
    prosecution = line_amount(find_line_containing(summary_pages, "543 Stá tní za s tupi tel s tví"), 4, 1000)
    prison_service = line_amount(find_line_containing(summary_pages, "544 Vězeňs tví"), 4, 1000)
    probation = line_amount(find_line_containing(summary_pages, "545 Proba ční"), 4, 1000)
    ministry_admin = line_amount(find_line_containing(summary_pages, "546 Sprá va v obl a s ti prá vní ochra ny"), 4, 1000)
    justice_research = line_amount(find_line_containing(summary_pages, "548 Výzkum v obl a s ti prá vní ochra ny"), 4, 1000)
    justice_other = line_amount(find_line_containing(summary_pages, "549 Os ta tní zá l eži tos ti prá vní ochra ny"), 4, 1000)
    total_expenditure = group3 + group4 + group5 + group6
    explicit = courts + prosecution + prison_service + probation + ministry_admin + justice_research + justice_other + group4
    residual = max(total_expenditure - explicit, 0)

    metrics = [
        ("chapter_total", "total_expenditure", "Výdaje celkem", total_expenditure),
        ("justice", "courts", "Soudy", courts),
        ("justice", "prosecution", "Státní zastupitelství", prosecution),
        ("security", "prison_service", "Vězeňská služba", prison_service),
        ("justice", "probation_service", "Probační a mediační služba", probation),
        ("operations", "ministry_admin", "Správa v oblasti právní ochrany", ministry_admin),
        ("operations", "justice_research", "Výzkum v oblasti právní ochrany", justice_research),
        ("operations", "justice_other", "Ostatní záležitosti právní ochrany", justice_other),
        ("social", "social_and_prevention", "Sociální dávky a prevence", group4),
        ("operations", "residual_other", "Správa a ostatní", residual),
    ]

    return [
        {
            "reporting_year": 2024,
            "basis": "realized",
            "chapter_code": CHAPTER_CODE,
            "chapter_name": CHAPTER_NAME,
            "metric_group": metric_group,
            "metric_code": metric_code,
            "metric_name": metric_name,
            "amount_czk": amount_czk,
            "source_url": source_url,
        }
        for metric_group, metric_code, metric_name, amount_czk in metrics
    ]


def build_rows_2025(pages: list[str], source_url: str) -> list[dict[str, object]]:
    line = normalize_space(" ".join(pages))

    def extract_amount(label: str) -> int:
        marker = re.escape(label) + r"\s+(\d[\d ]+)"
        match = re.search(marker, line)
        if not match:
            raise RuntimeError(f"Could not find amount for {label!r}")
        return int(match.group(1).replace(" ", ""))

    total_expenditure = extract_amount("Výdaje celkem")
    justice_block = extract_amount("Výdajový blok - Výdaje justiční část")
    prison_pensions = extract_amount("dávky důchodového pojištění")
    prison_other_social = extract_amount("ostatní sociální dávky")
    prison_service = extract_amount("ostatní výdaje vězeňské části")
    social_and_prevention = prison_pensions + prison_other_social

    metrics = [
        ("chapter_total", "total_expenditure", "Výdaje celkem", total_expenditure),
        ("justice", "justice_block", "Výdaje justiční část", justice_block),
        ("security", "prison_service", "Vězeňská služba", prison_service),
        ("social", "social_and_prevention", "Sociální dávky a prevence", social_and_prevention),
    ]

    return [
        {
            "reporting_year": 2025,
            "basis": "budgeted",
            "chapter_code": CHAPTER_CODE,
            "chapter_name": CHAPTER_NAME,
            "metric_group": metric_group,
            "metric_code": metric_code,
            "metric_name": metric_name,
            "amount_czk": amount_czk,
            "source_url": source_url,
        }
        for metric_group, metric_code, metric_name, amount_czk in metrics
    ]


def write_snapshot(*, out_dir: Path, snapshot: str, rows: list[dict[str, object]], metadata: dict) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "basis",
        "chapter_code",
        "chapter_name",
        "metric_group",
        "metric_code",
        "metric_name",
        "amount_czk",
        "source_url",
    ]

    with data_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    sidecar_path = data_path.with_suffix(data_path.suffix + ".download.json")
    sidecar_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return data_path


def main() -> None:
    args = parse_args()
    snapshot = timestamp_label(args.snapshot)
    years = args.year or sorted(SOURCE_SPECS)

    rows: list[dict[str, object]] = []
    sources: list[dict[str, object]] = []
    for year in years:
        spec = SOURCE_SPECS[year]
        pdf_bytes = fetch_bytes(spec["url"])
        pages = load_pdf_texts(pdf_bytes)
        if year == 2024:
            rows.extend(build_rows_2024(pages, spec["url"]))
        elif year == 2025:
            rows.extend(build_rows_2025(pages, spec["url"]))
        sources.append(
            {
                "year": year,
                "source_url": spec["url"],
                "basis": spec["basis"],
                "sha256": sha256_bytes(pdf_bytes),
                "size_bytes": len(pdf_bytes),
            }
        )

    metadata = {
        "dataset_code": DATASET_CODE,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "row_count": len(rows),
        "years": years,
        "sources": sources,
        "generator": "etl/justice/fetch_budget_aggregates.py",
        "user_agent": USER_AGENT,
    }
    data_path = write_snapshot(out_dir=args.out_dir, snapshot=snapshot, rows=rows, metadata=metadata)

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
