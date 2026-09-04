#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path

from pypdf import PdfReader

from _common import RAW_ROOT, USER_AGENT, fetch_bytes, timestamp_label

DATASET_CODE = "social_mpsv_aggregates"
SOURCE_URLS = {
    2024: "https://mf.gov.cz/assets/attachments/2025-04-28_H-Vysledky-rozpoctoveho-hospodareni-kapitol.pdf",
    2025: "https://mf.gov.cz/assets/attachments/2026-04-30_H-Ukazatele-kapitol-statniho-rozpoctu.pdf",
}
OUTPUT_FILE_NAME = "mpsv-budget-aggregates.csv"
CHAPTER_CODE = "313"
CHAPTER_NAME = "Ministerstvo práce a sociálních věcí"

METRIC_SPECS = [
    ("chapter_total", "total_expenditure", "Výdaje celkem"),
    ("benefit", "pensions", "Dávky důchodového pojištění"),
    ("benefit", "family_support", "Dávky státní sociální podpory a pěstounské péče"),
    ("benefit", "sickness", "Dávky nemocenského pojištění"),
    ("benefit", "material_need", "Dávky pomoci v hmotné nouzi"),
    ("benefit", "disability", "Dávky osobám se zdravotním postižením"),
    ("benefit", "substitute_alimony", "Náhradní výživné pro nezaopatřené dítě"),
    ("benefit", "other_social_benefits", "Ostatní sociální dávky"),
    ("benefit", "unemployment_support", "Podpory v nezaměstnanosti"),
    ("benefit", "care_allowance", "Příspěvek na péči podle zákona o sociálních službách"),
    ("benefit", "active_labour_policy", "Aktivní politika zaměstnanosti celkem"),
    ("benefit", "employment_insolvency", "Výdaje spojené s realizací zákona č. 118/2000 Sb."),
    ("benefit", "compensation_laws", "Výdaje spojené s realizací odškodňovacích zákonů"),
    ("benefit", "disabled_employment_support", "Příspěvek na podporu zaměstnávání osob se zdravotním postižením"),
    ("operations", "state_administration_other", "Ostatní výdaje organizačních složek státu"),
    ("transfer", "nonbenefit_transfers", "Neinvestiční nedávkové transfery"),
    ("transfer", "social_capital_support", "Transfery na podporu reprodukce majetku nestátním subjektům v sociální oblasti"),
]

NUMBER_RE = re.compile(r"\d[\d ]*\d,\d+")

logging.getLogger("pypdf").setLevel(logging.ERROR)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and parse MPSV budget aggregates from MF chapter results PDF")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def parse_czk_thousands(token: str) -> int:
    normalized = token.replace(" ", "").replace(",", ".")
    return int(round(float(normalized) * 1000))


def extract_mpsv_page_text(pdf_bytes: bytes) -> str:
    pdf_path = Path("/tmp/social_mpsv_budget_aggregates.pdf")
    pdf_path.write_bytes(pdf_bytes)
    reader = PdfReader(str(pdf_path))
    marker = "kapitola: 313 Ministerstvo práce a sociálních věcí"
    chapter_pages = [text for page in reader.pages if marker in (text := page.extract_text() or "")]
    if not chapter_pages:
        raise RuntimeError("Could not find MPSV chapter pages in MF PDF")
    return "\n".join(chapter_pages)


def parse_metric_amount(page_text: str, label: str) -> int:
    normalized_text = " ".join(page_text.split())
    normalized_label = " ".join(label.split())
    label_index = normalized_text.find(normalized_label)
    if label_index == -1:
        raise RuntimeError(f"Could not find metric line for {label!r}")

    value_text = normalized_text[label_index + len(normalized_label) :]
    tokens = NUMBER_RE.findall(value_text[:500])
    if len(tokens) < 5:
        raise RuntimeError(f"Unexpected numeric layout for {label!r}: {value_text[:500]}")
    return parse_czk_thousands(tokens[4])


def build_rows(page_text: str, year: int, source_url: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for metric_group, metric_code, metric_name in METRIC_SPECS:
        rows.append(
            {
                "reporting_year": year,
                "chapter_code": CHAPTER_CODE,
                "chapter_name": CHAPTER_NAME,
                "metric_group": metric_group,
                "metric_code": metric_code,
                "metric_name": metric_name,
                "amount_czk": parse_metric_amount(page_text, metric_name),
                "source_url": source_url,
            }
        )
    return rows


def write_snapshot(
    *,
    out_dir: Path,
    snapshot: str,
    rows: list[dict[str, object]],
    years: list[int],
    pdf_sizes: dict[int, int],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "chapter_code",
        "chapter_name",
        "metric_group",
        "metric_code",
        "metric_name",
        "amount_czk",
        "source_url",
    ]

    with data_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    sidecar = {
        "dataset_code": DATASET_CODE,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "years": years,
        "row_count": len(rows),
        "generator": "etl/social/fetch_mpsv_budget_aggregates.py",
        "user_agent": USER_AGENT,
        "sources": [SOURCE_URLS[year] for year in years],
        "pdf_size_bytes": {str(year): pdf_sizes[year] for year in years},
    }
    sidecar_path = data_path.with_suffix(data_path.suffix + ".download.json")
    sidecar_path.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2), encoding="utf-8")
    return data_path


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    unsupported_years = [year for year in years if year not in SOURCE_URLS]
    if unsupported_years:
        raise SystemExit(f"Unsupported MPSV budget year(s): {', '.join(str(year) for year in unsupported_years)}")

    rows: list[dict[str, object]] = []
    pdf_sizes: dict[int, int] = {}
    for year in years:
        source_url = SOURCE_URLS[year]
        pdf_bytes = fetch_bytes(source_url)
        page_text = extract_mpsv_page_text(pdf_bytes)
        rows.extend(build_rows(page_text, year, source_url))
        pdf_sizes[year] = len(pdf_bytes)

    snapshot = timestamp_label(args.snapshot)
    data_path = write_snapshot(
        out_dir=args.out_dir,
        snapshot=snapshot,
        rows=rows,
        years=years,
        pdf_sizes=pdf_sizes,
    )

    print(f"Wrote {data_path}")
    print(f"Years: {', '.join(str(year) for year in years)}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
