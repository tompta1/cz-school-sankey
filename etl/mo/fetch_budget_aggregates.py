#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

from pypdf import PdfReader

from _common import RAW_ROOT, fetch_bytes, sha256_bytes, timestamp_label, write_sidecar

DATASET_CODE = "mo_budget_aggregates"
OUTPUT_FILE_NAME = "mo-budget-aggregates.csv"
FACTS_TRENDS_PDF_URL = "https://mocr.mo.gov.cz/assets/finance-a-zakazky/resortni-rozpocet/fakta-a-trendy-2025.pdf"
NUMBER_PATTERN = r"(\d{1,3}(?: \d{3})*)"

ROWS = [
    ("PROGRAM_FINANCING", "Programové financování"),
    ("PERSONNEL_MANDATORY", "Osobní mandatorní výdaje"),
    ("OTHER_OPERATING", "Ostatní běžné výdaje"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch MO budget aggregates from Fakta a trendy 2025")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch. Supported years: 2024, 2025.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def extract_amounts(text: str, label: str) -> dict[int, int]:
    pattern = re.compile(
        rf"{re.escape(label)}\s+{NUMBER_PATTERN}\s+{NUMBER_PATTERN}\s+{NUMBER_PATTERN}\s+{NUMBER_PATTERN}\s+{NUMBER_PATTERN}",
        flags=re.IGNORECASE,
    )
    match = pattern.search(text)
    if not match:
        raise RuntimeError(f"Could not parse budget row for {label}")
    values = [int(group.replace(" ", "")) for group in match.groups()]
    return {
        2021: values[0],
        2022: values[1],
        2023: values[2],
        2024: values[3],
        2025: values[4],
    }


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    unsupported = [year for year in years if year not in {2024, 2025}]
    if unsupported:
        raise SystemExit(f"MO budget aggregates currently support only 2024 and 2025, got: {', '.join(map(str, unsupported))}")

    snapshot = timestamp_label(args.snapshot)
    payload = fetch_bytes(FACTS_TRENDS_PDF_URL)
    text = "\n".join(page.extract_text() or "" for page in PdfReader(BytesIO(payload)).pages)
    text = normalize_spaces(text)

    rows: list[dict[str, object]] = []
    for metric_code, metric_name in ROWS:
        yearly = extract_amounts(text, metric_name)
        for year in years:
            rows.append(
                {
                    "reporting_year": year,
                    "metric_code": metric_code,
                    "metric_name": metric_name,
                    "amount_czk": yearly[year] * 1_000_000,
                    "source_url": FACTS_TRENDS_PDF_URL,
                }
            )

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = ["reporting_year", "metric_code", "metric_name", "amount_czk", "source_url"]
    with data_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    write_sidecar(
        data_path,
        {
            "dataset_code": DATASET_CODE,
            "downloaded_at": datetime.now(UTC).isoformat(),
            "years": years,
            "row_count": len(rows),
            "source_url": FACTS_TRENDS_PDF_URL,
            "content_sha256": sha256_bytes(payload),
            "generator": "etl/mo/fetch_budget_aggregates.py",
        },
    )

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
