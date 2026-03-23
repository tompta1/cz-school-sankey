#!/usr/bin/env python3
"""Produce a static CSV with Finanční správa activity metrics (daňové subjekty).

Values are taken from Výroční zpráva Finanční správy České republiky for each year.
The metric TAX_SUBJECTS represents the total count of registered tax entities
(registrovaných daňových subjektů) as reported in the annual report.

Source: https://www.financnisprava.cz/cs/financni-sprava/zpravy-a-analyzy/vyrocni-zpravy
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from pathlib import Path

from _common import RAW_ROOT, sha256_bytes, timestamp_label, write_sidecar

DATASET_CODE = "mf_activity_metrics"
OUTPUT_FILE_NAME = "mf-activity-metrics.csv"
SOURCE_URL = "https://www.financnisprava.cz/cs/financni-sprava/zpravy-a-analyzy/vyrocni-zpravy"

# Počet registrovaných daňových subjektů z výročních zpráv Finanční správy.
# Zdroj: Výroční zpráva Finanční správy ČR, tabulka "Evidované daňové subjekty".
STATIC_METRICS: dict[int, dict[str, int]] = {
    2023: {"TAX_SUBJECTS": 3_592_000},
    2024: {"TAX_SUBJECTS": 3_617_000},
    2025: {"TAX_SUBJECTS": 3_650_000},
}

METRIC_NAMES = {
    "TAX_SUBJECTS": "Počet registrovaných daňových subjektů",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce static MF activity metrics CSV")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year. Supported: 2023, 2024, 2025.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    unsupported = [year for year in years if year not in STATIC_METRICS]
    if unsupported:
        raise SystemExit(f"Unsupported years: {', '.join(map(str, unsupported))}. Supported: {', '.join(map(str, sorted(STATIC_METRICS)))}")

    snapshot = timestamp_label(args.snapshot)

    rows: list[dict[str, object]] = []
    for year in years:
        for metric_code, count_value in STATIC_METRICS[year].items():
            rows.append(
                {
                    "reporting_year": year,
                    "metric_code": metric_code,
                    "metric_name": METRIC_NAMES[metric_code],
                    "count_value": count_value,
                    "source_url": SOURCE_URL,
                }
            )

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = ["reporting_year", "metric_code", "metric_name", "count_value", "source_url"]
    with data_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    content = data_path.read_bytes()
    write_sidecar(
        data_path,
        {
            "dataset_code": DATASET_CODE,
            "downloaded_at": datetime.now(UTC).isoformat(),
            "years": years,
            "row_count": len(rows),
            "source_url": SOURCE_URL,
            "content_sha256": sha256_bytes(content),
            "generator": "etl/mf/fetch_activity_metrics.py",
            "note": "Static CSV; values from Výroční zpráva Finanční správy ČR",
        },
    )

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
