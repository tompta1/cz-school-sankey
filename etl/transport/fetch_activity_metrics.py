#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from pathlib import Path

from _common import RAW_ROOT, timestamp_label, write_sidecar

DATASET_CODE = "transport_activity_metrics"
OUTPUT_FILE_NAME = "transport-activity-metrics.csv"

METRIC_ROWS = [
    {
        "reporting_year": 2024,
        "activity_domain": "rail",
        "metric_code": "rail_passengers_total",
        "metric_name": "Cestující v železniční osobní dopravě",
        "count_value": 191_893_200,
        "reference_amount_czk": 0,
        "source_url": "https://www.sydos.cz/cs/rocenka-2024",
        "note": "Ročenka dopravy 2024, železniční osobní doprava, přeprava cestujících celkem.",
    },
    {
        "reporting_year": 2024,
        "activity_domain": "roads_vignette",
        "metric_code": "vignettes_sold_total",
        "metric_name": "Prodané elektronické dálniční známky",
        "count_value": 9_000_000,
        "reference_amount_czk": 7_200_000_000,
        "source_url": "https://edalnice.cz/2025/03/25/prodeje-elektronickych-dalnicnich-znamek-v-roce-2024-prekonaly-ocekavani/",
        "note": "CENDIS/eDalnice uvádí více než 9 milionů prodaných známek a tržby přes 7,2 mld. Kč za rok 2024.",
    },
    {
        "reporting_year": 2025,
        "activity_domain": "roads_vignette",
        "metric_code": "vignettes_sold_total",
        "metric_name": "Prodané elektronické dálniční známky",
        "count_value": 10_300_000,
        "reference_amount_czk": 8_700_000_000,
        "source_url": "https://edalnice.cz/2026/01/20/v-roce-2025-se-prodalo-pres-10-milionu-dalnicnich-znamek-nejvic-v-historii/",
        "note": "CENDIS/eDalnice uvádí více než 10,3 milionu prodaných známek a tržby 8,7 mld. Kč za rok 2025.",
    },
    {
        "reporting_year": 2024,
        "activity_domain": "roads_toll",
        "metric_code": "toll_registered_vehicles_total",
        "metric_name": "Registrovaná zpoplatněná vozidla",
        "count_value": 882_000,
        "reference_amount_czk": 17_161_000_000,
        "source_url": "https://www.czechtoll.cz/en/about-us/press-center/press-releases/2025/the-czech-republic-collected-a-record-czk-17.161-billion-in-toll-in-2024",
        "note": "CzechToll uvádí 882 tisíc registrovaných vozidel v mýtném systému a výběr 17,161 mld. Kč za rok 2024.",
    },
    {
        "reporting_year": 2025,
        "activity_domain": "roads_toll",
        "metric_code": "toll_registered_vehicles_total",
        "metric_name": "Registrovaná zpoplatněná vozidla",
        "count_value": 919_000,
        "reference_amount_czk": 19_100_000_000,
        "source_url": "https://www.czechtoll.cz/en/about-us/press-center/press-releases/2026/the-czech-republic-collected-a-record-19.1-billion-crowns-in-tolls-last-year",
        "note": "CzechToll uvádí 919 tisíc registrovaných vozidel v mýtném systému a výběr 19,1 mld. Kč za rok 2025.",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write transport activity metrics snapshots for atlas denominators")
    parser.add_argument("--year", action="append", type=int, help="Restrict output to one or more years.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def write_snapshot(out_dir: Path, snapshot: str, rows: list[dict[str, object]], years: list[int]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "activity_domain",
        "metric_code",
        "metric_name",
        "count_value",
        "reference_amount_czk",
        "source_url",
        "note",
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
            "years": years,
            "row_count": len(rows),
            "generator": "etl/transport/fetch_activity_metrics.py",
            "sources": sorted({row["source_url"] for row in rows}),
        },
    )
    return data_path


def main() -> None:
    args = parse_args()
    wanted_years = sorted(set(args.year or [2024, 2025]))
    snapshot = timestamp_label(args.snapshot)
    rows = [row for row in METRIC_ROWS if int(row["reporting_year"]) in wanted_years]
    if not rows:
        raise SystemExit("No transport activity metric rows match requested years")

    path = write_snapshot(args.out_dir, snapshot, rows, wanted_years)
    print(f"Wrote {path}")
    print(f"Years: {', '.join(str(year) for year in wanted_years)}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
