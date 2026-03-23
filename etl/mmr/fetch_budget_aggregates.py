#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from pathlib import Path

from _common import RAW_ROOT, fetch_bytes, sha256_bytes, timestamp_label, write_sidecar

DATASET_CODE = "mmr_budget_aggregates"
BASE_URL = "https://mmr.gov.cz/MMR/media/MMR_MediaLib/Ministerstvo/Open%20data"
OUTPUT_FILE_NAME = "mmr-budget-aggregates.csv"

METRIC_MAP = {
    "Výdaje celkem": ("EXP_TOTAL", "Výdaje celkem", "summary"),
    "Podpora regionálního rozvoje a cestovního ruchu celkem": ("REGIONAL_SUPPORT", "Podpora regionálního rozvoje a cestovního ruchu", "branch"),
    "Podpora bydlení celkem": ("HOUSING_SUPPORT", "Podpora bydlení", "branch"),
    "Územní plánování a stavební řád": ("PLANNING", "Územní plánování a stavební řád", "branch"),
    "Ostatní činnosti resortu celkem": ("OTHER", "Ostatní činnosti resortu", "branch"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch MMR open budget aggregates")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch. Can be used multiple times.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def budget_csv_url(year: int) -> str:
    return f"{BASE_URL}/zavazne-ukazatele-mmr-{year}.csv"


def parse_amount(value: str) -> int:
    digits = "".join(ch for ch in value if ch.isdigit())
    return int(digits or "0")


def fetch_year_rows(year: int) -> tuple[list[dict[str, object]], dict[str, object]]:
    source_url = budget_csv_url(year)
    payload = fetch_bytes(source_url)
    decoded = payload.decode("utf-8-sig", "ignore")
    reader = csv.DictReader(decoded.splitlines())
    rows: list[dict[str, object]] = []

    for row in reader:
        name = str(row.get("Název ukazatele") or "").strip()
        if name not in METRIC_MAP:
            continue
        metric_code, metric_name, metric_group = METRIC_MAP[name]
        rows.append(
            {
                "reporting_year": year,
                "metric_code": metric_code,
                "metric_name": metric_name,
                "metric_group": metric_group,
                "amount_czk": parse_amount(str(row.get("Částka v Kč") or "0")),
                "source_url": source_url,
            }
        )

    return rows, {
        "year": year,
        "source_url": source_url,
        "content_sha256": sha256_bytes(payload),
        "row_count": len(rows),
    }


def write_snapshot(
    *,
    out_dir: Path,
    snapshot: str,
    rows: list[dict[str, object]],
    metadata_rows: list[dict[str, object]],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "metric_code",
        "metric_name",
        "metric_group",
        "amount_czk",
        "source_url",
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
            "year_count": len(metadata_rows),
            "years": sorted({int(row["year"]) for row in metadata_rows}),
            "sources": metadata_rows,
            "generator": "etl/mmr/fetch_budget_aggregates.py",
        },
    )
    return data_path


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    snapshot = timestamp_label(args.snapshot)

    rows: list[dict[str, object]] = []
    metadata_rows: list[dict[str, object]] = []
    for year in years:
        year_rows, meta = fetch_year_rows(year)
        rows.extend(year_rows)
        metadata_rows.append(meta)

    rows.sort(key=lambda row: (int(row["reporting_year"]), str(row["metric_code"])))
    data_path = write_snapshot(
        out_dir=args.out_dir,
        snapshot=snapshot,
        rows=rows,
        metadata_rows=metadata_rows,
    )

    print(f"Wrote {data_path}")
    print(f"Years: {', '.join(str(year) for year in years)}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
