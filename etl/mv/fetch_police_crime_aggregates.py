#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
from datetime import UTC, datetime
from pathlib import Path

from _common import RAW_ROOT, USER_AGENT, fetch_bytes, sha256_bytes, timestamp_label

DATASET_CODE = "mv_police_crime_aggregates"
SOURCE_URL = "https://data.csu.gov.cz/opendata/sady/KRI10/distribuce/csv"
OUTPUT_FILE_NAME = "mv-police-crime-aggregates.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch police crime aggregates from the official KRI10 open dataset")
    parser.add_argument("--year", type=int, action="append", help="Optional reporting year filter")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def normalize_region_name(name: str) -> str:
    value = name.strip()
    if value == "Česko":
        return "Česko"
    return value


def build_rows(csv_bytes: bytes, requested_years: set[int] | None) -> list[dict[str, object]]:
    text = csv_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, object]] = []

    for row in reader:
        region_name = row.get("Území-Kraj") or row.get("Území")
        region_code = row.get("Uz02.KRAJ") or row.get("Uz02")
        if not region_name or not region_code:
            raise RuntimeError("Police crime CSV is missing region columns")
        year = int(row["Roky"])
        if requested_years and year not in requested_years:
            continue
        rows.append(
            {
                "reporting_year": year,
                "region_name": normalize_region_name(region_name),
                "region_code": region_code,
                "indicator_code": row["IndicatorType"],
                "indicator_name": row["Ukazatel"],
                "crime_class_code": row["TSKKC"],
                "crime_class_name": row["Takticko statistická klasifikace kriminality Police ČR"],
                "count_value": int(row["Hodnota"]),
                "source_url": SOURCE_URL,
            }
        )
    return rows


def write_snapshot(*, out_dir: Path, snapshot: str, rows: list[dict[str, object]], metadata: dict) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "region_name",
        "region_code",
        "indicator_code",
        "indicator_name",
        "crime_class_code",
        "crime_class_name",
        "count_value",
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
    requested_years = set(args.year or [])
    csv_bytes = fetch_bytes(SOURCE_URL)
    rows = build_rows(csv_bytes, requested_years or None)

    metadata = {
      "dataset_code": DATASET_CODE,
      "source_url": SOURCE_URL,
      "downloaded_at": datetime.now(UTC).isoformat(),
      "row_count": len(rows),
      "years": sorted({int(row["reporting_year"]) for row in rows}),
      "sha256": sha256_bytes(csv_bytes),
      "size_bytes": len(csv_bytes),
      "generator": "etl/mv/fetch_police_crime_aggregates.py",
      "user_agent": USER_AGENT,
    }
    data_path = write_snapshot(out_dir=args.out_dir, snapshot=snapshot, rows=rows, metadata=metadata)

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
