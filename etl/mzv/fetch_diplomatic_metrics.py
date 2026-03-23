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

DATASET_CODE = "mzv_diplomatic_metrics"
OUTPUT_FILE_NAME = "mzv-diplomatic-metrics.csv"
DIPLOMACY_PDF_URL = "https://mzv.gov.cz/file/5947860/ceska_diplomacie_2024.pdf"

METRICS = [
    ("FOREIGN_POST_TOTAL", "Zastupitelské úřady a úřady v zahraničí", None),
    ("EMBASSY_POSTS", "Velvyslanectví ČR", None),
    ("PERMANENT_MISSIONS", "Stálé mise ČR", 8),
    ("GENERAL_CONSULATES", "Generální konzuláty ČR", 17),
    ("CONSULAR_AGENCIES", "Konzulární jednatelství", 3),
    ("OTHER_OFFICES", "Zastupitelské úřady ČR jiného typu", 2),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch MZV diplomatic network metrics from Česká diplomacie 2024")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch. Only 2024 is currently supported.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def extract_total_posts(text: str) -> int:
    match = re.search(r"na\s+(\d+)\s+zastupitelských úřadech v zahraničí", text, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError("Could not find total foreign-post count in Česká diplomacie 2024")
    return int(match.group(1))


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    unsupported = [year for year in years if year != 2024]
    if unsupported:
        raise SystemExit(f"MZV diplomatic metrics currently support only 2024, got: {', '.join(map(str, unsupported))}")

    snapshot = timestamp_label(args.snapshot)
    payload = fetch_bytes(DIPLOMACY_PDF_URL)
    reader = PdfReader(BytesIO(payload))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    total_posts = extract_total_posts(text)
    subtype_total = sum(count for _, _, count in METRICS if count is not None)
    embassy_posts = total_posts - subtype_total
    if embassy_posts <= 0:
        raise RuntimeError("Derived embassy count is not positive")

    rows: list[dict[str, object]] = []
    for year in years:
      for metric_code, metric_name, fixed_count in METRICS:
          count_value = embassy_posts if metric_code == "EMBASSY_POSTS" else total_posts if metric_code == "FOREIGN_POST_TOTAL" else int(fixed_count or 0)
          rows.append(
              {
                  "reporting_year": year,
                  "metric_code": metric_code,
                  "metric_name": metric_name,
                  "count_value": count_value,
                  "source_url": DIPLOMACY_PDF_URL,
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

    write_sidecar(
        data_path,
        {
            "dataset_code": DATASET_CODE,
            "downloaded_at": datetime.now(UTC).isoformat(),
            "years": years,
            "row_count": len(rows),
            "source_url": DIPLOMACY_PDF_URL,
            "content_sha256": sha256_bytes(payload),
            "foreign_post_total": total_posts,
            "derived_embassy_posts": embassy_posts,
            "generator": "etl/mzv/fetch_diplomatic_metrics.py",
        },
    )

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
