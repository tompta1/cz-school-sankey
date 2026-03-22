#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import re
from datetime import UTC, datetime
from pathlib import Path

from pypdf import PdfReader

from _common import RAW_ROOT, USER_AGENT, fetch_bytes, sha256_bytes, timestamp_label

DATASET_CODE = "mv_fire_rescue_activity_aggregates"
SOURCE_URL = "https://hzscr.gov.cz/hasicien/ViewFile.aspx?docid=22436114"
OUTPUT_FILE_NAME = "mv-fire-rescue-activity-aggregates.csv"
SUPPORTED_YEAR = 2024

REGION_MAP = {
    "Hl. m. Praha": ("CZ010", "Hlavní město Praha"),
    "Středočeský": ("CZ020", "Středočeský kraj"),
    "Jihočeský": ("CZ031", "Jihočeský kraj"),
    "Plzeňský": ("CZ032", "Plzeňský kraj"),
    "Karlovarský": ("CZ041", "Karlovarský kraj"),
    "Ústecký": ("CZ042", "Ústecký kraj"),
    "Liberecký": ("CZ051", "Liberecký kraj"),
    "Královéhradecký": ("CZ052", "Královéhradecký kraj"),
    "Pardubický": ("CZ053", "Pardubický kraj"),
    "Vysočina": ("CZ063", "Kraj Vysočina"),
    "Jihomoravský": ("CZ064", "Jihomoravský kraj"),
    "Olomoucký": ("CZ071", "Olomoucký kraj"),
    "Zlínský": ("CZ072", "Zlínský kraj"),
    "Moravskoslezský": ("CZ080", "Moravskoslezský kraj"),
}

REGION_LINE_RE = re.compile(
    r"^(?P<name>.+?)\s+"
    r"(?P<hzs>\d[\d ]*)\s+\d+\s+\d+,\d+\s+"
    r"(?P<jsdh>\d[\d ]*)\s+\d+\s+\d+,\d+\s+"
    r"(?P<hzs_podnik>\d[\d ]*)\s+\d+\s+\d+,\d+\s+"
    r"(?P<jsdh_podnik>\d[\d ]*)\s+\d+,\d+\s+"
    r"(?P<total>\d[\d ]*)\s+\d+$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch HZS activity aggregates from the official HZS statistical yearbook"
    )
    parser.add_argument("--year", type=int, default=SUPPORTED_YEAR, help="Reporting year, currently only 2024 is supported")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def parse_int(text: str) -> int:
    return int(text.replace(" ", ""))


def extract_region_rows(reader: PdfReader) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for page_index in (32, 33):
        text = reader.pages[page_index].extract_text() or ""
        for line in text.splitlines():
            candidate = line.strip()
            match = REGION_LINE_RE.match(candidate)
            if not match:
                continue
            name = match.group("name")
            if name not in REGION_MAP:
                continue
            region_code, region_name = REGION_MAP[name]
            rows.extend(
                [
                    {
                        "region_name": region_name,
                        "region_code": region_code,
                        "indicator_code": "hzs_interventions",
                        "indicator_name": "Počet zásahů HZS ČR",
                        "count_value": parse_int(match.group("hzs")),
                    },
                    {
                        "region_name": region_name,
                        "region_code": region_code,
                        "indicator_code": "jpo_total_interventions",
                        "indicator_name": "Počet zásahů jednotek požární ochrany celkem",
                        "count_value": parse_int(match.group("total")),
                    },
                ]
            )

    found_regions = {row["region_name"] for row in rows if row["indicator_code"] == "hzs_interventions"}
    expected_regions = {region_name for _, region_name in REGION_MAP.values()}
    if found_regions != expected_regions:
        missing = sorted(expected_regions - found_regions)
        raise RuntimeError(f"Missing HZS region rows for: {', '.join(missing)}")
    return rows


def extract_national_rows(reader: PdfReader) -> list[dict[str, object]]:
    text = reader.pages[29].extract_text() or ""
    national_line = None
    for line in text.splitlines():
        candidate = line.strip()
        if candidate.startswith("Celkem 135 632 148 836 110"):
            national_line = candidate
            break
    if not national_line:
        raise RuntimeError("Could not find national HZS totals in the HZS yearbook")

    tokens = national_line.split()[1:]
    if len(tokens) < 24:
        raise RuntimeError(f"Unexpected national HZS totals layout: {national_line}")

    national_hzs_2024 = parse_int(" ".join(tokens[2:4]))
    national_jpo_total_2024 = parse_int(" ".join(tokens[21:23]))
    return [
        {
            "region_name": "Česko",
            "region_code": "CZ",
            "indicator_code": "hzs_interventions",
            "indicator_name": "Počet zásahů HZS ČR",
            "count_value": national_hzs_2024,
        },
        {
            "region_name": "Česko",
            "region_code": "CZ",
            "indicator_code": "jpo_total_interventions",
            "indicator_name": "Počet zásahů jednotek požární ochrany celkem",
            "count_value": national_jpo_total_2024,
        },
    ]


def build_rows(pdf_bytes: bytes, reporting_year: int) -> list[dict[str, object]]:
    if reporting_year != SUPPORTED_YEAR:
        raise RuntimeError("Only 2024 HZS activity parsing is implemented")

    reader = PdfReader(io.BytesIO(pdf_bytes))
    rows = []
    rows.extend(extract_national_rows(reader))
    rows.extend(extract_region_rows(reader))
    for row in rows:
        row["reporting_year"] = reporting_year
        row["source_url"] = SOURCE_URL
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
    pdf_bytes = fetch_bytes(SOURCE_URL)
    rows = build_rows(pdf_bytes, args.year)

    metadata = {
        "dataset_code": DATASET_CODE,
        "source_url": SOURCE_URL,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "row_count": len(rows),
        "years": [args.year],
        "sha256": sha256_bytes(pdf_bytes),
        "size_bytes": len(pdf_bytes),
        "generator": "etl/mv/fetch_fire_rescue_activity_aggregates.py",
        "user_agent": USER_AGENT,
    }
    data_path = write_snapshot(out_dir=args.out_dir, snapshot=snapshot, rows=rows, metadata=metadata)

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
