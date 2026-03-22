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

DATASET_CODE = "mv_budget_aggregates"
CHAPTER_CODE = "314"
CHAPTER_NAME = "Ministerstvo vnitra"
OUTPUT_FILE_NAME = "mv-budget-aggregates.csv"

SOURCE_SPECS = {
    2024: {
        "url": "https://mv.gov.cz/soubor/navrh-zaverecneho-uctu-kapitoly-314-ministerstvo-vnitra-za-rok-2024.aspx",
        "basis": "realized",
        "summary_marker": "Ukazatel Schválený rozpočet Rozpočet po změnách Skutečnost",
        "number_index": -1,
        "multiplier": 1000,
    },
    2025: {
        "url": "https://mv.gov.cz/soubor/ukazatele-kapitoly-314-ministerstvo-vnitra-pro-rozpoctove-obdobi-2025-2027.aspx",
        "basis": "budgeted",
        "summary_marker": "Souhrnné ukazatele",
        "number_index": 0,
        "multiplier": 1,
    },
}

METRIC_SPECS = [
    ("chapter_total", "total_expenditure", "Výdaje celkem"),
    ("security", "police", "Výdaje Policie ČR"),
    ("security", "fire_rescue", "Výdaje Hasičského záchranného sboru ČR"),
    ("operations", "ministry_admin", "Výdaje na zabezpečení plnění úkolů Ministerstva vnitra"),
    ("operations", "sport", "Výdaje na sportovní reprezentaci"),
    ("social", "pensions", "Dávky důchodového pojištění"),
    ("social", "other_social", "Ostatní sociální dávky"),
]

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch MV budget aggregates from official MV budget PDFs")
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


def parse_amount(token: str, multiplier: int) -> int:
    normalized = token.replace("\xa0", "").replace(" ", "").replace(",", ".")
    return int(round(float(normalized) * multiplier))


def extract_summary_text(pdf_bytes: bytes, marker: str) -> str:
    pdf_path = Path("/tmp/mv_budget_aggregate.pdf")
    pdf_path.write_bytes(pdf_bytes)
    reader = PdfReader(str(pdf_path))
    for page in reader.pages:
        text = page.extract_text() or ""
        if marker in normalize_space(text):
            return text
    raise RuntimeError(f"Could not find summary marker {marker!r} in MV PDF")


def extract_metric_amount(
    summary_text: str,
    label: str,
    trailing_labels: list[str],
    number_index: int,
    multiplier: int,
) -> int:
    normalized_label = normalize_space(label)

    for line in summary_text.splitlines():
        normalized_line = normalize_space(line)
        if not normalized_line.startswith(normalized_label):
            continue
        chunks = re.findall(r"\d+", normalized_line[len(normalized_label):])
        if len(chunks) < 3 or len(chunks) % 3 != 0:
            continue
        group_size = len(chunks) // 3
        amounts = [
            "".join(chunks[index:index + group_size])
            for index in range(0, len(chunks), group_size)
        ]
        return parse_amount(amounts[number_index], multiplier)

    normalized_text = normalize_space(summary_text)
    start = normalized_text.find(normalized_label)
    if start < 0:
        raise RuntimeError(f"Could not find line for {label!r}")

    end = len(normalized_text)
    search_start = start + len(normalized_label)
    for trailing_label in trailing_labels:
        candidate = normalized_text.find(normalize_space(trailing_label), search_start)
        if candidate >= 0:
            end = min(end, candidate)

    snippet = normalized_text[search_start:end]
    chunks = re.findall(r"\d+", snippet)
    if len(chunks) < 3 or len(chunks) % 3 != 0:
        raise RuntimeError(f"Unexpected numeric layout for {label!r}: {snippet[:200]!r}")

    group_size = len(chunks) // 3
    amounts = [
        "".join(chunks[index:index + group_size])
        for index in range(0, len(chunks), group_size)
    ]
    return parse_amount(amounts[number_index], multiplier)


def build_rows(year: int, summary_text: str, basis: str, number_index: int, multiplier: int, source_url: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    labels = [label for _, _, label in METRIC_SPECS]
    for position, (metric_group, metric_code, label) in enumerate(METRIC_SPECS):
        rows.append(
            {
                "reporting_year": year,
                "basis": basis,
                "chapter_code": CHAPTER_CODE,
                "chapter_name": CHAPTER_NAME,
                "metric_group": metric_group,
                "metric_code": metric_code,
                "metric_name": label,
                "amount_czk": extract_metric_amount(summary_text, label, labels[position + 1:], number_index, multiplier),
                "source_url": source_url,
            }
        )
    return rows


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
        summary_text = extract_summary_text(pdf_bytes, spec["summary_marker"])
        rows.extend(
            build_rows(
                year=year,
                summary_text=summary_text,
                basis=str(spec["basis"]),
                number_index=int(spec["number_index"]),
                multiplier=int(spec["multiplier"]),
                source_url=str(spec["url"]),
            )
        )
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
        "generator": "etl/mv/fetch_budget_aggregates.py",
        "user_agent": USER_AGENT,
    }
    data_path = write_snapshot(out_dir=args.out_dir, snapshot=snapshot, rows=rows, metadata=metadata)

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
