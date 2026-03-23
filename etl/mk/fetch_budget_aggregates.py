#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

from pypdf import PdfReader

from _common import RAW_ROOT, fetch_bytes, parse_money, timestamp_label, write_sidecar

DATASET_CODE = "mk_budget_aggregates"
OUTPUT_FILE_NAME = "mk-budget-aggregates.csv"
FINAL_ACCOUNT_URL = "https://mk.gov.cz/doc/cms_library/01_zu_334_zaverecny_-ucet_mk-20244222211805-21299.pdf"

BUDGET_CODES = [
    ("CHURCH_SETTLEMENT_TOTAL", "Majetkové vyrovnání s církvemi a náboženskými společnostmi", "5020000000"),
    ("CHURCH_SUPPORT", "Příspěvek na podporu činnosti církví a náboženských společností", "5020020011"),
    ("MK_CONTRIBUTORY_ORGS_TOTAL", "Příspěvkové organizace zřízené Ministerstvem kultury", "5040000000"),
    ("MK_CULTURAL_SERVICES_TOTAL", "Kulturní služby, podpora živého umění", "5050000000"),
    ("MK_HERITAGE_TOTAL", "Záchrana a obnova kulturních památek, veřejné služby muzeí", "5060000000"),
    ("MK_REGIONAL_INFRA_TOTAL", "Podpora rozvoje a obnovy materiálně technické základny regionálních kulturních zařízení", "5080000000"),
    ("MK_FILM_TOTAL", "Státní fond kinematografie", "5090000000"),
    ("FILM_INCENTIVES", "Dotace na filmové pobídky", "5090010011"),
    ("FILM_FUND_OPERATING", "Dotace ze státního rozpočtu pro Státní fond kinematografie", "5090020011"),
    ("CULTURE_FUND", "Státní fond kultury České republiky", "5100010011"),
]

SUMMARY_LABELS = {
    "CHURCH_SETTLEMENT_TOTAL": "Výdaje dle zákona o majetkovém vyrovnání s církvemi a náboženskými společnostmi",
    "MK_CONTRIBUTORY_ORGS_TOTAL": "Příspěvkové organizace zřízené Ministerstvem kultury",
    "MK_CULTURAL_SERVICES_TOTAL": "Kulturní služby, podpora živého umění",
    "MK_HERITAGE_TOTAL": "Záchrana a obnova kulturních památek, veřejné služby muzeí",
    "MK_REGIONAL_INFRA_TOTAL": "Podpora rozvoje a obnovy materiálně technické základny regionálních kulturních zařízení",
    "MK_FILM_TOTAL": "Státní fond kinematografie",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch MK budget aggregates from the official final account PDF")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch. Only 2024 is currently supported.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def extract_actual_amount(text: str, pvs_code: str) -> float:
    index = text.find(pvs_code)
    if index < 0:
        raise ValueError(f"Code {pvs_code} not found in MK final account PDF")
    section = text[index : index + 800]
    amounts = [parse_money(match) for match in __import__("re").findall(r"\d[\d ]*,\d+", section)]
    if len(amounts) < 4:
        raise ValueError(f"Code {pvs_code} does not expose enough amount columns")
    return amounts[3]


def extract_summary_actual_amount(summary_text: str, label: str) -> float:
    index = summary_text.find(label)
    if index < 0:
        raise ValueError(f"Summary label {label!r} not found in MK final account PDF")
    section = summary_text[index : index + 700]
    amounts = [
        float(match.replace(".", "").replace(" ", "").replace(",", "."))
        for match in re.findall(r"\d[\d .]*,\d+", section)
    ]
    if len(amounts) < 5:
        raise ValueError(f"Summary label {label!r} does not expose enough amount columns")
    return amounts[4]


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    unsupported = [year for year in years if year != 2024]
    if unsupported:
        raise SystemExit(f"MK budget aggregates currently support only 2024, got: {', '.join(map(str, unsupported))}")

    snapshot = timestamp_label(args.snapshot)
    pdf_bytes = fetch_bytes(FINAL_ACCOUNT_URL)
    reader = PdfReader(BytesIO(pdf_bytes))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    normalized_text = re.sub(r"\s+", " ", text)
    summary_start = normalized_text.rfind("Specifické ukazatele - výdaje")
    summary_text = normalized_text[summary_start:] if summary_start >= 0 else normalized_text

    rows: list[dict[str, object]] = []
    for year in years:
        for metric_code, metric_name, pvs_code in BUDGET_CODES:
            if metric_code in SUMMARY_LABELS:
                amount_czk = extract_summary_actual_amount(summary_text, SUMMARY_LABELS[metric_code])
            else:
                amount_czk = extract_actual_amount(text, pvs_code)
            rows.append(
                {
                    "reporting_year": year,
                    "metric_code": metric_code,
                    "metric_name": metric_name,
                    "pvs_code": pvs_code,
                    "amount_czk": amount_czk * 1000,
                    "source_url": FINAL_ACCOUNT_URL,
                }
            )

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = ["reporting_year", "metric_code", "metric_name", "pvs_code", "amount_czk", "source_url"]
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
            "source_url": FINAL_ACCOUNT_URL,
            "generator": "etl/mk/fetch_budget_aggregates.py",
        },
    )

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
