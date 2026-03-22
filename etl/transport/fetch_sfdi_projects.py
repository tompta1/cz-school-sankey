#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from pathlib import Path

from _common import RAW_ROOT, USER_AGENT, fetch_bytes, sha256_bytes, timestamp_label, write_sidecar

DATASET_CODE = "transport_sfdi_projects"
OUTPUT_FILE_NAME = "sfdi-project-execution.csv"


def source_url(year: int) -> str:
    return f"https://kz.sfdi.cz/opendata/export_evid_m330_{year}.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch SFDI project execution CSVs")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch. Can be used multiple times.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def parse_decimal(text: str) -> str:
    return (text or "0").strip().replace(",", ".")


def decode_csv_rows(data: bytes):
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    for row in reader:
        yield row


def build_rows(year: int, data: bytes, url: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in decode_csv_rows(data):
        eu_paid = sum(
            float(parse_decimal(row.get(key, "0")))
            for key in ["opd2_zapl", "opd3_zapl", "cef_zapl", "cef2_zapl", "rrf_zapl", "osteu_zapl"]
        )
        rows.append(
            {
                "reporting_year": year,
                "action_id": row.get("evid_cis", "").strip(),
                "budget_area_code": row.get("kod_ra", "").strip(),
                "action_type_code": row.get("druh_akce", "").strip(),
                "financing_code": row.get("druh_fin", "").strip(),
                "status_code": row.get("stav_akce2", "").strip(),
                "project_name": row.get("nazev_akce", "").strip(),
                "total_cost_czk": parse_decimal(row.get("celk_nakl", "0")),
                "adjusted_budget_czk": parse_decimal(row.get("celk_upr", "0")),
                "paid_czk": parse_decimal(row.get("celk_zapl", "0")),
                "sfdi_paid_czk": parse_decimal(row.get("sfdi_zapl", "0")),
                "eu_paid_czk": f"{eu_paid:.2f}",
                "region_code": row.get("zkr_kraj", "").strip(),
                "investor_name": row.get("investor", "").strip(),
                "investor_ico": row.get("ico", "").strip(),
                "investor_address": row.get("sidlo", "").strip(),
                "start_period": row.get("zac_akce", "").strip(),
                "end_period": row.get("kon_akce", "").strip(),
                "source_url": url,
            }
        )
    return rows


def write_snapshot(*, out_dir: Path, snapshot: str, rows: list[dict[str, object]], metadata: dict) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "action_id",
        "budget_area_code",
        "action_type_code",
        "financing_code",
        "status_code",
        "project_name",
        "total_cost_czk",
        "adjusted_budget_czk",
        "paid_czk",
        "sfdi_paid_czk",
        "eu_paid_czk",
        "region_code",
        "investor_name",
        "investor_ico",
        "investor_address",
        "start_period",
        "end_period",
        "source_url",
    ]

    with data_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    write_sidecar(data_path, metadata)
    return data_path


def main() -> None:
    args = parse_args()
    snapshot = timestamp_label(args.snapshot)
    years = sorted(set(args.year))

    rows: list[dict[str, object]] = []
    sources: list[dict[str, object]] = []
    for year in years:
        url = source_url(year)
        data = fetch_bytes(url)
        rows.extend(build_rows(year, data, url))
        sources.append(
            {
                "year": year,
                "source_url": url,
                "sha256": sha256_bytes(data),
                "size_bytes": len(data),
                "user_agent": USER_AGENT,
            }
        )

    metadata = {
        "dataset_code": DATASET_CODE,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "years": years,
        "row_count": len(rows),
        "sources": sources,
        "generator": "etl/transport/fetch_sfdi_projects.py",
    }
    data_path = write_snapshot(out_dir=args.out_dir, snapshot=snapshot, rows=rows, metadata=metadata)

    print(f"Wrote {data_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
