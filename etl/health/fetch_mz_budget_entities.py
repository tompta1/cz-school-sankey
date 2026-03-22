#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from _common import RAW_ROOT, USER_AGENT, timestamp_label

DATASET_CODE = "health_mz_budget_entities"
MONITOR_BASE_URL = "https://monitor.statnipokladna.gov.cz"
MONITOR_API_URL = f"{MONITOR_BASE_URL}/api/ukazatele"
OUTPUT_FILE_NAME = "monitor-ukazatele-mz-budget-entities.csv"

MZ_BUDGET_ENTITIES = [
    {"entity_ico": "00024341", "entity_name": "Ministerstvo zdravotnictví", "region_name": "Česká republika", "entity_kind": "ministry_chapter_total"},
    {"entity_ico": "00023833", "entity_name": "Ústav zdravotnických informací a statistiky ČR", "region_name": "Hlavní město Praha", "entity_kind": "uzis"},
    {"entity_ico": "00023817", "entity_name": "Státní ústav pro kontrolu léčiv", "region_name": "Hlavní město Praha", "entity_kind": "sukl"},
    {"entity_ico": "71180397", "entity_name": "Koordinační středisko transplantací", "region_name": "Hlavní město Praha", "entity_kind": "kst"},
    {"entity_ico": "71009256", "entity_name": "Hygienická stanice hlavního města Prahy", "region_name": "Hlavní město Praha", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009159", "entity_name": "Krajská hygienická stanice Středočeského kraje", "region_name": "Středočeský kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009345", "entity_name": "Krajská hygienická stanice Jihočeského kraje", "region_name": "Jihočeský kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009299", "entity_name": "Krajská hygienická stanice Plzeňského kraje", "region_name": "Plzeňský kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009281", "entity_name": "Krajská hygienická stanice Karlovarského kraje", "region_name": "Karlovarský kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009183", "entity_name": "Krajská hygienická stanice Ústeckého kraje", "region_name": "Ústecký kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009302", "entity_name": "Krajská hygienická stanice Libereckého kraje", "region_name": "Liberecký kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009213", "entity_name": "Krajská hygienická stanice Královéhradeckého kraje", "region_name": "Královéhradecký kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009264", "entity_name": "Krajská hygienická stanice Pardubického kraje", "region_name": "Pardubický kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009311", "entity_name": "Krajská hygienická stanice kraje Vysočina", "region_name": "Kraj Vysočina", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009191", "entity_name": "Krajská hygienická stanice Jihomoravského kraje", "region_name": "Jihomoravský kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009248", "entity_name": "Krajská hygienická stanice Olomouckého kraje", "region_name": "Olomoucký kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009167", "entity_name": "Krajská hygienická stanice Moravskoslezského kraje", "region_name": "Moravskoslezský kraj", "entity_kind": "hygiene_station"},
    {"entity_ico": "71009221", "entity_name": "Krajská hygienická stanice Zlínského kraje", "region_name": "Zlínský kraj", "entity_kind": "hygiene_station"},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch MZ / KHS budget entities from Monitor MF")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch. Can be used multiple times.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    parser.add_argument("--workers", type=int, default=6, help="Parallel request workers")
    return parser.parse_args()


def monitor_period_code(year: int) -> str:
    return f"{str(year)[-2:]}12"


def fetch_entity_row(entity: dict[str, str], year: int) -> tuple[dict[str, object] | None, str | None]:
    entity_ico = entity["entity_ico"]
    source_url = f"{MONITOR_API_URL}?ic={entity_ico}&obdobi={monitor_period_code(year)}"
    request = Request(source_url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})

    try:
        with urlopen(request, timeout=60) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 404:
            return None, f"404:{entity_ico}:{year}"
        return None, f"http:{exc.code}:{entity_ico}:{year}"
    except URLError as exc:
        return None, f"url:{entity_ico}:{year}:{exc.reason}"

    return (
        {
            "reporting_year": year,
            "period_code": monitor_period_code(year),
            "entity_ico": entity_ico,
            "entity_name": entity["entity_name"],
            "entity_kind": entity["entity_kind"],
            "region_name": entity["region_name"],
            "expenses_czk": payload.get("vydaje", {}).get("value", 0),
            "costs_czk": payload.get("naklady", {}).get("value", 0),
            "revenues_czk": payload.get("vynosy", {}).get("value", 0),
            "result_czk": payload.get("vysledek", {}).get("value", 0),
            "assets_czk": payload.get("aktiva", {}).get("value", 0),
            "receivables_czk": payload.get("pohlbrut", {}).get("value", 0),
            "liabilities_czk": payload.get("cizzdr", {}).get("value", 0),
            "source_url": source_url,
        },
        None,
    )


def write_snapshot(
    *,
    out_dir: Path,
    snapshot: str,
    rows: list[dict[str, object]],
    years: list[int],
    failures: list[str],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "period_code",
        "entity_ico",
        "entity_name",
        "entity_kind",
        "region_name",
        "expenses_czk",
        "costs_czk",
        "revenues_czk",
        "result_czk",
        "assets_czk",
        "receivables_czk",
        "liabilities_czk",
        "source_url",
    ]

    with data_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    sidecar = {
        "dataset_code": DATASET_CODE,
        "source_url": MONITOR_API_URL,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "years": years,
        "entity_count": len(MZ_BUDGET_ENTITIES),
        "request_count": len(MZ_BUDGET_ENTITIES) * len(years),
        "row_count": len(rows),
        "failure_count": len(failures),
        "failures": failures[:100],
        "generator": "etl/health/fetch_mz_budget_entities.py",
    }
    sidecar_path = data_path.with_suffix(data_path.suffix + ".download.json")
    sidecar_path.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2), encoding="utf-8")
    return data_path


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    snapshot = timestamp_label(args.snapshot)

    jobs = [(entity, year) for year in years for entity in MZ_BUDGET_ENTITIES]
    rows: list[dict[str, object]] = []
    failures: list[str] = []

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = [executor.submit(fetch_entity_row, entity, year) for entity, year in jobs]
        for future in as_completed(futures):
            row, failure = future.result()
            if row is not None:
                rows.append(row)
            if failure is not None:
                failures.append(failure)

    rows.sort(key=lambda row: (int(row["reporting_year"]), str(row["entity_ico"])))
    data_path = write_snapshot(
        out_dir=args.out_dir,
        snapshot=snapshot,
        rows=rows,
        years=years,
        failures=failures,
    )

    print(f"Wrote {data_path}")
    print(f"Years: {', '.join(str(year) for year in years)}")
    print(f"Entities queried: {len(MZ_BUDGET_ENTITIES)}")
    print(f"Rows written: {len(rows)}")
    if failures:
        print(f"Failures: {len(failures)}")


if __name__ == "__main__":
    main()
