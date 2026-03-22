#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from _common import RAW_ROOT, USER_AGENT, timestamp_label, write_sidecar

DATASET_CODE = "transport_budget_entities"
MONITOR_BASE_URL = "https://monitor.statnipokladna.gov.cz"
MONITOR_API_URL = f"{MONITOR_BASE_URL}/api/ukazatele"
OUTPUT_FILE_NAME = "monitor-ukazatele-transport-entities.csv"

TRANSPORT_ENTITIES = [
    {
        "entity_ico": "66003008",
        "entity_name": "Ministerstvo dopravy",
        "entity_kind": "ministry_admin",
    },
    {
        "entity_ico": "70856508",
        "entity_name": "Státní fond dopravní infrastruktury",
        "entity_kind": "infrastructure_fund",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch transport-sector budget entities from Monitor MF")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch. Can be used multiple times.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument("--workers", type=int, default=4, help="Parallel request workers")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def monitor_period_code(year: int) -> str:
    return f"{str(year)[-2:]}12"


def fetch_entity_row(entity: dict[str, str], year: int) -> tuple[dict[str, object] | None, str | None]:
    source_url = f"{MONITOR_API_URL}?ic={entity['entity_ico']}&obdobi={monitor_period_code(year)}"
    request = Request(source_url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})

    try:
        with urlopen(request, timeout=60) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 404:
            return None, f"404:{entity['entity_ico']}:{year}"
        return None, f"http:{exc.code}:{entity['entity_ico']}:{year}"
    except URLError as exc:
        return None, f"url:{entity['entity_ico']}:{year}:{exc.reason}"

    return (
        {
            "reporting_year": year,
            "period_code": monitor_period_code(year),
            "entity_ico": entity["entity_ico"],
            "entity_name": entity["entity_name"],
            "entity_kind": entity["entity_kind"],
            "expenses_czk": payload.get("vydaje", {}).get("value", 0),
            "costs_czk": payload.get("naklady", {}).get("value", 0),
            "revenues_czk": payload.get("vynosy", {}).get("value", 0),
            "result_czk": payload.get("vysledek", {}).get("value", 0),
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
        "expenses_czk",
        "costs_czk",
        "revenues_czk",
        "result_czk",
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
            "source_url": MONITOR_API_URL,
            "downloaded_at": datetime.now(UTC).isoformat(),
            "years": years,
            "entity_count": len(TRANSPORT_ENTITIES),
            "request_count": len(TRANSPORT_ENTITIES) * len(years),
            "row_count": len(rows),
            "failure_count": len(failures),
            "failures": failures[:100],
            "generator": "etl/transport/fetch_budget_entities.py",
        },
    )
    return data_path


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    snapshot = timestamp_label(args.snapshot)

    rows: list[dict[str, object]] = []
    failures: list[str] = []
    jobs = [(entity, year) for year in years for entity in TRANSPORT_ENTITIES]

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
    print(f"Entities queried: {len(TRANSPORT_ENTITIES)}")
    print(f"Rows written: {len(rows)}")
    if failures:
        print(f"Failures: {len(failures)}")


if __name__ == "__main__":
    main()
