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

import psycopg

from _common import RAW_ROOT, USER_AGENT, timestamp_label

DATASET_CODE = "health_monitor_indicators"
MONITOR_BASE_URL = "https://monitor.statnipokladna.gov.cz"
MONITOR_API_URL = f"{MONITOR_BASE_URL}/api/ukazatele"
OUTPUT_FILE_NAME = "monitor-ukazatele-provider-ico.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Monitor MF finance indicators for focused health providers")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string used to discover candidate providers",
    )
    parser.add_argument("--year", action="append", type=int, help="Reporting year to fetch. Can be used multiple times.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit provider count for local validation")
    parser.add_argument("--workers", type=int, default=6, help="Parallel request workers")
    return parser.parse_args()


def monitor_period_code(year: int) -> str:
    return f"{str(year)[-2:]}12"


def get_years(conn: psycopg.Connection, explicit_years: list[int] | None) -> list[int]:
    if explicit_years:
      return sorted(set(explicit_years))

    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct reporting_year
            from mart.health_claims_provider_yearly
            order by reporting_year
            """
        )
        return [int(row[0]) for row in cur.fetchall()]


def get_candidate_providers(conn: psycopg.Connection, limit: int | None) -> list[dict[str, object]]:
    sql = """
      with provider_directory as (
        select
          provider_ico,
          max(provider_name) as provider_name,
          max(region_name) as region_name,
          bool_or(
            lower(coalesce(facility_type_name, '')) like '%%nemoc%%'
            or lower(coalesce(provider_type, '')) like '%%nemoc%%'
          ) as hospital_like,
          bool_or(
            lower(coalesce(care_field, '')) like '%%hygiena a epidemiologie%%'
            or lower(coalesce(facility_type_name, '')) like '%%zdravotní ústav%%'
            or lower(coalesce(provider_type, '')) like '%%zdravotní ústav%%'
          ) as public_health_like
        from mart.health_provider_directory
        where provider_ico is not null and provider_ico <> ''
        group by provider_ico
      )
      select distinct
        p.provider_ico,
        coalesce(d.provider_name, p.provider_ico) as provider_name,
        d.region_name,
        coalesce(d.hospital_like, false) as hospital_like,
        coalesce(d.public_health_like, false) as public_health_like
      from mart.health_claims_provider_yearly p
      join provider_directory d using (provider_ico)
      where d.hospital_like or d.public_health_like
      order by provider_name, p.provider_ico
    """
    if limit is not None:
        sql += " limit %s"
        params = (limit,)
    else:
        params = ()

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [
        {
            "provider_ico": row[0],
            "provider_name": row[1],
            "region_name": row[2],
            "hospital_like": bool(row[3]),
            "public_health_like": bool(row[4]),
        }
        for row in rows
    ]


def fetch_monitor_row(provider: dict[str, object], year: int) -> tuple[dict[str, object] | None, str | None]:
    provider_ico = str(provider["provider_ico"])
    source_url = f"{MONITOR_API_URL}?ic={provider_ico}&obdobi={monitor_period_code(year)}"
    request = Request(source_url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})

    try:
        with urlopen(request, timeout=60) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 404:
            return None, f"404:{provider_ico}:{year}"
        return None, f"http:{exc.code}:{provider_ico}:{year}"
    except URLError as exc:
        return None, f"url:{provider_ico}:{year}:{exc.reason}"

    return (
        {
            "reporting_year": year,
            "period_code": monitor_period_code(year),
            "provider_ico": provider_ico,
            "provider_name": str(provider["provider_name"] or provider_ico),
            "region_name": str(provider["region_name"] or ""),
            "hospital_like": "true" if provider["hospital_like"] else "false",
            "public_health_like": "true" if provider["public_health_like"] else "false",
            "revenues_czk": payload.get("vynosy", {}).get("value", 0),
            "costs_czk": payload.get("naklady", {}).get("value", 0),
            "result_czk": payload.get("vysledek", {}).get("value", 0),
            "assets_czk": payload.get("aktiva", {}).get("value", 0),
            "receivables_czk": payload.get("pohlbrut", {}).get("value", 0),
            "liabilities_czk": payload.get("cizzdr", {}).get("value", 0),
            "short_term_liabilities_czk": payload.get("kratzav", {}).get("value", 0),
            "long_term_liabilities_czk": payload.get("dlouzav", {}).get("value", 0),
            "total_debt_czk": payload.get("dluhcelk", {}).get("value", 0),
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
    provider_count: int,
    failures: list[str],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"

    fieldnames = [
        "reporting_year",
        "period_code",
        "provider_ico",
        "provider_name",
        "region_name",
        "hospital_like",
        "public_health_like",
        "revenues_czk",
        "costs_czk",
        "result_czk",
        "assets_czk",
        "receivables_czk",
        "liabilities_czk",
        "short_term_liabilities_czk",
        "long_term_liabilities_czk",
        "total_debt_czk",
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
        "provider_count": provider_count,
        "request_count": provider_count * len(years),
        "row_count": len(rows),
        "failure_count": len(failures),
        "failures": failures[:100],
        "generator": "etl/health/fetch_monitor_indicators.py",
    }
    sidecar_path = data_path.with_suffix(data_path.suffix + ".download.json")
    sidecar_path.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2), encoding="utf-8")
    return data_path


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    snapshot = timestamp_label(args.snapshot)

    with psycopg.connect(args.database_url, autocommit=True) as conn:
        years = get_years(conn, args.year)
        providers = get_candidate_providers(conn, args.limit)

    jobs = [(provider, year) for year in years for provider in providers]
    rows: list[dict[str, object]] = []
    failures: list[str] = []

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = [executor.submit(fetch_monitor_row, provider, year) for provider, year in jobs]
        for future in as_completed(futures):
            row, failure = future.result()
            if row is not None:
                rows.append(row)
            if failure is not None:
                failures.append(failure)

    rows.sort(key=lambda row: (int(row["reporting_year"]), str(row["provider_ico"])))
    data_path = write_snapshot(
        out_dir=args.out_dir,
        snapshot=snapshot,
        rows=rows,
        years=years,
        provider_count=len(providers),
        failures=failures,
    )

    print(f"Wrote {data_path}")
    print(f"Years: {', '.join(str(year) for year in years)}")
    print(f"Providers queried: {len(providers)}")
    print(f"Rows written: {len(rows)}")
    if failures:
        print(f"Failures: {len(failures)}")


if __name__ == "__main__":
    main()
