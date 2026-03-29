#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

import psycopg

ALIAS_MAP = {
    "regions": "mmr",
    "business": "mpo",
    "culture": "mk",
    "foreign": "mzv",
    "internal": "mv",
    "finance": "mf",
    "defense": "mo",
}

SUPPORTED_YEARS = {
    "school": {2024, 2025},
    "health": {2024, 2025},
    "social": {2024},
    "justice": {2024, 2025},
    "agriculture": {2024, 2025},
    "environment": {2024, 2025},
    "mmr": {2024, 2025},
    "mpo": {2024, 2025},
    "mk": {2024, 2025},
    "mzv": {2024, 2025},
    "transport": {2024, 2025},
    "mv": {2024, 2025},
    "mf": {2023, 2024, 2025},
    "mo": {2024, 2025},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify selected Neon ETL domains were loaded successfully")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument("--domain", action="append", required=True, help="Selected domain code or friendly alias")
    parser.add_argument("--year", action="append", required=True, type=int, help="Selected reporting year")
    parser.add_argument(
        "--verify-school-transforms",
        action="store_true",
        help="Also verify transformed school core tables for the selected years",
    )
    return parser.parse_args()


def normalize_domains(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        domain = ALIAS_MAP.get(value, value)
        if domain not in SUPPORTED_YEARS:
            raise SystemExit(f"Unknown domain: {value}")
        if domain not in seen:
            normalized.append(domain)
            seen.add(domain)
    return normalized


def format_year(value: int | None) -> str:
    return "n/a" if value is None else str(value)


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    selected_domains = normalize_domains(args.domain)
    selected_years = sorted(set(args.year))

    with psycopg.connect(args.database_url) as conn:
        release_rows = conn.execute(
            """
            select
              domain_code,
              reporting_year,
              count(*) as release_count,
              coalesce(sum(coalesce(row_count, 0)), 0) as total_rows,
              string_agg(distinct dataset_code, ', ' order by dataset_code) as dataset_codes
            from meta.dataset_release
            where domain_code = any(%s)
            group by domain_code, reporting_year
            order by domain_code, reporting_year
            """,
            (selected_domains,),
        ).fetchall()

        school_rows = []
        if args.verify_school_transforms and "school" in selected_domains:
            school_rows = conn.execute(
                """
                select
                  rp.calendar_year,
                  count(distinct sc.school_capacity_id) as school_capacity_rows,
                  count(distinct ff.financial_flow_id) as school_flow_rows,
                  count(distinct ff.financial_flow_id) filter (where ff.flow_type = 'direct_school_finance') as direct_school_finance_rows
                from core.reporting_period rp
                left join core.school_capacity sc
                  on sc.reporting_period_id = rp.reporting_period_id
                left join core.financial_flow ff
                  on ff.reporting_period_id = rp.reporting_period_id
                 and ff.budget_domain = 'school'
                where rp.domain_code = 'school'
                  and rp.calendar_year = any(%s)
                group by rp.calendar_year
                order by rp.calendar_year
                """,
                (selected_years,),
            ).fetchall()

    summary: dict[str, dict[int | None, dict[str, object]]] = defaultdict(dict)
    for row in release_rows:
        summary[str(row[0])][row[1]] = {
            "release_count": int(row[2]),
            "total_rows": int(row[3]),
            "dataset_codes": row[4] or "",
        }

    print("## Neon ETL Verification")
    print()
    print(f"Selected domains: {', '.join(selected_domains)}")
    print(f"Selected years: {', '.join(str(year) for year in selected_years)}")
    print()
    print("### Dataset Releases")
    print()
    print("| Domain | Year | Releases | Rows | Datasets |")
    print("|---|---:|---:|---:|---|")
    if not release_rows:
        print("| none | n/a | 0 | 0 | |")

    for domain in selected_domains:
        domain_rows = summary.get(domain, {})
        printed = False
        for year in sorted(domain_rows, key=lambda value: (-1 if value is None else value)):
            row = domain_rows[year]
            print(
                f"| {domain} | {format_year(year)} | {row['release_count']} | {row['total_rows']} | {row['dataset_codes']} |"
            )
            printed = True
        if not printed:
            print(f"| {domain} | n/a | 0 | 0 | |")

    errors: list[str] = []
    for domain in selected_domains:
        domain_rows = summary.get(domain, {})
        if not domain_rows:
            errors.append(f"{domain}: no dataset releases found")
            continue

        expected_years = [year for year in selected_years if year in SUPPORTED_YEARS.get(domain, set())]
        for year in expected_years:
            if year not in domain_rows:
                errors.append(f"{domain}: missing dataset release for reporting year {year}")

    if args.verify_school_transforms and "school" in selected_domains:
        school_summary = {int(row[0]): row for row in school_rows}
        print()
        print("### School Core Verification")
        print()
        print("| Year | School capacity rows | School flow rows | Direct school finance rows |")
        print("|---:|---:|---:|---:|")
        for year in selected_years:
            row = school_summary.get(year)
            if row is None:
                print(f"| {year} | 0 | 0 | 0 |")
                errors.append(f"school: missing transformed reporting period for {year}")
                continue

            capacity_rows = int(row[1])
            flow_rows = int(row[2])
            direct_rows = int(row[3])
            print(f"| {year} | {capacity_rows} | {flow_rows} | {direct_rows} |")

            if capacity_rows <= 0:
                errors.append(f"school: no core.school_capacity rows for {year}")
            if flow_rows <= 0:
                errors.append(f"school: no core.financial_flow rows for {year}")
            if direct_rows <= 0:
                errors.append(f"school: no direct_school_finance flows for {year}")

    if errors:
        print()
        print("### Verification Errors")
        print()
        for error in errors:
            print(f"- {error}")
        raise SystemExit(1)

    print()
    print("Neon ETL verification passed.")


if __name__ == "__main__":
    main()
