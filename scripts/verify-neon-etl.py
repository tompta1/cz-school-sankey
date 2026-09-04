#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from collections import defaultdict
from datetime import datetime

import psycopg

ALIAS_MAP = {
    "regions": "mmr",
    "business": "mpo",
    "culture": "mk",
    "foreign": "mzv",
    "internal": "security",
    "mv": "security",
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
    "security": {2024, 2025},
    "mf": {2023, 2024, 2025},
    "mo": {2024, 2025},
}

TOP_LEVEL_DATASETS = {
    "school": "school_state_budget",
    "health": "health_mz_budget_entities",
    "social": "social_mpsv_aggregates",
    "justice": "justice_budget_aggregates",
    "agriculture": "agriculture_budget_entities",
    "environment": "environment_budget_entities",
    "mmr": "mmr_budget_aggregates",
    "mpo": "mpo_budget_entities",
    "mk": "mk_budget_entities",
    "mzv": "mzv_budget_entities",
    "transport": "transport_budget_entities",
    "security": "mv_budget_aggregates",
    "mf": "mf_budget_entities",
    "mo": "mo_budget_entities",
}


def expected_datasets(domain: str, years: set[int]) -> set[str]:
    supported = years & SUPPORTED_YEARS[domain]
    if not supported:
        return set()
    if domain == "school":
        return {
            "school_entities",
            "school_allocations",
            "school_eu_projects",
            "school_founder_support",
            "school_state_budget",
        }
    if domain == "health":
        return {
            "nrpzs_provider_sites",
            "nrhzs_claims_provider_specialty",
            "nrhzs_claims_provider_ico",
            "nrhzs_claims_payer",
            "health_insurer_codebook",
            "health_monitor_indicators",
            "health_mz_budget_entities",
            "health_financing_aggregates",
            "health_zzs_activity_aggregates",
        }
    if domain == "social":
        return {"social_mpsv_aggregates", "social_recipient_metrics"}
    if domain == "justice":
        return {"justice_budget_aggregates"} | ({"justice_activity_aggregates"} if 2024 in supported else set())
    if domain == "agriculture":
        return {"agriculture_budget_entities"} | (
            {"agriculture_szif_payments", "agriculture_lpis_user_area"} if 2024 in supported else set()
        )
    if domain == "environment":
        return {"environment_budget_entities", "environment_sfzp_supports"}
    if domain == "mmr":
        return {"mmr_budget_aggregates", "mmr_irop_operations"}
    if domain == "mpo":
        return {"mpo_budget_entities", "mpo_optak_operations"}
    if domain == "mk":
        return {"mk_budget_entities"} | (
            {"mk_budget_aggregates", "mk_support_awards", "mk_region_metrics"} if 2024 in supported else set()
        )
    if domain == "mzv":
        return {"mzv_budget_entities"} | (
            {"mzv_diplomatic_metrics", "mzv_aid_operations"} if 2024 in supported else set()
        )
    if domain == "transport":
        return {"transport_budget_entities", "transport_sfdi_projects", "transport_activity_metrics"}
    if domain == "security":
        return {"mv_budget_aggregates", "mv_police_crime_aggregates"} | (
            {"mv_fire_rescue_activity_aggregates"} if 2024 in supported else set()
        )
    if domain == "mf":
        return {"mf_budget_entities", "mf_activity_metrics"}
    if domain == "mo":
        return {"mo_budget_entities", "mo_budget_aggregates", "mo_personnel_metrics"}
    return set()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify selected Neon ETL domains were loaded successfully")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--domain", action="append", required=True)
    parser.add_argument("--year", action="append", required=True, type=int)
    parser.add_argument(
        "--started-after",
        type=datetime.fromisoformat,
        help="Require expected releases to have been fetched at or after this ISO-8601 timestamp.",
    )
    parser.add_argument("--verify-school-transforms", action="store_true")
    return parser.parse_args()


def normalize_domains(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        domain = ALIAS_MAP.get(value, value)
        if domain not in SUPPORTED_YEARS:
            raise SystemExit(f"Unknown domain: {value}")
        if domain not in normalized:
            normalized.append(domain)
    return normalized


def release_years(reporting_year: int | None, metadata: object) -> set[int]:
    years: set[int] = set()
    if reporting_year is not None:
        years.add(int(reporting_year))
    if isinstance(metadata, dict):
        values = metadata.get("years")
        if isinstance(values, list):
            for value in values:
                if isinstance(value, int) or isinstance(value, str) and value.isdigit():
                    years.add(int(value))
    return years


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    selected_domains = normalize_domains(args.domain)
    selected_years = sorted(set(args.year))
    requested_years = set(selected_years)

    with psycopg.connect(args.database_url) as conn:
        releases = conn.execute(
            """
            select domain_code, dataset_code, reporting_year, metadata, row_count,
                   status, fetched_at, published_at, snapshot_label
            from meta.dataset_release
            where domain_code = any(%s)
            order by domain_code, dataset_code, coalesce(published_at, fetched_at) desc
            """,
            (selected_domains,),
        ).fetchall()

        school_rows = []
        if args.verify_school_transforms and "school" in selected_domains:
            school_rows = conn.execute(
                """
                select
                  rp.calendar_year,
                  (select count(*) from core.school_capacity sc where sc.reporting_period_id = rp.reporting_period_id),
                  (select count(*) from core.financial_flow ff where ff.reporting_period_id = rp.reporting_period_id and ff.budget_domain = 'school'),
                  (select count(*) from core.financial_flow ff where ff.reporting_period_id = rp.reporting_period_id and ff.budget_domain = 'school' and ff.flow_type = 'direct_school_finance')
                from core.reporting_period rp
                where rp.domain_code = 'school' and rp.calendar_year = any(%s)
                order by rp.calendar_year
                """,
                (selected_years,),
            ).fetchall()

    by_domain_dataset: dict[tuple[str, str], list[object]] = defaultdict(list)
    for row in releases:
        by_domain_dataset[(str(row[0]), str(row[1]))].append(row)

    print("## Neon ETL Verification")
    print()
    print(f"Selected domains: {', '.join(selected_domains)}")
    print(f"Selected years: {', '.join(str(year) for year in selected_years)}")
    if args.started_after:
        print(f"Freshness boundary: {args.started_after.isoformat()}")
    print()
    print("| Domain | Dataset | Snapshot | Rows | Status | Covered years | Fetched at |")
    print("|---|---|---|---:|---|---|---|")

    errors: list[str] = []
    for domain in selected_domains:
        datasets = expected_datasets(domain, requested_years)
        for dataset in sorted(datasets):
            candidates = by_domain_dataset.get((domain, dataset), [])
            current_candidates = [
                row for row in candidates if args.started_after is None or row[6] >= args.started_after
            ]
            if not current_candidates:
                print(f"| {domain} | {dataset} | missing | 0 | missing | | |")
                suffix = " refreshed by this run" if candidates else ""
                errors.append(f"{domain}/{dataset}: no release{suffix} found")
                continue

            for current in current_candidates:
                covered = release_years(current[2], current[3])
                row_count = int(current[4] or 0)
                status = str(current[5])
                fetched_at = current[6]
                print(
                    f"| {domain} | {dataset} | {current[8]} | {row_count} | {status} | "
                    f"{', '.join(str(year) for year in sorted(covered)) or 'n/a'} | {fetched_at.isoformat()} |"
                )

                if row_count <= 0:
                    errors.append(f"{domain}/{dataset}/{current[8]}: release has no rows")
                if status != "published" or current[7] is None:
                    errors.append(f"{domain}/{dataset}/{current[8]}: release is not published")

        top_level_dataset = TOP_LEVEL_DATASETS[domain]
        top_level_releases = [
            row
            for row in by_domain_dataset.get((domain, top_level_dataset), [])
            if args.started_after is None or row[6] >= args.started_after
        ]
        top_level_years = set().union(*(release_years(row[2], row[3]) for row in top_level_releases))
        for year in sorted(requested_years & SUPPORTED_YEARS[domain]):
            if year not in top_level_years:
                errors.append(f"{domain}/{top_level_dataset}: top-level source does not cover {year}")

    if args.verify_school_transforms and "school" in selected_domains:
        school_summary = {int(row[0]): row for row in school_rows}
        print()
        print("### School Core Verification")
        print()
        print("| Year | School capacity rows | School flow rows | Direct school finance rows |")
        print("|---:|---:|---:|---:|")
        for year in sorted(requested_years & SUPPORTED_YEARS["school"]):
            row = school_summary.get(year)
            counts = (0, 0, 0) if row is None else tuple(int(value) for value in row[1:4])
            print(f"| {year} | {counts[0]} | {counts[1]} | {counts[2]} |")
            if not all(count > 0 for count in counts):
                errors.append(f"school: incomplete transformed data for {year}")

    if errors:
        print()
        print("### Verification Errors")
        for error in errors:
            print(f"- {error}")
        raise SystemExit(1)

    print()
    print("Neon ETL verification passed.")


if __name__ == "__main__":
    main()
