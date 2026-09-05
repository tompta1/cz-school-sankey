#!/usr/bin/env python3
"""Apply validated recipient-level founder evidence without rebuilding school data."""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb


ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update only source-backed founder rows in raw and core tables"
    )
    parser.add_argument("--year", action="append", type=int, required=True)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Commit updates. Without this flag all validation runs and the transaction rolls back.",
    )
    return parser.parse_args()


def load_evidence(year: int) -> list[dict[str, str]]:
    path = RAW_ROOT / str(year) / "founder_recipient_evidence.csv"
    if not path.exists():
        raise RuntimeError(f"Missing {path}")
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise RuntimeError(f"No evidence rows in {path}")
    return rows


def apply_year(
    conn: psycopg.Connection,
    year: int,
    evidence_rows: list[dict[str, str]],
) -> tuple[int, int]:
    raw_updated = 0
    core_updated = 0
    with conn.cursor() as cur:
        for row in evidence_rows:
            institution_id = row["institution_id"]
            founder_id = row["founder_id"]
            amount = int(row["amount"])
            if amount < 0 or row["certainty"] != "observed":
                raise RuntimeError(f"Invalid recipient evidence for {institution_id}")

            payload = Jsonb(
                {
                    "attribution_method": row["attribution_method"],
                    "source_document_kind": row["source_document_kind"],
                    "source_url": row["source_url"],
                }
            )
            cur.execute(
                """
                update raw.school_founder_support support
                set amount_czk = %s,
                    basis = %s,
                    certainty = %s,
                    note = %s,
                    payload = support.payload || %s,
                    loaded_at = now()
                where support.reporting_year = %s
                  and support.institution_id = %s
                  and exists (
                    select 1
                    from raw.school_entities entity
                    where entity.reporting_year = support.reporting_year
                      and entity.institution_id = support.institution_id
                      and entity.founder_id = %s
                  )
                """,
                (
                    amount,
                    row["basis"],
                    row["certainty"],
                    row["note"],
                    payload,
                    year,
                    institution_id,
                    founder_id,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Expected one raw founder row for {institution_id} in {year}; updated {cur.rowcount}"
                )
            raw_updated += cur.rowcount

            lineage = Jsonb(
                {
                    "year": year,
                    "institution_id": institution_id,
                    "attribution_method": row["attribution_method"],
                    "source_document_kind": row["source_document_kind"],
                }
            )
            cur.execute(
                """
                update core.financial_flow flow
                set amount_czk = %s,
                    basis = %s,
                    certainty = %s,
                    note = %s,
                    source_url = %s,
                    lineage = flow.lineage || %s
                from core.reporting_period period,
                     core.organization source_org,
                     core.organization target_org
                where flow.reporting_period_id = period.reporting_period_id
                  and flow.source_organization_id = source_org.organization_id
                  and flow.target_organization_id = target_org.organization_id
                  and flow.budget_domain = 'school'
                  and flow.flow_type = 'founder_support'
                  and period.calendar_year = %s
                  and source_org.attributes ->> 'stable_key' = %s
                  and target_org.attributes ->> 'stable_key' = %s
                """,
                (
                    amount,
                    row["basis"],
                    row["certainty"],
                    row["note"],
                    row["source_url"],
                    lineage,
                    year,
                    founder_id,
                    institution_id,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Expected one core founder flow for {institution_id} in {year}; updated {cur.rowcount}"
                )
            core_updated += cur.rowcount
    return raw_updated, core_updated


def verify_year(conn: psycopg.Connection, year: int, expected: int) -> tuple[int, int]:
    row = conn.execute(
        """
        select count(*), coalesce(sum(amount_czk), 0)
        from raw.school_founder_support
        where reporting_year = %s
          and certainty = 'observed'
          and payload ->> 'attribution_method' = 'recipient_reported_founder_budget'
        """,
        (year,),
    ).fetchone()
    count = int(row[0])
    amount = int(row[1])
    if count != expected:
        raise RuntimeError(f"Expected {expected} verified raw rows for {year}; found {count}")
    return count, amount


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    with psycopg.connect(args.database_url, autocommit=False) as conn:
        print("### Founder recipient evidence update")
        print()
        print("| Year | Raw rows | Core rows | Source-backed total |")
        print("| ---: | ---: | ---: | ---: |")
        for year in sorted(set(args.year)):
            evidence_rows = load_evidence(year)
            raw_count, core_count = apply_year(conn, year, evidence_rows)
            verified_count, amount = verify_year(conn, year, len(evidence_rows))
            if verified_count != raw_count:
                raise RuntimeError(f"Verification count mismatch for {year}")
            print(f"| {year} | {raw_count:,} | {core_count:,} | {amount:,} CZK |")

        if args.execute:
            conn.commit()
            print("\nCommitted recipient-level founder evidence.")
        else:
            conn.rollback()
            print("\nDry run complete; transaction rolled back. Pass --execute to commit.")


if __name__ == "__main__":
    main()
