#!/usr/bin/env python3
"""Build a storage-neutral founder attribution evidence queue.

The MONITOR FINM extract reports each municipality or region's education
contributions, but it has no recipient-school dimension. This audit keeps that
aggregate evidence separate from the inferred school allocations and identifies
the founders that need a recipient-level budget schedule.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import fetch_founder_budgets as fb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "etl" / "data" / "quality" / "founder_attribution"
DEFAULT_SOURCE_CATALOG = ROOT / "etl" / "data" / "founder_source_catalog.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit founder attribution against the nationwide MONITOR FINM extract"
    )
    parser.add_argument(
        "--year",
        action="append",
        type=int,
        required=True,
        help="Reporting year to audit. Can be supplied more than once.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for compact CSV and JSON audit artifacts",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Download the MONITOR archive even when a cached copy exists",
    )
    parser.add_argument(
        "--source-catalog",
        type=Path,
        default=DEFAULT_SOURCE_CATALOG,
        help="Catalog of recipient-level founder documents already discovered",
    )
    return parser.parse_args()


def build_founder_index(
    school_entities: dict[str, dict[str, str]],
) -> dict[str, dict[str, Any]]:
    founders: dict[str, dict[str, Any]] = {}
    for entity in school_entities.values():
        founder_id = entity.get("founder_id", "").strip()
        founder_ico = founder_id.removeprefix("founder:").strip()
        institution_id = entity.get("institution_id", "").strip()
        if not founder_ico or not institution_id:
            continue
        founder = founders.setdefault(
            founder_ico,
            {
                "founder_id": founder_id,
                "founder_ico": founder_ico,
                "founder_name": entity.get("founder_name", "").strip(),
                "founder_type": entity.get("founder_type", "").strip(),
                "institution_ids": set(),
            },
        )
        founder["institution_ids"].add(institution_id)
    return founders


def find_missing_founders(
    school_entities: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for entity in school_entities.values():
        founder_ico = entity.get("founder_id", "").removeprefix("founder:").strip()
        if founder_ico:
            continue
        rows.append(
            {
                "institution_id": entity.get("institution_id", "").strip(),
                "ico": entity.get("ico", "").strip(),
                "institution_name": entity.get("institution_name", "").strip(),
                "municipality": entity.get("municipality", "").strip(),
                "region": entity.get("region", "").strip(),
                "evidence_status": "founder_registry_link_required",
            }
        )
    return sorted(rows, key=lambda row: (row["region"], row["municipality"], row["institution_id"]))


def load_current_support(year: int) -> dict[str, int]:
    path = fb.RAW_ROOT / str(year) / "founder_support.csv"
    totals: dict[str, int] = defaultdict(int)
    if not path.exists():
        return totals
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            institution_id = (row.get("institution_id") or "").strip()
            if institution_id:
                totals[institution_id] += fb.to_int(row.get("amount"))
    return totals


def load_source_catalog(path: Path) -> dict[tuple[int, str], dict[str, str]]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return {
            (int(row["reporting_year"]), row["founder_id"].strip()): row
            for row in csv.DictReader(handle)
            if row.get("reporting_year") and row.get("founder_id")
        }


def build_audit_rows(
    *,
    year: int,
    founders: dict[str, dict[str, Any]],
    finm_details: dict[str, dict[str, Any]],
    support_by_institution: dict[str, int],
    source_catalog: dict[tuple[int, str], dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    source_catalog = source_catalog or {}
    ranked_multi = sorted(
        (
            (founder_ico, founder)
            for founder_ico, founder in founders.items()
            if len(founder["institution_ids"]) > 1
        ),
        key=lambda pair: (
            -len(pair[1]["institution_ids"]),
            -int(finm_details.get(pair[0], {}).get("actual_amount_czk", 0)),
            pair[0],
        ),
    )
    priority_rank = {founder_ico: rank for rank, (founder_ico, _) in enumerate(ranked_multi, 1)}

    rows: list[dict[str, Any]] = []
    for founder_ico, founder in founders.items():
        institution_ids = founder["institution_ids"]
        school_count = len(institution_ids)
        detail = finm_details.get(founder_ico, {})
        finm_total = int(detail.get("actual_amount_czk", 0))
        recipient_source = source_catalog.get((year, founder["founder_id"]), {})

        if school_count > 20:
            priority_tier = "large_multi_school"
        elif school_count > 1:
            priority_tier = "multi_school"
        elif not detail:
            priority_tier = "missing_finm_total"
        else:
            priority_tier = "single_school"

        if recipient_source:
            evidence_status = "recipient_source_cataloged"
        elif not detail:
            evidence_status = "founder_source_required"
        elif school_count > 1:
            evidence_status = "recipient_schedule_required"
        else:
            evidence_status = "aggregate_candidate_needs_confirmation"

        rows.append(
            {
                "reporting_year": year,
                "priority_rank": priority_rank.get(founder_ico, ""),
                "priority_tier": priority_tier,
                "founder_id": founder["founder_id"],
                "founder_ico": founder_ico,
                "founder_name": founder["founder_name"],
                "founder_type": founder["founder_type"],
                "school_count": school_count,
                "finm_line_count": int(detail.get("line_count", 0)),
                "finm_operating_czk": int(detail.get("operating_amount_czk", 0)),
                "finm_investment_czk": int(detail.get("investment_amount_czk", 0)),
                "finm_total_czk": finm_total,
                "current_support_school_count": sum(
                    institution_id in support_by_institution
                    for institution_id in institution_ids
                ),
                "current_attributed_czk": sum(
                    support_by_institution.get(institution_id, 0)
                    for institution_id in institution_ids
                ),
                "evidence_status": evidence_status,
                "recipient_source_kind": recipient_source.get("document_kind", ""),
                "recipient_source_basis": recipient_source.get("basis", ""),
                "recipient_source_parser_status": recipient_source.get("parser_status", ""),
                "recipient_source_url": recipient_source.get("source_url", ""),
                "monitor_url": (
                    "https://monitor.statnipokladna.gov.cz/api/ukazatele"
                    f"?ic={founder_ico}&obdobi={str(year)[-2:]}12"
                ),
            }
        )

    return sorted(
        rows,
        key=lambda row: (
            0 if row["priority_rank"] != "" else 1,
            int(row["priority_rank"] or 0),
            str(row["founder_ico"]),
        ),
    )


def summarize(
    year: int,
    rows: list[dict[str, Any]],
    source_url: str,
    registry_school_count: int | None = None,
) -> dict[str, Any]:
    multi_rows = [row for row in rows if int(row["school_count"]) > 1]
    cataloged_rows = [row for row in rows if row["evidence_status"] == "recipient_source_cataloged"]
    linked_school_count = sum(int(row["school_count"]) for row in rows)
    registry_school_count = registry_school_count or linked_school_count
    support_school_count = sum(int(row["current_support_school_count"]) for row in rows)
    return {
        "reporting_year": year,
        "founder_count": len(rows),
        "school_count": registry_school_count,
        "linked_school_count": linked_school_count,
        "missing_founder_school_count": registry_school_count - linked_school_count,
        "current_support_school_count": support_school_count,
        "missing_current_support_school_count": linked_school_count - support_school_count,
        "single_school_founders": sum(int(row["school_count"]) == 1 for row in rows),
        "multi_school_founders": len(multi_rows),
        "large_multi_school_founders": sum(int(row["school_count"]) > 20 for row in rows),
        "founders_with_recipient_source_cataloged": len(cataloged_rows),
        "multi_school_founders_missing_recipient_source": sum(
            row["evidence_status"] != "recipient_source_cataloged" for row in multi_rows
        ),
        "schools_under_multi_school_founders": sum(int(row["school_count"]) for row in multi_rows),
        "founders_with_finm_total": sum(int(row["finm_line_count"]) > 0 for row in rows),
        "relevant_finm_lines": sum(int(row["finm_line_count"]) for row in rows),
        "finm_total_czk": sum(int(row["finm_total_czk"]) for row in rows),
        "current_attributed_czk": sum(int(row["current_attributed_czk"]) for row in rows),
        "source_url": source_url,
        "source_limitation": "FINM identifies the reporting founder but not the recipient school.",
    }


def write_outputs(
    out_dir: Path,
    year: int,
    rows: list[dict[str, Any]],
    missing_founders: list[dict[str, str]],
    summary: dict[str, Any],
) -> tuple[Path, Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{year}.csv"
    json_path = out_dir / f"{year}.summary.json"
    missing_path = out_dir / f"{year}.missing-founders.csv"
    fieldnames = list(rows[0]) if rows else []
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    missing_fields = [
        "institution_id",
        "ico",
        "institution_name",
        "municipality",
        "region",
        "evidence_status",
    ]
    with missing_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=missing_fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(missing_founders)
    return csv_path, json_path, missing_path


def print_summary(summary: dict[str, Any]) -> None:
    print(f"\n### Founder attribution audit {summary['reporting_year']}")
    print()
    print("| Metric | Count |")
    print("| --- | ---: |")
    print(f"| Founders | {summary['founder_count']:,} |")
    print(f"| Schools | {summary['school_count']:,} |")
    print(f"| Schools missing founder IČO | {summary['missing_founder_school_count']:,} |")
    print(f"| Linked schools missing current support | {summary['missing_current_support_school_count']:,} |")
    print(f"| Single-school candidates | {summary['single_school_founders']:,} |")
    print(f"| Multi-school founders needing recipient schedules | {summary['multi_school_founders']:,} |")
    print(f"| Large founders with more than 20 schools | {summary['large_multi_school_founders']:,} |")
    print(f"| Recipient-level sources cataloged | {summary['founders_with_recipient_source_cataloged']:,} |")
    print(f"| Multi-school sources still missing | {summary['multi_school_founders_missing_recipient_source']:,} |")
    print(f"| Relevant FINM lines retained logically | {summary['relevant_finm_lines']:,} |")
    print()
    print("FINM aggregate totals are evidence for founder spending, not direct school attribution.")


def audit_year(
    year: int,
    out_dir: Path,
    no_cache: bool,
    source_catalog: dict[tuple[int, str], dict[str, str]],
) -> dict[str, Any]:
    period = fb.DEFAULT_PERIOD.get(year) or f"{year}_12"
    source_url = fb.FIN12M_URL_TEMPLATES[0].format(
        year=period[:4],
        month=period[-2:],
        period=period,
    )
    archive = fb.resolve_zip(
        "FIN 2-12 M",
        fb.FIN12M_URL_TEMPLATES,
        period,
        year,
        f"fin2-12m-{period}.zip",
        None,
        no_cache,
    )
    if archive is None:
        raise RuntimeError(f"Unable to obtain MONITOR FINM archive for {year}")

    school_entities = fb.load_school_entities(year)
    founders = build_founder_index(school_entities)
    missing_founders = find_missing_founders(school_entities)
    details = fb.run_12m_detail_pass(archive, set(founders), False)
    rows = build_audit_rows(
        year=year,
        founders=founders,
        finm_details=details,
        support_by_institution=load_current_support(year),
        source_catalog=source_catalog,
    )
    summary = summarize(year, rows, source_url, len(school_entities))
    csv_path, json_path, missing_path = write_outputs(
        out_dir,
        year,
        rows,
        missing_founders,
        summary,
    )
    print_summary(summary)
    print(f"Audit CSV: `{csv_path.relative_to(ROOT)}`")
    print(f"Summary JSON: `{json_path.relative_to(ROOT)}`")
    print(f"Missing-founder CSV: `{missing_path.relative_to(ROOT)}`")
    return summary


def main() -> None:
    args = parse_args()
    source_catalog = load_source_catalog(args.source_catalog)
    for year in sorted(set(args.year)):
        audit_year(year, args.out_dir, args.no_cache, source_catalog)


if __name__ == "__main__":
    main()
