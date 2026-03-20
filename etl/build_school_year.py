#!/usr/bin/env python3
"""Build one yearly Sankey JSON export for the static UI.

The script is intentionally conservative:
- it prefers local CSV files dropped into etl/data/raw/<year>/
- it writes a versioned JSON payload to public/data/sankey/<year>.json
- it marks flows as observed or inferred explicitly

Input contracts for CSV mode are documented in etl/README.md.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw"
PUBLIC_ROOT = ROOT / "public" / "data"
MANIFEST_PATH = PUBLIC_ROOT / "manifest.json"
ARES_NAMES_PATH = ROOT / "etl" / "data" / "ares_names.json"


def load_ares_names() -> dict[str, dict]:
    if ARES_NAMES_PATH.exists():
        return json.loads(ARES_NAMES_PATH.read_text(encoding="utf-8"))
    return {}


def ares_name(ares: dict[str, dict], ico: str | None, fallback: str) -> str:
    if not ico:
        return fallback
    entry = ares.get(ico.zfill(8), {})
    return entry.get("name") or fallback


@dataclass(slots=True)
class Institution:
    institution_id: str
    name: str
    ico: str | None
    founder_id: str | None
    founder_name: str | None
    founder_type: str | None
    municipality: str | None
    region: str | None
    capacity: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build one school-year Sankey dataset")
    parser.add_argument("--year", type=int, required=True, help="Budget year to build")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Ignore local raw files and emit the built-in pilot dataset",
    )
    return parser.parse_args()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def read_institutions(year: int) -> list[Institution]:
    path = RAW_ROOT / str(year) / "school_entities.csv"
    if not path.exists():
        raise FileNotFoundError(path)

    institutions: list[Institution] = []
    for row in read_csv_rows(path):
        raw_cap = row.get("capacity") or ""
        capacity = int(raw_cap) if raw_cap.isdigit() else None
        institutions.append(
            Institution(
                institution_id=row["institution_id"],
                name=row["institution_name"],
                ico=row.get("ico") or None,
                founder_id=row.get("founder_id") or None,
                founder_name=row.get("founder_name") or None,
                founder_type=row.get("founder_type") or None,
                municipality=row.get("municipality") or None,
                region=row.get("region") or None,
                capacity=capacity,
            )
        )
    return institutions


def row_amount(row: dict[str, str], key: str) -> int:
    raw = (row.get(key) or "0").replace(" ", "").replace(",", ".")
    return int(round(float(raw)))


def node(node_id: str, name: str, category: str, level: int, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": node_id,
        "name": name,
        "category": category,
        "level": level,
    }
    payload.update({key: value for key, value in extra.items() if value is not None})
    return payload


def link(
    source: str,
    target: str,
    amount: int,
    year: int,
    flow_type: str,
    basis: str,
    certainty: str,
    dataset: str,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": source,
        "target": target,
        "value": amount,
        "amountCzk": amount,
        "year": year,
        "flowType": flow_type,
        "basis": basis,
        "certainty": certainty,
        "sourceDataset": dataset,
    }
    payload.update({key: value for key, value in extra.items() if value is not None})
    return payload


def build_demo_dataset(year: int) -> dict[str, Any]:
    sample_path = PUBLIC_ROOT / "sankey" / f"{year}.json"
    if sample_path.exists():
        return json.loads(sample_path.read_text(encoding="utf-8"))
    raise FileNotFoundError(sample_path)


def build_from_csv(year: int) -> dict[str, Any]:
    ares = load_ares_names()
    institutions = read_institutions(year)
    year_dir = RAW_ROOT / str(year)

    msmt_rows = read_csv_rows(year_dir / "msmt_allocations.csv")
    eu_rows = read_csv_rows(year_dir / "eu_projects.csv") if (year_dir / "eu_projects.csv").exists() else []
    founder_rows = (
        read_csv_rows(year_dir / "founder_support.csv") if (year_dir / "founder_support.csv").exists() else []
    )

    nodes: list[dict[str, Any]] = [
        node("state:cr", "State budget", "state", 0),
        node("msmt", "MŠMT direct school finance", "ministry", 1),
    ]
    links: list[dict[str, Any]] = []

    seen_nodes = {"state:cr", "msmt"}
    total_msmt = 0

    def ensure_node(payload: dict[str, Any]) -> None:
        node_id = payload["id"]
        if node_id not in seen_nodes:
            nodes.append(payload)
            seen_nodes.add(node_id)

    school_nodes_by_id: dict[str, Institution] = {institution.institution_id: institution for institution in institutions}
    school_nodes_by_ico: dict[str, Institution] = {
        institution.ico: institution for institution in institutions if institution.ico
    }

    cost_bucket_nodes = [
        node("bucket:pedagogues", "Pedagogical staff", "cost_bucket", 3),
        node("bucket:nonpedagogues", "Non-pedagogical staff", "cost_bucket", 3),
        node("bucket:oniv", "ONIV and materials", "cost_bucket", 3),
        node("bucket:other", "Other direct MŠMT", "cost_bucket", 3),
        node("bucket:operations", "Operations and energy", "cost_bucket", 3),
        node("bucket:investment", "Investment and equipment", "cost_bucket", 3),
    ]
    for bucket_node in cost_bucket_nodes:
        ensure_node(bucket_node)

    for institution in institutions:
        school_name = ares_name(ares, institution.ico, institution.name)
        ensure_node(
            node(
                institution.institution_id,
                school_name,
                "school_entity",
                2,
                ico=institution.ico,
                founderType=institution.founder_type,
                metadata={"capacity": institution.capacity} if institution.capacity else None,
            )
        )
        if institution.founder_id and institution.founder_name:
            founder_ico = institution.founder_id.removeprefix("founder:")
            founder_name = ares_name(ares, founder_ico, institution.founder_name)
            # Keep founderName consistent with the node name for graph lookups
            institution.founder_name = founder_name
            institution.name = school_name
            category = "municipality" if institution.founder_type == "obec" else "region"
            ensure_node(node(institution.founder_id, founder_name, category, 1))

    for row in msmt_rows:
        institution = None
        institution_id = row.get("institution_id") or None
        ico = row.get("ico") or None
        if institution_id:
            institution = school_nodes_by_id.get(institution_id)
        elif ico:
            institution = school_nodes_by_ico.get(ico)
        if institution is None:
            continue

        allocation_total = row_amount(row, "pedagogical_amount") + row_amount(row, "nonpedagogical_amount") + row_amount(
            row, "oniv_amount"
        ) + row_amount(row, "other_amount")
        total_msmt += allocation_total
        links.append(
            link(
                "msmt",
                institution.institution_id,
                allocation_total,
                year,
                "direct_school_finance",
                "allocated",
                "observed",
                "local.msmt_allocations",
                institutionId=institution.institution_id,
            )
        )

        for bucket_name, bucket_node_id in (
            ("pedagogical_amount", "bucket:pedagogues"),
            ("nonpedagogical_amount", "bucket:nonpedagogues"),
            ("oniv_amount", "bucket:oniv"),
            ("other_amount", "bucket:other"),
            ("operations_amount", "bucket:operations"),
            ("investment_amount", "bucket:investment"),
        ):
            amount = row_amount(row, bucket_name)
            if amount <= 0:
                continue
            links.append(
                link(
                    institution.institution_id,
                    bucket_node_id,
                    amount,
                    year,
                    "school_expenditure",
                    row.get("bucket_basis") or "budgeted",
                    row.get("bucket_certainty") or ("observed" if bucket_name.endswith("amount") else "inferred"),
                    "local.msmt_allocations",
                    institutionId=institution.institution_id,
                )
            )

    links.insert(
        0,
        link(
            "state:cr",
            "msmt",
            total_msmt,
            year,
            "state_to_ministry",
            "allocated",
            "observed",
            "derived.msmt_rollup",
            note="Roll-up from school-level MŠMT allocations for this export",
        ),
    )

    for row in eu_rows:
        institution = None
        institution_id = row.get("institution_id") or None
        ico = row.get("ico") or None
        if institution_id:
            institution = school_nodes_by_id.get(institution_id)
        elif ico:
            institution = school_nodes_by_ico.get(ico)
        if institution is None:
            continue

        programme_node_id = f"eu:{slugify(row.get('programme', 'programme'))}"
        project_node_id = f"project:{slugify(row.get('project_name', 'project'))}"
        ensure_node(node(programme_node_id, row.get("programme", "EU programme"), "eu_programme", 1))
        ensure_node(node(project_node_id, row.get("project_name", "EU project"), "eu_project", 2))
        amount = row_amount(row, "amount")
        links.append(
            link(
                programme_node_id,
                project_node_id,
                amount,
                year,
                "eu_project_support",
                row.get("basis") or "allocated",
                row.get("certainty") or "observed",
                "local.eu_projects",
            )
        )
        links.append(
            link(
                project_node_id,
                institution.institution_id,
                amount,
                year,
                "project_to_school",
                row.get("basis") or "allocated",
                row.get("certainty") or "observed",
                "local.eu_projects",
                institutionId=institution.institution_id,
            )
        )

    for row in founder_rows:
        institution = None
        institution_id = row.get("institution_id") or None
        ico = row.get("ico") or None
        if institution_id:
            institution = school_nodes_by_id.get(institution_id)
        elif ico:
            institution = school_nodes_by_ico.get(ico)
        if institution is None or not institution.founder_id:
            continue

        amount = row_amount(row, "amount")
        links.append(
            link(
                institution.founder_id,
                institution.institution_id,
                amount,
                year,
                "founder_support",
                row.get("basis") or "budgeted",
                row.get("certainty") or "inferred",
                "local.founder_support",
                institutionId=institution.institution_id,
                note=row.get("note") or None,
            )
        )

    dataset = {
        "year": year,
        "currency": "CZK",
        "title": f"Czech school budget Sankey — {year}",
        "subtitle": "Built from local raw files",
        "nodes": nodes,
        "links": links,
        "institutions": [
            {
                "id": institution.institution_id,
                "name": institution.name,
                "ico": institution.ico,
                "founderName": institution.founder_name,
                "founderType": institution.founder_type,
                "municipality": institution.municipality,
                "region": institution.region,
                **({"capacity": institution.capacity} if institution.capacity else {}),
            }
            for institution in institutions
        ],
        "sources": [
            {
                "id": "local-msmt",
                "label": "Local MŠMT allocations import",
                "coverage": "Per-school direct finance and expenditure buckets",
                "confidence": "high",
            },
            {
                "id": "local-eu",
                "label": "Local EU projects import",
                "coverage": "Recipient-level programme and project support",
                "confidence": "high",
            },
            {
                "id": "local-founder",
                "label": "Local founder support import",
                "coverage": "Municipal or regional support; often inferred",
                "confidence": "medium",
            },
        ],
    }
    return dataset


def slugify(value: str) -> str:
    return "-".join(
        "".join(character.lower() if character.isalnum() else " " for character in value).split()
    )


def write_dataset(year: int, dataset: dict[str, Any]) -> Path:
    out_path = PUBLIC_ROOT / "sankey" / f"{year}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def update_manifest(year: int) -> None:
    manifest: dict[str, Any]
    if MANIFEST_PATH.exists():
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    else:
        manifest = {"dataset": "cz-school-budget-sankey", "years": []}

    years = [entry for entry in manifest.get("years", []) if entry.get("year") != year]
    years.append(
        {
            "year": year,
            "title": f"School finance view {year}",
            "file": f"./data/sankey/{year}.json",
            "status": "pilot",
        }
    )
    years.sort(key=lambda item: item["year"])
    manifest["years"] = years
    MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    dataset = build_demo_dataset(args.year) if args.demo else build_from_csv(args.year)
    out_path = write_dataset(args.year, dataset)
    update_manifest(args.year)
    print(f"Wrote {out_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
