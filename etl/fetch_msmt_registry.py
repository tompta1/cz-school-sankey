#!/usr/bin/env python3
"""Enrich school and founder names from the MŠMT school registry (RSSZ).

The MŠMT publishes the Rejstřík škol a školských zařízení (RSSZ) as a
JSON-LD file via their open data catalog (lkod.msmt.gov.cz).  It is the
authoritative source for school names, founder names, municipalities, and
regions — more reliable than ARES for school-specific metadata.

What this script does
---------------------
1. Downloads the RSSZ JSON-LD for the target year (cached).
2. Builds a lookup: IČO → {name, municipality, region}.
3. Merges into ares_names.json:
     - Registry entries overwrite ARES entries for school legal entities.
     - Founder names from zrizovatele[] fill in or overwrite entries
       for founder IČOs that currently have placeholder names.
4. Optionally (--update-csv) rewrites school_entities.csv with
   proper names and municipality from the registry.

Usage:
    python3 etl/fetch_msmt_registry.py --year 2025
    python3 etl/fetch_msmt_registry.py --year 2025 --update-csv
    python3 etl/fetch_msmt_registry.py --year 2025 --no-cache
    python3 etl/fetch_msmt_registry.py --year 2025 \\
        --jsonld path/to/rssz-cela-cr-2025-12-31.jsonld
"""

from __future__ import annotations

import argparse
import csv
import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw"
CACHE_DIR = ROOT / "etl" / "data" / "msmt_registry_cache"
ARES_NAMES_PATH = ROOT / "etl" / "data" / "ares_names.json"

# MŠMT LKOD FTP base for RSSZ JSON-LD files.
# Dataset e9c07729 = "Rejstřík škol a školských zařízení pro rok 2025 - celá ČR"
# Dataset 1e789608 = "Rejstřík škol a školských zařízení - celá ČR (31.12.2024)"
REGISTRY_URLS: dict[int, str] = {
    2025: "https://lkod-ftp.msmt.gov.cz/00022985/e9c07729-877e-4af0-be4a-9d36e45806ae/rssz-cela-cr-2025-12-31.jsonld",
    2024: "https://lkod-ftp.msmt.gov.cz/00022985/1e789608-0836-48be-b6df-658dc43d43fa/RSSZ-cela-CR-2024-12-31.jsonld",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich ares_names.json from MŠMT school registry (RSSZ)"
    )
    parser.add_argument("--year", type=int, required=True, help="Registry year")
    parser.add_argument(
        "--jsonld",
        type=Path,
        help="Local JSON-LD file (skips download)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Re-download even if cached",
    )
    parser.add_argument(
        "--update-csv",
        action="store_true",
        help="Also rewrite school_entities.csv with registry names and municipalities",
    )
    return parser.parse_args()


def resolve_jsonld(args: argparse.Namespace) -> Path:
    if args.jsonld:
        if not args.jsonld.exists():
            raise SystemExit(f"Local file not found: {args.jsonld}")
        return args.jsonld

    url = REGISTRY_URLS.get(args.year)
    if not url:
        raise SystemExit(
            f"No registry URL configured for year {args.year}.\n"
            f"Available years: {sorted(REGISTRY_URLS)}\n"
            f"Pass --jsonld path/to/file to use a local file."
        )

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1]
    cached = CACHE_DIR / filename

    if cached.exists() and not args.no_cache:
        print(f"Using cached {cached.name}")
        return cached

    print(f"Downloading RSSZ registry for {args.year}…")
    print(f"  {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "cz-school-sankey/1.0"})
    with urllib.request.urlopen(req, timeout=180) as resp:
        data = resp.read()
    cached.write_bytes(data)
    print(f"  Downloaded {len(data) // 1024} KB → {cached.name}")
    return cached


def load_registry(path: Path) -> list[dict]:
    print(f"Parsing {path.name}…")
    obj = json.loads(path.read_text(encoding="utf-8"))
    # 2025 format: {"list": [...]}
    # 2024 format: {"http://msmt.cz/sub/data": {"list": [...]}}
    if "list" in obj:
        entities = obj["list"]
    else:
        nested = obj.get("http://msmt.cz/sub/data", {})
        entities = nested.get("list", [])
    print(f"  {len(entities)} school legal entities")
    return entities


def normalize_ico(raw: object) -> str:
    text = str(raw or "").strip().split(".")[0]
    digits = "".join(c for c in text if c.isdigit())
    return digits.zfill(8) if digits else ""


def build_lookups(
    entities: list[dict],
) -> tuple[dict[str, dict], dict[str, dict]]:
    """Return (school_lookup, founder_lookup).

    school_lookup:  ico → {name, municipality, region}
    founder_lookup: ico → {name, municipality}
    """
    school_lookup: dict[str, dict] = {}
    founder_lookup: dict[str, dict] = {}

    for entity in entities:
        ico = normalize_ico(entity.get("ico", ""))
        if not ico:
            continue

        name = (entity.get("uplnyNazev") or entity.get("zkracenyNazev") or "").strip()
        region = (entity.get("kraj") or "").strip()
        adresa = entity.get("adresa") or {}
        municipality = (adresa.get("obec") or "").strip()

        school_lookup[ico] = {
            "name": name,
            "municipality": municipality,
            "region": region,
        }

        for founder in entity.get("zrizovatele") or []:
            f_ico = normalize_ico(founder.get("ico", ""))
            f_name = (founder.get("nazevOsoby") or "").strip()
            f_adresa = founder.get("adresa") or ""
            # Founder address is sometimes a plain string "Street, City"
            f_municipality = ""
            if isinstance(f_adresa, dict):
                f_municipality = (f_adresa.get("obec") or "").strip()

            if f_ico and f_name:
                # Don't overwrite a founder entry that already has a better name
                existing = founder_lookup.get(f_ico, {})
                if not existing.get("name"):
                    founder_lookup[f_ico] = {
                        "name": f_name,
                        "municipality": f_municipality,
                    }

    return school_lookup, founder_lookup


def merge_into_ares(
    school_lookup: dict[str, dict],
    founder_lookup: dict[str, dict],
) -> tuple[int, int, int]:
    """Merge registry data into ares_names.json.

    Returns (schools_updated, founders_updated, total_entries).
    """
    ares: dict[str, dict] = {}
    if ARES_NAMES_PATH.exists():
        ares = json.loads(ARES_NAMES_PATH.read_text(encoding="utf-8"))

    schools_updated = founders_updated = 0

    for ico, entry in school_lookup.items():
        if not entry.get("name"):
            continue
        existing = ares.get(ico, {})
        # Registry always wins for school entities — it's the authoritative source
        new_entry = {
            "name": entry["name"],
            "municipality": entry["municipality"] or existing.get("municipality", ""),
        }
        if ares.get(ico) != new_entry:
            ares[ico] = new_entry
            schools_updated += 1

    for ico, entry in founder_lookup.items():
        if not entry.get("name"):
            continue
        existing = ares.get(ico, {})
        existing_name = existing.get("name", "")
        # Only update founders with blank or placeholder names
        is_placeholder = (
            not existing_name
            or existing_name.startswith("IČO")
            or existing_name.startswith("Zřizovatel")
        )
        if is_placeholder:
            ares[ico] = {
                "name": entry["name"],
                "municipality": entry["municipality"] or existing.get("municipality", ""),
            }
            founders_updated += 1

    ARES_NAMES_PATH.write_text(
        json.dumps(ares, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return schools_updated, founders_updated, len(ares)


def update_school_entities_csv(
    year: int,
    school_lookup: dict[str, dict],
    founder_lookup: dict[str, dict],
) -> tuple[int, int]:
    """Rewrite school_entities.csv with registry names and municipalities.

    Returns (schools_renamed, founders_renamed).
    """
    path = RAW_ROOT / str(year) / "school_entities.csv"
    if not path.exists():
        print(f"  {path} not found — skipping CSV update")
        return 0, 0

    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    fieldnames = list(rows[0].keys()) if rows else []

    schools_renamed = founders_renamed = 0

    for row in rows:
        ico = normalize_ico(row.get("ico", ""))
        reg = school_lookup.get(ico, {})

        # Update school name
        if reg.get("name"):
            if row.get("institution_name") != reg["name"]:
                row["institution_name"] = reg["name"]
                schools_renamed += 1

        # Update municipality (currently always empty)
        if reg.get("municipality") and not row.get("municipality"):
            row["municipality"] = reg["municipality"]

        # Update region if missing
        if reg.get("region") and not row.get("region"):
            row["region"] = reg["region"]

        # Update founder name from founder_lookup if it's a placeholder
        founder_id = row.get("founder_id", "")
        founder_ico = founder_id.removeprefix("founder:").strip()
        if founder_ico:
            f_entry = founder_lookup.get(founder_ico.zfill(8), {})
            f_name = f_entry.get("name", "")
            current_fname = row.get("founder_name", "")
            if f_name and (
                not current_fname
                or current_fname.startswith("Zřizovatel")
                or current_fname.startswith("IČO")
            ):
                row["founder_name"] = f_name
                founders_renamed += 1

    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return schools_renamed, founders_renamed


def main() -> None:
    args = parse_args()

    jsonld_path = resolve_jsonld(args)
    entities = load_registry(jsonld_path)
    school_lookup, founder_lookup = build_lookups(entities)

    print(
        f"Registry lookups built:\n"
        f"  {len(school_lookup)} school entities with names\n"
        f"  {len(founder_lookup)} unique founders with names"
    )

    schools_updated, founders_updated, total = merge_into_ares(school_lookup, founder_lookup)
    print(
        f"\nMerged into ares_names.json:\n"
        f"  {schools_updated} school entries updated\n"
        f"  {founders_updated} founder entries updated (placeholder → real name)\n"
        f"  {total} total entries in ares_names.json"
    )

    if args.update_csv:
        s, f = update_school_entities_csv(args.year, school_lookup, founder_lookup)
        print(
            f"\nUpdated school_entities.csv:\n"
            f"  {s} school names updated\n"
            f"  {f} founder names updated"
        )

    print(
        f"\nNext step: rebuild the Sankey JSON\n"
        f"  python3 etl/build_school_year.py --year {args.year}"
    )


if __name__ == "__main__":
    main()
