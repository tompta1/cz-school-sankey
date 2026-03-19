#!/usr/bin/env python3
"""Fetch EU grants from DotaceEU "Seznam operací" and produce eu_projects.csv.

Downloads the DotaceEU monthly XLSX snapshot for the 2021-2027 programming
period, filters rows whose recipient IČO matches a known school entity, and
writes etl/data/raw/<year>/eu_projects.csv in the format expected by
build_school_year.py.

Usage:
    python3 etl/fetch_eu_grants.py --year 2025
    python3 etl/fetch_eu_grants.py --year 2025 --snapshot 2025_12
    python3 etl/fetch_eu_grants.py --year 2025 --xlsx path/to/local.xlsx
"""

from __future__ import annotations

import argparse
import csv
import sys
import urllib.request
from pathlib import Path

try:
    import openpyxl
except ImportError:
    sys.exit("openpyxl is required: pip install openpyxl")

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw"
CACHE_DIR = ROOT / "etl" / "data" / "dotaceeu_cache"

# Monthly XLSX snapshots from DotaceEU (2021-2027 programming period).
# Keys are YYYY_MM strings; values are the full download URLs.
SNAPSHOT_URLS: dict[str, str] = {
    "2026_03": "https://www.dotaceeu.cz/getmedia/33db9a79-dd32-45f2-a063-c991201dea9a/2026_03_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2026_02": "https://www.dotaceeu.cz/getmedia/640136e2-d286-4c96-995a-468cb2e6a5b1/2026_02_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2026_01": "https://www.dotaceeu.cz/getmedia/e24a68d2-e2f4-4a6f-8f68-5ce80bb37220/2026_01_Seznam-operaci_List-od-Operations_21.xlsx.aspx?ext=.xlsx",
    # Note: the DotaceEU CMS serves the same file (Dec 2024 data) for both
    # 2024_12 and 2025_12 keys (same UUID). Use 2026_03 for 2025 Sankeys.
    "2025_12": "https://www.dotaceeu.cz/getmedia/56559aea-99b9-40a9-a8f3-63fa4a18922e/2025_12_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_11": "https://www.dotaceeu.cz/getmedia/ece408d5-c446-4688-8e07-0a840d1d800d/2025_11_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_10": "https://www.dotaceeu.cz/getmedia/ccda83c3-b4ef-4212-98a0-9c90ebd72117/2025_10_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_09": "https://www.dotaceeu.cz/getmedia/4c9489cf-7e75-4992-a8d1-75bf974552c9/2025_09_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_08": "https://www.dotaceeu.cz/getmedia/907e2206-6dc4-440f-b071-8001cb7b4997/2025_08_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_07": "https://www.dotaceeu.cz/getmedia/cc9c92fe-9893-47ed-a714-ee98e99ee375/2025_07_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_06": "https://www.dotaceeu.cz/getmedia/f25b531a-460e-48ea-b467-2f8dbfe92f0c/2025_06_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_05": "https://www.dotaceeu.cz/getmedia/b9058130-58e2-45b0-a312-a5cd8c2915fb/2025_05_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_04": "https://www.dotaceeu.cz/getmedia/a8fdeadf-9cc9-47bf-9cec-324c1b3d38c6/2025_04_Seznam-operaci_List-of-Operations_1.xlsx.aspx?ext=.xlsx",
    "2025_03": "https://www.dotaceeu.cz/getmedia/10cbf319-d20c-45d1-a7b3-12dc5b0d1133/2025_03_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_02": "https://www.dotaceeu.cz/getmedia/7628e470-59bd-444b-bae0-4409e8d897aa/2025_02_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2025_01": "https://www.dotaceeu.cz/getmedia/99b742c3-124a-42a6-a06e-08d2c3b6e245/2025_01_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_12": "https://www.dotaceeu.cz/getmedia/56559aea-99b9-40a9-a8f3-63fa4a18922e/2024_12_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_11": "https://www.dotaceeu.cz/getmedia/1b311b30-abad-4e4f-8c76-a504c360d7d4/2024_11_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_10": "https://www.dotaceeu.cz/getmedia/b24ff7c6-2f95-42a6-9921-5ba8feb4a4ba/2024_10_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_09": "https://www.dotaceeu.cz/getmedia/808b0c52-38f0-4a9e-b3f4-b8bd4cac7757/2024_09_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_08": "https://www.dotaceeu.cz/getmedia/23d032d3-55b4-4150-9349-55a015eb4ae0/2024_08_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_07": "https://www.dotaceeu.cz/getmedia/6cf07dab-d696-41cf-8ea6-f3372b7a29a2/2024_07_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_06": "https://www.dotaceeu.cz/getmedia/0d5b07c9-9eb7-45e5-9cd0-e2c2565485dd/2024_06_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_05": "https://www.dotaceeu.cz/getmedia/276cbb3f-feff-42a3-b5f9-942fb60e2aae/2024_05_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_04": "https://www.dotaceeu.cz/getmedia/5b8f78af-d676-433b-8e46-43b56e322d86/2024_04_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_03": "https://www.dotaceeu.cz/getmedia/72708040-55be-4a25-b094-e3a3bfc0c08e/2024_03_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_02": "https://www.dotaceeu.cz/getmedia/eedfa158-3748-4b00-89f3-881b372b9f90/2024_02_Seznam-operaci_List-of-Operations_21.xlsx.aspx?ext=.xlsx",
    "2024_01": "https://www.dotaceeu.cz/getmedia/75edaad5-339b-4b68-bded-22a0e2ab02a5/2024_01_Seznam-operaci_List-of-Operations_2.xlsx.aspx?ext=.xlsx",
}

# Default snapshot to use per budget year (end-of-year snapshot captures full
# year allocations while still being within the target year).
DEFAULT_SNAPSHOT: dict[int, str] = {
    2024: "2024_12",  # Dec 2024 data (actual generation date: 01.12.2024)
    2025: "2026_03",  # Use Mar 2026 snapshot — the "2025_12" key serves Dec 2024 data due to a DotaceEU CMS issue
    2026: "2026_03",
}

# ---------------------------------------------------------------------------
# Column name candidates (ordered by preference).
# The XLSX header row may use slightly different labels across snapshots.
# ---------------------------------------------------------------------------

ICO_CANDIDATES = [
    "IČ příjemce",
    "IČO příjemce",
    "IČ",
    "ICO",
    "IČO",
    "ic prijemce",
    "ico prijemce",
]

PROGRAMME_CANDIDATES = [
    "Název programu",
    "Program",
    "Zkratka programu",
    "nazev programu",
]

PROJECT_NAME_CANDIDATES = [
    "Název projektu",
    "nazev projektu",
    "Project name",
]

# Total eligible expenditure is used as the primary amount — this represents
# the full project budget attributed to the school, not just the EU share.
# Fall back to EU contribution if total is unavailable.
# Total eligible expenditure in legal acts (formal commitment) — best
# "allocated" proxy for what schools will actually receive.
AMOUNT_CANDIDATES = [
    "Finanční prostředky v právních aktech celkové způsobilé výdaje CZK",
    "Celkové náklady na operaci (CZK)",
    "Celkové způsobilé výdaje (Kč)",
    "Celkové způsobilé výdaje",
    "Celková výše podpory (Kč)",
    "Celková výše podpory",
    "Příspěvek EU (Kč)",
    "Příspěvek EU",
]

# Deduplicate on these columns — the XLSX has one row per project × procurement
# contract, so the same project may appear many times for the same school.
DEDUP_KEY_COLS = ["IČ příjemce", "Registrační číslo projektu"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch DotaceEU EU grants for schools")
    parser.add_argument("--year", type=int, required=True, help="Budget year to target")
    parser.add_argument(
        "--snapshot",
        help="DotaceEU snapshot key, e.g. 2025_12 (default: end-of-year for --year)",
    )
    parser.add_argument(
        "--xlsx",
        type=Path,
        help="Use a local XLSX file instead of downloading",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Re-download the XLSX even if cached locally",
    )
    parser.add_argument(
        "--list-columns",
        action="store_true",
        help="Print all column headers found in the XLSX and exit",
    )
    return parser.parse_args()


def load_school_icos(year: int) -> dict[str, str]:
    """Return mapping of IČO (8-digit string) → institution_id."""
    path = RAW_ROOT / str(year) / "school_entities.csv"
    if not path.exists():
        sys.exit(f"Missing {path}. Run parse_msmt_xlsx.py first.")
    mapping: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            ico = (row.get("ico") or "").strip()
            if ico:
                mapping[ico.zfill(8)] = row["institution_id"]
    print(f"Loaded {len(mapping)} school IČOs from {path.name}")
    return mapping


def resolve_xlsx(args: argparse.Namespace) -> Path:
    """Return path to the XLSX file, downloading if needed."""
    if args.xlsx:
        if not args.xlsx.exists():
            sys.exit(f"Local XLSX not found: {args.xlsx}")
        return args.xlsx

    snapshot = args.snapshot or DEFAULT_SNAPSHOT.get(args.year)
    if not snapshot:
        sys.exit(
            f"No default snapshot configured for year {args.year}. "
            f"Pass --snapshot YYYY_MM explicitly."
        )
    if snapshot not in SNAPSHOT_URLS:
        available = ", ".join(sorted(SNAPSHOT_URLS))
        sys.exit(f"Unknown snapshot '{snapshot}'. Available: {available}")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cached = CACHE_DIR / f"{snapshot}.xlsx"

    if cached.exists() and not args.no_cache:
        print(f"Using cached {cached.name}")
        return cached

    url = SNAPSHOT_URLS[snapshot]
    print(f"Downloading {snapshot} snapshot (~20 MB) …")
    req = urllib.request.Request(url, headers={"User-Agent": "cz-school-sankey/1.0"})
    with urllib.request.urlopen(req, timeout=120) as response, cached.open("wb") as out:
        data = response.read()
        out.write(data)
    print(f"Saved to {cached} ({len(data) // 1024} KB)")
    return cached


def find_column(headers: list[str], candidates: list[str]) -> str | None:
    """Return the first header that case-insensitively matches a candidate."""
    lower_headers = {h.lower().strip(): h for h in headers}
    for candidate in candidates:
        match = lower_headers.get(candidate.lower().strip())
        if match is not None:
            return match
    return None


def normalize_amount(raw: object) -> int:
    """Parse a cell value as a rounded integer CZK amount."""
    if raw is None:
        return 0
    text = str(raw).replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return int(round(float(text)))
    except ValueError:
        return 0


def normalize_ico(raw: object) -> str:
    """Return an 8-digit zero-padded IČO string, or '' if unparseable."""
    text = str(raw or "").strip().split(".")[0]  # strip decimal if numeric cell
    digits = "".join(c for c in text if c.isdigit())
    return digits.zfill(8) if digits else ""


def process_xlsx(
    xlsx_path: Path,
    school_icos: dict[str, str],
    list_columns: bool = False,
) -> list[dict[str, str]]:
    """Parse the XLSX and return matched eu_projects rows."""
    print(f"Opening {xlsx_path.name} …")
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)

    # The file has a 3-row preamble (title + generation date + blank) before
    # the actual Czech-language header row (row 4). Skip until we find it.
    headers: list[str] = []
    preamble_rows = 0
    for raw_row in rows:
        non_empty = [c for c in raw_row if c is not None and str(c).strip()]
        if len(non_empty) >= 5:
            headers = [str(h or "").strip() for h in raw_row]
            # Skip the bilingual English echo row immediately after
            next(rows, None)
            break
        preamble_rows += 1
        if preamble_rows > 10:
            sys.exit("Could not find header row in first 10 rows — unexpected XLSX format.")

    if not headers:
        sys.exit("XLSX appears empty")

    if list_columns:
        print("\nColumns in XLSX:")
        for i, h in enumerate(headers):
            print(f"  [{i}] {h!r}")
        wb.close()
        return []

    col_ico = find_column(headers, ICO_CANDIDATES)
    col_programme = find_column(headers, PROGRAMME_CANDIDATES)
    col_project = find_column(headers, PROJECT_NAME_CANDIDATES)
    col_amount = find_column(headers, AMOUNT_CANDIDATES)
    # Registration number used for deduplication across procurement sub-rows
    col_reg = find_column(headers, ["Registrační číslo projektu", "Registration number"])

    missing = [name for name, col in [
        ("IČO", col_ico), ("Programme", col_programme),
        ("Project name", col_project), ("Amount", col_amount),
    ] if col is None]
    if missing:
        print(
            f"\nWARNING: Could not detect columns: {', '.join(missing)}\n"
            "Run with --list-columns to inspect actual headers.\n"
            "Update the *_CANDIDATES lists in this script if needed.",
            file=sys.stderr,
        )
        if col_ico is None:
            wb.close()
            sys.exit("Cannot proceed without an IČO column.")

    print(f"Detected columns → IČO: {col_ico!r} | Programme: {col_programme!r} | "
          f"Project: {col_project!r} | Amount: {col_amount!r}")

    ico_idx = headers.index(col_ico)
    programme_idx = headers.index(col_programme) if col_programme else None
    project_idx = headers.index(col_project) if col_project else None
    amount_idx = headers.index(col_amount) if col_amount else None
    reg_idx = headers.index(col_reg) if col_reg else None

    # Deduplicate: the XLSX has one row per project × procurement contract.
    # Keep only the first occurrence of each (IČO, registration number) pair
    # so each school-project link appears exactly once.
    seen_keys: set[tuple[str, str]] = set()
    matched: list[dict[str, str]] = []
    total_rows = 0

    for row in rows:
        total_rows += 1
        ico = normalize_ico(row[ico_idx])
        if ico not in school_icos:
            continue

        reg_num = str(row[reg_idx] or "").strip() if reg_idx is not None else ""
        dedup_key = (ico, reg_num)
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        institution_id = school_icos[ico]
        programme = str(row[programme_idx] or "").strip() if programme_idx is not None else ""
        project_name = str(row[project_idx] or "").strip() if project_idx is not None else ""
        amount = normalize_amount(row[amount_idx]) if amount_idx is not None else 0

        if amount <= 0:
            continue

        matched.append({
            "institution_id": institution_id,
            "programme": programme or "EU 2021-2027",
            "project_name": project_name or "EU project",
            "amount": str(amount),
            "basis": "allocated",
            "certainty": "observed",
        })

    wb.close()
    print(f"Scanned {total_rows} rows → {len(seen_keys)} unique project-school pairs → {len(matched)} with non-zero amounts")
    return matched


def write_eu_projects(year: int, rows: list[dict[str, str]]) -> Path:
    out_path = RAW_ROOT / str(year) / "eu_projects.csv"
    fieldnames = ["institution_id", "programme", "project_name", "amount", "basis", "certainty"]
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return out_path


def main() -> None:
    args = parse_args()

    school_icos = load_school_icos(args.year)
    xlsx_path = resolve_xlsx(args)
    matched = process_xlsx(xlsx_path, school_icos, list_columns=args.list_columns)

    if args.list_columns:
        return

    out_path = write_eu_projects(args.year, matched)
    print(f"Wrote {len(matched)} rows → {out_path.relative_to(ROOT)}")

    if not matched:
        print(
            "\nNo matches found. Possible causes:\n"
            "  • IČO column not detected correctly (run --list-columns)\n"
            "  • school_entities.csv uses different IČO format\n"
            "  • Schools in your dataset are not EU grant recipients"
        )


if __name__ == "__main__":
    main()
