#!/usr/bin/env python3
"""Fetch founder support and school cost profiles from MONITOR.

Two-pass strategy
-----------------
Pass 1  VYKZZ (Výkaz zisku a ztráty, MONITOR national extract)
        Per-school realized public-transfer revenue from accounts 672/673,
        routed through the school's registered founder. The amount is observed,
        but founder attribution is inferred because these accounts can include
        transfers from several public budgets. The same pass builds compact
        realized school cost profiles.

Pass 2  FIN 2-12 M (MONITOR national extract)
        Fallback for schools without VYKZZ transfer revenue. Founder totals use
        own-budget items 5331/6351 in education paragraphs and are pro-rated by
        MŠMT allocation weight with certainty=inferred.

MONITOR extrakty base URL:
    https://monitor.statnipokladna.gov.cz/data/extrakty/csv/

Usage:
    python3 etl/fetch_founder_budgets.py --year 2025
    python3 etl/fetch_founder_budgets.py --year 2025 --period 2025_12
    python3 etl/fetch_founder_budgets.py --year 2025 --no-costs
    python3 etl/fetch_founder_budgets.py --year 2025 --list-columns
    python3 etl/fetch_founder_budgets.py --year 2025 \\
        --fin12m path/to/fin2-12m.csv \\
        --finpo  path/to/fin2-01po.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw"
CACHE_DIR = ROOT / "etl" / "data" / "monitor_cache"

# ---------------------------------------------------------------------------
# MONITOR extrakty URL templates (tried in order until one succeeds).
# The period string is YYYYMM, e.g. "202512".
# ---------------------------------------------------------------------------

# URL pattern (confirmed working):
#   https://monitor.statnipokladna.gov.cz/data/extrakty/csv/{Dir}/{YYYY}_{MM}_Data_CSUIS_{DATASET}.zip
# Where YYYY and MM come from the period string, e.g. "2025_12" for December 2025.
MONITOR_BASE = "https://monitor.statnipokladna.gov.cz/data/extrakty/csv"

FIN12M_URL_TEMPLATES = [
    f"{MONITOR_BASE}/FinM/{{year}}_{{month}}_Data_CSUIS_FINM.zip",
]

# VYKZZ = income statement for all public entities, including public schools.
FIN01PO_URL_TEMPLATES = [
    f"{MONITOR_BASE}/ZiskZtraty/{{year}}_{{month}}_Data_CSUIS_VYKZZ.zip",
]

# Default period for each budget year (use December = full-year execution).
# December 2025 data is typically published ~Feb-Mar 2026; use it when current.
DEFAULT_PERIOD: dict[int, str] = {
    2024: "2024_12",
    2025: "2025_12",
    2026: "2026_12",
}

# ---------------------------------------------------------------------------
# Education paragraph range (functional classification).
# §3100–§3299 covers pre-school through vocational/art education.
# ---------------------------------------------------------------------------
EDUCATION_PARA_MIN = 3100
EDUCATION_PARA_MAX = 3299

# Own-budget founder-to-PO items. Items 5336 and 6356 are pass-through
# transfers received from another public budget and must not be counted here.
# 5331  Neinvestiční příspěvky zřízeným příspěvkovým organizacím
# 6351  Investiční transfery zřízeným příspěvkovým organizacím
FOUNDER_TRANSFER_ITEMS = {"5331", "6351"}

# ---------------------------------------------------------------------------
# MONITOR CSV format notes:
#   - Delimiter is semicolon (;), not comma.
#   - First row is a SAP BW-style header: "Label"TECHNAME:TECHNAME;...
#   - Headers are normalized by: h.strip().split(":")[-1].strip().strip('"')
#
# FIN 2-12 M (FINM201) confirmed field names:
#   ZC_ICO       IČO of reporting entity (obec/kraj)
#   0FUNC_AREA   Paragraf (functional classification code)
#   ZCMMT_ITM    Položka (budget item code)
#   ZU_ROZKZ     Výsledek od počátku roku (actual execution amount)
#
# VYKZZ (Výkaz zisku a ztrát) confirmed field names:
#   ZC_ICO       IČO of reporting entity (school)
#   ZC_POLVYK    statement row (A. is total costs)
#   ZC_SYNUC     Syntetický účet (e.g. "502", "511")
#   ZU_HLCIN     Hlavní činnost amount (main activity = school operations)
# ---------------------------------------------------------------------------
ICO_COLS_12M = ["ZC_ICO", "ico", "IČO"]
PARA_COLS = ["0FUNC_AREA", "paragraf", "FUNC_AREA"]
ITEM_COLS = ["ZCMMT_ITM", "polozka", "POLOZKA"]
AMOUNT_COLS_12M = ["ZU_ROZKZ", "vysledek", "skutecnost"]

ICO_COLS_PO = ["ZC_ICO", "ico", "IČO"]
ACCOUNT_COLS_PO = ["ZC_SYNUC", "synteticky_ucet", "ucet", "SU"]
ROW_COLS_PO = ["ZC_POLVYK", "polozka_vykazu", "POLVYK"]
AMOUNT_COLS_PO = ["ZU_HLCIN", "hlavni_cinnost", "castka", "ZU_HLCIBO"]

SCHOOL_COST_ACCOUNTS = {
    "materials_amount": {"501", "503", "504", "506", "507", "508"},
    "energy_amount": {"502"},
    "repairs_amount": {"511"},
    "services_amount": {"512", "513", "516", "518"},
    "personnel_amount": {"521", "524", "525", "527", "528"},
    "depreciation_amount": {"551"},
}

# VYKZZ transfer-revenue accounts used by the former nationwide method.
# The value is observed at the school, while attribution to its registry founder
# remains inferred because the account does not identify the sending budget.
FOUNDER_INCOME_ACCOUNTS = {"672", "673"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build founder_support.csv and school_costs.csv from MONITOR"
    )
    parser.add_argument("--year", type=int, required=True, help="Budget year")
    parser.add_argument(
        "--period",
        help="MONITOR period string, e.g. 202512 (default: Dec of --year)",
    )
    parser.add_argument(
        "--fin12m",
        type=Path,
        help="Local FIN 2-12 M CSV (skips download)",
    )
    parser.add_argument(
        "--finpo",
        type=Path,
        help="Local VYKZZ ZIP/CSV (skips download)",
    )
    parser.add_argument(
        "--no-costs",
        "--no-po",
        dest="no_costs",
        action="store_true",
        help="Skip the VYKZZ school cost-profile pass",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Re-download even if cached",
    )
    parser.add_argument(
        "--list-columns",
        action="store_true",
        help="Print column headers from each file and exit",
    )
    return parser.parse_args()


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def find_col(headers: list[str], candidates: list[str]) -> str | None:
    """Return first header that case-insensitively matches any candidate."""
    lowered = {h.lower().strip(): h for h in headers}
    for c in candidates:
        match = lowered.get(c.lower().strip())
        if match is not None:
            return match
    return None


def to_int(raw: object) -> int:
    text = str(raw or "").replace("\xa0", "").replace(" ", "").replace(",", ".")
    if text.endswith("-"):
        text = f"-{text[:-1]}"
    try:
        return int(round(float(text)))
    except ValueError:
        return 0


def normalize_ico(raw: object) -> str:
    text = str(raw or "").strip().split(".")[0]
    digits = "".join(c for c in text if c.isdigit())
    return digits.zfill(8) if digits else ""


def normalize_code(raw: object) -> str:
    """Normalize paragraph/item/account codes: strip, remove dots/spaces."""
    return str(raw or "").strip().replace(".", "").replace(" ", "")


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def try_download(url: str, dest: Path) -> bool:
    """Try to download url to dest. Return True on success."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "cz-school-sankey/1.0"})
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = resp.read()
        dest.write_bytes(data)
        print(f"  Downloaded {len(data) // 1024} KB → {dest.name}")
        return True
    except Exception as exc:
        print(f"  {url} → {exc}", file=sys.stderr)
        return False


def resolve_zip(
    label: str,
    templates: list[str],
    period: str,
    year: int,
    cache_name: str,
    local_path: Path | None,
    no_cache: bool,
) -> Path | None:
    """Return a local path to the ZIP, downloading if needed."""
    if local_path is not None:
        if not local_path.exists():
            sys.exit(f"Local file not found: {local_path}")
        return local_path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cached = CACHE_DIR / cache_name

    if cached.exists() and not no_cache:
        print(f"Using cached {cached.name}")
        return cached

    # period is "YYYY_MM", split for template substitution
    parts = period.split("_")
    p_year = parts[0] if parts else str(year)
    p_month = parts[1] if len(parts) > 1 else "12"

    print(f"Downloading {label} for period {period}…")
    for template in templates:
        url = template.format(period=period, year=p_year, month=p_month)
        print(f"  Trying {url}")
        if try_download(url, cached):
            return cached
        time.sleep(0.5)

    print(
        f"\nCould not download {label}. Options:\n"
        f"  1. Download manually and pass --fin12m / --finpo path/to/file.csv\n"
        f"  2. Check https://monitor.statnipokladna.gov.cz/data/extrakty/csv/ for the correct URL\n"
        f"  3. Run with --no-po to skip the per-school pass\n",
        file=sys.stderr,
    )
    return None


def extract_csv_from_zip(zip_path: Path, preferred_prefix: str = "") -> io.TextIOWrapper | None:
    """Return a text stream for the target CSV inside a ZIP file."""
    try:
        zf = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile:
        print(f"  {zip_path.name} is not a valid ZIP — treating as plain CSV")
        return zip_path.open("r", encoding="utf-8-sig")

    csv_names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
    if not csv_names:
        print(f"No CSV found inside {zip_path.name}. Contents: {zf.namelist()}", file=sys.stderr)
        return None

    # Prefer a file whose name starts with the given prefix (e.g. "FINM201")
    if preferred_prefix:
        preferred = [n for n in csv_names if Path(n).name.startswith(preferred_prefix)]
        chosen = preferred[0] if preferred else csv_names[0]
    else:
        chosen = csv_names[0]

    others = [n for n in csv_names if n != chosen]
    if others:
        print(f"  Multiple CSVs in ZIP; using {chosen!r}. Others: {others}")
    raw = zf.open(chosen)
    return io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")



# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------

def load_school_entities(year: int) -> dict[str, dict[str, str]]:
    """Return {ico → row} for all schools with a non-empty ico."""
    path = RAW_ROOT / str(year) / "school_entities.csv"
    if not path.exists():
        sys.exit(f"Missing {path}. Run parse_msmt_xlsx.py first.")
    result: dict[str, dict[str, str]] = {}
    for row in load_csv(path):
        ico = normalize_ico(row.get("ico", ""))
        if ico:
            result[ico] = row
    print(f"Loaded {len(result)} school IČOs from school_entities.csv")
    return result


def load_msmt_weights(year: int) -> dict[str, int]:
    """Return {institution_id → total MŠMT allocation} for pro-ration."""
    path = RAW_ROOT / str(year) / "msmt_allocations.csv"
    if not path.exists():
        return {}
    weights: dict[str, int] = {}
    for row in load_csv(path):
        inst_id = row.get("institution_id", "")
        total = (
            to_int(row.get("pedagogical_amount"))
            + to_int(row.get("nonpedagogical_amount"))
            + to_int(row.get("oniv_amount"))
            + to_int(row.get("other_amount"))
        )
        if inst_id and total > 0:
            weights[inst_id] = total
    print(f"Loaded {len(weights)} MŠMT weights for pro-ration")
    return weights


# ---------------------------------------------------------------------------
# VYKZZ pass: observed transfer revenue attributed to the registry founder
# ---------------------------------------------------------------------------

def run_transfer_revenue_pass(
    zip_path: Path,
    school_icos: dict[str, dict[str, str]],
    list_columns: bool,
) -> dict[str, int]:
    """Return observed account 672/673 revenue keyed by school IČO."""
    stream = extract_csv_from_zip(zip_path)
    if stream is None:
        return {}

    reader = csv.reader(stream, delimiter=";")
    raw_row = next(reader, [])
    headers = [h.strip().split(":")[-1].strip().strip('"') for h in raw_row]

    if list_columns:
        return {}

    col_ico = find_col(headers, ICO_COLS_PO)
    col_account = find_col(headers, ACCOUNT_COLS_PO)
    col_amount = find_col(headers, AMOUNT_COLS_PO)
    if col_ico is None or col_account is None or col_amount is None:
        print(
            "WARNING: Required VYKZZ transfer-revenue columns not found; using FIN 2-12 M fallback.",
            file=sys.stderr,
        )
        return {}

    ico_idx = headers.index(col_ico)
    account_idx = headers.index(col_account)
    amount_idx = headers.index(col_amount)
    totals: dict[str, int] = {}
    matched = 0

    for row in reader:
        if len(row) <= max(ico_idx, account_idx, amount_idx):
            continue
        ico = normalize_ico(row[ico_idx])
        if ico not in school_icos:
            continue
        account = normalize_code(row[account_idx])
        if not any(account.startswith(code) for code in FOUNDER_INCOME_ACCOUNTS):
            continue
        amount = to_int(row[amount_idx])
        if amount <= 0:
            continue
        totals[ico] = totals.get(ico, 0) + amount
        matched += 1

    print(
        f"VYKZZ: matched {matched} account 672/673 rows -> "
        f"{len(totals)} school transfer-revenue totals"
    )
    return totals


# ---------------------------------------------------------------------------
# VYKZZ pass: observed per-school cost profile
# ---------------------------------------------------------------------------

def run_cost_profile_pass(
    zip_path: Path,
    school_icos: dict[str, dict[str, str]],
    list_columns: bool,
) -> dict[str, dict[str, int]]:
    """Return realized main-activity cost categories keyed by school IČO."""
    stream = extract_csv_from_zip(zip_path)
    if stream is None:
        return {}

    reader = csv.reader(stream, delimiter=";")
    raw_row = next(reader, [])

    # Normalize SAP BW-style headers: '"Label"TECHNAME:TECHNAME' → 'TECHNAME'
    # Works for both VYKZZ (ZC_ICO:ZC_ICO) and FINM201 ("IČO"ZC_ICO:ZC_ICO)
    headers = [h.strip().split(":")[-1].strip().strip('"') for h in raw_row]

    if list_columns:
        print(f"\nVYKZZ columns ({len(headers)} total):")
        for i, h in enumerate(headers):
            print(f"  [{i:3d}] {h!r}")
        return {}

    col_ico = find_col(headers, ICO_COLS_PO)
    col_account = find_col(headers, ACCOUNT_COLS_PO)
    col_row = find_col(headers, ROW_COLS_PO)
    col_amount = find_col(headers, AMOUNT_COLS_PO)

    print(
        f"VYKZZ columns → IČO: {col_ico!r} | Account: {col_account!r} | "
        f"Statement row: {col_row!r} | Amount: {col_amount!r}"
    )

    if col_ico is None or col_account is None or col_row is None or col_amount is None:
        print(
            "WARNING: Required VYKZZ columns not found; skipping cost profiles.\n"
            "Run --list-columns to inspect actual headers.",
            file=sys.stderr,
        )
        return {}

    ico_idx = headers.index(col_ico)
    account_idx = headers.index(col_account)
    row_idx = headers.index(col_row)
    amount_idx = headers.index(col_amount)

    profiles: dict[str, dict[str, int]] = {}
    matched = 0

    for row in reader:
        if len(row) <= max(ico_idx, account_idx, row_idx, amount_idx):
            continue
        ico = normalize_ico(row[ico_idx])
        if ico not in school_icos:
            continue

        profile = profiles.setdefault(
            ico,
            {"total_costs_amount": 0, **{key: 0 for key in SCHOOL_COST_ACCOUNTS}},
        )
        account = normalize_code(row[account_idx])
        statement_row = normalize_code(row[row_idx])
        amount = to_int(row[amount_idx])

        if account == "-" and statement_row == "A":
            profile["total_costs_amount"] = amount
            matched += 1
            continue

        for bucket, accounts in SCHOOL_COST_ACCOUNTS.items():
            if account in accounts:
                profile[bucket] += amount
                matched += 1
                break

    result: dict[str, dict[str, int]] = {}
    for ico, profile in profiles.items():
        total = profile["total_costs_amount"]
        if total <= 0:
            continue
        categorized = sum(profile[key] for key in SCHOOL_COST_ACCOUNTS)
        profile["other_costs_amount"] = max(total - categorized, 0)
        result[ico] = profile

    print(
        f"VYKZZ: matched {matched} cost rows → "
        f"{len(result)} school cost profiles"
    )
    return result


# ---------------------------------------------------------------------------
# FIN 2-12 M pass: per-founder own-budget education support
# ---------------------------------------------------------------------------

def run_12m_pass(
    zip_path: Path,
    founder_icos: set[str],
    list_columns: bool,
) -> dict[str, int]:
    """Return {founder_ico → total CZK education transfers to POs}."""
    # FINM201 = Plnění rozpočtu místně řízených organizací (the right table for ÚSC→PO flows)
    stream = extract_csv_from_zip(zip_path, preferred_prefix="FINM201")
    if stream is None:
        return {}

    reader = csv.reader(stream, delimiter=";")
    raw_row = next(reader, [])

    # Normalize SAP BW-style headers
    headers = [h.strip().split(":")[-1].strip().strip('"') for h in raw_row]

    if list_columns:
        print(f"\nFIN 2-12 M (FINM201) columns ({len(headers)} total):")
        for i, h in enumerate(headers):
            print(f"  [{i:3d}] {h!r}")
        return {}

    col_ico = find_col(headers, ICO_COLS_12M)
    col_para = find_col(headers, PARA_COLS)
    col_item = find_col(headers, ITEM_COLS)
    col_amount = find_col(headers, AMOUNT_COLS_12M)

    print(
        f"FIN 2-12 M columns → IČO: {col_ico!r} | "
        f"Paragraf: {col_para!r} | Položka: {col_item!r} | Amount: {col_amount!r}"
    )

    missing = [n for n, c in [("IČO", col_ico), ("Paragraf", col_para), ("Amount", col_amount)] if c is None]
    if missing:
        print(
            f"WARNING: Could not detect columns: {', '.join(missing)}\n"
            "Run --list-columns to inspect actual headers.",
            file=sys.stderr,
        )
    if col_ico is None or col_amount is None:
        return {}

    ico_idx = headers.index(col_ico)
    para_idx = headers.index(col_para) if col_para else None
    item_idx = headers.index(col_item) if col_item else None
    amount_idx = headers.index(col_amount)

    totals: dict[str, int] = {}
    scanned = matched = 0

    for row in reader:
        scanned += 1
        if len(row) <= ico_idx:
            continue
        ico = normalize_ico(row[ico_idx])
        if ico not in founder_icos:
            continue

        # Filter by education paragraph
        if para_idx is not None and para_idx < len(row):
            para_raw = normalize_code(row[para_idx])
            try:
                para_int = int(para_raw)
            except ValueError:
                continue
            if not (EDUCATION_PARA_MIN <= para_int <= EDUCATION_PARA_MAX):
                continue
        # If no paragraph column detected, accept all rows for this founder

        # Keep only the founder's own contribution, not pass-through transfers.
        if item_idx is not None and item_idx < len(row):
            item = normalize_code(row[item_idx])
            if item not in FOUNDER_TRANSFER_ITEMS:
                continue

        amount = to_int(row[amount_idx] if amount_idx < len(row) else 0)
        if amount <= 0:
            continue

        totals[ico] = totals.get(ico, 0) + amount
        matched += 1

    print(
        f"FIN 2-12 M: scanned {scanned} rows → {matched} matching "
        f"(education+transfer) → {len(totals)} unique founder IČOs"
    )
    return totals


# ---------------------------------------------------------------------------
# Pro-rate founder aggregate to schools (inferred)
# ---------------------------------------------------------------------------

def prorate_founder_to_schools(
    founder_ico: str,
    founder_total: int,
    schools: list[dict[str, str]],
    msmt_weights: dict[str, int],
) -> list[dict[str, Any]]:
    """Allocate founder_total to schools proportionally by MŠMT weight."""
    weighted = []
    for school in schools:
        inst_id = school["institution_id"]
        w = msmt_weights.get(inst_id, 0)
        if w > 0:
            weighted.append((inst_id, w))

    if not weighted:
        # No MŠMT weight data — split equally
        n = len(schools)
        if n == 0:
            return []
        share = founder_total // n
        return [
            {
                "institution_id": s["institution_id"],
                "amount": share,
                "basis": "realized",
                "certainty": "inferred",
                "note": (
                    f"FIN 2-12 M own-budget items 5331/6351 from founder {founder_ico}; "
                    f"equal split across {n} schools (no MŠMT weight available)"
                ),
            }
            for s in schools
        ]

    total_weight = sum(w for _, w in weighted)
    rows = []
    allocated = 0
    for i, (inst_id, w) in enumerate(weighted):
        if i == len(weighted) - 1:
            # Last school gets the remainder to avoid rounding drift
            share = founder_total - allocated
        else:
            share = int(round(founder_total * w / total_weight))
        allocated += share
        if share <= 0:
            continue
        rows.append(
            {
                "institution_id": inst_id,
                "amount": share,
                "basis": "realized",
                "certainty": "inferred",
                "note": (
                    f"FIN 2-12 M own-budget items 5331/6351 from founder {founder_ico}; "
                    f"pro-rated by MŠMT allocation share "
                    f"({w:,} / {total_weight:,} = {100*w/total_weight:.1f}%)"
                ),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Write output
# ---------------------------------------------------------------------------

def write_founder_support(year: int, rows: list[dict[str, Any]]) -> Path:
    out_path = RAW_ROOT / str(year) / "founder_support.csv"
    fieldnames = ["institution_id", "amount", "basis", "certainty", "note"]
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    return out_path


def write_school_costs(year: int, rows: list[dict[str, Any]]) -> Path:
    out_path = RAW_ROOT / str(year) / "school_costs.csv"
    fieldnames = [
        "institution_id",
        "ico",
        "total_costs_amount",
        "materials_amount",
        "energy_amount",
        "repairs_amount",
        "services_amount",
        "personnel_amount",
        "depreciation_amount",
        "other_costs_amount",
        "basis",
        "certainty",
        "note",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    return out_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    year = args.year
    period = args.period or DEFAULT_PERIOD.get(year) or f"{year}12"

    school_entities = load_school_entities(year)
    msmt_weights = load_msmt_weights(year)

    # Build index: founder_ico → list of school entity rows
    founder_to_schools: dict[str, list[dict[str, str]]] = {}
    for ico, entity in school_entities.items():
        founder_id = entity.get("founder_id", "")
        founder_ico = founder_id.removeprefix("founder:").strip()
        if founder_ico:
            founder_to_schools.setdefault(founder_ico, []).append(entity)

    founder_icos = set(founder_to_schools.keys())
    print(f"Targeting {len(founder_icos)} unique founder IČOs, {len(school_entities)} schools")

    transfer_totals: dict[str, int] = {}
    cost_profiles: dict[str, dict[str, int]] = {}
    if not args.no_costs:
        po_zip = resolve_zip(
            "VYKZZ",
            FIN01PO_URL_TEMPLATES,
            period,
            year,
            f"fin2-01po-{period}.zip",
            args.finpo,
            args.no_cache,
        )
        if po_zip is not None:
            transfer_totals = run_transfer_revenue_pass(po_zip, school_entities, args.list_columns)
            cost_profiles = run_cost_profile_pass(po_zip, school_entities, args.list_columns)

    fm12_totals: dict[str, int] = {}  # founder_ico → CZK
    covered_school_icos = set(transfer_totals)
    founders_needing_12m = {
        founder_ico
        for founder_ico, schools in founder_to_schools.items()
        if not {
            normalize_ico(school.get("ico", "")) for school in schools
        }.issubset(covered_school_icos)
    }
    if founders_needing_12m:
        fm12_zip = resolve_zip(
            "FIN 2-12 M",
            FIN12M_URL_TEMPLATES,
            period,
            year,
            f"fin2-12m-{period}.zip",
            args.fin12m,
            args.no_cache,
        )
        if fm12_zip is not None:
            fm12_totals = run_12m_pass(fm12_zip, founders_needing_12m, args.list_columns)

    if args.list_columns:
        return

    output_rows: list[dict[str, Any]] = []
    for school_ico, amount in transfer_totals.items():
        entity = school_entities.get(school_ico)
        if entity is None:
            continue
        output_rows.append(
            {
                "institution_id": entity["institution_id"],
                "amount": amount,
                "basis": "realized",
                "certainty": "inferred",
                "note": (
                    f"MONITOR VYKZZ account 672/673 public-transfer revenue for school IČO {school_ico}; "
                    "amount observed at school, attribution to registered founder inferred"
                ),
            }
        )

    for founder_ico, founder_total in fm12_totals.items():
        schools = founder_to_schools.get(founder_ico, [])
        uncovered = [
            school
            for school in schools
            if normalize_ico(school.get("ico", "")) not in covered_school_icos
        ]
        inferred = prorate_founder_to_schools(founder_ico, founder_total, uncovered, msmt_weights)
        output_rows.extend(inferred)

    output_rows.sort(key=lambda row: row["institution_id"])

    cost_rows: list[dict[str, Any]] = []
    for school_ico, profile in cost_profiles.items():
        entity = school_entities.get(school_ico)
        if entity is None:
            continue
        cost_rows.append(
            {
                "institution_id": entity["institution_id"],
                "ico": school_ico,
                **profile,
                "basis": "realized",
                "certainty": "observed",
                "note": "MONITOR VYKZZ main-activity costs; rent is included in account 518 services",
            }
        )
    cost_rows.sort(key=lambda row: row["institution_id"])

    # ------------------------------------------------------------------
    # Report and write
    # ------------------------------------------------------------------
    total_czk = sum(r["amount"] for r in output_rows)

    print(
        f"\nResults:\n"
        f"  VYKZZ transfer rows:      {len(transfer_totals)}\n"
        f"  FIN 2-12 M fallback rows: {len(output_rows) - len(transfer_totals)}\n"
        f"  attributed transfer total:{total_czk:>15,} CZK\n"
        f"  observed cost profiles:   {len(cost_rows)}"
    )

    if not output_rows:
        print(
            "\nNo rows produced. Possible causes:\n"
            "  • MONITOR files could not be downloaded — pass --fin12m/--finpo\n"
            "  • Column detection failed — run --list-columns to inspect headers\n"
            "  • Education paragraphs/items not present in this period's data\n"
            "  • IČO format mismatch between school_entities.csv and MONITOR\n"
        )
        return

    out_path = write_founder_support(year, output_rows)
    print(f"Wrote {len(output_rows)} rows → {out_path.relative_to(ROOT)}")
    if cost_rows:
        costs_path = write_school_costs(year, cost_rows)
        print(f"Wrote {len(cost_rows)} rows → {costs_path.relative_to(ROOT)}")
    print(
        f"\nNext step: run build_school_year.py to incorporate these flows\n"
        f"  python3 etl/build_school_year.py --year {args.year}"
    )


if __name__ == "__main__":
    main()
