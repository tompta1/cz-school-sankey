#!/usr/bin/env python3
"""Fetch founder-to-school budget transfers and produce founder_support.csv.

Two-pass strategy
-----------------
Pass 1  VYKZZ (Výkaz zisku a ztrát, MONITOR national extract)
        Per-school příspěvkové organizace income statement.
        Extracts account 672 (přijaté neinvestiční příspěvky od zřizovatele)
        and account 673 (přijaté investiční transfery od zřizovatele).
        Marked: basis=realized, certainty=observed.

Pass 2  FIN 2-12 M (MONITOR national extract)
        Per-founder ÚSC budget execution.
        Filters education paragraphs §3100–§3299 and transfer items
        5331 (neinvestiční příspěvky zřízeným PO) and 6351 (investiční
        transfery zřízeným PO).
        Applied to founders whose schools have no VYKZZ row.
        Amounts are pro-rated across the founder's schools by MŠMT
        allocation weight.
        Marked: basis=realized, certainty=inferred.

MONITOR extrakty base URL:
    https://monitor.statnipokladna.gov.cz/data/extrakty/csv/

Usage:
    python3 etl/fetch_founder_budgets.py --year 2025
    python3 etl/fetch_founder_budgets.py --year 2025 --period 2025_12
    python3 etl/fetch_founder_budgets.py --year 2025 --no-po
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

# VYKZZ = Výkaz zisků a ztrát (income statement for all public entities incl. schools).
# Contains account 672 (received non-investment grants) and 673 (investment transfers).
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

# Budget items: founder→PO transfers
# 5331  Neinvestiční příspěvky zřízeným příspěvkovým organizacím
# 5336  Neinvestiční transfery zřízeným příspěvkovým organizacím
# 6351  Investiční transfery zřízeným příspěvkovým organizacím
FOUNDER_TRANSFER_ITEMS = {"5331", "5336", "6351"}

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
#   ZC_SYNUC     Syntetický účet (e.g. "672", "673")
#   ZU_HLCIN     Hlavní činnost amount (main activity = school operations)
# ---------------------------------------------------------------------------
ICO_COLS_12M = ["ZC_ICO", "ico", "IČO"]
PARA_COLS = ["0FUNC_AREA", "paragraf", "FUNC_AREA"]
ITEM_COLS = ["ZCMMT_ITM", "polozka", "POLOZKA"]
AMOUNT_COLS_12M = ["ZU_ROZKZ", "vysledek", "skutecnost"]

ICO_COLS_PO = ["ZC_ICO", "ico", "IČO"]
ACCOUNT_COLS_PO = ["ZC_SYNUC", "synteticky_ucet", "ucet", "SU"]
AMOUNT_COLS_PO = ["ZU_HLCIN", "hlavni_cinnost", "castka", "ZU_HLCIBO"]

# Account codes for "received from founder" in VYKZZ
# 672 = přijaté neinvestiční příspěvky a náhrady (operating grant from founder)
# 673 = přijaté investiční transfery (investment transfer from founder)
FOUNDER_INCOME_ACCOUNTS = {"672", "6720", "6721", "6722", "673", "6730"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build founder_support.csv from MONITOR FIN 2-12 M and FIN 2-01 PO"
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
        help="Local FIN 2-01 PO CSV (skips download)",
    )
    parser.add_argument(
        "--no-po",
        action="store_true",
        help="Skip FIN 2-01 PO pass (produce only inferred rows)",
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
# Pass 1: FIN 2-01 PO — per-school observed příspěvek od zřizovatele
# ---------------------------------------------------------------------------

def run_po_pass(
    zip_path: Path,
    school_icos: dict[str, dict[str, str]],
    list_columns: bool,
) -> dict[str, int]:
    """Return {school_ico → total CZK received from founder} from VYKZZ."""
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
    col_amount = find_col(headers, AMOUNT_COLS_PO)

    print(
        f"FIN 2-01 PO columns → IČO: {col_ico!r} | "
        f"Account: {col_account!r} | Amount: {col_amount!r}"
    )

    if col_ico is None:
        print(
            "WARNING: IČO column not found in FIN 2-01 PO — skipping PO pass.\n"
            "Run --list-columns to inspect actual headers.",
            file=sys.stderr,
        )
        return {}

    ico_idx = headers.index(col_ico)
    account_idx = headers.index(col_account) if col_account else None
    amount_idx = headers.index(col_amount) if col_amount else None

    totals: dict[str, int] = {}
    scanned = 0

    for row in reader:
        if len(row) <= ico_idx:
            continue
        ico = normalize_ico(row[ico_idx])
        if ico not in school_icos:
            continue

        # Filter to founder-income accounts (672, 673) when account column present
        if account_idx is not None:
            account = normalize_code(row[account_idx] if account_idx < len(row) else "")
            # Match if the account starts with any known founder-income code
            if not any(account.startswith(a) for a in FOUNDER_INCOME_ACCOUNTS):
                continue

        amount = to_int(row[amount_idx]) if (amount_idx is not None and amount_idx < len(row)) else 0
        if amount <= 0:
            continue

        totals[ico] = totals.get(ico, 0) + amount
        scanned += 1

    print(
        f"FIN 2-01 PO: scanned {scanned} matching account rows → "
        f"{len(totals)} unique school IČOs with observed příspěvek"
    )
    return totals


# ---------------------------------------------------------------------------
# Pass 2: FIN 2-12 M — per-founder aggregate education transfers
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

        # Filter by transfer-to-PO budget items
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
                    f"FIN 2-12 M education transfers from founder {founder_ico}; "
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
                    f"FIN 2-12 M education transfers from founder {founder_ico}; "
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
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
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

    # ------------------------------------------------------------------
    # Pass 1: FIN 2-01 PO (observed per-school příspěvek)
    # ------------------------------------------------------------------
    po_totals: dict[str, int] = {}  # school_ico → CZK

    if not args.no_po:
        po_zip = resolve_zip(
            "FIN 2-01 PO",
            FIN01PO_URL_TEMPLATES,
            period,
            year,
            f"fin2-01po-{period}.zip",
            args.finpo,
            args.no_cache,
        )
        if po_zip is not None:
            po_totals = run_po_pass(po_zip, school_entities, args.list_columns)

    # ------------------------------------------------------------------
    # Pass 2: FIN 2-12 M (aggregate per-founder, for founders whose
    #          schools are not fully covered by Pass 1)
    # ------------------------------------------------------------------
    fm12_totals: dict[str, int] = {}  # founder_ico → CZK

    # Determine which founders still need aggregate coverage
    covered_school_icos = set(po_totals.keys())
    founders_needing_12m: set[str] = set()
    for founder_ico, schools in founder_to_schools.items():
        school_icos_for_founder = {normalize_ico(s.get("ico", "")) for s in schools}
        if not school_icos_for_founder.issubset(covered_school_icos):
            founders_needing_12m.add(founder_ico)

    if founders_needing_12m:
        print(
            f"{len(founders_needing_12m)} founders need FIN 2-12 M aggregate coverage"
        )
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

    # ------------------------------------------------------------------
    # Assemble output rows
    # ------------------------------------------------------------------
    output_rows: list[dict[str, Any]] = []

    # Observed rows from Pass 1
    for school_ico, amount in po_totals.items():
        entity = school_entities.get(school_ico)
        if entity is None:
            continue
        output_rows.append(
            {
                "institution_id": entity["institution_id"],
                "amount": amount,
                "basis": "realized",
                "certainty": "observed",
                "note": f"FIN 2-01 PO account 672/673; school IČO {school_ico}",
            }
        )

    # Inferred rows from Pass 2 (pro-rated per founder)
    observed_school_icos = set(po_totals.keys())

    for founder_ico, founder_total in fm12_totals.items():
        schools = founder_to_schools.get(founder_ico, [])
        # Only include schools not already covered by Pass 1
        uncovered = [
            s for s in schools
            if normalize_ico(s.get("ico", "")) not in observed_school_icos
        ]
        if not uncovered:
            continue
        inferred = prorate_founder_to_schools(founder_ico, founder_total, uncovered, msmt_weights)
        output_rows.extend(inferred)

    # ------------------------------------------------------------------
    # Report and write
    # ------------------------------------------------------------------
    n_observed = sum(1 for r in output_rows if r["certainty"] == "observed")
    n_inferred = sum(1 for r in output_rows if r["certainty"] == "inferred")
    total_czk = sum(r["amount"] for r in output_rows)

    print(
        f"\nResults: {len(output_rows)} rows total\n"
        f"  observed (FIN 2-01 PO):   {n_observed}\n"
        f"  inferred (FIN 2-12 M):    {n_inferred}\n"
        f"  total amount:             {total_czk:,} CZK"
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
    print(
        f"\nNext step: run build_school_year.py to incorporate these flows\n"
        f"  python3 etl/build_school_year.py --year {args.year}"
    )


if __name__ == "__main__":
    main()
