#!/usr/bin/env python3
"""Fetch Czech state budget (Státní rozpočet) revenue summary from MONITOR.

Source dataset: PBSR – Podrobný rozpis SR (detailed state budget breakdown).
URL pattern:
    https://monitor.statnipokladna.gov.cz/data/extrakty/csv/PBSR/{year}_12_Data_CSUIS_PBSR.zip

SAP BW header format: same as FINM / VYKZZ (semicolon-delimited, first row
is '"Label"TECHNAME:TECHNAME').

Confirmed / expected column names (may vary by year):
    KAPITOLA   – ministry chapter code (e.g. 0333 = MŠMT)
    POLOZKA    – budget item code
    ZU_ROZKZ   – realized amount from beginning of year (in CZK)

Budget item (POLOZKA) classification:
    1xxx        Daňové příjmy (tax revenues)
    2xxx        Nedaňové příjmy (non-tax revenues)
    3xxx        Kapitálové příjmy
    4111–4219   Přijaté transfery z EU (EU structural funds received by SR)
    4xxx other  Ostatní přijaté transfery
    5xxx–6xxx   Výdaje (expenditures)
    8xxx        Financování (net borrowing / debt)

Outputs  etl/data/raw/{year}/state_budget.csv:
    node_id,node_name,node_category,flow_type,amount_czk,basis,certainty,source_url

Flow types:
    state_revenue  – income flowing into state:cr
    state_to_other – residual expenditure (state:cr → state:other), derived

Usage:
    python3 etl/fetch_state_budget.py --year 2025
    python3 etl/fetch_state_budget.py --year 2025 --list-columns
    python3 etl/fetch_state_budget.py --year 2024 --pbsr path/to/pbsr.csv
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

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw"
CACHE_DIR = ROOT / "etl" / "data" / "monitor_cache"

MONITOR_BASE = "https://monitor.statnipokladna.gov.cz/data/extrakty/csv"
PBSR_URL_TEMPLATES = [
    f"{MONITOR_BASE}/PBSR/{{year}}_{{month}}_Data_CSUIS_PBSR.zip",
    f"{MONITOR_BASE}/Rozpocet/{{year}}_{{month}}_Data_CSUIS_PBSR.zip",
]

DEFAULT_PERIOD: dict[int, str] = {
    2024: "2024_12",
    2025: "2025_12",
    2026: "2026_12",
}

# Ministry of Education chapter code (MŠMT = Ministerstvo školství).
MSMT_CHAPTER = {"0333", "333"}

# POLOZKA prefix → revenue group (strip trailing zeros for matching)
REVENUE_GROUPS: list[tuple[set[str], str, str, str]] = [
    # (polozka_prefixes, node_id, node_name, category)
    ({"11", "12", "13", "15"}, "income:taxes",  "Daňové příjmy",          "other"),
    ({"4111", "4112", "4113", "4114", "4115", "4116",
      "4211", "4212", "4213", "4214", "4215", "4216"},
                               "income:eu",     "Přijaté transfery EU",    "other"),
    ({"21", "22", "23", "24", "31", "32", "33", "41", "42", "43", "44"},
                               "income:nontax", "Ostatní příjmy SR",       "other"),
    ({"81", "82", "83", "84", "85", "86"},
                               "income:debt",   "Financování dluhem",      "other"),
]

# POLOZKA prefix for expenditure items (5xxx, 6xxx)
EXPENDITURE_PREFIXES = {"5", "6"}

CHAPTER_COLS = ["KAPITOLA", "kapitola", "KAP"]
ITEM_COLS = ["POLOZKA", "ZCMMT_ITM", "polozka"]
AMOUNT_COLS = ["ZU_ROZKZ", "skutecnost", "ZU_ROZP_KR"]


# ---------------------------------------------------------------------------
# Helpers (shared with fetch_founder_budgets pattern)
# ---------------------------------------------------------------------------

def normalize_header(raw: str) -> str:
    return raw.strip().split(":")[-1].strip().strip('"')


def parse_csv_bytes(data: bytes) -> tuple[list[str], list[dict[str, str]]]:
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text), delimiter=";")
    raw_headers = next(reader, [])
    headers = [normalize_header(h) for h in raw_headers]
    rows = [dict(zip(headers, row)) for row in reader]
    return headers, rows


def find_col(row: dict[str, str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in row:
            return c
    return None


def to_int(val: str) -> int:
    cleaned = (val or "0").replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return int(round(float(cleaned)))
    except ValueError:
        return 0


def download_with_retry(url: str, dest: Path, no_cache: bool) -> bytes:
    if not no_cache and dest.exists():
        print(f"  cache hit: {dest.name}", file=sys.stderr)
        return dest.read_bytes()
    print(f"  GET {url}", file=sys.stderr)
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(url, timeout=120) as resp:
                data = resp.read()
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)
            return data
        except Exception as exc:
            if attempt == 3:
                raise
            print(f"  attempt {attempt} failed ({exc}), retrying…", file=sys.stderr)
            time.sleep(5)
    raise RuntimeError("unreachable")


def extract_csv_from_zip(data: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV found in ZIP. Contents: {zf.namelist()}")
        # Pick the largest CSV (usually the main data file)
        csv_names.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
        return zf.read(csv_names[0])


def load_pbsr(path: Path | None, year: int, period: str, no_cache: bool) -> bytes:
    if path:
        return extract_csv_from_zip(path.read_bytes()) if path.suffix.lower() == ".zip" else path.read_bytes()
    yr, mo = period.split("_")
    for template in PBSR_URL_TEMPLATES:
        url = template.format(year=yr, month=mo)
        cache_path = CACHE_DIR / f"PBSR_{period}.zip"
        try:
            data = download_with_retry(url, cache_path, no_cache)
            return extract_csv_from_zip(data)
        except Exception as exc:
            print(f"  URL failed ({url}): {exc}", file=sys.stderr)
    raise RuntimeError(
        "Could not download PBSR data. Check the URL templates in fetch_state_budget.py\n"
        "or supply a local file with --pbsr path/to/pbsr.csv"
    )


# ---------------------------------------------------------------------------
# Revenue matching
# ---------------------------------------------------------------------------

def polozka_group(polozka: str) -> tuple[str, str, str] | None:
    """Return (node_id, node_name, node_category) for a given POLOZKA code, or None."""
    code = polozka.strip().lstrip("0")
    for prefixes, node_id, node_name, cat in REVENUE_GROUPS:
        for prefix in prefixes:
            if code.startswith(prefix):
                return node_id, node_name, cat
    return None


def is_expenditure(polozka: str) -> bool:
    code = polozka.strip().lstrip("0")
    return any(code.startswith(p) for p in EXPENDITURE_PREFIXES)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def aggregate_pbsr(rows: list[dict[str, str]]) -> dict:
    """
    Returns:
        revenues: dict[node_id → int]  – total CZK received by state:cr
        msmt_expenditure: int          – total MŠMT (ch. 333) výdaje
        total_expenditure: int         – total all-chapter výdaje
    """
    if not rows:
        raise ValueError("PBSR CSV is empty")

    sample = rows[0]
    chapter_col = find_col(sample, CHAPTER_COLS)
    item_col = find_col(sample, ITEM_COLS)
    amount_col = find_col(sample, AMOUNT_COLS)

    if not chapter_col or not item_col or not amount_col:
        cols = sorted(sample.keys())
        raise KeyError(
            f"Required columns not found in PBSR.\n"
            f"Looking for:\n"
            f"  chapter: {CHAPTER_COLS}\n"
            f"  item:    {ITEM_COLS}\n"
            f"  amount:  {AMOUNT_COLS}\n"
            f"Available columns (first {min(40, len(cols))}):\n"
            f"  {cols[:40]}\n"
            "Adjust the column-name lists in fetch_state_budget.py."
        )

    revenues: dict[str, int] = {}
    msmt_expenditure = 0
    total_expenditure = 0

    for row in rows:
        polozka = row.get(item_col, "").strip()
        kapitola = row.get(chapter_col, "").strip()
        amount = to_int(row.get(amount_col, "0"))

        if amount == 0:
            continue

        group = polozka_group(polozka)
        if group:
            node_id = group[0]
            revenues[node_id] = revenues.get(node_id, 0) + amount

        if is_expenditure(polozka):
            total_expenditure += amount
            if kapitola in MSMT_CHAPTER:
                msmt_expenditure += amount

    return {
        "revenues": revenues,
        "msmt_expenditure": msmt_expenditure,
        "total_expenditure": total_expenditure,
    }


# ---------------------------------------------------------------------------
# CSV writing
# ---------------------------------------------------------------------------

FIELDNAMES = ["node_id", "node_name", "node_category", "flow_type",
              "amount_czk", "basis", "certainty", "source_url"]


def write_state_budget_csv(year: int, aggregated: dict, source_url: str) -> Path:
    revenues = aggregated["revenues"]
    msmt_exp = aggregated["msmt_expenditure"]
    total_exp = aggregated["total_expenditure"]
    other_exp = max(0, total_exp - msmt_exp)

    node_meta: dict[str, tuple[str, str]] = {
        nid: (name, cat)
        for _, nid, name, cat in REVENUE_GROUPS
    }

    out_path = RAW_ROOT / str(year) / "state_budget.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []

    for node_id, amount in revenues.items():
        if amount <= 0:
            continue
        node_name, node_cat = node_meta.get(node_id, (node_id, "other"))
        # EU transfers received by SR: treat as observed (directly reported)
        certainty = "observed" if node_id != "income:debt" else "inferred"
        rows.append({
            "node_id": node_id,
            "node_name": node_name,
            "node_category": node_cat,
            "flow_type": "state_revenue",
            "amount_czk": amount,
            "basis": "realized",
            "certainty": certainty,
            "source_url": source_url,
        })

    if other_exp > 0:
        rows.append({
            "node_id": "state:other",
            "node_name": "Ostatní výdaje SR",
            "node_category": "other",
            "flow_type": "state_to_other",
            "amount_czk": other_exp,
            "basis": "realized",
            "certainty": "inferred",
            "source_url": source_url,
        })

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch SR state budget summary from MONITOR")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--period", help="MONITOR period string YYYY_MM (default: Dec of --year)")
    parser.add_argument("--pbsr", type=Path, help="Local PBSR CSV or ZIP (skips download)")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--list-columns", action="store_true",
                        help="Print column names from downloaded file and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    period = args.period or DEFAULT_PERIOD.get(args.year, f"{args.year}_12")
    yr, mo = period.split("_")
    source_url = PBSR_URL_TEMPLATES[0].format(year=yr, month=mo)

    print(f"Fetching PBSR for period {period}…", file=sys.stderr)
    csv_bytes = load_pbsr(args.pbsr, args.year, period, args.no_cache)

    headers, rows = parse_csv_bytes(csv_bytes)

    if args.list_columns:
        print("\nColumns in PBSR CSV:")
        for h in headers:
            print(f"  {h}")
        return

    print(f"  {len(rows):,} rows loaded", file=sys.stderr)
    aggregated = aggregate_pbsr(rows)

    rev = aggregated["revenues"]
    msmt = aggregated["msmt_expenditure"]
    total = aggregated["total_expenditure"]

    print(f"\nRevenues:", file=sys.stderr)
    for nid, amt in rev.items():
        print(f"  {nid}: {amt/1e9:.1f} bn CZK", file=sys.stderr)
    print(f"MŠMT expenditure:   {msmt/1e9:.1f} bn CZK", file=sys.stderr)
    print(f"Total expenditure:  {total/1e9:.1f} bn CZK", file=sys.stderr)
    print(f"Other chapters:     {max(0, total-msmt)/1e9:.1f} bn CZK", file=sys.stderr)

    out_path = write_state_budget_csv(args.year, aggregated, source_url)
    print(f"\nWrote {out_path.relative_to(ROOT)}")
    print(f"\nNext step:")
    print(f"  python3 etl/build_school_year.py --year {args.year}")


if __name__ == "__main__":
    main()
