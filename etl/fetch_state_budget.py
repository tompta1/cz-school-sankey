#!/usr/bin/env python3
"""Fetch the official Czech state-budget final-account summary.

The source is the Ministry of Finance final-account workbook G. Table 1
contains realized revenue, expenditure, and deficit totals; table 2a contains
the four top-level revenue classes. Amounts in the PDF are reported in
thousands of CZK and are converted to whole CZK here.

Outputs ``etl/data/raw/{year}/state_budget.csv``. The ``state_budget_total``
row is reconciliation metadata and is intentionally not transformed into a
graph flow. ``state_to_other`` is the balancing residual after the tracked
MŠMT direct-school allocation rollup.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import sys
import time
import urllib.request
from decimal import Decimal
from pathlib import Path

from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw"
CACHE_DIR = ROOT / "etl" / "data" / "monitor_cache"

SOURCE_URLS = {
    2024: "https://mf.gov.cz/assets/attachments/2025-04-28_G-Tabulkova-cast.pdf",
    2025: "https://mf.gov.cz/assets/attachments/2026-04-30_G-Tabulkova-cast.pdf",
}

FIELDNAMES = [
    "node_id",
    "node_name",
    "node_category",
    "flow_type",
    "amount_czk",
    "basis",
    "certainty",
    "source_url",
]

NUMBER_RE = re.compile(r"-?\d[\d ]*\d,\d{2}")
REVENUE_LABELS = {
    "taxes": "1 Daňové příjmy",
    "nontax": "2 Nedaňové příjmy",
    "capital": "3 Kapitálové příjmy",
    "transfers": "4 Přijaté transfery",
}
CHAPTER_SPECS = {
    "306": "Ministerstvo zahraničních věcí",
    "307": "Ministerstvo obrany",
    "312": "Ministerstvo financí",
    "313": "Ministerstvo práce a sociálních věcí",
    "314": "Ministerstvo vnitra",
    "315": "Ministerstvo životního prostředí",
    "317": "Ministerstvo pro místní rozvoj",
    "322": "Ministerstvo průmyslu a obchodu",
    "327": "Ministerstvo dopravy",
    "329": "Ministerstvo zemědělství",
    "333": "Ministerstvo školství, mládeže a tělovýchovy",
    "334": "Ministerstvo kultury",
    "335": "Ministerstvo zdravotnictví",
    "336": "Ministerstvo spravedlnosti",
}

logging.getLogger("pypdf").setLevel(logging.ERROR)


def parse_czk_thousands(token: str) -> int:
    value = Decimal(token.replace(" ", "").replace(",", "."))
    return int(value * 1000)


def parse_actual_value(page_text: str, label: str) -> int:
    line = next((line for line in page_text.splitlines() if line.strip().startswith(label)), None)
    if line is None:
        raise RuntimeError(f"Could not find final-account row {label!r}")

    values = NUMBER_RE.findall(line)
    if len(values) < 4:
        raise RuntimeError(f"Unexpected numeric layout for {label!r}: {line}")
    # Final-account tables place the requested year's realized amount fourth.
    return parse_czk_thousands(values[3])


def parse_chapter_totals(page_text: str) -> dict[str, int]:
    totals: dict[str, int] = {}
    for line in page_text.splitlines():
        normalized = line.strip()
        code = normalized[:3]
        if code not in CHAPTER_SPECS or not normalized[3:].startswith(" "):
            continue
        columns = re.split(r"\s{2,}", normalized)
        if len(columns) < 3 or not NUMBER_RE.fullmatch(columns[-2]):
            raise RuntimeError(f"Could not parse final-account chapter {code}: {normalized}")
        totals[code] = parse_czk_thousands(columns[-2])

    missing = sorted(set(CHAPTER_SPECS) - set(totals))
    if missing:
        raise RuntimeError(f"Missing MF final-account chapter totals: {', '.join(missing)}")
    return totals


def parse_final_account(pdf_bytes: bytes, year: int) -> dict[str, object]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    # Both source tables are at the front; avoiding the remaining pages keeps
    # this lightweight and sidesteps malformed objects in some MF appendices.
    page_texts = [reader.pages[index].extract_text() or "" for index in range(min(6, len(reader.pages)))]
    table_one = next(
        (text for text in page_texts if "Tabulka č. 1:" in text and "Výdaje státního rozpočtu celkem" in text),
        None,
    )
    table_two = next(
        (text for text in page_texts if "Tabulka č. 2a:" in text and "1 Daňové příjmy" in text),
        None,
    )
    table_seven = None
    for page in reader.pages:
        text = page.extract_text() or ""
        if (
            "Tabulka č. 7:" in text
            and "Celkové výdaje státního rozpočtu podle kapitol" in text
            and "313 Ministerstvo práce a sociálních věcí" in text
        ):
            table_seven = page.extract_text(extraction_mode="layout") or ""
            break
    if table_one is None or table_two is None or table_seven is None:
        raise RuntimeError("Could not find final-account tables 1, 2a, and 7 in the MF PDF")
    if f"{year}" not in table_one:
        raise RuntimeError(f"MF final-account PDF does not appear to cover {year}")

    total_revenue = parse_actual_value(table_one, "Příjmy státního rozpočtu celkem")
    total_expenditure = parse_actual_value(table_one, "Výdaje státního rozpočtu celkem")
    revenues = {
        code: parse_actual_value(table_two, label)
        for code, label in REVENUE_LABELS.items()
    }
    revenue_class_total = sum(revenues.values())
    if revenue_class_total != total_revenue:
        raise RuntimeError(
            "MF revenue classes do not reconcile to total revenue: "
            f"{revenue_class_total} vs {total_revenue} CZK"
        )
    if total_expenditure < total_revenue:
        raise RuntimeError("A state-budget surplus needs an explicit outgoing surplus flow")

    return {
        "revenues": revenues,
        "total_revenue": total_revenue,
        "total_expenditure": total_expenditure,
        "deficit": total_expenditure - total_revenue,
        "chapters": parse_chapter_totals(table_seven),
    }


def school_allocation_total(path: Path) -> int:
    if not path.exists():
        raise FileNotFoundError(f"Missing MŠMT allocation CSV: {path}")

    columns = ("pedagogical_amount", "nonpedagogical_amount", "oniv_amount", "other_amount")
    total = 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            total += sum(int(row.get(column) or 0) for column in columns)
    if total <= 0:
        raise RuntimeError(f"MŠMT allocation total is not positive in {path}")
    return total


def download_with_retry(url: str, destination: Path, no_cache: bool) -> bytes:
    if not no_cache and destination.exists():
        print(f"  cache hit: {destination.name}", file=sys.stderr)
        return destination.read_bytes()

    request = urllib.request.Request(url, headers={"User-Agent": "cz-school-sankey-etl/1.0"})
    for attempt in range(1, 4):
        try:
            print(f"  GET {url} (attempt {attempt}/3)", file=sys.stderr)
            with urllib.request.urlopen(request, timeout=120) as response:
                data = response.read()
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
            return data
        except Exception as exc:
            if attempt == 3:
                raise
            print(f"  download failed ({exc}); retrying", file=sys.stderr)
            time.sleep(5)
    raise RuntimeError("unreachable")


def build_rows(
    aggregated: dict[str, object],
    *,
    allocation_total: int,
    source_url: str,
) -> list[dict[str, object]]:
    revenues = aggregated["revenues"]
    assert isinstance(revenues, dict)
    total_expenditure = int(aggregated["total_expenditure"])
    other_expenditure = total_expenditure - allocation_total
    if other_expenditure <= 0:
        raise RuntimeError(
            f"MŠMT allocation rollup {allocation_total} is not below state expenditure {total_expenditure}"
        )

    revenue_rows = [
        ("income:taxes", "Daňové a pojistné příjmy", int(revenues["taxes"]), "observed"),
        (
            "income:nontax",
            "Nedaňové a kapitálové příjmy",
            int(revenues["nontax"]) + int(revenues["capital"]),
            "observed",
        ),
        # Keep the established node ID for API compatibility; this now covers all received transfers.
        ("income:eu", "Přijaté transfery", int(revenues["transfers"]), "observed"),
        ("income:debt", "Financování schodku", int(aggregated["deficit"]), "inferred"),
    ]
    rows = [
        {
            "node_id": node_id,
            "node_name": node_name,
            "node_category": "other",
            "flow_type": "state_revenue",
            "amount_czk": amount,
            "basis": "realized",
            "certainty": certainty,
            "source_url": source_url,
        }
        for node_id, node_name, amount, certainty in revenue_rows
        if amount > 0
    ]
    chapters = aggregated["chapters"]
    assert isinstance(chapters, dict)
    rows.extend(
        {
            "node_id": f"chapter:{code}",
            "node_name": chapter_name,
            "node_category": "ministry",
            "flow_type": "state_chapter_total",
            "amount_czk": int(chapters[code]),
            "basis": "realized",
            "certainty": "observed",
            "source_url": source_url,
        }
        for code, chapter_name in CHAPTER_SPECS.items()
    )
    rows.extend(
        [
            {
                "node_id": "state:total",
                "node_name": "Výdaje státního rozpočtu celkem",
                "node_category": "state",
                "flow_type": "state_budget_total",
                "amount_czk": total_expenditure,
                "basis": "realized",
                "certainty": "observed",
                "source_url": source_url,
            },
            {
                "node_id": "state:other",
                "node_name": "Ostatní výdaje státního rozpočtu (zbytek)",
                "node_category": "other",
                "flow_type": "state_to_other",
                "amount_czk": other_expenditure,
                "basis": "realized",
                "certainty": "inferred",
                "source_url": source_url,
            },
        ]
    )
    return rows


def write_state_budget_csv(year: int, rows: list[dict[str, object]]) -> Path:
    output_path = RAW_ROOT / str(year) / "state_budget.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch the MF state-budget final-account summary")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--pdf", type=Path, help="Local final-account workbook G PDF")
    parser.add_argument("--allocations", type=Path, help="MŠMT allocation CSV used by the graph")
    parser.add_argument("--no-cache", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_url = SOURCE_URLS.get(args.year)
    if source_url is None:
        raise SystemExit(f"No official final-account source configured for {args.year}")

    allocation_path = args.allocations or RAW_ROOT / str(args.year) / "msmt_allocations.csv"
    if args.pdf:
        pdf_bytes = args.pdf.read_bytes()
    else:
        cache_path = CACHE_DIR / f"state-final-account-{args.year}.pdf"
        pdf_bytes = download_with_retry(source_url, cache_path, args.no_cache)

    aggregated = parse_final_account(pdf_bytes, args.year)
    allocation_total = school_allocation_total(allocation_path)
    rows = build_rows(aggregated, allocation_total=allocation_total, source_url=source_url)
    output_path = write_state_budget_csv(args.year, rows)

    print(f"State expenditure: {int(aggregated['total_expenditure']) / 1e9:.3f} bn CZK")
    print(f"MŠMT school rollup: {allocation_total / 1e9:.3f} bn CZK")
    print(f"Balancing residual: {(int(aggregated['total_expenditure']) - allocation_total) / 1e9:.3f} bn CZK")
    print(f"Wrote {output_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
