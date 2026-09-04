from __future__ import annotations

import csv
from pathlib import Path

import fetch_state_budget as etl


TABLE_ONE = """
Tabulka č. 1: Úhrnná bilance příjmů a výdajů státního rozpočtu (tis. Kč)
Příjmy státního rozpočtu celkem 1 900 000 000,00 2 000 000 000,00 2 050 000 000,00 2 081 089 369,65 98,56 105,89
Výdaje státního rozpočtu celkem 2 200 000 000,00 2 300 000 000,00 2 350 000 000,00 2 371 774 562,84 100,82 106,03
"""

TABLE_TWO = """
Tabulka č. 2a: Příjmy státního rozpočtu v druhovém třídění rozpočtové skladby (tis. Kč)
1 Daňové příjmy 1 700 000 000,00 1 800 000 000,00 1 840 000 000,00 1 864 145 305,43 101,31 24 045 488,58 146 677 862,29
2 Nedaňové příjmy 36 000 000,00 38 000 000,00 39 000 000,00 42 695 835,72 109,22 3 604 840,38 6 101 573,32
3 Kapitálové příjmy 19 000 000,00 34 000 000,00 34 300 000,00 21 120 863,18 61,47 -13 240 865,32 2 003 325,31
4 Přijaté transfery 192 000 000,00 172 000 000,00 198 000 000,00 153 127 365,32 77,34 -44 874 374,32 -39 085 385,98
"""


def test_parse_czk_thousands_preserves_reported_precision() -> None:
    assert etl.parse_czk_thousands("2 371 774 562,84") == 2_371_774_562_840


def test_parse_actual_value_uses_realized_year_column() -> None:
    assert etl.parse_actual_value(
        TABLE_ONE,
        "Výdaje státního rozpočtu celkem",
    ) == 2_371_774_562_840


def test_build_rows_balances_revenue_and_outflow() -> None:
    revenues = {
        "taxes": 1_864_145_305_430,
        "nontax": 42_695_835_720,
        "capital": 21_120_863_180,
        "transfers": 153_127_365_320,
    }
    aggregated = {
        "revenues": revenues,
        "total_revenue": sum(revenues.values()),
        "total_expenditure": 2_371_774_562_840,
        "deficit": 290_685_193_190,
        "chapters": {code: 1_000_000 for code in etl.CHAPTER_SPECS},
    }
    rows = etl.build_rows(
        aggregated,
        allocation_total=161_194_792_901,
        source_url="https://example.test/final-account.pdf",
    )

    revenue_total = sum(int(row["amount_czk"]) for row in rows if row["flow_type"] == "state_revenue")
    state_total = next(int(row["amount_czk"]) for row in rows if row["flow_type"] == "state_budget_total")
    residual = next(int(row["amount_czk"]) for row in rows if row["flow_type"] == "state_to_other")

    assert revenue_total == state_total
    assert residual + 161_194_792_901 == state_total
    assert len([row for row in rows if row["flow_type"] == "state_chapter_total"]) == 14


def test_parse_chapter_totals_uses_realized_total_column() -> None:
    lines = [
        f"{code}  {name}  1  2  3  100,00  90,0%  4  5  6  20,00  80,0%  7  8  9  {code} 120,00  95,0%"
        for code, name in etl.CHAPTER_SPECS.items()
    ]

    totals = etl.parse_chapter_totals("\n".join(lines))

    assert totals["313"] == 313_120_000
    assert totals["336"] == 336_120_000


def test_school_allocation_total(tmp_path: Path) -> None:
    path = tmp_path / "allocations.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "pedagogical_amount",
                "nonpedagogical_amount",
                "oniv_amount",
                "other_amount",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "pedagogical_amount": "100",
                "nonpedagogical_amount": "20",
                "oniv_amount": "3",
                "other_amount": "4",
            }
        )

    assert etl.school_allocation_total(path) == 127
