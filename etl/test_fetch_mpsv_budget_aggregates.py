from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "social"))

import fetch_mpsv_budget_aggregates as mpsv


def test_parse_metric_amount_selects_current_year_actual() -> None:
    page_text = (
        "Výdaje celkem 949 524 930,50 968 754 450,34 981 598 881,05 "
        "985 110 786,15 976 700 807,64 2 472 999,70 99,5 99,1"
    )

    assert mpsv.parse_metric_amount(page_text, "Výdaje celkem") == 976_700_807_640


def test_parse_metric_amount_accepts_wrapped_label() -> None:
    page_text = (
        "Transfery na podporu reprodukce majetku nestátním subjektům\n"
        "v sociální oblasti 899 104,98 5 167 094,74 5 167 094,74 "
        "6 010 795,98 3 231 685,18 386 021,22 62,5 53,8"
    )

    assert (
        mpsv.parse_metric_amount(
            page_text,
            "Transfery na podporu reprodukce majetku nestátním subjektům v sociální oblasti",
        )
        == 3_231_685_180
    )


def test_build_rows_keeps_year_specific_source() -> None:
    lines = []
    for index, (_, _, metric_name) in enumerate(mpsv.METRIC_SPECS, start=1):
        lines.append(f"{metric_name} 10,00 20,00 30,00 40,00 {index + 10},00 100,0 100,0")

    rows = mpsv.build_rows("\n".join(lines), 2025, mpsv.SOURCE_URLS[2025])

    assert len(rows) == len(mpsv.METRIC_SPECS)
    assert {row["reporting_year"] for row in rows} == {2025}
    assert {row["source_url"] for row in rows} == {mpsv.SOURCE_URLS[2025]}
    assert rows[-1]["amount_czk"] == (len(mpsv.METRIC_SPECS) + 10) * 1000
