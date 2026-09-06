import importlib.util
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent / "justice"))
spec = importlib.util.spec_from_file_location("justice_budget", Path(__file__).parent / "justice/fetch_budget_aggregates.py")
budget = importlib.util.module_from_spec(spec)
spec.loader.exec_module(budget)


def test_realized_column_and_category_reconciliation():
    labels = [
        ("3 Sl užby pro fyzi cké", "4 012,84"),
        ("4 Soci á l ní věci", "1 716 037,77"),
        ("5 Bezpečnos t s tá tu", "38 929 014,00"),
        ("6 Vš eobecná veřejná", "268 771,83"),
        ("542 Soudni ctví", "19 528 959,90"),
        ("543 Stá tní za s tupi tel s tví", "4 431 458,87"),
        ("544 Vězeňs tví", "12 236 879,59"),
        ("545 Proba ční", "474 706,70"),
        ("546 Sprá va v obl a s ti prá vní ochra ny", "2 147 725,45"),
        ("548 Výzkum v obl a s ti prá vní ochra ny", "6 721,95"),
        ("549 Os ta tní zá l eži tos ti prá vní ochra ny", "96 484,24"),
    ]
    page = "Tabulka č. 3d\n12.2021 - 12.2025\n" + "\n".join(
        f"{label} 1,00 2,00 3,00 4,00 {value} 110,0" for label, value in labels
    )
    rows = budget.build_rows_2025([page], "https://example.test/final-account")
    assert rows[0]["amount_czk"] == 40_917_836_440
    assert rows[1]["amount_czk"] == 19_528_959_900
    assert sum(row["amount_czk"] for row in rows[1:]) == rows[0]["amount_czk"]
    assert all(row["basis"] == "realized" and row["reporting_year"] == 2025 for row in rows)
    with pytest.raises(RuntimeError):
        budget.build_rows_2025([page.replace("12.2021 - 12.2025", "12.2020 - 12.2024")], "source")
    with pytest.raises(RuntimeError):
        budget.build_rows_2025([page, page], "source")
