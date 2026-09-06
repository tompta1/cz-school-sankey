import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "justice"))
import fetch_activity_aggregates as justice


def test_2025_final_account_completed_court_totals():
    text = (
        "Okresní soudy 2021 2022 2023 2024 2025 "
        "Počet vyřízených věcí 1 972 214 1 999 869 2 080 331 2 026 138 2 067 110 "
        "Výdaje celkem 1 2 3 4 5\n"
        "Krajské soudy 2021 2022 2023 2024 2025 "
        "Počet vyřízených věcí 253 967 257 215 262 512 267 441 226 500 "
        "Výdaje celkem 1 2 3 4 5"
    )

    assert justice.disposed_total_from_2025_pdf(text, "Okresní soudy") == 2_067_110
    assert justice.disposed_total_from_2025_pdf(text, "Krajské soudy") == 226_500
