import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent / "social"))
import fetch_recipient_metrics as metrics


def test_2025_unemployment_matches_recipient_count_not_registered_total():
    text = "Počet uchazečů o zaměstnání, kteří měli ke konci prosince 2025 nárok na podporu\n v nezaměstnanosti, dosáhl cca 90,2 tis., z celkového počtu 332,9 tis."
    assert metrics.parse_unemployment_supported_year_end_count(text, 2025) == 90200
    with pytest.raises(RuntimeError):
        metrics.parse_unemployment_supported_year_end_count(text, 2024)


def test_paid_count_ignores_prose_and_uses_current_year_column():
    text = "Příspěvek na péči byl zvýšen. Jiná tabulka 1,0 2,0 3,0\nPříspěvek na péči 375,4 382,4 101,9 3 803,0 3 929,9\nNáhradní výživné 5) 12,5 13,0 104,0 23,7"
    assert metrics.parse_paid_count_tis(text, "Příspěvek na péči") == 382400
    assert metrics.parse_paid_count_tis(text, "Náhradní výživné") == 13000
    with pytest.raises(RuntimeError):
        metrics.parse_paid_count_tis("Příspěvek na péči bez tabulky\nJiné 1,0 2,0 3,0", "Příspěvek na péči")


def test_build_2025_has_all_four_metrics(monkeypatch):
    monkeypatch.setattr(metrics, "load_pension_rows", lambda: [])
    monkeypatch.setattr(metrics, "pension_recipient_count", lambda rows, year: 2829884)
    monkeypatch.setattr(metrics, "load_mpsv_benefits_pdf_text", lambda year: (
        "Počet uchazečů o zaměstnání, ke konci prosince 2025 dosáhl cca 90,2 tis.\n"
        "Příspěvek na péči 375,4 382,4 101,9\nNáhradní výživné 5) 12,5 13,0 104,0", "https://example.test/2025.pdf"))
    rows = metrics.build_rows([2025])
    assert len(rows) == 4
    assert {row["reporting_year"] for row in rows} == {2025}
    assert [row["recipient_count"] for row in rows] == [2829884, 90200, 382400, 13000]
