from __future__ import annotations

import sys
from pathlib import Path

from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_founder_recipient_evidence as evidence


def test_parse_school_budget_workbook_reads_configured_year(tmp_path: Path) -> None:
    path = tmp_path / "budget.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "2025"
    sheet.append(["School budgets"])
    sheet.append(["Název příspěvkové organizace", "Výnosy z rozpočtu MČ"])
    sheet.append(["Mateřská škola Test, příspěvková organizace", 2600])
    workbook.save(path)

    rows = evidence.parse_school_budget_workbook(path, year=2025, unit_multiplier=1000)

    assert rows == [
        {
            "source_recipient_name": "Mateřská škola Test, příspěvková organizace",
            "amount": 2_600_000,
        }
    ]


def test_match_recipients_supports_cataloged_rename() -> None:
    source_rows = [{"source_recipient_name": "Old school name", "amount": 100}]
    registry_rows = [
        {
            "institution_id": "school:1",
            "institution_name": "New school name",
        }
    ]

    matched, unmatched_source, unmatched_registry = evidence.match_recipients(
        source_rows=source_rows,
        registry_rows=registry_rows,
        aliases={evidence.normalized_name("Old school name"): "school:1"},
    )

    assert matched[0]["institution_id"] == "school:1"
    assert matched[0]["match_method"] == "catalog_alias"
    assert unmatched_source == []
    assert unmatched_registry == []


def test_match_recipients_does_not_guess_ambiguous_name() -> None:
    source_rows = [{"source_recipient_name": "Primary school", "amount": 100}]
    registry_rows = [
        {"institution_id": "school:1", "institution_name": "Primary school North"},
        {"institution_id": "school:2", "institution_name": "Primary school South"},
    ]

    matched, unmatched_source, unmatched_registry = evidence.match_recipients(
        source_rows=source_rows,
        registry_rows=registry_rows,
        aliases={},
    )

    assert matched == []
    assert len(unmatched_source) == 1
    assert len(unmatched_registry) == 2
