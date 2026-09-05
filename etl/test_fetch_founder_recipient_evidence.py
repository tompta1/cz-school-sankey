from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

from openpyxl import Workbook
from pypdf import PdfWriter

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


def test_match_recipients_prefers_exact_ico() -> None:
    source_rows = [
        {
            "source_recipient_ico": "00000001",
            "source_recipient_name": "Former school name",
            "amount": 100,
        }
    ]
    registry_rows = [
        {
            "institution_id": "school:00000001",
            "institution_name": "Current school name",
        }
    ]

    matched, unmatched_source, unmatched_registry = evidence.match_recipients(
        source_rows=source_rows,
        registry_rows=registry_rows,
        aliases={},
    )

    assert matched[0]["institution_id"] == "school:00000001"
    assert matched[0]["match_method"] == "ico_exact"
    assert unmatched_source == []
    assert unmatched_registry == []


def test_parse_regional_operating_budget_pdf_requires_table(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "budget.pdf"
    PdfWriter().write(path)

    class Page:
        def extract_text(self, extraction_mode: str) -> str:
            assert extraction_mode == "layout"
            return """Tabulka č. 6: Závazné ukazatele pro příspěvkové organizace v odvětví školství
  00000001  School One, příspěvková organizace                           1 234
  00000002  School Two, příspěvková organizace                              56
Celkem                                                                  1 290
"""

    class Reader:
        pages = [Page()]

    monkeypatch.setattr(evidence, "PdfReader", lambda _: Reader())

    rows = evidence.parse_regional_operating_budget_pdf(path, unit_multiplier=1000)

    assert rows == [
        {
            "source_recipient_ico": "00000001",
            "source_recipient_name": "School One, příspěvková organizace",
            "amount": 1_234_000,
        },
        {
            "source_recipient_ico": "00000002",
            "source_recipient_name": "School Two, příspěvková organizace",
            "amount": 56_000,
        },
    ]


def test_parse_prague_founder_budget_keeps_omitted_component_distinct_from_zero(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class Sheet:
        rows = [
            ["title", "", "", "", ""],
            ["header", "", "", "", "", "Rozpočet r. 2025"],
            ["D1", "", "School One", "0091651", "", 130],
            ["D2", "", "", "", "000000091 - founder", 30],
            ["D2", "", "", "", "000033353 - direct", 100],
            ["D1", "", "Art School", "0091651", "", 80],
            ["D2", "", "", "", "000033353 - direct", 80],
        ]
        nrows = len(rows)

        def row_values(self, row_number: int) -> list[object]:
            return self.rows[row_number]

    class Workbook:
        def sheet_by_name(self, name: str) -> Sheet:
            assert name == "04"
            return Sheet()

    monkeypatch.setattr(evidence, "open_xls_workbook", lambda _: Workbook())
    monkeypatch.setattr(
        evidence,
        "parse_prague_direct_identity_workbook",
        lambda path, unit_multiplier: [
            {
                "source_recipient_ico": "00000001",
                "source_recipient_name": "School One",
                "direct_amount": 100_000,
            },
            {
                "source_recipient_ico": "00000002",
                "source_recipient_name": "Art School",
                "direct_amount": 80_000,
            },
        ],
    )

    rows = evidence.parse_prague_founder_budget(
        tmp_path / "budget.xls",
        identity_path=tmp_path / "identities.xlsx",
        year=2025,
        unit_multiplier=1000,
    )

    assert rows == [
        {
            "source_recipient_ico": "00000001",
            "source_recipient_name": "School One",
            "source_budget_name": "School One",
            "amount": 30_000,
        },
        {
            "source_recipient_ico": "00000002",
            "source_recipient_name": "Art School",
            "source_budget_name": "Art School",
            "amount": None,
        },
    ]


def test_build_year_skips_omitted_component_after_complete_match(
    monkeypatch,
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    year_dir = raw_root / "2025"
    year_dir.mkdir(parents=True)
    with (year_dir / "school_entities.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["institution_id", "institution_name", "founder_id"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "institution_id": "school:00000001",
                    "institution_name": "School One",
                    "founder_id": "founder:00064581",
                },
                {
                    "institution_id": "school:00000002",
                    "institution_name": "Art School",
                    "founder_id": "founder:00064581",
                },
            ]
        )

    catalog = tmp_path / "catalog.csv"
    with catalog.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "reporting_year",
                "founder_id",
                "founder_name",
                "document_kind",
                "basis",
                "source_format",
                "parser_profile",
                "unit_multiplier",
                "source_url",
                "identity_source_url",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "reporting_year": "2025",
                "founder_id": "founder:00064581",
                "founder_name": "Hlavní město Praha",
                "document_kind": "approved_city_school_budget",
                "basis": "budgeted",
                "source_format": "xls",
                "parser_profile": "prague_education_budget_components",
                "unit_multiplier": "1000",
                "source_url": "https://example.test/budget.xls",
                "identity_source_url": "https://example.test/identity.xlsx",
                "notes": "Test source.",
            }
        )

    aliases = tmp_path / "aliases.csv"
    aliases.write_text("reporting_year,founder_id,source_recipient_name,institution_id\n")
    monkeypatch.setattr(evidence, "ROOT", tmp_path)
    monkeypatch.setattr(evidence, "RAW_ROOT", raw_root)
    monkeypatch.setattr(evidence, "download_source", lambda *args, **kwargs: tmp_path / "source")
    monkeypatch.setattr(
        evidence,
        "parse_prague_founder_budget",
        lambda *args, **kwargs: [
            {
                "source_recipient_ico": "00000001",
                "source_recipient_name": "School One",
                "amount": 30_000,
            },
            {
                "source_recipient_ico": "00000002",
                "source_recipient_name": "Art School",
                "amount": None,
            },
        ],
    )

    evidence.build_year(2025, catalog, aliases, no_cache=False)

    rows = list(csv.DictReader((year_dir / "founder_recipient_evidence.csv").open()))
    metadata = json.loads((year_dir / "founder_recipient_evidence.meta.json").read_text())
    assert [row["institution_id"] for row in rows] == ["school:00000001"]
    assert metadata["sources"][0]["matched_school_count"] == 2
    assert metadata["sources"][0]["evidence_row_count"] == 1


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
