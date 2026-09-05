from __future__ import annotations

import csv
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import apply_founder_recipient_evidence as apply_evidence


def test_load_evidence_requires_rows(tmp_path: Path) -> None:
    year_dir = tmp_path / "2025"
    year_dir.mkdir()
    path = year_dir / "founder_recipient_evidence.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        csv.DictWriter(handle, fieldnames=["institution_id"]).writeheader()

    with patch.object(apply_evidence, "RAW_ROOT", tmp_path):
        with pytest.raises(RuntimeError, match="No evidence rows"):
            apply_evidence.load_evidence(2025)


def test_load_evidence_reads_compact_snapshot(tmp_path: Path) -> None:
    year_dir = tmp_path / "2025"
    year_dir.mkdir()
    path = year_dir / "founder_recipient_evidence.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["institution_id", "amount"])
        writer.writeheader()
        writer.writerow({"institution_id": "school:1", "amount": "2600000"})

    with patch.object(apply_evidence, "RAW_ROOT", tmp_path):
        rows = apply_evidence.load_evidence(2025)

    assert rows == [{"institution_id": "school:1", "amount": "2600000"}]


def test_apply_year_accepts_source_backed_zero() -> None:
    class Cursor:
        rowcount = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def execute(self, query, params) -> None:
            assert params[0] == 0
            self.rowcount = 1

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    evidence = [
        {
            "institution_id": "school:1",
            "founder_id": "founder:12345678",
            "amount": "0",
            "basis": "budgeted",
            "certainty": "observed",
            "attribution_method": "recipient_reported_founder_budget",
            "source_document_kind": "approved_city_school_budget",
            "source_url": "https://example.test/budget.xls",
            "note": "Explicit zero operating transfer",
        }
    ]

    assert apply_evidence.apply_year(Connection(), 2025, evidence) == (1, 1)
