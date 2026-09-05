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
