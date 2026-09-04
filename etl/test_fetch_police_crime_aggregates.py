from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent / "mv"))

import fetch_police_crime_aggregates as police


def test_parse_count_accepts_decimal_form_integer() -> None:
    assert police.parse_count("170051.0") == 170_051


def test_parse_count_rejects_fractional_value() -> None:
    with pytest.raises(RuntimeError, match="not an integer"):
        police.parse_count("1.5")
