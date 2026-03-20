"""Tests for etl/fetch_founder_budgets.py.

Run with:  python3 -m pytest etl/test_fetch_founder_budgets.py -v
"""

from __future__ import annotations

import csv
import io
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_founder_budgets as fb


# ---------------------------------------------------------------------------
# normalize_ico
# ---------------------------------------------------------------------------

class TestNormalizeIco:
    def test_pads_to_8_digits(self):
        assert fb.normalize_ico("12345") == "00012345"

    def test_already_8_digits(self):
        assert fb.normalize_ico("12345678") == "12345678"

    def test_strips_decimal(self):
        assert fb.normalize_ico("12345.0") == "00012345"

    def test_empty_returns_empty(self):
        assert fb.normalize_ico("") == ""

    def test_none_returns_empty(self):
        assert fb.normalize_ico(None) == ""


# ---------------------------------------------------------------------------
# normalize_code
# ---------------------------------------------------------------------------

class TestNormalizeCode:
    def test_strips_whitespace(self):
        assert fb.normalize_code("  5331  ") == "5331"

    def test_removes_dots(self):
        assert fb.normalize_code("3.100") == "3100"

    def test_removes_spaces(self):
        assert fb.normalize_code("3 100") == "3100"

    def test_empty(self):
        assert fb.normalize_code("") == ""


# ---------------------------------------------------------------------------
# to_int
# ---------------------------------------------------------------------------

class TestToInt:
    def test_plain_integer(self):
        assert fb.to_int("1000000") == 1_000_000

    def test_nbsp_thousands_separator(self):
        assert fb.to_int("1\xa0200\xa0000") == 1_200_000

    def test_space_thousands(self):
        assert fb.to_int("1 200 000") == 1_200_000

    def test_comma_decimal(self):
        assert fb.to_int("1000000,50") == 1_000_000

    def test_empty_returns_zero(self):
        assert fb.to_int("") == 0

    def test_non_numeric_returns_zero(self):
        assert fb.to_int("abc") == 0


# ---------------------------------------------------------------------------
# find_col
# ---------------------------------------------------------------------------

class TestFindCol:
    def test_exact_match(self):
        assert fb.find_col(["ZC_ICO", "ZU_ROZKZ"], ["ZC_ICO"]) == "ZC_ICO"

    def test_case_insensitive(self):
        assert fb.find_col(["zc_ico", "ZU_ROZKZ"], ["ZC_ICO"]) == "zc_ico"

    def test_first_candidate_wins(self):
        assert fb.find_col(["ico", "ZC_ICO"], ["ZC_ICO", "ico"]) == "ZC_ICO"

    def test_no_match_returns_none(self):
        assert fb.find_col(["a", "b"], ["c", "d"]) is None


# ---------------------------------------------------------------------------
# prorate_founder_to_schools
# ---------------------------------------------------------------------------

class TestProrateFounderToSchools:
    def _school(self, inst_id: str) -> dict:
        return {"institution_id": inst_id, "ico": ""}

    def test_single_school_gets_full_amount(self):
        schools = [self._school("school:a")]
        msmt = {"school:a": 5_000_000}
        rows = fb.prorate_founder_to_schools("f:1", 1_000_000, schools, msmt)
        assert len(rows) == 1
        assert rows[0]["amount"] == 1_000_000

    def test_two_equal_weight_schools_split_evenly(self):
        schools = [self._school("school:a"), self._school("school:b")]
        msmt = {"school:a": 1_000_000, "school:b": 1_000_000}
        rows = fb.prorate_founder_to_schools("f:1", 1_000_000, schools, msmt)
        amounts = sorted(r["amount"] for r in rows)
        assert amounts == [500_000, 500_000]

    def test_last_school_absorbs_rounding(self):
        schools = [self._school("school:a"), self._school("school:b"), self._school("school:c")]
        msmt = {"school:a": 1, "school:b": 1, "school:c": 1}
        total = 100
        rows = fb.prorate_founder_to_schools("f:1", total, schools, msmt)
        assert sum(r["amount"] for r in rows) == total

    def test_no_msmt_weight_falls_back_to_equal_split(self):
        schools = [self._school("school:a"), self._school("school:b")]
        rows = fb.prorate_founder_to_schools("f:1", 1_000, schools, {})
        assert len(rows) == 2
        assert all(r["amount"] == 500 for r in rows)

    def test_certainty_is_inferred(self):
        schools = [self._school("school:a")]
        rows = fb.prorate_founder_to_schools("f:1", 500, schools, {"school:a": 1})
        assert rows[0]["certainty"] == "inferred"

    def test_basis_is_realized(self):
        schools = [self._school("school:a")]
        rows = fb.prorate_founder_to_schools("f:1", 500, schools, {"school:a": 1})
        assert rows[0]["basis"] == "realized"

    def test_empty_school_list_returns_empty(self):
        rows = fb.prorate_founder_to_schools("f:1", 1_000, [], {})
        assert rows == []


# ---------------------------------------------------------------------------
# run_po_pass (VYKZZ)
# ---------------------------------------------------------------------------

def _make_zip_with_csv(csv_content: str, csv_name: str = "VYKZZ.csv") -> Path:
    """Write a ZIP file to a BytesIO and return it as a Path via tmp_path workaround."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(csv_name, csv_content)
    return buf.getvalue()


def _zip_path(tmp_path: Path, csv_content: str, zip_name: str = "test.zip", csv_name: str = "VYKZZ.csv") -> Path:
    p = tmp_path / zip_name
    p.write_bytes(_make_zip_with_csv(csv_content, csv_name))
    return p


class TestRunPoPass:
    def _vykzz_csv(self, rows: list[dict]) -> str:
        """Build a semicolon-delimited VYKZZ-style CSV with SAP BW header."""
        fields = ["ZC_ICO", "ZC_SYNUC", "ZU_HLCIN"]
        header = ";".join(f'"Label"{f}:{f}' for f in fields)
        lines = [header]
        for row in rows:
            lines.append(";".join(str(row.get(f, "")) for f in fields))
        return "\n".join(lines)

    def test_extracts_account_672(self, tmp_path):
        csv_content = self._vykzz_csv([
            {"ZC_ICO": "11111111", "ZC_SYNUC": "672", "ZU_HLCIN": "1000000"},
        ])
        zip_path = _zip_path(tmp_path, csv_content)
        school_icos = {"11111111": {"institution_id": "school:test"}}
        result = fb.run_po_pass(zip_path, school_icos, list_columns=False)
        assert result == {"11111111": 1_000_000}

    def test_extracts_account_673(self, tmp_path):
        csv_content = self._vykzz_csv([
            {"ZC_ICO": "22222222", "ZC_SYNUC": "673", "ZU_HLCIN": "500000"},
        ])
        zip_path = _zip_path(tmp_path, csv_content)
        school_icos = {"22222222": {"institution_id": "school:test2"}}
        result = fb.run_po_pass(zip_path, school_icos, list_columns=False)
        assert result == {"22222222": 500_000}

    def test_sums_multiple_account_rows(self, tmp_path):
        csv_content = self._vykzz_csv([
            {"ZC_ICO": "11111111", "ZC_SYNUC": "672", "ZU_HLCIN": "1000000"},
            {"ZC_ICO": "11111111", "ZC_SYNUC": "673", "ZU_HLCIN": "200000"},
        ])
        zip_path = _zip_path(tmp_path, csv_content)
        school_icos = {"11111111": {"institution_id": "school:test"}}
        result = fb.run_po_pass(zip_path, school_icos, list_columns=False)
        assert result == {"11111111": 1_200_000}

    def test_ignores_non_school_icos(self, tmp_path):
        csv_content = self._vykzz_csv([
            {"ZC_ICO": "99999999", "ZC_SYNUC": "672", "ZU_HLCIN": "999"},
        ])
        zip_path = _zip_path(tmp_path, csv_content)
        school_icos = {"11111111": {"institution_id": "school:test"}}
        result = fb.run_po_pass(zip_path, school_icos, list_columns=False)
        assert result == {}

    def test_ignores_non_founder_accounts(self, tmp_path):
        csv_content = self._vykzz_csv([
            {"ZC_ICO": "11111111", "ZC_SYNUC": "501", "ZU_HLCIN": "999"},
        ])
        zip_path = _zip_path(tmp_path, csv_content)
        school_icos = {"11111111": {"institution_id": "school:test"}}
        result = fb.run_po_pass(zip_path, school_icos, list_columns=False)
        assert result == {}

    def test_skips_zero_or_negative_amounts(self, tmp_path):
        csv_content = self._vykzz_csv([
            {"ZC_ICO": "11111111", "ZC_SYNUC": "672", "ZU_HLCIN": "0"},
            {"ZC_ICO": "11111111", "ZC_SYNUC": "672", "ZU_HLCIN": "-100"},
        ])
        zip_path = _zip_path(tmp_path, csv_content)
        school_icos = {"11111111": {"institution_id": "school:test"}}
        result = fb.run_po_pass(zip_path, school_icos, list_columns=False)
        assert result == {}


# ---------------------------------------------------------------------------
# run_12m_pass (FIN 2-12 M)
# ---------------------------------------------------------------------------

class TestRun12mPass:
    def _finm_csv(self, rows: list[dict]) -> str:
        fields = ["ZC_ICO", "0FUNC_AREA", "ZCMMT_ITM", "ZU_ROZKZ"]
        header = ";".join(f'"Label"{f}:{f}' for f in fields)
        lines = [header]
        for row in rows:
            lines.append(";".join(str(row.get(f, "")) for f in fields))
        return "\n".join(lines)

    def test_matches_education_para_and_transfer_item(self, tmp_path):
        csv_content = self._finm_csv([
            {"ZC_ICO": "11111111", "0FUNC_AREA": "3111", "ZCMMT_ITM": "5331", "ZU_ROZKZ": "5000000"},
        ])
        zip_path = _zip_path(tmp_path, csv_content, csv_name="FINM201.csv")
        result = fb.run_12m_pass(zip_path, {"11111111"}, list_columns=False)
        assert result == {"11111111": 5_000_000}

    def test_sums_multiple_matching_rows(self, tmp_path):
        csv_content = self._finm_csv([
            {"ZC_ICO": "11111111", "0FUNC_AREA": "3111", "ZCMMT_ITM": "5331", "ZU_ROZKZ": "3000000"},
            {"ZC_ICO": "11111111", "0FUNC_AREA": "3200", "ZCMMT_ITM": "6351", "ZU_ROZKZ": "1000000"},
        ])
        zip_path = _zip_path(tmp_path, csv_content, csv_name="FINM201.csv")
        result = fb.run_12m_pass(zip_path, {"11111111"}, list_columns=False)
        assert result == {"11111111": 4_000_000}

    def test_ignores_non_education_paragraphs(self, tmp_path):
        csv_content = self._finm_csv([
            {"ZC_ICO": "11111111", "0FUNC_AREA": "4100", "ZCMMT_ITM": "5331", "ZU_ROZKZ": "999"},
        ])
        zip_path = _zip_path(tmp_path, csv_content, csv_name="FINM201.csv")
        result = fb.run_12m_pass(zip_path, {"11111111"}, list_columns=False)
        assert result == {}

    def test_ignores_non_transfer_items(self, tmp_path):
        csv_content = self._finm_csv([
            {"ZC_ICO": "11111111", "0FUNC_AREA": "3111", "ZCMMT_ITM": "5139", "ZU_ROZKZ": "999"},
        ])
        zip_path = _zip_path(tmp_path, csv_content, csv_name="FINM201.csv")
        result = fb.run_12m_pass(zip_path, {"11111111"}, list_columns=False)
        assert result == {}

    def test_ignores_founders_not_in_set(self, tmp_path):
        csv_content = self._finm_csv([
            {"ZC_ICO": "99999999", "0FUNC_AREA": "3111", "ZCMMT_ITM": "5331", "ZU_ROZKZ": "999"},
        ])
        zip_path = _zip_path(tmp_path, csv_content, csv_name="FINM201.csv")
        result = fb.run_12m_pass(zip_path, {"11111111"}, list_columns=False)
        assert result == {}

    def test_education_para_boundary_inclusive(self, tmp_path):
        csv_content = self._finm_csv([
            {"ZC_ICO": "11111111", "0FUNC_AREA": "3100", "ZCMMT_ITM": "5331", "ZU_ROZKZ": "100"},
            {"ZC_ICO": "11111111", "0FUNC_AREA": "3299", "ZCMMT_ITM": "5331", "ZU_ROZKZ": "200"},
        ])
        zip_path = _zip_path(tmp_path, csv_content, csv_name="FINM201.csv")
        result = fb.run_12m_pass(zip_path, {"11111111"}, list_columns=False)
        assert result == {"11111111": 300}


# ---------------------------------------------------------------------------
# write_founder_support
# ---------------------------------------------------------------------------

class TestWriteFounderSupport:
    def test_writes_expected_csv(self, tmp_path):
        rows = [
            {"institution_id": "school:a", "amount": 1_000_000,
             "basis": "realized", "certainty": "observed", "note": "test"},
        ]
        with patch.object(fb, "RAW_ROOT", tmp_path):
            (tmp_path / "2025").mkdir()
            out = fb.write_founder_support(2025, rows)

        assert out.exists()
        written = list(csv.DictReader(out.open()))
        assert len(written) == 1
        assert written[0]["institution_id"] == "school:a"
        assert written[0]["amount"] == "1000000"
        assert written[0]["certainty"] == "observed"

    def test_creates_expected_fieldnames(self, tmp_path):
        with patch.object(fb, "RAW_ROOT", tmp_path):
            (tmp_path / "2025").mkdir()
            out = fb.write_founder_support(2025, [
                {"institution_id": "s:a", "amount": 1, "basis": "b", "certainty": "c", "note": "n"},
            ])
        with out.open() as fh:
            reader = csv.DictReader(fh)
            assert reader.fieldnames == ["institution_id", "amount", "basis", "certainty", "note"]
