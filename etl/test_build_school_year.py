"""Tests for etl/build_school_year.py.

Run with:  python3 -m pytest etl/test_build_school_year.py -v
"""

from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure the etl package is importable when run from the project root.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_school_year as etl


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(tmp_path: Path, filename: str, rows: list[dict]) -> Path:
    path = tmp_path / filename
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return path


def _minimal_raw_dir(tmp_path: Path, year: int = 2025) -> Path:
    raw_dir = tmp_path / "raw" / str(year)
    raw_dir.mkdir(parents=True)

    _write_csv(raw_dir, "school_entities.csv", [
        {
            "institution_id": "school:test-1",
            "institution_name": "Test School One",
            "ico": "11111111",
            "founder_id": "obec:test",
            "founder_name": "Test Obec",
            "founder_type": "obec",
            "municipality": "Test Town",
            "region": "Test Region",
        }
    ])

    _write_csv(raw_dir, "msmt_allocations.csv", [
        {
            "institution_id": "school:test-1",
            "pedagogical_amount": "5000000",
            "nonpedagogical_amount": "1000000",
            "oniv_amount": "500000",
            "other_amount": "200000",
            "operations_amount": "800000",
            "investment_amount": "300000",
            "bucket_basis": "budgeted",
            "bucket_certainty": "observed",
        }
    ])

    _write_csv(raw_dir, "eu_projects.csv", [
        {
            "institution_id": "school:test-1",
            "programme": "OP JAK",
            "project_name": "Test Project",
            "amount": "1000000",
            "basis": "allocated",
            "certainty": "observed",
        }
    ])

    _write_csv(raw_dir, "founder_support.csv", [
        {
            "institution_id": "school:test-1",
            "amount": "900000",
            "basis": "budgeted",
            "certainty": "inferred",
            "note": "Test note",
        }
    ])

    return raw_dir


# ---------------------------------------------------------------------------
# row_amount
# ---------------------------------------------------------------------------

class TestRowAmount:
    def test_plain_integer(self):
        assert etl.row_amount({"v": "1000000"}, "v") == 1_000_000

    def test_space_separated_thousands(self):
        assert etl.row_amount({"v": "1 200 000"}, "v") == 1_200_000

    def test_comma_decimal(self):
        # Python uses banker's rounding: round(x.5) rounds to nearest even.
        # 1000000.50 → 1000000 (even); 1000001.50 → 1000002 (even).
        assert etl.row_amount({"v": "1000000,50"}, "v") == 1_000_000
        assert etl.row_amount({"v": "1000001,50"}, "v") == 1_000_002

    def test_missing_key_returns_zero(self):
        assert etl.row_amount({}, "missing") == 0

    def test_empty_string_returns_zero(self):
        assert etl.row_amount({"v": ""}, "v") == 0


# ---------------------------------------------------------------------------
# read_csv_rows
# ---------------------------------------------------------------------------

class TestReadCsvRows:
    def test_returns_list_of_dicts(self, tmp_path):
        path = _write_csv(tmp_path, "t.csv", [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}])
        rows = etl.read_csv_rows(path)
        assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]

    def test_handles_utf8_bom(self, tmp_path):
        path = tmp_path / "bom.csv"
        path.write_bytes(b"\xef\xbb\xbfname,value\nhello,world\n")
        rows = etl.read_csv_rows(path)
        assert rows[0]["name"] == "hello"


# ---------------------------------------------------------------------------
# read_institutions
# ---------------------------------------------------------------------------

class TestReadInstitutions:
    def test_parses_all_fields(self, tmp_path):
        raw_dir = _minimal_raw_dir(tmp_path)
        with (
            patch.object(etl, "RAW_ROOT", tmp_path / "raw"),
        ):
            institutions = etl.read_institutions(2025)

        assert len(institutions) == 1
        inst = institutions[0]
        assert inst.institution_id == "school:test-1"
        assert inst.name == "Test School One"
        assert inst.ico == "11111111"
        assert inst.founder_id == "obec:test"
        assert inst.founder_type == "obec"

    def test_missing_optional_fields_become_none(self, tmp_path):
        raw_dir = tmp_path / "raw" / "2025"
        raw_dir.mkdir(parents=True)
        _write_csv(raw_dir, "school_entities.csv", [
            {"institution_id": "s:x", "institution_name": "Minimal School",
             "ico": "", "founder_id": "", "founder_name": "", "founder_type": "",
             "municipality": "", "region": ""}
        ])
        with patch.object(etl, "RAW_ROOT", tmp_path / "raw"):
            institutions = etl.read_institutions(2025)

        inst = institutions[0]
        assert inst.ico is None
        assert inst.founder_id is None

    def test_missing_file_raises(self, tmp_path):
        with (
            patch.object(etl, "RAW_ROOT", tmp_path / "raw"),
            pytest.raises(FileNotFoundError),
        ):
            etl.read_institutions(2025)


# ---------------------------------------------------------------------------
# build_from_csv — node and link consistency
# ---------------------------------------------------------------------------

class TestBuildFromCsv:
    def _build(self, tmp_path: Path) -> dict:
        _minimal_raw_dir(tmp_path)
        with (
            patch.object(etl, "RAW_ROOT", tmp_path / "raw"),
        ):
            return etl.build_from_csv(2025)

    def test_all_link_sources_have_matching_node(self, tmp_path):
        dataset = self._build(tmp_path)
        node_ids = {n["id"] for n in dataset["nodes"]}
        for link in dataset["links"]:
            assert link["source"] in node_ids, (
                f"link source '{link['source']}' has no matching node"
            )

    def test_all_link_targets_have_matching_node(self, tmp_path):
        dataset = self._build(tmp_path)
        node_ids = {n["id"] for n in dataset["nodes"]}
        for link in dataset["links"]:
            assert link["target"] in node_ids, (
                f"link target '{link['target']}' has no matching node"
            )

    def test_msmt_allocation_total_matches_allocation_link(self, tmp_path):
        dataset = self._build(tmp_path)
        # The link from msmt → school should equal ped + nonped + oniv + other
        msmt_link = next(
            lk for lk in dataset["links"]
            if lk["source"] == "msmt" and lk["target"] == "school:test-1"
        )
        expected = 5_000_000 + 1_000_000 + 500_000 + 200_000
        assert msmt_link["value"] == expected

    def test_other_amount_has_bucket_link(self, tmp_path):
        dataset = self._build(tmp_path)
        other_links = [
            lk for lk in dataset["links"]
            if lk["source"] == "school:test-1" and lk["target"] == "bucket:other"
        ]
        assert len(other_links) == 1
        assert other_links[0]["value"] == 200_000

    def test_eu_project_nodes_created(self, tmp_path):
        dataset = self._build(tmp_path)
        node_ids = {n["id"] for n in dataset["nodes"]}
        assert "eu:op-jak" in node_ids
        assert any("test-project" in nid for nid in node_ids)

    def test_founder_link_marked_inferred(self, tmp_path):
        dataset = self._build(tmp_path)
        founder_links = [
            lk for lk in dataset["links"] if lk["flowType"] == "founder_support"
        ]
        assert len(founder_links) == 1
        assert founder_links[0]["certainty"] == "inferred"
        assert founder_links[0]["value"] == 900_000

    def test_state_to_msmt_link_is_rollup_of_school_allocations(self, tmp_path):
        dataset = self._build(tmp_path)
        state_link = next(lk for lk in dataset["links"] if lk["source"] == "state:cr")
        msmt_links = [lk for lk in dataset["links"] if lk["source"] == "msmt"]
        assert state_link["value"] == sum(lk["value"] for lk in msmt_links)

    def test_no_duplicate_node_ids(self, tmp_path):
        dataset = self._build(tmp_path)
        ids = [n["id"] for n in dataset["nodes"]]
        assert len(ids) == len(set(ids)), "duplicate node IDs found"

    def test_year_field_present_on_all_links(self, tmp_path):
        dataset = self._build(tmp_path)
        for link in dataset["links"]:
            assert link["year"] == 2025

    def test_bucket_node_ids_are_consistent(self, tmp_path):
        dataset = self._build(tmp_path)
        bucket_node_ids = {n["id"] for n in dataset["nodes"] if n["category"] == "cost_bucket"}
        bucket_link_targets = {
            lk["target"] for lk in dataset["links"] if lk["flowType"] == "school_expenditure"
        }
        assert bucket_link_targets.issubset(bucket_node_ids), (
            "school_expenditure links reference bucket IDs not in node list"
        )


# ---------------------------------------------------------------------------
# build_from_csv — missing optional CSV files
# ---------------------------------------------------------------------------

class TestBuildFromCsvMissingOptionalFiles:
    def test_missing_eu_csv_is_allowed(self, tmp_path):
        raw_dir = _minimal_raw_dir(tmp_path)
        (raw_dir / "eu_projects.csv").unlink()
        with patch.object(etl, "RAW_ROOT", tmp_path / "raw"):
            dataset = etl.build_from_csv(2025)
        eu_links = [lk for lk in dataset["links"] if lk["flowType"] == "eu_project_support"]
        assert eu_links == []

    def test_missing_founder_csv_is_allowed(self, tmp_path):
        raw_dir = _minimal_raw_dir(tmp_path)
        (raw_dir / "founder_support.csv").unlink()
        with patch.object(etl, "RAW_ROOT", tmp_path / "raw"):
            dataset = etl.build_from_csv(2025)
        founder_links = [lk for lk in dataset["links"] if lk["flowType"] == "founder_support"]
        assert founder_links == []


# ---------------------------------------------------------------------------
# write_dataset and update_manifest
# ---------------------------------------------------------------------------

class TestWriteDataset:
    def test_writes_valid_json(self, tmp_path):
        dataset = {"year": 2025, "nodes": [], "links": []}
        with patch.object(etl, "PUBLIC_ROOT", tmp_path):
            out = etl.write_dataset(2025, dataset)
        assert out.exists()
        assert json.loads(out.read_text()) == dataset

    def test_creates_parent_directory(self, tmp_path):
        dataset = {"year": 2025}
        with patch.object(etl, "PUBLIC_ROOT", tmp_path / "deep" / "nested"):
            out = etl.write_dataset(2025, dataset)
        assert out.exists()


class TestUpdateManifest:
    def test_creates_manifest_if_missing(self, tmp_path):
        with patch.object(etl, "MANIFEST_PATH", tmp_path / "manifest.json"):
            etl.update_manifest(2025)
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert any(e["year"] == 2025 for e in manifest["years"])

    def test_does_not_duplicate_year(self, tmp_path):
        manifest_path = tmp_path / "manifest.json"
        with patch.object(etl, "MANIFEST_PATH", manifest_path):
            etl.update_manifest(2025)
            etl.update_manifest(2025)
        manifest = json.loads(manifest_path.read_text())
        years = [e["year"] for e in manifest["years"]]
        assert years.count(2025) == 1

    def test_keeps_existing_years(self, tmp_path):
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps({
            "dataset": "test", "years": [{"year": 2024, "title": "t", "file": "f", "status": "pilot"}]
        }))
        with patch.object(etl, "MANIFEST_PATH", manifest_path):
            etl.update_manifest(2025)
        manifest = json.loads(manifest_path.read_text())
        years = [e["year"] for e in manifest["years"]]
        assert 2024 in years and 2025 in years


# ---------------------------------------------------------------------------
# slugify
# ---------------------------------------------------------------------------

class TestSlugify:
    def test_lowercases_and_hyphens(self):
        assert etl.slugify("OP JAK") == "op-jak"

    def test_strips_special_chars(self):
        assert etl.slugify("Hello, World!") == "hello-world"

    def test_handles_czech_chars_passthrough(self):
        # Non-alphanumeric characters become spaces → stripped; letters stay
        result = etl.slugify("Středočeský kraj")
        assert result  # non-empty
        assert "-" in result or result.isalpha()


# ---------------------------------------------------------------------------
# ares_names.json quality gate
# ---------------------------------------------------------------------------

# School-entity keywords that must not appear in founder names.
# "prostřední" is excluded because it is a place-name component
# (Prostřední Bečva, Prostřední Poříčí), not a school keyword.
_SCHOOL_KEYWORDS = [
    "základní škola",
    "střední škola",
    "mateřská škola",
    "školní jídelna",
    "gymnázium",
    "konzervatoř",
    "speciální škola",
    "zvláštní škola",
    "učiliště",
]

# Known-bad IČOs that were fixed — tested individually so regressions are obvious.
_FIXED_ICOS = {
    "00064581": "Hlavní město Praha",
    "00075370": "Statutární město Plzeň",
    "00286168": "Obec Krahulčí",
    "00286265": "Městys Mrákotín",
    "00286435": "Město Polná",
    "00286656": "Městys Stonařov",
    "00301345": "Obec Jindřichov",
    "00845451": "Statutární město Ostrava",
    "44992785": "Statutární město Brno",
    "60609460": "Olomoucký kraj",
    "70890366": "Plzeňský kraj",
    "70890650": "Jihočeský kraj",
    "70892156": "Ústecký kraj",
}

ARES_NAMES_PATH = Path(__file__).resolve().parents[1] / "etl" / "data" / "ares_names.json"


@pytest.fixture(scope="module")
def ares_names() -> dict:
    if not ARES_NAMES_PATH.exists():
        pytest.skip("ares_names.json not present")
    return json.loads(ARES_NAMES_PATH.read_text(encoding="utf-8"))


class TestAresNamesQuality:
    def test_fixed_icos_have_correct_names(self, ares_names):
        """Previously-broken IČOs must now map to the correct government entity."""
        for ico, expected in _FIXED_ICOS.items():
            actual = ares_names.get(ico, {}).get("name", "")
            assert actual == expected, (
                f"IČO {ico}: expected {expected!r}, got {actual!r}"
            )

    def test_no_founder_ico_has_school_entity_name(self, ares_names):
        """Founder IČOs used in school_entities.csv must not resolve to school-type names.

        This catches ARES lookup collisions where a municipality IČO is
        accidentally mapped to a school or canteen name.
        """
        # Collect all founder IČOs actually used in source CSVs
        founder_icos: set[str] = set()
        raw_root = Path(__file__).resolve().parents[1] / "etl" / "data" / "raw"
        for year_dir in raw_root.iterdir():
            csv_path = year_dir / "school_entities.csv"
            if not csv_path.exists():
                continue
            with csv_path.open(encoding="utf-8-sig") as fh:
                for row in csv.DictReader(fh):
                    fid = row.get("founder_id", "")
                    if fid.startswith("founder:"):
                        founder_icos.add(fid.removeprefix("founder:").zfill(8))

        violations: list[str] = []
        for ico in founder_icos:
            name = ares_names.get(ico, {}).get("name", "").lower()
            for kw in _SCHOOL_KEYWORDS:
                if kw in name:
                    violations.append(f"IČO {ico}: {ares_names[ico]['name']!r} (matched {kw!r})")
                    break

        assert not violations, (
            f"Founder IČOs with school-entity names in ares_names.json "
            f"(fix by updating ares_names.json):\n" + "\n".join(violations)
        )
