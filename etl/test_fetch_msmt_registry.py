"""Tests for etl/fetch_msmt_registry.py.

Run with:  python3 -m pytest etl/test_fetch_msmt_registry.py -v
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_msmt_registry as reg


# ---------------------------------------------------------------------------
# normalize_ico
# ---------------------------------------------------------------------------

class TestNormalizeIco:
    def test_pads_to_8_digits(self):
        assert reg.normalize_ico("12345") == "00012345"

    def test_already_8_digits(self):
        assert reg.normalize_ico("12345678") == "12345678"

    def test_strips_decimal(self):
        assert reg.normalize_ico("12345.0") == "00012345"

    def test_empty_returns_empty(self):
        assert reg.normalize_ico("") == ""

    def test_none_returns_empty(self):
        assert reg.normalize_ico(None) == ""


# ---------------------------------------------------------------------------
# load_registry
# ---------------------------------------------------------------------------

class TestLoadRegistry:
    def _write_jsonld(self, tmp_path: Path, obj: dict, name: str = "reg.jsonld") -> Path:
        p = tmp_path / name
        p.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        return p

    def test_2025_format_top_level_list(self, tmp_path):
        entities = [{"ico": "11111111", "uplnyNazev": "Škola A"}]
        path = self._write_jsonld(tmp_path, {"list": entities})
        result = reg.load_registry(path)
        assert len(result) == 1

    def test_2024_format_nested_list(self, tmp_path):
        entities = [{"ico": "22222222", "uplnyNazev": "Škola B"}]
        path = self._write_jsonld(tmp_path, {
            "http://msmt.cz/sub/data": {"list": entities}
        })
        result = reg.load_registry(path)
        assert len(result) == 1

    def test_empty_list_returns_empty(self, tmp_path):
        path = self._write_jsonld(tmp_path, {"list": []})
        result = reg.load_registry(path)
        assert result == []

    def test_missing_list_key_returns_empty(self, tmp_path):
        path = self._write_jsonld(tmp_path, {"something_else": []})
        result = reg.load_registry(path)
        assert result == []


# ---------------------------------------------------------------------------
# build_lookups
# ---------------------------------------------------------------------------

class TestBuildLookups:
    def _entity(self, ico, name, region="Kraj", municipality="Město", founders=None):
        return {
            "ico": ico,
            "uplnyNazev": name,
            "kraj": region,
            "adresa": {"obec": municipality},
            "zrizovatele": founders or [],
        }

    def _founder(self, ico, name):
        return {"ico": ico, "nazevOsoby": name, "adresa": {"obec": "MěstoF"}}

    def test_builds_school_lookup(self):
        entities = [self._entity("11111111", "Škola A")]
        school_lk, _ = reg.build_lookups(entities)
        assert "11111111" in school_lk
        assert school_lk["11111111"]["name"] == "Škola A"

    def test_builds_founder_lookup(self):
        founder = self._founder("22222222", "Obec Test")
        entities = [self._entity("11111111", "Škola A", founders=[founder])]
        _, founder_lk = reg.build_lookups(entities)
        assert "22222222" in founder_lk
        assert founder_lk["22222222"]["name"] == "Obec Test"

    def test_skips_entity_without_ico(self):
        entities = [{"ico": "", "uplnyNazev": "No ICO School", "zrizovatele": []}]
        school_lk, _ = reg.build_lookups(entities)
        assert school_lk == {}

    def test_fallback_to_zkraceny_nazev(self):
        entities = [{
            "ico": "11111111",
            "uplnyNazev": "",
            "zkracenyNazev": "Short Name",
            "kraj": "",
            "adresa": {},
            "zrizovatele": [],
        }]
        school_lk, _ = reg.build_lookups(entities)
        assert school_lk["11111111"]["name"] == "Short Name"

    def test_does_not_overwrite_existing_founder(self):
        f1 = self._founder("22222222", "First Founder Name")
        f2 = self._founder("22222222", "Second Founder Name")
        entities = [
            self._entity("11111111", "School A", founders=[f1]),
            self._entity("33333333", "School B", founders=[f2]),
        ]
        _, founder_lk = reg.build_lookups(entities)
        # First entry wins
        assert founder_lk["22222222"]["name"] == "First Founder Name"

    def test_ico_padded_to_8_digits(self):
        entities = [self._entity("12345", "Short ICO School")]
        school_lk, _ = reg.build_lookups(entities)
        assert "00012345" in school_lk


# ---------------------------------------------------------------------------
# merge_into_ares
# ---------------------------------------------------------------------------

class TestMergeIntoAres:
    def test_creates_ares_file_if_missing(self, tmp_path):
        school_lk = {"11111111": {"name": "Škola A", "municipality": "Praha"}}
        with patch.object(reg, "ARES_NAMES_PATH", tmp_path / "ares_names.json"):
            schools_upd, founders_upd, total = reg.merge_into_ares(school_lk, {})
        assert schools_upd == 1
        assert total == 1

    def test_school_entry_overwrites_existing(self, tmp_path):
        ares_path = tmp_path / "ares_names.json"
        ares_path.write_text(json.dumps({"11111111": {"name": "Old Name", "municipality": ""}}))
        school_lk = {"11111111": {"name": "New Name", "municipality": "Praha"}}
        with patch.object(reg, "ARES_NAMES_PATH", ares_path):
            reg.merge_into_ares(school_lk, {})
        result = json.loads(ares_path.read_text())
        assert result["11111111"]["name"] == "New Name"

    def test_founder_placeholder_is_replaced(self, tmp_path):
        ares_path = tmp_path / "ares_names.json"
        ares_path.write_text(json.dumps({"22222222": {"name": "IČO 22222222", "municipality": ""}}))
        founder_lk = {"22222222": {"name": "Obec Nová", "municipality": "Nová"}}
        with patch.object(reg, "ARES_NAMES_PATH", ares_path):
            _, founders_upd, _ = reg.merge_into_ares({}, founder_lk)
        assert founders_upd == 1
        result = json.loads(ares_path.read_text())
        assert result["22222222"]["name"] == "Obec Nová"

    def test_founder_real_name_not_overwritten(self, tmp_path):
        ares_path = tmp_path / "ares_names.json"
        ares_path.write_text(json.dumps({"22222222": {"name": "Existing Real Name", "municipality": ""}}))
        founder_lk = {"22222222": {"name": "New Name", "municipality": ""}}
        with patch.object(reg, "ARES_NAMES_PATH", ares_path):
            _, founders_upd, _ = reg.merge_into_ares({}, founder_lk)
        assert founders_upd == 0
        result = json.loads(ares_path.read_text())
        assert result["22222222"]["name"] == "Existing Real Name"

    def test_skips_empty_name_entries(self, tmp_path):
        school_lk = {"11111111": {"name": "", "municipality": ""}}
        with patch.object(reg, "ARES_NAMES_PATH", tmp_path / "ares_names.json"):
            schools_upd, _, _ = reg.merge_into_ares(school_lk, {})
        assert schools_upd == 0


# ---------------------------------------------------------------------------
# update_school_entities_csv
# ---------------------------------------------------------------------------

class TestUpdateSchoolEntitiesCsv:
    def _write_entities(self, tmp_path: Path, year: int, rows: list[dict]) -> Path:
        year_dir = tmp_path / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        path = year_dir / "school_entities.csv"
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_updates_institution_name(self, tmp_path):
        self._write_entities(tmp_path, 2025, [{
            "institution_id": "school:test",
            "institution_name": "IČO 11111111",
            "ico": "11111111",
            "founder_id": "",
            "founder_name": "",
            "municipality": "",
            "region": "",
        }])
        school_lk = {"11111111": {"name": "Proper School Name", "municipality": "Praha", "region": "Praha"}}
        with patch.object(reg, "RAW_ROOT", tmp_path):
            schools_renamed, _ = reg.update_school_entities_csv(2025, school_lk, {})
        assert schools_renamed == 1
        rows = list(csv.DictReader((tmp_path / "2025" / "school_entities.csv").open()))
        assert rows[0]["institution_name"] == "Proper School Name"

    def test_updates_founder_placeholder_name(self, tmp_path):
        self._write_entities(tmp_path, 2025, [{
            "institution_id": "school:test",
            "institution_name": "Škola",
            "ico": "11111111",
            "founder_id": "founder:22222222",
            "founder_name": "Zřizovatel IČO 22222222",
            "municipality": "",
            "region": "",
        }])
        founder_lk = {"22222222": {"name": "Obec Dobříš", "municipality": "Dobříš"}}
        with patch.object(reg, "RAW_ROOT", tmp_path):
            _, founders_renamed = reg.update_school_entities_csv(2025, {}, founder_lk)
        assert founders_renamed == 1
        rows = list(csv.DictReader((tmp_path / "2025" / "school_entities.csv").open()))
        assert rows[0]["founder_name"] == "Obec Dobříš"

    def test_does_not_overwrite_real_founder_name(self, tmp_path):
        self._write_entities(tmp_path, 2025, [{
            "institution_id": "school:test",
            "institution_name": "Škola",
            "ico": "11111111",
            "founder_id": "founder:22222222",
            "founder_name": "Real Founder Name",
            "municipality": "",
            "region": "",
        }])
        founder_lk = {"22222222": {"name": "New Name", "municipality": ""}}
        with patch.object(reg, "RAW_ROOT", tmp_path):
            _, founders_renamed = reg.update_school_entities_csv(2025, {}, founder_lk)
        assert founders_renamed == 0

    def test_missing_csv_returns_zeros(self, tmp_path):
        with patch.object(reg, "RAW_ROOT", tmp_path):
            s, f = reg.update_school_entities_csv(2025, {}, {})
        assert s == 0 and f == 0
