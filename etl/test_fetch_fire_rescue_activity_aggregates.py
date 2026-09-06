import sys
from types import SimpleNamespace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "mv"))
import fetch_fire_rescue_activity_aggregates as hzs


def test_2025_region_table_extracts_official_region_totals():
    region_lines = [
        f"{name} 1 234 100 50,0 2 345 100 40,0 123 100 5,0 45 1,0 3 747 100"
        for name in hzs.REGION_MAP
    ]
    pages = [SimpleNamespace(extract_text=lambda: "") for _ in range(34)]
    pages[32] = SimpleNamespace(extract_text=lambda: "\n".join(region_lines[:7]))
    pages[33] = SimpleNamespace(extract_text=lambda: "\n".join(region_lines[7:]))

    rows = hzs.extract_2025_region_rows(SimpleNamespace(pages=pages))

    assert len(rows) == 28
    assert {row["region_code"] for row in rows} == {code for code, _ in hzs.REGION_MAP.values()}
    assert all(row["count_value"] == 1234 for row in rows if row["indicator_code"] == "hzs_interventions")
    assert all(row["count_value"] == 3747 for row in rows if row["indicator_code"] == "jpo_total_interventions")
