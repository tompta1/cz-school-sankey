import importlib.util
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent / "justice"))
spec = importlib.util.spec_from_file_location("justice_activity", Path(__file__).parent / "justice/fetch_activity_aggregates.py")
activity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(activity)


def test_population_uses_total_people_not_bed_capacity():
    text = "Průměrné ubytovací kapacity za rok 2025\nCelkem 1 834,83 18 233,08 20 067,92 1 660,58 17 902,75 19 562,92 90,50% 98,19% 97,48%"
    assert activity.parse_prison_population(text, 2025) == 19562.92
    with pytest.raises(RuntimeError):
        activity.parse_prison_population(text, 2024)
    with pytest.raises(RuntimeError):
        activity.parse_prison_population(text.replace("19 562,92", "29 562,92"), 2025)
