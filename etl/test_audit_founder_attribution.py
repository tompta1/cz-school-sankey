from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import audit_founder_attribution as audit


def _entity(institution_id: str, founder_ico: str, founder_name: str = "Founder") -> dict[str, str]:
    return {
        "institution_id": institution_id,
        "founder_id": f"founder:{founder_ico}",
        "founder_name": founder_name,
        "founder_type": "obec",
    }


def test_build_founder_index_groups_schools() -> None:
    founders = audit.build_founder_index(
        {
            "11111111": _entity("school:a", "12345678"),
            "22222222": _entity("school:b", "12345678"),
            "33333333": _entity("school:c", "87654321"),
        }
    )

    assert set(founders) == {"12345678", "87654321"}
    assert founders["12345678"]["institution_ids"] == {"school:a", "school:b"}


def test_find_missing_founders_keeps_registry_gap_visible() -> None:
    missing = audit.find_missing_founders(
        {
            "11111111": _entity("school:a", "12345678"),
            "22222222": _entity("school:b", ""),
        }
    )

    assert len(missing) == 1
    assert missing[0]["institution_id"] == "school:b"
    assert missing[0]["evidence_status"] == "founder_registry_link_required"


def test_audit_marks_multi_school_founder_as_recipient_gap() -> None:
    founders = audit.build_founder_index(
        {
            "11111111": _entity("school:a", "12345678"),
            "22222222": _entity("school:b", "12345678"),
            "33333333": _entity("school:c", "87654321"),
        }
    )
    details = {
        "12345678": {
            "actual_amount_czk": 300,
            "operating_amount_czk": 250,
            "investment_amount_czk": 50,
            "line_count": 2,
        },
        "87654321": {
            "actual_amount_czk": 100,
            "operating_amount_czk": 100,
            "investment_amount_czk": 0,
            "line_count": 1,
        },
    }

    rows = audit.build_audit_rows(
        year=2025,
        founders=founders,
        finm_details=details,
        support_by_institution={"school:a": 120, "school:b": 180, "school:c": 90},
        source_catalog={
            (2025, "founder:12345678"): {
                "document_kind": "approved_school_budget",
                "basis": "budgeted",
                "parser_status": "parser_pending",
                "source_url": "https://example.test/source.xlsx",
            }
        },
    )
    multi = next(row for row in rows if row["founder_ico"] == "12345678")
    single = next(row for row in rows if row["founder_ico"] == "87654321")

    assert multi["priority_rank"] == 1
    assert multi["evidence_status"] == "recipient_source_cataloged"
    assert multi["recipient_source_url"] == "https://example.test/source.xlsx"
    assert multi["current_attributed_czk"] == 300
    assert single["evidence_status"] == "aggregate_candidate_needs_confirmation"


def test_audit_prioritizes_larger_founders() -> None:
    entities = {
        f"school-ico-{index}": _entity(f"school:{index}", "11111111")
        for index in range(3)
    }
    entities.update(
        {
            "school-ico-a": _entity("school:a", "22222222"),
            "school-ico-b": _entity("school:b", "22222222"),
        }
    )
    founders = audit.build_founder_index(entities)
    rows = audit.build_audit_rows(
        year=2025,
        founders=founders,
        finm_details={},
        support_by_institution={},
    )

    ranks = {row["founder_ico"]: row["priority_rank"] for row in rows}
    assert ranks == {"11111111": 1, "22222222": 2}


def test_summary_counts_only_multi_school_work_queue() -> None:
    rows = [
        {"school_count": 1, "evidence_status": "aggregate_candidate_needs_confirmation", "current_support_school_count": 1, "finm_line_count": 1, "finm_total_czk": 10, "current_attributed_czk": 8},
        {"school_count": 2, "evidence_status": "recipient_schedule_required", "current_support_school_count": 2, "finm_line_count": 2, "finm_total_czk": 20, "current_attributed_czk": 18},
        {"school_count": 21, "evidence_status": "recipient_source_cataloged", "current_support_school_count": 20, "finm_line_count": 3, "finm_total_czk": 30, "current_attributed_czk": 28},
    ]

    summary = audit.summarize(2025, rows, "https://example.test/finm.zip", 25)

    assert summary["founder_count"] == 3
    assert summary["school_count"] == 25
    assert summary["missing_founder_school_count"] == 1
    assert summary["missing_current_support_school_count"] == 1
    assert summary["multi_school_founders"] == 2
    assert summary["large_multi_school_founders"] == 1
    assert summary["founders_with_recipient_source_cataloged"] == 1
    assert summary["multi_school_founders_missing_recipient_source"] == 1
    assert summary["schools_under_multi_school_founders"] == 23
    assert summary["relevant_finm_lines"] == 6
