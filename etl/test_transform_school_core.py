from __future__ import annotations

import transform_school_core as etl


def test_state_budget_total_is_metadata_not_graph_flow() -> None:
    source_rows = [
        {
            "dataset_release_id": 10,
            "node_id": "income:taxes",
            "flow_type": "state_revenue",
            "amount_czk": 900,
            "basis": "realized",
            "certainty": "observed",
            "source_url": "https://example.test/source.pdf",
        },
        {
            "dataset_release_id": 10,
            "node_id": "state:total",
            "flow_type": "state_budget_total",
            "amount_czk": 1000,
            "basis": "realized",
            "certainty": "observed",
            "source_url": "https://example.test/source.pdf",
        },
        {
            "dataset_release_id": 10,
            "node_id": "state:other",
            "flow_type": "state_to_other",
            "amount_czk": 800,
            "basis": "realized",
            "certainty": "inferred",
            "source_url": "https://example.test/source.pdf",
        },
    ]

    rows = etl.build_state_budget_flow_rows(
        reporting_period_id=20,
        year=2025,
        state_org_id=30,
        other_org_by_node_id={"income:taxes": 40, "state:other": 50},
        state_budget_rows=source_rows,
    )

    assert len(rows) == 2
    assert rows[0][3:7] == (40, 30, None, "state_revenue")
    assert rows[1][3:7] == (30, 50, None, "state_to_other")
