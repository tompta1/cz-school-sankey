import { describe, expect, it } from 'vitest';

import type { ApiGraph } from '../../types';
import { buildAtlasReferenceSummary } from '../atlasReference';

const graph: ApiGraph = {
  year: 2024,
  nodes: [
    { id: 'state:cr', name: 'State', category: 'state', level: 0 },
    { id: 'transport:sfdi:rail', name: 'Rail', category: 'other', level: 1, metadata: { capacity: 10 } },
    { id: 'transport:sfdi:roads-vignette', name: 'Vignette', category: 'other', level: 1, metadata: { capacity: 5 } },
  ],
  links: [
    {
      source: 'state:cr',
      target: 'transport:sfdi:rail',
      value: 100,
      amountCzk: 100,
      year: 2024,
      flowType: 'transport_rail_branch',
      basis: 'allocated',
      certainty: 'observed',
      sourceDataset: 'transport_activity_metrics',
      note: 'Rail uses annual passenger totals.',
    },
    {
      source: 'state:cr',
      target: 'transport:sfdi:roads-vignette',
      value: 100,
      amountCzk: 100,
      year: 2024,
      flowType: 'transport_road_vignette_branch',
      basis: 'allocated',
      certainty: 'inferred',
      sourceDataset: 'atlas.inferred',
      note: 'Motorway spend is split by revenue share.',
    },
    {
      source: 'state:cr',
      target: 'transport:sfdi:roads-vignette',
      value: 100,
      amountCzk: 100,
      year: 2024,
      flowType: 'transport_road_vignette_branch',
      basis: 'allocated',
      certainty: 'inferred',
      sourceDataset: 'atlas.inferred',
      note: 'Motorway spend is split by revenue share.',
    },
  ],
};

describe('atlasReference', () => {
  it('summarizes active metric groups, datasets, and deduplicated notes', () => {
    const summary = buildAtlasReferenceSummary(graph, true, 'srovnávací jednotku', 'srovnávacích jednotek');

    expect(summary.metrics.map((entry) => entry.group)).toEqual([
      'transport_rail_passenger',
      'transport_vignette_sale',
    ]);
    expect(summary.datasets.map((entry) => entry.datasetKey)).toEqual([
      'transport_activity_metrics',
      'atlas.inferred',
    ]);
    expect(summary.notes).toEqual([
      'Motorway spend is split by revenue share.',
      'Rail uses annual passenger totals.',
    ]);
    expect(summary.inferredFlowCount).toBe(2);
  });

  it('exposes the official MF final-account methodology card', () => {
    const stateGraph: ApiGraph = {
      year: 2025,
      nodes: graph.nodes,
      links: [
        {
          ...graph.links[0],
          year: 2025,
          sourceDataset: 'school_state_budget',
        },
      ],
    };

    const summary = buildAtlasReferenceSummary(stateGraph, false, '', '');
    expect(summary.datasets[0]).toMatchObject({
      datasetKey: 'school_state_budget',
      title: 'MF: souhrnný státní závěrečný účet, sešit G',
    });
  });

  it('adds denominator sources for active health metrics', () => {
    const healthGraph: ApiGraph = {
      year: 2024,
      nodes: [
        { id: 'health:system:public-insurance', name: 'Insurance', category: 'health_system', level: 0 },
        { id: 'health:owner:region', name: 'Hospitals', category: 'health_provider', level: 2, metadata: { capacity: 100 } },
        { id: 'health:system:zzs-mixed-financing', name: 'ZZS finance', category: 'health_system', level: 0 },
        { id: 'health:zzs', name: 'ZZS', category: 'health_provider', level: 2, metadata: { capacity: 50 } },
      ],
      links: [
        {
          source: 'health:system:public-insurance',
          target: 'health:owner:region',
          value: 1_000,
          amountCzk: 1_000,
          year: 2024,
          flowType: 'health_hospital_owner_group',
          basis: 'allocated',
          certainty: 'inferred',
          sourceDataset: 'health_monitor_indicators',
        },
        {
          source: 'health:system:zzs-mixed-financing',
          target: 'health:zzs',
          value: 500,
          amountCzk: 500,
          year: 2024,
          flowType: 'health_zzs_mixed_financing',
          basis: 'allocated',
          certainty: 'observed',
          sourceDataset: 'health_monitor_indicators',
        },
      ],
    };

    const summary = buildAtlasReferenceSummary(healthGraph, true, '', '');

    expect(summary.metrics.map((entry) => entry.group)).toEqual([
      'health_billed_procedure',
      'health_zzs_departure',
    ]);
    expect(summary.datasets.map((entry) => entry.datasetKey)).toEqual(expect.arrayContaining([
      'health_monitor_indicators',
      'nrhzs_claims_provider_ico',
      'health_zzs_activity_aggregates',
    ]));
  });
});
