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
});
