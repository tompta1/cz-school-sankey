import { describe, expect, it } from 'vitest';
import { metricCoverage } from '../metricCoverage';
import type { ApiGraph } from '../../types';

describe('metric coverage', () => {
  it('counts usable denominators separately from missing values and mixed aggregates', () => {
    const graph: ApiGraph = {
      year: 2025,
      nodes: [{ id: 'social:benefit:pensions', name: 'Pensions', category: 'other', level: 1, metadata: { capacity: 100 } }],
      links: ['social:benefit:pensions', 'social:benefit:unemployment', 'social:benefit:family'].map((target) => ({
        source: 'social', target, amountCzk: 1000, value: 1000, year: 2025,
        flowType: 'social_benefit_group', basis: 'realized', certainty: 'observed', sourceDataset: 'social',
      })),
    };
    const coverage = metricCoverage(graph);
    expect(coverage.available).toBe(1);
    expect(coverage.total).toBe(3);
    expect(coverage.missing).toHaveLength(2);
    graph.nodes[0].metadata!.capacity = 0;
    expect(metricCoverage(graph).available).toBe(0);
  });
});
