import { describe, expect, it } from 'vitest';

import { normalizationCapacity, normalizedValue, orderSankeyGraph } from '../sankeyOrdering';
import type { SankeyLink, SankeyNode } from '../../types';

const nodes: SankeyNode[] = [
  { id: 'state', name: 'State', category: 'state', level: 0 },
  { id: 'high', name: 'High normalized', category: 'other', level: 1, metadata: { capacity: 10 } },
  { id: 'low', name: 'Low normalized', category: 'other', level: 1, metadata: { capacity: 100 } },
  { id: 'na', name: 'Unsupported', category: 'other', level: 1 },
];

const links: SankeyLink[] = [
  {
    source: 'state',
    target: 'low',
    value: 1000,
    amountCzk: 1000,
    year: 2025,
    flowType: 'test',
    basis: 'allocated',
    certainty: 'observed',
    sourceDataset: 'test',
  },
  {
    source: 'state',
    target: 'high',
    value: 500,
    amountCzk: 500,
    year: 2025,
    flowType: 'test',
    basis: 'allocated',
    certainty: 'observed',
    sourceDataset: 'test',
  },
  {
    source: 'state',
    target: 'na',
    value: 100000,
    amountCzk: 100000,
    year: 2025,
    flowType: 'test',
    basis: 'allocated',
    certainty: 'observed',
    sourceDataset: 'test',
  },
];

describe('sankeyOrdering', () => {
  it('normalizes values only when capacity exists', () => {
    expect(normalizedValue(1000, 10, true)).toBe(100);
    expect(normalizedValue(1000, null, true)).toBe(0);
    expect(normalizedValue(1000, null, false)).toBe(1000);
  });

  it('orders normalized mode by descending per-unit weight and pushes unsupported nodes down', () => {
    const capacityMap = new Map<string, number>([
      ['high', 10],
      ['low', 100],
    ]);

    const { orderedNodes, orderedLinks } = orderSankeyGraph(nodes, links, capacityMap, true);

    expect(orderedLinks.map((link) => link.target)).toEqual(['high', 'low', 'na']);
    const orderedNodeIds = orderedNodes.map((node) => node.id);
    expect(orderedNodeIds.indexOf('high')).toBeLessThan(orderedNodeIds.indexOf('low'));
    expect(orderedNodeIds.indexOf('low')).toBeLessThan(orderedNodeIds.indexOf('na'));
  });

  it('suppresses synthetic allocated lower-level links in normalized mode', () => {
    const capacityMap = new Map<string, number>([
      ['region', 100],
      ['class', 25],
    ]);
    const allocatedLink: SankeyLink = {
      source: 'region',
      target: 'class',
      value: 250000,
      amountCzk: 250000,
      year: 2025,
      flowType: 'mv_police_crime_class_allocated_cost',
      basis: 'allocated',
      certainty: 'inferred',
      sourceDataset: 'test',
    };

    expect(normalizationCapacity(allocatedLink, capacityMap, true)).toBeNull();
    expect(normalizedValue(allocatedLink.amountCzk, normalizationCapacity(allocatedLink, capacityMap, true), true)).toBe(0);
  });
});
