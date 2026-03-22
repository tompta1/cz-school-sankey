import { describe, expect, it } from 'vitest';

import { comparableNodeMetric, normalizationCapacity, normalizedValue, orderSankeyGraph } from '../sankeyOrdering';
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

  it('normalizes school expenditure against the source capacity when the bucket has none', () => {
    const capacityMap = new Map<string, number>([['founder:municipality', 180]]);
    const schoolLink: SankeyLink = {
      source: 'founder:municipality',
      target: 'bucket:pedagogical',
      value: 540000,
      amountCzk: 540000,
      year: 2025,
      flowType: 'school_expenditure',
      basis: 'allocated',
      certainty: 'observed',
      sourceDataset: 'test',
    };

    expect(normalizationCapacity(schoolLink, capacityMap, true)).toBe(180);
    expect(normalizedValue(schoolLink.amountCzk, normalizationCapacity(schoolLink, capacityMap, true), true)).toBe(3000);
    expect(comparableNodeMetric('bucket:pedagogical', [schoolLink], capacityMap, true)).toEqual({
      totalAmount: 540000,
      totalCapacity: 180,
      group: 'school_pupil',
    });
  });

  it('aggregates comparable node metrics as sum Kč over sum denominator', () => {
    const capacityMap = new Map<string, number>([
      ['region:a', 100],
      ['region:b', 300],
    ]);
    const pedagogicalLinks: SankeyLink[] = [
      {
        source: 'region:a',
        target: 'bucket:pedagogical',
        value: 400000,
        amountCzk: 400000,
        year: 2025,
        flowType: 'school_expenditure',
        basis: 'allocated',
        certainty: 'observed',
        sourceDataset: 'test',
      },
      {
        source: 'region:b',
        target: 'bucket:pedagogical',
        value: 600000,
        amountCzk: 600000,
        year: 2025,
        flowType: 'school_expenditure',
        basis: 'allocated',
        certainty: 'observed',
        sourceDataset: 'test',
      },
    ];

    expect(comparableNodeMetric('bucket:pedagogical', pedagogicalLinks, capacityMap, true)).toEqual({
      totalAmount: 1000000,
      totalCapacity: 400,
      group: 'school_pupil',
    });
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

  it('suppresses synthetic police allocations in normalized mode', () => {
    const capacityMap = new Map<string, number>([
      ['police', 173322],
      ['region', 100],
      ['class', 25],
    ]);
    const regionAllocatedLink: SankeyLink = {
      source: 'police',
      target: 'region',
      value: 250000,
      amountCzk: 250000,
      year: 2025,
      flowType: 'mv_police_region_allocated_cost',
      basis: 'allocated',
      certainty: 'inferred',
      sourceDataset: 'test',
    };
    const classAllocatedLink: SankeyLink = {
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

    expect(normalizationCapacity(regionAllocatedLink, capacityMap, true)).toBeNull();
    expect(
      normalizedValue(
        regionAllocatedLink.amountCzk,
        normalizationCapacity(regionAllocatedLink, capacityMap, true),
        true,
      ),
    ).toBe(0);

    expect(normalizationCapacity(classAllocatedLink, capacityMap, true)).toBeNull();
    expect(
      normalizedValue(
        classAllocatedLink.amountCzk,
        normalizationCapacity(classAllocatedLink, capacityMap, true),
        true,
      ),
    ).toBe(0);
  });
});
