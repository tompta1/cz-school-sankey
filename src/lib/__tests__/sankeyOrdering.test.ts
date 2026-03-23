import { describe, expect, it } from 'vitest';

import { chartValuesForLinks, comparableNodeMetric, normalizationCapacity, normalizationGroup, normalizedValue, orderSankeyGraph } from '../sankeyOrdering';
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
  it('orders amount mode by descending absolute value within each level', () => {
    const capacityMap = new Map<string, number>();
    const { orderedNodes, orderedLinks } = orderSankeyGraph(nodes, links, capacityMap, false);

    expect(orderedLinks.map((link) => link.target)).toEqual(['na', 'low', 'high']);
    const orderedNodeIds = orderedNodes.map((node) => node.id);
    expect(orderedNodeIds.indexOf('low')).toBeLessThan(orderedNodeIds.indexOf('high'));
    expect(orderedNodeIds.indexOf('na')).toBeLessThan(orderedNodeIds.indexOf('high'));
  });

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

  it('maps transport branch metrics to specific denominator groups', () => {
    const railLink: SankeyLink = {
      source: 'transport:ministry:md',
      target: 'transport:sfdi:rail',
      value: 100,
      amountCzk: 100,
      year: 2025,
      flowType: 'transport_rail_branch',
      basis: 'allocated',
      certainty: 'observed',
      sourceDataset: 'test',
    };
    const vignetteLink: SankeyLink = {
      ...railLink,
      target: 'transport:sfdi:roads-vignette',
      flowType: 'transport_road_vignette_branch',
    };
    const tollLink: SankeyLink = {
      ...railLink,
      target: 'transport:sfdi:roads-toll',
      flowType: 'transport_road_toll_branch',
    };

    expect(normalizationGroup(railLink)).toBe('transport_rail_passenger');
    expect(normalizationGroup(vignetteLink)).toBe('transport_vignette_sale');
    expect(normalizationGroup(tollLink)).toBe('transport_toll_vehicle');
  });

  it('maps agriculture family links to the correct normalization groups', () => {
    const recipientFamilyLink: SankeyLink = {
      source: 'agriculture:ministry:mze',
      target: 'agriculture:subsidy:total',
      value: 100,
      amountCzk: 100,
      year: 2024,
      flowType: 'agriculture_subsidy_family_recipient',
      basis: 'allocated',
      certainty: 'observed',
      sourceDataset: 'test',
    };
    const areaFamilyLink: SankeyLink = {
      ...recipientFamilyLink,
      target: 'agriculture:subsidy:family:area',
      flowType: 'agriculture_subsidy_family_area',
    };
    const recipientDetailLink: SankeyLink = {
      ...recipientFamilyLink,
      source: 'agriculture:subsidy:family:investment',
      target: 'agriculture:recipient:12345678',
      flowType: 'agriculture_subsidy_recipient_detail',
    };
    const areaRecipientDetailLink: SankeyLink = {
      ...recipientDetailLink,
      source: 'agriculture:subsidy:family:area',
    };
    const recipientPageLink: SankeyLink = {
      ...recipientDetailLink,
      target: 'synthetic:next-window',
      flowType: 'agriculture_subsidy_recipient_page',
    };

    expect(normalizationGroup(recipientFamilyLink)).toBe('agriculture_subsidy_recipient');
    expect(normalizationGroup(areaFamilyLink)).toBe('agriculture_area_hectare');
    expect(normalizationGroup(recipientDetailLink)).toBe('agriculture_subsidy_recipient');
    expect(normalizationGroup(areaRecipientDetailLink)).toBe('agriculture_area_hectare');
    expect(normalizationGroup(recipientPageLink)).toBe('agriculture_subsidy_recipient');
  });

  it('normalizes agriculture recipient drilldowns by recipient or hectare where available', () => {
    const capacityMap = new Map<string, number>([
      ['agriculture:recipient:12345678', 1],
      ['agriculture:recipient:87654321', 15.5],
    ]);
    const investmentRecipientLink: SankeyLink = {
      source: 'agriculture:subsidy:family:investment',
      target: 'agriculture:recipient:12345678',
      value: 2500000,
      amountCzk: 2500000,
      year: 2024,
      flowType: 'agriculture_subsidy_recipient_detail',
      basis: 'allocated',
      certainty: 'observed',
      sourceDataset: 'test',
    };
    const areaRecipientLink: SankeyLink = {
      ...investmentRecipientLink,
      source: 'agriculture:subsidy:family:area',
      target: 'agriculture:recipient:87654321',
      amountCzk: 775000,
      value: 775000,
    };

    expect(normalizationCapacity(investmentRecipientLink, capacityMap, true)).toBe(1);
    expect(normalizationCapacity(areaRecipientLink, capacityMap, true)).toBe(15.5);
    expect(normalizedValue(areaRecipientLink.amountCzk, normalizationCapacity(areaRecipientLink, capacityMap, true), true)).toBe(50000);
  });

  it('falls back to raw amounts for layout when a metric view graph has no comparable denominators', () => {
    const capacityMap = new Map<string, number>();
    const zeroMetricLinks: SankeyLink[] = [
      {
        source: 'agriculture:subsidy:family:area',
        target: 'agriculture:recipient:1',
        value: 100,
        amountCzk: 100,
        year: 2024,
        flowType: 'agriculture_subsidy_recipient_detail',
        basis: 'allocated',
        certainty: 'observed',
        sourceDataset: 'test',
      },
      {
        source: 'agriculture:subsidy:family:area',
        target: 'agriculture:recipient:2',
        value: 50,
        amountCzk: 50,
        year: 2024,
        flowType: 'agriculture_subsidy_recipient_detail',
        basis: 'allocated',
        certainty: 'observed',
        sourceDataset: 'test',
      },
    ];

    expect(chartValuesForLinks(zeroMetricLinks, capacityMap, true)).toEqual([100, 50]);
  });

  it('normalizes transport investor drilldowns by project count', () => {
    const capacityMap = new Map<string, number>([
      ['transport:sfdi:rail', 191893200],
      ['transport:investor:123', 4],
      ['transport:project:1', 1],
    ]);
    const branchLink: SankeyLink = {
      source: 'transport:ministry:md',
      target: 'transport:sfdi:rail',
      value: 63112226149,
      amountCzk: 63112226149,
      year: 2024,
      flowType: 'transport_rail_branch',
      basis: 'allocated',
      certainty: 'observed',
      sourceDataset: 'test',
    };
    const investorLink: SankeyLink = {
      source: 'transport:sfdi:rail',
      target: 'transport:investor:123',
      value: 1000000000,
      amountCzk: 1000000000,
      year: 2024,
      flowType: 'transport_sfdi_investor',
      basis: 'allocated',
      certainty: 'observed',
      sourceDataset: 'test',
    };

    const projectLink: SankeyLink = {
      source: 'transport:investor:123',
      target: 'transport:project:1',
      value: 250000000,
      amountCzk: 250000000,
      year: 2024,
      flowType: 'transport_sfdi_project',
      basis: 'allocated',
      certainty: 'observed',
      sourceDataset: 'test',
    };

    expect(normalizationCapacity(branchLink, capacityMap, true)).toBe(191893200);
    expect(normalizationCapacity(investorLink, capacityMap, true)).toBe(4);
    expect(normalizedValue(investorLink.amountCzk, normalizationCapacity(investorLink, capacityMap, true), true)).toBe(250000000);
    expect(normalizationCapacity(projectLink, capacityMap, true)).toBe(1);
    expect(normalizationGroup(projectLink)).toBe('transport_project_count');
  });
});
