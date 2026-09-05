import { describe, expect, it } from 'vitest';

import type { ApiGraph, SankeyLink } from '../../types';
import { buildAtlasReferenceSummary } from '../atlasReference';

function link(overrides: Partial<SankeyLink> = {}): SankeyLink {
  return {
    source: 'state:cr',
    target: 'transport:sfdi:rail',
    value: 100,
    amountCzk: 100,
    year: 2024,
    flowType: 'transport_rail_branch',
    basis: 'allocated',
    certainty: 'observed',
    sourceDataset: 'transport_activity_metrics',
    ...overrides,
  };
}

function graph(links: SankeyLink[]): ApiGraph {
  return {
    year: 2024,
    nodes: [
      { id: 'state:cr', name: 'State', category: 'state', level: 0 },
      { id: 'transport:sfdi:rail', name: 'Rail', category: 'other', level: 1, metadata: { capacity: 10 } },
    ],
    links,
  };
}

describe('atlasReference', () => {
  it('lists public sources and hides the internal inferred marker', () => {
    const summary = buildAtlasReferenceSummary(graph([
      link(),
      link({ sourceDataset: 'atlas.inferred', certainty: 'inferred' }),
    ]), false);

    expect(summary.datasets).toEqual([
      expect.objectContaining({
        datasetKey: 'transport_activity_metrics',
        url: 'https://www.sydos.cz/cs/rocenka-2024',
      }),
    ]);
  });

  it('maps transformed state flows back to the official MF source', () => {
    const summary = buildAtlasReferenceSummary(graph([
      link({ sourceDataset: 'core.financial_flow' }),
    ]), false);

    expect(summary.datasets[0]).toMatchObject({
      datasetKey: 'core.financial_flow',
      title: 'MF: státní závěrečný účet',
      url: 'https://mf.gov.cz/cs/rozpoctova-politika/statni-rozpocet/plneni-statniho-rozpoctu',
    });
  });

  it('prefers the exact source URL carried by the active graph link', () => {
    const sourceUrl = 'https://example.test/current-source.csv';
    const summary = buildAtlasReferenceSummary(graph([link({ sourceUrl })]), false);

    expect(summary.datasets[0].url).toBe(sourceUrl);
  });

  it('adds numerator and denominator sources for comparative health links', () => {
    const summary = buildAtlasReferenceSummary(graph([
      link({
        target: 'health:owner:region',
        flowType: 'health_hospital_owner_group',
        sourceDataset: 'health_monitor_indicators',
      }),
      link({
        target: 'health:zzs',
        flowType: 'health_zzs_mixed_financing',
        sourceDataset: 'health_monitor_indicators',
      }),
    ]), true);

    expect(summary.datasets.map((entry) => entry.datasetKey)).toEqual(expect.arrayContaining([
      'health_monitor_indicators',
      'nrhzs_claims_provider_ico',
      'health_zzs_activity_aggregates',
    ]));
  });
});
