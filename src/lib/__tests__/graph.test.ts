import { describe, expect, it } from 'vitest';
import { aggregateGraph, drillFounder, drillIntoNode, drillRegion, filterGraph } from '../graph';
import type { InstitutionSummary, SankeyLink, SankeyNode, YearDataset } from '../../types';

// ── Fixtures ─────────────────────────────────────────────────────────────────

const nodes: SankeyNode[] = [
  { id: 'state:cr',        name: 'State budget',     category: 'state',        level: 0 },
  { id: 'msmt',            name: 'MŠMT',             category: 'ministry',     level: 1 },
  { id: 'founder:muni1',   name: 'Municipality A',   category: 'municipality', level: 1 },
  { id: 'founder:region1', name: 'Středočeský kraj',  category: 'region',       level: 1 },
  { id: 'school:s1',       name: 'School 1',         category: 'school_entity', level: 2, ico: 's1' },
  { id: 'school:s2',       name: 'School 2',         category: 'school_entity', level: 2, ico: 's2' },
  { id: 'school:s3',       name: 'School 3',         category: 'school_entity', level: 2, ico: 's3' },
  { id: 'bucket:wages',    name: 'Wages',            category: 'cost_bucket',  level: 3 },
  { id: 'bucket:ops',      name: 'Operations',       category: 'cost_bucket',  level: 3 },
];

function link(
  source: string, target: string, amountCzk: number,
  flowType: string, opts: Partial<SankeyLink> = {}
): SankeyLink {
  return {
    source, target, value: amountCzk, amountCzk,
    year: 2025, flowType,
    basis: 'allocated', certainty: 'observed', sourceDataset: 'test',
    ...opts,
  };
}

const links: SankeyLink[] = [
  link('state:cr',  'msmt',         1_000_000, 'state_to_ministry'),
  link('msmt', 'school:s1',           400_000, 'direct_school_finance', { institutionId: 'school:s1' }),
  link('msmt', 'school:s2',           350_000, 'direct_school_finance', { institutionId: 'school:s2' }),
  link('msmt', 'school:s3',           250_000, 'direct_school_finance', { institutionId: 'school:s3' }),
  link('school:s1', 'bucket:wages',   300_000, 'school_expenditure',    { institutionId: 'school:s1' }),
  link('school:s1', 'bucket:ops',     100_000, 'school_expenditure',    { institutionId: 'school:s1' }),
  link('school:s2', 'bucket:wages',   250_000, 'school_expenditure',    { institutionId: 'school:s2' }),
  link('school:s2', 'bucket:ops',     100_000, 'school_expenditure',    { institutionId: 'school:s2' }),
  link('school:s3', 'bucket:wages',   200_000, 'school_expenditure',    { institutionId: 'school:s3' }),
  link('school:s3', 'bucket:ops',      50_000, 'school_expenditure',    { institutionId: 'school:s3' }),
];

// s1+s2 are in "Středočeský" region, founder: Municipality A
// s3 is in "Praha" region, founder: Středočeský kraj (kraj-as-founder)
const institutions: InstitutionSummary[] = [
  { id: 'school:s1', name: 'School 1', ico: 's1', founderName: 'Municipality A', region: 'Středočeský' },
  { id: 'school:s2', name: 'School 2', ico: 's2', founderName: 'Municipality A', region: 'Středočeský' },
  { id: 'school:s3', name: 'School 3', ico: 's3', founderName: 'Středočeský kraj', region: 'Praha' },
];

const dataset: YearDataset = {
  year: 2025, currency: 'CZK',
  title: 'Test dataset', nodes, links, institutions, sources: [],
};

// ── filterGraph ───────────────────────────────────────────────────────────────

describe('filterGraph', () => {
  const allFilters = {
    year: 2025, certainty: 'all' as const, thresholdCzk: 0,
    institutionId: 'all' as const, flowView: 'all' as const,
  };

  it('returns all links with permissive filters', () => {
    expect(filterGraph(dataset, allFilters).links).toHaveLength(links.length);
  });

  it('filters out inferred links when certainty=observed', () => {
    const ds = { ...dataset, links: [...links, link('state:cr', 'msmt', 1, 'state_to_ministry', { certainty: 'inferred' })] };
    const g = filterGraph(ds, { ...allFilters, certainty: 'observed' });
    expect(g.links.every((l) => l.certainty === 'observed')).toBe(true);
  });

  it('drops links below threshold', () => {
    const g = filterGraph(dataset, { ...allFilters, thresholdCzk: 200_000 });
    expect(g.links.every((l) => l.amountCzk >= 200_000)).toBe(true);
  });

  it('only includes nodes referenced by surviving links', () => {
    const g = filterGraph(dataset, { ...allFilters, thresholdCzk: 200_000 });
    const ids = new Set(g.nodes.map((n) => n.id));
    for (const l of g.links) {
      expect(ids.has(l.source)).toBe(true);
      expect(ids.has(l.target)).toBe(true);
    }
  });

  it('sums totalFunding from funding flow types', () => {
    expect(filterGraph(dataset, allFilters).totalFunding).toBe(2_000_000);
  });

  it('sums totalSpending from school_expenditure links', () => {
    expect(filterGraph(dataset, allFilters).totalSpending).toBe(1_000_000);
  });

  it('computes observedShare = 0.5 for half-inferred dataset', () => {
    const ds = {
      ...dataset,
      links: [
        link('state:cr', 'msmt', 500_000, 'state_to_ministry', { certainty: 'observed' }),
        link('state:cr', 'msmt', 500_000, 'state_to_ministry', { certainty: 'inferred' }),
      ],
    };
    expect(filterGraph(ds, allFilters).observedShare).toBe(0.5);
  });

  it('returns observedShare=0 when no links survive', () => {
    expect(filterGraph(dataset, { ...allFilters, thresholdCzk: 999_999_999 }).observedShare).toBe(0);
  });

  it('filters to funding flows only', () => {
    const g = filterGraph(dataset, { ...allFilters, flowView: 'funding' });
    expect(g.links.every((l) => ['state_to_ministry', 'direct_school_finance'].includes(l.flowType))).toBe(true);
  });

  it('filters to spending flows only', () => {
    const g = filterGraph(dataset, { ...allFilters, flowView: 'spending' });
    expect(g.links.every((l) => l.flowType === 'school_expenditure')).toBe(true);
  });
});

// ── aggregateGraph ────────────────────────────────────────────────────────────

describe('aggregateGraph', () => {
  it('preserves the state→MŠMT link', () => {
    const l = aggregateGraph(dataset).links.find((l) => l.source === 'state:cr');
    expect(l?.amountCzk).toBe(1_000_000);
  });

  it('creates one MŠMT→region node per geographic region', () => {
    const g = aggregateGraph(dataset);
    const regionLinks = g.links.filter((l) => l.source === 'msmt');
    const targets = regionLinks.map((l) => l.target);
    expect(targets).toContain('region:Středočeský');
    expect(targets).toContain('region:Praha');
  });

  it('aggregates MŠMT inflow correctly per region', () => {
    const g = aggregateGraph(dataset);
    const sc = g.links.find((l) => l.target === 'region:Středočeský');
    const pr = g.links.find((l) => l.target === 'region:Praha');
    expect(sc?.amountCzk).toBe(750_000); // s1(400) + s2(350)
    expect(pr?.amountCzk).toBe(250_000); // s3(250)
  });

  it('aggregates spending per region→bucket', () => {
    const g = aggregateGraph(dataset);
    const wages = g.links.find((l) => l.source === 'region:Středočeský' && l.target === 'bucket:wages');
    expect(wages?.amountCzk).toBe(550_000); // s1(300) + s2(250)
  });

  it('excludes individual school nodes', () => {
    const ids = aggregateGraph(dataset).nodes.map((n) => n.id);
    expect(ids).not.toContain('school:s1');
  });

  it('includes synthetic region nodes in result', () => {
    const ids = aggregateGraph(dataset).nodes.map((n) => n.id);
    expect(ids).toContain('region:Středočeský');
    expect(ids).toContain('region:Praha');
  });
});

// ── drillRegion ───────────────────────────────────────────────────────────────

describe('drillRegion', () => {
  it('returns MŠMT→founder links for schools in the region', () => {
    const g = drillRegion(dataset, 'Středočeský');
    const founderLinks = g.links.filter((l) => l.source === 'msmt');
    expect(founderLinks.some((l) => l.target === 'founder:muni1')).toBe(true);
  });

  it('excludes schools from other regions', () => {
    const g = drillRegion(dataset, 'Středočeský');
    const ids = new Set([...g.links.map((l) => l.source), ...g.links.map((l) => l.target)]);
    expect(ids.has('school:s3')).toBe(false); // s3 is in Praha
  });

  it('includes founder→bucket spending links', () => {
    const g = drillRegion(dataset, 'Středočeský');
    const spending = g.links.filter((l) => l.source === 'founder:muni1');
    expect(spending.length).toBeGreaterThan(0);
  });

  it('collapses excess founders into Ostatní when over TOP_FOUNDERS limit', () => {
    // Build a dataset with 26 founders (each with one school)
    const manyNodes: SankeyNode[] = [
      { id: 'state:cr', name: 'State budget', category: 'state', level: 0 },
      { id: 'msmt', name: 'MŠMT', category: 'ministry', level: 1 },
      { id: 'bucket:wages', name: 'Wages', category: 'cost_bucket', level: 3 },
      ...Array.from({ length: 26 }, (_, i) => ({
        id: `founder:f${i}`, name: `Founder ${i}`, category: 'municipality' as const, level: 1,
      })),
      ...Array.from({ length: 26 }, (_, i) => ({
        id: `school:sc${i}`, name: `School ${i}`, category: 'school_entity' as const, level: 2,
      })),
    ];
    const manyInstitutions: InstitutionSummary[] = Array.from({ length: 26 }, (_, i) => ({
      id: `school:sc${i}`, name: `School ${i}`, founderName: `Founder ${i}`, region: 'Středočeský',
    }));
    const manyLinks: SankeyLink[] = [
      link('state:cr', 'msmt', 1_000_000, 'state_to_ministry'),
      ...Array.from({ length: 26 }, (_, i) =>
        link('msmt', `school:sc${i}`, 10_000 * (26 - i), 'direct_school_finance', { institutionId: `school:sc${i}` })
      ),
      ...Array.from({ length: 26 }, (_, i) =>
        link(`school:sc${i}`, 'bucket:wages', 5_000 * (26 - i), 'school_expenditure', { institutionId: `school:sc${i}` })
      ),
    ];
    const manyDataset: YearDataset = {
      year: 2025, currency: 'CZK', title: 'Test', sources: [],
      nodes: manyNodes, links: manyLinks, institutions: manyInstitutions,
    };
    const g = drillRegion(manyDataset, 'Středočeský');
    const ids = new Set(g.nodes.map((n) => n.id));
    expect(ids.has('synthetic:others')).toBe(true);
  });
});

// ── drillIntoNode ─────────────────────────────────────────────────────────────

describe('drillIntoNode', () => {
  it('delegates region:* to drillRegion', () => {
    const g = drillIntoNode(dataset, 'region:Středočeský');
    const founderLinks = g.links.filter((l) => l.source === 'msmt');
    expect(founderLinks.some((l) => l.target === 'founder:muni1')).toBe(true);
  });

  it('returns full graph for state node', () => {
    expect(drillIntoNode(dataset, 'state:cr').links).toHaveLength(links.length);
  });

  it('returns full graph for ministry node', () => {
    expect(drillIntoNode(dataset, 'msmt').links).toHaveLength(links.length);
  });

  it('returns full graph for unknown node id', () => {
    expect(drillIntoNode(dataset, 'does-not-exist').links).toHaveLength(links.length);
  });

  it('drills into school_entity — only that school\'s links', () => {
    const g = drillIntoNode(dataset, 'school:s1');
    expect(g.links).toHaveLength(3); // msmt→s1, s1→wages, s1→ops
    expect(g.links.every((l) => l.source === 'school:s1' || l.target === 'school:s1'
      || l.source === 'msmt')).toBe(true);
  });

  it('drills into municipality — shows schools of that founder as link endpoints', () => {
    const g = drillIntoNode(dataset, 'founder:muni1');
    const ids = new Set([...g.links.map((l) => l.source), ...g.links.map((l) => l.target)]);
    expect(ids.has('school:s1')).toBe(true);
    expect(ids.has('school:s2')).toBe(true);
    expect(ids.has('school:s3')).toBe(false);
  });

  it('drills into region founder — shows its school as a link endpoint', () => {
    const g = drillIntoNode(dataset, 'founder:region1');
    const ids = new Set([...g.links.map((l) => l.source), ...g.links.map((l) => l.target)]);
    expect(ids.has('school:s3')).toBe(true);
    expect(g.links.length).toBeGreaterThan(0);
  });

  it('drills into cost_bucket — includes schools and their MŠMT inflows', () => {
    const g = drillIntoNode(dataset, 'bucket:wages');
    const ids = new Set([...g.links.map((l) => l.source), ...g.links.map((l) => l.target)]);
    expect(ids.has('msmt')).toBe(true);
    expect(ids.has('bucket:wages')).toBe(true);
    expect(ids.has('bucket:ops')).toBe(false);
  });

  it('falls back to full graph when no links survive', () => {
    const sparse: YearDataset = { ...dataset, links: [] };
    expect(drillIntoNode(sparse, 'school:s1').links).toHaveLength(0);
  });
});

// ── drillFounder ──────────────────────────────────────────────────────────────

describe('drillFounder', () => {
  it('returns MŠMT→school links for every school of the founder', () => {
    const g = drillFounder(dataset, 'Municipality A');
    const ids = new Set([...g.links.map((l) => l.source), ...g.links.map((l) => l.target)]);
    expect(ids.has('school:s1')).toBe(true);
    expect(ids.has('school:s2')).toBe(true);
  });

  it('excludes schools from other founders', () => {
    const g = drillFounder(dataset, 'Municipality A');
    const ids = new Set([...g.links.map((l) => l.source), ...g.links.map((l) => l.target)]);
    expect(ids.has('school:s3')).toBe(false);
  });

  it('includes school→bucket spending links', () => {
    const g = drillFounder(dataset, 'Municipality A');
    const spending = g.links.filter((l) => l.flowType === 'school_expenditure');
    expect(spending.length).toBeGreaterThan(0);
  });

  // Regression test for the ECharts crash: "Graph nodes have duplicate name or id"
  // Czech school names are NOT unique — many share "Základní škola" or "Mateřská škola".
  // drillFounder must return graphs where every node.id is unique, because SankeyChartCard
  // uses node.id (not node.name) as the ECharts internal key to avoid this crash.
  it('node IDs are unique even when multiple schools share the same display name', () => {
    const SHARED_NAME = 'Základní škola'; // common duplicate in real data
    const dupeNodes: SankeyNode[] = [
      { id: 'state:cr',     name: 'State budget', category: 'state',        level: 0 },
      { id: 'msmt',         name: 'MŠMT',         category: 'ministry',     level: 1 },
      { id: 'founder:f1',   name: 'Obec Testov',  category: 'municipality', level: 1 },
      { id: 'school:11111111', name: SHARED_NAME, category: 'school_entity', level: 2 },
      { id: 'school:22222222', name: SHARED_NAME, category: 'school_entity', level: 2 },
      { id: 'school:33333333', name: SHARED_NAME, category: 'school_entity', level: 2 },
      { id: 'bucket:wages', name: 'Wages',         category: 'cost_bucket',  level: 3 },
    ];
    const dupeInstitutions: InstitutionSummary[] = [
      { id: 'school:11111111', name: SHARED_NAME, founderName: 'Obec Testov', region: 'Středočeský' },
      { id: 'school:22222222', name: SHARED_NAME, founderName: 'Obec Testov', region: 'Středočeský' },
      { id: 'school:33333333', name: SHARED_NAME, founderName: 'Obec Testov', region: 'Středočeský' },
    ];
    const dupeLinks: SankeyLink[] = [
      link('state:cr', 'msmt', 300_000, 'state_to_ministry'),
      link('msmt', 'school:11111111', 100_000, 'direct_school_finance', { institutionId: 'school:11111111' }),
      link('msmt', 'school:22222222', 100_000, 'direct_school_finance', { institutionId: 'school:22222222' }),
      link('msmt', 'school:33333333', 100_000, 'direct_school_finance', { institutionId: 'school:33333333' }),
      link('school:11111111', 'bucket:wages', 100_000, 'school_expenditure', { institutionId: 'school:11111111' }),
      link('school:22222222', 'bucket:wages', 100_000, 'school_expenditure', { institutionId: 'school:22222222' }),
      link('school:33333333', 'bucket:wages', 100_000, 'school_expenditure', { institutionId: 'school:33333333' }),
    ];
    const dupeDataset: YearDataset = {
      year: 2025, currency: 'CZK', title: 'Dupe test', sources: [],
      nodes: dupeNodes, links: dupeLinks, institutions: dupeInstitutions,
    };

    const g = drillFounder(dupeDataset, 'Obec Testov');

    // All three schools must appear as separate nodes despite identical display names
    const nodeIds = g.nodes.map((n) => n.id);
    expect(nodeIds).toContain('school:11111111');
    expect(nodeIds).toContain('school:22222222');
    expect(nodeIds).toContain('school:33333333');

    // Node IDs must be unique (the ECharts requirement)
    expect(new Set(nodeIds).size).toBe(nodeIds.length);

    // Node names may repeat — that is expected and handled by SankeyChartCard's formatter
    const nodeNames = g.nodes.filter((n) => n.category === 'school_entity').map((n) => n.name);
    expect(nodeNames.every((name) => name === SHARED_NAME)).toBe(true);
  });

  it('collapses schools beyond TOP_SCHOOLS limit into Ostatní školy', () => {
    const bigNodes: SankeyNode[] = [
      { id: 'state:cr', name: 'State budget', category: 'state', level: 0 },
      { id: 'msmt', name: 'MŠMT', category: 'ministry', level: 1 },
      { id: 'bucket:wages', name: 'Wages', category: 'cost_bucket', level: 3 },
      ...Array.from({ length: 35 }, (_, i) => ({
        id: `school:sc${i}`, name: `School ${i}`, category: 'school_entity' as const, level: 2,
      })),
    ];
    const bigInstitutions: InstitutionSummary[] = Array.from({ length: 35 }, (_, i) => ({
      id: `school:sc${i}`, name: `School ${i}`, founderName: 'Big Obec', region: 'Jihomoravský',
    }));
    const bigLinks: SankeyLink[] = [
      link('state:cr', 'msmt', 3_500_000, 'state_to_ministry'),
      ...Array.from({ length: 35 }, (_, i) =>
        link('msmt', `school:sc${i}`, 100_000, 'direct_school_finance', { institutionId: `school:sc${i}` })
      ),
      ...Array.from({ length: 35 }, (_, i) =>
        link(`school:sc${i}`, 'bucket:wages', 80_000, 'school_expenditure', { institutionId: `school:sc${i}` })
      ),
    ];
    const bigDataset: YearDataset = {
      year: 2025, currency: 'CZK', title: 'Big founder', sources: [],
      nodes: bigNodes, links: bigLinks, institutions: bigInstitutions,
    };

    const g = drillFounder(bigDataset, 'Big Obec');
    const nodeIds = new Set(g.nodes.map((n) => n.id));

    // Ostatní školy node must be present
    expect(nodeIds.has('synthetic:other-schools')).toBe(true);
    // No more than TOP_SCHOOLS (30) individual school nodes
    const schoolCount = g.nodes.filter((n) => n.category === 'school_entity').length;
    expect(schoolCount).toBeLessThanOrEqual(30);
    // Node IDs still unique
    const ids = g.nodes.map((n) => n.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('sums amounts correctly when collapsing into Ostatní školy', () => {
    // 31 schools × 100k each; top 30 get individual nodes, the 31st collapses
    const n = 31;
    const schools: SankeyNode[] = Array.from({ length: n }, (_, i) => ({
      id: `school:x${i}`, name: `School ${i}`, category: 'school_entity' as const, level: 2,
    }));
    const inst: InstitutionSummary[] = Array.from({ length: n }, (_, i) => ({
      id: `school:x${i}`, name: `School ${i}`, founderName: 'Obec X', region: 'Ústecký',
    }));
    const ls: SankeyLink[] = [
      // rank schools so sc0 is highest, sc30 is lowest (goes to Ostatní)
      ...Array.from({ length: n }, (_, i) =>
        link('msmt', `school:x${i}`, (n - i) * 10_000, 'direct_school_finance', { institutionId: `school:x${i}` })
      ),
      ...Array.from({ length: n }, (_, i) =>
        link(`school:x${i}`, 'bucket:wages', (n - i) * 8_000, 'school_expenditure', { institutionId: `school:x${i}` })
      ),
    ];
    const ds: YearDataset = {
      year: 2025, currency: 'CZK', title: 'T', sources: [],
      nodes: [
        { id: 'msmt', name: 'MŠMT', category: 'ministry', level: 1 },
        { id: 'bucket:wages', name: 'Wages', category: 'cost_bucket', level: 3 },
        ...schools,
      ],
      links: ls, institutions: inst,
    };

    const g = drillFounder(ds, 'Obec X');
    // The 31st school (sc30, amount 10_000) must be in Ostatní
    const othersLink = g.links.find((l) => l.source === 'synthetic:other-schools');
    expect(othersLink).toBeDefined();
    expect(othersLink!.amountCzk).toBe(8_000); // spending link for sc30
  });
});
