import { describe, expect, it } from 'vitest';
import { aggregateGraph, drillEU, drillFounder, drillFounderType, drillIntoNode, drillRegion, EU_ALL_ID, filterGraph, FOUNDERS_KRAJ, FOUNDERS_OBEC, NEXT_WINDOW_ID, PREV_WINDOW_ID, TOP_FOUNDERS } from '../graph';
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

// s1+s2 are in "Středočeský" region, founder: Municipality A (obec)
// s3 is in "Praha" region, founder: Středočeský kraj (kraj)
const institutions: InstitutionSummary[] = [
  { id: 'school:s1', name: 'School 1', ico: 's1', founderName: 'Municipality A', founderType: 'obec', region: 'Středočeský' },
  { id: 'school:s2', name: 'School 2', ico: 's2', founderName: 'Municipality A', founderType: 'obec', region: 'Středočeský' },
  { id: 'school:s3', name: 'School 3', ico: 's3', founderName: 'Středočeský kraj', founderType: 'kraj', region: 'Praha' },
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
  it('includes state→MŠMT link (state:cr is now visible at top level)', () => {
    const links = aggregateGraph(dataset).links;
    const stateLink = links.find((l) => l.source === 'state:cr' && l.target === 'msmt');
    expect(stateLink).toBeDefined();
    expect(stateLink!.amountCzk).toBe(1_000_000);
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
    expect(ids.has('synthetic:next-window')).toBe(true);
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

    // next-window node must be present when schools exceed TOP_SCHOOLS
    expect(nodeIds.has('synthetic:next-window')).toBe(true);
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
    // The 31st school (lowest rank) must be aggregated into next-window
    const nextLinks = g.links.filter((l) => l.source === 'synthetic:next-window');
    expect(nextLinks.length).toBeGreaterThan(0);
    // spending total for the 31st school: (31-30)*8_000 = 8_000
    const spendingLink = nextLinks.find((l) => l.target === 'bucket:wages');
    expect(spendingLink).toBeDefined();
    expect(spendingLink!.amountCzk).toBe(8_000);
  });
});

// ── EU flow tests ─────────────────────────────────────────────────────────────

// Shared EU fixture: one EU programme with one project → school:s1
const euNodes: SankeyNode[] = [
  ...nodes,
  { id: 'eu_prog:OPZ', name: 'OP Zaměstnanost', category: 'eu_programme', level: 0 },
  { id: 'eu_proj:P1',  name: 'Project 1',       category: 'eu_project',   level: 1 },
];
const euLinks: SankeyLink[] = [
  ...links,
  link('eu_prog:OPZ', 'eu_proj:P1',  50_000, 'eu_project_support'),
  link('eu_proj:P1',  'school:s1',   50_000, 'project_to_school', { institutionId: 'school:s1' }),
];
const euDataset: YearDataset = {
  year: 2025, currency: 'CZK', title: 'EU test',
  nodes: euNodes, links: euLinks, institutions, sources: [],
};

describe('EU flows — aggregateGraph', () => {
  it('produces an eu_project_support link from eu:all to the region', () => {
    const g = aggregateGraph(euDataset);
    const euLink = g.links.find(
      (l) => l.source === EU_ALL_ID && l.target === 'region:Středočeský'
    );
    expect(euLink).toBeDefined();
    expect(euLink!.amountCzk).toBe(50_000);
  });

  it('does not include the raw project node in the aggregate view', () => {
    const ids = aggregateGraph(euDataset).nodes.map((n) => n.id);
    expect(ids).not.toContain('eu_proj:P1');
  });

  it('includes the eu:all aggregate node (not individual programme) in the aggregate view', () => {
    const ids = aggregateGraph(euDataset).nodes.map((n) => n.id);
    expect(ids).toContain(EU_ALL_ID);
    expect(ids).not.toContain('eu_prog:OPZ');
  });
});

describe('EU flows — drillRegion', () => {
  it('produces an eu_project_support link from programme to the founder', () => {
    const g = drillRegion(euDataset, 'Středočeský');
    const euLink = g.links.find(
      (l) => l.source === 'eu_prog:OPZ' && l.target === 'founder:muni1'
    );
    expect(euLink).toBeDefined();
    expect(euLink!.amountCzk).toBe(50_000);
  });

  it('does not include raw project nodes at region drill level', () => {
    const ids = drillRegion(euDataset, 'Středočeský').nodes.map((n) => n.id);
    expect(ids).not.toContain('eu_proj:P1');
  });
});

describe('EU flows — drillFounder', () => {
  it('produces an eu_project_support link from programme directly to the school', () => {
    const g = drillFounder(euDataset, 'Municipality A');
    const euLink = g.links.find(
      (l) => l.source === 'eu_prog:OPZ' && l.target === 'school:s1'
    );
    expect(euLink).toBeDefined();
    expect(euLink!.amountCzk).toBe(50_000);
  });

  it('does not include raw project nodes at founder drill level', () => {
    const ids = drillFounder(euDataset, 'Municipality A').nodes.map((n) => n.id);
    expect(ids).not.toContain('eu_proj:P1');
  });

  it('EU school with no other links still appears when in window', () => {
    const g = drillFounder(euDataset, 'Municipality A');
    const ids = new Set([...g.links.map((l) => l.source), ...g.links.map((l) => l.target)]);
    expect(ids.has('school:s1')).toBe(true);
  });
});

// ── sliding window navigation tests ──────────────────────────────────────────

function makeWindowDataset(numSchools: number, founderName = 'Big Obec'): YearDataset {
  const schoolNodes: SankeyNode[] = Array.from({ length: numSchools }, (_, i) => ({
    id: `school:w${i}`, name: `School W${i}`, category: 'school_entity' as const, level: 2,
  }));
  const inst: InstitutionSummary[] = Array.from({ length: numSchools }, (_, i) => ({
    id: `school:w${i}`, name: `School W${i}`, founderName, region: 'Jihomoravský',
  }));
  const ls: SankeyLink[] = [
    ...Array.from({ length: numSchools }, (_, i) =>
      link('msmt', `school:w${i}`, (numSchools - i) * 10_000, 'direct_school_finance', { institutionId: `school:w${i}` })
    ),
    ...Array.from({ length: numSchools }, (_, i) =>
      link(`school:w${i}`, 'bucket:wages', (numSchools - i) * 8_000, 'school_expenditure', { institutionId: `school:w${i}` })
    ),
  ];
  return {
    year: 2025, currency: 'CZK', title: 'Window test', sources: [],
    nodes: [
      { id: 'msmt', name: 'MŠMT', category: 'ministry', level: 1 },
      { id: 'bucket:wages', name: 'Wages', category: 'cost_bucket', level: 3 },
      ...schoolNodes,
    ],
    links: ls, institutions: inst,
  };
}

describe('sliding window — drillFounder', () => {
  const ds = makeWindowDataset(65); // 65 schools, TOP_SCHOOLS=30

  it('offset=0: next-window present, prev-window absent', () => {
    const g = drillFounder(ds, 'Big Obec', 0);
    const ids = new Set(g.nodes.map((n) => n.id));
    expect(ids.has(NEXT_WINDOW_ID)).toBe(true);
    expect(ids.has(PREV_WINDOW_ID)).toBe(false);
  });

  it('offset=30: both prev-window and next-window present', () => {
    const g = drillFounder(ds, 'Big Obec', 30);
    const ids = new Set(g.nodes.map((n) => n.id));
    expect(ids.has(PREV_WINDOW_ID)).toBe(true);
    expect(ids.has(NEXT_WINDOW_ID)).toBe(true);
  });

  it('offset=60: prev-window present, next-window absent (only 5 left)', () => {
    const g = drillFounder(ds, 'Big Obec', 60);
    const ids = new Set(g.nodes.map((n) => n.id));
    expect(ids.has(PREV_WINDOW_ID)).toBe(true);
    expect(ids.has(NEXT_WINDOW_ID)).toBe(false);
  });

  it('window school count does not exceed TOP_SCHOOLS for any offset', () => {
    for (const offset of [0, 10, 30, 60]) {
      const g = drillFounder(ds, 'Big Obec', offset);
      const count = g.nodes.filter((n) => n.category === 'school_entity').length;
      expect(count).toBeLessThanOrEqual(30);
    }
  });

  it('different offsets show different school windows (no overlap)', () => {
    const g0 = drillFounder(ds, 'Big Obec', 0);
    const g30 = drillFounder(ds, 'Big Obec', 30);
    const schools0 = new Set(g0.nodes.filter((n) => n.category === 'school_entity').map((n) => n.id));
    const schools30 = new Set(g30.nodes.filter((n) => n.category === 'school_entity').map((n) => n.id));
    const overlap = [...schools0].filter((id) => schools30.has(id));
    expect(overlap).toHaveLength(0);
  });

  it('prev-window node name contains correct count', () => {
    const g = drillFounder(ds, 'Big Obec', 30);
    const prevNode = g.nodes.find((n) => n.id === PREV_WINDOW_ID);
    expect(prevNode?.name).toContain('30');
  });
});

// ── aggregateGraph — state budget passthrough + per-pupil capacity ────────────

describe('aggregateGraph — state budget', () => {
  const stateNodes: SankeyNode[] = [
    ...nodes,
    { id: 'income:taxes',  name: 'Daňové příjmy',       category: 'other', level: 0 },
    { id: 'income:eu',     name: 'Přijaté transfery EU', category: 'other', level: 0 },
    { id: 'state:other',   name: 'Ostatní výdaje SR',    category: 'other', level: 3 },
  ];
  const stateLinks: SankeyLink[] = [
    ...links,
    link('income:taxes', 'state:cr', 1_800_000_000, 'state_revenue',    { basis: 'realized' }),
    link('income:eu',    'state:cr',   200_000_000, 'state_revenue',    { basis: 'realized' }),
    link('state:cr',     'state:other', 400_000_000, 'state_to_other',  { basis: 'realized', certainty: 'inferred' }),
  ];
  const stateDataset: YearDataset = {
    year: 2025, currency: 'CZK', title: 'State budget test',
    nodes: stateNodes, links: stateLinks, institutions, sources: [],
  };

  it('passes through state_revenue links unchanged', () => {
    const g = aggregateGraph(stateDataset);
    const taxLink = g.links.find((l) => l.source === 'income:taxes' && l.target === 'state:cr');
    expect(taxLink?.amountCzk).toBe(1_800_000_000);
  });

  it('passes through state_to_other link unchanged', () => {
    const g = aggregateGraph(stateDataset);
    const otherLink = g.links.find((l) => l.source === 'state:cr' && l.target === 'state:other');
    expect(otherLink).toBeDefined();
  });

  it('includes income source nodes', () => {
    const ids = aggregateGraph(stateDataset).nodes.map((n) => n.id);
    expect(ids).toContain('income:taxes');
    expect(ids).toContain('income:eu');
  });
});

describe('aggregateGraph — per-pupil capacity on region nodes', () => {
  // s1 capacity=100, s2 capacity=80 → Středočeský total=180
  // s3 capacity=60 → Praha total=60
  const capInstitutions: InstitutionSummary[] = [
    { id: 'school:s1', name: 'School 1', founderType: 'obec', region: 'Středočeský', capacity: 100 },
    { id: 'school:s2', name: 'School 2', founderType: 'obec', region: 'Středočeský', capacity: 80 },
    { id: 'school:s3', name: 'School 3', founderType: 'kraj', region: 'Praha',        capacity: 60 },
  ];
  const capDataset: YearDataset = { ...dataset, institutions: capInstitutions };

  it('region nodes carry summed capacity in metadata', () => {
    const g = aggregateGraph(capDataset);
    const sc = g.nodes.find((n) => n.id === 'region:Středočeský');
    const pr = g.nodes.find((n) => n.id === 'region:Praha');
    expect(sc?.metadata?.capacity).toBe(180); // 100+80
    expect(pr?.metadata?.capacity).toBe(60);
  });

  it('region nodes without any institution capacity have no metadata', () => {
    const g = aggregateGraph(dataset); // original institutions have no capacity
    const sc = g.nodes.find((n) => n.id === 'region:Středočeský');
    expect(sc?.metadata?.capacity).toBeUndefined();
  });
});

// ── Founder aggregate fixture ─────────────────────────────────────────────────

// founderDataset: s1+s2 founded by Municipality A (obec), s3 founded by Středočeský kraj (kraj)
// Adds founder_support links so aggregateGraph can produce founders:kraj + founders:obec nodes
const founderNodes: SankeyNode[] = [
  ...nodes,
  { id: 'founder:muni1',   name: 'Municipality A',   category: 'municipality', level: 1 },
  { id: 'founder:region1', name: 'Středočeský kraj',  category: 'region',       level: 1 },
];
const founderLinks: SankeyLink[] = [
  ...links,
  link('founder:muni1',   'school:s1', 80_000, 'founder_support', { institutionId: 'school:s1' }),
  link('founder:muni1',   'school:s2', 60_000, 'founder_support', { institutionId: 'school:s2' }),
  link('founder:region1', 'school:s3', 40_000, 'founder_support', { institutionId: 'school:s3' }),
];
const founderDataset: YearDataset = {
  year: 2025, currency: 'CZK', title: 'Founder test',
  nodes: founderNodes, links: founderLinks, institutions, sources: [],
};

// ── aggregateGraph — founder aggregates ───────────────────────────────────────

describe('aggregateGraph — founder aggregates', () => {
  it('emits founders:obec → region link when obec founder_support exists', () => {
    const g = aggregateGraph(founderDataset);
    const l = g.links.find((l) => l.source === FOUNDERS_OBEC && l.target === 'region:Středočeský');
    expect(l).toBeDefined();
    expect(l!.amountCzk).toBe(140_000); // 80k + 60k
  });

  it('emits founders:kraj → region link when kraj founder_support exists', () => {
    const g = aggregateGraph(founderDataset);
    const l = g.links.find((l) => l.source === FOUNDERS_KRAJ && l.target === 'region:Praha');
    expect(l).toBeDefined();
    expect(l!.amountCzk).toBe(40_000);
  });

  it('includes founders:obec and founders:kraj nodes', () => {
    const ids = aggregateGraph(founderDataset).nodes.map((n) => n.id);
    expect(ids).toContain(FOUNDERS_OBEC);
    expect(ids).toContain(FOUNDERS_KRAJ);
  });

  it('omits founders:obec node when no obec support present', () => {
    // Dataset with only kraj founder_support
    const ds: YearDataset = {
      ...founderDataset,
      links: [
        ...links,
        link('founder:region1', 'school:s3', 40_000, 'founder_support', { institutionId: 'school:s3' }),
      ],
    };
    const ids = aggregateGraph(ds).nodes.map((n) => n.id);
    expect(ids).not.toContain(FOUNDERS_OBEC);
    expect(ids).toContain(FOUNDERS_KRAJ);
  });

  it('MŠMT region sums are unaffected by founder flows', () => {
    const g = aggregateGraph(founderDataset);
    const sc = g.links.find((l) => l.source === 'msmt' && l.target === 'region:Středočeský');
    expect(sc?.amountCzk).toBe(750_000); // same as without founders
  });
});

// ── drillEU ───────────────────────────────────────────────────────────────────

describe('drillEU', () => {
  it('shows individual EU programme nodes (not eu:all)', () => {
    const ids = drillEU(euDataset).nodes.map((n) => n.id);
    expect(ids).toContain('eu_prog:OPZ');
    expect(ids).not.toContain(EU_ALL_ID);
  });

  it('produces programme → region links (skipping project intermediate)', () => {
    const g = drillEU(euDataset);
    const l = g.links.find((l) => l.source === 'eu_prog:OPZ' && l.target === 'region:Středočeský');
    expect(l).toBeDefined();
    expect(l!.amountCzk).toBe(50_000);
  });

  it('does not include raw project nodes', () => {
    const ids = drillEU(euDataset).nodes.map((n) => n.id);
    expect(ids).not.toContain('eu_proj:P1');
  });

  it('creates synthetic region nodes for all regions receiving EU funds', () => {
    const ids = drillEU(euDataset).nodes.map((n) => n.id);
    expect(ids).toContain('region:Středočeský');
  });

  it('returns empty links when no EU flows exist', () => {
    expect(drillEU(dataset).links).toHaveLength(0);
  });

  it('aggregates multiple programmes to the same region as separate links', () => {
    const nodes2: SankeyNode[] = [
      ...euNodes,
      { id: 'eu_prog:IROP', name: 'IROP', category: 'eu_programme', level: 0 },
      { id: 'eu_proj:P2',   name: 'Project 2', category: 'eu_project', level: 1 },
    ];
    const links2: SankeyLink[] = [
      ...euLinks,
      link('eu_prog:IROP', 'eu_proj:P2', 30_000, 'eu_project_support'),
      link('eu_proj:P2', 'school:s1', 30_000, 'project_to_school', { institutionId: 'school:s1' }),
    ];
    const ds2: YearDataset = { ...euDataset, nodes: nodes2, links: links2 };
    const g = drillEU(ds2);
    const opzLink  = g.links.find((l) => l.source === 'eu_prog:OPZ'  && l.target === 'region:Středočeský');
    const iropLink = g.links.find((l) => l.source === 'eu_prog:IROP' && l.target === 'region:Středočeský');
    expect(opzLink?.amountCzk).toBe(50_000);
    expect(iropLink?.amountCzk).toBe(30_000);
  });
});

// ── drillFounderType ──────────────────────────────────────────────────────────

describe('drillFounderType — obec', () => {
  it('shows individual obec founder nodes', () => {
    const g = drillFounderType(founderDataset, 'obec');
    const ids = g.nodes.map((n) => n.id);
    expect(ids).toContain('founder:muni1');
  });

  it('does not include kraj founders', () => {
    const g = drillFounderType(founderDataset, 'obec');
    const ids = g.nodes.map((n) => n.id);
    expect(ids).not.toContain('founder:region1');
  });

  it('emits founders:obec → founder link with correct total', () => {
    const g = drillFounderType(founderDataset, 'obec');
    const l = g.links.find((l) => l.source === FOUNDERS_OBEC && l.target === 'founder:muni1');
    expect(l).toBeDefined();
    expect(l!.amountCzk).toBe(140_000); // 80k + 60k
  });

  it('includes the founders:obec aggregate node', () => {
    const ids = drillFounderType(founderDataset, 'obec').nodes.map((n) => n.id);
    expect(ids).toContain(FOUNDERS_OBEC);
  });
});

describe('drillFounderType — kraj', () => {
  it('shows individual kraj founder nodes', () => {
    const g = drillFounderType(founderDataset, 'kraj');
    const ids = g.nodes.map((n) => n.id);
    expect(ids).toContain('founder:region1');
  });

  it('does not include obec founders', () => {
    const g = drillFounderType(founderDataset, 'kraj');
    const ids = g.nodes.map((n) => n.id);
    expect(ids).not.toContain('founder:muni1');
  });

  it('emits founders:kraj → founder link with correct total', () => {
    const g = drillFounderType(founderDataset, 'kraj');
    const l = g.links.find((l) => l.source === FOUNDERS_KRAJ && l.target === 'founder:region1');
    expect(l).toBeDefined();
    expect(l!.amountCzk).toBe(40_000);
  });

  it('shows next-window node when founders exceed TOP_FOUNDERS', () => {
    const manyInst: InstitutionSummary[] = Array.from({ length: TOP_FOUNDERS + 5 }, (_, i) => ({
      id: `school:ft${i}`, name: `School FT${i}`, founderName: `Kraj ${i}`, founderType: 'kraj',
      region: 'Jihomoravský',
    }));
    const manyNodes: SankeyNode[] = [
      { id: 'msmt', name: 'MŠMT', category: 'ministry', level: 1 },
      ...Array.from({ length: TOP_FOUNDERS + 5 }, (_, i) => ({
        id: `founder:k${i}`, name: `Kraj ${i}`, category: 'region' as const, level: 1,
      })),
      ...Array.from({ length: TOP_FOUNDERS + 5 }, (_, i) => ({
        id: `school:ft${i}`, name: `School FT${i}`, category: 'school_entity' as const, level: 2,
      })),
    ];
    const manyLinks: SankeyLink[] = Array.from({ length: TOP_FOUNDERS + 5 }, (_, i) =>
      link(`founder:k${i}`, `school:ft${i}`, (TOP_FOUNDERS + 5 - i) * 1_000, 'founder_support',
        { institutionId: `school:ft${i}` })
    );
    const ds: YearDataset = {
      year: 2025, currency: 'CZK', title: 'Many kraj', sources: [],
      nodes: manyNodes, links: manyLinks, institutions: manyInst,
    };
    const g = drillFounderType(ds, 'kraj', 0);
    const ids = new Set(g.nodes.map((n) => n.id));
    expect(ids.has(NEXT_WINDOW_ID)).toBe(true);
    expect(ids.has(PREV_WINDOW_ID)).toBe(false);
  });

  it('shows both window nodes at offset=TOP_FOUNDERS when total > 2×TOP_FOUNDERS', () => {
    const total = TOP_FOUNDERS * 2 + 3;
    const manyInst: InstitutionSummary[] = Array.from({ length: total }, (_, i) => ({
      id: `school:fw${i}`, name: `School FW${i}`, founderName: `Kraj ${i}`, founderType: 'kraj',
      region: 'Jihomoravský',
    }));
    const manyNodes: SankeyNode[] = [
      { id: 'msmt', name: 'MŠMT', category: 'ministry', level: 1 },
      ...Array.from({ length: total }, (_, i) => ({
        id: `founder:w${i}`, name: `Kraj ${i}`, category: 'region' as const, level: 1,
      })),
      ...Array.from({ length: total }, (_, i) => ({
        id: `school:fw${i}`, name: `School FW${i}`, category: 'school_entity' as const, level: 2,
      })),
    ];
    const manyLinks: SankeyLink[] = Array.from({ length: total }, (_, i) =>
      link(`founder:w${i}`, `school:fw${i}`, (total - i) * 1_000, 'founder_support',
        { institutionId: `school:fw${i}` })
    );
    const ds: YearDataset = {
      year: 2025, currency: 'CZK', title: 'Many kraj 2', sources: [],
      nodes: manyNodes, links: manyLinks, institutions: manyInst,
    };
    const g = drillFounderType(ds, 'kraj', TOP_FOUNDERS);
    const ids = new Set(g.nodes.map((n) => n.id));
    expect(ids.has(PREV_WINDOW_ID)).toBe(true);
    expect(ids.has(NEXT_WINDOW_ID)).toBe(true);
  });
});

// ── drillIntoNode routing for new aggregate nodes ─────────────────────────────

describe('drillIntoNode — new aggregate node routing', () => {
  it('routes eu:all to drillEU (shows individual programmes)', () => {
    const g = drillIntoNode(euDataset, EU_ALL_ID);
    const ids = g.nodes.map((n) => n.id);
    expect(ids).toContain('eu_prog:OPZ');
    expect(ids).not.toContain(EU_ALL_ID);
  });

  it('routes founders:kraj to drillFounderType kraj', () => {
    const g = drillIntoNode(founderDataset, FOUNDERS_KRAJ);
    const ids = g.nodes.map((n) => n.id);
    expect(ids).toContain('founder:region1');
    expect(ids).not.toContain('founder:muni1');
  });

  it('routes founders:obec to drillFounderType obec', () => {
    const g = drillIntoNode(founderDataset, FOUNDERS_OBEC);
    const ids = g.nodes.map((n) => n.id);
    expect(ids).toContain('founder:muni1');
    expect(ids).not.toContain('founder:region1');
  });

  it('founders:kraj routing not misrouted to drillFounder (category=region)', () => {
    // FOUNDERS_KRAJ has category 'region' — must not fall through to drillFounder
    const g = drillIntoNode(founderDataset, FOUNDERS_KRAJ);
    // drillFounder('Regional founders (kraj)') would return empty; drillFounderType returns real data
    expect(g.links.length).toBeGreaterThan(0);
  });
});

describe('sliding window — drillRegion', () => {
  // Build a region dataset with 30 founders (26 > TOP_FOUNDERS=25)
  const numFounders = 30;
  const regionNodes: SankeyNode[] = [
    { id: 'state:cr', name: 'State budget', category: 'state', level: 0 },
    { id: 'msmt', name: 'MŠMT', category: 'ministry', level: 1 },
    { id: 'bucket:wages', name: 'Wages', category: 'cost_bucket', level: 3 },
    ...Array.from({ length: numFounders }, (_, i) => ({
      id: `founder:rf${i}`, name: `Founder RF${i}`, category: 'municipality' as const, level: 1,
    })),
    ...Array.from({ length: numFounders }, (_, i) => ({
      id: `school:rs${i}`, name: `School RS${i}`, category: 'school_entity' as const, level: 2,
    })),
  ];
  const regionInst: InstitutionSummary[] = Array.from({ length: numFounders }, (_, i) => ({
    id: `school:rs${i}`, name: `School RS${i}`,
    founderName: `Founder RF${i}`, region: 'Plzeňský',
  }));
  const regionLinks: SankeyLink[] = [
    link('state:cr', 'msmt', 3_000_000, 'state_to_ministry'),
    ...Array.from({ length: numFounders }, (_, i) =>
      link('msmt', `school:rs${i}`, (numFounders - i) * 10_000, 'direct_school_finance', { institutionId: `school:rs${i}` })
    ),
    ...Array.from({ length: numFounders }, (_, i) =>
      link(`school:rs${i}`, 'bucket:wages', (numFounders - i) * 8_000, 'school_expenditure', { institutionId: `school:rs${i}` })
    ),
  ];
  const regionDs: YearDataset = {
    year: 2025, currency: 'CZK', title: 'Region window test', sources: [],
    nodes: regionNodes, links: regionLinks, institutions: regionInst,
  };

  it('offset=0: next-window present for founders exceeding TOP_FOUNDERS', () => {
    const g = drillRegion(regionDs, 'Plzeňský', 0);
    const ids = new Set(g.nodes.map((n) => n.id));
    expect(ids.has(NEXT_WINDOW_ID)).toBe(true);
    expect(ids.has(PREV_WINDOW_ID)).toBe(false);
  });

  it('offset=25: prev-window present', () => {
    const g = drillRegion(regionDs, 'Plzeňský', 25);
    const ids = new Set(g.nodes.map((n) => n.id));
    expect(ids.has(PREV_WINDOW_ID)).toBe(true);
  });

  it('window contains at most TOP_FOUNDERS individual founders', () => {
    const g = drillRegion(regionDs, 'Plzeňský', 0);
    const founderCount = g.nodes.filter(
      (n) => n.category === 'municipality' || n.category === 'region'
    ).length;
    expect(founderCount).toBeLessThanOrEqual(25);
  });
});

// ── per-pupil capacity propagation ───────────────────────────────────────────
// Shared capacity fixture: institutions with capacity set
const capInst: InstitutionSummary[] = [
  { id: 'school:s1', name: 'School 1', founderName: 'Municipality A', founderType: 'obec', region: 'Středočeský', capacity: 100 },
  { id: 'school:s2', name: 'School 2', founderName: 'Municipality A', founderType: 'obec', region: 'Středočeský', capacity: 80 },
  { id: 'school:s3', name: 'School 3', founderName: 'Středočeský kraj', founderType: 'kraj', region: 'Praha',      capacity: 60 },
];
// School nodes with metadata.capacity (as build_school_year.py emits them)
const capNodes: SankeyNode[] = [
  { id: 'state:cr',        name: 'State budget',    category: 'state',        level: 0 },
  { id: 'msmt',            name: 'MŠMT',            category: 'ministry',     level: 1 },
  { id: 'founder:muni1',   name: 'Municipality A',  category: 'municipality', level: 1 },
  { id: 'founder:region1', name: 'Středočeský kraj', category: 'region',      level: 1 },
  { id: 'school:s1', name: 'School 1', category: 'school_entity', level: 2, metadata: { capacity: 100 } },
  { id: 'school:s2', name: 'School 2', category: 'school_entity', level: 2, metadata: { capacity: 80  } },
  { id: 'school:s3', name: 'School 3', category: 'school_entity', level: 2, metadata: { capacity: 60  } },
  { id: 'bucket:wages', name: 'Wages',      category: 'cost_bucket', level: 3 },
  { id: 'bucket:ops',   name: 'Operations', category: 'cost_bucket', level: 3 },
];
const capDatasetFull: YearDataset = { ...dataset, nodes: capNodes, institutions: capInst };

describe('per-pupil — aggregateGraph: state:cr and msmt get total capacity', () => {
  it('state:cr carries total capacity = 240 (100+80+60)', () => {
    const g = aggregateGraph(capDatasetFull);
    const stateCr = g.nodes.find((n) => n.id === 'state:cr');
    expect(stateCr?.metadata?.capacity).toBe(240);
  });

  it('msmt carries the same total capacity', () => {
    const g = aggregateGraph(capDatasetFull);
    const msmt = g.nodes.find((n) => n.id === 'msmt');
    expect(msmt?.metadata?.capacity).toBe(240);
  });

  it('region nodes carry per-region capacity', () => {
    const g = aggregateGraph(capDatasetFull);
    const sc = g.nodes.find((n) => n.id === 'region:Středočeský');
    const pr = g.nodes.find((n) => n.id === 'region:Praha');
    expect(sc?.metadata?.capacity).toBe(180);
    expect(pr?.metadata?.capacity).toBe(60);
  });
});

describe('per-pupil — drillRegion: founder nodes carry per-founder capacity', () => {
  it('founder node has capacity = sum of its schools in region', () => {
    const g = drillRegion(capDatasetFull, 'Středočeský');
    const muni1 = g.nodes.find((n) => n.id === 'founder:muni1');
    expect(muni1?.metadata?.capacity).toBe(180); // s1(100) + s2(80)
  });

  it('window nodes carry capacity of their aggregated founders', () => {
    // Build a region with more founders than TOP_FOUNDERS
    const n = 30;
    const manyInst: InstitutionSummary[] = Array.from({ length: n }, (_, i) => ({
      id: `school:rw${i}`, name: `School RW${i}`,
      founderName: `Founder RW${i}`, founderType: 'obec', region: 'Plzeňský',
      capacity: 50,
    }));
    const manyNodes: SankeyNode[] = [
      { id: 'msmt', name: 'MŠMT', category: 'ministry', level: 1 },
      { id: 'bucket:wages', name: 'Wages', category: 'cost_bucket', level: 3 },
      ...Array.from({ length: n }, (_, i) => ({
        id: `founder:rw${i}`, name: `Founder RW${i}`, category: 'municipality' as const, level: 1,
      })),
      ...Array.from({ length: n }, (_, i) => ({
        id: `school:rw${i}`, name: `School RW${i}`, category: 'school_entity' as const, level: 2,
        metadata: { capacity: 50 },
      })),
    ];
    const manyLinks: SankeyLink[] = [
      ...Array.from({ length: n }, (_, i) =>
        link('msmt', `school:rw${i}`, (n - i) * 10_000, 'direct_school_finance', { institutionId: `school:rw${i}` })
      ),
      ...Array.from({ length: n }, (_, i) =>
        link(`school:rw${i}`, 'bucket:wages', (n - i) * 8_000, 'school_expenditure', { institutionId: `school:rw${i}` })
      ),
    ];
    const ds: YearDataset = { year: 2025, currency: 'CZK', title: 'T', sources: [], nodes: manyNodes, links: manyLinks, institutions: manyInst };
    const g = drillRegion(ds, 'Plzeňský', 0);
    const nextNode = g.nodes.find((n) => n.id === NEXT_WINDOW_ID);
    // 5 overflow founders × 50 capacity each = 250
    expect(nextNode?.metadata?.capacity).toBe(250);
  });
});

describe('per-pupil — drillFounder: school nodes keep individual capacity; window nodes aggregate', () => {
  it('school nodes in window retain their own metadata.capacity', () => {
    const g = drillFounder(capDatasetFull, 'Municipality A');
    const s1 = g.nodes.find((n) => n.id === 'school:s1');
    const s2 = g.nodes.find((n) => n.id === 'school:s2');
    expect(s1?.metadata?.capacity).toBe(100);
    expect(s2?.metadata?.capacity).toBe(80);
  });

  it('next-window node carries sum of overflow school capacities', () => {
    const n = 35; // more than TOP_SCHOOLS=30
    const inst: InstitutionSummary[] = Array.from({ length: n }, (_, i) => ({
      id: `school:fw${i}`, name: `School FW${i}`, founderName: 'Big Obec',
      founderType: 'obec', region: 'Jihomoravský', capacity: 40,
    }));
    const schoolNodes: SankeyNode[] = Array.from({ length: n }, (_, i) => ({
      id: `school:fw${i}`, name: `School FW${i}`, category: 'school_entity' as const,
      level: 2, metadata: { capacity: 40 },
    }));
    const ls: SankeyLink[] = [
      ...Array.from({ length: n }, (_, i) =>
        link('msmt', `school:fw${i}`, (n - i) * 10_000, 'direct_school_finance', { institutionId: `school:fw${i}` })
      ),
      ...Array.from({ length: n }, (_, i) =>
        link(`school:fw${i}`, 'bucket:wages', (n - i) * 8_000, 'school_expenditure', { institutionId: `school:fw${i}` })
      ),
    ];
    const ds: YearDataset = {
      year: 2025, currency: 'CZK', title: 'T', sources: [],
      nodes: [{ id: 'msmt', name: 'MŠMT', category: 'ministry', level: 1 }, { id: 'bucket:wages', name: 'Wages', category: 'cost_bucket', level: 3 }, ...schoolNodes],
      links: ls, institutions: inst,
    };
    const g = drillFounder(ds, 'Big Obec', 0);
    const nextNode = g.nodes.find((n) => n.id === NEXT_WINDOW_ID);
    // 5 overflow schools × 40 capacity = 200
    expect(nextNode?.metadata?.capacity).toBe(200);
  });
});

describe('per-pupil — drillFounderType: founder nodes carry per-founder capacity', () => {
  const founderCapNodes: SankeyNode[] = [
    ...capNodes,
    { id: 'founder:muni1',   name: 'Municipality A',  category: 'municipality', level: 1 },
    { id: 'founder:region1', name: 'Středočeský kraj', category: 'region',       level: 1 },
  ];
  const founderCapLinks: SankeyLink[] = [
    ...links,
    link('founder:muni1',   'school:s1', 80_000, 'founder_support', { institutionId: 'school:s1' }),
    link('founder:muni1',   'school:s2', 60_000, 'founder_support', { institutionId: 'school:s2' }),
    link('founder:region1', 'school:s3', 40_000, 'founder_support', { institutionId: 'school:s3' }),
  ];
  const founderCapDataset: YearDataset = { ...dataset, nodes: founderCapNodes, links: founderCapLinks, institutions: capInst };

  it('obec founder node has capacity = sum of its schools', () => {
    const g = drillFounderType(founderCapDataset, 'obec');
    const muni1 = g.nodes.find((n) => n.id === 'founder:muni1');
    expect(muni1?.metadata?.capacity).toBe(180); // s1(100) + s2(80)
  });

  it('kraj founder node has capacity = its school capacity', () => {
    const g = drillFounderType(founderCapDataset, 'kraj');
    const region1 = g.nodes.find((n) => n.id === 'founder:region1');
    expect(region1?.metadata?.capacity).toBe(60); // s3(60)
  });
});
