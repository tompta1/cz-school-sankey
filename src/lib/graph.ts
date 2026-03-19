import type { Filters, SankeyLink, SankeyNode, YearDataset } from '../types';

export interface FilteredGraph {
  nodes: SankeyNode[];
  links: SankeyLink[];
  totalFunding: number;
  totalSpending: number;
  observedShare: number;
}

const FUNDING_FLOW_TYPES = new Set([
  'state_to_ministry',
  'direct_school_finance',
  'eu_project_support',
  'project_to_school',
  'founder_support'
]);

const SPENDING_FLOW_TYPES = new Set(['school_expenditure']);

const MSMT_ID = 'msmt';
export const TOP_SCHOOLS  = 30;
export const TOP_FOUNDERS = 25;
export const PREV_WINDOW_ID = 'synthetic:prev-window';
export const NEXT_WINDOW_ID = 'synthetic:next-window';

function prevWindowNode(count: number, label: string): SankeyNode {
  return { id: PREV_WINDOW_ID, name: `↑ ${count} more ${label}`, category: 'other', level: 2 };
}
function nextWindowNode(count: number, label: string): SankeyNode {
  return { id: NEXT_WINDOW_ID, name: `↓ ${count} more ${label}`, category: 'other', level: 2 };
}

// ── helpers ──────────────────────────────────────────────────────────────────

function syntheticLink(
  source: string, target: string, amountCzk: number,
  flowType: string, year: number
): SankeyLink {
  return {
    source, target, value: amountCzk, amountCzk,
    year, flowType,
    basis: flowType === 'school_expenditure' ? 'realized' : 'allocated',
    certainty: 'observed',
    sourceDataset: 'aggregated',
  };
}

function computeTotals(links: SankeyLink[]) {
  return {
    totalFunding: links.filter((l) => FUNDING_FLOW_TYPES.has(l.flowType)).reduce((s, l) => s + l.amountCzk, 0),
    totalSpending: links.filter((l) => SPENDING_FLOW_TYPES.has(l.flowType)).reduce((s, l) => s + l.amountCzk, 0),
  };
}

function nodeIdsFromLinks(links: SankeyLink[]): Set<string> {
  const ids = new Set<string>();
  for (const l of links) { ids.add(l.source); ids.add(l.target); }
  return ids;
}

// ── filterGraph ───────────────────────────────────────────────────────────────

export function filterGraph(dataset: YearDataset, filters: Filters): FilteredGraph {
  const relevantLinks = dataset.links.filter((link) => {
    if (filters.certainty !== 'all' && link.certainty !== filters.certainty) return false;
    if (link.amountCzk < filters.thresholdCzk) return false;
    if (filters.institutionId !== 'all' && link.institutionId !== filters.institutionId) {
      if (link.source !== filters.institutionId && link.target !== filters.institutionId) return false;
    }
    if (filters.flowView === 'funding' && !FUNDING_FLOW_TYPES.has(link.flowType)) return false;
    if (filters.flowView === 'spending' && !SPENDING_FLOW_TYPES.has(link.flowType)) return false;
    return true;
  });

  const usedIds = nodeIdsFromLinks(relevantLinks);
  const nodes = dataset.nodes.filter((n) => usedIds.has(n.id));
  const observedLinks = relevantLinks.filter((l) => l.certainty === 'observed');
  return {
    nodes,
    links: relevantLinks,
    ...computeTotals(relevantLinks),
    observedShare: relevantLinks.length === 0 ? 0 : observedLinks.length / relevantLinks.length,
  };
}

// ── aggregateGraph ────────────────────────────────────────────────────────────
// Default top-level view: State → MŠMT → Geographic Regions → Cost buckets

export function aggregateGraph(dataset: YearDataset): FilteredGraph {
  // school ID → geographic region ID (e.g. 'region:Středočeský')
  const schoolToRegion = new Map<string, string>();
  const regionDisplayNames = new Map<string, string>();

  for (const inst of dataset.institutions) {
    if (!inst.region) continue;
    const regionId = `region:${inst.region}`;
    schoolToRegion.set(inst.id, regionId);
    regionDisplayNames.set(regionId, inst.region);
  }

  const msmtToRegion = new Map<string, number>();
  const regionToBucket = new Map<string, number>();
  // EU: project_to_school flows, traced back to their programme via eu_project_support
  const projectToProgramme = new Map<string, string>();
  const euProgrammeToRegion = new Map<string, number>(); // key: `${progId}|${rid}`

  for (const link of dataset.links) {
    if (link.flowType === 'eu_project_support') {
      projectToProgramme.set(link.target, link.source);
    }
  }

  for (const link of dataset.links) {
    if (link.flowType === 'direct_school_finance') {
      const rid = schoolToRegion.get(link.target);
      if (rid) msmtToRegion.set(rid, (msmtToRegion.get(rid) ?? 0) + link.amountCzk);
    } else if (link.flowType === 'school_expenditure') {
      const rid = schoolToRegion.get(link.source);
      if (rid) {
        const key = `${rid}|${link.target}`;
        regionToBucket.set(key, (regionToBucket.get(key) ?? 0) + link.amountCzk);
      }
    } else if (link.flowType === 'project_to_school') {
      const rid = schoolToRegion.get(link.target);
      const progId = projectToProgramme.get(link.source);
      if (rid && progId) {
        const key = `${progId}|${rid}`;
        euProgrammeToRegion.set(key, (euProgrammeToRegion.get(key) ?? 0) + link.amountCzk);
      }
    }
  }

  const syntheticLinks: SankeyLink[] = [];

  for (const [rid, amount] of msmtToRegion) {
    syntheticLinks.push(syntheticLink(MSMT_ID, rid, amount, 'direct_school_finance', dataset.year));
  }
  for (const [key, amount] of regionToBucket) {
    const [rid, bucketId] = key.split('|');
    syntheticLinks.push(syntheticLink(rid, bucketId, amount, 'school_expenditure', dataset.year));
  }
  for (const [key, amount] of euProgrammeToRegion) {
    const [progId, rid] = key.split('|');
    syntheticLinks.push(syntheticLink(progId, rid, amount, 'eu_project_support', dataset.year));
  }

  const usedIds = nodeIdsFromLinks(syntheticLinks);
  const realNodes = dataset.nodes.filter((n) => usedIds.has(n.id));
  const syntheticRegionNodes: SankeyNode[] = [...regionDisplayNames.entries()]
    .filter(([id]) => msmtToRegion.has(id))
    .map(([id, name]) => ({ id, name, category: 'region' as const, level: 1 }));

  return {
    nodes: [...realNodes, ...syntheticRegionNodes],
    links: syntheticLinks,
    ...computeTotals(syntheticLinks),
    observedShare: 1,
  };
}

// ── drillRegion ───────────────────────────────────────────────────────────────
// Second level: MŠMT → windowed founders in a region → Cost buckets

export function drillRegion(dataset: YearDataset, regionName: string, offset = 0): FilteredGraph {
  const schoolsInRegion = new Set(
    dataset.institutions.filter((i) => i.region === regionName).map((i) => i.id)
  );

  const founderById = new Map(dataset.nodes
    .filter((n) => n.category === 'municipality' || n.category === 'region')
    .map((n) => [n.id, n]));

  const schoolToFounder = new Map<string, string>();
  for (const inst of dataset.institutions) {
    if (!schoolsInRegion.has(inst.id)) continue;
    const founderNode = [...founderById.values()].find((n) => n.name === inst.founderName);
    if (founderNode) schoolToFounder.set(inst.id, founderNode.id);
  }

  const msmtToFounder = new Map<string, number>();
  const founderToBucket = new Map<string, number>();
  const euProgrammeToFounder = new Map<string, number>();
  const projectToProgramme = new Map<string, string>();

  for (const link of dataset.links) {
    if (link.flowType === 'eu_project_support') projectToProgramme.set(link.target, link.source);
  }
  for (const link of dataset.links) {
    if (link.flowType === 'direct_school_finance' && schoolsInRegion.has(link.target)) {
      const fid = schoolToFounder.get(link.target);
      if (fid) msmtToFounder.set(fid, (msmtToFounder.get(fid) ?? 0) + link.amountCzk);
    } else if (link.flowType === 'school_expenditure' && schoolsInRegion.has(link.source)) {
      const fid = schoolToFounder.get(link.source);
      if (fid) founderToBucket.set(`${fid}|${link.target}`, (founderToBucket.get(`${fid}|${link.target}`) ?? 0) + link.amountCzk);
    } else if (link.flowType === 'project_to_school' && schoolsInRegion.has(link.target)) {
      const fid = schoolToFounder.get(link.target);
      const progId = projectToProgramme.get(link.source);
      if (fid && progId) euProgrammeToFounder.set(`${progId}|${fid}`, (euProgrammeToFounder.get(`${progId}|${fid}`) ?? 0) + link.amountCzk);
    }
  }

  const sorted = [...msmtToFounder.entries()].sort((a, b) => b[1] - a[1]);
  const windowIds = new Set(sorted.slice(offset, offset + TOP_FOUNDERS).map(([id]) => id));
  const prevIds   = new Set(sorted.slice(0, offset).map(([id]) => id));
  const nextIds   = new Set(sorted.slice(offset + TOP_FOUNDERS).map(([id]) => id));
  const prevCount = prevIds.size;
  const nextCount = nextIds.size;

  function bucket(fid: string): string {
    if (windowIds.has(fid)) return fid;
    if (prevIds.has(fid))   return PREV_WINDOW_ID;
    return NEXT_WINDOW_ID;
  }

  const syntheticLinks: SankeyLink[] = [];

  for (const [fid, amount] of msmtToFounder) {
    const target = bucket(fid);
    if (target === PREV_WINDOW_ID && !prevCount) continue;
    if (target === NEXT_WINDOW_ID && !nextCount) continue;
    syntheticLinks.push(syntheticLink(MSMT_ID, target, amount, 'direct_school_finance', dataset.year));
  }
  for (const [key, amount] of founderToBucket) {
    const [fid, bucketId] = key.split('|');
    const src = bucket(fid);
    if (src === PREV_WINDOW_ID && !prevCount) continue;
    if (src === NEXT_WINDOW_ID && !nextCount) continue;
    syntheticLinks.push(syntheticLink(src, bucketId, amount, 'school_expenditure', dataset.year));
  }
  for (const [key, amount] of euProgrammeToFounder) {
    const [progId, fid] = key.split('|');
    const target = bucket(fid);
    if (target === PREV_WINDOW_ID && !prevCount) continue;
    if (target === NEXT_WINDOW_ID && !nextCount) continue;
    syntheticLinks.push(syntheticLink(progId, target, amount, 'eu_project_support', dataset.year));
  }

  const usedIds = nodeIdsFromLinks(syntheticLinks);
  const realNodes = dataset.nodes.filter((n) => usedIds.has(n.id));
  const nodes: SankeyNode[] = [...realNodes];
  if (prevCount) nodes.push(prevWindowNode(prevCount, 'founders'));
  if (nextCount) nodes.push(nextWindowNode(nextCount, 'founders'));

  return { nodes, links: syntheticLinks, ...computeTotals(syntheticLinks), observedShare: 1 };
}

// ── drillFounder ──────────────────────────────────────────────────────────────
// Third level: MŠMT → top N schools under one founder → Cost buckets.
// Caps at TOP_SCHOOLS to keep ECharts manageable; the rest collapse into
// "Ostatní školy".

export function drillFounder(dataset: YearDataset, founderName: string, offset = 0): FilteredGraph {
  const institutionIds = new Set(
    dataset.institutions.filter((i) => i.founderName === founderName).map((i) => i.id)
  );

  const schoolInflow = new Map<string, number>();
  for (const link of dataset.links) {
    if (link.flowType === 'direct_school_finance' && institutionIds.has(link.target))
      schoolInflow.set(link.target, (schoolInflow.get(link.target) ?? 0) + link.amountCzk);
  }

  const sorted = [...schoolInflow.entries()].sort((a, b) => b[1] - a[1]);
  const windowIds = new Set(sorted.slice(offset, offset + TOP_SCHOOLS).map(([id]) => id));
  const prevIds   = new Set(sorted.slice(0, offset).map(([id]) => id));
  const nextIds   = new Set(sorted.slice(offset + TOP_SCHOOLS).map(([id]) => id));
  const prevCount = prevIds.size;
  const nextCount = nextIds.size;

  function bucket(schoolId: string): string {
    if (windowIds.has(schoolId)) return schoolId;
    if (prevIds.has(schoolId))   return PREV_WINDOW_ID;
    return NEXT_WINDOW_ID;
  }

  const projectToProgramme = new Map<string, string>();
  for (const link of dataset.links) {
    if (link.flowType === 'eu_project_support') projectToProgramme.set(link.target, link.source);
  }

  const msmtToSchool      = new Map<string, number>();
  const schoolToBucket    = new Map<string, number>();
  const euProgToSchool    = new Map<string, number>();

  for (const link of dataset.links) {
    if (link.flowType === 'direct_school_finance' && institutionIds.has(link.target)) {
      const target = bucket(link.target);
      if (target === PREV_WINDOW_ID && !prevCount) continue;
      if (target === NEXT_WINDOW_ID && !nextCount) continue;
      msmtToSchool.set(target, (msmtToSchool.get(target) ?? 0) + link.amountCzk);
    } else if (link.flowType === 'school_expenditure' && institutionIds.has(link.source)) {
      const src = bucket(link.source);
      if (src === PREV_WINDOW_ID && !prevCount) continue;
      if (src === NEXT_WINDOW_ID && !nextCount) continue;
      const key = `${src}|${link.target}`;
      schoolToBucket.set(key, (schoolToBucket.get(key) ?? 0) + link.amountCzk);
    } else if (link.flowType === 'project_to_school' && institutionIds.has(link.target)) {
      const progId = projectToProgramme.get(link.source);
      if (!progId) continue;
      const target = bucket(link.target);
      if (target === PREV_WINDOW_ID && !prevCount) continue;
      if (target === NEXT_WINDOW_ID && !nextCount) continue;
      const key = `${progId}|${target}`;
      euProgToSchool.set(key, (euProgToSchool.get(key) ?? 0) + link.amountCzk);
    }
  }

  const syntheticLinks: SankeyLink[] = [];
  for (const [schoolId, amount] of msmtToSchool)
    syntheticLinks.push(syntheticLink(MSMT_ID, schoolId, amount, 'direct_school_finance', dataset.year));
  for (const [key, amount] of schoolToBucket) {
    const [src, bucketId] = key.split('|');
    syntheticLinks.push(syntheticLink(src, bucketId, amount, 'school_expenditure', dataset.year));
  }
  for (const [key, amount] of euProgToSchool) {
    const [progId, schoolId] = key.split('|');
    syntheticLinks.push(syntheticLink(progId, schoolId, amount, 'eu_project_support', dataset.year));
  }

  const usedIds = nodeIdsFromLinks(syntheticLinks);
  const realNodes = dataset.nodes.filter((n) => usedIds.has(n.id));
  const nodes: SankeyNode[] = [...realNodes];
  if (prevCount) nodes.push(prevWindowNode(prevCount, 'schools'));
  if (nextCount) nodes.push(nextWindowNode(nextCount, 'schools'));

  return { nodes, links: syntheticLinks, ...computeTotals(syntheticLinks), observedShare: 1 };
}

// ── drillIntoNode ─────────────────────────────────────────────────────────────

export function drillIntoNode(dataset: YearDataset, nodeId: string, offset = 0): FilteredGraph {
  if (nodeId.startsWith('region:')) {
    return drillRegion(dataset, nodeId.replace('region:', ''), offset);
  }

  const node = dataset.nodes.find((n) => n.id === nodeId);
  const category = node?.category;

  const defaultFilters: Filters = {
    year: dataset.year, certainty: 'all', thresholdCzk: 0,
    institutionId: 'all', flowView: 'all',
  };

  if (!node || category === 'state' || category === 'ministry') {
    return filterGraph(dataset, defaultFilters);
  }

  let relevantLinks: SankeyLink[];

  if (category === 'school_entity') {
    relevantLinks = dataset.links.filter(
      (l) => l.institutionId === nodeId || l.source === nodeId || l.target === nodeId
    );
  } else if (category === 'municipality' || category === 'region') {
    return drillFounder(dataset, node.name, offset);
  } else if (category === 'cost_bucket') {
    const toBucket = dataset.links.filter((l) => l.target === nodeId);
    const schoolIds = new Set(toBucket.map((l) => l.source));
    const inboundMsmt = dataset.links.filter(
      (l) => schoolIds.has(l.target) && l.flowType === 'direct_school_finance'
    );
    relevantLinks = [...toBucket, ...inboundMsmt];
  } else {
    relevantLinks = dataset.links.filter((l) => l.source === nodeId || l.target === nodeId);
  }

  if (relevantLinks.length === 0) return filterGraph(dataset, defaultFilters);

  const usedIds = nodeIdsFromLinks(relevantLinks);
  const nodes = dataset.nodes.filter((n) => usedIds.has(n.id));
  const observedLinks = relevantLinks.filter((l) => l.certainty === 'observed');

  return {
    nodes, links: relevantLinks,
    ...computeTotals(relevantLinks),
    observedShare: relevantLinks.length === 0 ? 0 : observedLinks.length / relevantLinks.length,
  };
}
