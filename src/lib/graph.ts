import type { Filters, SankeyLink, SankeyNode, YearDataset } from '../types';

export interface FilteredGraph {
  nodes: SankeyNode[];
  links: SankeyLink[];
  totalFunding: number;
  totalSpending: number;
  observedShare: number;
}

const FUNDING_FLOW_TYPES = new Set([
  'state_revenue',
  'state_to_ministry',
  'direct_school_finance',
  'eu_project_support',
  'project_to_school',
  'founder_support',
]);

const SPENDING_FLOW_TYPES = new Set(['school_expenditure']);

const MSMT_ID = 'msmt';
export const TOP_SCHOOLS  = 30;
export const TOP_FOUNDERS = 25;
export const PREV_WINDOW_ID = 'synthetic:prev-window';
export const NEXT_WINDOW_ID = 'synthetic:next-window';
export const EU_ALL_ID       = 'eu:all';
export const FOUNDERS_KRAJ   = 'founders:kraj';
export const FOUNDERS_OBEC   = 'founders:obec';

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
// Top-level view: all four funding sources → geographic regions → cost buckets.
//
// Left:   state:cr → msmt                (MŠMT direct school finance)
//         eu:all                         (EU structural funds aggregate)
//         founders:kraj                  (all kraj-founded school support)
//         founders:obec                  (all municipality-founded school support)
// Middle: region:X (geographic regions)
// Right:  bucket:* (MŠMT expenditure breakdown; EU+founder money shows as unaccounted slack)

export function aggregateGraph(dataset: YearDataset): FilteredGraph {
  const schoolToRegion = new Map<string, string>();
  const regionDisplayNames = new Map<string, string>();
  const schoolFounderType = new Map<string, string>(); // schoolId → 'kraj' | 'obec' | other
  const regionCapacity = new Map<string, number>(); // regionId → sum of school capacities
  let obecCapacity = 0;  // total pupil capacity across all obec-founded schools
  let krajCapacity = 0;  // total pupil capacity across all kraj-founded schools

  for (const inst of dataset.institutions) {
    if (!inst.region) continue;
    const regionId = `region:${inst.region}`;
    schoolToRegion.set(inst.id, regionId);
    regionDisplayNames.set(regionId, inst.region);
    if (inst.founderType) schoolFounderType.set(inst.id, inst.founderType);
    if (inst.capacity) {
      regionCapacity.set(regionId, (regionCapacity.get(regionId) ?? 0) + inst.capacity);
      if (inst.founderType === 'obec') obecCapacity += inst.capacity;
      else if (inst.founderType === 'kraj') krajCapacity += inst.capacity;
    }
  }

  const msmtToRegion      = new Map<string, number>();
  const regionToBucket    = new Map<string, number>(); // key: `${rid}|${bucketId}`
  const euToRegion        = new Map<string, number>(); // all EU → region (via project_to_school)
  const krajToRegion      = new Map<string, number>(); // kraj founders → region
  const obecToRegion      = new Map<string, number>(); // obec founders → region

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
      // Aggregate all EU flows into eu:all → region (individual programme detail shown in drillEU)
      const rid = schoolToRegion.get(link.target);
      if (rid) euToRegion.set(rid, (euToRegion.get(rid) ?? 0) + link.amountCzk);
    } else if (link.flowType === 'founder_support') {
      // link.target = school (founder_support: source=founder, target=school)
      const rid = schoolToRegion.get(link.target);
      const ftype = schoolFounderType.get(link.target);
      if (rid) {
        if (ftype === 'kraj') {
          krajToRegion.set(rid, (krajToRegion.get(rid) ?? 0) + link.amountCzk);
        } else {
          obecToRegion.set(rid, (obecToRegion.get(rid) ?? 0) + link.amountCzk);
        }
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
  for (const [rid, amount] of euToRegion) {
    syntheticLinks.push(syntheticLink(EU_ALL_ID, rid, amount, 'eu_project_support', dataset.year));
  }
  for (const [rid, amount] of krajToRegion) {
    syntheticLinks.push(syntheticLink(FOUNDERS_KRAJ, rid, amount, 'founder_support', dataset.year));
  }
  for (const [rid, amount] of obecToRegion) {
    syntheticLinks.push(syntheticLink(FOUNDERS_OBEC, rid, amount, 'founder_support', dataset.year));
  }

  const syntheticRegionNodes: SankeyNode[] = [...regionDisplayNames.entries()]
    .filter(([id]) => msmtToRegion.has(id))
    .map(([id, name]) => {
      const cap = regionCapacity.get(id);
      return { id, name, category: 'region' as const, level: 2, ...(cap ? { metadata: { capacity: cap } } : {}) };
    });

  const extraNodes: SankeyNode[] = [];
  if ([...euToRegion.values()].some((v) => v > 0))
    extraNodes.push({ id: EU_ALL_ID, name: 'EU strukturální fondy', category: 'eu_programme', level: 0 });
  if ([...krajToRegion.values()].some((v) => v > 0))
    extraNodes.push({ id: FOUNDERS_KRAJ, name: 'Příspěvky krajů (provoz)', category: 'region',       level: 0, ...(krajCapacity ? { metadata: { capacity: krajCapacity } } : {}) });
  if ([...obecToRegion.values()].some((v) => v > 0))
    extraNodes.push({ id: FOUNDERS_OBEC, name: 'Příspěvky obcí (provoz)',  category: 'municipality', level: 0, ...(obecCapacity ? { metadata: { capacity: obecCapacity } } : {}) });

  // State → founders links: the operational school budgets of obec/kraj are largely funded
  // through state fiscal transfers (shared taxes, RUD). Show this connection and reduce
  // state:other by the same amount to keep state:cr balanced.
  const totalObecSupport = [...obecToRegion.values()].reduce((a, b) => a + b, 0);
  const totalKrajSupport = [...krajToRegion.values()].reduce((a, b) => a + b, 0);
  const founderTransfer  = totalObecSupport + totalKrajSupport;

  if (totalObecSupport > 0)
    syntheticLinks.push(syntheticLink('state:cr', FOUNDERS_OBEC, totalObecSupport, 'state_to_founders', dataset.year));
  if (totalKrajSupport > 0)
    syntheticLinks.push(syntheticLink('state:cr', FOUNDERS_KRAJ, totalKrajSupport, 'state_to_founders', dataset.year));

  // Passthrough state budget flows; adjust state_to_other so state:cr stays balanced.
  const stateBudgetLinks = dataset.links
    .filter((l) => l.flowType === 'state_revenue' || l.flowType === 'state_to_ministry' || l.flowType === 'state_to_other')
    .map((l) => {
      if (l.flowType === 'state_to_other' && founderTransfer > 0) {
        const adjusted = Math.max(0, l.amountCzk - founderTransfer);
        return { ...l, amountCzk: adjusted, value: adjusted };
      }
      return l;
    });

  const allLinks = [...syntheticLinks, ...stateBudgetLinks];
  const allUsedIds = nodeIdsFromLinks(allLinks);

  // Total pupil capacity across all regions — used as per-pupil denominator for
  // ministry-level and state-level links that have no finer-grained capacity.
  const totalCapacity = [...regionCapacity.values()].reduce((a, b) => a + b, 0);

  const allRealNodes = dataset.nodes
    .filter((n) => allUsedIds.has(n.id))
    .map((n) => {
      // Attach total capacity to state:cr and msmt so per-pupil works on top-level flows
      if (totalCapacity > 0 && (n.id === 'state:cr' || n.id === MSMT_ID))
        return { ...n, metadata: { ...(n.metadata ?? {}), capacity: totalCapacity } };
      return n;
    });

  return {
    nodes: [...allRealNodes, ...syntheticRegionNodes, ...extraNodes],
    links: allLinks,
    ...computeTotals(allLinks),
    observedShare: 1,
  };
}

// ── drillEU ───────────────────────────────────────────────────────────────────
// eu:all click → individual EU programmes → geographic regions

export function drillEU(dataset: YearDataset): FilteredGraph {
  const schoolToRegion = new Map<string, string>();
  const regionDisplayNames = new Map<string, string>();
  for (const inst of dataset.institutions) {
    if (!inst.region) continue;
    const regionId = `region:${inst.region}`;
    schoolToRegion.set(inst.id, regionId);
    regionDisplayNames.set(regionId, inst.region);
  }

  const projectToProgramme = new Map<string, string>();
  for (const link of dataset.links) {
    if (link.flowType === 'eu_project_support') projectToProgramme.set(link.target, link.source);
  }

  const euProgrammeToRegion = new Map<string, number>(); // key: `${progId}|${rid}`
  for (const link of dataset.links) {
    if (link.flowType === 'project_to_school') {
      const rid = schoolToRegion.get(link.target);
      const progId = projectToProgramme.get(link.source);
      if (rid && progId) {
        const key = `${progId}|${rid}`;
        euProgrammeToRegion.set(key, (euProgrammeToRegion.get(key) ?? 0) + link.amountCzk);
      }
    }
  }

  const syntheticLinks: SankeyLink[] = [];
  for (const [key, amount] of euProgrammeToRegion) {
    const [progId, rid] = key.split('|');
    syntheticLinks.push(syntheticLink(progId, rid, amount, 'eu_project_support', dataset.year));
  }

  const usedIds = nodeIdsFromLinks(syntheticLinks);
  const realNodes = dataset.nodes.filter((n) => usedIds.has(n.id));
  const syntheticRegionNodes: SankeyNode[] = [...regionDisplayNames.entries()]
    .filter(([id]) => syntheticLinks.some((l) => l.target === id))
    .map(([id, name]) => ({ id, name, category: 'region' as const, level: 1 }));

  return {
    nodes: [...realNodes, ...syntheticRegionNodes],
    links: syntheticLinks,
    ...computeTotals(syntheticLinks),
    observedShare: 1,
  };
}

// ── drillFounderType ──────────────────────────────────────────────────────────
// founders:kraj / founders:obec click → individual founders of that type (windowed)

export function drillFounderType(
  dataset: YearDataset,
  founderType: 'kraj' | 'obec',
  offset = 0,
): FilteredGraph {
  const schoolsOfType = new Set(
    dataset.institutions.filter((i) => i.founderType === founderType).map((i) => i.id)
  );

  const founderById = new Map(dataset.nodes
    .filter((n) => n.category === 'municipality' || n.category === 'region')
    .map((n) => [n.id, n]));

  const schoolToFounder = new Map<string, string>();
  for (const inst of dataset.institutions) {
    if (!schoolsOfType.has(inst.id)) continue;
    const founderNode = [...founderById.values()].find((n) => n.name === inst.founderName);
    if (founderNode) schoolToFounder.set(inst.id, founderNode.id);
  }

  // Per-pupil: sum school capacities per founder
  const founderCapacity = new Map<string, number>();
  for (const inst of dataset.institutions) {
    if (!schoolsOfType.has(inst.id) || !inst.capacity) continue;
    const fid = schoolToFounder.get(inst.id);
    if (fid) founderCapacity.set(fid, (founderCapacity.get(fid) ?? 0) + inst.capacity);
  }

  // Aggregate founder_support per founder
  const founderToTotal = new Map<string, number>();
  for (const link of dataset.links) {
    if (link.flowType !== 'founder_support') continue;
    if (!schoolsOfType.has(link.target)) continue;
    const founderId = schoolToFounder.get(link.target) ?? link.source;
    founderToTotal.set(founderId, (founderToTotal.get(founderId) ?? 0) + link.amountCzk);
  }

  const sorted = [...founderToTotal.entries()].sort((a, b) => b[1] - a[1]);
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

  const aggNodeId = founderType === 'kraj' ? FOUNDERS_KRAJ : FOUNDERS_OBEC;
  const syntheticLinks: SankeyLink[] = [];

  for (const [founderId, amount] of founderToTotal) {
    const target = bucket(founderId);
    if (target === PREV_WINDOW_ID && !prevCount) continue;
    if (target === NEXT_WINDOW_ID && !nextCount) continue;
    syntheticLinks.push(syntheticLink(aggNodeId, target, amount, 'founder_support', dataset.year));
  }

  const usedIds = nodeIdsFromLinks(syntheticLinks);
  const realNodes = dataset.nodes
    .filter((n) => usedIds.has(n.id))
    .map((n) => {
      const cap = founderCapacity.get(n.id);
      return cap ? { ...n, metadata: { ...(n.metadata ?? {}), capacity: cap } } : n;
    });

  const prevCapacity = [...prevIds].reduce((s, id) => s + (founderCapacity.get(id) ?? 0), 0);
  const nextCapacity = [...nextIds].reduce((s, id) => s + (founderCapacity.get(id) ?? 0), 0);

  const aggNode: SankeyNode = {
    id: aggNodeId,
    name: founderType === 'kraj' ? 'Příspěvky krajů (provoz)' : 'Příspěvky obcí (provoz)',
    category: founderType === 'kraj' ? 'region' : 'municipality',
    level: 0,
  };

  const nodes: SankeyNode[] = [aggNode, ...realNodes];
  if (prevCount) nodes.push(prevCapacity ? { ...prevWindowNode(prevCount, 'founders'), metadata: { capacity: prevCapacity } } : prevWindowNode(prevCount, 'founders'));
  if (nextCount) nodes.push(nextCapacity ? { ...nextWindowNode(nextCount, 'founders'), metadata: { capacity: nextCapacity } } : nextWindowNode(nextCount, 'founders'));

  return { nodes, links: syntheticLinks, ...computeTotals(syntheticLinks), observedShare: 1 };
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
  const founderTypeMap  = new Map<string, string>(); // founderId → 'kraj' | 'obec'
  for (const inst of dataset.institutions) {
    if (!schoolsInRegion.has(inst.id)) continue;
    const founderNode = [...founderById.values()].find((n) => n.name === inst.founderName);
    if (founderNode) {
      schoolToFounder.set(inst.id, founderNode.id);
      if (inst.founderType) founderTypeMap.set(founderNode.id, inst.founderType);
    }
  }

  // Per-pupil: sum school capacities per founder within this region
  const founderCapacity = new Map<string, number>();
  for (const inst of dataset.institutions) {
    if (!schoolsInRegion.has(inst.id) || !inst.capacity) continue;
    const fid = schoolToFounder.get(inst.id);
    if (fid) founderCapacity.set(fid, (founderCapacity.get(fid) ?? 0) + inst.capacity);
  }

  const msmtToFounder         = new Map<string, number>();
  const founderToBucket        = new Map<string, number>();
  const euProgrammeToFounder   = new Map<string, number>();
  const founderSupportToFounder = new Map<string, number>(); // city/region operational contribution
  const projectToProgramme     = new Map<string, string>();

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
    } else if (link.flowType === 'founder_support' && schoolsInRegion.has(link.target)) {
      const fid = schoolToFounder.get(link.target);
      if (fid) founderSupportToFounder.set(fid, (founderSupportToFounder.get(fid) ?? 0) + link.amountCzk);
    }
  }

  // Sort by total inflow (MŠMT + founder_support) so ranking reflects full funding picture
  const totalToFounder = new Map<string, number>();
  for (const [fid, amt] of msmtToFounder)          totalToFounder.set(fid, (totalToFounder.get(fid) ?? 0) + amt);
  for (const [fid, amt] of founderSupportToFounder) totalToFounder.set(fid, (totalToFounder.get(fid) ?? 0) + amt);
  for (const [key] of euProgrammeToFounder) { const [, fid] = key.split('|'); if (!totalToFounder.has(fid)) totalToFounder.set(fid, 0); }

  const sorted = [...totalToFounder.entries()].sort((a, b) => b[1] - a[1]);
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

  // Founder operational contributions (city/region own budget → schools)
  let hasFounderKraj = false, hasFounderObec = false;
  for (const [fid, amount] of founderSupportToFounder) {
    const target = bucket(fid);
    if (target === PREV_WINDOW_ID && !prevCount) continue;
    if (target === NEXT_WINDOW_ID && !nextCount) continue;
    const ftype = founderTypeMap.get(fid) ?? 'obec';
    const source = ftype === 'kraj' ? FOUNDERS_KRAJ : FOUNDERS_OBEC;
    if (ftype === 'kraj') hasFounderKraj = true; else hasFounderObec = true;
    syntheticLinks.push(syntheticLink(source, target, amount, 'founder_support', dataset.year));
  }

  const usedIds = nodeIdsFromLinks(syntheticLinks);
  const realNodes = dataset.nodes
    .filter((n) => usedIds.has(n.id))
    .map((n) => {
      const cap = founderCapacity.get(n.id);
      return cap ? { ...n, metadata: { ...(n.metadata ?? {}), capacity: cap } } : n;
    });

  const prevCapacity = [...prevIds].reduce((s, id) => s + (founderCapacity.get(id) ?? 0), 0);
  const nextCapacity = [...nextIds].reduce((s, id) => s + (founderCapacity.get(id) ?? 0), 0);

  const nodes: SankeyNode[] = [...realNodes];
  if (prevCount) nodes.push(prevCapacity ? { ...prevWindowNode(prevCount, 'founders'), metadata: { capacity: prevCapacity } } : prevWindowNode(prevCount, 'founders'));
  if (nextCount) nodes.push(nextCapacity ? { ...nextWindowNode(nextCount, 'founders'), metadata: { capacity: nextCapacity } } : nextWindowNode(nextCount, 'founders'));
  // FOUNDERS_KRAJ/OBEC are synthetic (not in dataset.nodes); add them explicitly
  if (hasFounderKraj) nodes.push({ id: FOUNDERS_KRAJ, name: 'Příspěvky krajů (provoz)', category: 'region',       level: 0 });
  if (hasFounderObec) nodes.push({ id: FOUNDERS_OBEC, name: 'Příspěvky obcí (provoz)',  category: 'municipality', level: 0 });

  return { nodes, links: syntheticLinks, ...computeTotals(syntheticLinks), observedShare: 1 };
}


// ── drillFounder ──────────────────────────────────────────────────────────────
// Third level: MŠMT → top N schools under one founder → Cost buckets.

export function drillFounder(dataset: YearDataset, founderName: string, offset = 0): FilteredGraph {
  const institutionIds = new Set(
    dataset.institutions.filter((i) => i.founderName === founderName).map((i) => i.id)
  );

  // Founder node — used as source for founder_support flows (city's own operational contribution)
  const founderNode = dataset.nodes.find(
    (n) => (n.category === 'municipality' || n.category === 'region') && n.name === founderName
  );

  // Sort by total inflow (MŠMT + founder support) so schools with no MŠMT data still appear
  const schoolInflow = new Map<string, number>();
  for (const link of dataset.links) {
    if ((link.flowType === 'direct_school_finance' || link.flowType === 'founder_support') && institutionIds.has(link.target))
      schoolInflow.set(link.target, (schoolInflow.get(link.target) ?? 0) + link.amountCzk);
  }

  // Per-pupil: capacity for window aggregate nodes
  const instCapacity = new Map<string, number>(
    dataset.institutions
      .filter((i) => institutionIds.has(i.id) && i.capacity)
      .map((i) => [i.id, i.capacity!]),
  );

  const sorted = [...schoolInflow.entries()].sort((a, b) => b[1] - a[1]);
  const windowIds = new Set(sorted.slice(offset, offset + TOP_SCHOOLS).map(([id]) => id));
  const prevIds   = new Set(sorted.slice(0, offset).map(([id]) => id));
  const nextIds   = new Set(sorted.slice(offset + TOP_SCHOOLS).map(([id]) => id));
  const prevCount = prevIds.size;
  const nextCount = nextIds.size;

  const prevCapacity = [...prevIds].reduce((s, id) => s + (instCapacity.get(id) ?? 0), 0);
  const nextCapacity = [...nextIds].reduce((s, id) => s + (instCapacity.get(id) ?? 0), 0);

  function bucket(schoolId: string): string {
    if (windowIds.has(schoolId)) return schoolId;
    if (prevIds.has(schoolId))   return PREV_WINDOW_ID;
    return NEXT_WINDOW_ID;
  }

  const projectToProgramme = new Map<string, string>();
  for (const link of dataset.links) {
    if (link.flowType === 'eu_project_support') projectToProgramme.set(link.target, link.source);
  }

  const msmtToSchool        = new Map<string, number>();
  const schoolToBucket      = new Map<string, number>();
  const euProgToSchool      = new Map<string, number>();
  const founderSupportToSchool = new Map<string, number>(); // city's own operational contribution

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
    } else if (link.flowType === 'founder_support' && institutionIds.has(link.target)) {
      const target = bucket(link.target);
      if (target === PREV_WINDOW_ID && !prevCount) continue;
      if (target === NEXT_WINDOW_ID && !nextCount) continue;
      founderSupportToSchool.set(target, (founderSupportToSchool.get(target) ?? 0) + link.amountCzk);
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
  // Founder's own operational contribution (city/region budget → each school)
  if (founderNode) {
    for (const [schoolId, amount] of founderSupportToSchool)
      syntheticLinks.push(syntheticLink(founderNode.id, schoolId, amount, 'founder_support', dataset.year));
  }

  const usedIds = nodeIdsFromLinks(syntheticLinks);
  const realNodes = dataset.nodes.filter((n) => usedIds.has(n.id));
  const nodes: SankeyNode[] = [...realNodes];
  if (prevCount) nodes.push(prevCapacity ? { ...prevWindowNode(prevCount, 'schools'), metadata: { capacity: prevCapacity } } : prevWindowNode(prevCount, 'schools'));
  if (nextCount) nodes.push(nextCapacity ? { ...nextWindowNode(nextCount, 'schools'), metadata: { capacity: nextCapacity } } : nextWindowNode(nextCount, 'schools'));

  return { nodes, links: syntheticLinks, ...computeTotals(syntheticLinks), observedShare: 1 };
}

// ── drillIntoNode ─────────────────────────────────────────────────────────────

export function drillIntoNode(dataset: YearDataset, nodeId: string, offset = 0): FilteredGraph {
  if (nodeId.startsWith('region:')) {
    return drillRegion(dataset, nodeId.replace('region:', ''), offset);
  }
  if (nodeId === EU_ALL_ID)     return drillEU(dataset);
  if (nodeId === FOUNDERS_KRAJ) return drillFounderType(dataset, 'kraj', offset);
  if (nodeId === FOUNDERS_OBEC) return drillFounderType(dataset, 'obec', offset);

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
