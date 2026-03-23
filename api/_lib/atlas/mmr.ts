import { query } from '../db.js';

interface AtlasNode {
  id: string;
  name: string;
  category: string;
  level: number;
  ico?: string;
  metadata?: Record<string, string | number | boolean | null>;
}

interface AtlasLink {
  source: string;
  target: string;
  value: number;
  amountCzk: number;
  year: number;
  flowType: string;
  basis: string;
  certainty: string;
  sourceDataset: string;
  note?: string;
}

interface MmrBudgetAggregate {
  year: number;
  metricCode: string;
  metricName: string;
  metricGroup: string;
  amount: number;
  sourceDataset: string;
}

interface MmrRecipientMetric {
  year: number;
  branchCode: 'REGIONAL' | 'HOUSING';
  branchName: string;
  recipientCount: number;
  projectCount: number;
  allocatedTotal: number;
  sourceDataset: string;
}

interface MmrRegionMetric {
  year: number;
  branchCode: 'REGIONAL' | 'HOUSING';
  branchName: string;
  regionCode: string;
  regionName: string;
  recipientCount: number;
  projectCount: number;
  allocatedTotal: number;
  sourceDataset: string;
}

interface MmrRecipientAggregate {
  year: number;
  branchCode: 'REGIONAL' | 'HOUSING';
  branchName: string;
  regionCode: string;
  regionName: string;
  recipientKey: string;
  recipientName: string;
  recipientIco: string | null;
  projectCount: number;
  allocatedTotal: number;
  sourceDataset: string;
}

const STATE_ID = 'state:cr';
const MMR_MINISTRY_ID = 'mmr:ministry:mmr';
const MMR_BRANCH_REGION_ID = 'mmr:branch:regional';
const MMR_BRANCH_HOUSING_ID = 'mmr:branch:housing';
const MMR_BRANCH_PLANNING_ID = 'mmr:branch:planning';
const MMR_BRANCH_OTHER_ID = 'mmr:branch:other';
const MMR_REGION_PREFIX = 'mmr:region:';
const MMR_RECIPIENT_PREFIX = 'mmr:recipient:';
const PREV_WINDOW_ID = 'synthetic:prev-window';
const NEXT_WINDOW_ID = 'synthetic:next-window';
const PAGE_SIZE = 28;

function toNumber(value: unknown): number {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function addNode(nodes: AtlasNode[], node: AtlasNode): void {
  if (!nodes.some((entry) => entry.id === node.id)) {
    nodes.push(node);
  }
}

function makeLink(
  source: string,
  target: string,
  amount: number,
  year: number,
  flowType: string,
  note: string,
  sourceDataset = 'atlas.inferred',
): AtlasLink {
  return {
    source,
    target,
    value: amount,
    amountCzk: amount,
    year,
    flowType,
    basis: 'reported',
    certainty: sourceDataset === 'atlas.inferred' ? 'inferred' : 'observed',
    sourceDataset,
    note,
  };
}

function branchNodeId(branchCode: 'REGIONAL' | 'HOUSING'): string {
  return branchCode === 'HOUSING' ? MMR_BRANCH_HOUSING_ID : MMR_BRANCH_REGION_ID;
}

function branchBudgetCode(branchCode: 'REGIONAL' | 'HOUSING'): string {
  return branchCode === 'HOUSING' ? 'HOUSING_SUPPORT' : 'REGIONAL_SUPPORT';
}

function regionNodeId(branchCode: 'REGIONAL' | 'HOUSING', regionCode: string): string {
  return `${MMR_REGION_PREFIX}${branchCode.toLowerCase()}|${regionCode}`;
}

function recipientNodeId(branchCode: 'REGIONAL' | 'HOUSING', recipientKey: string): string {
  return `${MMR_RECIPIENT_PREFIX}${branchCode.toLowerCase()}|${recipientKey}`;
}

function parseRegionNodeId(nodeId: string): { branchCode: 'REGIONAL' | 'HOUSING'; regionCode: string } | null {
  if (!nodeId.startsWith(MMR_REGION_PREFIX)) return null;
  const payload = nodeId.slice(MMR_REGION_PREFIX.length);
  const [branchRaw, regionCode] = payload.split('|', 2);
  if (!regionCode) return null;
  const branchCode = branchRaw.toUpperCase() === 'HOUSING' ? 'HOUSING' : branchRaw.toUpperCase() === 'REGIONAL' ? 'REGIONAL' : null;
  if (!branchCode) return null;
  return { branchCode, regionCode };
}

function createMinistryNode(): AtlasNode {
  return {
    id: MMR_MINISTRY_ID,
    name: 'Ministerstvo pro mistni rozvoj',
    category: 'ministry',
    level: 1,
    metadata: { focus: 'mmr' },
  };
}

function createBranchNode(
  id: string,
  name: string,
  level: number,
  capacity: number | null,
  drilldownAvailable: boolean,
): AtlasNode {
  return {
    id,
    name,
    category: 'other',
    level,
    metadata: {
      ...(capacity && capacity > 0 ? { capacity } : {}),
      drilldownAvailable,
      focus: 'mmr',
    },
  };
}

function createRegionNode(row: MmrRegionMetric): AtlasNode {
  return {
    id: regionNodeId(row.branchCode, row.regionCode),
    name: row.regionName,
    category: 'region',
    level: 3,
    metadata: {
      capacity: row.recipientCount,
      projectCount: row.projectCount,
      drilldownAvailable: true,
      focus: 'mmr',
    },
  };
}

function createRecipientNode(row: MmrRecipientAggregate): AtlasNode {
  return {
    id: recipientNodeId(row.branchCode, row.recipientKey),
    name: row.recipientName,
    category: 'other',
    level: 4,
    ico: row.recipientIco ?? undefined,
    metadata: {
      capacity: 1,
      projectCount: row.projectCount,
      regionName: row.regionName,
      focus: 'mmr',
    },
  };
}

function createPagerNode(id: typeof PREV_WINDOW_ID | typeof NEXT_WINDOW_ID, hiddenCount: number): AtlasNode {
  const arrow = id === PREV_WINDOW_ID ? '↑' : '↓';
  return {
    id,
    name: `${arrow} dalsi prijemci (${hiddenCount})`,
    category: 'other',
    level: 5,
    metadata: {
      capacity: hiddenCount,
      drilldownAvailable: true,
      focus: 'mmr',
    },
  };
}

function budgetAmount(rows: MmrBudgetAggregate[], metricCode: string): number {
  return rows.find((row) => row.metricCode === metricCode)?.amount ?? 0;
}

function recipientMetric(
  rows: MmrRecipientMetric[],
  branchCode: 'REGIONAL' | 'HOUSING',
): MmrRecipientMetric | null {
  return rows.find((row) => row.branchCode === branchCode) ?? null;
}

export async function getMmrBudgetAggregates(year: number): Promise<MmrBudgetAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        metric_code,
        metric_name,
        metric_group,
        amount_czk
      from mart.mmr_budget_aggregate_latest
      where reporting_year = $1
      order by metric_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    metricCode: String(row.metric_code),
    metricName: String(row.metric_name),
    metricGroup: String(row.metric_group),
    amount: toNumber(row.amount_czk),
    sourceDataset: 'mmr_budget_aggregates',
  }));
}

export async function getMmrRecipientMetrics(year: number): Promise<MmrRecipientMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        branch_code,
        branch_name,
        recipient_count,
        project_count,
        allocated_total_czk
      from mart.mmr_irop_recipient_metric_latest
      where reporting_year = $1
      order by branch_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    branchCode: String(row.branch_code) as 'REGIONAL' | 'HOUSING',
    branchName: String(row.branch_name),
    recipientCount: Number(row.recipient_count),
    projectCount: Number(row.project_count),
    allocatedTotal: toNumber(row.allocated_total_czk),
    sourceDataset: 'mmr_irop_operations',
  }));
}

async function getMmrRegionMetrics(year: number, branchCode: 'REGIONAL' | 'HOUSING'): Promise<MmrRegionMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        branch_code,
        branch_name,
        region_code,
        region_name,
        recipient_count,
        project_count,
        allocated_total_czk
      from mart.mmr_irop_region_metric_latest
      where reporting_year = $1
        and branch_code = $2
      order by allocated_total_czk desc, region_name asc
    `,
    [year, branchCode],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    branchCode: String(row.branch_code) as 'REGIONAL' | 'HOUSING',
    branchName: String(row.branch_name),
    regionCode: String(row.region_code),
    regionName: String(row.region_name),
    recipientCount: Number(row.recipient_count),
    projectCount: Number(row.project_count),
    allocatedTotal: toNumber(row.allocated_total_czk),
    sourceDataset: 'mmr_irop_operations',
  }));
}

async function getMmrRecipientsByRegion(
  year: number,
  branchCode: 'REGIONAL' | 'HOUSING',
  regionCode: string,
): Promise<MmrRecipientAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        branch_code,
        branch_name,
        region_code,
        region_name,
        recipient_key,
        recipient_name,
        recipient_ico,
        project_count,
        allocated_total_czk
      from mart.mmr_irop_recipient_yearly_latest
      where reporting_year = $1
        and branch_code = $2
        and region_code = $3
      order by allocated_total_czk desc, recipient_name asc
    `,
    [year, branchCode, regionCode],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    branchCode: String(row.branch_code) as 'REGIONAL' | 'HOUSING',
    branchName: String(row.branch_name),
    regionCode: String(row.region_code),
    regionName: String(row.region_name),
    recipientKey: String(row.recipient_key),
    recipientName: String(row.recipient_name),
    recipientIco: row.recipient_ico ? String(row.recipient_ico) : null,
    projectCount: Number(row.project_count),
    allocatedTotal: toNumber(row.allocated_total_czk),
    sourceDataset: 'mmr_irop_operations',
  }));
}

export function getMmrTotal(budgetRows: MmrBudgetAggregate[]): number {
  return budgetAmount(budgetRows, 'EXP_TOTAL');
}

export function appendMmrBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  budgetRows: MmrBudgetAggregate[],
  recipientMetrics: MmrRecipientMetric[],
): void {
  const totalBudget = getMmrTotal(budgetRows);
  if (totalBudget <= 0) return;

  const regionalAmount = budgetAmount(budgetRows, 'REGIONAL_SUPPORT');
  const housingAmount = budgetAmount(budgetRows, 'HOUSING_SUPPORT');
  const planningAmount = budgetAmount(budgetRows, 'PLANNING');
  const otherAmount = budgetAmount(budgetRows, 'OTHER');
  const regionalMetric = recipientMetric(recipientMetrics, 'REGIONAL');
  const housingMetric = recipientMetric(recipientMetrics, 'HOUSING');

  addNode(nodes, createMinistryNode());
  links.push(
    makeLink(
      STATE_ID,
      MMR_MINISTRY_ID,
      totalBudget,
      year,
      'state_to_mmr_resort',
      'První iterace MMR používá otevřené rozpočtové ukazatele MMR. Recipient-level drilldown je navázán jen na IROP operace zveřejněné přes DotaceEU.',
      'mmr_budget_aggregates',
    ),
  );

  if (regionalAmount > 0) {
    addNode(nodes, createBranchNode(MMR_BRANCH_REGION_ID, 'Regionalni rozvoj a cestovni ruch', 2, regionalMetric?.recipientCount ?? null, true));
    links.push(
      makeLink(
        MMR_MINISTRY_ID,
        MMR_BRANCH_REGION_ID,
        regionalAmount,
        year,
        'mmr_budget_group',
        'Recipient-level drilldown používá regionální IROP operace a alokuje tuto větev podle podílů přidělených způsobilých výdajů.',
        'mmr_budget_aggregates',
      ),
    );
  }

  if (housingAmount > 0) {
    addNode(nodes, createBranchNode(MMR_BRANCH_HOUSING_ID, 'Podpora bydleni', 2, housingMetric?.recipientCount ?? null, true));
    links.push(
      makeLink(
        MMR_MINISTRY_ID,
        MMR_BRANCH_HOUSING_ID,
        housingAmount,
        year,
        'mmr_budget_group',
        'Recipient-level drilldown používá bytově zaměřené IROP operace a alokuje tuto větev podle podílů přidělených způsobilých výdajů.',
        'mmr_budget_aggregates',
      ),
    );
  }

  if (planningAmount > 0) {
    addNode(nodes, createBranchNode(MMR_BRANCH_PLANNING_ID, 'Uzemni planovani a stavebni rad', 2, null, false));
    links.push(
      makeLink(
        MMR_MINISTRY_ID,
        MMR_BRANCH_PLANNING_ID,
        planningAmount,
        year,
        'mmr_budget_group',
        'Rozpočtová větev bez recipient-level denominatoru v první iteraci.',
        'mmr_budget_aggregates',
      ),
    );
  }

  if (otherAmount > 0) {
    addNode(nodes, createBranchNode(MMR_BRANCH_OTHER_ID, 'Ostatni cinnosti resortu', 2, null, false));
    links.push(
      makeLink(
        MMR_MINISTRY_ID,
        MMR_BRANCH_OTHER_ID,
        otherAmount,
        year,
        'mmr_budget_group',
        'Rozpočtová reziduální větev bez project/recipient drilldownu v první iteraci.',
        'mmr_budget_aggregates',
      ),
    );
  }
}

function buildMmrRootGraph(
  year: number,
  budgetRows: MmrBudgetAggregate[],
  recipientMetrics: MmrRecipientMetric[],
) {
  const nodes: AtlasNode[] = [createMinistryNode()];
  const links: AtlasLink[] = [];
  appendMmrBranch(nodes, links, year, budgetRows, recipientMetrics);
  return links.length ? { year, nodes, links } : null;
}

function buildMmrRegionGraph(
  year: number,
  branchRow: MmrRecipientMetric,
  branchBudgetAmount: number,
  regionRows: MmrRegionMetric[],
) {
  const totalAllocated = regionRows.reduce((sum, row) => sum + row.allocatedTotal, 0);
  if (branchBudgetAmount <= 0 || totalAllocated <= 0) return null;

  const sourceNodeId = branchNodeId(branchRow.branchCode);
  const nodes: AtlasNode[] = [
    createBranchNode(sourceNodeId, branchRow.branchCode === 'HOUSING' ? 'Podpora bydleni' : 'Regionalni rozvoj a cestovni ruch', 2, branchRow.recipientCount, true),
  ];
  const links: AtlasLink[] = [];

  for (const row of regionRows) {
    if (row.allocatedTotal <= 0) continue;
    addNode(nodes, createRegionNode(row));
    links.push(
      makeLink(
        sourceNodeId,
        regionNodeId(row.branchCode, row.regionCode),
        branchBudgetAmount * (row.allocatedTotal / totalAllocated),
        year,
        'mmr_irop_region_allocated',
        'Krajská vrstva je odvozená z podílu IROP operací podle přidělených způsobilých výdajů v otevřeném seznamu operací.',
        row.sourceDataset,
      ),
    );
  }

  return links.length ? { year, nodes, links } : null;
}

function buildMmrRecipientGraph(
  year: number,
  branchCode: 'REGIONAL' | 'HOUSING',
  regionRow: MmrRegionMetric,
  regionBudgetAmount: number,
  recipientRows: MmrRecipientAggregate[],
  offset: number,
) {
  const totalAllocated = recipientRows.reduce((sum, row) => sum + row.allocatedTotal, 0);
  if (regionBudgetAmount <= 0 || totalAllocated <= 0) return null;

  const sourceNodeId = regionNodeId(branchCode, regionRow.regionCode);
  const nodes: AtlasNode[] = [createRegionNode(regionRow)];
  const links: AtlasLink[] = [];

  const pageRows = recipientRows.slice(offset, offset + PAGE_SIZE);
  const prevRows = recipientRows.slice(0, offset);
  const nextRows = recipientRows.slice(offset + PAGE_SIZE);

  if (prevRows.length > 0) {
    addNode(nodes, createPagerNode(PREV_WINDOW_ID, prevRows.length));
    links.push(
      makeLink(
        sourceNodeId,
        PREV_WINDOW_ID,
        regionBudgetAmount * (prevRows.reduce((sum, row) => sum + row.allocatedTotal, 0) / totalAllocated),
        year,
        'mmr_irop_recipient_page',
        'Předchozí okno příjemců podpory podle přidělených způsobilých výdajů IROP.',
        'atlas.inferred',
      ),
    );
  }

  for (const row of pageRows) {
    addNode(nodes, createRecipientNode(row));
    links.push(
      makeLink(
        sourceNodeId,
        recipientNodeId(branchCode, row.recipientKey),
        regionBudgetAmount * (row.allocatedTotal / totalAllocated),
        year,
        'mmr_irop_recipient_allocated',
        'Recipient-level alokace používá podíl přidělených způsobilých výdajů IROP u daného příjemce v kraji.',
        row.sourceDataset,
      ),
    );
  }

  if (nextRows.length > 0) {
    addNode(nodes, createPagerNode(NEXT_WINDOW_ID, nextRows.length));
    links.push(
      makeLink(
        sourceNodeId,
        NEXT_WINDOW_ID,
        regionBudgetAmount * (nextRows.reduce((sum, row) => sum + row.allocatedTotal, 0) / totalAllocated),
        year,
        'mmr_irop_recipient_page',
        'Následující okno příjemců podpory podle přidělených způsobilých výdajů IROP.',
        'atlas.inferred',
      ),
    );
  }

  return { year, nodes, links };
}

export async function getAtlasMmrGraph(
  year: number,
  nodeId: string | null = null,
  offset = 0,
) {
  const [budgetRows, recipientMetrics] = await Promise.all([
    getMmrBudgetAggregates(year),
    getMmrRecipientMetrics(year),
  ]);

  if (!nodeId || nodeId === MMR_MINISTRY_ID) {
    return buildMmrRootGraph(year, budgetRows, recipientMetrics);
  }

  if (nodeId === MMR_BRANCH_REGION_ID || nodeId === MMR_BRANCH_HOUSING_ID) {
    const branchCode = nodeId === MMR_BRANCH_HOUSING_ID ? 'HOUSING' : 'REGIONAL';
    const branchRow = recipientMetric(recipientMetrics, branchCode);
    const branchBudgetAmount = budgetAmount(budgetRows, branchBudgetCode(branchCode));
    if (!branchRow || branchBudgetAmount <= 0) return null;
    const regionRows = await getMmrRegionMetrics(year, branchCode);
    return buildMmrRegionGraph(year, branchRow, branchBudgetAmount, regionRows);
  }

  const parsedRegion = parseRegionNodeId(nodeId);
  if (parsedRegion) {
    const branchRows = await getMmrRegionMetrics(year, parsedRegion.branchCode);
    const regionRow = branchRows.find((row) => row.regionCode === parsedRegion.regionCode) ?? null;
    const branchRow = recipientMetric(recipientMetrics, parsedRegion.branchCode);
    const branchBudgetAmount = budgetAmount(budgetRows, branchBudgetCode(parsedRegion.branchCode));
    if (!regionRow || !branchRow || branchBudgetAmount <= 0) return null;
    const totalAllocated = branchRows.reduce((sum, row) => sum + row.allocatedTotal, 0);
    if (totalAllocated <= 0) return null;
    const regionBudgetAmount = branchBudgetAmount * (regionRow.allocatedTotal / totalAllocated);
    const recipientRows = await getMmrRecipientsByRegion(year, parsedRegion.branchCode, parsedRegion.regionCode);
    return buildMmrRecipientGraph(year, parsedRegion.branchCode, regionRow, regionBudgetAmount, recipientRows, offset);
  }

  return null;
}
