import { query } from '../db.js';

interface AtlasNode {
  id: string;
  name: string;
  category: string;
  level: number;
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

interface TransportBudgetEntity {
  year: number;
  entityIco: string;
  entityName: string;
  entityKind: string;
  expenses: number;
  costs: number;
  revenues: number;
  result: number;
  sourceDataset: string;
}

interface TransportSfdiProject {
  year: number;
  actionId: string;
  budgetAreaCode: string;
  projectName: string;
  paidCzk: number;
  sfdiPaidCzk: number;
  euPaidCzk: number;
  regionCode: string;
  investorName: string;
  investorIco: string;
  sourceDataset: string;
}

interface TransportBranchConfig {
  id: string;
  name: string;
}

interface TransportInvestorAggregate {
  investorKey: string;
  investorName: string;
  investorIco: string;
  paidCzk: number;
  projectCount: number;
}

const STATE_ID = 'state:cr';
const TRANSPORT_ROOT_ID = 'transport:ministry:md';
const TRANSPORT_ADMIN_ID = 'transport:md-admin';
const TRANSPORT_ROADS_ID = 'transport:sfdi:roads';
const TRANSPORT_RAIL_ID = 'transport:sfdi:rail';
const TRANSPORT_WATERWAYS_ID = 'transport:sfdi:waterways';
const TRANSPORT_URBAN_RAIL_ID = 'transport:sfdi:urban-rail';
const TRANSPORT_OTHER_ID = 'transport:sfdi:other';
const TRANSPORT_RESIDUAL_ID = 'transport:sfdi:residual';
const TRANSPORT_INVESTOR_PREFIX = 'transport:investor:';
const TRANSPORT_PROJECT_PREFIX = 'transport:project:';

const TRANSPORT_BRANCHES: TransportBranchConfig[] = [
  { id: TRANSPORT_ROADS_ID, name: 'Silnice a dalnice' },
  { id: TRANSPORT_RAIL_ID, name: 'Zeleznice' },
  { id: TRANSPORT_WATERWAYS_ID, name: 'Vodni cesty' },
  { id: TRANSPORT_URBAN_RAIL_ID, name: 'Mestska kolejova doprava' },
  { id: TRANSPORT_OTHER_ID, name: 'Ostatni dopravni infrastruktura' },
];

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

function normalizeIdentifier(value: string): string {
  return value
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function createStateNode(): AtlasNode {
  return {
    id: STATE_ID,
    name: 'Statni rozpocet',
    category: 'state',
    level: 0,
  };
}

function createTransportRootNode(): AtlasNode {
  return {
    id: TRANSPORT_ROOT_ID,
    name: 'Ministerstvo dopravy a SFDI',
    category: 'ministry',
    level: 1,
    metadata: {
      focus: 'transport',
    },
  };
}

function createTransportBranchNode(
  id: string,
  name: string,
  capacity: number | null = null,
  drilldownAvailable = false,
): AtlasNode {
  return {
    id,
    name,
    category: 'other',
    level: 2,
    metadata: {
      ...(capacity && capacity > 0 ? { capacity } : {}),
      drilldownAvailable,
      focus: 'transport',
    },
  };
}

function createTransportInvestorNode(row: TransportInvestorAggregate): AtlasNode {
  return {
    id: investorNodeId(row.investorKey),
    name: row.investorName,
    category: 'other',
    level: 3,
    metadata: {
      capacity: row.projectCount,
      focus: 'transport',
    },
  };
}

function createTransportProjectNode(row: TransportSfdiProject): AtlasNode {
  return {
    id: projectNodeId(row),
    name: row.projectName,
    category: 'other',
    level: 4,
    metadata: {
      capacity: 1,
      focus: 'transport',
    },
  };
}

function transportNote(scope: 'root' | 'branch' | 'investor' | 'project'): string {
  if (scope === 'root') return 'Synteticka osa resortu dopravy: Ministerstvo dopravy + SFDI';
  if (scope === 'branch') return 'SFDI čerpání seskupené podle typu dopravní infrastruktury';
  if (scope === 'investor') return 'SFDI čerpání seskupené podle investora / příjemce';
  return 'SFDI čerpání konkrétní akce';
}

function branchById(branchId: string): TransportBranchConfig | null {
  return TRANSPORT_BRANCHES.find((branch) => branch.id === branchId) ?? null;
}

function investorKeyFor(row: TransportSfdiProject): string {
  return row.investorIco || `name:${normalizeIdentifier(row.investorName)}`;
}

function investorNodeId(investorKey: string): string {
  return `${TRANSPORT_INVESTOR_PREFIX}${investorKey}`;
}

function parseInvestorNodeId(nodeId: string): string | null {
  if (!nodeId.startsWith(TRANSPORT_INVESTOR_PREFIX)) return null;
  return nodeId.slice(TRANSPORT_INVESTOR_PREFIX.length);
}

function projectNodeId(row: TransportSfdiProject): string {
  return `${TRANSPORT_PROJECT_PREFIX}${row.actionId}|${row.budgetAreaCode}`;
}

function classifyTransportBranch(row: TransportSfdiProject): string {
  const investor = normalizeIdentifier(row.investorName);
  const project = normalizeIdentifier(row.projectName);
  const region = normalizeIdentifier(row.regionCode);

  if (
    investor.includes('sprava-zeleznic') ||
    investor.includes('ceske-drahy') ||
    project.includes('etcs') ||
    project.includes('zelezn') ||
    project.includes('koridor')
  ) {
    return TRANSPORT_RAIL_ID;
  }

  if (
    investor.includes('reditelstvi-vodnich-cest') ||
    project.includes('vodni-cest') ||
    project.includes('splavnost') ||
    project.includes('pristav')
  ) {
    return TRANSPORT_WATERWAYS_ID;
  }

  if (
    investor.includes('dopravni-podnik') ||
    (investor.includes('hlavni-mesto-praha') && (project.includes('tramvaj') || project.includes('metro'))) ||
    project.includes('tramvaj') ||
    project.includes('metro')
  ) {
    return TRANSPORT_URBAN_RAIL_ID;
  }

  if (
    investor.includes('rsd') ||
    investor.includes('sprava-a-udrzba-silnic') ||
    investor.includes('krajska-sprava-a-udrzba-silnic') ||
    region.startsWith('stc') ||
    /^d[0-9]/.test(project) ||
    /^i-[0-9]/.test(project) ||
    /^ii-[0-9]/.test(project) ||
    /^iii-[0-9]/.test(project) ||
    project.includes('obchvat') ||
    project.includes('silnic') ||
    project.includes('dalnic') ||
    project.includes('most')
  ) {
    return TRANSPORT_ROADS_ID;
  }

  return TRANSPORT_OTHER_ID;
}

function groupedBranchProjects(rows: TransportSfdiProject[]) {
  const grouped = new Map<string, TransportSfdiProject[]>();
  for (const row of rows) {
    if (row.paidCzk <= 0) continue;
    const branchId = classifyTransportBranch(row);
    grouped.set(branchId, [...(grouped.get(branchId) ?? []), row]);
  }
  return grouped;
}

function aggregateInvestors(rows: TransportSfdiProject[]): TransportInvestorAggregate[] {
  const grouped = new Map<string, TransportInvestorAggregate>();
  for (const row of rows) {
    const investorKey = investorKeyFor(row);
    const existing = grouped.get(investorKey) ?? {
      investorKey,
      investorName: row.investorName,
      investorIco: row.investorIco,
      paidCzk: 0,
      projectCount: 0,
    };
    existing.paidCzk += row.paidCzk;
    existing.projectCount += 1;
    grouped.set(investorKey, existing);
  }
  return [...grouped.values()].sort(
    (a, b) => b.paidCzk - a.paidCzk || a.investorName.localeCompare(b.investorName, 'cs'),
  );
}

function mdEntityTotal(rows: TransportBudgetEntity[]): number {
  return rows.find((row) => row.entityKind === 'ministry_admin')?.expenses ?? 0;
}

function sfdiEntityTotal(rows: TransportBudgetEntity[]): number {
  return rows.find((row) => row.entityKind === 'infrastructure_fund')?.expenses ?? 0;
}

export async function getTransportBudgetEntities(year: number): Promise<TransportBudgetEntity[]> {
  const result = await query(
    `
      select
        reporting_year,
        entity_ico,
        entity_name,
        entity_kind,
        expenses_czk,
        costs_czk,
        revenues_czk,
        result_czk
      from mart.transport_budget_entity_latest
      where reporting_year = $1
      order by entity_kind, entity_ico
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    entityIco: String(row.entity_ico),
    entityName: String(row.entity_name),
    entityKind: String(row.entity_kind),
    expenses: toNumber(row.expenses_czk),
    costs: toNumber(row.costs_czk),
    revenues: toNumber(row.revenues_czk),
    result: toNumber(row.result_czk),
    sourceDataset: 'transport_budget_entities',
  }));
}

export async function getTransportSfdiProjects(year: number): Promise<TransportSfdiProject[]> {
  const result = await query(
    `
      select
        reporting_year,
        action_id,
        budget_area_code,
        project_name,
        paid_czk,
        sfdi_paid_czk,
        eu_paid_czk,
        region_code,
        investor_name,
        investor_ico
      from mart.transport_sfdi_project_latest
      where reporting_year = $1
        and paid_czk > 0
      order by paid_czk desc, investor_name, project_name
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    actionId: String(row.action_id),
    budgetAreaCode: String(row.budget_area_code),
    projectName: String(row.project_name),
    paidCzk: toNumber(row.paid_czk),
    sfdiPaidCzk: toNumber(row.sfdi_paid_czk),
    euPaidCzk: toNumber(row.eu_paid_czk),
    regionCode: String(row.region_code ?? ''),
    investorName: String(row.investor_name),
    investorIco: String(row.investor_ico ?? ''),
    sourceDataset: 'transport_sfdi_projects',
  }));
}

export function getTransportTotal(
  budgetRows: TransportBudgetEntity[],
  projectRows: TransportSfdiProject[],
): number {
  const sfdiTotal = sfdiEntityTotal(budgetRows);
  const sfdiProjectTotal = projectRows.reduce((sum, row) => sum + row.paidCzk, 0);
  return mdEntityTotal(budgetRows) + Math.max(sfdiTotal, sfdiProjectTotal);
}

export function appendTransportBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  budgetRows: TransportBudgetEntity[],
  projectRows: TransportSfdiProject[],
): void {
  const mdTotal = mdEntityTotal(budgetRows);
  const sfdiTotal = sfdiEntityTotal(budgetRows);
  const branchRows = groupedBranchProjects(projectRows);
  const branchTotals = new Map<string, number>();
  const branchCounts = new Map<string, number>();

  for (const branch of TRANSPORT_BRANCHES) {
    const rows = branchRows.get(branch.id) ?? [];
    branchTotals.set(branch.id, rows.reduce((sum, row) => sum + row.paidCzk, 0));
    branchCounts.set(branch.id, rows.length);
  }

  const sfdiProjectTotal = [...branchTotals.values()].reduce((sum, value) => sum + value, 0);
  const sfdiResidual = Math.max(sfdiTotal - sfdiProjectTotal, 0);
  const rootTotal = mdTotal + Math.max(sfdiTotal, sfdiProjectTotal);
  if (rootTotal <= 0) return;

  addNode(nodes, createStateNode());
  addNode(nodes, createTransportRootNode());
  links.push(
    makeLink(
      STATE_ID,
      TRANSPORT_ROOT_ID,
      rootTotal,
      year,
      'state_to_transport_resort',
      transportNote('root'),
      'transport_budget_entities',
    ),
  );

  if (mdTotal > 0) {
    addNode(nodes, createTransportBranchNode(TRANSPORT_ADMIN_ID, 'MD sprava a ostatni'));
    links.push(
      makeLink(
        TRANSPORT_ROOT_ID,
        TRANSPORT_ADMIN_ID,
        mdTotal,
        year,
        'transport_ministry_admin',
        'Výdaje Ministerstva dopravy mimo projektové čerpání SFDI',
        'transport_budget_entities',
      ),
    );
  }

  for (const branch of TRANSPORT_BRANCHES) {
    const amount = branchTotals.get(branch.id) ?? 0;
    if (amount <= 0) continue;
    addNode(
      nodes,
      createTransportBranchNode(branch.id, branch.name, branchCounts.get(branch.id) ?? null, true),
    );
    links.push(
      makeLink(
        TRANSPORT_ROOT_ID,
        branch.id,
        amount,
        year,
        'transport_sfdi_branch',
        transportNote('branch'),
        'transport_sfdi_projects',
      ),
    );
  }

  if (sfdiResidual > 0) {
    addNode(nodes, createTransportBranchNode(TRANSPORT_RESIDUAL_ID, 'SFDI saldo a ostatni operace'));
    links.push(
      makeLink(
        TRANSPORT_ROOT_ID,
        TRANSPORT_RESIDUAL_ID,
        sfdiResidual,
        year,
        'transport_sfdi_residual',
        'Rozdíl mezi výdaji SFDI z Monitoru a sumou projektového čerpání v otevřeném CSV',
        'transport_budget_entities',
      ),
    );
  }
}

function buildTransportRootGraph(
  year: number,
  budgetRows: TransportBudgetEntity[],
  projectRows: TransportSfdiProject[],
) {
  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  appendTransportBranch(nodes, links, year, budgetRows, projectRows);
  return nodes.length ? { year, nodes, links } : null;
}

function buildTransportInvestorGraph(
  year: number,
  budgetRows: TransportBudgetEntity[],
  projectRows: TransportSfdiProject[],
  branchId: string,
) {
  const branch = branchById(branchId);
  if (!branch) return null;

  const branchProjects = (groupedBranchProjects(projectRows).get(branchId) ?? []).sort(
    (a, b) => b.paidCzk - a.paidCzk || a.projectName.localeCompare(b.projectName, 'cs'),
  );
  if (!branchProjects.length) return null;

  const mdTotal = mdEntityTotal(budgetRows);
  const sfdiTotal = sfdiEntityTotal(budgetRows);
  const sfdiProjectTotal = projectRows.reduce((sum, row) => sum + row.paidCzk, 0);
  const rootTotal = mdTotal + Math.max(sfdiTotal, sfdiProjectTotal);
  const branchTotal = branchProjects.reduce((sum, row) => sum + row.paidCzk, 0);

  const nodes: AtlasNode[] = [
    createStateNode(),
    createTransportRootNode(),
    createTransportBranchNode(branch.id, branch.name, branchProjects.length, true),
  ];
  const links: AtlasLink[] = [
    makeLink(STATE_ID, TRANSPORT_ROOT_ID, rootTotal, year, 'state_to_transport_resort', transportNote('root'), 'transport_budget_entities'),
    makeLink(TRANSPORT_ROOT_ID, branch.id, branchTotal, year, 'transport_sfdi_branch', transportNote('branch'), 'transport_sfdi_projects'),
  ];

  for (const investor of aggregateInvestors(branchProjects)) {
    addNode(nodes, createTransportInvestorNode(investor));
    links.push(
      makeLink(
        branch.id,
        investorNodeId(investor.investorKey),
        investor.paidCzk,
        year,
        'transport_sfdi_investor',
        transportNote('investor'),
        'transport_sfdi_projects',
      ),
    );
  }

  return { year, nodes, links };
}

function buildTransportProjectGraph(
  year: number,
  budgetRows: TransportBudgetEntity[],
  projectRows: TransportSfdiProject[],
  branchId: string,
  investorKey: string,
) {
  const branch = branchById(branchId);
  if (!branch) return null;
  const branchProjects = groupedBranchProjects(projectRows).get(branchId) ?? [];
  const investorProjects = branchProjects
    .filter((row) => investorKeyFor(row) === investorKey)
    .sort((a, b) => b.paidCzk - a.paidCzk || a.projectName.localeCompare(b.projectName, 'cs'));
  if (!investorProjects.length) return null;

  const investor = aggregateInvestors(investorProjects)[0];
  if (!investor) return null;

  const mdTotal = mdEntityTotal(budgetRows);
  const sfdiTotal = sfdiEntityTotal(budgetRows);
  const sfdiProjectTotal = projectRows.reduce((sum, row) => sum + row.paidCzk, 0);
  const rootTotal = mdTotal + Math.max(sfdiTotal, sfdiProjectTotal);
  const branchTotal = branchProjects.reduce((sum, row) => sum + row.paidCzk, 0);

  const nodes: AtlasNode[] = [
    createStateNode(),
    createTransportRootNode(),
    createTransportBranchNode(branch.id, branch.name, branchProjects.length, true),
    createTransportInvestorNode(investor),
  ];
  const links: AtlasLink[] = [
    makeLink(STATE_ID, TRANSPORT_ROOT_ID, rootTotal, year, 'state_to_transport_resort', transportNote('root'), 'transport_budget_entities'),
    makeLink(TRANSPORT_ROOT_ID, branch.id, branchTotal, year, 'transport_sfdi_branch', transportNote('branch'), 'transport_sfdi_projects'),
    makeLink(branch.id, investorNodeId(investor.investorKey), investor.paidCzk, year, 'transport_sfdi_investor', transportNote('investor'), 'transport_sfdi_projects'),
  ];

  for (const row of investorProjects) {
    addNode(nodes, createTransportProjectNode(row));
    links.push(
      makeLink(
        investorNodeId(investor.investorKey),
        projectNodeId(row),
        row.paidCzk,
        year,
        'transport_sfdi_project',
        transportNote('project'),
        row.sourceDataset,
      ),
    );
  }

  return { year, nodes, links };
}

export async function getAtlasTransportGraph(year: number, nodeId: string | null) {
  const [budgetRows, projectRows] = await Promise.all([
    getTransportBudgetEntities(year),
    getTransportSfdiProjects(year),
  ]);

  if (!budgetRows.length && !projectRows.length) return null;
  if (!nodeId || nodeId === TRANSPORT_ROOT_ID) {
    return buildTransportRootGraph(year, budgetRows, projectRows);
  }

  if (branchById(nodeId)) {
    return buildTransportInvestorGraph(year, budgetRows, projectRows, nodeId);
  }

  const investorKey = parseInvestorNodeId(nodeId);
  if (investorKey) {
    const branch = TRANSPORT_BRANCHES.find((candidate) =>
      groupedBranchProjects(projectRows)
        .get(candidate.id)
        ?.some((row) => investorKeyFor(row) === investorKey),
    );
    if (!branch) return null;
    return buildTransportProjectGraph(year, budgetRows, projectRows, branch.id, investorKey);
  }

  return null;
}
