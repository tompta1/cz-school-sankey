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

interface TransportActivityMetric {
  requestedYear: number;
  sourceYear: number;
  activityDomain: string;
  metricCode: string;
  metricName: string;
  countValue: number;
  referenceAmountCzk: number;
  sourceUrl: string | null;
  sourceDataset: string;
}

interface TransportBranchConfig {
  id: string;
  name: string;
  drilldownAvailable: boolean;
}

interface TransportInvestorAggregate {
  investorKey: string;
  investorName: string;
  investorIco: string;
  paidCzk: number;
}

interface TransportBranchSummary extends TransportBranchConfig {
  amount: number;
  capacity: number | null;
  flowType: string;
  note: string;
  sourceDataset: string;
  projectRows: TransportSfdiProject[];
}

type TransportProjectClass =
  | 'rail'
  | 'waterways'
  | 'urban_rail'
  | 'road_motorway'
  | 'road_other'
  | 'other';

const STATE_ID = 'state:cr';
const TRANSPORT_ROOT_ID = 'transport:ministry:md';
const TRANSPORT_ADMIN_ID = 'transport:md-admin';
const TRANSPORT_ROADS_VIGNETTE_ID = 'transport:sfdi:roads-vignette';
const TRANSPORT_ROADS_TOLL_ID = 'transport:sfdi:roads-toll';
const TRANSPORT_ROADS_OTHER_ID = 'transport:sfdi:roads-other';
const TRANSPORT_RAIL_ID = 'transport:sfdi:rail';
const TRANSPORT_WATERWAYS_ID = 'transport:sfdi:waterways';
const TRANSPORT_URBAN_RAIL_ID = 'transport:sfdi:urban-rail';
const TRANSPORT_OTHER_ID = 'transport:sfdi:other';
const TRANSPORT_RESIDUAL_ID = 'transport:sfdi:residual';
const TRANSPORT_INVESTOR_PREFIX = 'transport:investor:';
const TRANSPORT_PROJECT_PREFIX = 'transport:project:';

const TRANSPORT_BRANCHES: TransportBranchConfig[] = [
  { id: TRANSPORT_ROADS_VIGNETTE_ID, name: 'Dalnice pro osobni auta', drilldownAvailable: false },
  { id: TRANSPORT_ROADS_TOLL_ID, name: 'Mytna sit tezkych vozidel', drilldownAvailable: false },
  { id: TRANSPORT_ROADS_OTHER_ID, name: 'Ostatni silnicni infrastruktura', drilldownAvailable: true },
  { id: TRANSPORT_RAIL_ID, name: 'Zeleznice', drilldownAvailable: true },
  { id: TRANSPORT_WATERWAYS_ID, name: 'Vodni cesty', drilldownAvailable: true },
  { id: TRANSPORT_URBAN_RAIL_ID, name: 'Mestska kolejova doprava', drilldownAvailable: true },
  { id: TRANSPORT_OTHER_ID, name: 'Ostatni dopravni infrastruktura', drilldownAvailable: true },
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

function createTransportBranchNode(summary: TransportBranchSummary): AtlasNode {
  return {
    id: summary.id,
    name: summary.name,
    category: 'other',
    level: 2,
    metadata: {
      ...(summary.capacity && summary.capacity > 0 ? { capacity: summary.capacity } : {}),
      drilldownAvailable: summary.drilldownAvailable,
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

function isRailProject(investor: string, project: string): boolean {
  return (
    investor.includes('sprava-zeleznic') ||
    investor.includes('ceske-drahy') ||
    project.includes('etcs') ||
    project.includes('zelezn') ||
    project.includes('koridor')
  );
}

function isWaterwayProject(investor: string, project: string): boolean {
  return (
    investor.includes('reditelstvi-vodnich-cest') ||
    project.includes('vodni-cest') ||
    project.includes('splavnost') ||
    project.includes('pristav')
  );
}

function isUrbanRailProject(investor: string, project: string): boolean {
  return (
    investor.includes('dopravni-podnik') ||
    (investor.includes('hlavni-mesto-praha') && (project.includes('tramvaj') || project.includes('metro'))) ||
    project.includes('tramvaj') ||
    project.includes('metro')
  );
}

function isRoadProject(investor: string, project: string, region: string): boolean {
  return (
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
  );
}

function isMotorwayProject(project: string): boolean {
  return /^d[0-9]/.test(project) || project.includes('dalnic');
}

function classifyTransportProject(row: TransportSfdiProject): TransportProjectClass {
  const investor = normalizeIdentifier(row.investorName);
  const project = normalizeIdentifier(row.projectName);
  const region = normalizeIdentifier(row.regionCode);

  if (isRailProject(investor, project)) return 'rail';
  if (isWaterwayProject(investor, project)) return 'waterways';
  if (isUrbanRailProject(investor, project)) return 'urban_rail';
  if (isRoadProject(investor, project, region)) {
    return isMotorwayProject(project) ? 'road_motorway' : 'road_other';
  }
  return 'other';
}

function groupProjectsByClass(rows: TransportSfdiProject[]): Map<TransportProjectClass, TransportSfdiProject[]> {
  const grouped = new Map<TransportProjectClass, TransportSfdiProject[]>();
  for (const row of rows) {
    if (row.paidCzk <= 0) continue;
    const projectClass = classifyTransportProject(row);
    grouped.set(projectClass, [...(grouped.get(projectClass) ?? []), row]);
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
    };
    existing.paidCzk += row.paidCzk;
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

function sumProjectAmount(rows: TransportSfdiProject[]): number {
  return rows.reduce((sum, row) => sum + row.paidCzk, 0);
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

export async function getTransportActivityMetrics(year: number): Promise<TransportActivityMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        activity_domain,
        metric_code,
        metric_name,
        count_value,
        reference_amount_czk,
        source_url
      from mart.transport_activity_metric_latest
      where reporting_year <= $1
      order by activity_domain, metric_code, reporting_year desc
    `,
    [year],
  );

  return result.rows.map((row) => ({
    requestedYear: year,
    sourceYear: Number(row.reporting_year),
    activityDomain: String(row.activity_domain),
    metricCode: String(row.metric_code),
    metricName: String(row.metric_name),
    countValue: toNumber(row.count_value),
    referenceAmountCzk: toNumber(row.reference_amount_czk),
    sourceUrl: row.source_url ? String(row.source_url) : null,
    sourceDataset: 'transport_activity_metrics',
  }));
}

function latestMetric(
  metrics: TransportActivityMetric[],
  activityDomain: string,
  metricCode: string,
): TransportActivityMetric | null {
  return (
    metrics.find((row) => row.activityDomain === activityDomain && row.metricCode === metricCode) ?? null
  );
}

function appendMetricCoverage(note: string, metric: TransportActivityMetric | null, year: number): string {
  if (!metric) return note;
  if (metric.sourceYear === year) return note;
  return `${note}. Srovnávací jednotka používá poslední dostupný oficiální roční údaj z ${metric.sourceYear}.`;
}

function buildTransportBranchSummaries(
  year: number,
  projectRows: TransportSfdiProject[],
  activityMetrics: TransportActivityMetric[],
): TransportBranchSummary[] {
  const grouped = groupProjectsByClass(projectRows);
  const railRows = grouped.get('rail') ?? [];
  const waterwaysRows = grouped.get('waterways') ?? [];
  const urbanRailRows = grouped.get('urban_rail') ?? [];
  const motorwayRows = grouped.get('road_motorway') ?? [];
  const roadOtherRows = grouped.get('road_other') ?? [];
  const otherRows = grouped.get('other') ?? [];

  const railMetric = latestMetric(activityMetrics, 'rail', 'rail_passengers_total');
  const vignetteMetric = latestMetric(activityMetrics, 'roads_vignette', 'vignettes_sold_total');
  const tollMetric = latestMetric(activityMetrics, 'roads_toll', 'toll_registered_vehicles_total');

  const motorwayTotal = sumProjectAmount(motorwayRows);
  const roadOtherTotal = sumProjectAmount(roadOtherRows);
  const railTotal = sumProjectAmount(railRows);
  const waterwaysTotal = sumProjectAmount(waterwaysRows);
  const urbanRailTotal = sumProjectAmount(urbanRailRows);
  const otherTotal = sumProjectAmount(otherRows);

  const roadRevenueTotal = (vignetteMetric?.referenceAmountCzk ?? 0) + (tollMetric?.referenceAmountCzk ?? 0);
  const vignetteShare = roadRevenueTotal > 0 ? (vignetteMetric?.referenceAmountCzk ?? 0) / roadRevenueTotal : 0.5;
  const tollShare = roadRevenueTotal > 0 ? (tollMetric?.referenceAmountCzk ?? 0) / roadRevenueTotal : 0.5;
  const roadVignetteAmount = motorwayTotal * vignetteShare;
  const roadTollAmount = motorwayTotal * tollShare;

  return [
    {
      id: TRANSPORT_ROADS_VIGNETTE_ID,
      name: 'Dalnice pro osobni auta',
      drilldownAvailable: false,
      amount: roadVignetteAmount,
      capacity: vignetteMetric?.countValue ?? null,
      flowType: 'transport_road_vignette_branch',
      note: appendMetricCoverage(
        'Motorway-like silniční projekty rozdělené mezi osobní auta a mýtná vozidla podle podílu oficiálních výnosů z e-známek a mýta',
        vignetteMetric,
        year,
      ),
      sourceDataset: 'atlas.inferred',
      projectRows: [],
    },
    {
      id: TRANSPORT_ROADS_TOLL_ID,
      name: 'Mytna sit tezkych vozidel',
      drilldownAvailable: false,
      amount: roadTollAmount,
      capacity: tollMetric?.countValue ?? null,
      flowType: 'transport_road_toll_branch',
      note: appendMetricCoverage(
        'Motorway-like silniční projekty rozdělené mezi osobní auta a mýtná vozidla podle podílu oficiálních výnosů z e-známek a mýta',
        tollMetric,
        year,
      ),
      sourceDataset: 'atlas.inferred',
      projectRows: [],
    },
    {
      id: TRANSPORT_ROADS_OTHER_ID,
      name: 'Ostatni silnicni infrastruktura',
      drilldownAvailable: true,
      amount: roadOtherTotal,
      capacity: null,
      flowType: 'transport_road_other_branch',
      note: 'Silniční projekty mimo dálniční tahy, ponechané bez srovnávací jednotky',
      sourceDataset: 'transport_sfdi_projects',
      projectRows: roadOtherRows,
    },
    {
      id: TRANSPORT_RAIL_ID,
      name: 'Zeleznice',
      drilldownAvailable: true,
      amount: railTotal,
      capacity: railMetric?.countValue ?? null,
      flowType: 'transport_rail_branch',
      note: appendMetricCoverage('SFDI železniční projekty se srovnávací jednotkou podle počtu cestujících v železniční osobní dopravě', railMetric, year),
      sourceDataset: railMetric?.sourceDataset ?? 'transport_sfdi_projects',
      projectRows: railRows,
    },
    {
      id: TRANSPORT_WATERWAYS_ID,
      name: 'Vodni cesty',
      drilldownAvailable: true,
      amount: waterwaysTotal,
      capacity: null,
      flowType: 'transport_sfdi_branch',
      note: transportNote('branch'),
      sourceDataset: 'transport_sfdi_projects',
      projectRows: waterwaysRows,
    },
    {
      id: TRANSPORT_URBAN_RAIL_ID,
      name: 'Mestska kolejova doprava',
      drilldownAvailable: true,
      amount: urbanRailTotal,
      capacity: null,
      flowType: 'transport_sfdi_branch',
      note: transportNote('branch'),
      sourceDataset: 'transport_sfdi_projects',
      projectRows: urbanRailRows,
    },
    {
      id: TRANSPORT_OTHER_ID,
      name: 'Ostatni dopravni infrastruktura',
      drilldownAvailable: true,
      amount: otherTotal,
      capacity: null,
      flowType: 'transport_sfdi_branch',
      note: transportNote('branch'),
      sourceDataset: 'transport_sfdi_projects',
      projectRows: otherRows,
    },
  ].filter((summary) => summary.amount > 0);
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
  activityMetrics: TransportActivityMetric[],
): void {
  const mdTotal = mdEntityTotal(budgetRows);
  const sfdiTotal = sfdiEntityTotal(budgetRows);
  const branchSummaries = buildTransportBranchSummaries(year, projectRows, activityMetrics);

  const sfdiProjectTotal = projectRows.reduce((sum, row) => sum + row.paidCzk, 0);
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
    const adminSummary: TransportBranchSummary = {
      id: TRANSPORT_ADMIN_ID,
      name: 'MD sprava a ostatni',
      drilldownAvailable: false,
      amount: mdTotal,
      capacity: null,
      flowType: 'transport_ministry_admin',
      note: 'Výdaje Ministerstva dopravy mimo projektové čerpání SFDI',
      sourceDataset: 'transport_budget_entities',
      projectRows: [],
    };
    addNode(nodes, createTransportBranchNode(adminSummary));
    links.push(
      makeLink(
        TRANSPORT_ROOT_ID,
        TRANSPORT_ADMIN_ID,
        mdTotal,
        year,
        adminSummary.flowType,
        adminSummary.note,
        adminSummary.sourceDataset,
      ),
    );
  }

  for (const summary of branchSummaries) {
    addNode(nodes, createTransportBranchNode(summary));
    links.push(
      makeLink(
        TRANSPORT_ROOT_ID,
        summary.id,
        summary.amount,
        year,
        summary.flowType,
        summary.note,
        summary.sourceDataset,
      ),
    );
  }

  if (sfdiResidual > 0) {
    const residualSummary: TransportBranchSummary = {
      id: TRANSPORT_RESIDUAL_ID,
      name: 'SFDI saldo a ostatni operace',
      drilldownAvailable: false,
      amount: sfdiResidual,
      capacity: null,
      flowType: 'transport_sfdi_residual',
      note: 'Rozdíl mezi výdaji SFDI z Monitoru a sumou projektového čerpání v otevřeném CSV',
      sourceDataset: 'transport_budget_entities',
      projectRows: [],
    };
    addNode(nodes, createTransportBranchNode(residualSummary));
    links.push(
      makeLink(
        TRANSPORT_ROOT_ID,
        TRANSPORT_RESIDUAL_ID,
        sfdiResidual,
        year,
        residualSummary.flowType,
        residualSummary.note,
        residualSummary.sourceDataset,
      ),
    );
  }
}

function buildTransportRootGraph(
  year: number,
  budgetRows: TransportBudgetEntity[],
  projectRows: TransportSfdiProject[],
  activityMetrics: TransportActivityMetric[],
) {
  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  appendTransportBranch(nodes, links, year, budgetRows, projectRows, activityMetrics);
  return nodes.length ? { year, nodes, links } : null;
}

function buildTransportInvestorGraph(
  year: number,
  budgetRows: TransportBudgetEntity[],
  projectRows: TransportSfdiProject[],
  activityMetrics: TransportActivityMetric[],
  branchId: string,
) {
  const branch = branchById(branchId);
  if (!branch || !branch.drilldownAvailable) return null;

  const branchSummary = buildTransportBranchSummaries(year, projectRows, activityMetrics).find((entry) => entry.id === branchId);
  const branchProjects = (branchSummary?.projectRows ?? []).sort(
    (a, b) => b.paidCzk - a.paidCzk || a.projectName.localeCompare(b.projectName, 'cs'),
  );
  if (!branchSummary || !branchProjects.length) return null;

  const mdTotal = mdEntityTotal(budgetRows);
  const sfdiTotal = sfdiEntityTotal(budgetRows);
  const sfdiProjectTotal = projectRows.reduce((sum, row) => sum + row.paidCzk, 0);
  const rootTotal = mdTotal + Math.max(sfdiTotal, sfdiProjectTotal);

  const nodes: AtlasNode[] = [
    createStateNode(),
    createTransportRootNode(),
    createTransportBranchNode(branchSummary),
  ];
  const links: AtlasLink[] = [
    makeLink(STATE_ID, TRANSPORT_ROOT_ID, rootTotal, year, 'state_to_transport_resort', transportNote('root'), 'transport_budget_entities'),
    makeLink(TRANSPORT_ROOT_ID, branchSummary.id, branchSummary.amount, year, branchSummary.flowType, branchSummary.note, branchSummary.sourceDataset),
  ];

  for (const investor of aggregateInvestors(branchProjects)) {
    addNode(nodes, createTransportInvestorNode(investor));
    links.push(
      makeLink(
        branchSummary.id,
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
  activityMetrics: TransportActivityMetric[],
  branchId: string,
  investorKey: string,
) {
  const branch = branchById(branchId);
  if (!branch || !branch.drilldownAvailable) return null;
  const branchSummary = buildTransportBranchSummaries(year, projectRows, activityMetrics).find((entry) => entry.id === branchId);
  if (!branchSummary) return null;
  const investorProjects = branchSummary.projectRows
    .filter((row) => investorKeyFor(row) === investorKey)
    .sort((a, b) => b.paidCzk - a.paidCzk || a.projectName.localeCompare(b.projectName, 'cs'));
  if (!investorProjects.length) return null;

  const investor = aggregateInvestors(investorProjects)[0];
  if (!investor) return null;

  const mdTotal = mdEntityTotal(budgetRows);
  const sfdiTotal = sfdiEntityTotal(budgetRows);
  const sfdiProjectTotal = projectRows.reduce((sum, row) => sum + row.paidCzk, 0);
  const rootTotal = mdTotal + Math.max(sfdiTotal, sfdiProjectTotal);

  const nodes: AtlasNode[] = [
    createStateNode(),
    createTransportRootNode(),
    createTransportBranchNode(branchSummary),
    createTransportInvestorNode(investor),
  ];
  const links: AtlasLink[] = [
    makeLink(STATE_ID, TRANSPORT_ROOT_ID, rootTotal, year, 'state_to_transport_resort', transportNote('root'), 'transport_budget_entities'),
    makeLink(TRANSPORT_ROOT_ID, branchSummary.id, branchSummary.amount, year, branchSummary.flowType, branchSummary.note, branchSummary.sourceDataset),
    makeLink(branchSummary.id, investorNodeId(investor.investorKey), investor.paidCzk, year, 'transport_sfdi_investor', transportNote('investor'), 'transport_sfdi_projects'),
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
  const [budgetRows, projectRows, activityMetrics] = await Promise.all([
    getTransportBudgetEntities(year),
    getTransportSfdiProjects(year),
    getTransportActivityMetrics(year),
  ]);

  if (!budgetRows.length && !projectRows.length) return null;
  if (!nodeId || nodeId === TRANSPORT_ROOT_ID) {
    return buildTransportRootGraph(year, budgetRows, projectRows, activityMetrics);
  }

  const branch = branchById(nodeId);
  if (branch) {
    return buildTransportInvestorGraph(year, budgetRows, projectRows, activityMetrics, branch.id);
  }

  const investorKey = parseInvestorNodeId(nodeId);
  if (investorKey) {
    const branchWithInvestor = buildTransportBranchSummaries(year, projectRows, activityMetrics).find((summary) =>
      summary.drilldownAvailable && summary.projectRows.some((row) => investorKeyFor(row) === investorKey),
    );
    if (!branchWithInvestor) return null;
    return buildTransportProjectGraph(year, budgetRows, projectRows, activityMetrics, branchWithInvestor.id, investorKey);
  }

  return null;
}
