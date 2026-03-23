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

interface MzvBudgetEntity {
  year: number;
  entityIco: string;
  entityName: string;
  entityKind: string;
  expenses: number;
  costs: number;
  sourceDataset: string;
}

interface MzvDiplomaticMetric {
  requestedYear: number;
  sourceYear: number;
  metricCode: string;
  metricName: string;
  countValue: number;
  sourceDataset: string;
}

interface MzvAidBranchMetric {
  year: number;
  branchCode: 'DEVELOPMENT' | 'HUMANITARIAN';
  branchName: string;
  projectCount: number;
  recipientCount: number;
  actualAmount: number;
  sourceDataset: string;
}

interface MzvAidCountryMetric {
  year: number;
  branchCode: 'DEVELOPMENT' | 'HUMANITARIAN';
  branchName: string;
  countryName: string;
  projectCount: number;
  recipientCount: number;
  actualAmount: number;
  sourceDataset: string;
}

interface MzvAidProject {
  year: number;
  branchCode: 'DEVELOPMENT' | 'HUMANITARIAN';
  branchName: string;
  countryName: string;
  projectKey: string;
  projectName: string;
  recipientKey: string;
  recipientName: string;
  recipientIco: string | null;
  sectorName: string | null;
  managerCode: string | null;
  managerName: string | null;
  sourceWorkbook: string;
  actualAmount: number;
  sourceDataset: string;
}

const STATE_ID = 'state:cr';
const MZV_MINISTRY_ID = 'mzv:ministry:mzv';
const MZV_FOREIGN_SERVICE_ID = 'mzv:foreign-service';
const MZV_DEVELOPMENT_ID = 'mzv:aid:development';
const MZV_HUMANITARIAN_ID = 'mzv:aid:humanitarian';
const MZV_POST_TYPE_PREFIX = 'mzv:post-type:';
const MZV_COUNTRY_PREFIX = 'mzv:country:';
const MZV_PROJECT_PREFIX = 'mzv:project:';

const POST_TYPE_NODES = {
  EMBASSY_POSTS: { id: `${MZV_POST_TYPE_PREFIX}embassy`, name: 'Velvyslanectvi CR' },
  PERMANENT_MISSIONS: { id: `${MZV_POST_TYPE_PREFIX}mission`, name: 'Stale mise CR' },
  GENERAL_CONSULATES: { id: `${MZV_POST_TYPE_PREFIX}consulate`, name: 'Generalni konzulaty CR' },
  CONSULAR_AGENCIES: { id: `${MZV_POST_TYPE_PREFIX}agency`, name: 'Konzularni jednatelstvi' },
  OTHER_OFFICES: { id: `${MZV_POST_TYPE_PREFIX}other`, name: 'Jine urady v zahranici' },
} as const;

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

function budgetEntityAmount(entity: MzvBudgetEntity): number {
  return entity.expenses > 0 ? entity.expenses : entity.costs;
}

function branchNodeId(branchCode: 'DEVELOPMENT' | 'HUMANITARIAN'): string {
  return branchCode === 'HUMANITARIAN' ? MZV_HUMANITARIAN_ID : MZV_DEVELOPMENT_ID;
}

function countryNodeId(branchCode: 'DEVELOPMENT' | 'HUMANITARIAN', countryName: string): string {
  return `${MZV_COUNTRY_PREFIX}${branchCode.toLowerCase()}|${countryName}`;
}

function projectNodeId(branchCode: 'DEVELOPMENT' | 'HUMANITARIAN', projectKey: string): string {
  return `${MZV_PROJECT_PREFIX}${branchCode.toLowerCase()}|${projectKey}`;
}

function parseCountryNodeId(nodeId: string): { branchCode: 'DEVELOPMENT' | 'HUMANITARIAN'; countryName: string } | null {
  if (!nodeId.startsWith(MZV_COUNTRY_PREFIX)) return null;
  const payload = nodeId.slice(MZV_COUNTRY_PREFIX.length);
  const [branchRaw, countryName] = payload.split('|', 2);
  if (!countryName) return null;
  const branchCode = branchRaw.toUpperCase() === 'HUMANITARIAN' ? 'HUMANITARIAN' : branchRaw.toUpperCase() === 'DEVELOPMENT' ? 'DEVELOPMENT' : null;
  if (!branchCode) return null;
  return { branchCode, countryName };
}

function createMinistryNode(): AtlasNode {
  return {
    id: MZV_MINISTRY_ID,
    name: 'Ministerstvo zahranicnich veci',
    category: 'ministry',
    level: 1,
    metadata: { focus: 'mzv' },
  };
}

function createBranchNode(id: string, name: string, level: number, capacity: number | null, drilldownAvailable: boolean): AtlasNode {
  return {
    id,
    name,
    category: 'other',
    level,
    metadata: {
      ...(capacity && capacity > 0 ? { capacity } : {}),
      drilldownAvailable,
      focus: 'mzv',
    },
  };
}

function createCountryNode(row: MzvAidCountryMetric): AtlasNode {
  return {
    id: countryNodeId(row.branchCode, row.countryName),
    name: row.countryName,
    category: 'other',
    level: 3,
    metadata: {
      capacity: row.projectCount,
      recipientCount: row.recipientCount,
      drilldownAvailable: true,
      focus: 'mzv',
    },
  };
}

function createProjectNode(row: MzvAidProject): AtlasNode {
  return {
    id: projectNodeId(row.branchCode, row.projectKey),
    name: row.projectName,
    category: 'other',
    level: 4,
    ico: row.recipientIco ?? undefined,
    metadata: {
      capacity: 1,
      recipientName: row.recipientName,
      sectorName: row.sectorName,
      managerName: row.managerName,
      sourceWorkbook: row.sourceWorkbook,
      focus: 'mzv',
    },
  };
}

function foreignServiceNote(sourceYear: number): string {
  return `Reziduální provozní vrstva MZV po odečtení explicitně dohledatelných rozvojových a humanitárních projektů. Srovnávací metrika používá ${sourceYear === 2024 ? 'oficiální počet 117 zastupitelských úřadů v zahraničí za rok 2024' : `poslední dostupný oficiální počet zastupitelských úřadů za rok ${sourceYear}`}.`;
}

function aidBranchNote(kind: 'development' | 'humanitarian'): string {
  return kind === 'humanitarian'
    ? 'Větev používá skutečné čerpání humanitárních projektů z výročního přehledu MZV za rok 2024, včetně akcí hrazených MZV přes jiné složky.'
    : 'Větev používá skutečné čerpání rozvojových projektů z výročních workbooků MZV a ČRA za rok 2024.';
}

export async function getMzvBudgetEntities(year: number): Promise<MzvBudgetEntity[]> {
  const result = await query(
    `
      select
        reporting_year,
        entity_ico,
        entity_name,
        entity_kind,
        expenses_czk,
        costs_czk
      from mart.mzv_budget_entity_latest
      where reporting_year = $1
      order by entity_ico
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
    sourceDataset: 'mzv_budget_entities',
  }));
}

export async function getMzvDiplomaticMetrics(year: number): Promise<MzvDiplomaticMetric[]> {
  const result = await query(
    `
      with latest_year as (
        select max(reporting_year) as reporting_year
        from mart.mzv_diplomatic_metric_latest
        where reporting_year <= $1
      )
      select
        m.reporting_year,
        m.metric_code,
        m.metric_name,
        m.count_value
      from mart.mzv_diplomatic_metric_latest m
      join latest_year y on y.reporting_year = m.reporting_year
      order by m.metric_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    requestedYear: year,
    sourceYear: Number(row.reporting_year),
    metricCode: String(row.metric_code),
    metricName: String(row.metric_name),
    countValue: Number(row.count_value),
    sourceDataset: 'mzv_diplomatic_metrics',
  }));
}

export async function getMzvAidBranchMetrics(year: number): Promise<MzvAidBranchMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        branch_code,
        branch_name,
        project_count,
        recipient_count,
        actual_czk
      from mart.mzv_aid_branch_metric_latest
      where reporting_year = $1
      order by branch_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    branchCode: String(row.branch_code) as 'DEVELOPMENT' | 'HUMANITARIAN',
    branchName: String(row.branch_name),
    projectCount: Number(row.project_count),
    recipientCount: Number(row.recipient_count),
    actualAmount: toNumber(row.actual_czk),
    sourceDataset: 'mzv_aid_operations',
  }));
}

async function getMzvAidCountryMetrics(year: number, branchCode: 'DEVELOPMENT' | 'HUMANITARIAN'): Promise<MzvAidCountryMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        branch_code,
        branch_name,
        country_name,
        project_count,
        recipient_count,
        actual_czk
      from mart.mzv_aid_country_metric_latest
      where reporting_year = $1
        and branch_code = $2
      order by actual_czk desc, country_name asc
    `,
    [year, branchCode],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    branchCode: String(row.branch_code) as 'DEVELOPMENT' | 'HUMANITARIAN',
    branchName: String(row.branch_name),
    countryName: String(row.country_name),
    projectCount: Number(row.project_count),
    recipientCount: Number(row.recipient_count),
    actualAmount: toNumber(row.actual_czk),
    sourceDataset: 'mzv_aid_operations',
  }));
}

async function getMzvAidProjects(year: number, branchCode: 'DEVELOPMENT' | 'HUMANITARIAN', countryName: string): Promise<MzvAidProject[]> {
  const result = await query(
    `
      select
        reporting_year,
        branch_code,
        branch_name,
        country_name,
        project_key,
        project_name,
        recipient_key,
        recipient_name,
        recipient_ico,
        sector_name,
        manager_code,
        manager_name,
        source_workbook,
        actual_czk
      from mart.mzv_aid_project_latest
      where reporting_year = $1
        and branch_code = $2
        and country_name = $3
      order by actual_czk desc, project_name asc
    `,
    [year, branchCode, countryName],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    branchCode: String(row.branch_code) as 'DEVELOPMENT' | 'HUMANITARIAN',
    branchName: String(row.branch_name),
    countryName: String(row.country_name),
    projectKey: String(row.project_key),
    projectName: String(row.project_name),
    recipientKey: String(row.recipient_key),
    recipientName: String(row.recipient_name),
    recipientIco: row.recipient_ico ? String(row.recipient_ico) : null,
    sectorName: row.sector_name ? String(row.sector_name) : null,
    managerCode: row.manager_code ? String(row.manager_code) : null,
    managerName: row.manager_name ? String(row.manager_name) : null,
    sourceWorkbook: String(row.source_workbook),
    actualAmount: toNumber(row.actual_czk),
    sourceDataset: 'mzv_aid_operations',
  }));
}

export function getMzvTotal(budgetRows: MzvBudgetEntity[]): number {
  return budgetRows.reduce((sum, row) => sum + budgetEntityAmount(row), 0);
}

function diplomaticMetricCount(rows: MzvDiplomaticMetric[], metricCode: keyof typeof POST_TYPE_NODES | 'FOREIGN_POST_TOTAL'): number {
  return rows.find((row) => row.metricCode === metricCode)?.countValue ?? 0;
}

function aidBranchMetric(
  rows: MzvAidBranchMetric[],
  branchCode: 'DEVELOPMENT' | 'HUMANITARIAN',
): MzvAidBranchMetric | null {
  return rows.find((row) => row.branchCode === branchCode) ?? null;
}

export function appendMzvBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  budgetRows: MzvBudgetEntity[],
  diplomaticMetrics: MzvDiplomaticMetric[],
  aidBranchMetrics: MzvAidBranchMetric[],
): void {
  const totalBudget = getMzvTotal(budgetRows);
  if (totalBudget <= 0) return;

  const foreignPostCount = diplomaticMetricCount(diplomaticMetrics, 'FOREIGN_POST_TOTAL');
  const developmentMetric = aidBranchMetric(aidBranchMetrics, 'DEVELOPMENT');
  const humanitarianMetric = aidBranchMetric(aidBranchMetrics, 'HUMANITARIAN');
  const rawDevelopment = developmentMetric?.actualAmount ?? 0;
  const rawHumanitarian = humanitarianMetric?.actualAmount ?? 0;
  const rawAidTotal = rawDevelopment + rawHumanitarian;
  const aidScale = rawAidTotal > totalBudget && rawAidTotal > 0 ? totalBudget / rawAidTotal : 1;
  const developmentAmount = rawDevelopment * aidScale;
  const humanitarianAmount = rawHumanitarian * aidScale;
  const foreignServiceAmount = Math.max(totalBudget - developmentAmount - humanitarianAmount, 0);
  const diplomaticSourceYear = diplomaticMetrics[0]?.sourceYear ?? year;

  addNode(nodes, createMinistryNode());
  links.push(
    makeLink(
      STATE_ID,
      MZV_MINISTRY_ID,
      totalBudget,
      year,
      'state_to_mzv_resort',
      'Horní vrstva MZV používá skutečný roční objem z Monitoru MF. Drilldown odděluje explicitně dohledatelné rozvojové a humanitární projekty od reziduální zahraniční služby.',
      'mzv_budget_entities',
    ),
  );

  if (developmentAmount > 0 && developmentMetric) {
    addNode(nodes, createBranchNode(MZV_DEVELOPMENT_ID, 'Rozvojova spoluprace', 2, developmentMetric.projectCount, true));
    links.push(
      makeLink(
        MZV_MINISTRY_ID,
        MZV_DEVELOPMENT_ID,
        developmentAmount,
        year,
        'mzv_aid_branch',
        aidBranchNote('development'),
        developmentMetric.sourceDataset,
      ),
    );
  }

  if (humanitarianAmount > 0 && humanitarianMetric) {
    addNode(nodes, createBranchNode(MZV_HUMANITARIAN_ID, 'Humanitarni pomoc', 2, humanitarianMetric.projectCount, true));
    links.push(
      makeLink(
        MZV_MINISTRY_ID,
        MZV_HUMANITARIAN_ID,
        humanitarianAmount,
        year,
        'mzv_aid_branch',
        aidBranchNote('humanitarian'),
        humanitarianMetric.sourceDataset,
      ),
    );
  }

  if (foreignServiceAmount > 0) {
    addNode(nodes, createBranchNode(MZV_FOREIGN_SERVICE_ID, 'Zastupitelske urady a zahranicni sluzba', 2, foreignPostCount || null, true));
    links.push(
      makeLink(
        MZV_MINISTRY_ID,
        MZV_FOREIGN_SERVICE_ID,
        foreignServiceAmount,
        year,
        'mzv_foreign_service_branch',
        foreignServiceNote(diplomaticSourceYear),
        diplomaticMetrics[0]?.sourceDataset ?? 'mzv_budget_entities',
      ),
    );
  }
}

function buildMzvRootGraph(
  year: number,
  budgetRows: MzvBudgetEntity[],
  diplomaticMetrics: MzvDiplomaticMetric[],
  aidBranchMetrics: MzvAidBranchMetric[],
) {
  const nodes: AtlasNode[] = [createMinistryNode()];
  const links: AtlasLink[] = [];
  appendMzvBranch(nodes, links, year, budgetRows, diplomaticMetrics, aidBranchMetrics);
  return links.length ? { year, nodes, links } : null;
}

function buildMzvForeignServiceGraph(
  year: number,
  budgetRows: MzvBudgetEntity[],
  diplomaticMetrics: MzvDiplomaticMetric[],
  aidBranchMetrics: MzvAidBranchMetric[],
) {
  const totalBudget = getMzvTotal(budgetRows);
  const rawAidTotal = aidBranchMetrics.reduce((sum, row) => sum + row.actualAmount, 0);
  const aidScale = rawAidTotal > totalBudget && rawAidTotal > 0 ? totalBudget / rawAidTotal : 1;
  const foreignServiceAmount = Math.max(totalBudget - rawAidTotal * aidScale, 0);
  const totalPosts = diplomaticMetricCount(diplomaticMetrics, 'FOREIGN_POST_TOTAL');
  if (foreignServiceAmount <= 0 || totalPosts <= 0) return null;

  const nodes: AtlasNode[] = [
    createBranchNode(MZV_FOREIGN_SERVICE_ID, 'Zastupitelske urady a zahranicni sluzba', 2, totalPosts, true),
  ];
  const links: AtlasLink[] = [];
  const sourceYear = diplomaticMetrics[0]?.sourceYear ?? year;

  for (const [metricCode, config] of Object.entries(POST_TYPE_NODES)) {
    const countValue = diplomaticMetricCount(diplomaticMetrics, metricCode as keyof typeof POST_TYPE_NODES);
    if (countValue <= 0) continue;
    const allocatedAmount = (foreignServiceAmount * countValue) / totalPosts;
    addNode(nodes, createBranchNode(config.id, config.name, 3, countValue, false));
    links.push(
      makeLink(
        MZV_FOREIGN_SERVICE_ID,
        config.id,
        allocatedAmount,
        year,
        'mzv_foreign_service_type_allocated',
        `Rozpad zahraniční služby podle počtu typů úřadů z publikace Česká diplomacie ${sourceYear}. Částky jsou alokované podle počtu úřadů, ne přímo zveřejněné po typech.`,
        diplomaticMetrics[0]?.sourceDataset ?? 'mzv_diplomatic_metrics',
      ),
    );
  }

  return { year, nodes, links };
}

function buildMzvAidCountryGraph(
  year: number,
  branchMetric: MzvAidBranchMetric,
  countryRows: MzvAidCountryMetric[],
) {
  if (!countryRows.length) return null;
  const rootNodeId = branchNodeId(branchMetric.branchCode);
  const nodes: AtlasNode[] = [
    createBranchNode(rootNodeId, branchMetric.branchCode === 'HUMANITARIAN' ? 'Humanitarni pomoc' : 'Rozvojova spoluprace', 2, branchMetric.projectCount, true),
  ];
  const links: AtlasLink[] = [];

  for (const row of countryRows) {
    addNode(nodes, createCountryNode(row));
    links.push(
      makeLink(
        rootNodeId,
        countryNodeId(row.branchCode, row.countryName),
        row.actualAmount,
        year,
        'mzv_aid_country',
        `Rozpad ${row.branchCode === 'HUMANITARIAN' ? 'humanitární' : 'rozvojové'} větve podle země realizace z oficiálního výročního přehledu MZV/ČRA za rok 2024.`,
        row.sourceDataset,
      ),
    );
  }

  return { year, nodes, links };
}

function buildMzvAidProjectGraph(
  year: number,
  branchCode: 'DEVELOPMENT' | 'HUMANITARIAN',
  countryName: string,
  projectRows: MzvAidProject[],
) {
  if (!projectRows.length) return null;
  const rootNodeId = countryNodeId(branchCode, countryName);
  const nodes: AtlasNode[] = [
    createBranchNode(rootNodeId, countryName, 3, projectRows.length, true),
  ];
  const links: AtlasLink[] = [];

  for (const row of projectRows) {
    addNode(nodes, createProjectNode(row));
    links.push(
      makeLink(
        rootNodeId,
        projectNodeId(branchCode, row.projectKey),
        row.actualAmount,
        year,
        'mzv_aid_project',
        `Projekt ${row.branchCode === 'HUMANITARIAN' ? 'humanitární pomoci' : 'rozvojové spolupráce'} dle oficiálního výročního workbooku ${row.sourceWorkbook}.`,
        row.sourceDataset,
      ),
    );
  }

  return { year, nodes, links };
}

export async function getAtlasMzvGraph(year: number, nodeId: string | null = null) {
  const [budgetRows, diplomaticMetrics, aidBranchMetrics] = await Promise.all([
    getMzvBudgetEntities(year),
    getMzvDiplomaticMetrics(year),
    getMzvAidBranchMetrics(year),
  ]);

  if (!budgetRows.length) return null;
  if (!nodeId || nodeId === MZV_MINISTRY_ID) {
    return buildMzvRootGraph(year, budgetRows, diplomaticMetrics, aidBranchMetrics);
  }
  if (nodeId === MZV_FOREIGN_SERVICE_ID) {
    return buildMzvForeignServiceGraph(year, budgetRows, diplomaticMetrics, aidBranchMetrics);
  }
  if (nodeId === MZV_DEVELOPMENT_ID || nodeId === MZV_HUMANITARIAN_ID) {
    const branchCode = nodeId === MZV_HUMANITARIAN_ID ? 'HUMANITARIAN' : 'DEVELOPMENT';
    const branchMetric = aidBranchMetric(aidBranchMetrics, branchCode);
    if (!branchMetric) return null;
    const countryRows = await getMzvAidCountryMetrics(year, branchCode);
    return buildMzvAidCountryGraph(year, branchMetric, countryRows);
  }
  const country = parseCountryNodeId(nodeId);
  if (country) {
    const projectRows = await getMzvAidProjects(year, country.branchCode, country.countryName);
    return buildMzvAidProjectGraph(year, country.branchCode, country.countryName, projectRows);
  }
  return null;
}
