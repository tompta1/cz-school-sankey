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

interface MkBudgetEntity {
  year: number;
  entityIco: string;
  entityName: string;
  entityKind: string;
  expenses: number;
  costs: number;
  sourceDataset: string;
}

interface MkBudgetAggregate {
  year: number;
  metricCode: string;
  metricName: string;
  amount: number;
  sourceDataset: string;
}

interface MkProgramMetric {
  year: number;
  programCode: string;
  programName: string;
  recipientCount: number;
  awardedAmount: number;
  sourceDataset: string;
}

interface MkRecipientAggregate {
  year: number;
  programCode: string;
  programName: string;
  recipientKey: string;
  recipientName: string;
  recipientIco: string | null;
  projectCount: number;
  awardedAmount: number;
  sourceDataset: string;
}

interface MkRegionMetric {
  year: number;
  programCode: string;
  programName: string;
  regionCode: string | null;
  regionName: string;
  recipientCount: number;
  awardedAmount: number;
  sourceDataset: string;
}

const STATE_ID = 'state:cr';
const MK_MINISTRY_ID = 'mk:ministry:mk';
const MK_INSTITUTIONS_ID = 'mk:institutions';
const MK_CULTURE_ID = 'mk:support:culture';
const MK_HERITAGE_ID = 'mk:support:heritage';
const MK_FILM_ID = 'mk:film';
const MK_CHURCH_ID = 'mk:churches';
const MK_REGIONAL_INFRA_ID = 'mk:regional-infra';
const MK_ADMIN_ID = 'mk:admin';
const MK_PROGRAM_CULTURE_MUSEUMS_ID = 'mk:program:culture-museums';
const MK_HERITAGE_OTHER_ID = 'mk:heritage:other';
const MK_CULTURE_OTHER_ID = 'mk:culture:other';
const MK_RECIPIENT_PREFIX = 'mk:recipient:';
const MK_REGION_PREFIX = 'mk:region:';
const PREV_WINDOW_ID = 'synthetic:prev-window';
const NEXT_WINDOW_ID = 'synthetic:next-window';
const PAGE_SIZE = 28;

const PROGRAM_IDS = {
  CULTURE_MUSEUMS: MK_PROGRAM_CULTURE_MUSEUMS_ID,
  PZAD: 'mk:program:pzad',
} as const;

type ProgramCode = keyof typeof PROGRAM_IDS;

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

function budgetEntityAmount(entity: MkBudgetEntity): number {
  return entity.expenses > 0 ? entity.expenses : entity.costs;
}

function recipientNodeId(programCode: ProgramCode, recipientKey: string): string {
  return `${MK_RECIPIENT_PREFIX}${programCode.toLowerCase()}|${recipientKey}`;
}

function regionNodeId(regionCode: string | null, regionName: string): string {
  return `${MK_REGION_PREFIX}${regionCode || 'UNKNOWN'}|${regionName}`;
}

function createMinistryNode(): AtlasNode {
  return {
    id: MK_MINISTRY_ID,
    name: 'Ministerstvo kultury',
    category: 'ministry',
    level: 1,
    metadata: { focus: 'mk' },
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
      focus: 'mk',
    },
  };
}

function createRecipientNode(row: MkRecipientAggregate): AtlasNode {
  return {
    id: recipientNodeId(row.programCode as ProgramCode, row.recipientKey),
    name: row.recipientName,
    category: 'other',
    level: 4,
    ico: row.recipientIco ?? undefined,
    metadata: {
      capacity: 1,
      projectCount: row.projectCount,
      focus: 'mk',
    },
  };
}

function createRegionNode(row: MkRegionMetric): AtlasNode {
  return {
    id: regionNodeId(row.regionCode, row.regionName),
    name: row.regionName,
    category: 'region',
    level: 4,
    metadata: {
      capacity: row.recipientCount,
      focus: 'mk',
      drilldownAvailable: false,
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
      focus: 'mk',
    },
  };
}

function budgetAggregateAmount(rows: MkBudgetAggregate[], metricCode: string): number {
  return rows.find((row) => row.metricCode === metricCode)?.amount ?? 0;
}

function programMetric(rows: MkProgramMetric[], programCode: ProgramCode): MkProgramMetric | null {
  return rows.find((row) => row.programCode === programCode) ?? null;
}

function mkCultureNote() {
  return 'Větev používá oficiální skutečnost 2024 pro ukazatel Kulturní služby, podpora živého umění. Drilldown dále rozlišuje explicitně zveřejněný podprogram Kulturní aktivity pro spolky v muzejnictví a zbytek ukazatele.';
}

function mkHeritageNote() {
  return 'Větev památkové péče používá oficiální skutečnost 2024 pro ukazatel Záchrana a obnova kulturních památek, veřejné služby muzeí. Drilldown dále rozlišuje explicitně zveřejněný program PZAD a zbytek ukazatele.';
}

function mkInstitutionsNote() {
  return 'Větev používá oficiální skutečnost 2024 pro ukazatel Příspěvkové organizace zřízené Ministerstvem kultury.';
}

function mkChurchNote() {
  return 'Větev používá oficiální skutečnost 2024 pro ukazatel Výdaje dle zákona o majetkovém vyrovnání s církvemi a náboženskými společnostmi.';
}

function mkRegionalInfraNote() {
  return 'Větev používá oficiální skutečnost 2024 pro ukazatel Podpora rozvoje a obnovy materiálně technické základny regionálních kulturních zařízení.';
}

function mkFilmNote() {
  return 'Větev používá oficiální skutečnost 2024 pro ukazatel Státní fond kinematografie a navazující Státní fond kultury ČR.';
}

export async function getMkBudgetEntities(year: number): Promise<MkBudgetEntity[]> {
  const result = await query(
    `
      select
        reporting_year,
        entity_ico,
        entity_name,
        entity_kind,
        expenses_czk,
        costs_czk
      from mart.mk_budget_entity_latest
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
    sourceDataset: 'mk_budget_entities',
  }));
}

export async function getMkBudgetAggregates(year: number): Promise<MkBudgetAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        metric_code,
        metric_name,
        amount_czk
      from mart.mk_budget_aggregate_latest
      where reporting_year = $1
      order by metric_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    metricCode: String(row.metric_code),
    metricName: String(row.metric_name),
    amount: toNumber(row.amount_czk),
    sourceDataset: 'mk_budget_aggregates',
  }));
}

export async function getMkProgramMetrics(year: number): Promise<MkProgramMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        program_code,
        program_name,
        recipient_count,
        awarded_czk
      from mart.mk_support_program_metric_latest
      where reporting_year = $1
      order by program_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    programCode: String(row.program_code),
    programName: String(row.program_name),
    recipientCount: Number(row.recipient_count),
    awardedAmount: toNumber(row.awarded_czk),
    sourceDataset: row.program_code === 'PZAD' ? 'mk_region_metrics' : 'mk_support_awards',
  }));
}

async function getMkRecipients(year: number, programCode: ProgramCode): Promise<MkRecipientAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        program_code,
        program_name,
        recipient_key,
        recipient_name,
        recipient_ico,
        project_count,
        awarded_czk
      from mart.mk_support_recipient_latest
      where reporting_year = $1
        and program_code = $2
      order by awarded_czk desc, recipient_name asc
    `,
    [year, programCode],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    programCode: String(row.program_code),
    programName: String(row.program_name),
    recipientKey: String(row.recipient_key),
    recipientName: String(row.recipient_name),
    recipientIco: row.recipient_ico ? String(row.recipient_ico) : null,
    projectCount: Number(row.project_count),
    awardedAmount: toNumber(row.awarded_czk),
    sourceDataset: 'mk_support_awards',
  }));
}

async function getMkRegionMetrics(year: number, programCode: ProgramCode): Promise<MkRegionMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        program_code,
        program_name,
        region_code,
        region_name,
        recipient_count,
        awarded_czk
      from mart.mk_region_metric_latest
      where reporting_year = $1
        and program_code = $2
      order by awarded_czk desc, region_name asc
    `,
    [year, programCode],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    programCode: String(row.program_code),
    programName: String(row.program_name),
    regionCode: row.region_code ? String(row.region_code) : null,
    regionName: String(row.region_name),
    recipientCount: Number(row.recipient_count),
    awardedAmount: toNumber(row.awarded_czk),
    sourceDataset: 'mk_region_metrics',
  }));
}

export function getMkTotal(budgetRows: MkBudgetEntity[]): number {
  return budgetRows.reduce((sum, row) => sum + budgetEntityAmount(row), 0);
}

export function appendMkBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  budgetRows: MkBudgetEntity[],
  budgetAggregates: MkBudgetAggregate[],
  programMetrics: MkProgramMetric[],
): void {
  const totalBudget = getMkTotal(budgetRows);
  if (totalBudget <= 0) return;

  const cultureMetric = programMetric(programMetrics, 'CULTURE_MUSEUMS');
  const pzadMetric = programMetric(programMetrics, 'PZAD');

  const institutionsAmount = budgetAggregateAmount(budgetAggregates, 'MK_CONTRIBUTORY_ORGS_TOTAL');
  const cultureAmount = budgetAggregateAmount(budgetAggregates, 'MK_CULTURAL_SERVICES_TOTAL');
  const heritageAmount = budgetAggregateAmount(budgetAggregates, 'MK_HERITAGE_TOTAL');
  const regionalInfraAmount = budgetAggregateAmount(budgetAggregates, 'MK_REGIONAL_INFRA_TOTAL');
  const filmAmount = budgetAggregateAmount(budgetAggregates, 'MK_FILM_TOTAL');
  const churchAmount = budgetAggregateAmount(budgetAggregates, 'CHURCH_SETTLEMENT_TOTAL');
  const adminAmount = Math.max(totalBudget - institutionsAmount - cultureAmount - heritageAmount - regionalInfraAmount - filmAmount - churchAmount, 0);

  addNode(nodes, createMinistryNode());
  links.push(
    makeLink(
      STATE_ID,
      MK_MINISTRY_ID,
      totalBudget,
      year,
      'state_to_mk_resort',
      'MK větev používá skutečný roční objem z Monitoru MF. Drilldown je zatím navázaný jen na explicitně zveřejněné dotační výsledky a agregace vybraných programů MK.',
      'mk_budget_entities',
    ),
  );

  if (institutionsAmount > 0) {
    addNode(nodes, createBranchNode(MK_INSTITUTIONS_ID, 'Prispevkove organizace MK', 2, null, false));
    links.push(makeLink(MK_MINISTRY_ID, MK_INSTITUTIONS_ID, institutionsAmount, year, 'mk_institutions_branch', mkInstitutionsNote(), 'mk_budget_aggregates'));
  }

  if (cultureAmount > 0) {
    addNode(
      nodes,
      createBranchNode(
        MK_CULTURE_ID,
        'Kulturni sluzby a zive umeni',
        2,
        cultureMetric?.recipientCount ?? null,
        true,
      ),
    );
    links.push(makeLink(MK_MINISTRY_ID, MK_CULTURE_ID, cultureAmount, year, 'mk_support_culture_branch', mkCultureNote(), 'mk_budget_aggregates'));
  }

  if (heritageAmount > 0) {
    addNode(
      nodes,
      createBranchNode(
        MK_HERITAGE_ID,
        'Pamatkova pece a muzea',
        2,
        pzadMetric?.recipientCount ?? null,
        true,
      ),
    );
    links.push(makeLink(MK_MINISTRY_ID, MK_HERITAGE_ID, heritageAmount, year, 'mk_support_heritage_branch', mkHeritageNote(), 'mk_budget_aggregates'));
  }

  if (churchAmount > 0) {
    addNode(nodes, createBranchNode(MK_CHURCH_ID, 'Cirkve a majetkove narovnani', 2, null, false));
    links.push(makeLink(MK_MINISTRY_ID, MK_CHURCH_ID, churchAmount, year, 'mk_church_branch', mkChurchNote(), 'mk_budget_aggregates'));
  }

  if (regionalInfraAmount > 0) {
    addNode(nodes, createBranchNode(MK_REGIONAL_INFRA_ID, 'Regionalni kulturni infrastruktura', 2, null, false));
    links.push(makeLink(MK_MINISTRY_ID, MK_REGIONAL_INFRA_ID, regionalInfraAmount, year, 'mk_regional_infra_branch', mkRegionalInfraNote(), 'mk_budget_aggregates'));
  }

  if (filmAmount > 0) {
    addNode(nodes, createBranchNode(MK_FILM_ID, 'Kinematografie a kulturni fondy', 2, null, false));
    links.push(makeLink(MK_MINISTRY_ID, MK_FILM_ID, filmAmount, year, 'mk_film_branch', mkFilmNote(), 'mk_budget_aggregates'));
  }

  if (adminAmount > 0) {
    addNode(nodes, createBranchNode(MK_ADMIN_ID, 'MK a ostatni vydaje resortu', 2, null, false));
    links.push(makeLink(MK_MINISTRY_ID, MK_ADMIN_ID, adminAmount, year, 'mk_admin_branch', 'Reziduální vrstva po oddělení explicitně zdrojově podložených podpůrných programů, filmových pobídek a církevní podpory.', 'mk_budget_entities'));
  }
}

function buildMkRootGraph(year: number, budgetRows: MkBudgetEntity[], budgetAggregates: MkBudgetAggregate[], programMetrics: MkProgramMetric[]) {
  const nodes: AtlasNode[] = [createMinistryNode()];
  const links: AtlasLink[] = [];
  appendMkBranch(nodes, links, year, budgetRows, budgetAggregates, programMetrics);
  return links.length ? { year, nodes, links } : null;
}

function buildCultureProgramGraph(year: number, branchAmount: number, programMetricRows: MkProgramMetric[]) {
  if (branchAmount <= 0) return null;

  const cultureMetric = programMetric(programMetricRows, 'CULTURE_MUSEUMS');
  const explicitAmount = cultureMetric?.awardedAmount ?? 0;
  const residualAmount = Math.max(branchAmount - explicitAmount, 0);
  const nodes: AtlasNode[] = [createBranchNode(MK_CULTURE_ID, 'Kulturni sluzby a zive umeni', 2, cultureMetric?.recipientCount ?? null, true)];
  const links: AtlasLink[] = [];

  if (explicitAmount > 0 && cultureMetric) {
    addNode(nodes, createBranchNode(MK_PROGRAM_CULTURE_MUSEUMS_ID, 'Kulturni aktivity pro spolky v muzejnictvi', 3, cultureMetric.recipientCount, true));
    links.push(makeLink(MK_CULTURE_ID, MK_PROGRAM_CULTURE_MUSEUMS_ID, explicitAmount, year, 'mk_support_program', mkCultureNote(), cultureMetric.sourceDataset));
  }

  if (residualAmount > 0) {
    addNode(nodes, createBranchNode(MK_CULTURE_OTHER_ID, 'Ostatni kulturni sluzby a zive umeni', 3, null, false));
    links.push(makeLink(MK_CULTURE_ID, MK_CULTURE_OTHER_ID, residualAmount, year, 'mk_support_culture_residual', 'Reziduální část oficiálního ukazatele Kulturní služby, podpora živého umění po oddělení explicitně zveřejněného podprogramu.', 'mk_budget_aggregates'));
  }

  return { year, nodes, links };
}

function buildHeritageProgramGraph(year: number, branchAmount: number, programMetricRows: MkProgramMetric[]) {
  if (branchAmount <= 0) return null;

  const pzadMetric = programMetric(programMetricRows, 'PZAD');
  const explicitAmount = pzadMetric?.awardedAmount ?? 0;
  const residualAmount = Math.max(branchAmount - explicitAmount, 0);
  const nodes: AtlasNode[] = [createBranchNode(MK_HERITAGE_ID, 'Pamatkova pece a muzea', 2, pzadMetric?.recipientCount ?? null, true)];
  const links: AtlasLink[] = [];

  if (explicitAmount > 0 && pzadMetric) {
    addNode(nodes, createBranchNode(PROGRAM_IDS.PZAD, 'PZAD', 3, pzadMetric.recipientCount, true));
    links.push(makeLink(MK_HERITAGE_ID, PROGRAM_IDS.PZAD, explicitAmount, year, 'mk_support_program', 'Program záchrany architektonického dědictví (PZAD) podle zveřejněných výsledků MK.', pzadMetric.sourceDataset));
  }

  if (residualAmount > 0) {
    addNode(nodes, createBranchNode(MK_HERITAGE_OTHER_ID, 'Ostatni pamatkova pece a muzea', 3, null, false));
    links.push(makeLink(MK_HERITAGE_ID, MK_HERITAGE_OTHER_ID, residualAmount, year, 'mk_support_heritage_residual', 'Reziduální část oficiálního ukazatele památkové péče po oddělení explicitně zveřejněného programu PZAD.', 'mk_budget_aggregates'));
  }

  return { year, nodes, links };
}

function buildPagedRecipientGraph(
  year: number,
  branchId: string,
  branchName: string,
  note: string,
  metric: MkProgramMetric,
  recipientRows: MkRecipientAggregate[],
  offset: number,
) {
  const totalAwarded = recipientRows.reduce((sum, row) => sum + row.awardedAmount, 0);
  if (totalAwarded <= 0 || metric.awardedAmount <= 0) return null;

  const nodes: AtlasNode[] = [createBranchNode(branchId, branchName, 2, metric.recipientCount, true)];
  const links: AtlasLink[] = [];
  const pageRows = recipientRows.slice(offset, offset + PAGE_SIZE);
  const prevRows = recipientRows.slice(0, offset);
  const nextRows = recipientRows.slice(offset + PAGE_SIZE);

  if (prevRows.length > 0) {
    addNode(nodes, createPagerNode(PREV_WINDOW_ID, prevRows.length));
    links.push(makeLink(branchId, PREV_WINDOW_ID, metric.awardedAmount * (prevRows.reduce((sum, row) => sum + row.awardedAmount, 0) / totalAwarded), year, 'mk_support_page', note, 'atlas.inferred'));
  }

  for (const row of pageRows) {
    addNode(nodes, createRecipientNode(row));
    links.push(makeLink(branchId, recipientNodeId(row.programCode as ProgramCode, row.recipientKey), metric.awardedAmount * (row.awardedAmount / totalAwarded), year, 'mk_support_recipient', note, row.sourceDataset));
  }

  if (nextRows.length > 0) {
    addNode(nodes, createPagerNode(NEXT_WINDOW_ID, nextRows.length));
    links.push(makeLink(branchId, NEXT_WINDOW_ID, metric.awardedAmount * (nextRows.reduce((sum, row) => sum + row.awardedAmount, 0) / totalAwarded), year, 'mk_support_page', note, 'atlas.inferred'));
  }

  return { year, nodes, links };
}

function buildPzadRegionGraph(year: number, metric: MkProgramMetric, regionRows: MkRegionMetric[], branchId = MK_HERITAGE_ID, branchName = 'Pamatkova pece') {
  const totalAwarded = regionRows.reduce((sum, row) => sum + row.awardedAmount, 0);
  if (totalAwarded <= 0 || metric.awardedAmount <= 0) return null;

  const nodes: AtlasNode[] = [createBranchNode(branchId, branchName, 2, metric.recipientCount, true)];
  const links: AtlasLink[] = [];

  for (const row of regionRows) {
    addNode(nodes, createRegionNode(row));
    links.push(
      makeLink(
        branchId,
        regionNodeId(row.regionCode, row.regionName),
        metric.awardedAmount * (row.awardedAmount / totalAwarded),
        year,
        'mk_support_region',
        'Regionální drilldown používá oficiální souhrnné tabulky PZAD 2024 s alokací a počtem příjemců podle kraje.',
        row.sourceDataset,
      ),
    );
  }

  return { year, nodes, links };
}

export async function getAtlasMkGraph(year: number, nodeId: string | null = null, offset = 0) {
  const [budgetRows, budgetAggregates, programMetrics] = await Promise.all([
    getMkBudgetEntities(year),
    getMkBudgetAggregates(year),
    getMkProgramMetrics(year),
  ]);

  if (!nodeId || nodeId === MK_MINISTRY_ID) {
    return buildMkRootGraph(year, budgetRows, budgetAggregates, programMetrics);
  }

  const cultureMetric = programMetric(programMetrics, 'CULTURE_MUSEUMS');
  const pzadMetric = programMetric(programMetrics, 'PZAD');
  const cultureBranchAmount = budgetAggregateAmount(budgetAggregates, 'MK_CULTURAL_SERVICES_TOTAL');
  const heritageBranchAmount = budgetAggregateAmount(budgetAggregates, 'MK_HERITAGE_TOTAL');

  if (nodeId === MK_CULTURE_ID) {
    return buildCultureProgramGraph(year, cultureBranchAmount, programMetrics);
  }

  if (nodeId === MK_PROGRAM_CULTURE_MUSEUMS_ID) {
    if (!cultureMetric) return null;
    const recipientRows = await getMkRecipients(year, 'CULTURE_MUSEUMS');
    return buildPagedRecipientGraph(year, MK_PROGRAM_CULTURE_MUSEUMS_ID, 'Kulturni aktivity pro spolky v muzejnictvi', mkCultureNote(), cultureMetric, recipientRows, offset);
  }

  if (nodeId === MK_HERITAGE_ID) {
    return buildHeritageProgramGraph(year, heritageBranchAmount, programMetrics);
  }

  if (nodeId === PROGRAM_IDS.PZAD) {
    if (!pzadMetric) return null;
    const regionRows = await getMkRegionMetrics(year, 'PZAD');
    return buildPzadRegionGraph(year, pzadMetric, regionRows, PROGRAM_IDS.PZAD, 'PZAD');
  }

  return null;
}
