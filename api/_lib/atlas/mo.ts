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

interface MoBudgetEntity {
  year: number;
  entityIco: string;
  entityName: string;
  entityKind: string;
  expenses: number;
  costs: number;
  sourceDataset: string;
}

interface MoBudgetAggregate {
  year: number;
  metricCode: string;
  metricName: string;
  amount: number;
  sourceDataset: string;
}

interface MoPersonnelMetric {
  year: number;
  metricCode: string;
  metricName: string;
  countValue: number;
  sourceDataset: string;
}

const STATE_ID = 'state:cr';
const MO_MINISTRY_ID = 'defense:ministry:mo';
const MO_PROGRAM_ID = 'defense:program-financing';
const MO_PERSONNEL_ID = 'defense:personnel-mandatory';
const MO_OPERATIONS_ID = 'defense:operations-other';

const BRANCHES = [
  {
    metricCode: 'PROGRAM_FINANCING',
    id: MO_PROGRAM_ID,
    name: 'Programove financovani a modernizace',
  },
  {
    metricCode: 'PERSONNEL_MANDATORY',
    id: MO_PERSONNEL_ID,
    name: 'Osobni mandatorni vydaje',
  },
  {
    metricCode: 'OTHER_OPERATING',
    id: MO_OPERATIONS_ID,
    name: 'Ostatni bezne vydaje',
  },
] as const;

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

function budgetEntityAmount(entity: MoBudgetEntity): number {
  return entity.expenses > 0 ? entity.expenses : entity.costs;
}

function createMinistryNode(capacity: number | null): AtlasNode {
  return {
    id: MO_MINISTRY_ID,
    name: 'Ministerstvo obrany',
    category: 'ministry',
    level: 1,
    metadata: {
      ...(capacity && capacity > 0 ? { capacity } : {}),
      focus: 'defense',
      drilldownAvailable: true,
    },
  };
}

function createBranchNode(id: string, name: string, capacity: number | null): AtlasNode {
  return {
    id,
    name,
    category: 'other',
    level: 2,
    metadata: {
      ...(capacity && capacity > 0 ? { capacity } : {}),
      focus: 'defense',
      drilldownAvailable: false,
    },
  };
}

function branchAmount(rows: MoBudgetAggregate[], metricCode: string): number {
  return rows.find((row) => row.metricCode === metricCode)?.amount ?? 0;
}

function soldierCount(rows: MoPersonnelMetric[]): number | null {
  const value = rows.find((row) => row.metricCode === 'PROFESSIONAL_SOLDIERS')?.countValue ?? 0;
  return value > 0 ? value : null;
}

export async function getMoBudgetEntities(year: number): Promise<MoBudgetEntity[]> {
  const result = await query(
    `
      select
        reporting_year,
        entity_ico,
        entity_name,
        entity_kind,
        expenses_czk,
        costs_czk
      from mart.mo_budget_entity_latest
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
    sourceDataset: 'mo_budget_entities',
  }));
}

export async function getMoBudgetAggregates(year: number): Promise<MoBudgetAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        metric_code,
        metric_name,
        amount_czk
      from mart.mo_budget_aggregate_latest
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
    sourceDataset: 'mo_budget_aggregates',
  }));
}

export async function getMoPersonnelMetrics(year: number): Promise<MoPersonnelMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        metric_code,
        metric_name,
        count_value
      from mart.mo_personnel_metric_latest
      where reporting_year = $1
      order by metric_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    metricCode: String(row.metric_code),
    metricName: String(row.metric_name),
    countValue: Number(row.count_value),
    sourceDataset: 'mo_personnel_metrics',
  }));
}

export function getMoTotal(budgetRows: MoBudgetEntity[]): number {
  return budgetRows.reduce((sum, row) => sum + budgetEntityAmount(row), 0);
}

export function appendMoBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  budgetRows: MoBudgetEntity[],
  aggregateRows: MoBudgetAggregate[],
  personnelRows: MoPersonnelMetric[],
): void {
  const totalBudget = getMoTotal(budgetRows);
  if (totalBudget <= 0) return;

  const denominator = soldierCount(personnelRows);
  const aggregateTotal = aggregateRows.reduce((sum, row) => sum + row.amount, 0);
  const scale = aggregateTotal > 0 ? totalBudget / aggregateTotal : 0;

  addNode(nodes, createMinistryNode(denominator));
  links.push(
    makeLink(
      STATE_ID,
      MO_MINISTRY_ID,
      totalBudget,
      year,
      'defense_ministry_total',
      'Horní vrstva MO používá skutečný roční objem z Monitoru MF. Rozpad do tří kategorií je odvozen z oficiální tabulky Fakta a trendy 2025 a škálován na celkový objem kapitoly.',
      budgetRows[0]?.sourceDataset ?? 'mo_budget_entities',
    ),
  );

  for (const branch of BRANCHES) {
    const branchRawAmount = branchAmount(aggregateRows, branch.metricCode);
    if (branchRawAmount <= 0) continue;
    const branchAmountScaled = scale > 0 ? branchRawAmount * scale : branchRawAmount;
    addNode(nodes, createBranchNode(branch.id, branch.name, denominator));
    links.push(
      makeLink(
        MO_MINISTRY_ID,
        branch.id,
        branchAmountScaled,
        year,
        'defense_budget_category',
        'Rozpad obranné kapitoly podle oficiálních kategorií Programové financování, Osobní mandatorní výdaje a Ostatní běžné výdaje z publikace Fakta a trendy 2025; na atlasový celkový objem je přepočten poměrově.',
        'mo_budget_aggregates',
      ),
    );
  }
}

function buildMoRootGraph(
  year: number,
  budgetRows: MoBudgetEntity[],
  aggregateRows: MoBudgetAggregate[],
  personnelRows: MoPersonnelMetric[],
) {
  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  appendMoBranch(nodes, links, year, budgetRows, aggregateRows, personnelRows);
  return links.length ? { year, nodes, links } : null;
}

export async function getAtlasMoGraph(year: number, nodeId: string | null = null) {
  const [budgetRows, aggregateRows, personnelRows] = await Promise.all([
    getMoBudgetEntities(year),
    getMoBudgetAggregates(year),
    getMoPersonnelMetrics(year),
  ]);

  if (!budgetRows.length) return null;
  if (!nodeId || nodeId === MO_MINISTRY_ID) {
    return buildMoRootGraph(year, budgetRows, aggregateRows, personnelRows);
  }
  return null;
}
