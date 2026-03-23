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

interface MfBudgetEntity {
  year: number;
  entityIco: string;
  entityName: string;
  entityKind: string;
  expenses: number;
  costs: number;
  sourceDataset: string;
}

interface MfActivityMetric {
  year: number;
  metricCode: string;
  metricName: string;
  countValue: number;
  sourceDataset: string;
}

const STATE_ID = 'state:cr';
const MF_MINISTRY_ID = 'mf:ministry:mf';
const MF_TAX_ADMIN_ID = 'mf:branch:tax-admin';
const MF_CUSTOMS_ID = 'mf:branch:customs';
const MF_CORE_ID = 'mf:branch:ministry-core';

const ENTITY_KIND_TO_BRANCH: Record<string, { id: string; name: string }> = {
  tax_admin: { id: MF_TAX_ADMIN_ID, name: 'Finanční správa (GFŘ)' },
  customs: { id: MF_CUSTOMS_ID, name: 'Celní správa (GŘC)' },
  ministry_core: { id: MF_CORE_ID, name: 'MF – vlastní aparát' },
};

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

function budgetEntityAmount(entity: MfBudgetEntity): number {
  return entity.expenses > 0 ? entity.expenses : entity.costs;
}

function taxSubjectCount(rows: MfActivityMetric[]): number | null {
  const value = rows.find((row) => row.metricCode === 'TAX_SUBJECTS')?.countValue ?? 0;
  return value > 0 ? value : null;
}

export async function getMfBudgetEntities(year: number): Promise<MfBudgetEntity[]> {
  const result = await query(
    `
      select
        reporting_year,
        entity_ico,
        entity_name,
        entity_kind,
        expenses_czk,
        costs_czk
      from mart.mf_budget_entity_latest
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
    sourceDataset: 'mf_budget_entities',
  }));
}

export async function getMfActivityMetrics(year: number): Promise<MfActivityMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        metric_code,
        metric_name,
        count_value
      from mart.mf_activity_metric_latest
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
    sourceDataset: 'mf_activity_metrics',
  }));
}

export function getMfTotal(budgetRows: MfBudgetEntity[]): number {
  return budgetRows.reduce((sum, row) => sum + budgetEntityAmount(row), 0);
}

export function appendMfBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  budgetRows: MfBudgetEntity[],
  activityMetrics: MfActivityMetric[],
): void {
  const totalBudget = getMfTotal(budgetRows);
  if (totalBudget <= 0) return;

  const denominator = taxSubjectCount(activityMetrics);

  addNode(nodes, {
    id: MF_MINISTRY_ID,
    name: 'Ministerstvo financí',
    category: 'ministry',
    level: 1,
    metadata: {
      ...(denominator && denominator > 0 ? { capacity: denominator } : {}),
      focus: 'finance',
      drilldownAvailable: true,
    },
  });
  links.push(
    makeLink(
      STATE_ID,
      MF_MINISTRY_ID,
      totalBudget,
      year,
      'mf_ministry_total',
      'Celkový objem kapitoly MF jako součet výdajů Ministerstva financí, Generálního finančního ředitelství a Generálního ředitelství cel z Monitoru MF.',
      budgetRows[0]?.sourceDataset ?? 'mf_budget_entities',
    ),
  );

  for (const entity of budgetRows) {
    const branch = ENTITY_KIND_TO_BRANCH[entity.entityKind];
    if (!branch) continue;
    const amount = budgetEntityAmount(entity);
    if (amount <= 0) continue;
    addNode(nodes, {
      id: branch.id,
      name: branch.name,
      category: 'other',
      level: 2,
      metadata: {
        ...(denominator && denominator > 0 ? { capacity: denominator } : {}),
        focus: 'finance',
        drilldownAvailable: false,
      },
    });
    links.push(
      makeLink(
        MF_MINISTRY_ID,
        branch.id,
        amount,
        year,
        'mf_budget_entity',
        `Výdaje organizační složky státu ${entity.entityName} (IČO ${entity.entityIco}) z Monitoru MF; jde o přímo pozorovaný rozpočtový objem příslušné složky.`,
        entity.sourceDataset,
      ),
    );
  }
}

function buildMfRootGraph(
  year: number,
  budgetRows: MfBudgetEntity[],
  activityMetrics: MfActivityMetric[],
) {
  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  appendMfBranch(nodes, links, year, budgetRows, activityMetrics);
  return links.length ? { year, nodes, links } : null;
}

export async function getAtlasMfGraph(year: number, nodeId: string | null = null) {
  const [budgetRows, activityMetrics] = await Promise.all([
    getMfBudgetEntities(year),
    getMfActivityMetrics(year),
  ]);

  if (!budgetRows.length) return null;
  if (!nodeId || nodeId === MF_MINISTRY_ID) {
    return buildMfRootGraph(year, budgetRows, activityMetrics);
  }
  return null;
}
