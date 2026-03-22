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

interface JusticeBudgetAggregate {
  year: number;
  basis: string;
  metricGroup: string;
  metricCode: string;
  metricName: string;
  amount: number;
  sourceDataset: string;
}

interface JusticeActivityAggregate {
  year: number;
  activityDomain: string;
  metricCode: string;
  metricName: string;
  countValue: number;
  sourceDataset: string;
}

interface JusticeBranchRow {
  id: string;
  name: string;
  amount: number;
  capacity: number | null;
  note: string;
}

const STATE_ID = 'state:cr';
const JUSTICE_MINISTRY_ID = 'justice:ministry:msp';

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

function justiceAmountByCode(rows: JusticeBudgetAggregate[], metricCode: string): number {
  return rows.find((row) => row.metricCode === metricCode)?.amount ?? 0;
}

function justiceActivityByCode(rows: JusticeActivityAggregate[], metricCode: string): number | null {
  const value = rows.find((row) => row.metricCode === metricCode)?.countValue ?? 0;
  return value > 0 ? value : null;
}

function createJusticeMinistryNode(): AtlasNode {
  return {
    id: JUSTICE_MINISTRY_ID,
    name: 'Ministerstvo spravedlnosti',
    category: 'ministry',
    level: 1,
  };
}

function createJusticeBranchNode(
  id: string,
  name: string,
  capacity: number | null = null,
): AtlasNode {
  return {
    id,
    name,
    category: 'other',
    level: 2,
    metadata: {
      ...(capacity ? { capacity } : {}),
      focus: 'justice',
    },
  };
}

function buildJusticeBranchRows(
  budgetRows: JusticeBudgetAggregate[],
  activityRows: JusticeActivityAggregate[],
): JusticeBranchRow[] {
  const courtsAmount = justiceAmountByCode(budgetRows, 'courts');
  const justiceBlock = justiceAmountByCode(budgetRows, 'justice_block');
  const prosecutionAmount = justiceAmountByCode(budgetRows, 'prosecution');
  const prisonServiceAmount = justiceAmountByCode(budgetRows, 'prison_service');
  const probationAmount = justiceAmountByCode(budgetRows, 'probation_service');
  const socialAmount = justiceAmountByCode(budgetRows, 'social_and_prevention');
  const adminAmount =
    justiceAmountByCode(budgetRows, 'ministry_admin') +
    justiceAmountByCode(budgetRows, 'justice_research') +
    justiceAmountByCode(budgetRows, 'justice_other') +
    justiceAmountByCode(budgetRows, 'residual_other');

  const courtsCapacity = justiceActivityByCode(activityRows, 'courts_disposed_total');
  const prisonCapacity = justiceActivityByCode(activityRows, 'prison_average_daily_inmates_total');

  if (courtsAmount > 0) {
    return [
      {
        id: 'justice:courts',
        name: 'Soudy',
        amount: courtsAmount,
        capacity: courtsCapacity,
        note: 'Výdaje soudnictví; metrika používá vyřízené věci v hlavních agendách soudů',
      },
      {
        id: 'justice:prosecution',
        name: 'Státní zastupitelství',
        amount: prosecutionAmount,
        capacity: null,
        note: 'Výdaje státního zastupitelství',
      },
      {
        id: 'justice:prison-service',
        name: 'Vězeňská služba',
        amount: prisonServiceAmount,
        capacity: prisonCapacity,
        note: 'Výdaje vězeňství; metrika používá průměrný denní stav vězněných osob',
      },
      {
        id: 'justice:probation',
        name: 'Probační a mediační služba',
        amount: probationAmount,
        capacity: null,
        note: 'Výdaje Probační a mediační služby',
      },
      {
        id: 'justice:social',
        name: 'Sociální dávky a prevence',
        amount: socialAmount,
        capacity: null,
        note: 'Sociální dávky, podpory a související prevenční výdaje kapitoly MSp',
      },
      {
        id: 'justice:admin',
        name: 'Správa a ostatní',
        amount: adminAmount,
        capacity: null,
        note: 'Správa, výzkum, ostatní právní ochrana a reziduální výdaje kapitoly MSp',
      },
    ];
  }

  return [
    {
      id: 'justice:justice-block',
      name: 'Justiční část',
      amount: justiceBlock,
      capacity: null,
      note: 'Rozpočtový blok justiční části kapitoly MSp',
    },
    {
      id: 'justice:prison-service',
      name: 'Vězeňská služba',
      amount: prisonServiceAmount,
      capacity: null,
      note: 'Ostatní výdaje vězeňské části',
    },
    {
      id: 'justice:social',
      name: 'Sociální dávky a prevence',
      amount: socialAmount,
      capacity: null,
      note: 'Dávky důchodového pojištění, ostatní sociální dávky a prevenční programy kapitoly MSp',
    },
  ];
}

export async function getJusticeBudgetAggregates(year: number): Promise<JusticeBudgetAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        basis,
        metric_group,
        metric_code,
        metric_name,
        amount_czk
      from mart.justice_budget_aggregate_latest
      where reporting_year = $1
      order by metric_group, metric_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    basis: String(row.basis),
    metricGroup: String(row.metric_group),
    metricCode: String(row.metric_code),
    metricName: String(row.metric_name),
    amount: toNumber(row.amount_czk),
    sourceDataset: 'justice_budget_aggregates',
  }));
}

export async function getJusticeActivityAggregates(year: number): Promise<JusticeActivityAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        activity_domain,
        metric_code,
        metric_name,
        count_value
      from mart.justice_activity_aggregate_latest
      where reporting_year = $1
      order by activity_domain, metric_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    activityDomain: String(row.activity_domain),
    metricCode: String(row.metric_code),
    metricName: String(row.metric_name),
    countValue: toNumber(row.count_value),
    sourceDataset: 'justice_activity_aggregates',
  }));
}

export function getJusticeTotal(rows: JusticeBudgetAggregate[]): number {
  return justiceAmountByCode(rows, 'total_expenditure');
}

export function appendJusticeBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  justiceBudgetRows: JusticeBudgetAggregate[],
  justiceActivityRows: JusticeActivityAggregate[],
): void {
  const justiceTotal = getJusticeTotal(justiceBudgetRows);
  if (justiceTotal <= 0) return;

  addNode(nodes, createJusticeMinistryNode());
  links.push(
    makeLink(
      STATE_ID,
      JUSTICE_MINISTRY_ID,
      justiceTotal,
      year,
      'state_to_justice_ministry',
      'MSp: rozpočtové ukazatele / závěrečný účet kapitoly 336',
      'justice_budget_aggregates',
    ),
  );

  for (const branch of buildJusticeBranchRows(justiceBudgetRows, justiceActivityRows)) {
    if (branch.amount <= 0) continue;
    addNode(nodes, createJusticeBranchNode(branch.id, branch.name, branch.capacity));
    links.push(
      makeLink(
        JUSTICE_MINISTRY_ID,
        branch.id,
        branch.amount,
        year,
        'justice_branch_cost',
        branch.note,
        'justice_budget_aggregates',
      ),
    );
  }
}

export function buildJusticeRootGraph(
  year: number,
  budgetRows: JusticeBudgetAggregate[],
  activityRows: JusticeActivityAggregate[],
) {
  const total = getJusticeTotal(budgetRows);
  if (total <= 0) return null;

  const nodes: AtlasNode[] = [createJusticeMinistryNode()];
  const links: AtlasLink[] = [];

  for (const branch of buildJusticeBranchRows(budgetRows, activityRows)) {
    if (branch.amount <= 0) continue;
    addNode(nodes, createJusticeBranchNode(branch.id, branch.name, branch.capacity));
    links.push(
      makeLink(
        JUSTICE_MINISTRY_ID,
        branch.id,
        branch.amount,
        year,
        'justice_branch_cost',
        branch.note,
        'justice_budget_aggregates',
      ),
    );
  }

  return { year, nodes, links };
}
