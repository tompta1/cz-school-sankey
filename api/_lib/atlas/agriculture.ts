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

interface AgricultureBudgetEntity {
  year: number;
  entityIco: string;
  entityName: string;
  entityKind: string;
  expenses: number;
  costs: number;
  sourceDataset: string;
}

interface AgricultureRecipientMetric {
  year: number;
  fundingSourceCode: 'TOTAL' | 'EU' | 'NATIONAL';
  fundingSourceName: string;
  recipientCount: number;
  amount: number;
  sourceDataset: string;
}

interface AgricultureRecipientAggregate {
  year: number;
  fundingSourceCode: 'EU' | 'NATIONAL';
  fundingSourceName: string;
  recipientKey: string;
  recipientName: string;
  recipientIco: string | null;
  municipality: string | null;
  district: string | null;
  amount: number;
  paymentCount: number;
  sourceDataset: string;
}

const STATE_ID = 'state:cr';
const AGRICULTURE_MINISTRY_ID = 'agriculture:ministry:mze';
const AGRICULTURE_SUBSIDY_TOTAL_ID = 'agriculture:subsidy:total';
const AGRICULTURE_SUBSIDY_EU_ID = 'agriculture:subsidy:eu';
const AGRICULTURE_SUBSIDY_NATIONAL_ID = 'agriculture:subsidy:national';
const AGRICULTURE_ADMIN_ID = 'agriculture:admin';
const AGRICULTURE_RECIPIENT_PREFIX = 'agriculture:recipient:';
const PREV_WINDOW_ID = 'synthetic:prev-window';
const NEXT_WINDOW_ID = 'synthetic:next-window';
const PAGE_SIZE = 28;
const ADMIN_LIKE_RECIPIENT_ICOS = ['00020478', '48133981'] as const;

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

function budgetEntityAmount(entity: AgricultureBudgetEntity): number {
  return entity.expenses > 0 ? entity.expenses : entity.costs;
}

function agricultureMetricBySource(
  metrics: AgricultureRecipientMetric[],
  fundingSourceCode: AgricultureRecipientMetric['fundingSourceCode'],
): AgricultureRecipientMetric | null {
  return metrics.find((row) => row.fundingSourceCode === fundingSourceCode) ?? null;
}

function agricultureRecipientNodeId(recipientKey: string): string {
  return `${AGRICULTURE_RECIPIENT_PREFIX}${recipientKey}`;
}

function createAgricultureMinistryNode(): AtlasNode {
  return {
    id: AGRICULTURE_MINISTRY_ID,
    name: 'Ministerstvo zemedelstvi',
    category: 'ministry',
    level: 1,
    metadata: {
      focus: 'agriculture',
    },
  };
}

function createAgricultureBranchNode(
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
      focus: 'agriculture',
    },
  };
}

function createAgricultureRecipientNode(row: AgricultureRecipientAggregate): AtlasNode {
  return {
    id: agricultureRecipientNodeId(row.recipientKey),
    name: row.recipientName,
    category: 'other',
    level: 3,
    ico: row.recipientIco ?? undefined,
    metadata: {
      capacity: 1,
      municipality: row.municipality,
      district: row.district,
      paymentCount: row.paymentCount,
      focus: 'agriculture',
    },
  };
}

function createPagerNode(id: typeof PREV_WINDOW_ID | typeof NEXT_WINDOW_ID, label: string): AtlasNode {
  return {
    id,
    name: label,
    category: 'other',
    level: 4,
    metadata: {
      focus: 'agriculture',
      drilldownAvailable: true,
    },
  };
}

function agricultureAdminAmount(budgetRows: AgricultureBudgetEntity[]): number {
  return budgetRows.reduce((sum, row) => sum + budgetEntityAmount(row), 0);
}

export function getAgricultureTotal(
  budgetRows: AgricultureBudgetEntity[],
  metrics: AgricultureRecipientMetric[],
): number {
  const totalMetric = agricultureMetricBySource(metrics, 'TOTAL');
  if (!totalMetric || totalMetric.amount <= 0) return 0;
  return agricultureAdminAmount(budgetRows) + totalMetric.amount;
}

export async function getAgricultureBudgetEntities(year: number): Promise<AgricultureBudgetEntity[]> {
  const result = await query(
    `
      select
        reporting_year,
        entity_ico,
        entity_name,
        entity_kind,
        expenses_czk,
        costs_czk
      from mart.agriculture_budget_entity_latest
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
    sourceDataset: 'agriculture_budget_entities',
  }));
}

export async function getAgricultureRecipientMetrics(year: number): Promise<AgricultureRecipientMetric[]> {
  const filteredResult = await query(
    `
      with filtered as (
        select
          reporting_year,
          funding_source_code,
          max(funding_source_name) as funding_source_name,
          count(*) filter (where amount_czk > 0)::integer as recipient_count,
          sum(amount_czk) as amount_czk
        from mart.agriculture_szif_recipient_yearly_latest
        where reporting_year = $1
          and coalesce(recipient_ico, '') not in (${ADMIN_LIKE_RECIPIENT_ICOS.map((_, index) => `$${index + 2}`).join(', ')})
        group by reporting_year, funding_source_code
      ),
      total as (
        select
          reporting_year,
          'TOTAL'::text as funding_source_code,
          'Zemedelske dotace pres SZIF'::text as funding_source_name,
          count(*) filter (where amount_czk > 0)::integer as recipient_count,
          sum(amount_czk) as amount_czk
        from (
          select
            reporting_year,
            recipient_key,
            sum(amount_czk) as amount_czk
          from mart.agriculture_szif_recipient_yearly_latest
          where reporting_year = $1
            and coalesce(recipient_ico, '') not in (${ADMIN_LIKE_RECIPIENT_ICOS.map((_, index) => `$${index + 2}`).join(', ')})
          group by reporting_year, recipient_key
        ) dedup
        group by reporting_year
      )
      select * from filtered
      union all
      select * from total
      order by funding_source_code
    `,
    [year, ...ADMIN_LIKE_RECIPIENT_ICOS],
  );

  return filteredResult.rows.map((row) => ({
    year: Number(row.reporting_year),
    fundingSourceCode: String(row.funding_source_code) as AgricultureRecipientMetric['fundingSourceCode'],
    fundingSourceName: String(row.funding_source_name),
    recipientCount: Number(row.recipient_count),
    amount: toNumber(row.amount_czk),
    sourceDataset: 'agriculture_szif_payments',
  }));
}

export async function getAgricultureRecipients(
  year: number,
  fundingSourceCode: 'EU' | 'NATIONAL',
): Promise<AgricultureRecipientAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        funding_source_code,
        funding_source_name,
        recipient_key,
        recipient_name,
        recipient_ico,
        municipality,
        district,
        amount_czk,
        payment_count
      from mart.agriculture_szif_recipient_yearly_latest
      where reporting_year = $1
        and funding_source_code = $2
        and amount_czk > 0
        and coalesce(recipient_ico, '') not in (${ADMIN_LIKE_RECIPIENT_ICOS.map((_, index) => `$${index + 3}`).join(', ')})
      order by amount_czk desc, recipient_name asc
    `,
    [year, fundingSourceCode, ...ADMIN_LIKE_RECIPIENT_ICOS],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    fundingSourceCode: String(row.funding_source_code) as 'EU' | 'NATIONAL',
    fundingSourceName: String(row.funding_source_name),
    recipientKey: String(row.recipient_key),
    recipientName: String(row.recipient_name),
    recipientIco: row.recipient_ico ? String(row.recipient_ico) : null,
    municipality: row.municipality ? String(row.municipality) : null,
    district: row.district ? String(row.district) : null,
    amount: toNumber(row.amount_czk),
    paymentCount: Number(row.payment_count),
    sourceDataset: 'agriculture_szif_payments',
  }));
}

export function appendAgricultureBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  budgetRows: AgricultureBudgetEntity[],
  metrics: AgricultureRecipientMetric[],
): void {
  const totalMetric = agricultureMetricBySource(metrics, 'TOTAL');
  if (!totalMetric || totalMetric.amount <= 0) return;

  const adminAmount = agricultureAdminAmount(budgetRows);
  const rootTotal = totalMetric.amount + adminAmount;

  addNode(nodes, createAgricultureMinistryNode());
  links.push(
    makeLink(
      STATE_ID,
      AGRICULTURE_MINISTRY_ID,
      rootTotal,
      year,
      'state_to_agriculture_resort',
      'Synteticka osa resortu zemedelstvi: MZe a SZIF sprava z Monitoru MF plus vyplacene dotace publikovane SZIF',
      'atlas.inferred',
    ),
  );

  addNode(
    nodes,
    createAgricultureBranchNode(
      AGRICULTURE_SUBSIDY_TOTAL_ID,
      'Zemedelske dotace pres SZIF',
      2,
      totalMetric.recipientCount,
      true,
    ),
  );
  links.push(
    makeLink(
      AGRICULTURE_MINISTRY_ID,
      AGRICULTURE_SUBSIDY_TOTAL_ID,
      totalMetric.amount,
      year,
      'agriculture_subsidy_branch',
      'SZIF: soucet fondovych i narodnich zemedelskych dotaci bez technicke pomoci pro MZe a SZIF; metrika pouziva pocet jedinecnych prijemcu s kladnou cistou castkou ve fiskalnim roce',
      totalMetric.sourceDataset,
    ),
  );

  if (adminAmount > 0) {
    addNode(nodes, createAgricultureBranchNode(AGRICULTURE_ADMIN_ID, 'MZe a SZIF sprava', 2, null, false));
    links.push(
      makeLink(
        AGRICULTURE_MINISTRY_ID,
        AGRICULTURE_ADMIN_ID,
        adminAmount,
        year,
        'agriculture_admin_branch',
        'Monitor MF: vydaje a naklady MZe a provozu SZIF mimo prime dotacni platby',
        'agriculture_budget_entities',
      ),
    );
  }
}

function buildAgricultureRootGraph(
  year: number,
  budgetRows: AgricultureBudgetEntity[],
  metrics: AgricultureRecipientMetric[],
) {
  const totalMetric = agricultureMetricBySource(metrics, 'TOTAL');
  if (!totalMetric || totalMetric.amount <= 0) return null;

  const adminAmount = agricultureAdminAmount(budgetRows);
  const nodes: AtlasNode[] = [createAgricultureMinistryNode()];
  const links: AtlasLink[] = [];

  addNode(
    nodes,
    createAgricultureBranchNode(
      AGRICULTURE_SUBSIDY_TOTAL_ID,
      'Zemedelske dotace pres SZIF',
      2,
      totalMetric.recipientCount,
      true,
    ),
  );
  links.push(
    makeLink(
      AGRICULTURE_MINISTRY_ID,
      AGRICULTURE_SUBSIDY_TOTAL_ID,
      totalMetric.amount,
      year,
      'agriculture_subsidy_branch',
      'SZIF: soucet fondovych i narodnich zemedelskych dotaci bez technicke pomoci pro MZe a SZIF; metrika pouziva pocet jedinecnych prijemcu s kladnou cistou castkou ve fiskalnim roce',
      totalMetric.sourceDataset,
    ),
  );

  if (adminAmount > 0) {
    addNode(nodes, createAgricultureBranchNode(AGRICULTURE_ADMIN_ID, 'MZe a SZIF sprava', 2, null, false));
    links.push(
      makeLink(
        AGRICULTURE_MINISTRY_ID,
        AGRICULTURE_ADMIN_ID,
        adminAmount,
        year,
        'agriculture_admin_branch',
        'Monitor MF: vydaje a naklady MZe a provozu SZIF mimo prime dotacni platby',
        'agriculture_budget_entities',
      ),
    );
  }

  return { year, nodes, links };
}

function buildAgricultureFundingGraph(year: number, metrics: AgricultureRecipientMetric[]) {
  const totalMetric = agricultureMetricBySource(metrics, 'TOTAL');
  if (!totalMetric || totalMetric.amount <= 0) return null;

  const euMetric = agricultureMetricBySource(metrics, 'EU');
  const nationalMetric = agricultureMetricBySource(metrics, 'NATIONAL');
  const nodes: AtlasNode[] = [
    createAgricultureBranchNode(
      AGRICULTURE_SUBSIDY_TOTAL_ID,
      'Zemedelske dotace pres SZIF',
      2,
      totalMetric.recipientCount,
      true,
    ),
  ];
  const links: AtlasLink[] = [];

  if (euMetric && euMetric.amount > 0) {
    addNode(nodes, createAgricultureBranchNode(AGRICULTURE_SUBSIDY_EU_ID, euMetric.fundingSourceName, 3, euMetric.recipientCount, true));
    links.push(
      makeLink(
        AGRICULTURE_SUBSIDY_TOTAL_ID,
        AGRICULTURE_SUBSIDY_EU_ID,
        euMetric.amount,
        year,
        'agriculture_subsidy_funding',
        'SZIF: uzavreny fiskalni rok EU a spolufinancovani bez technicke pomoci pro MZe a SZIF; metrika pouziva pocet jedinecnych prijemcu s kladnou cistou castkou',
        euMetric.sourceDataset,
      ),
    );
  }

  if (nationalMetric && nationalMetric.amount > 0) {
    addNode(
      nodes,
      createAgricultureBranchNode(
        AGRICULTURE_SUBSIDY_NATIONAL_ID,
        nationalMetric.fundingSourceName,
        3,
        nationalMetric.recipientCount,
        true,
      ),
    );
    links.push(
      makeLink(
        AGRICULTURE_SUBSIDY_TOTAL_ID,
        AGRICULTURE_SUBSIDY_NATIONAL_ID,
        nationalMetric.amount,
        year,
        'agriculture_subsidy_funding',
        'SZIF: narodni zemedelske dotace ve fiskalnim roce bez technicke pomoci pro MZe a SZIF; metrika pouziva pocet jedinecnych prijemcu s kladnou cistou castkou',
        nationalMetric.sourceDataset,
      ),
    );
  }

  return { year, nodes, links };
}

function buildAgricultureRecipientGraph(
  year: number,
  fundingMetric: AgricultureRecipientMetric,
  recipients: AgricultureRecipientAggregate[],
  offset = 0,
) {
  const sourceNodeId =
    fundingMetric.fundingSourceCode === 'EU' ? AGRICULTURE_SUBSIDY_EU_ID : AGRICULTURE_SUBSIDY_NATIONAL_ID;
  const nodes: AtlasNode[] = [
    createAgricultureBranchNode(sourceNodeId, fundingMetric.fundingSourceName, 3, fundingMetric.recipientCount, true),
  ];
  const links: AtlasLink[] = [];

  const pageRows = recipients.slice(offset, offset + PAGE_SIZE);
  const prevRows = recipients.slice(0, offset);
  const nextRows = recipients.slice(offset + PAGE_SIZE);

  if (prevRows.length > 0) {
    addNode(nodes, createPagerNode(PREV_WINDOW_ID, '↑ dalsi prijemci'));
    links.push(
      makeLink(
        sourceNodeId,
        PREV_WINDOW_ID,
        prevRows.reduce((sum, row) => sum + row.amount, 0),
        year,
        'agriculture_subsidy_recipient_page',
        'Predchozi okno prijemcu dotaci podle objemu podpory',
        'atlas.inferred',
      ),
    );
  }

  for (const row of pageRows) {
    addNode(nodes, createAgricultureRecipientNode(row));
    links.push(
      makeLink(
        sourceNodeId,
        agricultureRecipientNodeId(row.recipientKey),
        row.amount,
        year,
        'agriculture_subsidy_recipient',
        'SZIF: agregovany cisty objem dotaci prijemce ve zvolenem fiskalnim roce',
        row.sourceDataset,
      ),
    );
  }

  if (nextRows.length > 0) {
    addNode(nodes, createPagerNode(NEXT_WINDOW_ID, '↓ dalsi prijemci'));
    links.push(
      makeLink(
        sourceNodeId,
        NEXT_WINDOW_ID,
        nextRows.reduce((sum, row) => sum + row.amount, 0),
        year,
        'agriculture_subsidy_recipient_page',
        'Nasledujici okno prijemcu dotaci podle objemu podpory',
        'atlas.inferred',
      ),
    );
  }

  return { year, nodes, links };
}

export async function getAtlasAgricultureGraph(year: number, nodeId: string | null = null, offset = 0) {
  if (!nodeId || nodeId === AGRICULTURE_MINISTRY_ID) {
    const [budgetRows, metrics] = await Promise.all([
      getAgricultureBudgetEntities(year),
      getAgricultureRecipientMetrics(year),
    ]);
    return buildAgricultureRootGraph(year, budgetRows, metrics);
  }

  if (nodeId === AGRICULTURE_SUBSIDY_TOTAL_ID) {
    const metrics = await getAgricultureRecipientMetrics(year);
    return buildAgricultureFundingGraph(year, metrics);
  }

  if (nodeId === AGRICULTURE_SUBSIDY_EU_ID || nodeId === AGRICULTURE_SUBSIDY_NATIONAL_ID) {
    const [metrics, recipients] = await Promise.all([
      getAgricultureRecipientMetrics(year),
      getAgricultureRecipients(year, nodeId === AGRICULTURE_SUBSIDY_EU_ID ? 'EU' : 'NATIONAL'),
    ]);
    const fundingMetric = agricultureMetricBySource(metrics, nodeId === AGRICULTURE_SUBSIDY_EU_ID ? 'EU' : 'NATIONAL');
    if (!fundingMetric || fundingMetric.amount <= 0) return null;
    return buildAgricultureRecipientGraph(year, fundingMetric, recipients, offset);
  }

  return null;
}
