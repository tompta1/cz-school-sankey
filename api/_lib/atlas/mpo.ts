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

interface MpoBudgetEntity {
  year: number;
  entityIco: string;
  entityName: string;
  entityKind: string;
  expenses: number;
  costs: number;
  sourceDataset: string;
}

interface MpoRecipientMetric {
  year: number;
  recipientCount: number;
  projectCount: number;
  allocatedTotal: number;
  sourceDataset: string;
}

interface MpoRecipientAggregate {
  year: number;
  recipientKey: string;
  recipientName: string;
  recipientIco: string | null;
  projectCount: number;
  allocatedTotal: number;
  sourceDataset: string;
}

const STATE_ID = 'state:cr';
const MPO_MINISTRY_ID = 'mpo:ministry:mpo';
const MPO_SUPPORT_ID = 'mpo:optak:support';
const MPO_ADMIN_ID = 'mpo:admin';
const MPO_RECIPIENT_PREFIX = 'mpo:recipient:';
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

function budgetEntityAmount(entity: MpoBudgetEntity): number {
  return entity.expenses > 0 ? entity.expenses : entity.costs;
}

function recipientNodeId(recipientKey: string): string {
  return `${MPO_RECIPIENT_PREFIX}${recipientKey}`;
}

function createMinistryNode(): AtlasNode {
  return {
    id: MPO_MINISTRY_ID,
    name: 'Ministerstvo prumyslu a obchodu',
    category: 'ministry',
    level: 1,
    metadata: { focus: 'mpo' },
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
      focus: 'mpo',
    },
  };
}

function createRecipientNode(row: MpoRecipientAggregate): AtlasNode {
  return {
    id: recipientNodeId(row.recipientKey),
    name: row.recipientName,
    category: 'other',
    level: 3,
    ico: row.recipientIco ?? undefined,
    metadata: {
      capacity: 1,
      projectCount: row.projectCount,
      focus: 'mpo',
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
      focus: 'mpo',
    },
  };
}

export async function getMpoBudgetEntities(year: number): Promise<MpoBudgetEntity[]> {
  const result = await query(
    `
      select
        reporting_year,
        entity_ico,
        entity_name,
        entity_kind,
        expenses_czk,
        costs_czk
      from mart.mpo_budget_entity_latest
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
    sourceDataset: 'mpo_budget_entities',
  }));
}

export async function getMpoRecipientMetric(year: number): Promise<MpoRecipientMetric | null> {
  const result = await query(
    `
      select
        reporting_year,
        recipient_count,
        project_count,
        allocated_total_czk
      from mart.mpo_optak_recipient_metric_latest
      where reporting_year = $1
    `,
    [year],
  );

  const row = result.rows[0];
  if (!row) return null;
  return {
    year: Number(row.reporting_year),
    recipientCount: Number(row.recipient_count),
    projectCount: Number(row.project_count),
    allocatedTotal: toNumber(row.allocated_total_czk),
    sourceDataset: 'mpo_optak_operations',
  };
}

async function getMpoRecipients(year: number): Promise<MpoRecipientAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        recipient_key,
        max(recipient_name) as recipient_name,
        max(recipient_ico) as recipient_ico,
        count(*)::integer as project_count,
        sum(allocated_total_czk) as allocated_total_czk
      from mart.mpo_optak_operation_yearly_latest
      where reporting_year = $1
      group by reporting_year, recipient_key
      order by allocated_total_czk desc, recipient_name asc
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    recipientKey: String(row.recipient_key),
    recipientName: String(row.recipient_name),
    recipientIco: row.recipient_ico ? String(row.recipient_ico) : null,
    projectCount: Number(row.project_count),
    allocatedTotal: toNumber(row.allocated_total_czk),
    sourceDataset: 'mpo_optak_operations',
  }));
}

export function getMpoTotal(budgetRows: MpoBudgetEntity[]): number {
  return budgetRows.reduce((sum, row) => sum + budgetEntityAmount(row), 0);
}

export function appendMpoBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  budgetRows: MpoBudgetEntity[],
  recipientMetric: MpoRecipientMetric | null,
): void {
  const totalBudget = getMpoTotal(budgetRows);
  if (totalBudget <= 0) return;

  const supportCommitmentAmount = recipientMetric?.allocatedTotal ?? 0;
  const supportAmount = Math.min(totalBudget, supportCommitmentAmount);
  const adminAmount = Math.max(totalBudget - supportAmount, 0);

  addNode(nodes, createMinistryNode());
  links.push(
    makeLink(
      STATE_ID,
      MPO_MINISTRY_ID,
      totalBudget,
      year,
      'state_to_mpo_resort',
      'První iterace MPO používá skutečný roční objem z Monitoru MF. Drilldown je zatím navázaný jen na OP TAK operace ze seznamu příjemců DotaceEU.',
      'mpo_budget_entities',
    ),
  );

  if (supportAmount > 0 && recipientMetric) {
    addNode(nodes, createBranchNode(MPO_SUPPORT_ID, 'OP TAK podpory podnikum', 2, recipientMetric.recipientCount, true));
    links.push(
      makeLink(
        MPO_MINISTRY_ID,
        MPO_SUPPORT_ID,
        supportAmount,
        year,
        'mpo_optak_support_branch',
        supportCommitmentAmount > totalBudget
          ? 'Větev používá přidělené způsobilé výdaje OP TAK a drilldown do kraje a IČO z veřejného workbooku DotaceEU. Pokud veřejně evidované OP TAK závazky převyšují roční výdaj kapitoly MPO, atlas větev ořezává na skutečný roční výdaj MPO.'
          : 'Větev používá přidělené způsobilé výdaje OP TAK a drilldown do kraje a IČO z veřejného workbooku DotaceEU.',
        recipientMetric.sourceDataset,
      ),
    );
  }

  if (adminAmount > 0) {
    addNode(nodes, createBranchNode(MPO_ADMIN_ID, 'MPO a ostatni vydaje resortu', 2, null, false));
    links.push(
      makeLink(
        MPO_MINISTRY_ID,
        MPO_ADMIN_ID,
        adminAmount,
        year,
        'mpo_admin_branch',
        'Reziduální vrstva po oddělení veřejně dohledatelných OP TAK podpor.',
        'mpo_budget_entities',
      ),
    );
  }
}

function buildMpoRootGraph(year: number, budgetRows: MpoBudgetEntity[], recipientMetric: MpoRecipientMetric | null) {
  const nodes: AtlasNode[] = [createMinistryNode()];
  const links: AtlasLink[] = [];
  appendMpoBranch(nodes, links, year, budgetRows, recipientMetric);
  return links.length ? { year, nodes, links } : null;
}

function buildMpoRecipientGraph(
  year: number,
  recipientMetric: MpoRecipientMetric,
  supportAmount: number,
  recipientRows: MpoRecipientAggregate[],
  offset: number,
) {
  const totalAllocated = recipientRows.reduce((sum, row) => sum + row.allocatedTotal, 0);
  if (supportAmount <= 0 || totalAllocated <= 0) return null;

  const nodes: AtlasNode[] = [createBranchNode(MPO_SUPPORT_ID, 'OP TAK podpory podnikum', 2, recipientMetric.recipientCount, true)];
  const links: AtlasLink[] = [];

  const pageRows = recipientRows.slice(offset, offset + PAGE_SIZE);
  const prevRows = recipientRows.slice(0, offset);
  const nextRows = recipientRows.slice(offset + PAGE_SIZE);

  if (prevRows.length > 0) {
    addNode(nodes, createPagerNode(PREV_WINDOW_ID, prevRows.length));
    links.push(
      makeLink(
        MPO_SUPPORT_ID,
        PREV_WINDOW_ID,
        supportAmount * (prevRows.reduce((sum, row) => sum + row.allocatedTotal, 0) / totalAllocated),
        year,
        'mpo_optak_recipient_page',
        'Předchozí okno příjemců OP TAK podle přidělených způsobilých výdajů.',
        'atlas.inferred',
      ),
    );
  }

  for (const row of pageRows) {
    addNode(nodes, createRecipientNode(row));
    links.push(
      makeLink(
        MPO_SUPPORT_ID,
        recipientNodeId(row.recipientKey),
        supportAmount * (row.allocatedTotal / totalAllocated),
        year,
        'mpo_optak_recipient_allocated',
        'Recipient-level alokace používá podíl přidělených způsobilých výdajů OP TAK u daného příjemce.',
        row.sourceDataset,
      ),
    );
  }

  if (nextRows.length > 0) {
    addNode(nodes, createPagerNode(NEXT_WINDOW_ID, nextRows.length));
    links.push(
      makeLink(
        MPO_SUPPORT_ID,
        NEXT_WINDOW_ID,
        supportAmount * (nextRows.reduce((sum, row) => sum + row.allocatedTotal, 0) / totalAllocated),
        year,
        'mpo_optak_recipient_page',
        'Následující okno příjemců OP TAK podle přidělených způsobilých výdajů.',
        'atlas.inferred',
      ),
    );
  }

  return { year, nodes, links };
}

export async function getAtlasMpoGraph(year: number, nodeId: string | null = null, offset = 0) {
  const [budgetRows, recipientMetric] = await Promise.all([
    getMpoBudgetEntities(year),
    getMpoRecipientMetric(year),
  ]);

  if (!nodeId || nodeId === MPO_MINISTRY_ID) {
    return buildMpoRootGraph(year, budgetRows, recipientMetric);
  }

  if (nodeId === MPO_SUPPORT_ID) {
    if (!recipientMetric) return null;
    const recipientRows = await getMpoRecipients(year);
    return buildMpoRecipientGraph(year, recipientMetric, Math.min(getMpoTotal(budgetRows), recipientMetric.allocatedTotal), recipientRows, offset);
  }

  return null;
}
