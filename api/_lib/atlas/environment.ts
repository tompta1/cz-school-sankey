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

interface EnvironmentBudgetEntity {
  year: number;
  entityIco: string;
  entityName: string;
  entityKind: string;
  expenses: number;
  costs: number;
  sourceDataset: string;
}

interface EnvironmentRecipientMetric {
  year: number;
  recipientCount: number;
  supportAmount: number;
  paidAmount: number;
  sourceDataset: string;
}

interface EnvironmentFamilyMetric {
  year: number;
  programCode: string;
  programName: string;
  recipientCount: number;
  supportAmount: number;
  paidAmount: number;
  sourceDataset: string;
}

interface EnvironmentRecipientAggregate {
  year: number;
  programCode: string;
  programName: string;
  recipientKey: string;
  recipientName: string;
  recipientIco: string | null;
  municipality: string | null;
  supportAmount: number;
  paidAmount: number;
  projectCount: number;
  sourceDataset: string;
}

const STATE_ID = 'state:cr';
const ENVIRONMENT_MINISTRY_ID = 'environment:ministry:mzp';
const ENVIRONMENT_SUPPORT_ID = 'environment:sfzp:support';
const ENVIRONMENT_ADMIN_ID = 'environment:admin';
const ENVIRONMENT_RECIPIENT_PREFIX = 'environment:recipient:';
const ENVIRONMENT_ADMIN_ENTITY_PREFIX = 'environment:admin-entity:';
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

function budgetEntityAmount(entity: EnvironmentBudgetEntity): number {
  return entity.expenses > 0 ? entity.expenses : entity.costs;
}

function environmentRecipientNodeId(recipientKey: string): string {
  return `${ENVIRONMENT_RECIPIENT_PREFIX}${recipientKey}`;
}

function environmentAdminEntityNodeId(entityIco: string): string {
  return `${ENVIRONMENT_ADMIN_ENTITY_PREFIX}${entityIco}`;
}

function familyNodeId(programCode: string): string {
  return `environment:family:${programCode.toLowerCase()}`;
}

function createEnvironmentMinistryNode(): AtlasNode {
  return {
    id: ENVIRONMENT_MINISTRY_ID,
    name: 'Ministerstvo zivotniho prostredi',
    category: 'ministry',
    level: 1,
    metadata: { focus: 'environment' },
  };
}

function createEnvironmentBranchNode(
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
      focus: 'environment',
    },
  };
}

function createEnvironmentRecipientNode(row: EnvironmentRecipientAggregate): AtlasNode {
  return {
    id: environmentRecipientNodeId(row.recipientKey),
    name: row.recipientName,
    category: 'other',
    level: 4,
    ico: row.recipientIco ?? undefined,
    metadata: {
      capacity: 1,
      municipality: row.municipality,
      projectCount: row.projectCount,
      focus: 'environment',
    },
  };
}

function createEnvironmentAdminEntityNode(row: EnvironmentBudgetEntity, name = row.entityName): AtlasNode {
  return {
    id: environmentAdminEntityNodeId(row.entityIco),
    name,
    category: row.entityKind === 'ministry_admin' ? 'ministry' : 'other',
    level: 3,
    ico: row.entityIco,
    metadata: {
      drilldownAvailable: false,
      focus: 'environment',
      entityKind: row.entityKind,
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
      focus: 'environment',
      drilldownAvailable: true,
    },
  };
}

function environmentRecipientMetric(metrics: EnvironmentRecipientMetric[]): EnvironmentRecipientMetric | null {
  return metrics[0] ?? null;
}

export async function getEnvironmentBudgetEntities(year: number): Promise<EnvironmentBudgetEntity[]> {
  const result = await query(
    `
      select
        reporting_year,
        entity_ico,
        entity_name,
        entity_kind,
        expenses_czk,
        costs_czk
      from mart.environment_budget_entity_latest
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
    sourceDataset: 'environment_budget_entities',
  }));
}

export async function getEnvironmentRecipientMetrics(year: number): Promise<EnvironmentRecipientMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        recipient_count,
        support_czk,
        paid_czk
      from mart.environment_sfzp_recipient_metric_latest
      where reporting_year = $1
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    recipientCount: Number(row.recipient_count),
    supportAmount: toNumber(row.support_czk),
    paidAmount: toNumber(row.paid_czk),
    sourceDataset: 'environment_sfzp_supports',
  }));
}

export async function getEnvironmentFamilyMetrics(year: number): Promise<EnvironmentFamilyMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        program_code,
        program_name,
        recipient_count,
        support_czk,
        paid_czk
      from mart.environment_sfzp_family_metric_latest
      where reporting_year = $1
      order by support_czk desc, program_code asc
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    programCode: String(row.program_code),
    programName: String(row.program_name),
    recipientCount: Number(row.recipient_count),
    supportAmount: toNumber(row.support_czk),
    paidAmount: toNumber(row.paid_czk),
    sourceDataset: 'environment_sfzp_supports',
  }));
}

export async function getEnvironmentRecipientsByFamily(year: number, programCode: string): Promise<EnvironmentRecipientAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        program_code,
        program_name,
        recipient_key,
        recipient_name,
        recipient_ico,
        municipality,
        support_czk,
        paid_czk,
        project_count
      from mart.environment_sfzp_support_yearly_latest
      where reporting_year = $1
        and program_code = $2
        and support_czk > 0
      order by support_czk desc, recipient_name asc
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
    municipality: row.municipality ? String(row.municipality) : null,
    supportAmount: toNumber(row.support_czk),
    paidAmount: toNumber(row.paid_czk),
    projectCount: Number(row.project_count),
    sourceDataset: 'environment_sfzp_supports',
  }));
}

export function getEnvironmentTotal(
  budgetRows: EnvironmentBudgetEntity[],
): number {
  return budgetRows.reduce((sum, row) => sum + budgetEntityAmount(row), 0);
}

export function appendEnvironmentBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  budgetRows: EnvironmentBudgetEntity[],
  recipientMetrics: EnvironmentRecipientMetric[],
): void {
  const totalBudget = getEnvironmentTotal(budgetRows);
  if (totalBudget <= 0) return;

  const sfzpBudget = budgetRows
    .filter((row) => row.entityIco === '00020729')
    .reduce((sum, row) => sum + budgetEntityAmount(row), 0);
  const adminBudget = Math.max(totalBudget - sfzpBudget, 0);
  const supportMetric = environmentRecipientMetric(recipientMetrics);

  addNode(nodes, createEnvironmentMinistryNode());
  links.push(
    makeLink(
      STATE_ID,
      ENVIRONMENT_MINISTRY_ID,
      totalBudget,
      year,
      'state_to_environment_resort',
      'Resort MŽP v první iteraci pokrývá Monitor data za MŽP a SFŽP. Podpory SFŽP se dále rozpadají podle otevřeného registru podpor.',
      'environment_budget_entities',
    ),
  );

  if (sfzpBudget > 0 && supportMetric && supportMetric.supportAmount > 0) {
    addNode(nodes, createEnvironmentBranchNode(ENVIRONMENT_SUPPORT_ID, 'SFZP podpory', 2, supportMetric.recipientCount, true));
    links.push(
      makeLink(
        ENVIRONMENT_MINISTRY_ID,
        ENVIRONMENT_SUPPORT_ID,
        sfzpBudget,
        year,
        'environment_sfzp_support_branch',
        'Skutečné výdaje SFŽP z Monitoru MF; drilldown do programů používá otevřený registr SFŽP a rozděluje tuto větev podle podílů schválených podpor v roce podpisu rozhodnutí.',
        'environment_budget_entities',
      ),
    );
  }

  if (adminBudget > 0) {
    addNode(nodes, createEnvironmentBranchNode(ENVIRONMENT_ADMIN_ID, 'MZP a ostatni vydaje resortu', 2, null, true));
    links.push(
      makeLink(
        ENVIRONMENT_MINISTRY_ID,
        ENVIRONMENT_ADMIN_ID,
        adminBudget,
        year,
        'environment_admin_branch',
        'Reziduální vrstva po oddělení větve SFŽP podpory.',
        'environment_budget_entities',
      ),
    );
  }
}

function buildEnvironmentRootGraph(
  year: number,
  budgetRows: EnvironmentBudgetEntity[],
  recipientMetrics: EnvironmentRecipientMetric[],
) {
  const nodes: AtlasNode[] = [createEnvironmentMinistryNode()];
  const links: AtlasLink[] = [];
  appendEnvironmentBranch(nodes, links, year, budgetRows, recipientMetrics);
  return links.length ? { year, nodes, links } : null;
}

function buildEnvironmentAdminGraph(
  year: number,
  budgetRows: EnvironmentBudgetEntity[],
) {
  const adminRows = budgetRows
    .filter((row) => row.entityIco !== '00020729')
    .sort((a, b) => budgetEntityAmount(b) - budgetEntityAmount(a) || a.entityName.localeCompare(b.entityName, 'cs'));

  if (!adminRows.length) return null;

  const nodes: AtlasNode[] = [createEnvironmentBranchNode(ENVIRONMENT_ADMIN_ID, 'MZP a ostatni vydaje resortu', 2, null, true)];
  const links: AtlasLink[] = [];

  for (const row of adminRows) {
    const amount = budgetEntityAmount(row);
    if (amount <= 0) continue;
    addNode(nodes, createEnvironmentAdminEntityNode(row));
    links.push(
      makeLink(
        ENVIRONMENT_ADMIN_ID,
        environmentAdminEntityNodeId(row.entityIco),
        amount,
        year,
        'environment_admin_entity',
        'Rozpad reziduální vrstvy podle budget entity z Monitoru MF.',
        row.sourceDataset,
      ),
    );
  }

  return links.length ? { year, nodes, links } : null;
}

function buildEnvironmentFamilyGraph(
  year: number,
  supportBranchAmount: number,
  familyMetrics: EnvironmentFamilyMetric[],
) {
  const totalSupportAmount = familyMetrics.reduce((sum, row) => sum + row.supportAmount, 0);
  if (supportBranchAmount <= 0 || totalSupportAmount <= 0) return null;

  const nodes: AtlasNode[] = [
    createEnvironmentBranchNode(ENVIRONMENT_SUPPORT_ID, 'SFZP podpory', 2, familyMetrics.reduce((sum, row) => sum + row.recipientCount, 0), true),
  ];
  const links: AtlasLink[] = [];

  for (const metric of familyMetrics) {
    if (metric.supportAmount <= 0) continue;
    const amount = supportBranchAmount * (metric.supportAmount / totalSupportAmount);
    const nodeId = familyNodeId(metric.programCode);
    addNode(nodes, createEnvironmentBranchNode(nodeId, metric.programName, 3, metric.recipientCount, true));
    links.push(
      makeLink(
        ENVIRONMENT_SUPPORT_ID,
        nodeId,
        amount,
        year,
        'environment_support_family_allocated',
        'Větev rodiny podpor je alokována podle podílu schválené podpory v aktivním registru SFŽP za rok podpisu rozhodnutí.',
        metric.sourceDataset,
      ),
    );
  }

  return links.length ? { year, nodes, links } : null;
}

function buildEnvironmentRecipientGraph(
  year: number,
  familyMetric: EnvironmentFamilyMetric,
  familyBranchAmount: number,
  recipients: EnvironmentRecipientAggregate[],
  offset: number,
) {
  if (familyBranchAmount <= 0) return null;
  const totalSupportAmount = recipients.reduce((sum, row) => sum + row.supportAmount, 0);
  if (totalSupportAmount <= 0) return null;

  const sourceNodeId = familyNodeId(familyMetric.programCode);
  const nodes: AtlasNode[] = [
    createEnvironmentBranchNode(sourceNodeId, familyMetric.programName, 3, familyMetric.recipientCount, true),
  ];
  const links: AtlasLink[] = [];

  const sortedRecipients = [...recipients].sort((a, b) => b.supportAmount - a.supportAmount || a.recipientName.localeCompare(b.recipientName, 'cs'));
  const pageRows = sortedRecipients.slice(offset, offset + PAGE_SIZE);
  const prevRows = sortedRecipients.slice(0, offset);
  const nextRows = sortedRecipients.slice(offset + PAGE_SIZE);

  if (prevRows.length > 0) {
    addNode(nodes, createPagerNode(PREV_WINDOW_ID, prevRows.length));
    links.push(
      makeLink(
        sourceNodeId,
        PREV_WINDOW_ID,
        familyBranchAmount * (prevRows.reduce((sum, row) => sum + row.supportAmount, 0) / totalSupportAmount),
        year,
        'environment_support_recipient_page',
        'Předchozí okno příjemců podpory podle objemu podpory.',
        'atlas.inferred',
      ),
    );
  }

  for (const row of pageRows) {
    addNode(nodes, createEnvironmentRecipientNode(row));
    links.push(
      makeLink(
        sourceNodeId,
        environmentRecipientNodeId(row.recipientKey),
        familyBranchAmount * (row.supportAmount / totalSupportAmount),
        year,
        'environment_support_recipient_allocated',
        'Příjemce podpory je alokován podle podílu schválené podpory v aktivním registru SFŽP za rok podpisu rozhodnutí.',
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
        familyBranchAmount * (nextRows.reduce((sum, row) => sum + row.supportAmount, 0) / totalSupportAmount),
        year,
        'environment_support_recipient_page',
        'Následující okno příjemců podpory podle objemu podpory.',
        'atlas.inferred',
      ),
    );
  }

  return { year, nodes, links };
}

export async function getAtlasEnvironmentGraph(
  year: number,
  nodeId: string | null = null,
  offset = 0,
) {
  const [budgetRows, recipientMetrics, familyMetrics] = await Promise.all([
    getEnvironmentBudgetEntities(year),
    getEnvironmentRecipientMetrics(year),
    getEnvironmentFamilyMetrics(year),
  ]);

  const totalBudget = getEnvironmentTotal(budgetRows);
  const sfzpBudget = budgetRows
    .filter((row) => row.entityIco === '00020729')
    .reduce((sum, row) => sum + budgetEntityAmount(row), 0);

  if (!nodeId || nodeId === ENVIRONMENT_MINISTRY_ID) {
    return buildEnvironmentRootGraph(year, budgetRows, recipientMetrics);
  }

  if (nodeId === ENVIRONMENT_ADMIN_ID) {
    return buildEnvironmentAdminGraph(year, budgetRows);
  }

  if (nodeId === ENVIRONMENT_SUPPORT_ID) {
    return buildEnvironmentFamilyGraph(year, sfzpBudget, familyMetrics);
  }

  const familyMetric = familyMetrics.find((row) => familyNodeId(row.programCode) === nodeId) ?? null;
  if (familyMetric) {
    const totalSupportAmount = familyMetrics.reduce((sum, row) => sum + row.supportAmount, 0);
    if (sfzpBudget <= 0 || totalSupportAmount <= 0) return null;
    const familyBranchAmount = sfzpBudget * (familyMetric.supportAmount / totalSupportAmount);
    const recipients = await getEnvironmentRecipientsByFamily(year, familyMetric.programCode);
    return buildEnvironmentRecipientGraph(year, familyMetric, familyBranchAmount, recipients, offset);
  }

  return null;
}
