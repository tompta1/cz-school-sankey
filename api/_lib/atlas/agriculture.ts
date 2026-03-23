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

interface AgricultureFamilyMetric {
  year: number;
  familyCode: 'AREA' | 'LIVESTOCK' | 'INVESTMENT' | 'OTHER';
  familyName: string;
  recipientCount: number;
  amount: number;
  sourceDataset: string;
}

interface AgricultureLpisMetric {
  year: number;
  areaHa: number;
  userCount: number;
  sourceDataset: string;
}

interface AgricultureRecipientAggregate {
  year: number;
  familyCode: AgricultureFamilyMetric['familyCode'];
  familyName: string;
  recipientKey: string;
  recipientName: string;
  recipientIco: string | null;
  municipality: string | null;
  district: string | null;
  amount: number;
  paymentCount: number;
  areaHa: number | null;
  sourceDataset: string;
}

const STATE_ID = 'state:cr';
const AGRICULTURE_MINISTRY_ID = 'agriculture:ministry:mze';
const AGRICULTURE_SUBSIDY_TOTAL_ID = 'agriculture:subsidy:total';
const AGRICULTURE_SUBSIDY_AREA_ID = 'agriculture:subsidy:family:area';
const AGRICULTURE_SUBSIDY_LIVESTOCK_ID = 'agriculture:subsidy:family:livestock';
const AGRICULTURE_SUBSIDY_INVESTMENT_ID = 'agriculture:subsidy:family:investment';
const AGRICULTURE_SUBSIDY_OTHER_ID = 'agriculture:subsidy:family:other';
const AGRICULTURE_ADMIN_ID = 'agriculture:admin';
const AGRICULTURE_ADMIN_ENTITY_PREFIX = 'agriculture:admin-entity:';
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

function familyNodeId(familyCode: AgricultureFamilyMetric['familyCode']): string {
  if (familyCode === 'AREA') return AGRICULTURE_SUBSIDY_AREA_ID;
  if (familyCode === 'LIVESTOCK') return AGRICULTURE_SUBSIDY_LIVESTOCK_ID;
  if (familyCode === 'INVESTMENT') return AGRICULTURE_SUBSIDY_INVESTMENT_ID;
  return AGRICULTURE_SUBSIDY_OTHER_ID;
}

function familyFlowType(familyCode: AgricultureFamilyMetric['familyCode']): string {
  if (familyCode === 'AREA') return 'agriculture_subsidy_family_area';
  return 'agriculture_subsidy_family_recipient';
}

function agricultureRecipientNodeId(recipientKey: string): string {
  return `${AGRICULTURE_RECIPIENT_PREFIX}${recipientKey}`;
}

function agricultureAdminEntityNodeId(entityIco: string): string {
  return `${AGRICULTURE_ADMIN_ENTITY_PREFIX}${entityIco}`;
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
  const capacity = row.familyCode === 'AREA'
    ? (row.areaHa && row.areaHa > 0 ? row.areaHa : null)
    : 1;
  return {
    id: agricultureRecipientNodeId(row.recipientKey),
    name: row.recipientName,
    category: 'other',
    level: 4,
    ico: row.recipientIco ?? undefined,
    metadata: {
      ...(capacity ? { capacity } : {}),
      municipality: row.municipality,
      district: row.district,
      paymentCount: row.paymentCount,
      focus: 'agriculture',
    },
  };
}

function createAgricultureAdminEntityNode(row: AgricultureBudgetEntity, name = row.entityName): AtlasNode {
  return {
    id: agricultureAdminEntityNodeId(row.entityIco),
    name,
    category: row.entityKind === 'ministry' ? 'ministry' : 'other',
    level: 3,
    ico: row.entityIco,
    metadata: {
      drilldownAvailable: false,
      focus: 'agriculture',
      entityKind: row.entityKind,
    },
  };
}

function createPagerNode(id: typeof PREV_WINDOW_ID | typeof NEXT_WINDOW_ID, label: string): AtlasNode {
  return {
    id,
    name: label,
    category: 'other',
    level: 5,
    metadata: {
      focus: 'agriculture',
      drilldownAvailable: true,
    },
  };
}

function createRecipientPagerLabel(direction: 'prev' | 'next', hiddenCount: number): string {
  const arrow = direction === 'prev' ? '↑' : '↓';
  return `${arrow} dalsi prijemci (${hiddenCount})`;
}

function agricultureAdminAmount(budgetRows: AgricultureBudgetEntity[]): number {
  return budgetRows.reduce((sum, row) => sum + budgetEntityAmount(row), 0);
}

function agricultureResidualAmount(
  budgetRows: AgricultureBudgetEntity[],
  metrics: AgricultureRecipientMetric[],
): number {
  const totalMetric = agricultureMetricBySource(metrics, 'TOTAL');
  if (!totalMetric) return 0;
  return Math.max(agricultureAdminAmount(budgetRows) - totalMetric.amount, 0);
}

function agricultureResidualEntityAmount(row: AgricultureBudgetEntity, subsidyAmount: number): number {
  const amount = budgetEntityAmount(row);
  if (row.entityIco === '48133981') {
    return Math.max(amount - subsidyAmount, 0);
  }
  return amount;
}

function familyMetricByCode(
  metrics: AgricultureFamilyMetric[],
  familyCode: AgricultureFamilyMetric['familyCode'],
): AgricultureFamilyMetric | null {
  return metrics.find((row) => row.familyCode === familyCode) ?? null;
}

function lpisMetricValue(lpisMetric: AgricultureLpisMetric | null): number | null {
  if (!lpisMetric || lpisMetric.areaHa <= 0) return null;
  return lpisMetric.areaHa;
}

export function getAgricultureTotal(
  budgetRows: AgricultureBudgetEntity[],
  metrics: AgricultureRecipientMetric[],
): number {
  const totalMetric = agricultureMetricBySource(metrics, 'TOTAL');
  if (!totalMetric || totalMetric.amount <= 0) return 0;
  return agricultureAdminAmount(budgetRows);
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

export async function getAgricultureFamilyMetrics(year: number): Promise<AgricultureFamilyMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        family_code,
        family_name,
        recipient_count,
        amount_czk
      from mart.agriculture_szif_family_metric_latest
      where reporting_year = $1
      order by amount_czk desc, family_code asc
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    familyCode: String(row.family_code) as AgricultureFamilyMetric['familyCode'],
    familyName: String(row.family_name),
    recipientCount: Number(row.recipient_count),
    amount: toNumber(row.amount_czk),
    sourceDataset: 'agriculture_szif_payments',
  }));
}

export async function getAgricultureLpisMetric(year: number): Promise<AgricultureLpisMetric | null> {
  const result = await query(
    `
      with matched_users as (
        select distinct
          reporting_year,
          recipient_name_normalized
        from mart.agriculture_szif_family_recipient_yearly_latest
        where reporting_year = $1
          and family_code = 'AREA'
          and amount_czk > 0
          and recipient_name_normalized <> ''
      )
      select
        l.reporting_year,
        sum(l.area_ha) as area_ha,
        count(*)::integer as user_count
      from mart.agriculture_lpis_user_area_yearly_latest l
      join matched_users m
        on m.reporting_year = l.reporting_year
       and m.recipient_name_normalized = l.user_name_normalized
      group by l.reporting_year
    `,
    [year],
  );
  if (!result.rows.length) return null;
  const row = result.rows[0];
  return {
    year: Number(row.reporting_year),
    areaHa: toNumber(row.area_ha),
    userCount: Number(row.user_count),
    sourceDataset: 'agriculture_lpis_user_area',
  };
}

export async function getAgricultureRecipientsByFamily(
  year: number,
  familyCode: AgricultureFamilyMetric['familyCode'],
): Promise<AgricultureRecipientAggregate[]> {
  const result = await query(
    `
      with recipients as (
        select
          reporting_year,
          family_code,
          family_name,
          recipient_key,
          recipient_name,
          recipient_name_normalized,
          recipient_ico,
          municipality,
          district,
          amount_czk,
          payment_count
        from mart.agriculture_szif_family_recipient_yearly_latest
        where reporting_year = $1
          and family_code = $2
          and amount_czk > 0
          and coalesce(recipient_ico, '') not in (${ADMIN_LIKE_RECIPIENT_ICOS.map((_, index) => `$${index + 3}`).join(', ')})
      )
      select
        r.reporting_year,
        r.family_code,
        r.family_name,
        r.recipient_key,
        r.recipient_name,
        r.recipient_ico,
        r.municipality,
        r.district,
        r.amount_czk,
        r.payment_count,
        case
          when r.family_code = 'AREA' then l.area_ha
          else null
        end as area_ha
      from recipients r
      left join mart.agriculture_lpis_user_area_yearly_latest l
        on l.reporting_year = r.reporting_year
       and l.user_name_normalized = r.recipient_name_normalized
      order by amount_czk desc, recipient_name asc
    `,
    [year, familyCode, ...ADMIN_LIKE_RECIPIENT_ICOS],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    familyCode: String(row.family_code) as AgricultureFamilyMetric['familyCode'],
    familyName: String(row.family_name),
    recipientKey: String(row.recipient_key),
    recipientName: String(row.recipient_name),
    recipientIco: row.recipient_ico ? String(row.recipient_ico) : null,
    municipality: row.municipality ? String(row.municipality) : null,
    district: row.district ? String(row.district) : null,
    amount: toNumber(row.amount_czk),
    paymentCount: Number(row.payment_count),
    areaHa: row.area_ha == null ? null : toNumber(row.area_ha),
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

  const residualAmount = agricultureResidualAmount(budgetRows, metrics);
  const rootTotal = agricultureAdminAmount(budgetRows);

  addNode(nodes, createAgricultureMinistryNode());
  links.push(
    makeLink(
      STATE_ID,
      AGRICULTURE_MINISTRY_ID,
      rootTotal,
      year,
      'state_to_agriculture_resort',
      'Synteticka osa resortu zemedelstvi: celkove vydaje MZe a SZIF z Monitoru MF, z nichz je vyclenena primarni dotacni vetev publikovana SZIF',
      'atlas.inferred',
    ),
  );

  addNode(nodes, createAgricultureBranchNode(AGRICULTURE_SUBSIDY_TOTAL_ID, 'Zemedelske dotace pres SZIF', 2, null, true));
  links.push(
    makeLink(
      AGRICULTURE_MINISTRY_ID,
      AGRICULTURE_SUBSIDY_TOTAL_ID,
      totalMetric.amount,
      year,
      'agriculture_subsidy_branch',
      'SZIF: soucet fondovych i narodnich zemedelskych dotaci bez technicke pomoci pro MZe a SZIF',
      totalMetric.sourceDataset,
    ),
  );

  if (residualAmount > 0) {
    addNode(nodes, createAgricultureBranchNode(AGRICULTURE_ADMIN_ID, 'MZe a SZIF ostatni vydaje', 2, null, true));
    links.push(
      makeLink(
        AGRICULTURE_MINISTRY_ID,
        AGRICULTURE_ADMIN_ID,
        residualAmount,
        year,
        'agriculture_admin_branch',
        'Monitor MF: rezidualni vydaje MZe a SZIF po odecteni primych dotaci publikovanych v seznamech SZIF',
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

  const residualAmount = agricultureResidualAmount(budgetRows, metrics);
  const nodes: AtlasNode[] = [createAgricultureMinistryNode()];
  const links: AtlasLink[] = [];

  addNode(nodes, createAgricultureBranchNode(AGRICULTURE_SUBSIDY_TOTAL_ID, 'Zemedelske dotace pres SZIF', 2, null, true));
  links.push(
    makeLink(
      AGRICULTURE_MINISTRY_ID,
      AGRICULTURE_SUBSIDY_TOTAL_ID,
      totalMetric.amount,
      year,
      'agriculture_subsidy_branch',
      'SZIF: soucet fondovych i narodnich zemedelskych dotaci bez technicke pomoci pro MZe a SZIF',
      totalMetric.sourceDataset,
    ),
  );

  if (residualAmount > 0) {
    addNode(nodes, createAgricultureBranchNode(AGRICULTURE_ADMIN_ID, 'MZe a SZIF ostatni vydaje', 2, null, true));
    links.push(
      makeLink(
        AGRICULTURE_MINISTRY_ID,
        AGRICULTURE_ADMIN_ID,
        residualAmount,
        year,
        'agriculture_admin_branch',
        'Monitor MF: rezidualni vydaje MZe a SZIF po odecteni primych dotaci publikovanych v seznamech SZIF',
        'agriculture_budget_entities',
      ),
    );
  }

  return { year, nodes, links };
}

function buildAgricultureAdminGraph(year: number, budgetRows: AgricultureBudgetEntity[], metrics: AgricultureRecipientMetric[]) {
  const totalMetric = agricultureMetricBySource(metrics, 'TOTAL');
  if (!totalMetric || totalMetric.amount <= 0) return null;

  const adminRows = budgetRows
    .map((row) => ({ ...row, amount: agricultureResidualEntityAmount(row, totalMetric.amount) }))
    .filter((row) => row.amount > 0)
    .sort((a, b) => b.amount - a.amount || a.entityName.localeCompare(b.entityName, 'cs'));

  if (adminRows.length === 0) return null;

  const nodes: AtlasNode[] = [createAgricultureBranchNode(AGRICULTURE_ADMIN_ID, 'MZe a SZIF ostatni vydaje', 2, null, true)];
  const links: AtlasLink[] = [];

  for (const row of adminRows) {
    const name = row.entityIco === '48133981' ? 'SZIF mimo prime dotace' : row.entityName;
    addNode(nodes, createAgricultureAdminEntityNode(row, name));
    links.push(
      makeLink(
        AGRICULTURE_ADMIN_ID,
        agricultureAdminEntityNodeId(row.entityIco),
        row.amount,
        year,
        'agriculture_admin_entity',
        'Monitor MF: rozpad rezidualni vrstvy MZe a SZIF po odecteni primych dotaci vyplacenych pres SZIF',
        row.sourceDataset,
      ),
    );
  }

  return { year, nodes, links };
}

function buildAgricultureFamilyGraph(
  year: number,
  totalMetric: AgricultureRecipientMetric,
  familyMetrics: AgricultureFamilyMetric[],
  lpisMetric: AgricultureLpisMetric | null,
) {
  const nodes: AtlasNode[] = [
    createAgricultureBranchNode(AGRICULTURE_SUBSIDY_TOTAL_ID, 'Zemedelske dotace pres SZIF', 2, null, true),
  ];
  const links: AtlasLink[] = [];

  for (const familyMetric of familyMetrics) {
    if (familyMetric.amount <= 0) continue;
    const nodeId = familyNodeId(familyMetric.familyCode);
    const capacity = familyMetric.familyCode === 'AREA' ? lpisMetricValue(lpisMetric) : familyMetric.recipientCount;
    addNode(nodes, createAgricultureBranchNode(nodeId, familyMetric.familyName, 3, capacity, true));
    links.push(
      makeLink(
        AGRICULTURE_SUBSIDY_TOTAL_ID,
        nodeId,
        familyMetric.amount,
        year,
        familyFlowType(familyMetric.familyCode),
        familyMetric.familyCode === 'AREA'
          ? 'SZIF area-linkovane podpory; srovnavaci metrika pouziva celkovou vymeru aktualni verejne vrstvy LPIS jako nejblizsi dostupny hektarovy jmenovatel'
          : 'SZIF agregace prijemcu do tematicke rodiny opatreni; srovnavaci metrika na teto vetvi zustava u poctu prijemcu',
        familyMetric.familyCode === 'AREA' && lpisMetric ? 'agriculture_lpis_user_area' : familyMetric.sourceDataset,
      ),
    );
  }

  if (!links.length && totalMetric.amount <= 0) return null;
  return { year, nodes, links };
}

function buildAgricultureRecipientGraph(
  year: number,
  familyMetric: AgricultureFamilyMetric,
  recipients: AgricultureRecipientAggregate[],
  offset = 0,
) {
  const sourceNodeId = familyNodeId(familyMetric.familyCode);
  const familyCapacity = familyMetric.familyCode === 'AREA'
    ? recipients.reduce((sum, row) => sum + (row.areaHa ?? 0), 0) || null
    : familyMetric.recipientCount;
  const nodes: AtlasNode[] = [
    createAgricultureBranchNode(sourceNodeId, familyMetric.familyName, 3, familyCapacity, true),
  ];
  const links: AtlasLink[] = [];

  const pageRows = recipients.slice(offset, offset + PAGE_SIZE);
  const prevRows = recipients.slice(0, offset);
  const nextRows = recipients.slice(offset + PAGE_SIZE);

  const aggregatePageCapacity = (rows: AgricultureRecipientAggregate[]) => {
    if (!rows.length) return null;
    if (familyMetric.familyCode === 'AREA') {
      const area = rows.reduce((sum, row) => sum + (row.areaHa ?? 0), 0);
      return area > 0 ? area : null;
    }
    return rows.length;
  };

  if (prevRows.length > 0) {
    const prevCapacity = aggregatePageCapacity(prevRows);
    const prevNode = createPagerNode(PREV_WINDOW_ID, createRecipientPagerLabel('prev', prevRows.length));
    if (prevCapacity) {
      prevNode.metadata = { ...(prevNode.metadata ?? {}), capacity: prevCapacity };
    }
    addNode(nodes, prevNode);
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
        'agriculture_subsidy_recipient_detail',
        'SZIF: agregovany cisty objem dotaci prijemce ve zvolene rodine opatreni',
        row.sourceDataset,
      ),
    );
  }

  if (nextRows.length > 0) {
    const nextCapacity = aggregatePageCapacity(nextRows);
    const nextNode = createPagerNode(NEXT_WINDOW_ID, createRecipientPagerLabel('next', nextRows.length));
    if (nextCapacity) {
      nextNode.metadata = { ...(nextNode.metadata ?? {}), capacity: nextCapacity };
    }
    addNode(nodes, nextNode);
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
    const [metrics, familyMetrics, lpisMetric] = await Promise.all([
      getAgricultureRecipientMetrics(year),
      getAgricultureFamilyMetrics(year),
      getAgricultureLpisMetric(year),
    ]);
    const totalMetric = agricultureMetricBySource(metrics, 'TOTAL');
    if (!totalMetric || totalMetric.amount <= 0) return null;
    return buildAgricultureFamilyGraph(year, totalMetric, familyMetrics, lpisMetric);
  }

  if (nodeId === AGRICULTURE_ADMIN_ID) {
    const [budgetRows, metrics] = await Promise.all([getAgricultureBudgetEntities(year), getAgricultureRecipientMetrics(year)]);
    return buildAgricultureAdminGraph(year, budgetRows, metrics);
  }

  const familyCodeByNodeId: Record<string, AgricultureFamilyMetric['familyCode']> = {
    [AGRICULTURE_SUBSIDY_AREA_ID]: 'AREA',
    [AGRICULTURE_SUBSIDY_LIVESTOCK_ID]: 'LIVESTOCK',
    [AGRICULTURE_SUBSIDY_INVESTMENT_ID]: 'INVESTMENT',
    [AGRICULTURE_SUBSIDY_OTHER_ID]: 'OTHER',
  };
  const familyCode = familyCodeByNodeId[nodeId];
  if (familyCode) {
    const [familyMetrics, recipients] = await Promise.all([
      getAgricultureFamilyMetrics(year),
      getAgricultureRecipientsByFamily(year, familyCode),
    ]);
    const familyMetric = familyMetricByCode(familyMetrics, familyCode);
    if (!familyMetric || familyMetric.amount <= 0) return null;
    return buildAgricultureRecipientGraph(year, familyMetric, recipients, offset);
  }

  return null;
}
