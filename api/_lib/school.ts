import { query } from './db.js';

type SchoolFounderType = 'kraj' | 'obec' | null;

interface SchoolEntity {
  institutionId: string;
  institutionName: string;
  ico: string | null;
  founderId: string | null;
  founderName: string | null;
  founderType: SchoolFounderType;
  municipality: string | null;
  region: string | null;
  capacity: number | null;
}

interface SchoolAllocationRow {
  institutionId: string;
  pedagogicalAmount: number;
  nonpedagogicalAmount: number;
  onivAmount: number;
  otherAmount: number;
  operationsAmount: number;
  investmentAmount: number;
  bucketBasis: string;
  bucketCertainty: string;
}

interface SchoolEuProjectRow {
  institutionId: string;
  programme: string;
  projectName: string;
  amountCzk: number;
  basis: string;
  certainty: string;
}

interface SchoolFounderSupportRow {
  institutionId: string;
  amountCzk: number;
  basis: string;
  certainty: string;
  note: string | null;
}

type SchoolNode = {
  id: string;
  name: string;
  category: string;
  level: number;
  ico?: string;
  founderType?: string;
  metadata?: Record<string, number>;
};

type SchoolLink = ReturnType<typeof makeLink>;

const BUCKET_META = {
  pedagogical: { id: 'bucket:pedagogical', name: 'Pedagogical staff' },
  nonpedagogical: { id: 'bucket:nonpedagogical', name: 'Non-pedagogical staff' },
  oniv: { id: 'bucket:oniv', name: 'ONIV and materials' },
  other: { id: 'bucket:other', name: 'Other direct MŠMT' },
  operations: { id: 'bucket:operations', name: 'Operations and energy' },
  investment: { id: 'bucket:investment', name: 'Investment and equipment' },
};

const FOUNDERS_KRAJ = 'founders:kraj';
const FOUNDERS_OBEC = 'founders:obec';
const EU_ALL_ID = 'eu:all';
const STATE_ID = 'state:cr';
const MSMT_ID = 'msmt';
const SCHOOL_ROOT_ID = 'school:root';
const TOP_SCHOOLS = 30;
const TOP_FOUNDERS = 25;
const PREV_WINDOW_ID = 'synthetic:prev-window';
const NEXT_WINDOW_ID = 'synthetic:next-window';
const BUCKET_REGION_PREFIX = 'school:bucket-region:';

async function getSchoolPeriod(year: number) {
  const result = await query(
    `
      select reporting_period_id, period_code, calendar_year
      from core.reporting_period
      where domain_code = 'school' and calendar_year = $1
      limit 1
    `,
    [year],
  );
  return result.rows[0] ?? null;
}

export async function getAvailableYears() {
  const result = await query(
    `
      select
        rp.calendar_year as year,
        count(*) filter (where ff.flow_type = 'direct_school_finance') as direct_school_finance_rows,
        count(*) filter (where ff.flow_type = 'school_expenditure') as school_expenditure_rows
      from core.reporting_period rp
      left join core.financial_flow ff on ff.reporting_period_id = rp.reporting_period_id
      where rp.domain_code = 'school'
      group by rp.calendar_year
      order by rp.calendar_year
    `,
  );
  return result.rows.map((row) => ({
    year: Number(row.year),
    directSchoolFinanceRows: Number(row.direct_school_finance_rows),
    schoolExpenditureRows: Number(row.school_expenditure_rows),
  }));
}

export async function getSchoolSummary(year: number) {
  const period = await getSchoolPeriod(year);
  if (!period) return null;

  const [institutionCounts, flowTotals, sources] = await Promise.all([
    query(
      `
        select count(*) as institutions
        from raw.school_entities
        where reporting_year = $1
      `,
      [year],
    ),
    query(
      `
        select flow_type, sum(amount_czk) as total_amount
        from core.financial_flow
        where budget_domain = 'school' and reporting_period_id = $1
        group by flow_type
      `,
      [period.reporting_period_id],
    ),
    query(
      `
        select dataset_code, row_count, status
        from meta.dataset_release
        where domain_code = 'school' and reporting_year = $1
        order by dataset_code
      `,
      [year],
    ),
  ]);

  const totals = Object.fromEntries(
    flowTotals.rows.map((row) => [row.flow_type, Number(row.total_amount)]),
  );

  return {
    year,
    institutions: Number(institutionCounts.rows[0]?.institutions ?? 0),
    totals: {
      directSchoolFinance: totals.direct_school_finance ?? 0,
      founderSupport: totals.founder_support ?? 0,
      euProjectSupport: totals.eu_project_support ?? 0,
      schoolExpenditure: totals.school_expenditure ?? 0,
      stateRevenue: totals.state_revenue ?? 0,
      stateToOther: totals.state_to_other ?? 0,
    },
    sources: sources.rows.map((row) => ({
      datasetCode: row.dataset_code,
      rowCount: Number(row.row_count ?? 0),
      status: row.status,
    })),
  };
}

export async function searchInstitutions(year: number, q: string, limit = 8) {
  const entities = await getSchoolEntities(year);
  const normalizedQuery = q
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase();

  return entities
    .filter((entity) => {
      const haystack = `${entity.institutionName} ${entity.municipality ?? ''} ${entity.region ?? ''}`
        .normalize('NFD')
        .replace(/\p{Diacritic}/gu, '')
        .toLowerCase();
      return haystack.includes(normalizedQuery);
    })
    .slice(0, limit)
    .map((entity) => ({
      id: entity.institutionId,
      name: entity.institutionName,
      ico: entity.ico,
      founderName: entity.founderName,
      founderType: entity.founderType,
      municipality: entity.municipality,
      region: entity.region,
      capacity: entity.capacity ?? undefined,
    }));
}

async function getSchoolEntities(year: number): Promise<SchoolEntity[]> {
  const result = await query(
    `
      select
        institution_id,
        institution_name,
        ico,
        founder_id,
        founder_name,
        founder_type,
        municipality,
        region,
        capacity
      from raw.school_entities
      where reporting_year = $1
      order by institution_id
    `,
    [year],
  );
  return result.rows.map((row) => ({
    institutionId: row.institution_id,
    institutionName: row.institution_name,
    ico: row.ico,
    founderId: row.founder_id,
    founderName: row.founder_name,
    founderType: row.founder_type,
    municipality: row.municipality,
    region: row.region,
    capacity: row.capacity == null ? null : Number(row.capacity),
  }));
}

async function getSchoolAllocations(year: number): Promise<SchoolAllocationRow[]> {
  const result = await query(
    `
      select
        institution_id,
        pedagogical_amount,
        nonpedagogical_amount,
        oniv_amount,
        other_amount,
        operations_amount,
        investment_amount,
        bucket_basis,
        bucket_certainty
      from raw.school_allocations
      where reporting_year = $1
      order by institution_id
    `,
    [year],
  );
  return result.rows.map((row) => ({
    institutionId: row.institution_id,
    pedagogicalAmount: Number(row.pedagogical_amount),
    nonpedagogicalAmount: Number(row.nonpedagogical_amount),
    onivAmount: Number(row.oniv_amount),
    otherAmount: Number(row.other_amount),
    operationsAmount: Number(row.operations_amount),
    investmentAmount: Number(row.investment_amount),
    bucketBasis: row.bucket_basis,
    bucketCertainty: row.bucket_certainty,
  }));
}

async function getSchoolEuProjects(year: number): Promise<SchoolEuProjectRow[]> {
  const result = await query(
    `
      select institution_id, programme, project_name, amount_czk, basis, certainty
      from raw.school_eu_projects
      where reporting_year = $1
      order by institution_id, programme, project_name
    `,
    [year],
  );
  return result.rows.map((row) => ({
    institutionId: row.institution_id,
    programme: row.programme,
    projectName: row.project_name,
    amountCzk: Number(row.amount_czk),
    basis: row.basis,
    certainty: row.certainty,
  }));
}

async function getSchoolFounderSupport(year: number): Promise<SchoolFounderSupportRow[]> {
  const result = await query(
    `
      select institution_id, amount_czk, basis, certainty, note
      from raw.school_founder_support
      where reporting_year = $1
      order by institution_id
    `,
    [year],
  );
  return result.rows.map((row) => ({
    institutionId: row.institution_id,
    amountCzk: Number(row.amount_czk),
    basis: row.basis,
    certainty: row.certainty,
    note: row.note,
  }));
}

async function loadSchoolYearRaw(year: number) {
  const [entities, allocations, euProjects, founderSupport] = await Promise.all([
    getSchoolEntities(year),
    getSchoolAllocations(year),
    getSchoolEuProjects(year),
    getSchoolFounderSupport(year),
  ]);
  return { entities, allocations, euProjects, founderSupport };
}

function ensureNode(nodesById: Map<string, SchoolNode>, node: SchoolNode) {
  if (!nodesById.has(node.id)) {
    nodesById.set(node.id, node);
  }
}

function slugify(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '');
}

function makeLink(
  source: string,
  target: string,
  amount: number,
  year: number,
  flowType: string,
  sourceDataset = 'api.aggregated',
) {
  return {
    source,
    target,
    value: amount,
    amountCzk: amount,
    year,
    flowType,
    basis: 'allocated',
    certainty: 'observed',
    sourceDataset,
  };
}

function bucketNode(id: string, count: number, label: string, capacity: number | null = null): SchoolNode {
  return {
    id,
    name: `${id === PREV_WINDOW_ID ? '↑' : '↓'} ${count} more ${label}`,
    category: 'other',
    level: 2,
    ...(capacity ? { metadata: { capacity } } : {}),
  };
}

function createInstitutionNode(entity: SchoolEntity | undefined): SchoolNode {
  if (!entity) {
    throw new Error('Missing school entity for node creation');
  }
  return {
    id: entity.institutionId,
    name: entity.institutionName,
    category: 'school_entity',
    level: 2,
    ...(entity.ico ? { ico: entity.ico } : {}),
    ...(entity.founderType ? { founderType: entity.founderType } : {}),
    ...(entity.capacity ? { metadata: { capacity: entity.capacity } } : {}),
  };
}

function createFounderNode(entity: SchoolEntity, capacity: number | null = null): SchoolNode {
  return {
    id: entity.founderId,
    name: entity.founderName,
    category: entity.founderType === 'kraj' ? 'region' : 'municipality',
    level: 1,
    ...(capacity ? { metadata: { capacity } } : {}),
  };
}

function createProgrammeNode(programme: string): SchoolNode {
  return {
    id: `eu:${slugify(programme)}`,
    name: programme,
    category: 'eu_programme',
    level: 0,
  };
}

function createBucketNode(bucketCode: string, level = 3, capacity: number | null = null): SchoolNode {
  const bucket = BUCKET_META[bucketCode] ?? {
    id: `bucket:${bucketCode}`,
    name: bucketCode,
  };
  return {
    id: bucket.id,
    name: bucket.name,
    category: 'cost_bucket',
    level,
    ...(capacity ? { metadata: { capacity } } : {}),
  };
}

function createSchoolDepartmentNode(capacity: number | null = null): SchoolNode {
  return {
    id: SCHOOL_ROOT_ID,
    name: 'Školství',
    category: 'ministry',
    level: 0,
    ...(capacity ? { metadata: { capacity } } : {}),
  };
}

function schoolBucketRegionNodeId(bucketCode: string, regionName: string) {
  return `${BUCKET_REGION_PREFIX}${bucketCode}|${regionName}`;
}

function parseSchoolBucketRegionNodeId(nodeId: string): { bucketCode: string; regionName: string } | null {
  if (!nodeId.startsWith(BUCKET_REGION_PREFIX)) return null;
  const rest = nodeId.slice(BUCKET_REGION_PREFIX.length);
  const separator = rest.indexOf('|');
  if (separator === -1) return null;
  const bucketCode = rest.slice(0, separator);
  const regionName = rest.slice(separator + 1);
  if (!bucketCode || !regionName) return null;
  return { bucketCode, regionName };
}

function allocationTotal(row: SchoolAllocationRow) {
  return (
    row.pedagogicalAmount +
    row.nonpedagogicalAmount +
    row.onivAmount +
    row.otherAmount
  );
}

function allocationBuckets(row: SchoolAllocationRow): Array<[string, number]> {
  return [
    ['pedagogical', row.pedagogicalAmount],
    ['nonpedagogical', row.nonpedagogicalAmount],
    ['oniv', row.onivAmount],
    ['other', row.otherAmount],
    ['operations', row.operationsAmount],
    ['investment', row.investmentAmount],
  ];
}

function buildWindow(sortedEntries: Array<[string, number]>, offset: number, windowSize: number) {
  const windowIds = new Set(sortedEntries.slice(offset, offset + windowSize).map(([id]) => id));
  const prevIds = new Set(sortedEntries.slice(0, offset).map(([id]) => id));
  const nextIds = new Set(sortedEntries.slice(offset + windowSize).map(([id]) => id));
  return {
    windowIds,
    prevIds,
    nextIds,
    prevCount: prevIds.size,
    nextCount: nextIds.size,
    bucket(id) {
      if (windowIds.has(id)) return id;
      if (prevIds.has(id)) return PREV_WINDOW_ID;
      return NEXT_WINDOW_ID;
    },
  };
}

function sumCapacity(entities: SchoolEntity[]): number {
  return entities.reduce((sum, entity) => sum + (entity.capacity ?? 0), 0);
}

function buildRegionCapacityMap(entities: SchoolEntity[]): Map<string, number> {
  const regionCapacity = new Map<string, number>();
  for (const entity of entities) {
    if (!entity.region) continue;
    regionCapacity.set(entity.region, (regionCapacity.get(entity.region) ?? 0) + (entity.capacity ?? 0));
  }
  return regionCapacity;
}

async function getSchoolDepartmentRootGraph(year: number) {
  const { entities, allocations } = await loadSchoolYearRaw(year);
  if (!entities.length || !allocations.length) return null;

  const totalCapacity = sumCapacity(entities);
  const bucketTotals = new Map<string, number>();
  for (const row of allocations) {
    for (const [bucketCode, amount] of allocationBuckets(row)) {
      if (amount <= 0) continue;
      bucketTotals.set(bucketCode, (bucketTotals.get(bucketCode) ?? 0) + amount);
    }
  }

  const nodesById = new Map<string, SchoolNode>();
  const links: SchoolLink[] = [];
  ensureNode(nodesById, createSchoolDepartmentNode(totalCapacity || null));

  for (const [bucketCode, amount] of [...bucketTotals.entries()].sort((a, b) => b[1] - a[1])) {
    ensureNode(nodesById, createBucketNode(bucketCode, 1, totalCapacity || null));
    links.push(makeLink(SCHOOL_ROOT_ID, `bucket:${bucketCode}`, amount, year, 'school_expenditure'));
  }

  return { year, nodes: [...nodesById.values()], links };
}

async function getSchoolBucketRegionGraph(year: number, bucketCode: string) {
  const { entities, allocations } = await loadSchoolYearRaw(year);
  if (!entities.length || !allocations.length) return null;

  const regionCapacity = buildRegionCapacityMap(entities);
  const totalCapacity = sumCapacity(entities);
  const bucketTotalsByRegion = new Map<string, number>();
  let bucketTotal = 0;

  const schoolById = new Map(entities.map((entity) => [entity.institutionId, entity]));
  for (const row of allocations) {
    const entity = schoolById.get(row.institutionId);
    const regionName = entity?.region;
    if (!regionName) continue;
    const amount = Object.fromEntries(allocationBuckets(row))[bucketCode] ?? 0;
    if (amount <= 0) continue;
    bucketTotal += amount;
    bucketTotalsByRegion.set(regionName, (bucketTotalsByRegion.get(regionName) ?? 0) + amount);
  }

  if (bucketTotal <= 0) return null;

  const nodesById = new Map<string, SchoolNode>();
  const links: SchoolLink[] = [];
  ensureNode(nodesById, createSchoolDepartmentNode(totalCapacity || null));
  ensureNode(nodesById, createBucketNode(bucketCode, 1, totalCapacity || null));
  links.push(makeLink(SCHOOL_ROOT_ID, `bucket:${bucketCode}`, bucketTotal, year, 'school_expenditure'));

  for (const [regionName, amount] of [...bucketTotalsByRegion.entries()].sort((a, b) => b[1] - a[1])) {
    const regionNodeId = schoolBucketRegionNodeId(bucketCode, regionName);
    ensureNode(nodesById, {
      id: regionNodeId,
      name: regionName,
      category: 'region',
      level: 2,
      ...(regionCapacity.get(regionName) ? { metadata: { capacity: regionCapacity.get(regionName) } } : {}),
    });
    links.push(makeLink(`bucket:${bucketCode}`, regionNodeId, amount, year, 'school_expenditure'));
  }

  return { year, bucketCode, nodes: [...nodesById.values()], links };
}

async function getSchoolBucketRegionSchoolGraph(year: number, bucketCode: string, regionName: string, offset = 0) {
  const { entities, allocations } = await loadSchoolYearRaw(year);
  const schools = entities.filter((entity) => entity.region === regionName);
  if (!schools.length) return null;

  const schoolById = new Map(schools.map((entity) => [entity.institutionId, entity]));
  const schoolTotals = new Map<string, number>();

  for (const row of allocations) {
    if (!schoolById.has(row.institutionId)) continue;
    const amount = Object.fromEntries(allocationBuckets(row))[bucketCode] ?? 0;
    if (amount <= 0) continue;
    schoolTotals.set(row.institutionId, (schoolTotals.get(row.institutionId) ?? 0) + amount);
  }

  const sorted = [...schoolTotals.entries()].sort((a, b) => b[1] - a[1]);
  if (!sorted.length) return null;

  const window = buildWindow(sorted, offset, TOP_SCHOOLS);
  const regionTotal = sorted.reduce((sum, [, amount]) => sum + amount, 0);
  const regionCapacity = schools.reduce((sum, school) => sum + (school.capacity ?? 0), 0);
  const regionNodeId = schoolBucketRegionNodeId(bucketCode, regionName);

  const nodesById = new Map<string, SchoolNode>();
  const links: SchoolLink[] = [];
  ensureNode(nodesById, createSchoolDepartmentNode(regionCapacity || null));
  ensureNode(nodesById, createBucketNode(bucketCode, 1, regionCapacity || null));
  ensureNode(nodesById, {
    id: regionNodeId,
    name: regionName,
    category: 'region',
    level: 2,
    ...(regionCapacity ? { metadata: { capacity: regionCapacity } } : {}),
  });

  links.push(makeLink(SCHOOL_ROOT_ID, `bucket:${bucketCode}`, regionTotal, year, 'school_expenditure'));
  links.push(makeLink(`bucket:${bucketCode}`, regionNodeId, regionTotal, year, 'school_expenditure'));

  for (const [schoolId, amount] of schoolTotals) {
    const target = window.bucket(schoolId);
    if (target === PREV_WINDOW_ID && !window.prevCount) continue;
    if (target === NEXT_WINDOW_ID && !window.nextCount) continue;
    if (target === schoolId) {
      const schoolNode = createInstitutionNode(schoolById.get(schoolId));
      ensureNode(nodesById, { ...schoolNode, level: 3 });
    }
    links.push(makeLink(regionNodeId, target, amount, year, 'school_expenditure'));
  }

  const prevCapacity = [...window.prevIds].reduce((sum, id) => sum + (schoolById.get(id)?.capacity ?? 0), 0);
  const nextCapacity = [...window.nextIds].reduce((sum, id) => sum + (schoolById.get(id)?.capacity ?? 0), 0);
  if (window.prevCount) ensureNode(nodesById, bucketNode(PREV_WINDOW_ID, window.prevCount, 'schools', prevCapacity));
  if (window.nextCount) ensureNode(nodesById, bucketNode(NEXT_WINDOW_ID, window.nextCount, 'schools', nextCapacity));

  return { year, bucketCode, regionName, nodes: [...nodesById.values()], links };
}

export async function getSchoolOverviewGraph(year: number) {
  const period = await getSchoolPeriod(year);
  if (!period) return null;

  const periodId = period.reporting_period_id;

  const [
    regionCapacityRes,
    directRes,
    bucketRes,
    founderRes,
    euRes,
    stateRes,
  ] = await Promise.all([
    query(
      `
        select o.region_name, sum(sc.capacity) as total_capacity
        from core.school_capacity sc
        join core.organization o on o.organization_id = sc.school_organization_id
        where sc.reporting_period_id = $1
        group by o.region_name
      `,
      [periodId],
    ),
    query(
      `
        select o.region_name, sum(ff.amount_czk) as total_amount
        from core.financial_flow ff
        join core.organization o on o.organization_id = ff.target_organization_id
        where ff.reporting_period_id = $1
          and ff.flow_type = 'direct_school_finance'
        group by o.region_name
      `,
      [periodId],
    ),
    query(
      `
        select o.region_name, ff.cost_bucket_code, sum(ff.amount_czk) as total_amount
        from core.financial_flow ff
        join core.organization o on o.organization_id = ff.source_organization_id
        where ff.reporting_period_id = $1
          and ff.flow_type = 'school_expenditure'
        group by o.region_name, ff.cost_bucket_code
      `,
      [periodId],
    ),
    query(
      `
        select
          o.region_name,
          coalesce(o.attributes ->> 'founder_type', 'obec') as founder_type,
          sum(ff.amount_czk) as total_amount
        from core.financial_flow ff
        join core.organization o on o.organization_id = ff.target_organization_id
        where ff.reporting_period_id = $1
          and ff.flow_type = 'founder_support'
        group by o.region_name, coalesce(o.attributes ->> 'founder_type', 'obec')
      `,
      [periodId],
    ),
    query(
      `
        select o.region_name, sum(ff.amount_czk) as total_amount
        from core.financial_flow ff
        join core.organization o on o.organization_id = ff.target_organization_id
        where ff.reporting_period_id = $1
          and ff.flow_type = 'eu_project_support'
        group by o.region_name
      `,
      [periodId],
    ),
    query(
      `
        select
          ff.flow_type,
          ff.amount_czk,
          source_o.name as source_name,
          target_o.name as target_name,
          source_o.attributes ->> 'node_id' as source_node_id,
          target_o.attributes ->> 'node_id' as target_node_id
        from core.financial_flow ff
        left join core.organization source_o on source_o.organization_id = ff.source_organization_id
        left join core.organization target_o on target_o.organization_id = ff.target_organization_id
        where ff.reporting_period_id = $1
          and ff.flow_type in ('state_revenue', 'state_to_ministry', 'state_to_other')
      `,
      [periodId],
    ),
  ]);

  const nodesById = new Map<string, SchoolNode>();
  const links: SchoolLink[] = [];

  const regionCapacity = new Map<string, number>(
    regionCapacityRes.rows.map((row) => [
      row.region_name,
      Number(row.total_capacity),
    ]),
  );

  const totalCapacity = [...regionCapacity.values()].reduce((sum, value) => sum + value, 0);
  const founderTypeCapacity: Record<'kraj' | 'obec', number> = { kraj: 0, obec: 0 };

  ensureNode(nodesById, {
    id: STATE_ID,
    name: 'State budget',
    category: 'state',
    level: 0,
    metadata: totalCapacity > 0 ? { capacity: totalCapacity } : undefined,
  });
  ensureNode(nodesById, {
    id: MSMT_ID,
    name: 'MŠMT direct school finance',
    category: 'ministry',
    level: 1,
    metadata: totalCapacity > 0 ? { capacity: totalCapacity } : undefined,
  });

  for (const row of directRes.rows) {
    const regionName = row.region_name || 'Unknown';
    const regionId = `region:${regionName}`;
    const capacity = regionCapacity.get(regionName);
    ensureNode(nodesById, {
      id: regionId,
      name: regionName,
      category: 'region',
      level: 2,
      metadata: capacity ? { capacity } : undefined,
    });
    links.push(makeLink(MSMT_ID, regionId, Number(row.total_amount), year, 'direct_school_finance'));
  }

  for (const row of bucketRes.rows) {
    const regionName = row.region_name || 'Unknown';
    const regionId = `region:${regionName}`;
    const bucket = BUCKET_META[row.cost_bucket_code] ?? {
      id: `bucket:${row.cost_bucket_code}`,
      name: row.cost_bucket_code,
    };
    ensureNode(nodesById, {
      id: regionId,
      name: regionName,
      category: 'region',
      level: 2,
      metadata: regionCapacity.get(regionName)
        ? { capacity: regionCapacity.get(regionName) }
        : undefined,
    });
    ensureNode(nodesById, {
      id: bucket.id,
      name: bucket.name,
      category: 'cost_bucket',
      level: 3,
    });
    links.push(makeLink(regionId, bucket.id, Number(row.total_amount), year, 'school_expenditure'));
  }

  for (const row of founderRes.rows) {
    const regionName = row.region_name || 'Unknown';
    const regionId = `region:${regionName}`;
    const founderType = row.founder_type === 'kraj' ? 'kraj' : 'obec';
    const founderId = founderType === 'kraj' ? FOUNDERS_KRAJ : FOUNDERS_OBEC;
    const founderName =
      founderType === 'kraj'
        ? 'Příspěvky krajů (provoz)'
        : 'Příspěvky obcí (provoz)';
    const category = founderType === 'kraj' ? 'region' : 'municipality';
    const capacity = regionCapacity.get(regionName) ?? 0;
    founderTypeCapacity[founderType] += capacity;
    ensureNode(nodesById, {
      id: founderId,
      name: founderName,
      category,
      level: 0,
    });
    ensureNode(nodesById, {
      id: regionId,
      name: regionName,
      category: 'region',
      level: 2,
      metadata: capacity ? { capacity } : undefined,
    });
    links.push(makeLink(founderId, regionId, Number(row.total_amount), year, 'founder_support'));
  }

  if (nodesById.has(FOUNDERS_KRAJ) && founderTypeCapacity.kraj > 0) {
    nodesById.set(FOUNDERS_KRAJ, {
      ...nodesById.get(FOUNDERS_KRAJ),
      metadata: { capacity: founderTypeCapacity.kraj },
    });
  }
  if (nodesById.has(FOUNDERS_OBEC) && founderTypeCapacity.obec > 0) {
    nodesById.set(FOUNDERS_OBEC, {
      ...nodesById.get(FOUNDERS_OBEC),
      metadata: { capacity: founderTypeCapacity.obec },
    });
  }

  const founderTotals = {
    kraj: links
      .filter((link) => link.source === FOUNDERS_KRAJ)
      .reduce((sum, link) => sum + link.amountCzk, 0),
    obec: links
      .filter((link) => link.source === FOUNDERS_OBEC)
      .reduce((sum, link) => sum + link.amountCzk, 0),
  };

  if (founderTotals.kraj > 0) {
    links.push(makeLink(STATE_ID, FOUNDERS_KRAJ, founderTotals.kraj, year, 'state_to_founders'));
  }
  if (founderTotals.obec > 0) {
    links.push(makeLink(STATE_ID, FOUNDERS_OBEC, founderTotals.obec, year, 'state_to_founders'));
  }

  let founderTransfer = founderTotals.kraj + founderTotals.obec;

  if (euRes.rows.length > 0) {
    ensureNode(nodesById, {
      id: EU_ALL_ID,
      name: 'EU structural funds',
      category: 'eu_programme',
      level: 0,
    });
    for (const row of euRes.rows) {
      const regionName = row.region_name || 'Unknown';
      const regionId = `region:${regionName}`;
      ensureNode(nodesById, {
        id: regionId,
        name: regionName,
        category: 'region',
        level: 2,
        metadata: regionCapacity.get(regionName)
          ? { capacity: regionCapacity.get(regionName) }
          : undefined,
      });
      links.push(makeLink(EU_ALL_ID, regionId, Number(row.total_amount), year, 'eu_project_support'));
    }
  }

  for (const row of stateRes.rows) {
    const sourceId = row.source_node_id || `other:${row.source_name}`;
    const targetId = row.target_node_id || `other:${row.target_name}`;

    if (row.source_node_id) {
      ensureNode(nodesById, {
        id: sourceId,
        name: row.source_name,
        category: sourceId.startsWith('income:') ? 'other' : 'state',
        level: 0,
      });
    }

    if (row.target_node_id) {
      ensureNode(nodesById, {
        id: targetId,
        name: row.target_name,
        category:
          targetId === MSMT_ID
            ? 'ministry'
            : targetId.startsWith('income:')
              ? 'other'
              : 'other',
        level: targetId === MSMT_ID ? 1 : 0,
      });
    }

    let amount = Number(row.amount_czk);
    if (row.flow_type === 'state_to_other' && founderTransfer > 0) {
      amount = Math.max(0, amount - founderTransfer);
    }
    links.push(makeLink(sourceId, targetId, amount, year, row.flow_type, 'core.financial_flow'));
  }

  return {
    year,
    nodes: [...nodesById.values()],
    links,
  };
}

export async function getSchoolEuGraph(year: number) {
  const { entities, euProjects } = await loadSchoolYearRaw(year);
  const schoolById = new Map<string, SchoolEntity>(entities.map((entity) => [entity.institutionId, entity]));
  const programmeRegionTotals = new Map<string, number>();
  const regionCapacity = new Map<string, number>();

  for (const entity of entities) {
    if (!entity.region) continue;
    regionCapacity.set(entity.region, (regionCapacity.get(entity.region) ?? 0) + (entity.capacity ?? 0));
  }

  for (const row of euProjects) {
    const school = schoolById.get(row.institutionId);
    const region = school?.region;
    if (!region) continue;
    const key = `${row.programme}|${region}`;
    programmeRegionTotals.set(key, (programmeRegionTotals.get(key) ?? 0) + row.amountCzk);
  }

  const nodesById = new Map<string, SchoolNode>();
  const links: SchoolLink[] = [];

  for (const [key, amount] of programmeRegionTotals) {
    const [programme, region] = key.split('|');
    const programmeNode = createProgrammeNode(programme);
    const regionId = `region:${region}`;
    ensureNode(nodesById, programmeNode);
    ensureNode(nodesById, {
      id: regionId,
      name: region,
      category: 'region',
      level: 1,
      ...(regionCapacity.get(region) ? { metadata: { capacity: regionCapacity.get(region) } } : {}),
    });
    links.push(makeLink(programmeNode.id, regionId, amount, year, 'eu_project_support'));
  }

  return { year, nodes: [...nodesById.values()], links };
}

export async function getSchoolFounderTypeGraph(year: number, founderType: 'kraj' | 'obec', offset = 0) {
  const { entities, founderSupport } = await loadSchoolYearRaw(year);
  const entitiesOfType = entities.filter((entity) => entity.founderType === founderType);
  if (entitiesOfType.length === 0) return null;

  const founderMeta = new Map<string, SchoolEntity>();
  const founderCapacity = new Map<string, number>();
  const founderTotals = new Map<string, number>();

  for (const entity of entitiesOfType) {
    founderMeta.set(entity.founderId, entity);
    founderCapacity.set(
      entity.founderId,
      (founderCapacity.get(entity.founderId) ?? 0) + (entity.capacity ?? 0),
    );
  }

  const entityById = new Map<string, SchoolEntity>(entitiesOfType.map((entity) => [entity.institutionId, entity]));
  for (const row of founderSupport) {
    const entity = entityById.get(row.institutionId);
    if (!entity?.founderId) continue;
    founderTotals.set(
      entity.founderId,
      (founderTotals.get(entity.founderId) ?? 0) + row.amountCzk,
    );
  }

  const sorted = [...founderTotals.entries()].sort((a, b) => b[1] - a[1]);
  const window = buildWindow(sorted, offset, TOP_FOUNDERS);
  const aggNodeId = founderType === 'kraj' ? FOUNDERS_KRAJ : FOUNDERS_OBEC;
  const aggNodeName =
    founderType === 'kraj' ? 'Příspěvky krajů (provoz)' : 'Příspěvky obcí (provoz)';

  const nodesById = new Map<string, SchoolNode>();
  ensureNode(nodesById, {
    id: aggNodeId,
    name: aggNodeName,
    category: founderType === 'kraj' ? 'region' : 'municipality',
    level: 0,
  });

  const links: SchoolLink[] = [];
  for (const [founderId, amount] of founderTotals) {
    const target = window.bucket(founderId);
    if (target === PREV_WINDOW_ID && !window.prevCount) continue;
    if (target === NEXT_WINDOW_ID && !window.nextCount) continue;
    links.push(makeLink(aggNodeId, target, amount, year, 'founder_support'));
    if (target === founderId) {
      const entity = founderMeta.get(founderId);
      ensureNode(nodesById, createFounderNode(entity, founderCapacity.get(founderId) ?? 0));
    }
  }

  const prevCapacity = [...window.prevIds].reduce((sum, id) => sum + (founderCapacity.get(id) ?? 0), 0);
  const nextCapacity = [...window.nextIds].reduce((sum, id) => sum + (founderCapacity.get(id) ?? 0), 0);
  if (window.prevCount) ensureNode(nodesById, bucketNode(PREV_WINDOW_ID, window.prevCount, 'founders', prevCapacity));
  if (window.nextCount) ensureNode(nodesById, bucketNode(NEXT_WINDOW_ID, window.nextCount, 'founders', nextCapacity));

  return { year, nodes: [...nodesById.values()], links };
}

export async function getSchoolRegionGraph(year: number, region: string, offset = 0) {
  const { entities, allocations, euProjects, founderSupport } = await loadSchoolYearRaw(year);
  const schools = entities.filter((entity) => entity.region === region);
  if (schools.length === 0) return null;

  const schoolIds = new Set(schools.map((entity) => entity.institutionId));
  const schoolById = new Map<string, SchoolEntity>(schools.map((entity) => [entity.institutionId, entity]));

  const founderCapacity = new Map<string, number>();
  const founderMeta = new Map<string, SchoolEntity>();
  const msmtToFounder = new Map<string, number>();
  const founderToBucket = new Map<string, number>();
  const founderSupportToFounder = new Map<string, number>();
  const euProgrammeToFounder = new Map<string, number>();
  const totalToFounder = new Map<string, number>();

  for (const school of schools) {
    founderMeta.set(school.founderId, school);
    founderCapacity.set(
      school.founderId,
      (founderCapacity.get(school.founderId) ?? 0) + (school.capacity ?? 0),
    );
  }

  for (const row of allocations) {
    if (!schoolIds.has(row.institutionId)) continue;
    const school = schoolById.get(row.institutionId);
    const founderId = school?.founderId;
    if (!founderId) continue;

    const total = allocationTotal(row);
    msmtToFounder.set(founderId, (msmtToFounder.get(founderId) ?? 0) + total);
    totalToFounder.set(founderId, (totalToFounder.get(founderId) ?? 0) + total);

    for (const [bucketCode, amount] of allocationBuckets(row)) {
      if (amount <= 0) continue;
      const key = `${founderId}|${bucketCode}`;
      founderToBucket.set(key, (founderToBucket.get(key) ?? 0) + amount);
    }
  }

  for (const row of founderSupport) {
    if (!schoolIds.has(row.institutionId)) continue;
    const school = schoolById.get(row.institutionId);
    const founderId = school?.founderId;
    if (!founderId) continue;
    founderSupportToFounder.set(
      founderId,
      (founderSupportToFounder.get(founderId) ?? 0) + row.amountCzk,
    );
    totalToFounder.set(founderId, (totalToFounder.get(founderId) ?? 0) + row.amountCzk);
  }

  for (const row of euProjects) {
    if (!schoolIds.has(row.institutionId)) continue;
    const school = schoolById.get(row.institutionId);
    const founderId = school?.founderId;
    if (!founderId) continue;
    const key = `${row.programme}|${founderId}`;
    euProgrammeToFounder.set(key, (euProgrammeToFounder.get(key) ?? 0) + row.amountCzk);
    if (!totalToFounder.has(founderId)) totalToFounder.set(founderId, 0);
  }

  const sorted = [...totalToFounder.entries()].sort((a, b) => b[1] - a[1]);
  const window = buildWindow(sorted, offset, TOP_FOUNDERS);
  const nodesById = new Map<string, SchoolNode>();
  const links: SchoolLink[] = [];
  let hasFounderKraj = false;
  let hasFounderObec = false;

  ensureNode(nodesById, {
    id: MSMT_ID,
    name: 'MŠMT direct school finance',
    category: 'ministry',
    level: 0,
  });

  for (const [founderId, amount] of msmtToFounder) {
    const target = window.bucket(founderId);
    if (target === PREV_WINDOW_ID && !window.prevCount) continue;
    if (target === NEXT_WINDOW_ID && !window.nextCount) continue;
    links.push(makeLink(MSMT_ID, target, amount, year, 'direct_school_finance'));
    if (target === founderId) {
      ensureNode(nodesById, createFounderNode(founderMeta.get(founderId), founderCapacity.get(founderId) ?? 0));
    }
  }

  for (const [key, amount] of founderToBucket) {
    const [founderId, bucketCode] = key.split('|');
    const source = window.bucket(founderId);
    if (source === PREV_WINDOW_ID && !window.prevCount) continue;
    if (source === NEXT_WINDOW_ID && !window.nextCount) continue;
    ensureNode(nodesById, createBucketNode(bucketCode));
    if (source === founderId) {
      ensureNode(nodesById, createFounderNode(founderMeta.get(founderId), founderCapacity.get(founderId) ?? 0));
    }
    links.push(makeLink(source, createBucketNode(bucketCode).id, amount, year, 'school_expenditure'));
  }

  for (const [key, amount] of euProgrammeToFounder) {
    const [programme, founderId] = key.split('|');
    const target = window.bucket(founderId);
    if (target === PREV_WINDOW_ID && !window.prevCount) continue;
    if (target === NEXT_WINDOW_ID && !window.nextCount) continue;
    const programmeNode = createProgrammeNode(programme);
    ensureNode(nodesById, programmeNode);
    if (target === founderId) {
      ensureNode(nodesById, createFounderNode(founderMeta.get(founderId), founderCapacity.get(founderId) ?? 0));
    }
    links.push(makeLink(programmeNode.id, target, amount, year, 'eu_project_support'));
  }

  for (const [founderId, amount] of founderSupportToFounder) {
    const target = window.bucket(founderId);
    if (target === PREV_WINDOW_ID && !window.prevCount) continue;
    if (target === NEXT_WINDOW_ID && !window.nextCount) continue;
    const entity = founderMeta.get(founderId);
    const sourceId = entity?.founderType === 'kraj' ? FOUNDERS_KRAJ : FOUNDERS_OBEC;
    if (sourceId === FOUNDERS_KRAJ) hasFounderKraj = true;
    if (sourceId === FOUNDERS_OBEC) hasFounderObec = true;
    ensureNode(nodesById, {
      id: sourceId,
      name: sourceId === FOUNDERS_KRAJ ? 'Příspěvky krajů (provoz)' : 'Příspěvky obcí (provoz)',
      category: sourceId === FOUNDERS_KRAJ ? 'region' : 'municipality',
      level: 0,
    });
    if (target === founderId) {
      ensureNode(nodesById, createFounderNode(entity, founderCapacity.get(founderId) ?? 0));
    }
    links.push(makeLink(sourceId, target, amount, year, 'founder_support'));
  }

  const prevCapacity = [...window.prevIds].reduce((sum, id) => sum + (founderCapacity.get(id) ?? 0), 0);
  const nextCapacity = [...window.nextIds].reduce((sum, id) => sum + (founderCapacity.get(id) ?? 0), 0);
  if (window.prevCount) ensureNode(nodesById, bucketNode(PREV_WINDOW_ID, window.prevCount, 'founders', prevCapacity));
  if (window.nextCount) ensureNode(nodesById, bucketNode(NEXT_WINDOW_ID, window.nextCount, 'founders', nextCapacity));

  return { year, region, nodes: [...nodesById.values()], links, hasFounderKraj, hasFounderObec };
}

export async function getSchoolFounderGraph(year: number, founderId: string, offset = 0) {
  const { entities, allocations, euProjects, founderSupport } = await loadSchoolYearRaw(year);
  const schools = entities.filter((entity) => entity.founderId === founderId);
  if (schools.length === 0) return null;

  const schoolIds = new Set(schools.map((entity) => entity.institutionId));
  const schoolById = new Map<string, SchoolEntity>(schools.map((entity) => [entity.institutionId, entity]));
  const founderMeta = schools[0];

  const schoolInflow = new Map<string, number>();
  for (const row of allocations) {
    if (!schoolIds.has(row.institutionId)) continue;
    schoolInflow.set(row.institutionId, (schoolInflow.get(row.institutionId) ?? 0) + allocationTotal(row));
  }
  for (const row of founderSupport) {
    if (!schoolIds.has(row.institutionId)) continue;
    schoolInflow.set(row.institutionId, (schoolInflow.get(row.institutionId) ?? 0) + row.amountCzk);
  }

  const sorted = [...schoolInflow.entries()].sort((a, b) => b[1] - a[1]);
  const window = buildWindow(sorted, offset, TOP_SCHOOLS);
  const nodesById = new Map<string, SchoolNode>();
  const links: SchoolLink[] = [];

  ensureNode(nodesById, {
    id: MSMT_ID,
    name: 'MŠMT direct school finance',
    category: 'ministry',
    level: 0,
  });
  ensureNode(nodesById, createFounderNode(founderMeta));

  for (const row of allocations) {
    if (!schoolIds.has(row.institutionId)) continue;
    const target = window.bucket(row.institutionId);
    if (target === PREV_WINDOW_ID && !window.prevCount) continue;
    if (target === NEXT_WINDOW_ID && !window.nextCount) continue;
    if (target === row.institutionId) {
      ensureNode(nodesById, createInstitutionNode(schoolById.get(row.institutionId)));
    }
    links.push(makeLink(MSMT_ID, target, allocationTotal(row), year, 'direct_school_finance'));
    for (const [bucketCode, amount] of allocationBuckets(row)) {
      if (amount <= 0) continue;
      ensureNode(nodesById, createBucketNode(bucketCode));
      links.push(makeLink(target, createBucketNode(bucketCode).id, amount, year, 'school_expenditure'));
    }
  }

  for (const row of euProjects) {
    if (!schoolIds.has(row.institutionId)) continue;
    const target = window.bucket(row.institutionId);
    if (target === PREV_WINDOW_ID && !window.prevCount) continue;
    if (target === NEXT_WINDOW_ID && !window.nextCount) continue;
    if (target === row.institutionId) {
      ensureNode(nodesById, createInstitutionNode(schoolById.get(row.institutionId)));
    }
    const programmeNode = createProgrammeNode(row.programme);
    ensureNode(nodesById, programmeNode);
    links.push(makeLink(programmeNode.id, target, row.amountCzk, year, 'eu_project_support'));
  }

  for (const row of founderSupport) {
    if (!schoolIds.has(row.institutionId)) continue;
    const target = window.bucket(row.institutionId);
    if (target === PREV_WINDOW_ID && !window.prevCount) continue;
    if (target === NEXT_WINDOW_ID && !window.nextCount) continue;
    if (target === row.institutionId) {
      ensureNode(nodesById, createInstitutionNode(schoolById.get(row.institutionId)));
    }
    links.push(makeLink(founderId, target, row.amountCzk, year, 'founder_support'));
  }

  const prevCapacity = [...window.prevIds].reduce((sum, id) => sum + (schoolById.get(id)?.capacity ?? 0), 0);
  const nextCapacity = [...window.nextIds].reduce((sum, id) => sum + (schoolById.get(id)?.capacity ?? 0), 0);
  if (window.prevCount) ensureNode(nodesById, bucketNode(PREV_WINDOW_ID, window.prevCount, 'schools', prevCapacity));
  if (window.nextCount) ensureNode(nodesById, bucketNode(NEXT_WINDOW_ID, window.nextCount, 'schools', nextCapacity));

  return { year, founderId, nodes: [...nodesById.values()], links };
}

export async function getSchoolNodeGraph(year: number, nodeId: string, offset = 0) {
  if (nodeId === SCHOOL_ROOT_ID || nodeId === MSMT_ID) {
    return getSchoolDepartmentRootGraph(year);
  }
  const bucketRegionNode = parseSchoolBucketRegionNodeId(nodeId);
  if (bucketRegionNode) {
    return getSchoolBucketRegionSchoolGraph(year, bucketRegionNode.bucketCode, bucketRegionNode.regionName, offset);
  }
  if (nodeId.startsWith('bucket:')) {
    return getSchoolBucketRegionGraph(year, nodeId.replace('bucket:', ''));
  }
  if (nodeId.startsWith('region:')) {
    return getSchoolRegionGraph(year, nodeId.replace('region:', ''), offset);
  }
  if (nodeId === EU_ALL_ID) {
    return getSchoolEuGraph(year);
  }
  if (nodeId === FOUNDERS_KRAJ) {
    return getSchoolFounderTypeGraph(year, 'kraj', offset);
  }
  if (nodeId === FOUNDERS_OBEC) {
    return getSchoolFounderTypeGraph(year, 'obec', offset);
  }
  if (nodeId.startsWith('founder:')) {
    return getSchoolFounderGraph(year, nodeId, offset);
  }

  const { entities, allocations, euProjects, founderSupport } = await loadSchoolYearRaw(year);

  if (nodeId.startsWith('school:')) {
    const entity = entities.find((row) => row.institutionId === nodeId);
    if (!entity) return null;

    const nodesById = new Map<string, SchoolNode>();
    const links: SchoolLink[] = [];
    ensureNode(nodesById, createInstitutionNode(entity));
    ensureNode(nodesById, {
      id: MSMT_ID,
      name: 'MŠMT direct school finance',
      category: 'ministry',
      level: 0,
    });
    ensureNode(nodesById, createFounderNode(entity));

    for (const row of allocations.filter((row) => row.institutionId === nodeId)) {
      links.push(makeLink(MSMT_ID, nodeId, allocationTotal(row), year, 'direct_school_finance'));
      for (const [bucketCode, amount] of allocationBuckets(row)) {
        if (amount <= 0) continue;
        ensureNode(nodesById, createBucketNode(bucketCode));
        links.push(makeLink(nodeId, createBucketNode(bucketCode).id, amount, year, 'school_expenditure'));
      }
    }
    for (const row of euProjects.filter((row) => row.institutionId === nodeId)) {
      const programmeNode = createProgrammeNode(row.programme);
      ensureNode(nodesById, programmeNode);
      links.push(makeLink(programmeNode.id, nodeId, row.amountCzk, year, 'eu_project_support'));
    }
    for (const row of founderSupport.filter((row) => row.institutionId === nodeId)) {
      links.push(makeLink(entity.founderId, nodeId, row.amountCzk, year, 'founder_support'));
    }

    return { year, nodeId, nodes: [...nodesById.values()], links };
  }

  if (nodeId.startsWith('bucket:')) {
    const bucketCode = nodeId.replace('bucket:', '');
    const nodesById = new Map<string, SchoolNode>();
    const links: SchoolLink[] = [];
    ensureNode(nodesById, createBucketNode(bucketCode));
    ensureNode(nodesById, {
      id: MSMT_ID,
      name: 'MŠMT direct school finance',
      category: 'ministry',
      level: 0,
    });

    const entityById = new Map<string, SchoolEntity>(entities.map((entity) => [entity.institutionId, entity]));
    for (const row of allocations) {
      const amount = Object.fromEntries(allocationBuckets(row))[bucketCode] ?? 0;
      if (amount <= 0) continue;
      const entity = entityById.get(row.institutionId);
      if (!entity) continue;
      ensureNode(nodesById, createInstitutionNode(entity));
      links.push(makeLink(row.institutionId, nodeId, amount, year, 'school_expenditure'));
      links.push(makeLink(MSMT_ID, row.institutionId, allocationTotal(row), year, 'direct_school_finance'));
    }
    return { year, nodeId, nodes: [...nodesById.values()], links };
  }

  return null;
}
