import { query } from './db.js';
import { getAvailableYears, getSchoolOverviewGraph, searchInstitutions } from './school.js';

const STATE_ID = 'state:cr';
const HEALTH_MINISTRY_ID = 'health:ministry:mzcr';
const HEALTH_INSURANCE_ID = 'health:system:public-insurance';
const HEALTH_PUBLIC_HEALTH_ID = 'health:public-health';
const HEALTH_ADMIN_ID = 'health:admin:residual';
const HEALTH_COSTS_ID = 'health:costs';
const SOCIAL_MINISTRY_ID = 'social:ministry:mpsv';
const MV_MINISTRY_ID = 'security:ministry:mv';
const MV_POLICE_ID = 'security:police';
const MV_FIRE_RESCUE_ID = 'security:fire-rescue';
const MV_ADMIN_ID = 'security:mv-admin';
const MV_SOCIAL_ID = 'security:mv-social';
const PREV_WINDOW_ID = 'synthetic:prev-window';
const NEXT_WINDOW_ID = 'synthetic:next-window';

const ADMIN_ENTITY_NODES = {
  uzis: { id: 'health:admin:uzis', name: 'UZIS' },
  sukl: { id: 'health:admin:sukl', name: 'SUKL' },
  kst: { id: 'health:admin:kst', name: 'Koordinacni stredisko transplantaci' },
} as const;

const OUTPATIENT_SUBTYPE_NODES = {
  HP31: { id: 'health:outpatient:hp31', name: 'Samostatne ordinace lekaru' },
  HP32: { id: 'health:outpatient:hp32', name: 'Samostatne ordinace zubnich lekaru' },
  HP34: { id: 'health:outpatient:hp34', name: 'Ambulantni centra' },
  HP33: { id: 'health:outpatient:hp33', name: 'Ostatni poskytovatele ambulantni pece' },
  HP35: { id: 'health:outpatient:hp35', name: 'Poskytovatele sluzeb domaci pece' },
} as const;

const OWNER_BRANCH = {
  region: 'region',
  municipality: 'municipality',
  centralState: 'central_state',
  unverified: 'unverified',
} as const;

const OWNER_NODES = {
  [OWNER_BRANCH.region]: {
    id: 'health:owner:region',
    name: 'Krajske nemocnice',
    category: 'region',
  },
  [OWNER_BRANCH.municipality]: {
    id: 'health:owner:municipality',
    name: 'Obecni a mestske nemocnice',
    category: 'municipality',
  },
  [OWNER_BRANCH.centralState]: {
    id: 'health:owner:central_state',
    name: 'Statni a fakultni nemocnice',
    category: 'state',
  },
  [OWNER_BRANCH.unverified]: {
    id: 'health:owner:unverified',
    name: 'Nemocnice bez overeneho zrizovatele',
    category: 'other',
  },
} as const;

type OwnerBranch = keyof typeof OWNER_NODES;
type HealthFocus = 'hospital' | 'public_health' | 'other';

interface AtlasNode {
  id: string;
  name: string;
  category: string;
  level: number;
  ico?: string;
  founderType?: string;
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

interface HealthFinanceRow {
  providerIco: string;
  providerName: string;
  providerType: string | null;
  regionName: string | null;
  founderType: string | null;
  focus: HealthFocus;
  ownerBranch: OwnerBranch | null;
  costs: number;
  patientCount: number;
  contactCount: number;
  totalQuantity: number;
  dataKind: 'provider_finance' | 'budget_entity';
  entityKind: string | null;
  sourceDataset: string;
}

interface HealthMzAggregate {
  year: number;
  amount: number;
  entityName: string;
  sourceDataset: string;
}

interface HealthMzAdminEntity {
  ico: string;
  name: string;
  kind: keyof typeof ADMIN_ENTITY_NODES;
  amount: number;
  regionName: string | null;
  sourceDataset: string;
}

interface HealthInsuranceAggregate {
  requestedYear: number;
  sourceYear: number;
  amount: number;
  providerTypeCode: string;
  providerTypeName: string;
  financingSubtypeCode: string;
  financingSubtypeName: string;
  sourceDataset: string;
}

interface HealthOutpatientSubtypeAggregate {
  requestedYear: number;
  sourceYear: number;
  amount: number;
  providerSubtypeCode: keyof typeof OUTPATIENT_SUBTYPE_NODES;
  providerSubtypeName: string;
  sourceDataset: string;
}

interface SocialMpsvAggregate {
  year: number;
  metricGroup: string;
  metricCode: string;
  metricName: string;
  amount: number;
  sourceDataset: string;
}

interface SocialRecipientMetric {
  year: number;
  metricCode: string;
  metricName: string;
  denominatorKind: string;
  recipientCount: number;
  sourceDataset: string;
}

interface MvBudgetAggregate {
  year: number;
  basis: string;
  metricGroup: string;
  metricCode: string;
  metricName: string;
  amount: number;
  sourceDataset: string;
}

interface MvPoliceCrimeAggregate {
  year: number;
  regionName: string;
  regionCode: string;
  indicatorCode: string;
  indicatorName: string;
  crimeClassCode: string;
  crimeClassName: string;
  countValue: number;
  sourceDataset: string;
}

interface MvFireRescueActivityAggregate {
  year: number;
  regionName: string;
  regionCode: string;
  indicatorCode: string;
  indicatorName: string;
  countValue: number;
  sourceDataset: string;
}

interface OutpatientDirectoryRow {
  providerIco: string;
  providerName: string;
  providerType: string | null;
  regionName: string | null;
  municipality: string | null;
  specialtyName: string | null;
  subtypeCode: keyof typeof OUTPATIENT_SUBTYPE_NODES;
  siteCount: number;
}

const OUTPATIENT_PROVIDER_LIMIT = 28;
const OUTPATIENT_UNKNOWN_SPECIALTY = 'Ostatni odbornosti';

function toNumber(value: unknown): number {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function normalizeFounderType(founderType: string | null): OwnerBranch | null {
  if (founderType === 'Kraj') return OWNER_BRANCH.region;
  if (founderType === 'Obec, město') return OWNER_BRANCH.municipality;
  if (founderType === 'MZ' || founderType === 'Ostatní centrální orgány') {
    return OWNER_BRANCH.centralState;
  }
  return null;
}

function addNode(nodes: AtlasNode[], node: AtlasNode): void {
  if (!nodes.some((entry) => entry.id === node.id)) {
    nodes.push(node);
  }
}

function bucketNode(id: string, count: number, label: string): AtlasNode {
  return {
    id,
    name: `${id === PREV_WINDOW_ID ? '↑' : '↓'} ${count} more ${label}`,
    category: 'other',
    level: 5,
  };
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

function sumAmount(rows: HealthFinanceRow[]): number {
  return rows.reduce((sum, row) => sum + row.costs, 0);
}

function sumPeople(rows: HealthFinanceRow[]): number {
  return rows.reduce((sum, row) => sum + row.patientCount, 0);
}

function sumSiteCount(rows: OutpatientDirectoryRow[]): number {
  return rows.reduce((sum, row) => sum + row.siteCount, 0);
}

function ownerNode(ownerBranch: OwnerBranch): typeof OWNER_NODES[OwnerBranch] {
  return OWNER_NODES[ownerBranch];
}

function regionNodeId(branchKey: string, regionName: string): string {
  return `health:region:${branchKey}|${regionName}`;
}

function parseRegionNodeId(nodeId: string): { branchKey: string; regionName: string } | null {
  if (!nodeId.startsWith('health:region:')) return null;
  const payload = nodeId.replace('health:region:', '');
  const splitIndex = payload.indexOf('|');
  if (splitIndex <= 0) return null;
  return {
    branchKey: payload.slice(0, splitIndex),
    regionName: payload.slice(splitIndex + 1),
  };
}

function specialtyNodeId(subtypeCode: keyof typeof OUTPATIENT_SUBTYPE_NODES, regionName: string, specialtyName: string): string {
  return `health:specialty:${subtypeCode}|${regionName}|${specialtyName}`;
}

function parseSpecialtyNodeId(nodeId: string): { subtypeCode: keyof typeof OUTPATIENT_SUBTYPE_NODES; regionName: string; specialtyName: string } | null {
  if (!nodeId.startsWith('health:specialty:')) return null;
  const payload = nodeId.replace('health:specialty:', '');
  const parts = payload.split('|');
  if (parts.length < 3) return null;
  const [subtypeCode, regionName, ...specialtyParts] = parts;
  if (!(subtypeCode in OUTPATIENT_SUBTYPE_NODES)) return null;
  return {
    subtypeCode: subtypeCode as keyof typeof OUTPATIENT_SUBTYPE_NODES,
    regionName,
    specialtyName: specialtyParts.join('|'),
  };
}

function createStateNode(capacity: number | null = null): AtlasNode {
  return {
    id: STATE_ID,
    name: 'State budget',
    category: 'state',
    level: 0,
    ...(capacity ? { metadata: { capacity } } : {}),
  };
}

function createInsuranceNode(capacity: number | null = null): AtlasNode {
  return {
    id: HEALTH_INSURANCE_ID,
    name: 'Verejne zdravotni pojisteni',
    category: 'health_system',
    level: 1,
    ...(capacity ? { metadata: { capacity } } : {}),
  };
}

function createOutpatientNode(aggregate: HealthOutpatientSubtypeAggregate): AtlasNode {
  return {
    id: OUTPATIENT_SUBTYPE_NODES[aggregate.providerSubtypeCode].id,
    name: OUTPATIENT_SUBTYPE_NODES[aggregate.providerSubtypeCode].name,
    category: 'health_provider',
    level: 2,
    metadata: {
      sourceYear: aggregate.sourceYear,
      providerSubtypeCode: aggregate.providerSubtypeCode,
      focus: 'outpatient',
    },
  };
}

function createOutpatientSpecialtyNode(
  subtypeCode: keyof typeof OUTPATIENT_SUBTYPE_NODES,
  regionName: string,
  specialtyName: string,
  rows: OutpatientDirectoryRow[],
): AtlasNode {
  return {
    id: specialtyNodeId(subtypeCode, regionName, specialtyName),
    name: specialtyName,
    category: 'health_provider',
    level: 4,
    metadata: {
      providerCount: rows.length,
      siteCount: sumSiteCount(rows),
      focus: 'outpatient',
      subtypeCode,
      regionName,
      specialtyName,
    },
  };
}

function createMinistryNode(capacity: number | null = null): AtlasNode {
  return {
    id: HEALTH_MINISTRY_ID,
    name: 'Ministerstvo zdravotnictvi',
    category: 'ministry',
    level: 1,
    ...(capacity ? { metadata: { capacity } } : {}),
  };
}

function createSocialMinistryNode(): AtlasNode {
  return {
    id: SOCIAL_MINISTRY_ID,
    name: 'Ministerstvo prace a socialnich veci',
    category: 'ministry',
    level: 1,
  };
}

function createMvMinistryNode(): AtlasNode {
  return {
    id: MV_MINISTRY_ID,
    name: 'Ministerstvo vnitra',
    category: 'ministry',
    level: 1,
  };
}

function createMvBranchNode(
  id: string,
  name: string,
  capacity: number | null = null,
  drilldownAvailable = false,
): AtlasNode {
  return {
    id,
    name,
    category: 'other',
    level: 2,
    metadata: {
      ...(capacity ? { capacity } : {}),
      drilldownAvailable,
      focus: 'security',
    },
  };
}

function createSocialBenefitNode(id: string, name: string, capacity: number | null = null): AtlasNode {
  return {
    id,
    name,
    category: 'other',
    level: 2,
    metadata: {
      ...(capacity ? { capacity } : {}),
      focus: 'social',
    },
  };
}

function createPublicHealthNode(rows: HealthFinanceRow[]): AtlasNode {
  return {
    id: HEALTH_PUBLIC_HEALTH_ID,
    name: 'Hygiena a verejne zdravi',
    category: 'other',
    level: 2,
    metadata: {
      capacity: sumPeople(rows) || null,
      providerCount: rows.length,
      focus: 'public_health',
    },
  };
}

function createHealthAdminNode(amount: number): AtlasNode {
  return {
    id: HEALTH_ADMIN_ID,
    name: 'MZ administrativa a ostatni OSS',
    category: 'other',
    level: 2,
    metadata: {
      amountCzk: amount,
      focus: 'admin',
    },
  };
}

function createNamedAdminNode(entity: HealthMzAdminEntity): AtlasNode {
  return {
    id: ADMIN_ENTITY_NODES[entity.kind].id,
    name: ADMIN_ENTITY_NODES[entity.kind].name,
    category: 'other',
    level: 2,
    ico: entity.ico,
    metadata: {
      amountCzk: entity.amount,
      entityName: entity.name,
      regionName: entity.regionName,
      focus: 'admin',
      entityKind: entity.kind,
    },
  };
}

function createRegionNode(
  id: string,
  name: string,
  level: number,
  rows: HealthFinanceRow[],
  extra: Record<string, string | number | boolean | null> = {},
): AtlasNode {
  return {
    id,
    name,
    category: 'region',
    level,
    metadata: {
      capacity: sumPeople(rows) || null,
      providerCount: rows.length,
      ...extra,
    },
  };
}

function createProviderNode(row: HealthFinanceRow, level: number): AtlasNode {
  return {
    id: `health:provider:${row.providerIco}`,
    name: row.providerName,
    category: 'health_provider',
    level,
    ico: row.providerIco,
    ...(row.founderType ? { founderType: row.founderType } : {}),
    metadata: {
      capacity: row.patientCount || null,
      patientCount: row.patientCount,
      contactCount: row.contactCount,
      totalQuantity: row.totalQuantity,
      regionName: row.regionName,
      focus: row.focus,
      ownerBranch: row.ownerBranch,
      dataKind: row.dataKind,
      entityKind: row.entityKind,
    },
  };
}

function createOutpatientProviderNode(row: OutpatientDirectoryRow, level: number): AtlasNode {
  return {
    id: `health:provider:${row.providerIco}`,
    name: row.providerName,
    category: 'health_provider',
    level,
    ico: row.providerIco,
    metadata: {
      regionName: row.regionName,
      municipality: row.municipality,
      providerType: row.providerType,
      focus: 'outpatient',
      subtypeCode: row.subtypeCode,
      specialtyName: row.specialtyName ?? OUTPATIENT_UNKNOWN_SPECIALTY,
      siteCount: row.siteCount,
    },
  };
}

function createCostsNode(capacity: number | null): AtlasNode {
  return {
    id: HEALTH_COSTS_ID,
    name: 'Naklady zdravotnich instituci',
    category: 'cost_bucket',
    level: 4,
    ...(capacity ? { metadata: { capacity } } : {}),
  };
}

async function getHealthFinanceRows(year: number): Promise<HealthFinanceRow[]> {
  const result = await query(
    `
      with provider_directory as (
        select
          provider_ico,
          max(founder_type) as founder_type
        from mart.health_provider_directory
        group by provider_ico
      ),
      provider_finance as (
        select
          f.provider_ico,
          f.provider_name,
          f.provider_type,
          f.region_name,
          f.hospital_like,
          f.public_health_like,
          d.founder_type,
          f.costs_czk as amount_czk,
          f.total_quantity,
          f.patient_count,
          f.contact_count,
          'provider_finance'::text as data_kind,
          null::text as entity_kind,
          'health_monitor_indicators'::text as source_dataset
        from mart.health_provider_finance_yearly f
        left join provider_directory d using (provider_ico)
        where f.reporting_year = $1
          and (f.hospital_like or f.public_health_like)
          and f.costs_czk > 0
      ),
      mz_budget_entities as (
        select
          b.entity_ico as provider_ico,
          b.entity_name as provider_name,
          b.entity_kind as provider_type,
          b.region_name,
          false as hospital_like,
          true as public_health_like,
          null::text as founder_type,
          coalesce(nullif(b.expenses_czk, 0), b.costs_czk) as amount_czk,
          0::numeric as total_quantity,
          0::numeric as patient_count,
          0::numeric as contact_count,
          'budget_entity'::text as data_kind,
          b.entity_kind,
          'health_mz_budget_entities'::text as source_dataset
        from mart.health_mz_budget_entity_latest b
        where b.reporting_year = $1
          and b.entity_kind = 'hygiene_station'
          and coalesce(nullif(b.expenses_czk, 0), b.costs_czk) > 0
      )
      select
        s.provider_ico,
        s.provider_name,
        s.provider_type,
        s.region_name,
        s.hospital_like,
        s.public_health_like,
        s.founder_type,
        s.amount_czk,
        s.total_quantity,
        s.patient_count,
        s.contact_count,
        s.data_kind,
        s.entity_kind,
        s.source_dataset
      from (
        select * from provider_finance
        union all
        select * from mz_budget_entities
      ) s
      order by s.amount_czk desc, s.provider_ico
    `,
    [year],
  );

  return result.rows.map((row) => ({
    providerIco: String(row.provider_ico),
    providerName: String(row.provider_name ?? row.provider_ico),
    providerType: row.provider_type == null ? null : String(row.provider_type),
    regionName: row.region_name == null ? null : String(row.region_name),
    founderType: row.founder_type == null ? null : String(row.founder_type),
    focus: row.hospital_like ? 'hospital' : row.public_health_like ? 'public_health' : 'other',
    ownerBranch: row.hospital_like ? normalizeFounderType(row.founder_type == null ? null : String(row.founder_type)) : null,
    costs: toNumber(row.amount_czk),
    patientCount: toNumber(row.patient_count),
    contactCount: toNumber(row.contact_count),
    totalQuantity: toNumber(row.total_quantity),
    dataKind: row.data_kind === 'budget_entity' ? 'budget_entity' : 'provider_finance',
    entityKind: row.entity_kind == null ? null : String(row.entity_kind),
    sourceDataset: String(row.source_dataset ?? 'atlas.inferred'),
  }));
}

async function getHealthMzAggregate(year: number): Promise<HealthMzAggregate | null> {
  const result = await query(
    `
      select
        reporting_year,
        entity_name,
        coalesce(nullif(expenses_czk, 0), costs_czk) as amount_czk
      from mart.health_mz_budget_entity_latest
      where reporting_year = $1
        and entity_kind = 'ministry_chapter_total'
      limit 1
    `,
    [year],
  );

  const row = result.rows[0];
  if (!row) return null;
  return {
    year: Number(row.reporting_year),
    amount: toNumber(row.amount_czk),
    entityName: String(row.entity_name ?? 'Ministerstvo zdravotnictvi'),
    sourceDataset: 'health_mz_budget_entities',
  };
}

async function getHealthMzAdminEntities(year: number): Promise<HealthMzAdminEntity[]> {
  const result = await query(
    `
      select
        entity_ico,
        entity_name,
        entity_kind,
        region_name,
        coalesce(nullif(expenses_czk, 0), costs_czk) as amount_czk
      from mart.health_mz_budget_entity_latest
      where reporting_year = $1
        and entity_kind in ('uzis', 'sukl', 'kst')
        and coalesce(nullif(expenses_czk, 0), costs_czk) > 0
      order by amount_czk desc, entity_ico
    `,
    [year],
  );

  return result.rows
    .filter((row) => row.entity_kind === 'uzis' || row.entity_kind === 'sukl' || row.entity_kind === 'kst')
    .map((row) => ({
      ico: String(row.entity_ico),
      name: String(row.entity_name),
      kind: row.entity_kind as keyof typeof ADMIN_ENTITY_NODES,
      amount: toNumber(row.amount_czk),
      regionName: row.region_name == null ? null : String(row.region_name),
      sourceDataset: 'health_mz_budget_entities',
    }));
}

async function getHealthOutpatientAggregate(year: number): Promise<HealthInsuranceAggregate | null> {
  const result = await query(
    `
      select
        reporting_year,
        financing_subtype_code,
        financing_subtype_name,
        provider_type_code,
        provider_type_name,
        amount_czk
      from mart.health_financing_aggregate_latest
      where reporting_year <= $1
        and financing_subtype_code = 'HF12'
        and provider_type_code = 'HP3'
        and provider_subtype_code is null
      order by reporting_year desc
      limit 1
    `,
    [year],
  );

  const row = result.rows[0];
  if (!row) return null;
  return {
    requestedYear: year,
    sourceYear: Number(row.reporting_year),
    amount: toNumber(row.amount_czk),
    providerTypeCode: String(row.provider_type_code),
    providerTypeName: String(row.provider_type_name),
    financingSubtypeCode: String(row.financing_subtype_code),
    financingSubtypeName: String(row.financing_subtype_name),
    sourceDataset: 'health_financing_aggregates',
  };
}

async function getHealthOutpatientSubtypeAggregates(year: number): Promise<HealthOutpatientSubtypeAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        provider_subtype_code,
        provider_subtype_name,
        amount_czk
      from mart.health_financing_aggregate_latest
      where reporting_year <= $1
        and financing_subtype_code = 'HF12'
        and provider_type_code = 'HP3'
        and provider_subtype_code in ('HP31', 'HP32', 'HP33', 'HP34', 'HP35')
      order by reporting_year desc, amount_czk desc
    `,
    [year],
  );

  const bySubtype = new Map<string, HealthOutpatientSubtypeAggregate>();
  for (const row of result.rows) {
    const code = String(row.provider_subtype_code);
    if (bySubtype.has(code)) continue;
    if (!(code in OUTPATIENT_SUBTYPE_NODES)) continue;
    bySubtype.set(code, {
      requestedYear: year,
      sourceYear: Number(row.reporting_year),
      amount: toNumber(row.amount_czk),
      providerSubtypeCode: code as keyof typeof OUTPATIENT_SUBTYPE_NODES,
      providerSubtypeName: String(row.provider_subtype_name),
      sourceDataset: 'health_financing_aggregates',
    });
  }
  return [...bySubtype.values()];
}

async function getSocialMpsvAggregates(year: number): Promise<SocialMpsvAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        metric_group,
        metric_code,
        metric_name,
        amount_czk
      from mart.social_mpsv_aggregate_latest
      where reporting_year = $1
      order by metric_group, metric_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    metricGroup: String(row.metric_group),
    metricCode: String(row.metric_code),
    metricName: String(row.metric_name),
    amount: toNumber(row.amount_czk),
    sourceDataset: 'social_mpsv_aggregates',
  }));
}

async function getSocialRecipientMetrics(year: number): Promise<SocialRecipientMetric[]> {
  const result = await query(
    `
      select
        reporting_year,
        metric_code,
        metric_name,
        denominator_kind,
        recipient_count
      from mart.social_recipient_metric_latest
      where reporting_year = $1
      order by metric_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    metricCode: String(row.metric_code),
    metricName: String(row.metric_name),
    denominatorKind: String(row.denominator_kind),
    recipientCount: toNumber(row.recipient_count),
    sourceDataset: 'social_recipient_metrics',
  }));
}

async function getMvBudgetAggregates(year: number): Promise<MvBudgetAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        basis,
        metric_group,
        metric_code,
        metric_name,
        amount_czk
      from mart.mv_budget_aggregate_latest
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
    sourceDataset: 'mv_budget_aggregates',
  }));
}

async function getMvPoliceCrimeAggregates(year: number): Promise<MvPoliceCrimeAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        region_name,
        region_code,
        indicator_code,
        indicator_name,
        crime_class_code,
        crime_class_name,
        count_value
      from mart.mv_police_crime_aggregate_latest
      where reporting_year = $1
        and crime_class_code = '0-999'
      order by region_name, indicator_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    regionName: String(row.region_name),
    regionCode: String(row.region_code),
    indicatorCode: String(row.indicator_code),
    indicatorName: String(row.indicator_name),
    crimeClassCode: String(row.crime_class_code),
    crimeClassName: String(row.crime_class_name),
    countValue: toNumber(row.count_value),
    sourceDataset: 'mv_police_crime_aggregates',
  }));
}

async function getMvFireRescueActivityAggregates(year: number): Promise<MvFireRescueActivityAggregate[]> {
  const result = await query(
    `
      select
        reporting_year,
        region_name,
        region_code,
        indicator_code,
        indicator_name,
        count_value
      from mart.mv_fire_rescue_activity_aggregate_latest
      where reporting_year = $1
      order by region_name, indicator_code
    `,
    [year],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    regionName: String(row.region_name),
    regionCode: String(row.region_code),
    indicatorCode: String(row.indicator_code),
    indicatorName: String(row.indicator_name),
    countValue: toNumber(row.count_value),
    sourceDataset: 'mv_fire_rescue_activity_aggregates',
  }));
}

async function getOutpatientDirectoryRows(
  subtypeCode?: keyof typeof OUTPATIENT_SUBTYPE_NODES,
): Promise<OutpatientDirectoryRow[]> {
  const result = await query(
    `
      with classified as (
        select
          provider_ico,
          max(provider_name) as provider_name,
          max(provider_type) as provider_type,
          coalesce(max(provider_region_name), max(region_name)) as region_name,
          max(provider_municipality) as municipality,
          max(
            case
              when care_form ilike '%ambul%'
                and provider_type ilike '%stomatolog%'
                and (
                  provider_type ilike 'Samostatná ordinace%'
                  or provider_type ilike 'Samost.%ordinace%'
                )
              then
                case
                  when care_field ilike '%ortodon%' then 'ortodoncie'
                  when care_field ilike '%dentální hygien%' then 'Dentální hygienistka'
                  when care_field ilike '%orální%' then 'orální a maxilofaciální chirurgie'
                  when care_field ilike '%zub%' then 'zubní lékařství'
                  else 'zubní lékařství'
                end
              when care_form ilike '%ambul%'
                and (
                  provider_type ilike 'Samostatná ordinace%'
                  or provider_type ilike 'Samost.%ordinace%'
                )
                and provider_type not ilike '%stomatolog%'
              then
                nullif(
                  trim(
                    split_part(
                      case
                        when care_field ilike '%zub%' then provider_type
                        else coalesce(care_field, provider_type)
                      end,
                      ',',
                      1
                    )
                  ),
                  ''
                )
              else null
            end
          ) as specialty_name,
          count(distinct zz_kod) as site_count,
          max(
            case
              when care_form ilike '%ambul%'
                and (
                  provider_type ilike 'Samostatná ordinace%'
                  or provider_type ilike 'Samost.%ordinace%'
                )
                and provider_type not ilike '%stomatolog%'
              then 'HP31'
              when care_form ilike '%ambul%'
                and provider_type ilike '%stomatolog%'
                and (
                  provider_type ilike 'Samostatná ordinace%'
                  or provider_type ilike 'Samost.%ordinace%'
                )
              then 'HP32'
              else null
            end
          ) as subtype_code
        from raw.health_provider_site
        where provider_ico is not null
          and provider_ico <> ''
        group by provider_ico
      )
      select
        provider_ico,
        provider_name,
        provider_type,
        region_name,
        municipality,
        specialty_name,
        subtype_code,
        site_count
      from classified
      where subtype_code is not null
        and ($1::text is null or subtype_code = $1::text)
      order by provider_name, provider_ico
    `,
    [subtypeCode ?? null],
  );

  return result.rows
    .filter((row) => row.subtype_code === 'HP31' || row.subtype_code === 'HP32')
    .map((row) => ({
      providerIco: String(row.provider_ico),
      providerName: String(row.provider_name ?? row.provider_ico),
      providerType: row.provider_type == null ? null : String(row.provider_type),
      regionName: row.region_name == null ? null : String(row.region_name),
      municipality: row.municipality == null ? null : String(row.municipality),
      specialtyName: row.specialty_name == null ? null : String(row.specialty_name),
      subtypeCode: row.subtype_code as keyof typeof OUTPATIENT_SUBTYPE_NODES,
      siteCount: Math.max(1, Number(row.site_count ?? 1)),
    }));
}

function providerCostNote(row: HealthFinanceRow): string {
  if (row.dataKind === 'budget_entity') {
    return 'Monitor MF: výdaje rozpočtové entity MZd / KHS';
  }
  if (row.focus === 'public_health') {
    return 'Monitor MF: náklady instituce veřejného zdraví';
  }
  return 'Monitor MF: náklady poskytovatele';
}

function outpatientNote(sourceYear: number, scope: 'region' | 'specialty' | 'provider'): string {
  const suffix =
    scope === 'region'
      ? 'podle poctu registrovanych pracovist v kraji'
      : scope === 'specialty'
        ? 'podle poctu registrovanych pracovist v odbornosti'
        : 'podle poctu registrovanych pracovist poskytovatele';
  return `CZSO ZDR02: narodni agregat roku ${sourceYear}; nizsi cleneni je odhadnute z registru poskytovatelu ${suffix}`;
}

function sumAdminAmount(rows: HealthMzAdminEntity[]): number {
  return rows.reduce((sum, row) => sum + row.amount, 0);
}

function buildOwnerGroups(rows: HealthFinanceRow[]): Array<{ ownerBranch: OwnerBranch; rows: HealthFinanceRow[] }> {
  const groups = new Map<OwnerBranch, HealthFinanceRow[]>();
  for (const row of rows) {
    const ownerBranch = row.ownerBranch ?? OWNER_BRANCH.unverified;
    const bucket = groups.get(ownerBranch) ?? [];
    bucket.push(row);
    groups.set(ownerBranch, bucket);
  }

  return [OWNER_BRANCH.centralState, OWNER_BRANCH.region, OWNER_BRANCH.municipality, OWNER_BRANCH.unverified]
    .map((ownerBranch) => ({ ownerBranch, rows: groups.get(ownerBranch) ?? [] }))
    .filter((entry) => entry.rows.length > 0);
}

function socialAmountByCode(rows: SocialMpsvAggregate[], metricCode: string): number {
  return rows.find((row) => row.metricCode === metricCode)?.amount ?? 0;
}

function socialRecipientCountByCode(rows: SocialRecipientMetric[], metricCode: string): number | null {
  const value = rows.find((row) => row.metricCode === metricCode)?.recipientCount ?? 0;
  return value > 0 ? value : null;
}

function mvAmountByCode(rows: MvBudgetAggregate[], metricCode: string): number {
  return rows.find((row) => row.metricCode === metricCode)?.amount ?? 0;
}

function mvNationalCrimeCount(rows: MvPoliceCrimeAggregate[], indicatorName: string): number | null {
  const value =
    rows.find((row) => row.regionCode === 'CZ' && row.indicatorName === indicatorName)?.countValue ?? 0;
  return value > 0 ? value : null;
}

function buildMvPoliceRegionRows(rows: MvPoliceCrimeAggregate[]) {
  const registeredByRegion = new Map<string, { regionName: string; registeredCount: number; clearedCount: number }>();

  for (const row of rows) {
    if (row.regionCode === 'CZ') continue;
    const bucket = registeredByRegion.get(row.regionCode) ?? {
      regionName: row.regionName,
      registeredCount: 0,
      clearedCount: 0,
    };
    if (row.indicatorName === 'Počet registrovaných skutků') {
      bucket.registeredCount = row.countValue;
    } else if (row.indicatorName === 'Počet objasněných skutků') {
      bucket.clearedCount = row.countValue;
    }
    registeredByRegion.set(row.regionCode, bucket);
  }

  return [...registeredByRegion.entries()]
    .map(([regionCode, row]) => ({
      regionCode,
      regionName: row.regionName,
      registeredCount: row.registeredCount,
      clearedCount: row.clearedCount,
    }))
    .filter((row) => row.registeredCount > 0)
    .sort((a, b) => b.registeredCount - a.registeredCount || a.regionName.localeCompare(b.regionName, 'cs'));
}

function mvNationalFireRescueCount(rows: MvFireRescueActivityAggregate[], indicatorCode: string): number | null {
  const value = rows.find((row) => row.regionCode === 'CZ' && row.indicatorCode === indicatorCode)?.countValue ?? 0;
  return value > 0 ? value : null;
}

function buildMvFireRescueRegionRows(rows: MvFireRescueActivityAggregate[]) {
  const interventionsByRegion = new Map<string, { regionName: string; interventionCount: number; totalJpoCount: number }>();

  for (const row of rows) {
    if (row.regionCode === 'CZ') continue;
    const bucket = interventionsByRegion.get(row.regionCode) ?? {
      regionName: row.regionName,
      interventionCount: 0,
      totalJpoCount: 0,
    };
    if (row.indicatorCode === 'hzs_interventions') {
      bucket.interventionCount = row.countValue;
    } else if (row.indicatorCode === 'jpo_total_interventions') {
      bucket.totalJpoCount = row.countValue;
    }
    interventionsByRegion.set(row.regionCode, bucket);
  }

  return [...interventionsByRegion.entries()]
    .map(([regionCode, row]) => ({
      regionCode,
      regionName: row.regionName,
      interventionCount: row.interventionCount,
      totalJpoCount: row.totalJpoCount,
    }))
    .filter((row) => row.interventionCount > 0)
    .sort((a, b) => b.interventionCount - a.interventionCount || a.regionName.localeCompare(b.regionName, 'cs'));
}

function buildRegionGroups(rows: HealthFinanceRow[]): Array<{ regionName: string; rows: HealthFinanceRow[] }> {
  const grouped = new Map<string, HealthFinanceRow[]>();
  for (const row of rows) {
    const regionName = row.regionName || 'Nezname uzemi';
    const bucket = grouped.get(regionName) ?? [];
    bucket.push(row);
    grouped.set(regionName, bucket);
  }

  return [...grouped.entries()]
    .map(([regionName, regionRows]) => ({ regionName, rows: regionRows }))
    .sort((a, b) => sumAmount(b.rows) - sumAmount(a.rows));
}

function buildOutpatientRegionGroups(rows: OutpatientDirectoryRow[]): Array<{ regionName: string; rows: OutpatientDirectoryRow[] }> {
  const grouped = new Map<string, OutpatientDirectoryRow[]>();
  for (const row of rows) {
    const regionName = row.regionName || 'Nezname uzemi';
    const bucket = grouped.get(regionName) ?? [];
    bucket.push(row);
    grouped.set(regionName, bucket);
  }

  return [...grouped.entries()]
    .map(([regionName, regionRows]) => ({ regionName, rows: regionRows }))
    .sort((a, b) => sumSiteCount(b.rows) - sumSiteCount(a.rows));
}

function buildOutpatientSpecialtyGroups(
  rows: OutpatientDirectoryRow[],
): Array<{ specialtyName: string; rows: OutpatientDirectoryRow[] }> {
  const grouped = new Map<string, OutpatientDirectoryRow[]>();
  for (const row of rows) {
    const specialtyName = row.specialtyName || OUTPATIENT_UNKNOWN_SPECIALTY;
    const bucket = grouped.get(specialtyName) ?? [];
    bucket.push(row);
    grouped.set(specialtyName, bucket);
  }

  return [...grouped.entries()]
    .map(([specialtyName, specialtyRows]) => ({ specialtyName, rows: specialtyRows }))
    .sort((a, b) => sumSiteCount(b.rows) - sumSiteCount(a.rows));
}

function allocateAmount(totalAmount: number, partWeight: number, totalWeight: number): number {
  if (totalAmount <= 0 || partWeight <= 0 || totalWeight <= 0) return 0;
  return (totalAmount * partWeight) / totalWeight;
}

function buildWindow<T extends string>(sortedEntries: Array<readonly [T, number]>, offset: number, windowSize: number) {
  const windowIds = new Set(sortedEntries.slice(offset, offset + windowSize).map(([id]) => id));
  const prevIds = new Set(sortedEntries.slice(0, offset).map(([id]) => id));
  const nextIds = new Set(sortedEntries.slice(offset + windowSize).map(([id]) => id));
  return {
    windowIds,
    prevIds,
    nextIds,
    prevCount: prevIds.size,
    nextCount: nextIds.size,
    bucket(id: T) {
      if (windowIds.has(id)) return id;
      if (prevIds.has(id)) return PREV_WINDOW_ID;
      return NEXT_WINDOW_ID;
    },
  };
}

function buildCombinedRootGraph(
  year: number,
  schoolGraph: { year: number; nodes: AtlasNode[]; links: AtlasLink[] },
  socialRows: SocialMpsvAggregate[],
  socialRecipientMetrics: SocialRecipientMetric[],
  mvBudgetRows: MvBudgetAggregate[],
  mvPoliceCrimeRows: MvPoliceCrimeAggregate[],
  mvFireRescueRows: MvFireRescueActivityAggregate[],
  healthRows: HealthFinanceRow[],
  mzAggregate: HealthMzAggregate | null,
  adminEntities: HealthMzAdminEntity[],
  outpatientAggregate: HealthInsuranceAggregate | null,
  outpatientSubtypes: HealthOutpatientSubtypeAggregate[],
) {
  const nodes = [...schoolGraph.nodes];
  const links = [...schoolGraph.links];
  const socialTotal = socialAmountByCode(socialRows, 'total_expenditure');
  const mvTotal = mvAmountByCode(mvBudgetRows, 'total_expenditure');
  const stateOtherLink = links.find((link) => link.source === STATE_ID && link.target === 'state:other');

  const hospitalRows = healthRows.filter((row) => row.focus === 'hospital');
  const publicHealthRows = healthRows.filter((row) => row.focus === 'public_health');
  const hospitalAmount = sumAmount(hospitalRows);
  const outpatientAmount = outpatientAggregate?.amount ?? 0;
  const publicHealthAmount = sumAmount(publicHealthRows);
  const ministryTotal = mzAggregate?.amount ?? publicHealthAmount;
  const namedAdminAmount = sumAdminAmount(adminEntities);
  const adminAmount = Math.max(ministryTotal - publicHealthAmount - namedAdminAmount, 0);
  const explicitAtlasTopLevelAmount = socialTotal + mvTotal + hospitalAmount + outpatientAmount + ministryTotal;

  if (stateOtherLink) {
    stateOtherLink.amountCzk = Math.max(0, stateOtherLink.amountCzk - explicitAtlasTopLevelAmount);
    stateOtherLink.value = stateOtherLink.amountCzk;
    stateOtherLink.note = 'Zbytkova statni vydajova vetev po odecteni explicitne zobrazenych skolskych, socialnich a zdravotnich vetvi atlasu';
  }

  if (socialTotal > 0) {
    const pensions = socialAmountByCode(socialRows, 'pensions');
    const familySupport = socialAmountByCode(socialRows, 'family_support');
    const substituteAlimony = socialAmountByCode(socialRows, 'substitute_alimony');
    const sickness = socialAmountByCode(socialRows, 'sickness');
    const careAllowance = socialAmountByCode(socialRows, 'care_allowance');
    const disability = socialAmountByCode(socialRows, 'disability');
    const unemploymentSupport = socialAmountByCode(socialRows, 'unemployment_support');
    const employmentSupport =
      socialAmountByCode(socialRows, 'active_labour_policy') +
      socialAmountByCode(socialRows, 'disabled_employment_support') +
      socialAmountByCode(socialRows, 'employment_insolvency');
    const materialNeed = socialAmountByCode(socialRows, 'material_need');
    const residual = Math.max(
      socialTotal
        - pensions
        - familySupport
        - substituteAlimony
        - sickness
        - careAllowance
        - disability
        - unemploymentSupport
        - employmentSupport
        - materialNeed,
      0,
    );
    const pensionRecipients = socialRecipientCountByCode(socialRecipientMetrics, 'pensions_recipients_year_end');
    const unemploymentRecipients = socialRecipientCountByCode(
      socialRecipientMetrics,
      'unemployment_support_year_end_recipients',
    );
    const careAllowanceRecipients = socialRecipientCountByCode(
      socialRecipientMetrics,
      'care_allowance_december_recipients',
    );
    const substituteAlimonyRecipients = socialRecipientCountByCode(
      socialRecipientMetrics,
      'substitute_alimony_december_recipients',
    );

    addNode(nodes, createSocialMinistryNode());
    links.push(
      makeLink(
        STATE_ID,
        SOCIAL_MINISTRY_ID,
        socialTotal,
        year,
        'state_to_social_ministry',
        'MF: výsledky rozpočtového hospodaření kapitol, kapitola 313 MPSV',
        'social_mpsv_aggregates',
      ),
    );

    const benefitBuckets = [
      {
        id: 'social:benefit:pensions',
        name: 'Duchody',
        amount: pensions,
        note: 'Dávky důchodového pojištění',
        capacity: pensionRecipients,
      },
      {
        id: 'social:benefit:family',
        name: 'Rodiny a deti',
        amount: familySupport,
        note: 'Státní sociální podpora a pěstounská péče',
      },
      {
        id: 'social:benefit:substitute-alimony',
        name: 'Nahradni vyzivne',
        amount: substituteAlimony,
        note: 'Náhradní výživné',
        capacity: substituteAlimonyRecipients,
      },
      { id: 'social:benefit:sickness', name: 'Nemocenske davky', amount: sickness, note: 'Dávky nemocenského pojištění' },
      {
        id: 'social:benefit:care-allowance',
        name: 'Prispevek na peci',
        amount: careAllowance,
        note: 'Příspěvek na péči',
        capacity: careAllowanceRecipients,
      },
      {
        id: 'social:benefit:disability',
        name: 'Davky OZP',
        amount: disability,
        note: 'Dávky osobám se zdravotním postižením',
      },
      {
        id: 'social:benefit:unemployment',
        name: 'Podpory v nezamestnanosti',
        amount: unemploymentSupport,
        note: 'Podpory v nezaměstnanosti',
        capacity: unemploymentRecipients,
      },
      {
        id: 'social:benefit:employment-support',
        name: 'Zamestnanost a OZP',
        amount: employmentSupport,
        note: 'Aktivní politika zaměstnanosti, podpora zaměstnávání OZP a související podpory',
      },
      { id: 'social:benefit:material-need', name: 'Hmotna nouze', amount: materialNeed, note: 'Dávky pomoci v hmotné nouzi' },
      { id: 'social:benefit:admin-other', name: 'Sprava a ostatni socialni vydaje', amount: residual, note: 'Správa resortu, nedávkové transfery a ostatní sociální výdaje' },
    ];

    for (const bucket of benefitBuckets.filter((entry) => entry.amount > 0)) {
      addNode(nodes, createSocialBenefitNode(bucket.id, bucket.name, bucket.capacity ?? null));
      links.push(
        makeLink(
          SOCIAL_MINISTRY_ID,
          bucket.id,
          bucket.amount,
          year,
          'social_benefit_group',
          `MF kapitola 313: ${bucket.note}`,
          'social_mpsv_aggregates',
        ),
      );
    }
  }

  if (mvTotal > 0) {
    const policeAmount = mvAmountByCode(mvBudgetRows, 'police');
    const fireRescueAmount = mvAmountByCode(mvBudgetRows, 'fire_rescue');
    const adminAmountMv = mvAmountByCode(mvBudgetRows, 'ministry_admin') + mvAmountByCode(mvBudgetRows, 'sport');
    const socialAmountMv = mvAmountByCode(mvBudgetRows, 'pensions') + mvAmountByCode(mvBudgetRows, 'other_social');
    const policeCapacity = mvNationalCrimeCount(mvPoliceCrimeRows, 'Počet registrovaných skutků');
    const policeDrilldownAvailable = buildMvPoliceRegionRows(mvPoliceCrimeRows).length > 0;
    const fireRescueCapacity = mvNationalFireRescueCount(mvFireRescueRows, 'hzs_interventions');
    const fireRescueDrilldownAvailable = buildMvFireRescueRegionRows(mvFireRescueRows).length > 0;

    addNode(nodes, createMvMinistryNode());
    links.push(
      makeLink(
        STATE_ID,
        MV_MINISTRY_ID,
        mvTotal,
        year,
        'state_to_mv_ministry',
        'MV rozpočet: kapitola 314 podle oficiálních rozpočtových dokumentů MV',
        'mv_budget_aggregates',
      ),
    );

    const mvBuckets = [
      {
        id: MV_POLICE_ID,
        name: 'Policie CR',
        amount: policeAmount,
        capacity: policeCapacity,
        drilldownAvailable: policeDrilldownAvailable,
        note: 'Specifický ukazatel kapitoly 314: Výdaje Policie ČR',
      },
      {
        id: MV_FIRE_RESCUE_ID,
        name: 'HZS CR',
        amount: fireRescueAmount,
        capacity: fireRescueCapacity,
        drilldownAvailable: fireRescueDrilldownAvailable,
        note: 'Specifický ukazatel kapitoly 314: Výdaje Hasičského záchranného sboru ČR',
      },
      {
        id: MV_ADMIN_ID,
        name: 'MV a ostatni OSS',
        amount: adminAmountMv,
        capacity: null,
        drilldownAvailable: false,
        note: 'Ministerstvo vnitra, ostatní organizační složky státu a sportovní reprezentace',
      },
      {
        id: MV_SOCIAL_ID,
        name: 'Socialni davky MV',
        amount: socialAmountMv,
        capacity: null,
        drilldownAvailable: false,
        note: 'Důchody a ostatní sociální dávky vyplácené v kapitole MV',
      },
    ];

    for (const bucket of mvBuckets.filter((entry) => entry.amount > 0)) {
      addNode(nodes, createMvBranchNode(bucket.id, bucket.name, bucket.capacity, bucket.drilldownAvailable));
      links.push(
        makeLink(
          MV_MINISTRY_ID,
          bucket.id,
          bucket.amount,
          year,
          'mv_budget_group',
          bucket.note,
          'mv_budget_aggregates',
        ),
      );
    }
  }

  if (hospitalRows.length > 0) {
    const ownerGroups = buildOwnerGroups(hospitalRows);
    addNode(nodes, createInsuranceNode(sumPeople(hospitalRows) || null));
    links.push(
      makeLink(
        STATE_ID,
        HEALTH_INSURANCE_ID,
        hospitalAmount + outpatientAmount,
        year,
        'state_to_public_health_insurance',
        outpatientAggregate && outpatientAggregate.sourceYear !== year
          ? `Synteticka osa pro nemocnice a ambulantni peci pod verejnym zdravotnim pojistenim; ambulantni agregat pouziva rok ${outpatientAggregate.sourceYear}`
          : 'Synteticka osa pro nemocnice a ambulantni peci pod verejnym zdravotnim pojistenim',
      ),
    );

    for (const group of ownerGroups) {
      const node = ownerNode(group.ownerBranch);
      addNode(nodes, {
        id: node.id,
        name: node.name,
        category: node.category,
        level: 2,
        metadata: {
          capacity: sumPeople(group.rows) || null,
          providerCount: group.rows.length,
          ownerBranch: group.ownerBranch,
          focus: 'hospital',
        },
      });
      links.push(
        makeLink(
          HEALTH_INSURANCE_ID,
          node.id,
          sumAmount(group.rows),
          year,
          'health_hospital_owner_group',
          'Overene zrizovatelske seskupeni nemocnic pro drilldown, ne skutecny platebni mezistupen',
        ),
      );
    }

    for (const subtype of outpatientSubtypes) {
      addNode(nodes, createOutpatientNode(subtype));
      links.push(
        makeLink(
          HEALTH_INSURANCE_ID,
          OUTPATIENT_SUBTYPE_NODES[subtype.providerSubtypeCode].id,
          subtype.amount,
          year,
          'health_outpatient_subtype_aggregate',
          subtype.sourceYear !== year
            ? `CZSO ZDR02: ambulantni podsegment, pouzit posledni dostupny rok ${subtype.sourceYear}`
            : 'CZSO ZDR02: ambulantni podsegment pod zdravotnimi pojistovnami',
          subtype.sourceDataset,
        ),
      );
    }
  }

  if (ministryTotal > 0) {
    addNode(nodes, createMinistryNode(sumPeople(publicHealthRows) || null));
    links.push(
      makeLink(
        STATE_ID,
        HEALTH_MINISTRY_ID,
        ministryTotal,
        year,
        'state_to_health_ministry',
        mzAggregate
          ? 'Monitor MF: agregovana vydajova osa kapitoly Ministerstva zdravotnictvi'
          : 'Synteticka osa pro hygienu a verejne zdravi pod MZd',
        mzAggregate?.sourceDataset ?? 'atlas.inferred',
      ),
    );
  }

  if (publicHealthRows.length > 0) {
    addNode(nodes, createPublicHealthNode(publicHealthRows));
    links.push(
      makeLink(
        HEALTH_MINISTRY_ID,
        HEALTH_PUBLIC_HEALTH_ID,
        publicHealthAmount,
        year,
        'health_public_health_group',
        'Agregovane verejne zdravi a hygiena',
      ),
    );
  }

  if (adminAmount > 0) {
    addNode(nodes, createHealthAdminNode(adminAmount));
  }

  for (const entity of adminEntities) {
    addNode(nodes, createNamedAdminNode(entity));
    links.push(
      makeLink(
        HEALTH_MINISTRY_ID,
        ADMIN_ENTITY_NODES[entity.kind].id,
        entity.amount,
        year,
        'health_ministry_named_admin_entity',
        `Monitor MF: ${entity.name}`,
        entity.sourceDataset,
      ),
    );
  }

  if (adminAmount > 0) {
    links.push(
      makeLink(
        HEALTH_MINISTRY_ID,
        HEALTH_ADMIN_ID,
        adminAmount,
        year,
        'health_ministry_admin_residual',
        'Zbytek kapitoly MZd po odecteni KHS, SZU/ZU a vybranych centralnich instituci',
        mzAggregate?.sourceDataset ?? 'atlas.inferred',
      ),
    );
  }

  return { year, nodes, links };
}

function buildMvRootGraph(
  year: number,
  mvBudgetRows: MvBudgetAggregate[],
  mvPoliceCrimeRows: MvPoliceCrimeAggregate[],
  mvFireRescueRows: MvFireRescueActivityAggregate[],
) {
  const mvTotal = mvAmountByCode(mvBudgetRows, 'total_expenditure');
  if (mvTotal <= 0) return null;

  const policeAmount = mvAmountByCode(mvBudgetRows, 'police');
  const fireRescueAmount = mvAmountByCode(mvBudgetRows, 'fire_rescue');
  const adminAmountMv = mvAmountByCode(mvBudgetRows, 'ministry_admin') + mvAmountByCode(mvBudgetRows, 'sport');
  const socialAmountMv = mvAmountByCode(mvBudgetRows, 'pensions') + mvAmountByCode(mvBudgetRows, 'other_social');
  const policeCapacity = mvNationalCrimeCount(mvPoliceCrimeRows, 'Počet registrovaných skutků');
  const policeDrilldownAvailable = buildMvPoliceRegionRows(mvPoliceCrimeRows).length > 0;
  const fireRescueCapacity = mvNationalFireRescueCount(mvFireRescueRows, 'hzs_interventions');
  const fireRescueDrilldownAvailable = buildMvFireRescueRegionRows(mvFireRescueRows).length > 0;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];

  addNode(nodes, createStateNode(null));
  addNode(nodes, createMvMinistryNode());
  links.push(
    makeLink(
      STATE_ID,
      MV_MINISTRY_ID,
      mvTotal,
      year,
      'state_to_mv_ministry',
      'MV rozpočet: kapitola 314 podle oficiálních rozpočtových dokumentů MV',
      'mv_budget_aggregates',
    ),
  );

  const buckets = [
    {
      id: MV_POLICE_ID,
      name: 'Policie CR',
      amount: policeAmount,
      capacity: policeCapacity,
      drilldownAvailable: policeDrilldownAvailable,
      note: 'Specifický ukazatel kapitoly 314: Výdaje Policie ČR',
    },
    {
      id: MV_FIRE_RESCUE_ID,
      name: 'HZS CR',
      amount: fireRescueAmount,
      capacity: fireRescueCapacity,
      drilldownAvailable: fireRescueDrilldownAvailable,
      note: 'Specifický ukazatel kapitoly 314: Výdaje Hasičského záchranného sboru ČR',
    },
    {
      id: MV_ADMIN_ID,
      name: 'MV a ostatni OSS',
      amount: adminAmountMv,
      capacity: null,
      drilldownAvailable: false,
      note: 'Ministerstvo vnitra, ostatní organizační složky státu a sportovní reprezentace',
    },
    {
      id: MV_SOCIAL_ID,
      name: 'Socialni davky MV',
      amount: socialAmountMv,
      capacity: null,
      drilldownAvailable: false,
      note: 'Důchody a ostatní sociální dávky vyplácené v kapitole MV',
    },
  ];

  for (const bucket of buckets.filter((entry) => entry.amount > 0)) {
    addNode(nodes, createMvBranchNode(bucket.id, bucket.name, bucket.capacity, bucket.drilldownAvailable));
    links.push(
      makeLink(
        MV_MINISTRY_ID,
        bucket.id,
        bucket.amount,
        year,
        'mv_budget_group',
        bucket.note,
        'mv_budget_aggregates',
      ),
    );
  }

  return { year, nodes, links };
}

function buildMvPoliceRegionGraph(year: number, mvBudgetRows: MvBudgetAggregate[], mvPoliceCrimeRows: MvPoliceCrimeAggregate[]) {
  const policeAmount = mvAmountByCode(mvBudgetRows, 'police');
  const nationalRegisteredCount = mvNationalCrimeCount(mvPoliceCrimeRows, 'Počet registrovaných skutků');
  const regions = buildMvPoliceRegionRows(mvPoliceCrimeRows);
  if (policeAmount <= 0 || !nationalRegisteredCount || !regions.length) return null;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];

  addNode(nodes, createStateNode(null));
  addNode(nodes, createMvMinistryNode());
  addNode(nodes, createMvBranchNode(MV_POLICE_ID, 'Policie CR', nationalRegisteredCount, true));

  links.push(
    makeLink(
      STATE_ID,
      MV_MINISTRY_ID,
      policeAmount,
      year,
      'state_to_mv_ministry',
      'Zúžený pohled na policejní část kapitoly 314',
      'mv_budget_aggregates',
    ),
  );
  links.push(
    makeLink(
      MV_MINISTRY_ID,
      MV_POLICE_ID,
      policeAmount,
      year,
      'mv_budget_group',
      'Specifický ukazatel kapitoly 314: Výdaje Policie ČR',
      'mv_budget_aggregates',
    ),
  );

  for (const region of regions) {
    const allocatedAmount = allocateAmount(policeAmount, region.registeredCount, nationalRegisteredCount);
    const regionId = `security:police:region:${region.regionCode}`;
    addNode(nodes, {
      id: regionId,
      name: region.regionName,
      category: 'region',
      level: 3,
      metadata: {
        capacity: region.registeredCount,
        clearedCount: region.clearedCount,
        drilldownAvailable: false,
        focus: 'security',
      },
    });
    links.push(
      makeLink(
        MV_POLICE_ID,
        regionId,
        allocatedAmount,
        year,
        'mv_police_region_allocated_cost',
        'Regionální rozdělení policejního rozpočtu je odhadnuto podle podílu registrovaných skutků z otevřeného datasetu KRI10',
        'mv_police_crime_aggregates',
      ),
    );
  }

  return { year, nodes, links };
}

function buildMvFireRescueRegionGraph(
  year: number,
  mvBudgetRows: MvBudgetAggregate[],
  mvFireRescueRows: MvFireRescueActivityAggregate[],
) {
  const fireRescueAmount = mvAmountByCode(mvBudgetRows, 'fire_rescue');
  const nationalInterventionCount = mvNationalFireRescueCount(mvFireRescueRows, 'hzs_interventions');
  const regions = buildMvFireRescueRegionRows(mvFireRescueRows);
  if (fireRescueAmount <= 0 || !regions.length) return null;

  const regionalInterventionTotal = regions.reduce((sum, row) => sum + row.interventionCount, 0);

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];

  addNode(nodes, createStateNode(null));
  addNode(nodes, createMvMinistryNode());
  addNode(nodes, createMvBranchNode(MV_FIRE_RESCUE_ID, 'HZS CR', nationalInterventionCount, true));

  links.push(
    makeLink(
      STATE_ID,
      MV_MINISTRY_ID,
      fireRescueAmount,
      year,
      'state_to_mv_ministry',
      'Zúžený pohled na výdaje HZS v kapitole 314',
      'mv_budget_aggregates',
    ),
  );
  links.push(
    makeLink(
      MV_MINISTRY_ID,
      MV_FIRE_RESCUE_ID,
      fireRescueAmount,
      year,
      'mv_budget_group',
      'Specifický ukazatel kapitoly 314: Výdaje Hasičského záchranného sboru ČR',
      'mv_budget_aggregates',
    ),
  );

  for (const region of regions) {
    const allocatedAmount = allocateAmount(fireRescueAmount, region.interventionCount, regionalInterventionTotal);
    const regionId = `security:fire-rescue:region:${region.regionCode}`;
    addNode(nodes, {
      id: regionId,
      name: region.regionName,
      category: 'region',
      level: 3,
      metadata: {
        capacity: region.interventionCount,
        totalJpoCount: region.totalJpoCount,
        drilldownAvailable: false,
        focus: 'security',
      },
    });
    links.push(
      makeLink(
        MV_FIRE_RESCUE_ID,
        regionId,
        allocatedAmount,
        year,
        'mv_fire_rescue_region_allocated_cost',
        'Regionální rozdělení rozpočtu HZS je odhadnuto podle podílu zásahů HZS ČR z oficiální statistické ročenky HZS ČR',
        'mv_fire_rescue_activity_aggregates',
      ),
    );
  }

  return { year, nodes, links };
}

function buildHealthRootGraph(
  year: number,
  healthRows: HealthFinanceRow[],
  mzAggregate: HealthMzAggregate | null,
  adminEntities: HealthMzAdminEntity[],
  outpatientAggregate: HealthInsuranceAggregate | null,
  outpatientSubtypes: HealthOutpatientSubtypeAggregate[],
) {
  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  const hospitalRows = healthRows.filter((row) => row.focus === 'hospital');
  const publicHealthRows = healthRows.filter((row) => row.focus === 'public_health');
  const hospitalAmount = sumAmount(hospitalRows);
  const outpatientAmount = outpatientAggregate?.amount ?? 0;
  const publicHealthAmount = sumAmount(publicHealthRows);
  const ministryTotal = mzAggregate?.amount ?? publicHealthAmount;
  const namedAdminAmount = sumAdminAmount(adminEntities);
  const adminAmount = Math.max(ministryTotal - publicHealthAmount - namedAdminAmount, 0);

  addNode(nodes, createStateNode(sumPeople(healthRows) || null));

  if (hospitalRows.length > 0) {
    addNode(nodes, createInsuranceNode(sumPeople(hospitalRows) || null));
    links.push(
      makeLink(
        STATE_ID,
        HEALTH_INSURANCE_ID,
        hospitalAmount + outpatientAmount,
        year,
        'state_to_public_health_insurance',
        outpatientAggregate && outpatientAggregate.sourceYear !== year
          ? `Synteticka osa pro nemocnice a ambulantni peci pod verejnym zdravotnim pojistenim; ambulantni agregat pouziva rok ${outpatientAggregate.sourceYear}`
          : 'Synteticka osa pro nemocnice a ambulantni peci pod verejnym zdravotnim pojistenim',
      ),
    );

    for (const group of buildOwnerGroups(hospitalRows)) {
      const node = ownerNode(group.ownerBranch);
      addNode(nodes, {
        id: node.id,
        name: node.name,
        category: node.category,
        level: 2,
        metadata: {
          capacity: sumPeople(group.rows) || null,
          providerCount: group.rows.length,
          ownerBranch: group.ownerBranch,
          focus: 'hospital',
        },
      });
      links.push(
        makeLink(
          HEALTH_INSURANCE_ID,
          node.id,
          sumAmount(group.rows),
          year,
          'health_hospital_owner_group',
          'Overene zrizovatelske seskupeni nemocnic pro drilldown, ne skutecny platebni mezistupen',
        ),
      );
    }

    for (const subtype of outpatientSubtypes) {
      addNode(nodes, createOutpatientNode(subtype));
      links.push(
        makeLink(
          HEALTH_INSURANCE_ID,
          OUTPATIENT_SUBTYPE_NODES[subtype.providerSubtypeCode].id,
          subtype.amount,
          year,
          'health_outpatient_subtype_aggregate',
          subtype.sourceYear !== year
            ? `CZSO ZDR02: ambulantni podsegment, pouzit posledni dostupny rok ${subtype.sourceYear}`
            : 'CZSO ZDR02: ambulantni podsegment pod zdravotnimi pojistovnami',
          subtype.sourceDataset,
        ),
      );
    }
  }

  if (ministryTotal > 0) {
    addNode(nodes, createMinistryNode(sumPeople(publicHealthRows) || null));
    links.push(
      makeLink(
        STATE_ID,
        HEALTH_MINISTRY_ID,
        ministryTotal,
        year,
        'state_to_health_ministry',
        mzAggregate
          ? 'Monitor MF: agregovana vydajova osa kapitoly Ministerstva zdravotnictvi'
          : 'Synteticka osa pro hygienu a verejne zdravi pod MZd',
        mzAggregate?.sourceDataset ?? 'atlas.inferred',
      ),
    );
  }

  if (publicHealthRows.length > 0) {
    addNode(nodes, createPublicHealthNode(publicHealthRows));
    links.push(
      makeLink(
        HEALTH_MINISTRY_ID,
        HEALTH_PUBLIC_HEALTH_ID,
        publicHealthAmount,
        year,
        'health_public_health_group',
        'Agregovane verejne zdravi a hygiena',
      ),
    );
  }

  if (adminAmount > 0) {
    addNode(nodes, createHealthAdminNode(adminAmount));
  }

  for (const entity of adminEntities) {
    addNode(nodes, createNamedAdminNode(entity));
    links.push(
      makeLink(
        HEALTH_MINISTRY_ID,
        ADMIN_ENTITY_NODES[entity.kind].id,
        entity.amount,
        year,
        'health_ministry_named_admin_entity',
        `Monitor MF: ${entity.name}`,
        entity.sourceDataset,
      ),
    );
  }

  if (adminAmount > 0) {
    links.push(
      makeLink(
        HEALTH_MINISTRY_ID,
        HEALTH_ADMIN_ID,
        adminAmount,
        year,
        'health_ministry_admin_residual',
        'Zbytek kapitoly MZd po odecteni KHS, SZU/ZU a vybranych centralnich instituci',
        mzAggregate?.sourceDataset ?? 'atlas.inferred',
      ),
    );
  }

  return { year, nodes, links };
}

function buildOwnerRegionGraph(year: number, ownerBranch: OwnerBranch, rows: HealthFinanceRow[]) {
  const ownerRows = rows.filter((row) => (row.ownerBranch ?? OWNER_BRANCH.unverified) === ownerBranch);
  if (!ownerRows.length) return null;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  const owner = ownerNode(ownerBranch);
  const regions = buildRegionGroups(ownerRows);

  addNode(nodes, createStateNode(sumPeople(ownerRows) || null));
  addNode(nodes, createInsuranceNode(sumPeople(ownerRows) || null));
  addNode(nodes, {
    id: owner.id,
    name: owner.name,
    category: owner.category,
    level: 2,
    metadata: {
      capacity: sumPeople(ownerRows) || null,
      providerCount: ownerRows.length,
      ownerBranch,
      focus: 'hospital',
    },
  });

  links.push(
    makeLink(
      STATE_ID,
      HEALTH_INSURANCE_ID,
      sumAmount(ownerRows),
      year,
      'state_to_public_health_insurance',
      'Synteticka osa pro nemocnice vedene pod verejnym zdravotnim pojistenim',
    ),
  );
  links.push(
    makeLink(
      HEALTH_INSURANCE_ID,
      owner.id,
      sumAmount(ownerRows),
      year,
      'health_hospital_owner_group',
      'Overene zrizovatelske seskupeni nemocnic pro drilldown, ne skutecny platebni mezistupen',
    ),
  );

  for (const region of regions) {
    const regionId = regionNodeId(ownerBranch, region.regionName);
    addNode(nodes, createRegionNode(regionId, region.regionName, 3, region.rows, { branchKey: ownerBranch }));
    links.push(
      makeLink(
        owner.id,
        regionId,
        sumAmount(region.rows),
        year,
        'health_region_group',
        'Regionální seskupeni nemocnic pod overenym zrizovatelem',
      ),
    );
  }

  return { year, nodes, links };
}

function buildPublicHealthRegionGraph(year: number, rows: HealthFinanceRow[]) {
  const publicHealthRows = rows.filter((row) => row.focus === 'public_health');
  if (!publicHealthRows.length) return null;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  const regions = buildRegionGroups(publicHealthRows);

  addNode(nodes, createStateNode(sumPeople(publicHealthRows) || null));
  addNode(nodes, createMinistryNode(sumPeople(publicHealthRows) || null));
  addNode(nodes, createPublicHealthNode(publicHealthRows));

  links.push(
    makeLink(
      STATE_ID,
      HEALTH_MINISTRY_ID,
      sumAmount(publicHealthRows),
      year,
      'state_to_health_ministry',
      'Synteticka osa pro hygienu a verejne zdravi pod MZd',
    ),
  );
  links.push(
    makeLink(
      HEALTH_MINISTRY_ID,
      HEALTH_PUBLIC_HEALTH_ID,
      sumAmount(publicHealthRows),
      year,
      'health_public_health_group',
      'Agregovane verejne zdravi a hygiena',
    ),
  );

  for (const region of regions) {
    const regionId = regionNodeId('public_health', region.regionName);
    addNode(nodes, createRegionNode(regionId, region.regionName, 3, region.rows, { branchKey: 'public_health' }));
    links.push(
      makeLink(
        HEALTH_PUBLIC_HEALTH_ID,
        regionId,
        sumAmount(region.rows),
        year,
        'health_public_health_region_group',
        'Regionální seskupeni hygieny a verejneho zdravi',
      ),
    );
  }

  return { year, nodes, links };
}

function buildOutpatientSubtypeRegionGraph(
  year: number,
  aggregate: HealthOutpatientSubtypeAggregate,
  rows: OutpatientDirectoryRow[],
) {
  if (!rows.length) return null;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  const totalSites = sumSiteCount(rows);
  const subtypeId = OUTPATIENT_SUBTYPE_NODES[aggregate.providerSubtypeCode].id;

  addNode(nodes, createStateNode(null));
  addNode(nodes, createInsuranceNode(null));
  addNode(nodes, createOutpatientNode(aggregate));

  links.push(
    makeLink(
      STATE_ID,
      HEALTH_INSURANCE_ID,
      aggregate.amount,
      year,
      'state_to_public_health_insurance',
      aggregate.sourceYear !== year
        ? `Synteticka osa pro ambulantni peci pod verejnym zdravotnim pojistenim; pouzit posledni dostupny rok ${aggregate.sourceYear}`
        : 'Synteticka osa pro ambulantni peci pod verejnym zdravotnim pojistenim',
      aggregate.sourceDataset,
    ),
  );
  links.push(
    makeLink(
      HEALTH_INSURANCE_ID,
      subtypeId,
      aggregate.amount,
      year,
      'health_outpatient_subtype_aggregate',
      aggregate.sourceYear !== year
        ? `CZSO ZDR02: ambulantni podsegment, pouzit posledni dostupny rok ${aggregate.sourceYear}`
        : 'CZSO ZDR02: ambulantni podsegment pod zdravotnimi pojistovnami',
      aggregate.sourceDataset,
    ),
  );

  for (const region of buildOutpatientRegionGroups(rows)) {
    const amount = allocateAmount(aggregate.amount, sumSiteCount(region.rows), totalSites);
    const regionId = regionNodeId(aggregate.providerSubtypeCode.toLowerCase(), region.regionName);
    addNode(nodes, {
      id: regionId,
      name: region.regionName,
      category: 'region',
      level: 3,
      metadata: {
        providerCount: region.rows.length,
        siteCount: sumSiteCount(region.rows),
        focus: 'outpatient',
        subtypeCode: aggregate.providerSubtypeCode,
      },
    });
    links.push(
      makeLink(
        subtypeId,
        regionId,
        amount,
        year,
        'health_outpatient_region_group',
        outpatientNote(aggregate.sourceYear, 'region'),
      ),
    );
  }

  return { year, nodes, links };
}

function buildOutpatientRegionSpecialtyGraph(
  year: number,
  aggregate: HealthOutpatientSubtypeAggregate,
  regionName: string,
  rows: OutpatientDirectoryRow[],
) {
  const regionRows = rows.filter((row) => (row.regionName || 'Nezname uzemi') === regionName);
  if (!regionRows.length) return null;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  const subtypeRowsSiteCount = sumSiteCount(rows);
  const regionSiteCount = sumSiteCount(regionRows);
  const regionAmount = allocateAmount(aggregate.amount, regionSiteCount, subtypeRowsSiteCount);
  const subtypeId = OUTPATIENT_SUBTYPE_NODES[aggregate.providerSubtypeCode].id;
  const regionId = regionNodeId(aggregate.providerSubtypeCode.toLowerCase(), regionName);

  addNode(nodes, createStateNode(null));
  addNode(nodes, createInsuranceNode(null));
  addNode(nodes, createOutpatientNode(aggregate));
  addNode(nodes, {
    id: regionId,
    name: regionName,
    category: 'region',
    level: 3,
    metadata: {
      providerCount: regionRows.length,
      siteCount: regionSiteCount,
      focus: 'outpatient',
      subtypeCode: aggregate.providerSubtypeCode,
    },
  });

  links.push(makeLink(STATE_ID, HEALTH_INSURANCE_ID, aggregate.amount, year, 'state_to_public_health_insurance', 'Synteticka osa pro ambulantni peci pod verejnym zdravotnim pojistenim', aggregate.sourceDataset));
  links.push(makeLink(HEALTH_INSURANCE_ID, subtypeId, aggregate.amount, year, 'health_outpatient_subtype_aggregate', 'CZSO ZDR02: ambulantni podsegment pod zdravotnimi pojistovnami', aggregate.sourceDataset));
  links.push(makeLink(subtypeId, regionId, regionAmount, year, 'health_outpatient_region_group', outpatientNote(aggregate.sourceYear, 'region')));

  for (const specialty of buildOutpatientSpecialtyGroups(regionRows)) {
    const specialtyAmount = allocateAmount(regionAmount, sumSiteCount(specialty.rows), regionSiteCount);
    const specialtyNode = createOutpatientSpecialtyNode(aggregate.providerSubtypeCode, regionName, specialty.specialtyName, specialty.rows);
    addNode(nodes, specialtyNode);
    links.push(
      makeLink(
        regionId,
        specialtyNode.id,
        specialtyAmount,
        year,
        'health_outpatient_specialty_group',
        outpatientNote(aggregate.sourceYear, 'specialty'),
      ),
    );
  }

  return { year, nodes, links };
}

function buildOutpatientSpecialtyProviderGraph(
  year: number,
  aggregate: HealthOutpatientSubtypeAggregate,
  regionName: string,
  specialtyName: string,
  rows: OutpatientDirectoryRow[],
  offset = 0,
) {
  const regionRows = rows.filter((row) => (row.regionName || 'Nezname uzemi') === regionName);
  const specialtyRows = regionRows.filter((row) => (row.specialtyName || OUTPATIENT_UNKNOWN_SPECIALTY) === specialtyName);
  if (!specialtyRows.length) return null;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  const totalSubtypeSites = sumSiteCount(rows);
  const totalRegionSites = sumSiteCount(regionRows);
  const totalSpecialtySites = sumSiteCount(specialtyRows);
  const regionAmount = allocateAmount(aggregate.amount, totalRegionSites, totalSubtypeSites);
  const specialtyAmount = allocateAmount(regionAmount, totalSpecialtySites, totalRegionSites);
  const subtypeId = OUTPATIENT_SUBTYPE_NODES[aggregate.providerSubtypeCode].id;
  const regionId = regionNodeId(aggregate.providerSubtypeCode.toLowerCase(), regionName);
  const specialtyId = specialtyNodeId(aggregate.providerSubtypeCode, regionName, specialtyName);

  addNode(nodes, createStateNode(null));
  addNode(nodes, createInsuranceNode(null));
  addNode(nodes, createOutpatientNode(aggregate));
  addNode(nodes, {
    id: regionId,
    name: regionName,
    category: 'region',
    level: 3,
    metadata: {
      providerCount: regionRows.length,
      siteCount: totalRegionSites,
      focus: 'outpatient',
      subtypeCode: aggregate.providerSubtypeCode,
    },
  });
  addNode(nodes, createOutpatientSpecialtyNode(aggregate.providerSubtypeCode, regionName, specialtyName, specialtyRows));

  links.push(makeLink(STATE_ID, HEALTH_INSURANCE_ID, aggregate.amount, year, 'state_to_public_health_insurance', 'Synteticka osa pro ambulantni peci pod verejnym zdravotnim pojistenim', aggregate.sourceDataset));
  links.push(makeLink(HEALTH_INSURANCE_ID, subtypeId, aggregate.amount, year, 'health_outpatient_subtype_aggregate', 'CZSO ZDR02: ambulantni podsegment pod zdravotnimi pojistovnami', aggregate.sourceDataset));
  links.push(makeLink(subtypeId, regionId, regionAmount, year, 'health_outpatient_region_group', outpatientNote(aggregate.sourceYear, 'region')));
  links.push(makeLink(regionId, specialtyId, specialtyAmount, year, 'health_outpatient_specialty_group', outpatientNote(aggregate.sourceYear, 'specialty')));

  const sortedProviders = [...specialtyRows].sort((a, b) => b.siteCount - a.siteCount || a.providerName.localeCompare(b.providerName, 'cs'));
  const sortedEntries = sortedProviders.map((row) => [row.providerIco, row.siteCount] as const);
  const window = buildWindow(sortedEntries, offset, OUTPATIENT_PROVIDER_LIMIT);
  const visibleProviders = sortedProviders.filter((row) => window.windowIds.has(row.providerIco));

  for (const row of visibleProviders) {
    const providerAmount = allocateAmount(specialtyAmount, row.siteCount, totalSpecialtySites);
    addNode(nodes, createOutpatientProviderNode(row, 5));
    links.push(
      makeLink(
        specialtyId,
        `health:provider:${row.providerIco}`,
        providerAmount,
        year,
        'health_outpatient_provider_allocated_cost',
        outpatientNote(aggregate.sourceYear, 'provider'),
      ),
    );
  }

  if (window.prevCount) {
    addNode(nodes, bucketNode(PREV_WINDOW_ID, window.prevCount, 'providers'));
    links.push(
      makeLink(
        specialtyId,
        PREV_WINDOW_ID,
        sortedProviders
          .filter((row) => window.prevIds.has(row.providerIco))
          .reduce((sum, row) => sum + allocateAmount(specialtyAmount, row.siteCount, totalSpecialtySites), 0),
        year,
        'health_outpatient_provider_allocated_cost',
        outpatientNote(aggregate.sourceYear, 'provider'),
      ),
    );
  }

  if (window.nextCount) {
    addNode(nodes, bucketNode(NEXT_WINDOW_ID, window.nextCount, 'providers'));
    links.push(
      makeLink(
        specialtyId,
        NEXT_WINDOW_ID,
        sortedProviders
          .filter((row) => window.nextIds.has(row.providerIco))
          .reduce((sum, row) => sum + allocateAmount(specialtyAmount, row.siteCount, totalSpecialtySites), 0),
        year,
        'health_outpatient_provider_allocated_cost',
        outpatientNote(aggregate.sourceYear, 'provider'),
      ),
    );
  }

  return { year, nodes, links };
}

function buildRegionProviderGraph(year: number, branchKey: string, regionName: string, rows: HealthFinanceRow[]) {
  const regionRows = rows.filter((row) => {
    const rowRegion = row.regionName || 'Nezname uzemi';
    if (rowRegion !== regionName) return false;
    if (branchKey === 'public_health') return row.focus === 'public_health';
    return (row.ownerBranch ?? OWNER_BRANCH.unverified) === branchKey;
  });
  if (!regionRows.length) return null;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  const totalAmount = sumAmount(regionRows);
  const totalPeople = sumPeople(regionRows) || null;
  const regionId = regionNodeId(branchKey, regionName);

  addNode(nodes, createStateNode(totalPeople));
  addNode(nodes, createCostsNode(totalPeople));

  if (branchKey === 'public_health') {
    addNode(nodes, createMinistryNode(totalPeople));
    addNode(nodes, createPublicHealthNode(regionRows));
    addNode(nodes, createRegionNode(regionId, regionName, 3, regionRows, { branchKey }));
    links.push(makeLink(STATE_ID, HEALTH_MINISTRY_ID, totalAmount, year, 'state_to_health_ministry', 'Synteticka osa pro hygienu a verejne zdravi pod MZd'));
    links.push(makeLink(HEALTH_MINISTRY_ID, HEALTH_PUBLIC_HEALTH_ID, totalAmount, year, 'health_public_health_group', 'Agregovane verejne zdravi a hygiena'));
    links.push(makeLink(HEALTH_PUBLIC_HEALTH_ID, regionId, totalAmount, year, 'health_public_health_region_group', 'Regionální seskupeni hygieny a verejneho zdravi'));
  } else {
    const owner = ownerNode(branchKey as OwnerBranch);
    addNode(nodes, createInsuranceNode(totalPeople));
    addNode(nodes, {
      id: owner.id,
      name: owner.name,
      category: owner.category,
      level: 2,
      metadata: {
        capacity: totalPeople,
        providerCount: regionRows.length,
        ownerBranch: branchKey,
        focus: 'hospital',
      },
    });
    addNode(nodes, createRegionNode(regionId, regionName, 3, regionRows, { branchKey }));
    links.push(makeLink(STATE_ID, HEALTH_INSURANCE_ID, totalAmount, year, 'state_to_public_health_insurance', 'Synteticka osa pro nemocnice vedene pod verejnym zdravotnim pojistenim'));
    links.push(makeLink(HEALTH_INSURANCE_ID, owner.id, totalAmount, year, 'health_hospital_owner_group', 'Overene zrizovatelske seskupeni nemocnic pro drilldown, ne skutecny platebni mezistupen'));
    links.push(makeLink(owner.id, regionId, totalAmount, year, 'health_region_group', 'Regionální seskupeni nemocnic pod overenym zrizovatelem'));
  }

  for (const row of regionRows.sort((a, b) => b.costs - a.costs)) {
    const providerId = `health:provider:${row.providerIco}`;
    addNode(nodes, createProviderNode(row, 4));
    links.push(
      makeLink(
        regionId,
        providerId,
        row.costs,
        year,
        'health_provider_costs',
        providerCostNote(row),
        row.sourceDataset,
      ),
    );
    links.push(
      makeLink(
        providerId,
        HEALTH_COSTS_ID,
        row.costs,
        year,
        'health_operating_costs',
        providerCostNote(row),
        row.sourceDataset,
      ),
    );
  }

  return { year, nodes, links };
}

function buildOutpatientProviderDetailGraph(
  year: number,
  aggregate: HealthOutpatientSubtypeAggregate,
  providerIco: string,
  rows: OutpatientDirectoryRow[],
) {
  const row = rows.find((entry) => entry.providerIco === providerIco);
  if (!row) return null;

  const regionName = row.regionName || 'Nezname uzemi';
  const specialtyName = row.specialtyName || OUTPATIENT_UNKNOWN_SPECIALTY;
  const subtypeRows = rows;
  const regionRows = rows.filter((entry) => (entry.regionName || 'Nezname uzemi') === regionName);
  const specialtyRows = regionRows.filter((entry) => (entry.specialtyName || OUTPATIENT_UNKNOWN_SPECIALTY) === specialtyName);
  const totalSubtypeSites = sumSiteCount(subtypeRows);
  const totalRegionSites = sumSiteCount(regionRows);
  const totalSpecialtySites = sumSiteCount(specialtyRows);
  const regionAmount = allocateAmount(aggregate.amount, totalRegionSites, totalSubtypeSites);
  const specialtyAmount = allocateAmount(regionAmount, totalSpecialtySites, totalRegionSites);
  const providerAmount = allocateAmount(specialtyAmount, row.siteCount, totalSpecialtySites);
  const subtypeId = OUTPATIENT_SUBTYPE_NODES[aggregate.providerSubtypeCode].id;
  const regionId = regionNodeId(aggregate.providerSubtypeCode.toLowerCase(), regionName);
  const specialtyId = specialtyNodeId(aggregate.providerSubtypeCode, regionName, specialtyName);
  const providerId = `health:provider:${row.providerIco}`;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];

  addNode(nodes, createStateNode(null));
  addNode(nodes, createInsuranceNode(null));
  addNode(nodes, createOutpatientNode(aggregate));
  addNode(nodes, {
    id: regionId,
    name: regionName,
    category: 'region',
    level: 3,
    metadata: {
      providerCount: regionRows.length,
      siteCount: totalRegionSites,
      focus: 'outpatient',
      subtypeCode: aggregate.providerSubtypeCode,
    },
  });
  addNode(nodes, createOutpatientSpecialtyNode(aggregate.providerSubtypeCode, regionName, specialtyName, specialtyRows));
  addNode(nodes, createOutpatientProviderNode(row, 5));
  addNode(nodes, createCostsNode(null));

  links.push(makeLink(STATE_ID, HEALTH_INSURANCE_ID, aggregate.amount, year, 'state_to_public_health_insurance', 'Synteticka osa pro ambulantni peci pod verejnym zdravotnim pojistenim', aggregate.sourceDataset));
  links.push(makeLink(HEALTH_INSURANCE_ID, subtypeId, aggregate.amount, year, 'health_outpatient_subtype_aggregate', 'CZSO ZDR02: ambulantni podsegment pod zdravotnimi pojistovnami', aggregate.sourceDataset));
  links.push(makeLink(subtypeId, regionId, regionAmount, year, 'health_outpatient_region_group', outpatientNote(aggregate.sourceYear, 'region')));
  links.push(makeLink(regionId, specialtyId, specialtyAmount, year, 'health_outpatient_specialty_group', outpatientNote(aggregate.sourceYear, 'specialty')));
  links.push(makeLink(specialtyId, providerId, providerAmount, year, 'health_outpatient_provider_allocated_cost', outpatientNote(aggregate.sourceYear, 'provider')));
  links.push(makeLink(providerId, HEALTH_COSTS_ID, providerAmount, year, 'health_operating_costs', outpatientNote(aggregate.sourceYear, 'provider')));

  return { year, nodes, links };
}

function buildProviderDetailGraph(year: number, providerIco: string, rows: HealthFinanceRow[]) {
  const row = rows.find((entry) => entry.providerIco === providerIco);
  if (!row) return null;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  const providerId = `health:provider:${row.providerIco}`;
  const regionName = row.regionName || 'Nezname uzemi';
  const branchKey = row.focus === 'public_health' ? 'public_health' : (row.ownerBranch ?? OWNER_BRANCH.unverified);
  const regionId = regionNodeId(branchKey, regionName);
  const capacity = row.patientCount || null;

  addNode(nodes, createStateNode(capacity));
  addNode(nodes, createCostsNode(capacity));
  addNode(nodes, createProviderNode(row, 4));

  if (row.focus === 'public_health') {
    addNode(nodes, createMinistryNode(capacity));
    addNode(nodes, createPublicHealthNode([row]));
    addNode(nodes, createRegionNode(regionId, regionName, 3, [row], { branchKey }));
    links.push(makeLink(STATE_ID, HEALTH_MINISTRY_ID, row.costs, year, 'state_to_health_ministry', 'Synteticka osa pro hygienu a verejne zdravi pod MZd'));
    links.push(makeLink(HEALTH_MINISTRY_ID, HEALTH_PUBLIC_HEALTH_ID, row.costs, year, 'health_public_health_group', 'Agregovane verejne zdravi a hygiena'));
    links.push(makeLink(HEALTH_PUBLIC_HEALTH_ID, regionId, row.costs, year, 'health_public_health_region_group', 'Regionální seskupeni hygieny a verejneho zdravi'));
  } else {
    const owner = ownerNode((row.ownerBranch ?? OWNER_BRANCH.unverified) as OwnerBranch);
    addNode(nodes, createInsuranceNode(capacity));
    addNode(nodes, {
      id: owner.id,
      name: owner.name,
      category: owner.category,
      level: 2,
      metadata: {
        capacity,
        providerCount: 1,
        ownerBranch: row.ownerBranch ?? OWNER_BRANCH.unverified,
        focus: 'hospital',
      },
    });
    addNode(nodes, createRegionNode(regionId, regionName, 3, [row], { branchKey }));
    links.push(makeLink(STATE_ID, HEALTH_INSURANCE_ID, row.costs, year, 'state_to_public_health_insurance', 'Synteticka osa pro nemocnice vedene pod verejnym zdravotnim pojistenim'));
    links.push(makeLink(HEALTH_INSURANCE_ID, owner.id, row.costs, year, 'health_hospital_owner_group', 'Overene zrizovatelske seskupeni nemocnic pro drilldown, ne skutecny platebni mezistupen'));
    links.push(makeLink(owner.id, regionId, row.costs, year, 'health_region_group', 'Regionální seskupeni nemocnic pod overenym zrizovatelem'));
  }

  links.push(makeLink(regionId, providerId, row.costs, year, 'health_provider_costs', providerCostNote(row), row.sourceDataset));
  links.push(makeLink(providerId, HEALTH_COSTS_ID, row.costs, year, 'health_operating_costs', providerCostNote(row), row.sourceDataset));

  return { year, nodes, links };
}

export async function getAtlasYears() {
  const [schoolYears, healthYears] = await Promise.all([
    getAvailableYears(),
    query(
      `
        select distinct reporting_year
        from (
          select reporting_year
          from mart.health_provider_finance_yearly
          where costs_czk > 0
          union
          select reporting_year
          from mart.health_mz_budget_entity_latest
          where coalesce(nullif(expenses_czk, 0), costs_czk) > 0
          union
          select reporting_year
          from mart.health_financing_aggregate_latest
          where financing_subtype_code = 'HF12'
            and provider_type_code = 'HP3'
            and amount_czk > 0
        ) years
        order by reporting_year
      `,
    ),
  ]);
  const healthYearSet = new Set(healthYears.rows.map((row) => Number(row.reporting_year)));

  return schoolYears
    .filter((row) => healthYearSet.has(row.year))
    .map((row) => ({
      year: row.year,
      school: row.directSchoolFinanceRows > 0,
      healthActivity: true,
      healthFinancePilot: true,
    }));
}

export async function getAtlasOverview(year: number) {
  const [
    schoolGraph,
    socialRows,
    socialRecipientMetrics,
    mvBudgetRows,
    mvPoliceCrimeRows,
    mvFireRescueRows,
    healthRows,
    mzAggregate,
    adminEntities,
    outpatientAggregate,
    outpatientSubtypes,
  ] = await Promise.all([
    getSchoolOverviewGraph(year),
    getSocialMpsvAggregates(year),
    getSocialRecipientMetrics(year),
    getMvBudgetAggregates(year),
    getMvPoliceCrimeAggregates(year),
    getMvFireRescueActivityAggregates(year),
    getHealthFinanceRows(year),
    getHealthMzAggregate(year),
    getHealthMzAdminEntities(year),
    getHealthOutpatientAggregate(year),
    getHealthOutpatientSubtypeAggregates(year),
  ]);

  if (!schoolGraph) return null;
  return buildCombinedRootGraph(
    year,
    schoolGraph,
    socialRows,
    socialRecipientMetrics,
    mvBudgetRows,
    mvPoliceCrimeRows,
    mvFireRescueRows,
    healthRows,
    mzAggregate,
    adminEntities,
    outpatientAggregate,
    outpatientSubtypes,
  );
}

export async function getAtlasHealthGraph(year: number, nodeId: string | null = null, offset = 0) {
  const [rows, mzAggregate, adminEntities, outpatientAggregate, outpatientSubtypes, outpatientDirectoryRows] = await Promise.all([
    getHealthFinanceRows(year),
    getHealthMzAggregate(year),
    getHealthMzAdminEntities(year),
    getHealthOutpatientAggregate(year),
    getHealthOutpatientSubtypeAggregates(year),
    getOutpatientDirectoryRows(),
  ]);
  if (!rows.length) return null;

  if (!nodeId || nodeId === HEALTH_INSURANCE_ID || nodeId === HEALTH_MINISTRY_ID) {
    return buildHealthRootGraph(year, rows, mzAggregate, adminEntities, outpatientAggregate, outpatientSubtypes);
  }

  if (nodeId === HEALTH_PUBLIC_HEALTH_ID) {
    return buildPublicHealthRegionGraph(year, rows);
  }

  const ownerEntry = Object.values(OWNER_NODES).find((entry) => entry.id === nodeId);
  if (ownerEntry) {
    const ownerBranch = (Object.entries(OWNER_NODES).find(([, entry]) => entry.id === nodeId)?.[0] ?? null) as OwnerBranch | null;
    return ownerBranch ? buildOwnerRegionGraph(year, ownerBranch, rows) : null;
  }

  const outpatientSubtype = outpatientSubtypes.find(
    (entry) => OUTPATIENT_SUBTYPE_NODES[entry.providerSubtypeCode].id === nodeId && (entry.providerSubtypeCode === 'HP31' || entry.providerSubtypeCode === 'HP32'),
  );
  if (outpatientSubtype) {
    const subtypeRows = outpatientDirectoryRows.filter((row) => row.subtypeCode === outpatientSubtype.providerSubtypeCode);
    return buildOutpatientSubtypeRegionGraph(year, outpatientSubtype, subtypeRows);
  }

  const region = parseRegionNodeId(nodeId);
  if (region) {
    if (region.branchKey === 'hp31' || region.branchKey === 'hp32') {
      const subtypeCode = region.branchKey.toUpperCase() as keyof typeof OUTPATIENT_SUBTYPE_NODES;
      const outpatientAggregate = outpatientSubtypes.find((entry) => entry.providerSubtypeCode === subtypeCode);
      if (!outpatientAggregate) return null;
      const subtypeRows = outpatientDirectoryRows.filter((row) => row.subtypeCode === subtypeCode);
      return buildOutpatientRegionSpecialtyGraph(year, outpatientAggregate, region.regionName, subtypeRows);
    }
    return buildRegionProviderGraph(year, region.branchKey, region.regionName, rows);
  }

  const specialty = parseSpecialtyNodeId(nodeId);
  if (specialty) {
    const outpatientAggregate = outpatientSubtypes.find((entry) => entry.providerSubtypeCode === specialty.subtypeCode);
    if (!outpatientAggregate) return null;
    const subtypeRows = outpatientDirectoryRows.filter((row) => row.subtypeCode === specialty.subtypeCode);
    return buildOutpatientSpecialtyProviderGraph(
      year,
      outpatientAggregate,
      specialty.regionName,
      specialty.specialtyName,
      subtypeRows,
      offset,
    );
  }

  if (nodeId.startsWith('health:provider:')) {
    const providerIco = nodeId.replace('health:provider:', '');
    const healthProviderGraph = buildProviderDetailGraph(year, providerIco, rows);
    if (healthProviderGraph) return healthProviderGraph;

    const outpatientRow = outpatientDirectoryRows.find((row) => row.providerIco === providerIco);
    if (!outpatientRow) return null;
    const outpatientAggregate = outpatientSubtypes.find((entry) => entry.providerSubtypeCode === outpatientRow.subtypeCode);
    if (!outpatientAggregate) return null;
    const subtypeRows = outpatientDirectoryRows.filter((row) => row.subtypeCode === outpatientRow.subtypeCode);
    return buildOutpatientProviderDetailGraph(year, outpatientAggregate, providerIco, subtypeRows);
  }

  return null;
}

export async function getAtlasMvGraph(year: number, nodeId: string | null = null) {
  const [mvBudgetRows, mvPoliceCrimeRows, mvFireRescueRows] = await Promise.all([
    getMvBudgetAggregates(year),
    getMvPoliceCrimeAggregates(year),
    getMvFireRescueActivityAggregates(year),
  ]);

  if (!mvBudgetRows.length) return null;
  if (!nodeId || nodeId === MV_MINISTRY_ID) {
    return buildMvRootGraph(year, mvBudgetRows, mvPoliceCrimeRows, mvFireRescueRows);
  }
  if (nodeId === MV_POLICE_ID) {
    return buildMvPoliceRegionGraph(year, mvBudgetRows, mvPoliceCrimeRows);
  }
  if (nodeId === MV_FIRE_RESCUE_ID) {
    return buildMvFireRescueRegionGraph(year, mvBudgetRows, mvFireRescueRows);
  }
  return null;
}

export async function searchAtlasEntities(year: number, q: string, limit = 8) {
  const needle = q.trim();
  if (needle.length < 2) return [];

  const perDomainLimit = Math.max(4, Math.ceil(limit / 2));
  const [schoolResults, healthResult] = await Promise.all([
    searchInstitutions(year, needle, perDomainLimit),
    query(
      `
        with provider_directory as (
          select
            provider_ico,
            max(provider_name) as provider_name,
            max(region_name) as region_name,
            max(municipality) as municipality,
            max(provider_type) as provider_type
          from mart.health_provider_directory
          group by provider_ico
        ),
        health_candidates as (
          select
            f.provider_ico as node_ico,
            coalesce(f.provider_name, d.provider_name, f.provider_ico) as display_name,
            f.region_name,
            null::text as municipality,
            f.provider_type
          from mart.health_provider_finance_yearly f
          left join provider_directory d using (provider_ico)
          where f.reporting_year = $1
            and f.costs_czk > 0
            and (
              f.hospital_like
              or f.public_health_like
            )
          union all
          select
            b.entity_ico as node_ico,
            b.entity_name as display_name,
            b.region_name,
            null::text as municipality,
            b.entity_kind as provider_type
          from mart.health_mz_budget_entity_latest b
          where b.reporting_year = $1
            and b.entity_kind = 'hygiene_station'
            and coalesce(nullif(b.expenses_czk, 0), b.costs_czk) > 0
          union all
          select
            c.provider_ico as node_ico,
            coalesce(c.provider_name, d.provider_name, c.provider_ico) as display_name,
            c.region_name,
            c.municipality,
            c.provider_type
          from (
            select
              provider_ico,
              max(provider_name) as provider_name,
              max(provider_type) as provider_type,
              coalesce(max(provider_region_name), max(region_name)) as region_name,
              max(provider_municipality) as municipality
            from raw.health_provider_site
            where provider_ico is not null
              and provider_ico <> ''
              and care_form ilike '%ambul%'
              and (
                (
                  provider_type ilike 'Samostatná ordinace%'
                  or provider_type ilike 'Samost.%ordinace%'
                )
              )
            group by provider_ico
          ) c
          left join provider_directory d using (provider_ico)
        )
        ,
        directory_only as (
          select
            d.provider_ico as node_ico,
            d.provider_name as display_name,
            d.region_name,
            d.municipality,
            d.provider_type,
            false as available,
            'V atlasu zatím chybí finanční nebo výkonová data této instituce'::text as reason
          from provider_directory d
          where lower(coalesce(d.provider_name, '')) like lower('%' || $2 || '%')
            and not exists (
              select 1 from health_candidates h where h.node_ico = d.provider_ico
            )
        ),
        atlas_backed as (
          select
            node_ico,
            display_name,
            region_name,
            municipality,
            provider_type,
            true as available,
            null::text as reason
          from health_candidates
          where lower(coalesce(display_name, '')) like lower('%' || $2 || '%')
        )
        select distinct on (node_ico)
          node_ico,
          display_name,
          region_name,
          municipality,
          provider_type,
          available,
          reason
        from (
          select * from atlas_backed
          union all
          select * from directory_only
        ) hits
        order by node_ico, available desc, display_name
        limit $3
      `,
      [year, needle, perDomainLimit],
    ),
  ]);

  const schoolHits = schoolResults.map((row) => ({
    id: row.id,
    name: row.name,
    domain: 'school' as const,
    region: row.region ?? undefined,
    municipality: row.municipality ?? undefined,
    available: true,
  }));

  const healthHits = healthResult.rows.map((row) => ({
    id: `health:provider:${row.node_ico}`,
    name: String(row.display_name ?? row.node_ico),
    domain: 'health' as const,
    region: row.region_name == null ? undefined : String(row.region_name),
    municipality: row.municipality == null ? undefined : String(row.municipality),
    providerType: row.provider_type == null ? undefined : String(row.provider_type),
    available: Boolean(row.available),
    reason: row.reason == null ? undefined : String(row.reason),
  }));

  const merged = [];
  const maxLength = Math.max(schoolHits.length, healthHits.length);
  for (let index = 0; index < maxLength; index += 1) {
    if (healthHits[index]) merged.push(healthHits[index]);
    if (schoolHits[index]) merged.push(schoolHits[index]);
  }

  return merged.slice(0, limit);
}
