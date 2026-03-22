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

const STATE_ID = 'state:cr';
const MV_MINISTRY_ID = 'security:ministry:mv';
const MV_POLICE_ID = 'security:police';
const MV_FIRE_RESCUE_ID = 'security:fire-rescue';
const MV_ADMIN_ID = 'security:mv-admin';
const MV_SOCIAL_ID = 'security:mv-social';

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

function createStateNode(capacity: number | null = null): AtlasNode {
  return {
    id: STATE_ID,
    name: 'Statni rozpocet',
    category: 'state',
    level: 0,
    ...(capacity ? { metadata: { capacity } } : {}),
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

function mvAmountByCode(rows: MvBudgetAggregate[], metricCode: string): number {
  return rows.find((row) => row.metricCode === metricCode)?.amount ?? 0;
}

function mvNationalCrimeCount(rows: MvPoliceCrimeAggregate[], indicatorName: string): number | null {
  const value = rows.find((row) => row.regionCode === 'CZ' && row.indicatorName === indicatorName)?.countValue ?? 0;
  return value > 0 ? value : null;
}

function buildMvPoliceRegionRows(rows: MvPoliceCrimeAggregate[]) {
  const registeredByRegion = new Map<string, { regionName: string; registeredCount: number; clearedCount: number }>();

  for (const row of rows) {
    if (row.regionCode === 'CZ' || row.crimeClassCode !== '0-999') continue;
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

function buildMvPoliceCrimeClassRows(rows: MvPoliceCrimeAggregate[], regionCode: string) {
  const grouped = new Map<string, { crimeClassName: string; registeredCount: number; clearedCount: number }>();

  for (const row of rows) {
    if (row.regionCode !== regionCode || row.crimeClassCode === '0-999') continue;
    const bucket = grouped.get(row.crimeClassCode) ?? {
      crimeClassName: row.crimeClassName,
      registeredCount: 0,
      clearedCount: 0,
    };
    if (row.indicatorName === 'Počet registrovaných skutků') {
      bucket.registeredCount = row.countValue;
    } else if (row.indicatorName === 'Počet objasněných skutků') {
      bucket.clearedCount = row.countValue;
    }
    grouped.set(row.crimeClassCode, bucket);
  }

  return [...grouped.entries()]
    .map(([crimeClassCode, row]) => ({
      crimeClassCode,
      crimeClassName: row.crimeClassName,
      registeredCount: row.registeredCount,
      clearedCount: row.clearedCount,
    }))
    .filter((row) => row.registeredCount > 0)
    .sort((a, b) => b.registeredCount - a.registeredCount || a.crimeClassName.localeCompare(b.crimeClassName, 'cs'));
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

function allocateAmount(totalAmount: number, partWeight: number, totalWeight: number): number {
  if (totalAmount <= 0 || partWeight <= 0 || totalWeight <= 0) return 0;
  return (totalAmount * partWeight) / totalWeight;
}

export async function getMvBudgetAggregates(year: number): Promise<MvBudgetAggregate[]> {
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

export async function getMvPoliceCrimeAggregates(year: number): Promise<MvPoliceCrimeAggregate[]> {
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

export async function getMvFireRescueActivityAggregates(year: number): Promise<MvFireRescueActivityAggregate[]> {
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

export function getMvTotal(rows: MvBudgetAggregate[]): number {
  return mvAmountByCode(rows, 'total_expenditure');
}

export function appendMvBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  mvBudgetRows: MvBudgetAggregate[],
  mvPoliceCrimeRows: MvPoliceCrimeAggregate[],
  mvFireRescueRows: MvFireRescueActivityAggregate[],
): void {
  const mvTotal = getMvTotal(mvBudgetRows);
  if (mvTotal <= 0) return;

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
}

export function buildMvRootGraph(
  year: number,
  mvBudgetRows: MvBudgetAggregate[],
  mvPoliceCrimeRows: MvPoliceCrimeAggregate[],
  mvFireRescueRows: MvFireRescueActivityAggregate[],
) {
  const mvTotal = getMvTotal(mvBudgetRows);
  if (mvTotal <= 0) return null;

  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];
  addNode(nodes, createStateNode(null));
  appendMvBranch(nodes, links, year, mvBudgetRows, mvPoliceCrimeRows, mvFireRescueRows);
  return { year, nodes, links };
}

export function buildMvPoliceRegionGraph(
  year: number,
  mvBudgetRows: MvBudgetAggregate[],
  mvPoliceCrimeRows: MvPoliceCrimeAggregate[],
) {
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
        drilldownAvailable: true,
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

export function buildMvPoliceCrimeClassGraph(
  year: number,
  mvBudgetRows: MvBudgetAggregate[],
  mvPoliceCrimeRows: MvPoliceCrimeAggregate[],
  regionCode: string,
) {
  const policeAmount = mvAmountByCode(mvBudgetRows, 'police');
  const nationalRegisteredCount = mvNationalCrimeCount(mvPoliceCrimeRows, 'Počet registrovaných skutků');
  const regions = buildMvPoliceRegionRows(mvPoliceCrimeRows);
  const region = regions.find((entry) => entry.regionCode === regionCode);
  if (policeAmount <= 0 || !nationalRegisteredCount || !region) return null;

  const crimeClasses = buildMvPoliceCrimeClassRows(mvPoliceCrimeRows, regionCode);
  if (!crimeClasses.length) return null;

  const regionAmount = allocateAmount(policeAmount, region.registeredCount, nationalRegisteredCount);
  const nodes: AtlasNode[] = [];
  const links: AtlasLink[] = [];

  addNode(nodes, createStateNode(null));
  addNode(nodes, createMvMinistryNode());
  addNode(nodes, createMvBranchNode(MV_POLICE_ID, 'Policie CR', nationalRegisteredCount, true));

  const regionId = `security:police:region:${region.regionCode}`;
  addNode(nodes, {
    id: regionId,
    name: region.regionName,
    category: 'region',
    level: 3,
    metadata: {
      capacity: region.registeredCount,
      clearedCount: region.clearedCount,
      drilldownAvailable: true,
      focus: 'security',
    },
  });

  links.push(
    makeLink(
      STATE_ID,
      MV_MINISTRY_ID,
      regionAmount,
      year,
      'state_to_mv_ministry',
      `Zúžený pohled na policejní výdaje v regionu ${region.regionName}`,
      'mv_budget_aggregates',
    ),
  );
  links.push(
    makeLink(
      MV_MINISTRY_ID,
      MV_POLICE_ID,
      regionAmount,
      year,
      'mv_budget_group',
      'Specifický ukazatel kapitoly 314: Výdaje Policie ČR',
      'mv_budget_aggregates',
    ),
  );
  links.push(
    makeLink(
      MV_POLICE_ID,
      regionId,
      regionAmount,
      year,
      'mv_police_region_allocated_cost',
      'Regionální rozdělení policejního rozpočtu je odhadnuto podle podílu registrovaných skutků z otevřeného datasetu KRI10',
      'mv_police_crime_aggregates',
    ),
  );

  for (const crimeClass of crimeClasses) {
    const classId = `security:police:crime-class:${regionCode}:${crimeClass.crimeClassCode}`;
    addNode(nodes, {
      id: classId,
      name: crimeClass.crimeClassName,
      category: 'other',
      level: 4,
      metadata: {
        capacity: crimeClass.registeredCount,
        clearedCount: crimeClass.clearedCount,
        drilldownAvailable: false,
        focus: 'security',
      },
    });
    links.push(
      makeLink(
        regionId,
        classId,
        allocateAmount(regionAmount, crimeClass.registeredCount, region.registeredCount),
        year,
        'mv_police_crime_class_allocated_cost',
        'Rozdělení regionální policejní částky podle struktury registrovaných skutků v otevřeném datasetu KRI10',
        'mv_police_crime_aggregates',
      ),
    );
  }

  return { year, nodes, links };
}

export function buildMvFireRescueRegionGraph(
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
