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

const STATE_ID = 'state:cr';
const SOCIAL_MINISTRY_ID = 'social:ministry:mpsv';

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

function createSocialMinistryNode(): AtlasNode {
  return {
    id: SOCIAL_MINISTRY_ID,
    name: 'Ministerstvo prace a socialnich veci',
    category: 'ministry',
    level: 1,
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

function socialAmountByCode(rows: SocialMpsvAggregate[], metricCode: string): number {
  return rows.find((row) => row.metricCode === metricCode)?.amount ?? 0;
}

function socialRecipientCountByCode(rows: SocialRecipientMetric[], metricCode: string): number | null {
  const value = rows.find((row) => row.metricCode === metricCode)?.recipientCount ?? 0;
  return value > 0 ? value : null;
}

export async function getSocialMpsvAggregates(year: number): Promise<SocialMpsvAggregate[]> {
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

export async function getSocialRecipientMetrics(year: number): Promise<SocialRecipientMetric[]> {
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

export function getSocialTotal(rows: SocialMpsvAggregate[]): number {
  return socialAmountByCode(rows, 'total_expenditure');
}

export function appendSocialBranch(
  nodes: AtlasNode[],
  links: AtlasLink[],
  year: number,
  socialRows: SocialMpsvAggregate[],
  socialRecipientMetrics: SocialRecipientMetric[],
): void {
  const socialTotal = getSocialTotal(socialRows);
  if (socialTotal <= 0) return;

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
      note: 'Aktivní politika zaměstnanosti, podpora zaměstnávání OZP a insolvence',
    },
    {
      id: 'social:benefit:material-need',
      name: 'Hmotna nouze',
      amount: materialNeed,
      note: 'Dávky pomoci v hmotné nouzi',
    },
    {
      id: 'social:benefit:residual',
      name: 'Sprava a ostatni socialni vydaje',
      amount: residual,
      note: 'Zbytkové správní a ostatní sociální výdaje v kapitole MPSV',
    },
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
        bucket.note,
        'social_mpsv_aggregates',
      ),
    );
  }
}
