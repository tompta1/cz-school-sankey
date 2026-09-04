#!/usr/bin/env node

import pg from 'pg';

const { Client } = pg;
const databaseUrl = process.env.DATABASE_URL;
const baseUrl = (process.env.ATLAS_API_BASE_URL || 'https://cz-school-sankey.vercel.app').replace(/\/$/, '');
const years = [...new Set((process.env.DQ_YEARS || '2024 2025').split(/\s+/).filter(Boolean).map(Number))];
const absoluteToleranceCzk = Number(process.env.DQ_ABSOLUTE_TOLERANCE_CZK || 1);
const relativeTolerance = Number(process.env.DQ_RELATIVE_TOLERANCE || 0.000001);

const checks = [
  {
    domain: 'school',
    target: 'msmt',
    sourceKind: 'official allocation rollup',
    sourceDataset: 'core.financial_flow',
    sql: `
      select coalesce(sum(ff.amount_czk), 0) as amount_czk
      from core.financial_flow ff
      join core.reporting_period rp on rp.reporting_period_id = ff.reporting_period_id
      join core.organization target_o on target_o.organization_id = ff.target_organization_id
      where rp.domain_code = 'school'
        and rp.calendar_year = $1
        and ff.flow_type = 'state_to_ministry'
        and target_o.attributes ->> 'node_id' = 'msmt'
    `,
  },
  {
    domain: 'social',
    target: 'social:ministry:mpsv',
    sourceKind: 'official chapter aggregate',
    sourceDataset: 'social_mpsv_aggregates',
    sql: `select coalesce(max(amount_czk), 0) as amount_czk from mart.social_mpsv_aggregate_latest where reporting_year = $1 and metric_code = 'total_expenditure'`,
  },
  {
    domain: 'internal',
    target: 'security:ministry:mv',
    sourceKind: 'official chapter aggregate',
    sourceDataset: 'mv_budget_aggregates',
    sql: `select coalesce(max(amount_czk), 0) as amount_czk from mart.mv_budget_aggregate_latest where reporting_year = $1 and metric_code = 'total_expenditure'`,
  },
  {
    domain: 'justice',
    target: 'justice:ministry:msp',
    sourceKind: 'official chapter aggregate',
    sourceDataset: 'justice_budget_aggregates',
    sql: `select coalesce(max(amount_czk), 0) as amount_czk from mart.justice_budget_aggregate_latest where reporting_year = $1 and metric_code = 'total_expenditure'`,
  },
  {
    domain: 'transport',
    target: 'transport:ministry:md',
    sourceKind: 'Monitor plus SFDI execution',
    sourceDataset: 'transport_budget_entities',
    sql: `
      with budget as (
        select
          coalesce(max(expenses_czk) filter (where entity_kind = 'ministry_admin'), 0) as md_czk,
          coalesce(max(expenses_czk) filter (where entity_kind = 'infrastructure_fund'), 0) as sfdi_czk
        from mart.transport_budget_entity_latest
        where reporting_year = $1
      ), projects as (
        select coalesce(sum(paid_czk) filter (where paid_czk > 0), 0) as sfdi_project_czk
        from mart.transport_sfdi_project_latest
        where reporting_year = $1
      )
      select budget.md_czk + greatest(budget.sfdi_czk, projects.sfdi_project_czk) as amount_czk
      from budget cross join projects
    `,
  },
  {
    domain: 'regions',
    target: 'mmr:ministry:mmr',
    sourceKind: 'official chapter aggregate',
    sourceDataset: 'mmr_budget_aggregates',
    sql: `select coalesce(max(amount_czk), 0) as amount_czk from mart.mmr_budget_aggregate_latest where reporting_year = $1 and metric_code = 'EXP_TOTAL'`,
  },
  ...[
    ['business', 'mpo:ministry:mpo', 'mpo_budget_entity_latest', 'mpo_budget_entities'],
    ['culture', 'mk:ministry:mk', 'mk_budget_entity_latest', 'mk_budget_entities'],
    ['foreign', 'mzv:ministry:mzv', 'mzv_budget_entity_latest', 'mzv_budget_entities'],
    ['defense', 'defense:ministry:mo', 'mo_budget_entity_latest', 'mo_budget_entities'],
    ['finance', 'mf:ministry:mf', 'mf_budget_entity_latest', 'mf_budget_entities'],
    ['environment', 'environment:ministry:mzp', 'environment_budget_entity_latest', 'environment_budget_entities'],
  ].map(([domain, target, view, sourceDataset]) => ({
    domain,
    target,
    sourceKind: 'Monitor aggregate',
    sourceDataset,
    sql: `select coalesce(sum(case when expenses_czk > 0 then expenses_czk else costs_czk end), 0) as amount_czk from mart.${view} where reporting_year = $1`,
  })),
  {
    domain: 'agriculture',
    target: 'agriculture:ministry:mze',
    sourceKind: 'Monitor aggregate',
    sourceDataset: 'agriculture_budget_entities',
    sql: `select coalesce(sum(case when expenses_czk > 0 then expenses_czk else costs_czk end), 0) as amount_czk from mart.agriculture_budget_entity_latest where reporting_year = $1`,
  },
  {
    domain: 'health',
    target: 'health:ministry:mzcr',
    sourceKind: 'Monitor chapter aggregate',
    sourceDataset: 'health_mz_budget_entities',
    sql: `select coalesce(max(coalesce(nullif(expenses_czk, 0), costs_czk)), 0) as amount_czk from mart.health_mz_budget_entity_latest where reporting_year = $1 and entity_kind = 'ministry_chapter_total'`,
  },
];

function formatAmount(value) {
  if (Math.abs(value) < 0.005) return '0';
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(value);
}

export function amountsMatch(actual, expected) {
  const delta = Math.abs(actual - expected);
  return delta <= Math.max(absoluteToleranceCzk, Math.abs(expected) * relativeTolerance);
}

async function fetchOverview(year) {
  const response = await fetch(`${baseUrl}/api/atlas/overview?year=${year}&metric=cost`, {
    headers: { accept: 'application/json' },
  });
  if (!response.ok) {
    throw new Error(`Atlas overview ${year} returned HTTP ${response.status}`);
  }
  return response.json();
}

async function main() {
  if (!databaseUrl) throw new Error('Missing DATABASE_URL');
  if (!years.length || years.some((year) => !Number.isInteger(year))) {
    throw new Error('DQ_YEARS must contain one or more four-digit years');
  }

  const localDatabase = databaseUrl.includes('localhost') || databaseUrl.includes('127.0.0.1');
  const connectionUrl = new URL(databaseUrl);
  if (!localDatabase) connectionUrl.searchParams.set('sslmode', 'verify-full');
  const client = new Client({
    connectionString: connectionUrl.toString(),
    ssl: localDatabase ? false : { rejectUnauthorized: true },
    connectionTimeoutMillis: 10_000,
  });
  await client.connect();

  const errors = [];
  const rows = [];
  try {
    for (const year of years) {
      const overview = await fetchOverview(year);
      const stateLinks = (overview.links || []).filter((link) => link.source === 'state:cr');
      const linksByTarget = new Map(stateLinks.map((link) => [link.target, link]));

      for (const check of checks) {
        const result = await client.query(check.sql, [year]);
        const expected = Number(result.rows[0]?.amount_czk ?? 0);
        const link = linksByTarget.get(check.target);
        const actual = Number(link?.amountCzk ?? 0);
        const available = expected > 0 || Boolean(link);
        const status = !available ? 'not available' : amountsMatch(actual, expected) ? 'pass' : 'FAIL';
        const delta = actual - expected;

        rows.push({ year, ...check, actual, expected, delta, status });
        if (status === 'FAIL') {
          errors.push(`${year} ${check.domain}: Atlas ${formatAmount(actual)} vs source ${formatAmount(expected)} CZK`);
        }
      }

      const mappedTargets = new Set(checks.map((check) => check.target));
      const envelopeResult = await client.query(
        `
          select coalesce(sum(ff.amount_czk), 0) as amount_czk
          from core.financial_flow ff
          join core.reporting_period rp on rp.reporting_period_id = ff.reporting_period_id
          where rp.domain_code = 'school'
            and rp.calendar_year = $1
            and ff.flow_type in ('state_to_ministry', 'state_to_other')
        `,
        [year],
      );
      const envelope = Number(envelopeResult.rows[0]?.amount_czk ?? 0);
      const mappedOutflow = stateLinks
        .filter((link) => link.target === 'state:other' || mappedTargets.has(link.target))
        .reduce((sum, link) => sum + Number(link.amountCzk ?? 0), 0);
      const envelopeDelta = mappedOutflow - envelope;
      rows.push({
        year,
        domain: 'state envelope',
        target: 'state:cr total mapped outflow',
        sourceKind: 'inferred all-state budget envelope',
        sourceDataset: 'core.financial_flow',
        actual: mappedOutflow,
        expected: envelope,
        delta: envelopeDelta,
        status: amountsMatch(mappedOutflow, envelope) ? 'pass' : 'warning',
      });

      for (const link of stateLinks) {
        if (link.target === 'state:other' || mappedTargets.has(link.target)) continue;
        rows.push({
          year,
          domain: 'unmapped',
          target: link.target,
          sourceKind: 'mixed/synthetic branch',
          sourceDataset: link.sourceDataset || 'unknown',
          actual: Number(link.amountCzk ?? 0),
          expected: null,
          delta: null,
          status: 'review',
        });
      }
    }
  } finally {
    await client.end();
  }

  console.log('## Atlas Top-Level Reconciliation');
  console.log();
  console.log(`API: \`${baseUrl}\``);
  console.log(`Tolerance: max of ${formatAmount(absoluteToleranceCzk)} CZK or ${(relativeTolerance * 100).toFixed(4)}%`);
  console.log();
  console.log('| Year | Domain | Atlas target | Atlas CZK | Source CZK | Delta CZK | Source basis | Status |');
  console.log('|---:|---|---|---:|---:|---:|---|---|');
  for (const row of rows) {
    console.log(`| ${row.year} | ${row.domain} | ${row.target} | ${formatAmount(row.actual)} | ${row.expected == null ? 'n/a' : formatAmount(row.expected)} | ${row.delta == null ? 'n/a' : formatAmount(row.delta)} | ${row.sourceKind} (${row.sourceDataset}) | ${row.status} |`);
  }

  if (errors.length) {
    console.log();
    console.log('### Reconciliation Errors');
    for (const error of errors) console.log(`- ${error}`);
    process.exitCode = 1;
    return;
  }

  console.log();
  console.log('Top-level source reconciliation passed. Review-only rows are mixed or synthetic funding branches, not Monitor chapter totals.');
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
