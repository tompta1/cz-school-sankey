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
    sourceDataset: 'school_state_budget',
    sql: `select coalesce(max(amount_czk), 0) as amount_czk from raw.school_state_budget where reporting_year = $1 and node_id = 'chapter:314' and flow_type = 'state_chapter_total'`,
  },
  {
    domain: 'justice',
    target: 'justice:ministry:msp',
    sourceKind: 'official chapter aggregate',
    sourceDataset: 'school_state_budget',
    sql: `select coalesce(max(amount_czk), 0) as amount_czk from raw.school_state_budget where reporting_year = $1 and node_id = 'chapter:336' and flow_type = 'state_chapter_total'`,
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
    sourceDataset: 'school_state_budget',
    sql: `select coalesce(max(amount_czk), 0) as amount_czk from raw.school_state_budget where reporting_year = $1 and node_id = 'chapter:317' and flow_type = 'state_chapter_total'`,
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

const mixedScopeChecks = [
  {
    domain: 'transport',
    target: 'transport:ministry:md',
    component: 'SFDI state-fund execution',
    sql: `
      with budget as (
        select coalesce(max(expenses_czk) filter (where entity_kind = 'infrastructure_fund'), 0) as amount_czk
        from mart.transport_budget_entity_latest
        where reporting_year = $1
      ), projects as (
        select coalesce(sum(paid_czk) filter (where paid_czk > 0), 0) as amount_czk
        from mart.transport_sfdi_project_latest
        where reporting_year = $1
      )
      select greatest(budget.amount_czk, projects.amount_czk) as amount_czk
      from budget cross join projects
    `,
  },
  {
    domain: 'agriculture',
    target: 'agriculture:ministry:mze',
    component: 'SZIF institution expenses',
    sql: `
      select coalesce(sum(case when expenses_czk > 0 then expenses_czk else costs_czk end), 0) as amount_czk
      from mart.agriculture_budget_entity_latest
      where reporting_year = $1 and entity_kind = 'intervention_fund_admin'
    `,
  },
  {
    domain: 'environment',
    target: 'environment:ministry:mzp',
    component: 'SFZP institution expenses',
    sql: `
      select coalesce(sum(case when expenses_czk > 0 then expenses_czk else costs_czk end), 0) as amount_czk
      from mart.environment_budget_entity_latest
      where reporting_year = $1 and entity_kind = 'environment_fund_admin'
    `,
  },
  {
    domain: 'health',
    target: 'health:zzs',
    component: 'regional emergency-service costs',
    sql: `
      select coalesce(sum(costs_czk) filter (where costs_czk > 0), 0) as amount_czk
      from mart.health_provider_finance_yearly
      where reporting_year = $1 and zzs_like
    `,
  },
];

const chapterChecks = [
  { domain: 'school', target: 'msmt', chapterCode: '333', strictYears: [], scope: 'partial: direct school allocations only' },
  { domain: 'social', target: 'social:ministry:mpsv', chapterCode: '313', strictYears: [2024, 2025], scope: 'realized chapter total' },
  { domain: 'internal', target: 'security:ministry:mv', chapterCode: '314', strictYears: [2024, 2025], scope: 'realized root; budget-based branch classification' },
  { domain: 'justice', target: 'justice:ministry:msp', chapterCode: '336', strictYears: [2024, 2025], scope: 'realized root; budget-based branch classification' },
  { domain: 'transport', target: 'transport:ministry:md', chapterCode: '327', strictYears: [], scope: 'mixed: MD plus SFDI' },
  { domain: 'regions', target: 'mmr:ministry:mmr', chapterCode: '317', strictYears: [2024, 2025], scope: 'realized root; budget-based branch classification' },
  { domain: 'business', target: 'mpo:ministry:mpo', chapterCode: '322', strictYears: [], scope: 'Monitor institution scope' },
  { domain: 'culture', target: 'mk:ministry:mk', chapterCode: '334', strictYears: [2024, 2025], scope: 'realized chapter total' },
  { domain: 'foreign', target: 'mzv:ministry:mzv', chapterCode: '306', strictYears: [], scope: 'Monitor ministry-institution scope' },
  { domain: 'defense', target: 'defense:ministry:mo', chapterCode: '307', strictYears: [2024, 2025], scope: 'realized chapter total' },
  { domain: 'finance', target: 'mf:ministry:mf', chapterCode: '312', strictYears: [], scope: 'selected Monitor institutions' },
  { domain: 'agriculture', target: 'agriculture:ministry:mze', chapterCode: '329', strictYears: [], scope: 'mixed: MZe plus SZIF' },
  { domain: 'environment', target: 'environment:ministry:mzp', chapterCode: '315', strictYears: [], scope: 'mixed: MZP plus SFZP' },
  { domain: 'health', target: 'health:ministry:mzcr', chapterCode: '335', strictYears: [], scope: 'Monitor ministry-institution scope' },
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
  const chapterRows = [];
  const scopeRows = [];
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

      for (const chapterCheck of chapterChecks) {
        const result = await client.query(
          `
            select coalesce(max(amount_czk), 0) as amount_czk
            from raw.school_state_budget
            where reporting_year = $1
              and node_id = $2
              and flow_type = 'state_chapter_total'
          `,
          [year, `chapter:${chapterCheck.chapterCode}`],
        );
        const official = Number(result.rows[0]?.amount_czk ?? 0);
        const atlas = Number(linksByTarget.get(chapterCheck.target)?.amountCzk ?? 0);
        const strict = chapterCheck.strictYears.includes(year);
        const matches = official > 0 && amountsMatch(atlas, official);
        const status = strict ? (matches ? 'pass' : 'FAIL') : matches ? 'pass (scope review)' : 'review';
        chapterRows.push({
          year,
          ...chapterCheck,
          atlas,
          official,
          delta: atlas - official,
          coverage: official > 0 ? atlas / official : null,
          status,
        });
        if (strict && !matches) {
          errors.push(
            `${year} ${chapterCheck.domain} chapter ${chapterCheck.chapterCode}: ` +
            `Atlas ${formatAmount(atlas)} vs MF ${formatAmount(official)} CZK`,
          );
        }
      }

      const envelopeResult = await client.query(
        `
          select coalesce(max(amount_czk), 0) as amount_czk
          from raw.school_state_budget
          where reporting_year = $1
            and flow_type = 'state_budget_total'
        `,
        [year],
      );
      const envelope = Number(envelopeResult.rows[0]?.amount_czk ?? 0);
      const totalOutflow = stateLinks.reduce((sum, link) => sum + Number(link.amountCzk ?? 0), 0);
      const totalInflow = (overview.links || [])
        .filter((link) => link.target === 'state:cr')
        .reduce((sum, link) => sum + Number(link.amountCzk ?? 0), 0);
      const residual = Number(linksByTarget.get('state:other')?.amountCzk ?? 0);
      const mappedWithoutResidual = totalOutflow - residual;
      const expectedResidual = Math.max(envelope - mappedWithoutResidual, 0);

      const envelopeStatus = envelope > 0 && amountsMatch(totalOutflow, envelope) ? 'pass' : 'FAIL';
      rows.push({
        year,
        domain: 'state envelope (outflow)',
        target: 'state:cr',
        sourceKind: 'official final-account expenditure',
        sourceDataset: 'school_state_budget',
        actual: totalOutflow,
        expected: envelope,
        delta: totalOutflow - envelope,
        status: envelopeStatus,
      });
      if (envelopeStatus === 'FAIL') {
        errors.push(`${year} state outflow: Atlas ${formatAmount(totalOutflow)} vs official ${formatAmount(envelope)} CZK`);
      }

      const inflowStatus = envelope > 0 && amountsMatch(totalInflow, envelope) ? 'pass' : 'FAIL';
      rows.push({
        year,
        domain: 'state envelope (inflow)',
        target: 'state:cr',
        sourceKind: 'official final-account revenue plus deficit',
        sourceDataset: 'school_state_budget',
        actual: totalInflow,
        expected: envelope,
        delta: totalInflow - envelope,
        status: inflowStatus,
      });
      if (inflowStatus === 'FAIL') {
        errors.push(`${year} state inflow: Atlas ${formatAmount(totalInflow)} vs official ${formatAmount(envelope)} CZK`);
      }

      const residualStatus = envelope > 0 && amountsMatch(residual, expectedResidual) ? 'pass' : 'FAIL';
      rows.push({
        year,
        domain: 'state residual',
        target: 'state:other',
        sourceKind: 'balancing residual, not an observed chapter total',
        sourceDataset: 'atlas.inferred',
        actual: residual,
        expected: expectedResidual,
        delta: residual - expectedResidual,
        status: residualStatus,
      });
      if (residualStatus === 'FAIL') {
        errors.push(`${year} state residual: Atlas ${formatAmount(residual)} vs expected ${formatAmount(expectedResidual)} CZK`);
      }

      for (const scopeCheck of mixedScopeChecks) {
        const result = await client.query(scopeCheck.sql, [year]);
        const componentAmount = Number(result.rows[0]?.amount_czk ?? 0);
        const rootAmount = Number(linksByTarget.get(scopeCheck.target)?.amountCzk ?? 0);
        scopeRows.push({
          year,
          ...scopeCheck,
          rootAmount,
          componentAmount,
          share: rootAmount > 0 ? componentAmount / rootAmount : null,
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

  console.log();
  console.log('### Independent MF Chapter Reconciliation');
  console.log();
  console.log('| Year | Domain | Chapter | Atlas CZK | MF realized CZK | Delta CZK | Atlas / MF | Scope | Status |');
  console.log('|---:|---|---:|---:|---:|---:|---:|---|---|');
  for (const row of chapterRows) {
    const coverage = row.coverage == null ? 'n/a' : `${(row.coverage * 100).toFixed(1)}%`;
    console.log(`| ${row.year} | ${row.domain} | ${row.chapterCode} | ${formatAmount(row.atlas)} | ${formatAmount(row.official)} | ${formatAmount(row.delta)} | ${coverage} | ${row.scope} | ${row.status} |`);
  }

  console.log();
  console.log('### Mixed-Scope Diagnostics');
  console.log();
  console.log('These components are visible public spending, but are not clean state-budget chapter amounts and may overlap transfers already recorded in a chapter.');
  console.log();
  console.log('| Year | Domain | Atlas root CZK | Mixed-scope component CZK | Share of root | Component |');
  console.log('|---:|---|---:|---:|---:|---|');
  for (const row of scopeRows) {
    const share = row.share == null ? 'n/a' : `${(row.share * 100).toFixed(1)}%`;
    console.log(`| ${row.year} | ${row.domain} | ${formatAmount(row.rootAmount)} | ${formatAmount(row.componentAmount)} | ${share} | ${row.component} |`);
  }

  if (errors.length) {
    console.log();
    console.log('### Reconciliation Errors');
    for (const error of errors) console.log(`- ${error}`);
    process.exitCode = 1;
    return;
  }

  console.log();
  console.log('Top-level source and official state-envelope reconciliation passed. Mixed-scope rows remain explicit diagnostics.');
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
