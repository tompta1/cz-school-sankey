import { describe, expect, it } from 'vitest';

import { appendJusticeBranch } from './justice.js';
import { appendMmrBranch } from './mmr.js';
import { appendMvBranch } from './mv.js';

function amountByFlow(links: Array<{ flowType: string; amountCzk: number }>, flowType: string): number {
  return links
    .filter((link) => link.flowType === flowType)
    .reduce((sum, link) => sum + link.amountCzk, 0);
}

describe('official chapter reconciliation branches', () => {
  it('balances the MV realized root against its budget classification', () => {
    const nodes: Parameters<typeof appendMvBranch>[0] = [];
    const links: Parameters<typeof appendMvBranch>[1] = [];
    const budgetRows = [
      ['total_expenditure', 100, 'school_state_budget'],
      ['police', 30, 'mv_budget_aggregates'],
      ['fire_rescue', 20, 'mv_budget_aggregates'],
      ['ministry_admin', 10, 'mv_budget_aggregates'],
      ['pensions', 15, 'mv_budget_aggregates'],
    ].map(([metricCode, amount, sourceDataset]) => ({
      year: 2025,
      basis: 'realized',
      metricGroup: 'budget',
      metricCode: String(metricCode),
      metricName: String(metricCode),
      amount: Number(amount),
      sourceDataset: String(sourceDataset),
    }));

    appendMvBranch(nodes, links, 2025, budgetRows, [], []);

    expect(amountByFlow(links, 'state_to_mv_ministry')).toBe(100);
    expect(amountByFlow(links, 'mv_budget_group')).toBe(100);
    expect(links.find((link) => link.target === 'security:mv-reconciliation')).toMatchObject({
      amountCzk: 25,
      sourceDataset: 'school_state_budget',
    });
  });

  it('balances the justice realized root against its budget classification', () => {
    const nodes: Parameters<typeof appendJusticeBranch>[0] = [];
    const links: Parameters<typeof appendJusticeBranch>[1] = [];
    const budgetRows = [
      ['total_expenditure', 100, 'school_state_budget'],
      ['justice_block', 50, 'justice_budget_aggregates'],
      ['prison_service', 20, 'justice_budget_aggregates'],
      ['social_and_prevention', 10, 'justice_budget_aggregates'],
    ].map(([metricCode, amount, sourceDataset]) => ({
      year: 2025,
      basis: 'realized',
      metricGroup: 'budget',
      metricCode: String(metricCode),
      metricName: String(metricCode),
      amount: Number(amount),
      sourceDataset: String(sourceDataset),
    }));

    appendJusticeBranch(nodes, links, 2025, budgetRows, []);

    expect(amountByFlow(links, 'state_to_justice_ministry')).toBe(100);
    expect(amountByFlow(links, 'justice_branch_cost')).toBe(100);
    expect(links.find((link) => link.target === 'justice:reconciliation')).toMatchObject({
      amountCzk: 20,
      sourceDataset: 'school_state_budget',
    });
  });

  it('balances the MMR realized root against its budget classification', () => {
    const nodes: Parameters<typeof appendMmrBranch>[0] = [];
    const links: Parameters<typeof appendMmrBranch>[1] = [];
    const budgetRows = [
      ['EXP_TOTAL', 100, 'school_state_budget'],
      ['REGIONAL_SUPPORT', 20, 'mmr_budget_aggregates'],
      ['HOUSING_SUPPORT', 15, 'mmr_budget_aggregates'],
      ['PLANNING', 10, 'mmr_budget_aggregates'],
      ['OTHER', 25, 'mmr_budget_aggregates'],
    ].map(([metricCode, amount, sourceDataset]) => ({
      year: 2025,
      metricCode: String(metricCode),
      metricName: String(metricCode),
      metricGroup: 'budget',
      amount: Number(amount),
      sourceDataset: String(sourceDataset),
    }));

    appendMmrBranch(nodes, links, 2025, budgetRows, []);

    expect(amountByFlow(links, 'state_to_mmr_resort')).toBe(100);
    expect(
      links
        .filter((link) => link.source === 'mmr:ministry:mmr')
        .reduce((sum, link) => sum + link.amountCzk, 0),
    ).toBe(100);
    expect(links.find((link) => link.target === 'mmr:branch:reconciliation')).toMatchObject({
      amountCzk: 30,
      sourceDataset: 'school_state_budget',
    });
  });
});
