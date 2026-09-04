import { describe, expect, it } from 'vitest';

import { reconcileOfficialChapterTotal } from './state-budget.js';

const rows = [
  {
    year: 2025,
    basis: 'budgeted',
    metricGroup: 'headline',
    metricCode: 'EXP_TOTAL',
    metricName: 'Total',
    amount: 80,
    sourceDataset: 'domain_budget',
  },
];

describe('reconcileOfficialChapterTotal', () => {
  it('replaces a budget total with a higher realized chapter total', () => {
    expect(reconcileOfficialChapterTotal(rows, 'EXP_TOTAL', 100)).toEqual([
      {
        ...rows[0],
        basis: 'realized',
        metricName: 'Total (skutecnost MF)',
        amount: 100,
        sourceDataset: 'school_state_budget',
      },
    ]);
  });

  it('retains the budget total when a lower official amount would overstate its branches', () => {
    expect(reconcileOfficialChapterTotal(rows, 'EXP_TOTAL', 70)).toBe(rows);
  });

  it('retains the budget total when official metadata is unavailable', () => {
    expect(reconcileOfficialChapterTotal(rows, 'EXP_TOTAL', null)).toBe(rows);
  });
});
