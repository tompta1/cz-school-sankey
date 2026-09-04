import { describe, expect, it } from 'vitest';

import { getAgricultureTotal } from './agriculture.js';

describe('getAgricultureTotal', () => {
  it('keeps the Monitor total when recipient detail is unavailable', () => {
    const total = getAgricultureTotal(
      [
        {
          year: 2025,
          entityIco: '00020478',
          entityName: 'MZe',
          entityKind: 'ministry_admin',
          expenses: 80,
          costs: 100,
          sourceDataset: 'agriculture_budget_entities',
        },
        {
          year: 2025,
          entityIco: '48133981',
          entityName: 'SZIF',
          entityKind: 'paying_agency',
          expenses: 0,
          costs: 20,
          sourceDataset: 'agriculture_budget_entities',
        },
      ],
      [],
    );

    expect(total).toBe(100);
  });
});
