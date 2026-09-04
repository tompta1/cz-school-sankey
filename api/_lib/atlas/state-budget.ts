import { query } from '../db.js';

interface ReconciliableBudgetRow {
  metricCode: string;
  metricName: string;
  amount: number;
  sourceDataset: string;
  basis?: string;
}

export async function getOfficialStateChapterTotal(
  year: number,
  chapterCode: string,
): Promise<number | null> {
  const result = await query(
    `
      select amount_czk
      from raw.school_state_budget
      where reporting_year = $1
        and node_id = $2
        and flow_type = 'state_chapter_total'
      order by loaded_at desc
      limit 1
    `,
    [year, `chapter:${chapterCode}`],
  );
  const amount = Number(result.rows[0]?.amount_czk);
  return Number.isFinite(amount) && amount > 0 ? amount : null;
}

export function reconcileOfficialChapterTotal<T extends ReconciliableBudgetRow>(
  rows: T[],
  totalMetricCode: string,
  officialAmount: number | null,
): T[] {
  if (!officialAmount) return rows;

  const currentTotal = rows.find((row) => row.metricCode === totalMetricCode);
  if (!currentTotal || officialAmount < currentTotal.amount) return rows;

  return rows.map((row) =>
    row.metricCode === totalMetricCode
      ? {
          ...row,
          metricName: `${row.metricName} (skutecnost MF)`,
          amount: officialAmount,
          sourceDataset: 'school_state_budget',
          basis: 'realized',
        }
      : row,
  );
}
