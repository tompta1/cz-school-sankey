import pg from 'pg';
import type { QueryResult, QueryResultRow } from 'pg';

const { Pool } = pg;

const DEFAULT_LOCAL_URL = 'postgresql://app:app@127.0.0.1:55432/cz_school_sankey';

let pool: pg.Pool | undefined;

function getConnectionConfig() {
  const connectionString = process.env.DATABASE_URL || DEFAULT_LOCAL_URL;
  const isLocal =
    connectionString.includes('127.0.0.1') ||
    connectionString.includes('localhost');

  return {
    connectionString,
    ssl: isLocal ? false : { rejectUnauthorized: false },
  };
}

export function getPool() {
  if (!pool) {
    pool = new Pool(getConnectionConfig());
  }
  return pool;
}

export async function query<Row extends QueryResultRow = QueryResultRow>(
  text: string,
  params: unknown[] = [],
): Promise<QueryResult<Row>> {
  return getPool().query<Row>(text, params);
}

export async function closePool() {
  if (pool) {
    await pool.end();
    pool = undefined;
  }
}
