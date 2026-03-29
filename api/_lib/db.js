import pg from 'pg';

const { Pool } = pg;

const DEFAULT_LOCAL_URL = 'postgresql://app:app@127.0.0.1:55432/cz_school_sankey';

let pool;

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

export async function query(text, params = []) {
  return getPool().query(text, params);
}

export async function closePool() {
  if (pool) {
    await pool.end();
    pool = undefined;
  }
}
