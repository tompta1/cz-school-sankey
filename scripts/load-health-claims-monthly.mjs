import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import zlib from 'node:zlib';
import { fileURLToPath } from 'node:url';

import { getPool, closePool } from '../api/_lib/db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const RAW_ROOT = path.join(ROOT, 'etl', 'data', 'raw', 'health');

const SOURCES = {
  uzis: ['UZIS', 'https://datanzis.uzis.gov.cz'],
};

const DATASETS = {
  nrhzs_claims_payer: {
    sourceCode: 'uzis',
    targetTable: 'raw.health_claims_payer_monthly',
    aggregate: aggregatePayerClaims,
  },
  nrhzs_claims_provider_specialty: {
    sourceCode: 'uzis',
    targetTable: 'raw.health_claims_provider_monthly',
    aggregate: aggregateProviderClaims,
  },
  nrhzs_claims_provider_ico: {
    sourceCode: 'uzis',
    targetTable: 'raw.health_claims_provider_ico_yearly',
    aggregate: aggregateProviderClaimsByIco,
  },
};

const HOSPITAL_FACILITY_TYPES = new Set([
  'Nemocnice',
  'Fakultní nemocnice',
  'Specializovaná nemocnice',
  'Psychiatrická nemocnice',
  'Psychiatrická léčebna',
  'Nemocnice následné péče',
  'Léčebna pro dlouhodobě nemocné (LDN)',
]);

const PUBLIC_HEALTH_FACILITY_TYPES = new Set([
  'Zdravotní ústav',
  'Státní zdravotní ústav',
  'Další zařízení hygienické služby',
]);

function latestDataPath(datasetCode) {
  const datasetDir = path.join(RAW_ROOT, datasetCode);
  if (!fs.existsSync(datasetDir)) return null;
  const candidates = fs
    .readdirSync(datasetDir)
    .filter((name) => !name.endsWith('.download.json') && !name.endsWith('-metadata.json') && !name.endsWith('.json'))
    .map((name) => path.join(datasetDir, name))
    .filter((fullPath) => fs.statSync(fullPath).isFile())
    .sort();
  return candidates.at(-1) ?? null;
}

function snapshotLabelFor(filePath) {
  return path.basename(filePath).split('__', 1)[0];
}

function sidecarFor(filePath) {
  const sidecarPath = `${filePath}.download.json`;
  if (!fs.existsSync(sidecarPath)) return {};
  return JSON.parse(fs.readFileSync(sidecarPath, 'utf8'));
}

function metadataPathFor(filePath) {
  const snapshot = snapshotLabelFor(filePath);
  const dir = path.dirname(filePath);
  const match = fs
    .readdirSync(dir)
    .filter((name) => name.startsWith(`${snapshot}__`) && name.endsWith('-metadata.json'))
    .sort()
    .at(-1);
  return match ? path.join(dir, match) : null;
}

function createLineReader(filePath) {
  const source = fs.createReadStream(filePath);
  const input = filePath.endsWith('.gz') ? source.pipe(zlib.createGunzip()) : source;
  return readline.createInterface({ input, crlfDelay: Infinity });
}

function parseCsvLine(line) {
  const values = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          current += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else if (ch === ',') {
      values.push(current);
      current = '';
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch !== '\r') {
      current += ch;
    }
  }

  values.push(current);
  return values;
}

function normalizeHeaders(values) {
  return values.map((value, index) => {
    const normalized = value.trim();
    return index === 0 ? normalized.replace(/^\uFEFF/, '') : normalized;
  });
}

function normalizeText(value) {
  return String(value ?? '').trim();
}

function lowerText(value) {
  return normalizeText(value).toLowerCase();
}

function latestProviderSitesPath() {
  return latestDataPath('nrpzs_provider_sites');
}

function normalizeIco(value) {
  const digits = normalizeText(value).replace(/\D/g, '');
  return digits ? digits.padStart(8, '0') : '';
}

async function buildFocusedProviderIcoMap() {
  const providerPath = latestProviderSitesPath();
  if (!providerPath) {
    throw new Error('Missing provider sites source file for provider claims filtering');
  }

  const rl = createLineReader(providerPath);
  let headers = null;
  const focusMap = new Map();
  let matchedRows = 0;

  for await (const line of rl) {
    if (!line) continue;
    const values = parseCsvLine(line);
    if (!headers) {
      headers = normalizeHeaders(values);
      continue;
    }

    const row = Object.fromEntries(headers.map((header, index) => [header, values[index] ?? '']));
    const providerIco = normalizeIco(row.poskytovatel_ICO);
    if (!providerIco) continue;

    const text = [
      lowerText(row.ZZ_druh_nazev),
      lowerText(row.ZZ_druh_nazev_sekundarni),
      lowerText(row.ZZ_nazev),
      lowerText(row.poskytovatel_druh),
      lowerText(row.poskytovatel_nazev),
    ].join(' | ');
    const careField = lowerText(row.ZZ_obor_pece);

    const isHospital =
      HOSPITAL_FACILITY_TYPES.has(normalizeText(row.ZZ_druh_nazev)) ||
      text.includes('nemocnic') ||
      text.includes('léčebna pro dlouhodobě nemocné') ||
      text.includes(' ldn');
    const isPublicHealth =
      PUBLIC_HEALTH_FACILITY_TYPES.has(normalizeText(row.ZZ_druh_nazev)) ||
      PUBLIC_HEALTH_FACILITY_TYPES.has(normalizeText(row.poskytovatel_druh)) ||
      careField.includes('hygiena a epidemiologie') ||
      text.includes('hygienická stanice') ||
      text.includes('zdravotní ústav');

    if (!isHospital && !isPublicHealth) continue;

    matchedRows += 1;
    const tags = focusMap.get(providerIco) ?? new Set();
    if (isHospital) tags.add('hospital');
    if (isPublicHealth) tags.add('public_health');
    focusMap.set(providerIco, tags);
  }

  return {
    providerPath,
    matchedRows,
    focusMap: new Map(
      Array.from(focusMap, ([providerIco, tags]) => [providerIco, Array.from(tags).sort()]),
    ),
  };
}

async function aggregatePayerClaims(filePath) {
  const rl = createLineReader(filePath);
  let headers = null;
  const aggregates = new Map();
  let skippedRows = 0;

  for await (const line of rl) {
    if (!line) continue;
    const values = parseCsvLine(line);
    if (!headers) {
      headers = normalizeHeaders(values);
      continue;
    }
    const row = Object.fromEntries(headers.map((header, index) => [header, values[index] ?? '']));
    const year = Number(row.rok);
    const month = Number(row.mesic);
    const payerCode = row.pojistovna;
    const quantity = Number(row.mnozstvi);
    if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(quantity) || !payerCode) {
      skippedRows += 1;
      continue;
    }
    const key = `${year}\t${month}\t${payerCode}`;
    aggregates.set(key, (aggregates.get(key) ?? 0) + quantity);
  }

  return {
    rows: Array.from(aggregates, ([key, totalQuantity]) => {
      const [year, month, payerCode] = key.split('\t');
      return {
        reporting_year: Number(year),
        reporting_month: Number(month),
        payer_code: payerCode,
        total_quantity: totalQuantity,
      };
    }),
    skippedRows,
  };
}

async function aggregateProviderClaims(filePath) {
  throw new Error(
    'nrhzs_claims_provider_specialty uses ICZ, but the local provider directory has no ICZ crosswalk. Use nrhzs_claims_provider_ico for hospital/public-health loading.',
  );
}

async function aggregateProviderClaimsByIco(filePath) {
  const { providerPath, matchedRows, focusMap } = await buildFocusedProviderIcoMap();
  const rl = createLineReader(filePath);
  let headers = null;
  const aggregates = new Map();
  let skippedRows = 0;
  let filteredOutRows = 0;

  for await (const line of rl) {
    if (!line) continue;
    const values = parseCsvLine(line);
    if (!headers) {
      headers = normalizeHeaders(values);
      continue;
    }
    const row = Object.fromEntries(headers.map((header, index) => [header, values[index] ?? '']));
    const year = Number(row.rok);
    const providerIco = normalizeIco(row.ICO);
    const quantity = Number(row.mnozstvi);
    const patientCount = Number(row.pocet_pacientu);
    const contactCount = Number(row.pocet_kontaktu);
    if (
      !Number.isFinite(year) ||
      !providerIco ||
      !Number.isFinite(quantity) ||
      !Number.isFinite(patientCount) ||
      !Number.isFinite(contactCount)
    ) {
      skippedRows += 1;
      continue;
    }
    const tags = focusMap.get(providerIco);
    if (!tags?.length) {
      filteredOutRows += 1;
      continue;
    }
    const key = `${year}\t${providerIco}`;
    const aggregate = aggregates.get(key) ?? { totalQuantity: 0, patientCount: 0, contactCount: 0 };
    aggregate.totalQuantity += quantity;
    aggregate.patientCount += patientCount;
    aggregate.contactCount += contactCount;
    aggregates.set(key, aggregate);
  }

  return {
    rows: Array.from(aggregates, ([key, aggregate]) => {
      const [year, providerIco] = key.split('\t');
      return {
        reporting_year: Number(year),
        provider_ico: providerIco,
        total_quantity: aggregate.totalQuantity,
        patient_count: aggregate.patientCount,
        contact_count: aggregate.contactCount,
        payload: { focus_tags: focusMap.get(providerIco) ?? [] },
      };
    }),
    skippedRows,
    filteredOutRows,
    metadata: {
      focus_scope: 'hospital_public_health',
      focus_provider_ico_count: focusMap.size,
      focus_provider_site_matches: matchedRows,
      focus_provider_sites_path: path.relative(ROOT, providerPath),
    },
  };
}

async function upsertSourceSystem(client, code) {
  const [name, baseUrl] = SOURCES[code];
  const result = await client.query(
    `
      insert into meta.source_system (code, name, base_url)
      values ($1, $2, $3)
      on conflict (code) do update
        set name = excluded.name,
            base_url = excluded.base_url
      returning source_system_id
    `,
    [code, name, baseUrl],
  );
  return Number(result.rows[0].source_system_id);
}

async function upsertDatasetRelease(client, { sourceSystemId, datasetCode, filePath, rowCount = 0 }) {
  const snapshotLabel = snapshotLabelFor(filePath);
  const downloadSidecar = sidecarFor(filePath);
  const metadataPath = metadataPathFor(filePath);
  const metadata = { loader: 'scripts/load-health-claims-monthly.mjs' };

  if (metadataPath) {
    metadata.metadata_path = path.relative(ROOT, metadataPath);
  }
  if (downloadSidecar.metadata_url) {
    metadata.metadata_url = downloadSidecar.metadata_url;
  }

  const result = await client.query(
    `
      insert into meta.dataset_release (
        source_system_id,
        domain_code,
        dataset_code,
        snapshot_label,
        source_url,
        local_path,
        content_sha256,
        row_count,
        metadata,
        status
      )
      values ($1, 'health', $2, $3, $4, $5, $6, $7, $8::jsonb, 'staged')
      on conflict (domain_code, dataset_code, snapshot_label) do update
        set source_url = excluded.source_url,
            local_path = excluded.local_path,
            content_sha256 = excluded.content_sha256,
            row_count = excluded.row_count,
            metadata = excluded.metadata,
            status = excluded.status
      returning dataset_release_id
    `,
    [
      sourceSystemId,
      datasetCode,
      snapshotLabel,
      downloadSidecar.source_url ?? null,
      path.relative(ROOT, filePath),
      downloadSidecar.sha256 ?? null,
      rowCount,
      JSON.stringify(metadata),
    ],
  );
  return Number(result.rows[0].dataset_release_id);
}

async function finalizeDatasetRelease(client, datasetReleaseId, rowCount) {
  await client.query(
    `
      update meta.dataset_release
      set row_count = $1,
          status = 'staged'
      where dataset_release_id = $2
    `,
    [rowCount, datasetReleaseId],
  );
}

async function insertChunked(client, targetTable, datasetReleaseId, payload, rows) {
  const chunkSize = 1000;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize).map((row) => ({
      dataset_release_id: datasetReleaseId,
      ...row,
      payload: row.payload ?? payload,
    }));

    if (targetTable === 'raw.health_claims_payer_monthly') {
      await client.query(
        `
          insert into raw.health_claims_payer_monthly (
            dataset_release_id,
            reporting_year,
            reporting_month,
            payer_code,
            total_quantity,
            payload
          )
          select
            dataset_release_id,
            reporting_year,
            reporting_month,
            payer_code,
            total_quantity,
            payload
          from jsonb_to_recordset($1::jsonb) as t(
            dataset_release_id bigint,
            reporting_year integer,
            reporting_month integer,
            payer_code text,
            total_quantity bigint,
            payload jsonb
          )
        `,
        [JSON.stringify(chunk)],
      );
    } else if (targetTable === 'raw.health_claims_provider_monthly') {
      await client.query(
        `
          insert into raw.health_claims_provider_monthly (
            dataset_release_id,
            reporting_year,
            reporting_month,
            icz,
            total_quantity,
            payload
          )
          select
            dataset_release_id,
            reporting_year,
            reporting_month,
            icz,
            total_quantity,
            payload
          from jsonb_to_recordset($1::jsonb) as t(
            dataset_release_id bigint,
            reporting_year integer,
            reporting_month integer,
            icz text,
            total_quantity bigint,
            payload jsonb
          )
        `,
        [JSON.stringify(chunk)],
      );
    } else if (targetTable === 'raw.health_claims_provider_ico_yearly') {
      await client.query(
        `
          insert into raw.health_claims_provider_ico_yearly (
            dataset_release_id,
            reporting_year,
            provider_ico,
            total_quantity,
            patient_count,
            contact_count,
            payload
          )
          select
            dataset_release_id,
            reporting_year,
            provider_ico,
            total_quantity,
            patient_count,
            contact_count,
            payload
          from jsonb_to_recordset($1::jsonb) as t(
            dataset_release_id bigint,
            reporting_year integer,
            provider_ico text,
            total_quantity bigint,
            patient_count bigint,
            contact_count bigint,
            payload jsonb
          )
        `,
        [JSON.stringify(chunk)],
      );
    } else {
      throw new Error(`Unsupported target table: ${targetTable}`);
    }
  }
}

async function loadDataset(datasetCode) {
  const config = DATASETS[datasetCode];
  const filePath = latestDataPath(datasetCode);
  if (!filePath) {
    console.log(`${datasetCode}: 0 rows (missing source file)`);
    return 0;
  }

  const aggregateResult = await config.aggregate(filePath);
  const rows = aggregateResult.rows;
  const skippedRows = aggregateResult.skippedRows ?? 0;
  const filteredOutRows = aggregateResult.filteredOutRows ?? 0;
  const payload = {
    local_path: path.relative(ROOT, filePath),
    skipped_rows: skippedRows,
    filtered_out_rows: filteredOutRows,
  };
  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query('begin');
    const sourceSystemId = await upsertSourceSystem(client, config.sourceCode);
    const datasetReleaseId = await upsertDatasetRelease(client, {
      sourceSystemId,
      datasetCode,
      filePath,
      rowCount: 0,
    });
    const datasetMetadata = {
      ...(aggregateResult.metadata ?? {}),
      local_path: path.relative(ROOT, filePath),
      skipped_rows: skippedRows,
      filtered_out_rows: filteredOutRows,
    };
    await client.query(
      `
        update meta.dataset_release
        set metadata = coalesce(metadata, '{}'::jsonb) || $1::jsonb
        where dataset_release_id = $2
      `,
      [JSON.stringify(datasetMetadata), datasetReleaseId],
    );
    await client.query(`delete from ${config.targetTable} where dataset_release_id = $1`, [datasetReleaseId]);
    await insertChunked(client, config.targetTable, datasetReleaseId, payload, rows);
    await finalizeDatasetRelease(client, datasetReleaseId, rows.length);
    await client.query('commit');
    const details = [];
    if (skippedRows) details.push(`skipped ${skippedRows} invalid rows`);
    if (filteredOutRows) details.push(`filtered out ${filteredOutRows} non-focus rows`);
    console.log(`${datasetCode}: ${rows.length} rows${details.length ? ` (${details.join(', ')})` : ''}`);
    return rows.length;
  } catch (error) {
    await client.query('rollback');
    throw error;
  } finally {
    client.release();
  }
}

const requested = process.argv.slice(2);
const datasets =
  requested.length > 0
    ? requested
    : Object.keys(DATASETS).filter((datasetCode) => datasetCode !== 'nrhzs_claims_provider_specialty');

let total = 0;
try {
  for (const datasetCode of datasets) {
    if (!(datasetCode in DATASETS)) {
      throw new Error(`Unsupported dataset: ${datasetCode}`);
    }
    total += await loadDataset(datasetCode);
  }
  console.log(`Loaded ${total} health rows`);
} finally {
  await closePool();
}
