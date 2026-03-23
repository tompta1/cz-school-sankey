#!/usr/bin/env node

const baseUrl = (process.env.ATLAS_API_BASE_URL || 'https://cz-school-sankey.vercel.app').replace(/\/$/, '');

async function fetchJson(path) {
  const response = await fetch(`${baseUrl}${path}`, {
    headers: { accept: 'application/json' },
  });

  if (!response.ok) {
    throw new Error(`${path} -> ${response.status}`);
  }

  return response.json();
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const years = await fetchJson('/api/atlas/years');
  assert(years.domain === 'atlas', 'atlas years endpoint returned unexpected domain');

  const overview2024 = await fetchJson('/api/atlas/overview?year=2024&metric=cost');
  assert(Array.isArray(overview2024.nodes) && overview2024.nodes.length > 20, 'overview 2024 has too few nodes');
  assert(
    overview2024.links.some((link) => link.target === 'transport:ministry:md'),
    'overview 2024 is missing transport root link',
  );
  assert(
    overview2024.links.some((link) => link.target === 'agriculture:ministry:mze'),
    'overview 2024 is missing agriculture root link',
  );

  const transport2024 = await fetchJson('/api/atlas/transport?year=2024');
  assert(
    transport2024.nodes.some((node) => node.id === 'transport:sfdi:rail' && node.metadata?.capacity),
    'transport 2024 is missing rail denominator',
  );
  assert(
    transport2024.nodes.some((node) => node.id === 'transport:sfdi:roads-vignette' && node.metadata?.capacity),
    'transport 2024 is missing vignette denominator',
  );
  assert(
    transport2024.nodes.some((node) => node.id === 'transport:sfdi:roads-toll' && node.metadata?.capacity),
    'transport 2024 is missing toll denominator',
  );

  const mv2024 = await fetchJson('/api/atlas/mv?year=2024');
  assert(
    mv2024.nodes.some((node) => node.id === 'security:police'),
    'MV 2024 is missing police branch',
  );

  const justice2024 = await fetchJson('/api/atlas/justice?year=2024');
  assert(
    justice2024.nodes.some((node) => node.id === 'justice:courts'),
    'justice 2024 is missing courts branch',
  );

  const agriculture2024 = await fetchJson('/api/atlas/agriculture?year=2024');
  assert(
    agriculture2024.nodes.some((node) => node.id === 'agriculture:subsidy:total' && node.metadata?.capacity),
    'agriculture 2024 is missing subsidy denominator',
  );

  console.log(`Atlas smoke checks passed for ${baseUrl}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
